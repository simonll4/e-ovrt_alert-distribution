"""Modo live (spec 45 §9.4) y cancelacion cooperativa via la API HTTP.

Reusa el patron de `tests/test_zmq_source.py`: un PUB/XPUB real efimero via
`bind_to_random_port` -- NUNCA un puerto fijo, porque un puerto ya ocupado por
una corrida anterior (o por otro proceso) volveria el test intermitente -- y
sincronizacion DETERMINISTICA (esperar las dos suscripciones del `ZmqSource`
en el XPUB) en vez de `sleep`, con timeouts defensivos en todo `join`/`poll`
para que un cuelgue haga FALLAR el test en vez de trabar la suite entera.

La trampa que estos tests deben DETECTAR, nunca causar: cerrar un socket ZMQ
desde un hilo distinto del que lo creo mientras otro esta en `recv_multipart`
aborta el proceso con SIGABRT (ver `transport/zmq_source.py`). Si eso pasara,
el proceso de pytest moriria entero y ningun `assert` de este archivo llegaria
a ejecutarse -- por eso la parada es siempre cooperativa (`request_stop()`).
"""

from __future__ import annotations

import json

import msgpack
import pytest
import zmq
from fastapi.testclient import TestClient

from eovrt_distribution.service.app import create_app
from eovrt_distribution.service.settings import ServiceSettings


def _envelope_frames(topic: str, seq: int, payload: dict, ts: float = 1750000000000.0):
    """Frames de `bus.envelope.v1` -- mismo formato que `tests/test_zmq_source.py`."""
    env = {
        "schema_version": "bus.envelope.v1",
        "topic": topic,
        "key": payload.get("source_id", ""),
        "seq": seq,
        "ts_publish_ms": ts,
        "payload": json.dumps(payload, ensure_ascii=True).encode(),
    }
    return [topic.encode(), msgpack.packb(env)]


@pytest.fixture
def bus():
    """XPUB efimero (puerto elegido por el SO/zmq, nunca fijo). Espejo de la
    fixture `publisher` de `tests/test_zmq_source.py`."""
    ctx = zmq.Context.instance()
    sock = ctx.socket(zmq.XPUB)
    port = sock.bind_to_random_port("tcp://127.0.0.1")
    yield sock, f"tcp://127.0.0.1:{port}"
    sock.close(0)


def _wait_subscriptions(sock, expected: int = 2) -> None:
    """El `ZmqSource` suscribe DOS prefijos (alertas + lifecycle) al arrancar
    su hilo, ANTES de entrar al loop de `recv_multipart`. Leerlos del XPUB es
    la señal deterministica de que el socket ya se creo/conecto/suscribio --
    reemplaza cualquier `sleep` para "esperar a que el hilo arranque". Cada
    `poll` lleva timeout defensivo: si la suscripcion nunca llega, el test
    falla con un mensaje claro en vez de colgar la suite."""
    for _ in range(expected):
        assert sock.poll(timeout=5000), "el ZmqSource nunca se suscribio (deadlock?)"
        sock.recv()


def _live_body(tmp_path, endpoint, *, idle_timeout_ms: float | None = None) -> dict:
    body = {
        "mode": "live",
        "out_dir": str(tmp_path / "out"),
        "endpoint": endpoint,
        "config": {"channel": {"mode": "dry_run"}},
    }
    if idle_timeout_ms is not None:
        body["idle_timeout_ms"] = idle_timeout_ms
    return body


def test_live_termina_por_idle_timeout(tmp_path, bus):
    """Fix round 1, Hallazgo 1: sin `_wait_subscriptions`, este test pasaba
    igual con un endpoint donde NADIE bindeo nunca un XPUB (confirmado
    empiricamente por el revisor) -- solo probaba "un idle timeout produce un
    estado terminal", no que el `ZmqSource` se haya conectado y suscripto al
    bus de verdad. Ahora exige la MISMA prueba de conexion que los otros tres
    tests (leer las dos suscripciones reales en el XPUB) antes de esperar el
    timeout, y la aserción mira el `termination_reason` real del summary, no
    solo el `status` -- ver `verify_idle_timeout_needs_listener.py` (scratch,
    no forma parte de la suite) para la reproduccion del fallo sin el fix."""
    sock, endpoint = bus
    app = create_app(ServiceSettings(runs_dir=tmp_path / "runs"))
    with TestClient(app) as client:
        created = client.post(
            "/api/runs", json=_live_body(tmp_path, endpoint, idle_timeout_ms=500.0)
        )
        assert created.status_code == 201
        run_id = created.json()["distribution_run_id"]
        _wait_subscriptions(sock)  # prueba real de conexion: sin esto, un endpoint
        # muerto pasaria igual (idle timeout dispara de cualquier forma)
        app.state.manager.join_active(timeout=15.0)
        info = client.get(f"/api/runs/{run_id}").json()
    assert info["status"] in {"succeeded", "cancelled"}
    assert info["summary"]["source_stats"]["termination_reason"] == "idle_timeout"


def test_cancel_detiene_sin_sigabrt(tmp_path, bus):
    """Sin `idle_timeout_ms`: la corrida solo puede terminar por cancelacion.

    Si el socket se cerrara desde otro hilo en vez de pararse cooperativamente,
    el proceso moriria con SIGABRT y el test no llegaria a la asercion final.
    """
    sock, endpoint = bus
    app = create_app(ServiceSettings(runs_dir=tmp_path / "runs"))
    with TestClient(app) as client:
        created = client.post("/api/runs", json=_live_body(tmp_path, endpoint))
        assert created.status_code == 201
        run_id = created.json()["distribution_run_id"]
        _wait_subscriptions(sock)  # el ZmqSource ya conecto y esta en su recv-loop
        cancelled = client.post(f"/api/runs/{run_id}/cancel")
        assert cancelled.status_code == 202
        app.state.manager.join_active(timeout=15.0)
        info = client.get(f"/api/runs/{run_id}").json()
    # Fix round 1, hallazgo menor: sin `idle_timeout_ms` la corrida SOLO puede
    # terminar por cancelacion -- "succeeded" era rama muerta. El
    # `termination_reason` es lo que realmente distingue una parada
    # cooperativa de cualquier otro motivo de fin.
    assert info["status"] == "cancelled"
    assert info["summary"]["source_stats"]["termination_reason"] == "requested_stop"


def test_live_entrega_una_alerta_real_de_punta_a_punta(tmp_path, bus, make_alert):
    """Fix round 1, Hallazgo 2: los otros tres tests cubren ciclo de vida
    (arranque/cancelacion/exclusion/apagado) pero ninguno hace pasar un
    mensaje real -- el camino `HTTP -> ZmqSource -> policy -> channel ->
    artefactos` no tenia cobertura a nivel de servicio (si a nivel unitario en
    `tests/test_zmq_source.py`, que nunca pasa por el servicio). Este test
    publica UNA alerta real por el XPUB, la sigue con el `run_finished` de
    lifecycle (mismo patron determinista que
    `test_zmq_source.py::test_live_stream_until_run_finished`: sin eso, la
    corrida se quedaria esperando en el bus para siempre, sin `idle_timeout_ms`
    -- publicar antes de `_wait_subscriptions` perderia el mensaje, un SUB no
    recibe nada emitido antes de que su suscripcion llegue al XPUB) y verifica
    que el servicio la proceso de punta a punta: el summary refleja la entrega
    y `notifications.jsonl` quedo escrito en `out_dir` con esa alerta."""
    sock, endpoint = bus
    app = create_app(ServiceSettings(runs_dir=tmp_path / "runs"))
    with TestClient(app) as client:
        created = client.post("/api/runs", json=_live_body(tmp_path, endpoint))
        assert created.status_code == 201
        run_id = created.json()["distribution_run_id"]
        _wait_subscriptions(sock)  # sin esto, publicar ahora perderia el mensaje

        alert = make_alert()  # control_run_id="cr-test" por default (conftest.py)
        sock.send_multipart(_envelope_frames("control.alert.v1.cr-test", 0, alert))
        sock.send_multipart(
            _envelope_frames("run.lifecycle.v1.cr-test", 1, {"event": "run_finished"})
        )

        app.state.manager.join_active(timeout=15.0)
        info = client.get(f"/api/runs/{run_id}").json()

    assert info["status"] == "succeeded"
    assert info["summary"]["source_stats"]["termination_reason"] == "run_finished"
    assert info["summary"]["source_stats"]["read"] == 1
    assert info["summary"]["counts"] == {"delivered": 1}

    notifications_path = tmp_path / "out" / "notifications.jsonl"
    records = [
        json.loads(line)
        for line in notifications_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(records) == 1
    assert records[0]["outcome"] == "delivered"
    assert records[0]["alert_id"] == alert["alert_id"]


def test_segunda_corrida_con_live_activa_da_409(tmp_path, bus):
    sock, endpoint = bus
    app = create_app(ServiceSettings(runs_dir=tmp_path / "runs"))
    with TestClient(app) as client:
        created = client.post("/api/runs", json=_live_body(tmp_path, endpoint))
        assert created.status_code == 201
        first_run_id = created.json()["distribution_run_id"]
        _wait_subscriptions(sock)  # la primera corrida ya esta demostrablemente activa

        segunda = client.post("/api/runs", json=_live_body(tmp_path, endpoint))
        assert segunda.status_code == 409
        assert segunda.json()["active_run_id"] == first_run_id

        # Limpieza deterministica: sin `idle_timeout_ms` la corrida no termina
        # sola -- cancelarla explicitamente evita depender del shutdown del
        # lifespan (que igual la pararia, pero mas lento) para este join.
        client.post(f"/api/runs/{first_run_id}/cancel")
        app.state.manager.join_active(timeout=15.0)


def test_apagado_con_live_activa_no_cuelga(tmp_path, bus):
    """Salir del `with TestClient` dispara el lifespan de apagado
    (`manager.shutdown()` + `manager.join_active(timeout=10.0)`, ver
    `service/app.py`): tiene que parar la corrida live cooperativamente. Si
    `request_stop()` no fuera realmente cooperativo (p.ej. si algo cerrara el
    socket desde otro hilo), esto colgaria o abortaria el proceso con SIGABRT
    en vez de llegar hasta la asercion final.
    """
    sock, endpoint = bus
    app = create_app(ServiceSettings(runs_dir=tmp_path / "runs"))
    with TestClient(app) as client:
        created = client.post("/api/runs", json=_live_body(tmp_path, endpoint))
        assert created.status_code == 201
        _wait_subscriptions(sock)

    runs = app.state.manager.list_runs()
    assert runs, "el apagado no debe perder el registro de la corrida"
    assert runs[0]["status"] in {"cancelled", "succeeded", "failed"}
