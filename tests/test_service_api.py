import json
import threading
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from eovrt_distribution.distributor import Distributor
from eovrt_distribution.service.app import create_app
from eovrt_distribution.service.run_manager import RunBusyError, RunManager
from eovrt_distribution.service.settings import ServiceSettings


def _client(tmp_path: Path) -> TestClient:
    app = create_app(ServiceSettings(runs_dir=tmp_path / "runs"))
    return TestClient(app)


def test_healthz_responde_ok(tmp_path: Path) -> None:
    with _client(tmp_path) as client:
        response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_readyz_responde_ok(tmp_path: Path) -> None:
    with _client(tmp_path) as client:
        response = client.get("/readyz")
    assert response.status_code == 200


def test_lifespan_crea_runs_dir(tmp_path: Path) -> None:
    runs_dir = tmp_path / "runs"
    app = create_app(ServiceSettings(runs_dir=runs_dir))
    with TestClient(app):
        pass
    assert runs_dir.is_dir()


def _replay_body(tmp_path):
    alerts = tmp_path / "alerts.jsonl"
    alerts.write_text(
        json.dumps(
            {
                "alert_id": "a1",
                "experiment_id": "exp",
                "pattern_id": "CR-01",
                "severity": "high",
                "source_id": "cam1",
                "ts_ms": 1000,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    return {
        "mode": "replay",
        "out_dir": str(tmp_path / "out"),
        "alerts_path": str(alerts),
        "config": {"channel": {"mode": "dry_run"}},
    }


def test_post_runs_devuelve_201_y_llega_a_terminal(tmp_path):
    with _client(tmp_path) as client:
        created = client.post("/api/runs", json=_replay_body(tmp_path))
        assert created.status_code == 201
        run_id = created.json()["distribution_run_id"]
        client.app.state.manager.join_active(timeout=10.0)
        info = client.get(f"/api/runs/{run_id}")
    assert info.status_code == 200
    assert info.json()["status"] == "succeeded"
    assert info.json()["summary"] is not None


def test_campo_desconocido_da_422(tmp_path):
    body = _replay_body(tmp_path) | {"modo": "replay"}
    with _client(tmp_path) as client:
        response = client.post("/api/runs", json=body)
    assert response.status_code == 422


def test_run_desconocido_da_404(tmp_path):
    with _client(tmp_path) as client:
        response = client.get("/api/runs/no-existe")
    assert response.status_code == 404


def test_run_id_con_traversal_da_404(tmp_path):
    """Contrato end-to-end: un `run_id` tipo path-traversal siempre da 404.

    `%2F` (barra) nunca llega al handler: Starlette la decodifica antes de
    matchear la ruta y devuelve el 404 GENERICO de routing. `%2e%2e` (puntos
    codificados, SIN barra) si matchea `/api/runs/{run_id}` con
    `run_id=".."` y llega de verdad al handler y a `require_valid_run_id`.

    OJO -- esto NO es una prueba de que el guard dispare: se verifico a mano
    que este test sigue pasando igual si se comenta `require_valid_run_id`
    en `get_run`, porque el fallback de `RunManager.get()` (un `KeyError` de
    dict) produce el MISMO 404 con el MISMO detail para cualquier id
    inexistente, sea invalido o simplemente no-creado-todavia. La cobertura
    que SI distingue "el guard disparo" de "el guard no estaba" vive en
    `tests/test_run_ids.py` (unit, sobre `is_valid_run_id`/
    `require_valid_run_id` directamente)."""
    with _client(tmp_path) as client:
        response = client.get("/api/runs/%2e%2e")
    assert response.status_code == 404
    assert response.json()["detail"] == "Run desconocido: .."


def test_current_sin_corrida_da_404(tmp_path):
    with _client(tmp_path) as client:
        response = client.get("/api/runs/current")
    assert response.status_code == 404


def _bloquear_distributor_run() -> tuple[threading.Event, object]:
    """Devuelve `(unblock_event, patcher)` para bloquear `Distributor.run`
    de forma DETERMINISTICA (sin `sleep`) -- misma tecnica que
    `tests/test_run_manager.py::test_segunda_corrida_con_una_activa_es_busy`:
    bloquear DENTRO de `Distributor.run` para que la corrida este
    demostrablemente activa (el hilo nunca llega a su `finally`, que es lo
    que libera el slot) mientras el test ejerce la ruta concurrente."""
    unblock_run = threading.Event()
    orig_run = Distributor.run

    def blocking_run(self: Distributor):
        assert unblock_run.wait(timeout=5.0), "unblock_run nunca se solto (deadlock?)"
        return orig_run(self)

    return unblock_run, patch.object(Distributor, "run", blocking_run)


def test_post_runs_con_una_activa_da_409_con_active_run_id(tmp_path):
    with _client(tmp_path) as client:
        unblock_run, patcher = _bloquear_distributor_run()
        with patcher:
            first = client.post("/api/runs", json=_replay_body(tmp_path))
            assert first.status_code == 201
            first_run_id = first.json()["distribution_run_id"]
            try:
                second = client.post("/api/runs", json=_replay_body(tmp_path))
                assert second.status_code == 409
                assert second.json()["active_run_id"] == first_run_id
            finally:
                unblock_run.set()
                client.app.state.manager.join_active(timeout=10.0)


def test_alerts_path_inexistente_da_422(tmp_path):
    """Camino real del `except (ValueError, FileNotFoundError)` del handler
    -- a diferencia de `test_campo_desconocido_da_422`, que Pydantic rechaza
    ANTES de que el handler se ejecute."""
    body = _replay_body(tmp_path)
    body["alerts_path"] = str(tmp_path / "no-existe.jsonl")
    with _client(tmp_path) as client:
        response = client.post("/api/runs", json=body)
    assert response.status_code == 422


def test_delete_run_terminada_borra_del_registro_pero_no_el_out_dir(tmp_path):
    with _client(tmp_path) as client:
        created = client.post("/api/runs", json=_replay_body(tmp_path))
        run_id = created.json()["distribution_run_id"]
        client.app.state.manager.join_active(timeout=10.0)
        deleted = client.delete(f"/api/runs/{run_id}")
        assert deleted.status_code == 204
        after = client.get(f"/api/runs/{run_id}")
    assert after.status_code == 404
    # `out_dir` es del cliente: el DELETE no debe tocarlo.
    assert (tmp_path / "out" / "distribution_summary.json").is_file()


def test_doble_delete_del_mismo_run_da_404_en_el_segundo(tmp_path):
    """Contrato basico: repetir un DELETE ya aplicado da 404, no un error.

    OJO -- esto NO reproduce por si solo el bug real de la ronda 2
    (`forget()` sin capturar reventando en 500): en un doble DELETE
    SECUENCIAL, el SEGUNDO llamado nunca llega a la linea vulnerable,
    porque su PROPIO `manager.get(run_id)` de arriba (que nunca dejo de
    estar en un `try/except`) ya ve el run ausente y corta ahi con 404 --
    confirmado corriendo este test con `forget()` reintroducido AFUERA de
    su `try/except`: sigue pasando igual. Es un buen test de contrato, pero
    la regresion real (dos requests que ambos pasan el `get()` mientras el
    run todavia existe, y compiten por `forget()`) esta cubierta por
    `test_dos_delete_concurrentes_del_mismo_run_nunca_revientan_con_excepcion`
    de aca abajo, que SI la reproduce (ver su docstring)."""
    with _client(tmp_path) as client:
        created = client.post("/api/runs", json=_replay_body(tmp_path))
        run_id = created.json()["distribution_run_id"]
        client.app.state.manager.join_active(timeout=10.0)
        primero = client.delete(f"/api/runs/{run_id}")
        segundo = client.delete(f"/api/runs/{run_id}")
    assert primero.status_code == 204
    assert segundo.status_code == 404


def test_dos_delete_concurrentes_del_mismo_run_nunca_revientan_con_excepcion(tmp_path):
    """Reproduce DETERMINISTICAMENTE (sin `sleep`) la carrera real del
    Hallazgo 1 (ronda 2): dos DELETE que ambos pasan el `manager.get(run_id)`
    inicial mientras el run TODAVIA existe, y compiten por `forget()` --
    exactamente el escenario que el revisor reprodujo y que termino en
    `run_manager.py:205 raise UnknownRunError` sin capturar.

    Por que no alcanza con bloquear por NOMBRE de hilo (la tecnica que ya
    usa el resto de la suite, p.ej. `test_segunda_corrida_con_una_activa_es_busy`):
    los endpoints sync de FastAPI corren en un threadpool de anyio, asi que
    el hilo que de verdad ejecuta `delete_run` NUNCA tiene el nombre del
    hilo Python que llamo a `client.delete(...)` -- confirmado a mano
    (`threading.current_thread().name` adentro del handler no es
    "delete-a"/"delete-b"). En cambio, se serializa por ORDEN DE LLEGADA a
    `forget()`: el PRIMERO en llegar (sea cual sea su hilo) se bloquea
    ANTES de tocar `self._runs` y espera a que el SEGUNDO corra su
    `forget()` de punta a punta (que si ve el run, todavia no lo borraron, y
    lo borra con exito -> 204); recien entonces el primero se desbloquea y
    llama al `forget()` real sobre un run que YA NO ESTA -> exactamente la
    condicion que `UnknownRunError` sin capturar hacia explotar.
    """
    with _client(tmp_path) as client:
        created = client.post("/api/runs", json=_replay_body(tmp_path))
        run_id = created.json()["distribution_run_id"]
        client.app.state.manager.join_active(timeout=10.0)

        orig_forget = RunManager.forget
        lock = threading.Lock()
        counter = {"n": 0}
        first_blocked = threading.Event()
        second_done = threading.Event()

        def blocking_forget(self: RunManager, forget_run_id: str):
            with lock:
                counter["n"] += 1
                is_first = counter["n"] == 1
            if is_first:
                first_blocked.set()
                assert second_done.wait(timeout=5.0), "el segundo forget() nunca corrio (deadlock?)"
            try:
                return orig_forget(self, forget_run_id)
            finally:
                if not is_first:
                    second_done.set()

        results: dict[str, tuple[str, object]] = {}

        def call_delete(label: str) -> None:
            try:
                response = client.delete(f"/api/runs/{run_id}")
                results[label] = ("ok", response.status_code)
            except Exception as exc:  # noqa: BLE001 -- justo lo que este test quiere detectar
                results[label] = ("exception", repr(exc))

        with patch.object(RunManager, "forget", blocking_forget):
            thread_a = threading.Thread(target=call_delete, args=("a",), name="delete-a")
            thread_a.start()
            assert first_blocked.wait(timeout=5.0), "ningun DELETE llego a forget() (deadlock?)"

            thread_b = threading.Thread(target=call_delete, args=("b",), name="delete-b")
            thread_b.start()
            thread_b.join(timeout=5.0)
            assert not thread_b.is_alive(), "hilo B no termino a tiempo"

            thread_a.join(timeout=5.0)
            assert not thread_a.is_alive(), "hilo A no termino a tiempo"

    assert "a" in results and "b" in results, f"faltan resultados: {results}"
    assert results["a"][0] == "ok", f"el DELETE perdedor no debe reventar: {results['a']}"
    assert results["b"][0] == "ok", f"el DELETE ganador no debe reventar: {results['b']}"
    assert {results["a"][1], results["b"][1]} == {204, 404}


def test_delete_run_activa_da_409(tmp_path):
    with _client(tmp_path) as client:
        unblock_run, patcher = _bloquear_distributor_run()
        with patcher:
            created = client.post("/api/runs", json=_replay_body(tmp_path))
            run_id = created.json()["distribution_run_id"]
            try:
                response = client.delete(f"/api/runs/{run_id}")
                assert response.status_code == 409
            finally:
                unblock_run.set()
                client.app.state.manager.join_active(timeout=10.0)


def test_delete_run_da_409_si_forget_levanta_run_busy_error(tmp_path):
    """Regresion de la ronda 3 de fixes: `forget()` puede levantar
    `RunBusyError` (guard de la ronda 2, defensa en profundidad) cuando el
    `run_id` es el de la corrida activa. Eso es real incluso para una
    corrida ya "terminada" desde afuera: hay una ventana de pocos
    bytecodes en `_target` (`run_manager.py`) entre que `state.status` se
    escribe SIN lock (ya terminal) y `self._active` se limpia bajo lock en
    el `finally` -- si `delete_run` cae ahi, `forget()` ve `self._active`
    todavia apuntando a este run y levanta `RunBusyError`.

    Esa ventana es de nanosegundos y no es practico de reproducir con
    timing real (a diferencia de `test_delete_run_activa_da_409`, que
    bloquea la corrida de punta a punta con un Event). Para cubrir el
    camino del router sin depender de esa ventana, se parchea `forget()`
    directamente para forzar el `RunBusyError` -- lo que importa ac'a es
    que `delete_run` lo atrape y devuelva 409 (nunca una excepcion sin
    manejar)."""
    with _client(tmp_path) as client:
        created = client.post("/api/runs", json=_replay_body(tmp_path))
        run_id = created.json()["distribution_run_id"]
        client.app.state.manager.join_active(timeout=10.0)

        def forget_ocupado(self: RunManager, forget_run_id: str) -> None:
            raise RunBusyError(forget_run_id)

        with patch.object(RunManager, "forget", forget_ocupado):
            response = client.delete(f"/api/runs/{run_id}")

    assert response.status_code == 409
    assert response.json()["active_run_id"] == run_id


def test_cancel_run_desconocido_da_404(tmp_path):
    with _client(tmp_path) as client:
        response = client.post("/api/runs/no-existe/cancel")
    assert response.status_code == 404
