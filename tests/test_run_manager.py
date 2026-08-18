import json
import threading
from pathlib import Path
from unittest.mock import patch

import pytest

from eovrt_distribution.distributor import Distributor
from eovrt_distribution.service.run_manager import RunBusyError, RunManager, UnknownRunError
from eovrt_distribution.service.run_request import DistributionRunRequest
from eovrt_distribution.service.settings import ServiceSettings

ALERTA = {
    "alert_id": "a1",
    "experiment_id": "exp",
    "pattern_id": "CR-01",
    "severity": "high",
    "source_id": "cam1",
    "ts_ms": 1000,
}


def _alerts_file(tmp_path: Path) -> Path:
    path = tmp_path / "alerts.jsonl"
    path.write_text(json.dumps(ALERTA) + "\n", encoding="utf-8")
    return path


def _request(tmp_path: Path) -> DistributionRunRequest:
    return DistributionRunRequest(
        mode="replay",
        out_dir=str(tmp_path / "out"),
        alerts_path=str(_alerts_file(tmp_path)),
        config={"channel": {"mode": "dry_run"}},
    )


def _manager(tmp_path: Path) -> RunManager:
    return RunManager(ServiceSettings(runs_dir=tmp_path / "runs"))


def test_start_run_devuelve_id_y_termina(tmp_path: Path) -> None:
    manager = _manager(tmp_path)
    run_id = manager.start_run(_request(tmp_path))
    manager.join_active(timeout=10.0)
    info = manager.get(run_id)
    assert info["status"] == "succeeded"
    assert info["summary"] is not None


def test_run_id_es_segmento_de_path_seguro(tmp_path: Path) -> None:
    manager = _manager(tmp_path)
    run_id = manager.start_run(_request(tmp_path))
    manager.join_active(timeout=10.0)
    assert "/" not in run_id and ".." not in run_id


def test_segunda_corrida_con_una_activa_es_busy(tmp_path: Path) -> None:
    """Cobertura explicita de "una corrida activa por vez" (spec).

    Deterministico (sin `sleep`): la primera corrida se bloquea DENTRO de
    `Distributor.run`, vía un `threading.Event` que el test controla, para
    que este demostrablemente activa -- `self._active` no puede limpiarse,
    porque el hilo de trabajo nunca llega a su `finally` -- cuando se dispara
    la segunda `start_run()`.

    Antes de esto, el test llamaba a `start_run` dos veces seguidas contra
    una corrida real y trivial (un solo alert, canal dry_run), que en la
    practica termina en microsegundos. Tras el fix del segundo race (round 1
    de esta tarea: el slot se libera bajo lock apenas la corrida termina, no
    cuando alguien lo infiere de `status`), ese test paso a depender de que
    la primera corrida siguiera viva en el instante exacto del segundo
    `start_run()` -- una carrera contra el scheduler. Confirmado en CI: fallo
    intermitente, 1 de 12 corridas completas de la suite. Bloquear la corrida
    en un punto conocido elimina la carrera por completo: no importa que tan
    rapido o lento se planifique el hilo de trabajo, no puede terminar hasta
    que el test suelte el Event.
    """
    manager = _manager(tmp_path)

    unblock_run = threading.Event()
    orig_run = Distributor.run

    def blocking_run(self: Distributor):
        assert unblock_run.wait(timeout=5.0), "unblock_run nunca se solto (deadlock?)"
        return orig_run(self)

    with patch.object(Distributor, "run", blocking_run):
        first_run_id = manager.start_run(_request(tmp_path))
        try:
            with pytest.raises(RunBusyError) as exc_info:
                manager.start_run(_request(tmp_path))
            assert exc_info.value.active_run_id == first_run_id
        finally:
            unblock_run.set()
            manager.join_active(timeout=10.0)


def test_get_de_id_desconocido(tmp_path: Path) -> None:
    with pytest.raises(UnknownRunError):
        _manager(tmp_path).get("no-existe")


def test_alerts_inexistente_es_file_not_found(tmp_path: Path) -> None:
    manager = _manager(tmp_path)
    request = DistributionRunRequest(
        mode="replay",
        out_dir=str(tmp_path / "out"),
        alerts_path=str(tmp_path / "no-esta.jsonl"),
        config={"channel": {"mode": "dry_run"}},
    )
    with pytest.raises(FileNotFoundError):
        manager.start_run(request)


def test_reserva_de_start_run_es_atomica_bajo_concurrencia(tmp_path: Path) -> None:
    """Regresion deterministica (sin `sleep`) para la condicion de carrera de
    `start_run`: el chequeo de "hay una activa" y la reserva del slot deben
    ocurrir bajo LA MISMA toma de `self._lock`, sin soltarlo entre medio.

    Estrategia: bloquear `_build_source` con un `threading.Event` mientras el
    hilo A todavia sostiene `self._lock` (dentro de la seccion critica de
    `start_run`). El hilo B intenta arrancar mientras A esta bloqueado:

    - Con la reserva ATOMICA (correcta): B se queda bloqueado en
      `self._lock.acquire()` -- un mutex real, no una carrera de timing --
      hasta que A libera el lock ya con el slot reservado. Cuando B por fin
      entra, ve `self._active is not None` y levanta `RunBusyError` de
      inmediato.
    - Con check-then-act (el bug original del brief: chequear, SOLTAR el
      lock, construir, recien ahi reservar): B pasaria el chequeo mientras A
      todavia esta construyendo (el lock ya esta libre en ese punto), y
      terminarian arrancando DOS corridas.

    Tambien se bloquea `Distributor.run` (con un segundo Event) para que el
    hilo de trabajo de A no pueda terminar -- y liberar el slot -- antes de
    que se capture el resultado de B. Sin este segundo bloqueo, una corrida
    trivial (un solo alert, dry_run) podria terminar tan rapido que B viera
    el slot libre por una razon LEGITIMA (A ya termino de verdad), lo cual no
    es el bug que este test busca detectar -- confirmado empiricamente: un
    primer intento de este tipo de test, sin este segundo bloqueo, reportaba
    "mas de una corrida exitosa" con el codigo YA CORREGIDO, por esa razon.

    Nota sobre determinismo: `blocking_build_source` solo bloquea al hilo
    identificado como "start-a" (por nombre); si el codigo tuviera el bug de
    check-then-act, el chequeo de B pasaria con el lock YA LIBRE (A lo suelta
    antes de bloquearse en el build) y B llegaria aca sin bloquear, libre
    para terminar de inmediato. Por eso, ANTES de liberar `unblock_build`,
    este test le da a B una ventana bien holgada (`join(timeout=...)`, nunca
    un `sleep` ciego) para terminar por su cuenta si el chequeo lo dejo
    pasar: en la version corregida esto NUNCA puede pasar (B esta bloqueado
    en un mutex real, `self._lock.acquire()`, que solo A puede liberar), asi
    que la espera agota su timeout sin ambiguedad; en la version con el bug,
    el trabajo de B es sincrono y trivial (un `stat` de archivo y construir
    un par de objetos) y termina en microsegundos, muy por debajo del margen
    de la ventana -- confirmado revirtiendo el fix a mano: con esta ventana
    de observacion el test detecto el bug en 10/10 corridas (sin ella,
    ~60%, porque a veces el chequeo de B llegaba a tiempo por casualidad).

    Todos los `.wait()`/`.join()` llevan timeout defensivo: si algo se traba,
    el test falla con un mensaje claro en vez de colgar la suite.
    """
    manager = _manager(tmp_path)

    build_started = threading.Event()
    unblock_build = threading.Event()
    unblock_run = threading.Event()

    orig_build_source = manager._build_source  # bound method: ya incluye `self`

    def blocking_build_source(request: DistributionRunRequest):
        if threading.current_thread().name == "start-a":
            build_started.set()
            assert unblock_build.wait(timeout=5.0), "unblock_build nunca se solto (deadlock?)"
        return orig_build_source(request)

    manager._build_source = blocking_build_source  # type: ignore[method-assign]

    orig_distributor_run = Distributor.run

    def blocking_distributor_run(self: Distributor):
        assert unblock_run.wait(timeout=5.0), "unblock_run nunca se solto (deadlock?)"
        return orig_distributor_run(self)

    results: dict[str, tuple[str, str]] = {}

    def call_start(label: str) -> None:
        try:
            run_id = manager.start_run(_request(tmp_path))
            results[label] = ("ok", run_id)
        except RunBusyError as exc:
            results[label] = ("busy", exc.active_run_id)

    with patch.object(Distributor, "run", blocking_distributor_run):
        thread_a = threading.Thread(target=call_start, args=("a",), name="start-a")
        thread_a.start()
        assert build_started.wait(timeout=5.0), "el hilo A nunca entro a _build_source"

        # En este punto A sostiene `self._lock` DENTRO de `_build_source`. Si
        # B llega a `with self._lock:` ahora, tiene que bloquear de verdad
        # (no es una carrera: es un mutex tomado) -- EN LA VERSION CORRECTA.
        thread_b = threading.Thread(target=call_start, args=("b",), name="start-b")
        thread_b.start()

        # Ventana de observacion, ver docstring: en la version correcta este
        # `join` SIEMPRE agota el timeout (B no puede terminar, esta
        # bloqueado en el lock); en una version con check-then-act, B corre
        # libre (el wrapper no lo bloquea a el) y termina mucho antes de que
        # se cumpla este margen generoso.
        thread_b.join(timeout=0.3)

        unblock_build.set()

        thread_a.join(timeout=5.0)
        thread_b.join(timeout=5.0)
        assert not thread_a.is_alive(), "hilo A no termino a tiempo (revisar deadlock)"
        assert not thread_b.is_alive(), "hilo B no termino a tiempo (revisar deadlock)"

        assert "a" in results and "b" in results, f"faltan resultados: {results}"
        outcomes = {results["a"][0], results["b"][0]}
        assert outcomes == {"ok", "busy"}, (
            f"se esperaba exactamente un 'ok' y un 'busy', se obtuvo: {results}"
        )

        ok_label = "a" if results["a"][0] == "ok" else "b"
        busy_label = "b" if ok_label == "a" else "a"
        # El `RunBusyError` del perdedor tiene que apuntar al run_id del ganador.
        assert results[busy_label][1] == results[ok_label][1]

        unblock_run.set()
        manager.join_active(timeout=10.0)


def test_forget_elimina_la_corrida_del_registro(tmp_path: Path) -> None:
    manager = _manager(tmp_path)
    run_id = manager.start_run(_request(tmp_path))
    manager.join_active(timeout=10.0)

    manager.forget(run_id)

    with pytest.raises(UnknownRunError):
        manager.get(run_id)


def test_forget_no_toca_out_dir(tmp_path: Path) -> None:
    """`forget` es puramente un olvido del registro EN MEMORIA -- `out_dir`
    es del cliente, no de `RunManager`, y debe seguir intacto en disco."""
    manager = _manager(tmp_path)
    request = _request(tmp_path)
    run_id = manager.start_run(request)
    manager.join_active(timeout=10.0)

    manager.forget(run_id)

    assert (Path(request.out_dir) / "distribution_summary.json").is_file()


def test_forget_de_id_desconocido_levanta_unknown_run_error(tmp_path: Path) -> None:
    with pytest.raises(UnknownRunError):
        _manager(tmp_path).forget("no-existe")


def test_forget_de_corrida_activa_levanta_run_busy_error(tmp_path: Path) -> None:
    """Defensa en profundidad (ronda 2 de fixes): el router de HTTP ya
    chequea `status == "running"` antes de llamar a `forget`, pero el metodo
    es publico -- un llamador directo no deberia poder dejar `self._active`
    apuntando a un estado que ya no esta en `self._runs`. Bloqueo
    deterministico (sin `sleep`), misma tecnica que
    `test_segunda_corrida_con_una_activa_es_busy`.
    """
    manager = _manager(tmp_path)

    unblock_run = threading.Event()
    orig_run = Distributor.run

    def blocking_run(self: Distributor):
        assert unblock_run.wait(timeout=5.0), "unblock_run nunca se solto (deadlock?)"
        return orig_run(self)

    with patch.object(Distributor, "run", blocking_run):
        run_id = manager.start_run(_request(tmp_path))
        try:
            with pytest.raises(RunBusyError) as exc_info:
                manager.forget(run_id)
            assert exc_info.value.active_run_id == run_id
            # No se borro: sigue en el registro.
            manager.get(run_id)
        finally:
            unblock_run.set()
            manager.join_active(timeout=10.0)
