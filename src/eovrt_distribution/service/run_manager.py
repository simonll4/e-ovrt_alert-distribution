"""Gestor de corridas del servicio de distribucion (spec 45 §9.4).

Una corrida activa por vez. La corrida vive en un hilo propio; la fuente ZeroMQ
se detiene de forma COOPERATIVA (`request_stop()`), nunca cerrando su socket
desde otro hilo: eso aborta el proceso con SIGABRT.

`start_run` reserva el slot de forma ATOMICA, con dos mitades:

1. Chequeo + reserva sin soltar el lock. El chequeo de "hay una corrida
   activa" (`self._active is not None`) y la reserva (`self._active = state`)
   ocurren bajo LA MISMA toma de `self._lock`, sin soltarlo entre medio --
   incluida la carga de la config y la construccion de la fuente. FastAPI
   corre los endpoints sync en un threadpool, asi que dos `POST /api/runs`
   concurrentes son alcanzables de verdad; si el chequeo y la reserva se
   hicieran en dos tomas de lock separadas (check-then-act con el lock suelto
   en el medio), ambos POST podrian pasar el chequeo antes de que ninguno
   reserve, y arrancarian dos corridas. Con una sola toma continua, el segundo
   `start_run` que entra a la seccion critica ve el slot ya tomado por el
   primero y levanta `RunBusyError` de inmediato. Si la carga de la config o
   la construccion de la fuente fallan (`FileNotFoundError`, error de
   validacion), la excepcion se propaga ANTES de llegar a `self._active =
   state`: el `with` libera el lock igual (una excepcion no lo deja tomado) y
   el manager queda libre para el proximo intento -- no hay reserva colgada
   de una corrida que nunca llego a construirse.

2. La liberacion del slot es TAMBIEN bajo lock, hecha por el propio hilo de
   la corrida al terminar (ver `_target` mas abajo), nunca inferida leyendo
   `state.status` sin sincronizacion. Con corridas triviales (un solo alert,
   canal dry_run) el hilo puede terminar en microsegundos: si `start_run`
   determinara "hay una activa" mirando `state.status == "running"`, una
   corrida podria terminar y cambiar ese campo justo entre el chequeo y la
   reserva de otro llamador, sin que ninguno tomara el lock en el medio --
   dos corridas arrancarian igual (confirmado con un script de estres: 20
   `start_run` concurrentes, barrera comun, corrida dry_run trivial -> mas de
   una ganaba). Por eso `self._active` se limpia a `None` en el mismo lock
   que lo pone: es la unica fuente de verdad para "hay una corrida viva", y
   su lectura y escritura estan siempre serializadas por `self._lock`.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from eovrt_distribution.channels.mqtt import MqttChannel
from eovrt_distribution.config import DistributionConfig
from eovrt_distribution.distributor import Distributor
from eovrt_distribution.policy import NotificationPolicy
from eovrt_distribution.service.run_ids import new_distribution_run_id
from eovrt_distribution.service.run_request import DistributionRunRequest
from eovrt_distribution.service.settings import ServiceSettings
from eovrt_distribution.sources import JsonlReplaySource


class RunBusyError(RuntimeError):
    def __init__(self, active_run_id: str) -> None:
        super().__init__(f"Ya hay una corrida activa: {active_run_id}")
        self.active_run_id = active_run_id


class UnknownRunError(KeyError):
    pass


@dataclass
class _RunState:
    run_id: str
    status: str = "running"
    summary: dict[str, Any] | None = None
    error: str | None = None
    out_dir: str = ""
    source: Any = None
    thread: threading.Thread | None = field(default=None, repr=False)


class RunManager:
    def __init__(self, settings: ServiceSettings) -> None:
        self._settings = settings
        self._lock = threading.Lock()
        self._runs: dict[str, _RunState] = {}
        self._active: _RunState | None = None

    def start_run(self, request: DistributionRunRequest) -> str:
        # TODA la seccion critica -- chequeo de "hay activa", carga de config,
        # construccion de la fuente y reserva del slot -- vive bajo UNA sola
        # toma de `self._lock`. Ver docstring del modulo: esto es lo que hace
        # atomica la reserva y elimina la condicion de carrera del check-then-act.
        with self._lock:
            if self._active is not None:
                raise RunBusyError(self._active.run_id)

            cfg = self._load_config(request)
            source = self._build_source(request)  # valida rutas: puede levantar FileNotFoundError
            run_id = new_distribution_run_id()
            state = _RunState(run_id=run_id, out_dir=request.out_dir, source=source)

            distributor = Distributor(
                source=source,
                channel=MqttChannel(
                    mode=cfg.channel.mode,  # type: ignore[arg-type]
                    host=cfg.channel.host,
                    port=cfg.channel.port,
                    topic_prefix=cfg.channel.topic_prefix,
                    qos=cfg.channel.qos,
                ),
                policy=NotificationPolicy(
                    cooldown_ms=cfg.notification_policy.cooldown_ms,
                    key_fields=tuple(cfg.notification_policy.key),
                ),
                out_dir=Path(request.out_dir),
                max_attempts=cfg.retry.max_attempts,
                retry_wait_ms=cfg.retry.wait_ms,
            )

            def _target() -> None:
                try:
                    summary = distributor.run()
                except Exception as exc:  # noqa: BLE001 — el hilo no propaga: queda en el estado
                    state.status = "failed"
                    state.error = f"{type(exc).__name__}: {exc}"
                else:
                    state.summary = summary
                    # `termination_reason` vive anidado en `source_stats` (lo que
                    # expone `ZmqSource.stats`/`JsonlReplaySource.stats`), NUNCA en
                    # el nivel superior del summary de `Distributor.run()` -- leerlo
                    # ahi (bug real, fix round 1 de la tarea 5) hacia que "cancelled"
                    # fuera un estado inalcanzable: toda corrida terminada sin
                    # excepcion se reportaba "succeeded", incluso si se la habia
                    # cancelado. Acceso defensivo por si `source_stats` faltara.
                    reason = (
                        summary.get("source_stats", {}).get("termination_reason")
                        if isinstance(summary, dict)
                        else None
                    )
                    state.status = "cancelled" if reason == "requested_stop" else "succeeded"
                finally:
                    # El slot se libera ACA, bajo lock, en el momento real en que el
                    # hilo termina -- no basta con inspeccionar `state.status` desde
                    # `start_run` sin sincronizacion: ese flag lo escribe este hilo
                    # sin tomar `self._lock`, y una corrida que termina en el instante
                    # exacto en que otra la esta chequeando podria dejar pasar dos
                    # corridas activas a la vez (visto empiricamente con corridas
                    # triviales que terminan en microsegundos). Limpiar `self._active`
                    # aca, bajo la misma `self._lock` que usa `start_run` para el
                    # chequeo, hace que "hay una activa" sea una lectura sincronizada
                    # y siempre correcta.
                    with self._lock:
                        if self._active is state:
                            self._active = None

            thread = threading.Thread(target=_target, name=f"dist-{run_id}", daemon=True)
            state.thread = thread
            self._runs[run_id] = state
            self._active = state

        thread.start()
        return run_id

    def get(self, run_id: str) -> dict[str, Any]:
        state = self._runs.get(run_id)
        if state is None:
            raise UnknownRunError(run_id)
        return {
            "distribution_run_id": state.run_id,
            "status": state.status,
            "out_dir": state.out_dir,
            "summary": state.summary,
            "error": state.error,
        }

    def current(self) -> dict[str, Any]:
        with self._lock:
            active = self._active
        if active is None:
            raise UnknownRunError("no hay run activo")
        return self.get(active.run_id)

    def list_runs(self) -> list[dict[str, Any]]:
        # Instantanea de las claves bajo lock: `start_run` inserta en este
        # mismo dict bajo `self._lock`, y en CPython iterar un dict mientras
        # otro hilo lo muta levanta `RuntimeError: dictionary changed size
        # during iteration`. Iterar la lista (ya copiada) fuera del lock es
        # seguro: `get()` no borra entradas, solo `start_run` inserta.
        with self._lock:
            run_ids = list(self._runs)
        return [self.get(run_id) for run_id in run_ids]

    def cancel(self, run_id: str) -> None:
        state = self._runs.get(run_id)
        if state is None:
            raise UnknownRunError(run_id)
        request_stop = getattr(state.source, "request_stop", None)
        if request_stop is not None:
            request_stop()

    def forget(self, run_id: str) -> None:
        """Elimina la corrida del registro en memoria.

        NO borra nada en disco: `out_dir` es una ruta arbitraria elegida por
        el cliente en el request, no algo que este manager crea o posea (a
        diferencia de `runs_dir`, que aca nadie escribe). El que crea el
        directorio es quien lo limpia.

        Muta `self._runs` bajo `self._lock` -- igual que `list_runs()` y
        `shutdown()` snapshotean bajo ese mismo lock -- porque `start_run`
        inserta en este dict desde otro hilo; borrar sin lock reintroduciria
        el `RuntimeError: dictionary changed size during iteration` que ya
        se evito ahi.

        Defensa en profundidad: si `run_id` es el de la corrida ACTIVA, se
        levanta `RunBusyError` en vez de borrarla. El router de HTTP ya
        chequea `status == "running"` antes de llamar a `forget`, asi que
        esto no cambia ningun camino existente -- es para que un llamador
        directo (no HTTP) no pueda dejar `self._active` apuntando a un
        estado que ya no esta en `self._runs`, lo cual haria que `current()`
        levantara `UnknownRunError` (via `self.get()`) para una corrida que
        en los hechos sigue viva.
        """
        with self._lock:
            if run_id not in self._runs:
                raise UnknownRunError(run_id)
            if self._active is not None and self._active.run_id == run_id:
                raise RunBusyError(run_id)
            del self._runs[run_id]

    def join_active(self, timeout: float) -> None:
        with self._lock:
            active = self._active
        if active is not None and active.thread is not None:
            active.thread.join(timeout=timeout)

    def shutdown(self) -> None:
        # Mismo motivo que `list_runs`: instantanea de los VALORES bajo lock
        # antes de iterar (evita el `RuntimeError` de iterar un dict mutado
        # por `start_run` desde otro hilo). Las llamadas a `request_stop()`
        # quedan afuera del lock a proposito -- son cooperativas (solo setean
        # un `threading.Event`) pero no hay motivo para sostener el lock
        # mientras se las invoca, y si alguna futura fuente bloqueara, no
        # queremos bloquear a otros hilos que esperan `self._lock`.
        with self._lock:
            states = list(self._runs.values())
        for state in states:
            if state.status == "running":
                request_stop = getattr(state.source, "request_stop", None)
                if request_stop is not None:
                    request_stop()

    def _load_config(self, request: DistributionRunRequest) -> DistributionConfig:
        if request.config_path is not None:
            return DistributionConfig.load(Path(request.config_path))
        return DistributionConfig.model_validate(request.config)

    def _build_source(self, request: DistributionRunRequest) -> Any:
        if request.mode == "replay":
            alerts_path = Path(request.alerts_path or "")
            if not alerts_path.is_file():
                raise FileNotFoundError(f"no existe: {alerts_path}")
            return JsonlReplaySource(alerts_path)

        from eovrt_distribution.transport.zmq_source import ZmqSource

        backfill_path = Path(request.backfill) if request.backfill else None
        if backfill_path is not None and not backfill_path.is_file():
            raise FileNotFoundError(f"no existe: {backfill_path}")
        return ZmqSource(
            endpoint=request.endpoint or "",
            backfill_path=backfill_path,
            idle_timeout_ms=request.idle_timeout_ms,
            control_run_id=request.control_run_id,
        )
