"""API de corridas del servicio de distribucion (spec 45 §9.3)."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, Response

from eovrt_distribution.service.run_ids import require_valid_run_id
from eovrt_distribution.service.run_manager import RunBusyError, RunManager, UnknownRunError
from eovrt_distribution.service.run_request import DistributionRunRequest

router = APIRouter(prefix="/api")


def _manager(request: Request) -> RunManager:
    return request.app.state.manager


@router.post("/runs", status_code=201)
def create_run(body: DistributionRunRequest, request: Request):
    try:
        run_id = _manager(request).start_run(body)
    except RunBusyError as exc:
        return JSONResponse(
            status_code=409,
            content={"detail": str(exc), "active_run_id": exc.active_run_id},
        )
    except (ValueError, FileNotFoundError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {"distribution_run_id": run_id}


@router.get("/runs")
def list_runs(request: Request):
    return _manager(request).list_runs()


# ANTES de /runs/{run_id}: si no, `current` se matchea como un run_id.
@router.get("/runs/current")
def get_current_run(request: Request):
    try:
        return _manager(request).current()
    except UnknownRunError as exc:
        raise HTTPException(status_code=404, detail="No hay run activo") from exc


@router.get("/runs/{run_id}")
def get_run(run_id: str, request: Request):
    require_valid_run_id(run_id)
    try:
        return _manager(request).get(run_id)
    except UnknownRunError as exc:
        raise HTTPException(status_code=404, detail=f"Run desconocido: {run_id}") from exc


@router.post("/runs/{run_id}/cancel", status_code=202)
def cancel_run(run_id: str, request: Request):
    """Parada COOPERATIVA (`request_stop`). Desvio deliberado del espejo: el
    control-plane no expone cancelacion y su corrida live no se puede cancelar."""
    require_valid_run_id(run_id)
    try:
        _manager(request).cancel(run_id)
    except UnknownRunError as exc:
        raise HTTPException(status_code=404, detail=f"Run desconocido: {run_id}") from exc
    return {"status": "cancelling", "distribution_run_id": run_id}


@router.delete("/runs/{run_id}", status_code=204)
def delete_run(run_id: str, request: Request):
    """Olvida la corrida del REGISTRO en memoria. NO borra `out_dir` ni nada
    dentro de el: es una ruta arbitraria que elige el cliente en el request,
    no algo que este servicio posea. Borrarla desde aca seria borrado
    arbitrario de directorios a pedido del cliente. El que crea `out_dir` es
    quien lo limpia -- no "completar" esto con un rmtree.

    `RunManager.forget()` es la UNICA autoridad, ya que hace su chequeo bajo
    `self._lock` (ver su docstring). Antes esta funcion tambien llamaba a
    `manager.get(run_id)` y miraba `info["status"] == "running"` como guard
    previo -- una lectura SIN lock que, en la ventana de pocos bytecodes
    entre que `_target` escribe `state.status` (sin lock) y limpia
    `self._active` (con lock, en su `finally`), podia leer un `status` ya
    terminal mientras `self._active` TODAVIA apuntaba a ese run. Si esto
    pasaba, el guard previo daba OK, y `forget()` -- que si mira bajo lock
    -- levantaba `RunBusyError` sin que nadie la atajara: 500 sobre una
    corrida que el cliente ya veia terminada (ronda 3 de fixes). Sacar el
    guard duplicado no angosta la ventana: la elimina, porque ya no hay
    ninguna decision basada en una lectura sin lock -- la unica decision es
    la de `forget()`, atomica con la propia mutacion del registro."""
    require_valid_run_id(run_id)
    manager = _manager(request)
    try:
        manager.forget(run_id)
    except UnknownRunError as exc:
        raise HTTPException(status_code=404, detail=f"Run desconocido: {run_id}") from exc
    except RunBusyError as exc:
        return JSONResponse(
            status_code=409,
            content={"detail": str(exc), "active_run_id": exc.active_run_id},
        )
    return Response(status_code=204)
