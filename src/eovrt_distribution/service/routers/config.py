"""Config efectiva del servicio de distribucion."""

from __future__ import annotations

from fastapi import APIRouter, Request

router = APIRouter(prefix="/api")


@router.get("/config")
def effective_config(request: Request) -> dict[str, str]:
    return {"runs_dir": str(request.app.state.settings.runs_dir)}
