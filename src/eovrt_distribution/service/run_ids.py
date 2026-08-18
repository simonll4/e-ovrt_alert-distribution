"""Validacion del `run_id` como segmento de path, y generacion de ids unicos.

Mismo criterio que el control-plane y el media-plane: alfanumericos, `_` y `-`.
Descarta `..`, `/` y demas ANTES de construir cualquier ruta bajo `runs_dir`.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from uuid import uuid4

from fastapi import HTTPException

RUN_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")


def is_valid_run_id(run_id: str) -> bool:
    return bool(RUN_ID_RE.match(run_id))


def require_valid_run_id(run_id: str) -> None:
    """Levanta 404 ("run desconocido") si el id no es un segmento seguro."""
    if not is_valid_run_id(run_id):
        raise HTTPException(status_code=404, detail=f"Run desconocido: {run_id}")


def new_distribution_run_id() -> str:
    """Postcondicion: el id devuelto siempre pasa `is_valid_run_id`."""
    stamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"dist_{stamp}_{uuid4().hex[:6]}"
