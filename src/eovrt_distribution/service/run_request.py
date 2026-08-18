"""Contrato del request de corrida de distribucion (spec 45 §9.2, ADR-019)."""

from __future__ import annotations

import math
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, model_validator


class DistributionRunRequest(BaseModel):
    # extra="forbid": un campo desconocido en el body -> 422, no se ignora en silencio.
    model_config = ConfigDict(extra="forbid")

    mode: Literal["replay", "live"]
    out_dir: str
    # ADR-009: la config llega por referencia (path a un YAML) o por payload completo.
    config_path: str | None = None
    config: dict[str, Any] | None = None
    # replay
    alerts_path: str | None = None
    # live
    endpoint: str | None = None
    control_run_id: str | None = None
    backfill: str | None = None
    idle_timeout_ms: float | None = None

    @model_validator(mode="after")
    def exactly_one_config_source(self) -> DistributionRunRequest:
        if (self.config_path is None) == (self.config is None):
            raise ValueError(
                "Exactamente uno de `config_path` (por referencia) o `config` (por payload)"
            )
        return self

    @model_validator(mode="after")
    def campos_segun_modo(self) -> DistributionRunRequest:
        if self.mode == "replay" and not self.alerts_path:
            raise ValueError("`alerts_path` es obligatorio en modo replay")
        if self.mode == "live" and not self.endpoint:
            raise ValueError("`endpoint` es obligatorio en modo live")
        return self

    @model_validator(mode="after")
    def idle_timeout_positivo(self) -> DistributionRunRequest:
        if self.idle_timeout_ms is not None and (
            not math.isfinite(self.idle_timeout_ms) or self.idle_timeout_ms <= 0
        ):
            raise ValueError("`idle_timeout_ms` debe ser un numero finito mayor que cero")
        return self
