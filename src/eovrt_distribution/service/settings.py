"""Configuracion operacional del servicio (ADR-009: vive con el servicio)."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ServiceSettings:
    runs_dir: Path

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> ServiceSettings:
        env = os.environ if env is None else env
        return cls(runs_dir=Path(env.get("EOVRT_DISTRIBUTION_RUNS_DIR", "runs")).resolve())
