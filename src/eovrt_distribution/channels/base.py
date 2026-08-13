from __future__ import annotations

from dataclasses import dataclass


class ChannelError(Exception):
    pass


@dataclass
class SendResult:
    ok: bool
    error: str | None = None
    puback_wall_ms: float | None = None
