"""Contrato control.delivery.v1 (doc 06 §6.2 + suppressed_cooldown + experiment_id)."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel

Outcome = Literal["delivered", "failed", "skipped_duplicate", "dead_letter", "suppressed_cooldown"]


class DeliveryRecord(BaseModel):
    schema_version: str = "control.delivery.v1"
    event_type: str = "delivery_record"
    control_run_id: str
    notification_id: str
    alert_id: str
    channel: str
    mode: Literal["dry_run", "live"]
    attempt: int
    outcome: Outcome
    error: str | None = None
    talert_notification_ms: float | None = None
    latency_mode: Literal["live", "wall_clock_dbe"] | None = None
    attempted_at: str
    delivered_at: str | None = None
    experiment_id: str | None = None
