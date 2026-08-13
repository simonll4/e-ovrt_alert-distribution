"""Contrato control.notification.v1 (doc 06 §6.1 + source_id + experiment_id)."""

from __future__ import annotations

import hashlib
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


def notification_id_for(alert_id: str) -> str:
    return hashlib.sha1(alert_id.encode("utf-8")).hexdigest()[:16]


class _AlertInput(BaseModel):
    """Frontera compatible y aditiva con ``control.alert.v1``."""

    model_config = ConfigDict(extra="allow")

    schema_version: Literal["control.alert.v1"]
    event_type: Literal["alert_event"]
    control_run_id: str
    media_run_id: str
    alert_id: str
    pattern_id: str
    condition_id: str
    source_id: str
    subject_key: str
    severity: str
    state: str = "open"
    timestamp_ms: float | None = Field(default=None, allow_inf_nan=False)
    evidence: dict | None = None
    experiment_id: str | None = None


class NotificationEnvelope(BaseModel):
    schema_version: str = "control.notification.v1"
    event_type: str = "notification_envelope"
    notification_id: str
    control_run_id: str
    media_run_id: str
    alert_id: str
    pattern_id: str
    condition_id: str
    source_id: str
    subject_key: str
    severity: str
    episode_state: str
    media_timestamp_ms: float | None = Field(default=None, allow_inf_nan=False)
    confirmed_wall_ms: float | None = Field(
        default=None, allow_inf_nan=False
    )  # ts_publish_ms del bus; None en replay
    evidence_ref: dict | None = None
    summary_text: str
    experiment_id: str | None = None

    @classmethod
    def from_alert(cls, alert: dict, ts_publish_ms: float | None = None) -> NotificationEnvelope:
        parsed = _AlertInput.model_validate(alert)
        return cls(
            notification_id=notification_id_for(parsed.alert_id),
            control_run_id=parsed.control_run_id,
            media_run_id=parsed.media_run_id,
            alert_id=parsed.alert_id,
            pattern_id=parsed.pattern_id,
            condition_id=parsed.condition_id,
            source_id=parsed.source_id,
            subject_key=parsed.subject_key,
            severity=parsed.severity,
            episode_state=parsed.state,
            media_timestamp_ms=parsed.timestamp_ms,
            confirmed_wall_ms=ts_publish_ms,
            evidence_ref=parsed.evidence,
            summary_text=(
                f"[{parsed.severity}] {parsed.condition_id} confirmada en "
                f"{parsed.source_id} (sujeto {parsed.subject_key})"
            ),
            experiment_id=parsed.experiment_id,
        )
