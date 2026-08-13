import pytest
from pydantic import ValidationError

from eovrt_distribution.contracts.delivery import DeliveryRecord
from eovrt_distribution.contracts.notification import (
    NotificationEnvelope,
    notification_id_for,
)


def test_notification_id_is_deterministic_and_short():
    a = notification_id_for("00000000-0000-5000-8000-000000000001")
    b = notification_id_for("00000000-0000-5000-8000-000000000001")
    c = notification_id_for("00000000-0000-5000-8000-000000000002")
    assert a == b
    assert a != c
    assert len(a) == 16


def test_envelope_from_alert_maps_fields(make_alert):
    alert = make_alert(severity="medium")
    env = NotificationEnvelope.from_alert(alert, ts_publish_ms=1750000000000.0)
    assert env.schema_version == "control.notification.v1"
    assert env.event_type == "notification_envelope"
    assert env.alert_id == alert["alert_id"]
    assert env.notification_id == notification_id_for(alert["alert_id"])
    assert env.condition_id == "CR-01"
    assert env.source_id == "cam-01"
    assert env.severity == "medium"
    assert env.media_timestamp_ms == alert["timestamp_ms"]
    assert env.confirmed_wall_ms == 1750000000000.0
    assert env.experiment_id == "exp-test"
    assert alert["condition_id"] in env.summary_text
    assert alert["source_id"] in env.summary_text


def test_envelope_from_alert_replay_has_no_wall_clock(make_alert):
    env = NotificationEnvelope.from_alert(make_alert())
    assert env.confirmed_wall_ms is None


def test_delivery_record_rejects_unknown_outcome(make_alert):
    with pytest.raises(ValidationError):
        DeliveryRecord(
            control_run_id="cr",
            notification_id="n",
            alert_id="a",
            channel="mqtt",
            mode="dry_run",
            attempt=1,
            outcome="exploded",
            attempted_at="2026-07-18T00:00:00Z",
        )


def test_delivery_record_accepts_suppressed_cooldown():
    rec = DeliveryRecord(
        control_run_id="cr",
        notification_id="n",
        alert_id="a",
        channel="mqtt",
        mode="dry_run",
        attempt=0,
        outcome="suppressed_cooldown",
        attempted_at="2026-07-18T00:00:00Z",
    )
    assert rec.schema_version == "control.delivery.v1"
    assert rec.latency_mode is None
