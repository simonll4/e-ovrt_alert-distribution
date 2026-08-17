import json

from eovrt_distribution.contracts.delivery import DeliveryRecord
from eovrt_distribution.ledger import DeliveryLedger


def _rec(
    outcome: str,
    nid: str = "n1",
    attempt: int = 1,
    control_run_id: str = "cr",
) -> DeliveryRecord:
    return DeliveryRecord(
        control_run_id=control_run_id,
        notification_id=nid,
        alert_id="a1",
        channel="mqtt",
        mode="dry_run",
        attempt=attempt,
        outcome=outcome,
        attempted_at="2026-07-18T00:00:00Z",
    )


def test_delivered_marks_seen(tmp_path):
    ledger = DeliveryLedger(tmp_path / "notifications.jsonl")
    assert not ledger.seen("n1", "mqtt")
    ledger.append(_rec("delivered"))
    assert ledger.seen("n1", "mqtt")
    assert not ledger.seen("n1", "telegram")


def test_non_delivered_outcomes_do_not_mark_seen(tmp_path):
    ledger = DeliveryLedger(tmp_path / "notifications.jsonl")
    ledger.append(_rec("failed"))
    ledger.append(_rec("suppressed_cooldown"))
    assert not ledger.seen("n1", "mqtt")


def test_rehydrates_from_existing_file(tmp_path):
    path = tmp_path / "notifications.jsonl"
    first = DeliveryLedger(path)
    first.append(_rec("delivered", nid="n9"))
    second = DeliveryLedger(path)
    assert second.seen("n9", "mqtt")


def test_rehydrates_from_existing_file_preserving_latest_delivered(tmp_path):
    path = tmp_path / "notifications.jsonl"
    path.write_text(
        "\n".join(
            [
                json.dumps(_rec("failed", nid="n1").model_dump(mode="json")),
                json.dumps(_rec("delivered", nid="n1", attempt=1).model_dump(mode="json")),
                json.dumps(_rec("failed", nid="n2").model_dump(mode="json")),
                json.dumps(_rec("delivered", nid="n1", attempt=2).model_dump(mode="json")),
                json.dumps(_rec("delivered", nid="n2").model_dump(mode="json")),
            ]
        )
        + "\n"
    )
    ledger = DeliveryLedger(path)
    assert ledger.seen("n1", "mqtt")
    assert ledger.seen("n2", "mqtt")
    archived = tmp_path / "notifications.1.jsonl"
    lines = [json.loads(line) for line in archived.read_text().splitlines() if line.strip()]
    assert len(lines) == 5
    assert {row["notification_id"] for row in lines} == {"n1", "n2"}
    assert len([row for row in lines if row["outcome"] == "delivered"]) == 3
    assert not path.exists()


def test_reopen_archives_previous_file_intact_and_rehydrates(tmp_path):
    path = tmp_path / "notifications.jsonl"
    path.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "notification_id": "n1",
                        "channel": "mqtt",
                        "outcome": "delivered",
                        "attempt": 1,
                        "control_run_id": "cr",
                        "alert_id": "a1",
                        "mode": "dry_run",
                        "event_type": "control.alert.v1",
                    }
                ),
                json.dumps(
                    {
                        "notification_id": "n2",
                        "channel": "mqtt",
                        "outcome": "suppressed_cooldown",
                        "attempt": 1,
                        "control_run_id": "cr",
                        "alert_id": "a2",
                        "mode": "dry_run",
                        "event_type": "control.alert.v1",
                    }
                ),
                json.dumps(
                    {
                        "notification_id": "n3",
                        "channel": "mqtt",
                        "outcome": "dead_letter",
                        "attempt": 1,
                        "control_run_id": "cr",
                        "alert_id": "a3",
                        "mode": "dry_run",
                        "event_type": "control.alert.v1",
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    ledger = DeliveryLedger(path)

    assert ledger.seen("n1", "mqtt")
    assert not ledger.seen("n2", "mqtt")
    assert not ledger.seen("n3", "mqtt")
    archived = tmp_path / "notifications.1.jsonl"
    assert archived.exists()
    assert len(archived.read_text(encoding="utf-8").splitlines()) == 3
    assert not path.exists()


def test_reopen_twice_increments_generation(tmp_path):
    path = tmp_path / "notifications.jsonl"
    path.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "notification_id": "a",
                        "channel": "mqtt",
                        "outcome": "delivered",
                        "attempt": 1,
                        "control_run_id": "cr",
                        "alert_id": "a1",
                        "mode": "dry_run",
                        "event_type": "control.alert.v1",
                    }
                )
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    DeliveryLedger(path)
    path.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "notification_id": "b",
                        "channel": "mqtt",
                        "outcome": "failed",
                        "attempt": 1,
                        "control_run_id": "cr",
                        "alert_id": "b1",
                        "mode": "dry_run",
                        "event_type": "control.alert.v1",
                    }
                )
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    DeliveryLedger(path)

    assert (tmp_path / "notifications.1.jsonl").exists()
    assert (tmp_path / "notifications.2.jsonl").exists()


def test_delivered_survives_across_multiple_generations(tmp_path):
    """La deduplicación es acumulativa sobre TODAS las generaciones, no solo la última."""
    path = tmp_path / "notifications.jsonl"

    first = DeliveryLedger(path)
    first.append(_rec("delivered", nid="n1"))

    second = DeliveryLedger(path)  # archiva la generación de n1 -> .1
    assert second.seen("n1", "mqtt")
    second.append(_rec("failed", nid="n2"))

    third = DeliveryLedger(path)  # archiva la generación de n2 -> .2

    assert (tmp_path / "notifications.1.jsonl").exists()
    assert (tmp_path / "notifications.2.jsonl").exists()
    # el delivered vive en la generación .1: rehidratarlo exige leer todas, no la última
    assert third.seen("n1", "mqtt")
    assert not third.seen("n2", "mqtt")


def test_existing_file_without_deliveries_is_archived_intact(tmp_path):
    path = tmp_path / "notifications.jsonl"
    path.write_text(
        json.dumps(_rec("failed", nid="x").model_dump(mode="json")) + "\n",
        encoding="utf-8",
    )
    ledger = DeliveryLedger(path)

    assert not ledger.seen("x", "mqtt")
    assert (tmp_path / "notifications.1.jsonl").read_text(encoding="utf-8").count("\n") == 1


def test_appends_one_json_line_per_record(tmp_path):
    path = tmp_path / "notifications.jsonl"
    ledger = DeliveryLedger(path)
    ledger.append(_rec("delivered"))
    ledger.append(_rec("skipped_duplicate"))
    lines = path.read_text().strip().splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0])["outcome"] == "delivered"
