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
    lines = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    assert len(lines) == 2
    assert all(row["outcome"] == "delivered" for row in lines)
    assert {row["notification_id"]: row["attempt"] for row in lines} == {"n1": 2, "n2": 1}


def test_existing_file_without_deliveries_is_compacted_to_empty(tmp_path):
    path = tmp_path / "notifications.jsonl"
    path.write_text(
        "\n".join(
            [
                json.dumps(_rec("failed", nid="n1").model_dump(mode="json")),
                json.dumps(_rec("dead_letter", nid="n1").model_dump(mode="json")),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    ledger = DeliveryLedger(path)

    assert not ledger.seen("n1", "mqtt")
    assert path.read_text(encoding="utf-8") == ""


def test_appends_one_json_line_per_record(tmp_path):
    path = tmp_path / "notifications.jsonl"
    ledger = DeliveryLedger(path)
    ledger.append(_rec("delivered"))
    ledger.append(_rec("skipped_duplicate"))
    lines = path.read_text().strip().splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0])["outcome"] == "delivered"
