import json
import time

from eovrt_distribution.channels.base import SendResult
from eovrt_distribution.channels.mqtt import MqttChannel
from eovrt_distribution.distributor import Distributor
from eovrt_distribution.policy import NotificationPolicy
from eovrt_distribution.sources import DirectSource, SourcedAlert


class FailingChannel(MqttChannel):
    def __init__(self, fail_times: int) -> None:
        super().__init__(mode="dry_run")
        self._remaining = fail_times

    def send(self, env) -> SendResult:
        if self._remaining > 0:
            self._remaining -= 1
            return SendResult(ok=False, error="boom")
        return super().send(env)


class ClosingChannel(MqttChannel):
    def __init__(self) -> None:
        super().__init__(mode="dry_run")
        self.closed = False

    def close(self) -> None:
        self.closed = True


class PubackChannel(MqttChannel):
    def __init__(self) -> None:
        super().__init__(mode="dry_run")
        self.puback_wall_ms: float | None = None

    def send(self, env) -> SendResult:
        self.puback_wall_ms = time.time() * 1000.0 + 500.0
        return SendResult(ok=True, puback_wall_ms=self.puback_wall_ms)


class RecordingPolicy(NotificationPolicy):
    def __init__(self) -> None:
        super().__init__(cooldown_ms=30000.0)
        self.marked_wall_ms: float | None = None

    def mark_notified(self, env, now_wall_ms: float) -> None:
        self.marked_wall_ms = now_wall_ms
        super().mark_notified(env, now_wall_ms)


def _run(tmp_path, alerts, channel=None, cooldown_ms=30000.0, max_attempts=3):
    dist = Distributor(
        source=DirectSource(alerts),
        channel=channel or MqttChannel(mode="dry_run"),
        policy=NotificationPolicy(cooldown_ms=cooldown_ms),
        out_dir=tmp_path,
        max_attempts=max_attempts,
        retry_wait_ms=0.0,
    )
    return dist.run()


def _outcomes(tmp_path):
    lines = (tmp_path / "notifications.jsonl").read_text().strip().splitlines()
    return [json.loads(line)["outcome"] for line in lines]


def _count_rows(path):
    return len([line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()])


def test_happy_path_delivers_and_summarizes(tmp_path, make_alert):
    summary = _run(tmp_path, [make_alert(), make_alert(source_id="cam-02")])
    assert summary["counts"] == {"delivered": 2}
    assert summary["schema_version"] == "control.distribution_summary.v1"
    assert (tmp_path / "distribution_summary.json").exists()
    assert _outcomes(tmp_path) == ["delivered", "delivered"]


def test_dead_letter_is_reinitialized_each_run(tmp_path, make_alert):
    dead = tmp_path / "dead_letter.jsonl"
    dead.write_text('{"outcome": "dead_letter"}\n')
    _run(tmp_path, [make_alert()])
    assert (tmp_path / "dead_letter.1.jsonl").exists()
    assert not dead.exists()


def test_rerun_over_same_out_dir_archives_dead_letter(tmp_path, make_alert):
    out_dir = tmp_path
    dead = out_dir / "dead_letter.jsonl"
    dead.write_text('{"outcome": "dead_letter"}\n', encoding="utf-8")

    _run(
        out_dir,
        [make_alert()],
        channel=FailingChannel(fail_times=99),
        max_attempts=1,
        cooldown_ms=0.0,
    )
    assert dead.exists()
    assert (out_dir / "dead_letter.1.jsonl").read_text(encoding="utf-8").count("\n") == 1
    first_run_rows = _count_rows(out_dir / "notifications.jsonl")
    assert first_run_rows == 2  # failed + dead_letter de la 1ª corrida

    _run(
        out_dir,
        [make_alert()],
        channel=FailingChannel(fail_times=99),
        max_attempts=1,
        cooldown_ms=0.0,
    )
    assert (out_dir / "dead_letter.2.jsonl").exists()
    assert dead.exists()

    # las filas de la 1ª corrida NO desaparecen: quedan archivadas junto a la vigente
    archived = out_dir / "notifications.1.jsonl"
    assert archived.exists()
    assert _count_rows(archived) == first_run_rows
    second_run_rows = _count_rows(out_dir / "notifications.jsonl")
    assert second_run_rows == 2
    total_rows = sum(_count_rows(path) for path in out_dir.glob("notifications*.jsonl"))
    assert total_rows == first_run_rows + second_run_rows == 4


def test_burst_suppressed_by_cooldown_and_recorded(tmp_path, make_alert):
    alerts = [
        make_alert(timestamp_ms=1000.0),
        make_alert(timestamp_ms=2000.0, subject_key="otro"),
        make_alert(timestamp_ms=3000.0, subject_key="tercero"),
    ]
    summary = _run(tmp_path, alerts)
    assert summary["counts"] == {"delivered": 1, "suppressed_cooldown": 2}
    assert _outcomes(tmp_path) == [
        "delivered",
        "suppressed_cooldown",
        "suppressed_cooldown",
    ]


def test_rerun_is_fully_skipped_duplicate(tmp_path, make_alert):
    alerts = [make_alert(), make_alert(source_id="cam-02")]
    _run(tmp_path, alerts)
    summary = _run(tmp_path, alerts, cooldown_ms=0.0)
    assert summary["counts"] == {"skipped_duplicate": 2}


def test_exact_duplicate_is_classified_before_cooldown(tmp_path, make_alert):
    alert = make_alert(timestamp_ms=1000.0)
    summary = _run(tmp_path, [alert, dict(alert)])
    assert summary["counts"] == {"delivered": 1, "skipped_duplicate": 1}


def test_failed_delivery_does_not_consume_cooldown(tmp_path, make_alert):
    alerts = [
        make_alert(timestamp_ms=1000.0),
        make_alert(timestamp_ms=2000.0, subject_key="otro"),
    ]
    summary = _run(
        tmp_path,
        alerts,
        channel=FailingChannel(fail_times=99),
        max_attempts=1,
    )
    assert summary["counts"] == {"failed": 2, "dead_letter": 2}


def test_wall_clock_cooldown_starts_at_puback_not_before_delivery(tmp_path, make_alert):
    channel = PubackChannel()
    policy = RecordingPolicy()
    dist = Distributor(
        source=DirectSource([make_alert(timestamp_ms=None)]),
        channel=channel,
        policy=policy,
        out_dir=tmp_path,
        retry_wait_ms=0.0,
    )

    dist.run()

    assert policy.marked_wall_ms == channel.puback_wall_ms


def test_retry_then_delivered(tmp_path, make_alert):
    summary = _run(tmp_path, [make_alert()], channel=FailingChannel(fail_times=2))
    assert summary["counts"] == {"failed": 2, "delivered": 1}
    assert _outcomes(tmp_path) == ["failed", "failed", "delivered"]


def test_exhausted_goes_to_dead_letter(tmp_path, make_alert):
    summary = _run(tmp_path, [make_alert()], channel=FailingChannel(fail_times=99), max_attempts=3)
    assert summary["counts"] == {"failed": 3, "dead_letter": 1}
    dead = (tmp_path / "dead_letter.jsonl").read_text().strip().splitlines()
    assert len(dead) == 1
    assert json.loads(dead[0])["outcome"] == "dead_letter"


def test_latency_mode_is_wall_clock_dbe_without_bus_timestamp(tmp_path, make_alert):
    _run(tmp_path, [make_alert()])
    row = json.loads((tmp_path / "notifications.jsonl").read_text().splitlines()[0])
    assert row["latency_mode"] == "wall_clock_dbe"
    assert row["talert_notification_ms"] is not None


def test_alert_missing_required_field_is_skipped_not_fatal(tmp_path, make_alert):
    good_before = make_alert()
    incomplete = make_alert(source_id="cam-02")
    del incomplete["severity"]
    good_after = make_alert(source_id="cam-03")
    summary = _run(tmp_path, [good_before, incomplete, good_after])
    assert summary["counts"] == {"delivered": 2}
    assert summary["skipped_invalid_alerts"] == 1
    assert (tmp_path / "distribution_summary.json").exists()


def test_non_object_and_wrong_contract_alerts_are_skipped_not_fatal(tmp_path, make_alert):
    wrong_contract = make_alert(source_id="cam-02")
    wrong_contract["schema_version"] = "media.detection.v1"
    summary = _run(tmp_path, [make_alert(), None, ["not", "an", "alert"], wrong_contract])
    assert summary["counts"] == {"delivered": 1}
    assert summary["skipped_invalid_alerts"] == 3


def test_alert_with_non_finite_timestamp_is_skipped_not_fatal(tmp_path, make_alert):
    summary = _run(tmp_path, [make_alert(timestamp_ms=float("nan")), make_alert()])

    assert summary["counts"] == {"delivered": 1}
    assert summary["skipped_invalid_alerts"] == 1


def test_channel_is_closed_after_run(tmp_path, make_alert):
    channel = ClosingChannel()
    _run(tmp_path, [make_alert()], channel=channel)
    assert channel.closed is True


def test_summary_reports_latency_aggregates_split_by_mode(tmp_path, make_alert):
    alerts = [
        SourcedAlert(alert=make_alert(), ts_publish_ms=1_000_000.0),  # live
        SourcedAlert(alert=make_alert(source_id="cam-02")),  # wall_clock_dbe
    ]
    dist = Distributor(
        source=DirectSource(alerts),
        channel=MqttChannel(mode="dry_run"),
        policy=NotificationPolicy(cooldown_ms=30000.0),
        out_dir=tmp_path,
        max_attempts=3,
        retry_wait_ms=0.0,
    )
    summary = dist.run()
    latency = summary["talert_notification_ms"]
    assert set(latency.keys()) == {"live", "wall_clock_dbe"}
    assert latency["live"]["count"] == 1
    assert latency["wall_clock_dbe"]["count"] == 1


def test_p95_uses_nearest_rank(tmp_path, make_alert):
    alerts = [make_alert(source_id=f"cam-{idx}") for idx in range(30)]
    channel = MqttChannel(mode="dry_run")
    dist = Distributor(
        source=DirectSource(alerts),
        channel=channel,
        policy=NotificationPolicy(cooldown_ms=0.0),
        out_dir=tmp_path,
        retry_wait_ms=0.0,
    )
    summary = dist.run()
    values = sorted(
        json.loads(line)["talert_notification_ms"]
        for line in (tmp_path / "notifications.jsonl").read_text().splitlines()
    )
    assert summary["talert_notification_ms"]["wall_clock_dbe"]["p95"] == values[28]


def test_dbe_latency_includes_retry_wait(tmp_path, make_alert):
    dist = Distributor(
        source=DirectSource([make_alert()]),
        channel=FailingChannel(fail_times=1),
        policy=NotificationPolicy(cooldown_ms=0.0),
        out_dir=tmp_path,
        max_attempts=2,
        retry_wait_ms=20.0,
    )
    started = time.monotonic()
    summary = dist.run()
    elapsed_ms = (time.monotonic() - started) * 1000.0
    measured = summary["talert_notification_ms"]["wall_clock_dbe"]["p95"]
    assert measured >= 15.0
    assert measured <= elapsed_ms
