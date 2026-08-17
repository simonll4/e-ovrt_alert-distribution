import json

import pytest
from pydantic import ValidationError

from eovrt_distribution.cli import main
from eovrt_distribution.config import DistributionConfig


def test_config_defaults():
    cfg = DistributionConfig.load(None)
    assert cfg.notification_policy.cooldown_ms == 30000.0
    assert cfg.channel.mode == "dry_run"
    assert cfg.retry.max_attempts == 3


def test_config_from_yaml(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text(
        "notification_policy:\n  cooldown_ms: 5000\nchannel:\n  mode: dry_run\n"
        "retry:\n  max_attempts: 2\n  wait_ms: 10\n"
    )
    cfg = DistributionConfig.load(p)
    assert cfg.notification_policy.cooldown_ms == 5000
    assert cfg.retry.max_attempts == 2


def test_cli_replay_end_to_end(tmp_path, make_alert, capsys):
    alerts = tmp_path / "alerts.jsonl"
    alerts.write_text(
        json.dumps(make_alert()) + "\n" + json.dumps(make_alert(source_id="cam-02")) + "\n"
    )
    out = tmp_path / "out"
    code = main(["replay", "--alerts", str(alerts), "--out-dir", str(out)])
    assert code == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["counts"] == {"delivered": 2}
    assert (out / "notifications.jsonl").exists()
    # re-ejecución: 100% skipped_duplicate (criterio de terminado del spec §7)
    code = main(["replay", "--alerts", str(alerts), "--out-dir", str(out)])
    assert code == 0
    summary2 = json.loads(capsys.readouterr().out)
    assert summary2["counts"] == {"skipped_duplicate": 2}
    size_after_second = len((out / "notifications.jsonl").read_text().splitlines())

    code = main(["replay", "--alerts", str(alerts), "--out-dir", str(out)])
    assert code == 0
    summary3 = json.loads(capsys.readouterr().out)
    assert summary3["counts"] == {"skipped_duplicate": 2}
    assert len((out / "notifications.jsonl").read_text().splitlines()) == size_after_second == 2
    assert (out / "notifications.2.jsonl").exists()
    assert (out / "notifications.1.jsonl").exists()


def test_cli_replay_missing_file_exits_2(tmp_path):
    assert main(["replay", "--alerts", str(tmp_path / "no.jsonl"), "--out-dir", str(tmp_path)]) == 2


@pytest.mark.parametrize(
    "yaml_text",
    [
        "channel:\n  mode: typo\n",
        "channel:\n  qos: 0\n",
        "retry:\n  max_attempts: 0\n",
        "notification_policy:\n  cooldown_ms: -1\n",
        "notification_policy:\n  cooldown_ms: .nan\n",
        "retry:\n  wait_ms: .inf\n",
        "notification_policy:\n  key: [missing_field]\n",
        "channel:\n  topic_prefix: eovrt/#\n",
        "unknown_section: true\n",
    ],
)
def test_config_rejects_values_outside_the_distribution_contract(tmp_path, yaml_text):
    path = tmp_path / "invalid.yaml"
    path.write_text(yaml_text, encoding="utf-8")
    with pytest.raises(ValidationError):
        DistributionConfig.load(path)


def test_cli_live_missing_declared_backfill_exits_2(tmp_path):
    assert (
        main(
            [
                "live",
                "--endpoint",
                "tcp://127.0.0.1:5558",
                "--backfill",
                str(tmp_path / "missing.jsonl"),
                "--out-dir",
                str(tmp_path / "out"),
            ]
        )
        == 2
    )


@pytest.mark.parametrize("value", ["0", "-1", "nan", "inf"])
def test_cli_live_rejects_non_positive_or_non_finite_idle_timeout(tmp_path, value, monkeypatch):
    from eovrt_distribution.sources import DirectSource

    monkeypatch.setattr(
        "eovrt_distribution.transport.zmq_source.ZmqSource",
        lambda **_: DirectSource([]),
    )
    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "live",
                "--endpoint",
                "tcp://127.0.0.1:5558",
                "--out-dir",
                str(tmp_path / "out"),
                "--idle-timeout-ms",
                value,
            ]
        )

    assert exc_info.value.code == 2
