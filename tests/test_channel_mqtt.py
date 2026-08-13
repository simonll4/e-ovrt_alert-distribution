from eovrt_distribution.channels.mqtt import MqttChannel
from eovrt_distribution.contracts.notification import NotificationEnvelope


def test_dry_run_send_ok_without_io(make_alert):
    channel = MqttChannel(mode="dry_run")
    env = NotificationEnvelope.from_alert(make_alert(severity="high"))
    result = channel.send(env)
    assert result.ok
    assert result.error is None
    assert channel.name == "mqtt"


def test_topic_includes_severity(make_alert):
    channel = MqttChannel(mode="dry_run")
    env = NotificationEnvelope.from_alert(make_alert(severity="medium"))
    assert channel.topic_for(env) == "eovrt/alerts/medium"


def test_dry_run_records_published_payloads(make_alert):
    channel = MqttChannel(mode="dry_run")
    env = NotificationEnvelope.from_alert(make_alert())
    channel.send(env)
    assert len(channel.dry_run_published) == 1
    topic, payload = channel.dry_run_published[0]
    assert topic == "eovrt/alerts/high"
    assert env.notification_id in payload


def test_live_without_paho_raises_channel_error(monkeypatch, make_alert):
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name.startswith("paho"):
            raise ImportError("no paho")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    channel = MqttChannel(mode="live")
    env = NotificationEnvelope.from_alert(make_alert())
    import pytest as _pytest

    from eovrt_distribution.channels.base import ChannelError

    with _pytest.raises(ChannelError, match=r"\[mqtt\]"):
        channel.send(env)


def test_qos1_duplicate_delivery_deduped_by_ledger(tmp_path, make_alert):
    """QoS 1 puede duplicar: el mismo alert re-entregado debe terminar en
    skipped_duplicate por ledger (doc 07 D5.2) — criterio de terminado spec §7."""
    from eovrt_distribution.distributor import Distributor
    from eovrt_distribution.policy import NotificationPolicy
    from eovrt_distribution.sources import DirectSource

    alert = make_alert()
    dist = Distributor(
        source=DirectSource([alert, dict(alert)]),  # duplicado exacto
        channel=MqttChannel(mode="dry_run"),
        policy=NotificationPolicy(cooldown_ms=0.0),
        out_dir=tmp_path,
        retry_wait_ms=0.0,
    )
    summary = dist.run()
    assert summary["counts"] == {"delivered": 1, "skipped_duplicate": 1}
