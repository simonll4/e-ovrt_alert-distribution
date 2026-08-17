from eovrt_distribution.channels.mqtt import MqttChannel
from eovrt_distribution.contracts.notification import NotificationEnvelope


def test_failed_publish_resets_client_so_next_attempt_reconnects(monkeypatch, make_alert):
    class _Info:
        def is_published(self) -> bool:
            return False

        def wait_for_publish(self, timeout: float) -> None:
            return None

    class _FakeClient:
        def __init__(self, *args, **kwargs) -> None:
            self.connect_calls = 0
            self.loop_start_calls = 0
            self.loop_stop_calls = 0
            self.disconnect_calls = 0
            self.published = False

        def username_pw_set(self, *args, **kwargs) -> None:
            pass

        def connect(self, *args, **kwargs) -> None:
            self.connect_calls += 1

        def loop_start(self) -> None:
            self.loop_start_calls += 1

        def publish(self, *args, **kwargs):
            self.published = True
            return _Info()

        def loop_stop(self) -> None:
            self.loop_stop_calls += 1

        def disconnect(self) -> None:
            self.disconnect_calls += 1

    created: list[_FakeClient] = []

    def fake_client(*args, **kwargs):
        client = _FakeClient(*args, **kwargs)
        created.append(client)
        return client

    class _FakeMqtt:
        class CallbackAPIVersion:
            VERSION2 = object()

        Client = staticmethod(fake_client)

    monkeypatch.setattr("eovrt_distribution.channels.mqtt.mqtt", _FakeMqtt)

    channel = MqttChannel(mode="live")
    env = NotificationEnvelope.from_alert(make_alert())
    result = channel.send(env)
    assert result.ok is False

    assert created
    first = created[0]
    assert first.connect_calls == 1
    assert first.loop_stop_calls == 1
    assert first.disconnect_calls == 1
    assert len(created) == 1

    # Segundo intento: debería crear un cliente nuevo porque se reseteo al fallo.
    result2 = channel.send(env)
    assert result2.ok is False
    assert len(created) == 2
    second = created[1]
    assert second.connect_calls == 1


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
    monkeypatch.setattr("eovrt_distribution.channels.mqtt.mqtt", None)
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
