"""Integración: requiere Mosquitto local (p.ej. docker run -p 1883:1883 eclipse-mosquitto
con listener anónimo). Corre solo con: pytest -m integration"""

import pytest

pytest.importorskip("paho.mqtt.client")

from eovrt_distribution.channels.mqtt import MqttChannel
from eovrt_distribution.contracts.notification import NotificationEnvelope

pytestmark = pytest.mark.integration


def test_live_publish_receives_puback(make_alert):
    channel = MqttChannel(mode="live", host="127.0.0.1", port=1883)
    env = NotificationEnvelope.from_alert(make_alert())
    result = channel.send(env)
    assert result.ok, result.error
    assert result.puback_wall_ms is not None
