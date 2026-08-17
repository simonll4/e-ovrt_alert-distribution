"""Integración: requiere un broker MQTT local en 127.0.0.1:1883 (ver README, sección
"Test de integración con broker"). Corre solo con: pytest -m integration"""

import pytest

from eovrt_distribution.channels.base import ChannelError
from eovrt_distribution.channels.mqtt import MqttChannel
from eovrt_distribution.contracts.notification import NotificationEnvelope

pytest.importorskip("paho.mqtt.client")

pytestmark = pytest.mark.integration

# `MqttChannel._send_live` captura (OSError, RuntimeError, ValueError) y devuelve
# SendResult(ok=False, error=...) en vez de propagar: la ausencia de broker llega acá
# como texto en `result.error`, no como excepción. Por eso se inspecciona el resultado
# ANTES de asertar, además de conservar el except (que cubre ChannelError y lo que sí
# se propague).
_BROKER_DOWN_MARKERS = (
    "TimeoutError",
    "timed out",
    "ConnectionRefused",
    "Connection refused",
    "ConnectionError",
    "No route to host",
    "Operation not permitted",
    "PermissionError",
    "OSError",
)


def _broker_unavailable(error: str | None) -> bool:
    if not error:
        return False
    return any(marker in error for marker in _BROKER_DOWN_MARKERS)


def test_live_publish_receives_puback(make_alert):
    channel = MqttChannel(mode="live", host="127.0.0.1", port=1883)
    env = NotificationEnvelope.from_alert(make_alert())
    try:
        result = channel.send(env)
    except (ChannelError, OSError, TimeoutError, ConnectionRefusedError, ConnectionResetError) as exc:
        if isinstance(exc, (OSError, TimeoutError)):
            pytest.skip(f"broker MQTT no disponible en 127.0.0.1:1883: {exc}")
        if "broker" in str(exc).lower():
            pytest.skip(f"broker MQTT no disponible en 127.0.0.1:1883: {exc}")
        raise
    if result.ok is False and _broker_unavailable(result.error):
        pytest.skip(f"broker MQTT no disponible en 127.0.0.1:1883: {result.error}")
    assert result.ok, result.error
    assert result.puback_wall_ms is not None
