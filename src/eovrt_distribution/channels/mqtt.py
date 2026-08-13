"""Único canal de esta iteración (ADR-005). dry_run: sin I/O, cubre CI."""

from __future__ import annotations

import json
import logging
from typing import Literal

from eovrt_distribution.channels.base import ChannelError, SendResult
from eovrt_distribution.contracts.notification import NotificationEnvelope

logger = logging.getLogger(__name__)


class MqttChannel:
    name = "mqtt"

    def __init__(
        self,
        mode: Literal["dry_run", "live"] = "dry_run",
        host: str = "127.0.0.1",
        port: int = 1883,
        topic_prefix: str = "eovrt/alerts",
        qos: int = 1,
    ) -> None:
        self.mode = mode
        self.host = host
        self.port = port
        self.topic_prefix = topic_prefix
        self.qos = qos
        self.dry_run_published: list[tuple[str, str]] = []

    def topic_for(self, env: NotificationEnvelope) -> str:
        return f"{self.topic_prefix}/{env.severity}"

    def send(self, env: NotificationEnvelope) -> SendResult:
        payload = json.dumps(env.model_dump(mode="json"), ensure_ascii=True)
        if self.mode == "dry_run":
            self.dry_run_published.append((self.topic_for(env), payload))
            return SendResult(ok=True)
        return self._send_live(self.topic_for(env), payload)

    def _send_live(self, topic: str, payload: str) -> SendResult:
        import os
        import time

        try:
            import paho.mqtt.client as mqtt
        except ImportError as exc:
            raise ChannelError(
                "modo live requiere paho-mqtt: pip install 'eovrt-alert-distribution[mqtt]'"
            ) from exc
        try:
            if getattr(self, "_client", None) is None:
                client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
                user = os.environ.get("EOVRT_MQTT_USERNAME")
                if user:
                    client.username_pw_set(user, os.environ.get("EOVRT_MQTT_PASSWORD"))
                client.connect(self.host, self.port, keepalive=30)
                client.loop_start()
                self._client = client
            info = self._client.publish(topic, payload, qos=self.qos)
            info.wait_for_publish(timeout=5)
            if not info.is_published():
                return SendResult(ok=False, error="publish timeout (sin PUBACK)")
            return SendResult(ok=True, puback_wall_ms=time.time() * 1000.0)
        except ChannelError:
            raise
        except (OSError, RuntimeError, ValueError) as exc:
            # Broker caído, DNS, auth o estado inválido: el Distributor decide
            # el retry sin perder la causa original.
            return SendResult(ok=False, error=f"{type(exc).__name__}: {exc}")

    def close(self) -> None:
        client = getattr(self, "_client", None)
        if client is None:
            return
        try:
            client.disconnect()
        except (OSError, RuntimeError, ValueError):
            logger.debug("falló disconnect MQTT durante el cierre", exc_info=True)
        try:
            client.loop_stop()
        except (OSError, RuntimeError, ValueError):
            logger.debug("falló loop_stop MQTT durante el cierre", exc_info=True)
        self._client = None
