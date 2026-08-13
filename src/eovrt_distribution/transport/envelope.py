"""bus.envelope.v1 — wire-compatible con el publisher del control-plane."""

from __future__ import annotations

import json
import math

import msgpack

ALERT_TOPIC_PREFIX = "control.alert.v1."
LIFECYCLE_TOPIC_PREFIX = "run.lifecycle.v1."


def decode_envelope(frames: list[bytes]) -> dict:
    if len(frames) != 2:
        raise ValueError(f"esperados 2 frames, llegaron {len(frames)}")
    try:
        wire_topic = frames[0].decode("utf-8")
        env = msgpack.unpackb(frames[1], raw=False)
        if not isinstance(env, dict) or env.get("schema_version") != "bus.envelope.v1":
            raise ValueError(f"envelope desconocido: {env!r:.120}")
        topic = env["topic"]
        seq = env["seq"]
        ts_publish_ms = env.get("ts_publish_ms")
        if not isinstance(topic, str) or topic != wire_topic:
            raise ValueError("topic del frame no coincide con el topic del envelope")
        if isinstance(seq, bool) or not isinstance(seq, int) or seq < 0:
            raise ValueError("seq debe ser un entero no negativo")
        if ts_publish_ms is not None and (
            isinstance(ts_publish_ms, bool)
            or not isinstance(ts_publish_ms, (int, float))
            or not math.isfinite(ts_publish_ms)
        ):
            raise ValueError("ts_publish_ms debe ser numerico o null")
        payload_raw = env["payload"]
        if isinstance(payload_raw, (bytes, bytearray)):
            payload = json.loads(payload_raw.decode("utf-8"))
        elif isinstance(payload_raw, str):
            payload = json.loads(payload_raw)
        else:
            raise TypeError("payload debe ser JSON serializado como bytes o string")
        if not isinstance(payload, dict):
            raise TypeError("payload JSON debe ser un objeto")
    except (KeyError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise ValueError(f"envelope invalido: {exc}") from exc
    return {
        "topic": topic,
        "key": env.get("key"),
        "seq": seq,
        "ts_publish_ms": ts_publish_ms,
        "payload": payload,
    }
