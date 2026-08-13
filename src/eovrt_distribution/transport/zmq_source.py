"""ZmqSource: SUB al bus de alertas del control-plane, con backfill.

Parada cooperativa: request_stop() setea un flag; el recv-loop usa RCVTIMEO
y el socket SIEMPRE se crea y cierra en el hilo que itera — cerrar un socket
ZMQ desde otro hilo con un recv en curso aborta el proceso (SIGABRT).
"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from pathlib import Path

import zmq

from eovrt_distribution.sources import JsonlReplaySource, SourcedAlert
from eovrt_distribution.transport.envelope import (
    ALERT_TOPIC_PREFIX,
    LIFECYCLE_TOPIC_PREFIX,
    decode_envelope,
)


class ZmqSource:
    def __init__(
        self,
        endpoint: str,
        backfill_path: Path | None = None,
        recv_timeout_ms: int = 500,
        idle_timeout_ms: float | None = None,
        control_run_id: str | None = None,
    ) -> None:
        self.endpoint = endpoint
        self.backfill_path = Path(backfill_path) if backfill_path else None
        self.recv_timeout_ms = recv_timeout_ms
        self.idle_timeout_ms = idle_timeout_ms
        self.control_run_id = control_run_id
        self._stop = threading.Event()
        self.stats = {
            "read": 0,
            "skipped_malformed": 0,
            "backfill_read": 0,
            "bus_dropped_events": 0,
            "duplicates_from_backfill": 0,
            "termination_reason": None,
        }

    def request_stop(self) -> None:
        self._stop.set()

    def __iter__(self) -> Iterator[SourcedAlert]:
        seen_alert_ids: set[str] = set()
        ctx = zmq.Context.instance()
        sock = ctx.socket(zmq.SUB)
        sock.setsockopt(zmq.RCVTIMEO, self.recv_timeout_ms)
        sock.connect(self.endpoint)
        if self.control_run_id:
            sock.setsockopt_string(zmq.SUBSCRIBE, f"{ALERT_TOPIC_PREFIX}{self.control_run_id}")
            sock.setsockopt_string(zmq.SUBSCRIBE, f"{LIFECYCLE_TOPIC_PREFIX}{self.control_run_id}")
        else:
            sock.setsockopt_string(zmq.SUBSCRIBE, ALERT_TOPIC_PREFIX)
            sock.setsockopt_string(zmq.SUBSCRIBE, LIFECYCLE_TOPIC_PREFIX)
        try:
            target_run_id = self.control_run_id
            if self.backfill_path and self.backfill_path.exists():
                replay = JsonlReplaySource(self.backfill_path)
                for sourced in replay:
                    source_run_id = sourced.alert.get("control_run_id")
                    if target_run_id is None and isinstance(source_run_id, str):
                        target_run_id = source_run_id
                    if target_run_id is not None and source_run_id != target_run_id:
                        self.stats["skipped_malformed"] += 1
                        continue
                    seen_alert_ids.add(sourced.alert.get("alert_id", ""))
                    self.stats["backfill_read"] += 1
                    yield sourced
                self.stats["skipped_malformed"] += replay.stats["skipped_malformed"]

            last_seq: dict[str, int] = {}
            idle_ms = 0.0
            while not self._stop.is_set():
                try:
                    frames = sock.recv_multipart()
                except zmq.Again:
                    idle_ms += self.recv_timeout_ms
                    if self.idle_timeout_ms and idle_ms >= self.idle_timeout_ms:
                        self.stats["termination_reason"] = "idle_timeout"
                        break
                    continue
                idle_ms = 0.0
                try:
                    env = decode_envelope(frames)
                except ValueError:
                    self.stats["skipped_malformed"] += 1
                    continue
                topic, seq = env["topic"], env["seq"]
                if topic.startswith(ALERT_TOPIC_PREFIX):
                    topic_run_id = topic.removeprefix(ALERT_TOPIC_PREFIX)
                elif topic.startswith(LIFECYCLE_TOPIC_PREFIX):
                    topic_run_id = topic.removeprefix(LIFECYCLE_TOPIC_PREFIX)
                else:
                    continue
                if target_run_id is None and topic.startswith(ALERT_TOPIC_PREFIX):
                    payload_run_id = env["payload"].get("control_run_id")
                    if isinstance(payload_run_id, str):
                        target_run_id = payload_run_id
                if target_run_id is None or topic_run_id != target_run_id:
                    continue
                prev = last_seq.get("_bus")
                if prev is None and seq > 0:
                    self.stats["bus_dropped_events"] += seq
                elif prev is not None and seq > prev + 1:
                    self.stats["bus_dropped_events"] += seq - prev - 1
                last_seq["_bus"] = seq
                if topic.startswith(LIFECYCLE_TOPIC_PREFIX):
                    if env["payload"].get("event") == "run_finished":
                        self.stats["termination_reason"] = "run_finished"
                        break
                    continue
                if not topic.startswith(ALERT_TOPIC_PREFIX):
                    continue
                alert = env["payload"]
                if alert.get("alert_id") in seen_alert_ids:
                    self.stats["duplicates_from_backfill"] += 1
                    continue
                self.stats["read"] += 1
                yield SourcedAlert(alert=alert, ts_publish_ms=env["ts_publish_ms"])
        finally:
            if self._stop.is_set() and self.stats["termination_reason"] is None:
                self.stats["termination_reason"] = "requested_stop"
            sock.close(0)
