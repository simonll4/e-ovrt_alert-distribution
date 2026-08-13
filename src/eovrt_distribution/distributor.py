"""Pipeline: source → policy → ledger → channel(retry) → records + summary."""

from __future__ import annotations

import json
import math
import statistics
import time
from datetime import UTC, datetime
from pathlib import Path

from pydantic import ValidationError

from eovrt_distribution.channels.mqtt import MqttChannel
from eovrt_distribution.contracts.delivery import DeliveryRecord
from eovrt_distribution.contracts.notification import NotificationEnvelope
from eovrt_distribution.ledger import DeliveryLedger
from eovrt_distribution.policy import NotificationPolicy


def _now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _p95(values: list[float]) -> float:
    ordered = sorted(values)
    idx = max(0, math.ceil(0.95 * len(ordered)) - 1)
    return ordered[idx]


class Distributor:
    def __init__(
        self,
        source,
        channel: MqttChannel,
        policy: NotificationPolicy,
        out_dir: Path,
        max_attempts: int = 3,
        retry_wait_ms: float = 500.0,
    ) -> None:
        self.source = source
        self.channel = channel
        self.policy = policy
        self.out_dir = Path(out_dir)
        self.max_attempts = max_attempts
        self.retry_wait_ms = retry_wait_ms

    def run(self) -> dict:
        self.out_dir.mkdir(parents=True, exist_ok=True)
        ledger = DeliveryLedger(self.out_dir / "notifications.jsonl")
        dead_letter_path = self.out_dir / "dead_letter.jsonl"
        dead_letter_path.unlink(missing_ok=True)
        counts: dict[str, int] = {}
        latencies_by_mode: dict[str, list[float]] = {}
        run_meta: dict = {}
        skipped_invalid_alerts = 0

        def record(rec: DeliveryRecord) -> None:
            ledger.append(rec)
            counts[rec.outcome] = counts.get(rec.outcome, 0) + 1
            if (
                rec.talert_notification_ms is not None
                and rec.outcome == "delivered"
                and rec.latency_mode is not None
            ):
                latencies_by_mode.setdefault(rec.latency_mode, []).append(
                    rec.talert_notification_ms
                )
            if rec.outcome == "dead_letter":
                with dead_letter_path.open("a", encoding="utf-8") as fh:
                    fh.write(json.dumps(rec.model_dump(mode="json"), ensure_ascii=True))
                    fh.write("\n")

        try:
            for sourced in self.source:
                try:
                    env = NotificationEnvelope.from_alert(
                        sourced.alert, ts_publish_ms=sourced.ts_publish_ms
                    )
                except (KeyError, TypeError, ValueError, ValidationError):
                    skipped_invalid_alerts += 1
                    continue
                run_meta.setdefault("control_run_id", env.control_run_id)
                run_meta.setdefault("experiment_id", env.experiment_id)
                base = {
                    "control_run_id": env.control_run_id,
                    "notification_id": env.notification_id,
                    "alert_id": env.alert_id,
                    "channel": self.channel.name,
                    "mode": self.channel.mode,
                    "experiment_id": env.experiment_id,
                }

                if ledger.seen(env.notification_id, self.channel.name):
                    record(
                        DeliveryRecord(
                            **base,
                            attempt=0,
                            outcome="skipped_duplicate",
                            attempted_at=_now_iso(),
                        )
                    )
                    continue

                now_wall_ms = time.time() * 1000.0
                if self.policy.is_suppressed(env, now_wall_ms=now_wall_ms):
                    record(
                        DeliveryRecord(
                            **base,
                            attempt=0,
                            outcome="suppressed_cooldown",
                            attempted_at=_now_iso(),
                        )
                    )
                    continue

                delivered = False
                last_error: str | None = None
                notification_start_ms = time.time() * 1000.0
                for attempt in range(1, self.max_attempts + 1):
                    attempted_at = _now_iso()
                    result = self.channel.send(env)
                    if result.ok:
                        end_ms = result.puback_wall_ms or time.time() * 1000.0
                        if env.confirmed_wall_ms is not None:
                            latency, mode = end_ms - env.confirmed_wall_ms, "live"
                        else:
                            latency, mode = (
                                end_ms - notification_start_ms,
                                "wall_clock_dbe",
                            )
                        record(
                            DeliveryRecord(
                                **base,
                                attempt=attempt,
                                outcome="delivered",
                                talert_notification_ms=latency,
                                latency_mode=mode,
                                attempted_at=attempted_at,
                                delivered_at=_now_iso(),
                            )
                        )
                        self.policy.mark_notified(env, now_wall_ms=end_ms)
                        delivered = True
                        break
                    last_error = result.error
                    record(
                        DeliveryRecord(
                            **base,
                            attempt=attempt,
                            outcome="failed",
                            error=last_error,
                            attempted_at=attempted_at,
                        )
                    )
                    if attempt < self.max_attempts and self.retry_wait_ms > 0:
                        time.sleep(self.retry_wait_ms / 1000.0)

                if not delivered:
                    record(
                        DeliveryRecord(
                            **base,
                            attempt=self.max_attempts,
                            outcome="dead_letter",
                            error=last_error or "max_attempts exhausted",
                            attempted_at=_now_iso(),
                        )
                    )
        finally:
            self.channel.close()

        summary = {
            "schema_version": "control.distribution_summary.v1",
            "control_run_id": run_meta.get("control_run_id"),
            "experiment_id": run_meta.get("experiment_id"),
            "channel": self.channel.name,
            "mode": self.channel.mode,
            "counts": counts,
            "skipped_invalid_alerts": skipped_invalid_alerts,
            "source_stats": dict(self.source.stats),
            "talert_notification_ms": (
                {
                    mode: {
                        "count": len(values),
                        "min": min(values),
                        "mean": statistics.fmean(values),
                        "p95": _p95(values),
                    }
                    for mode, values in latencies_by_mode.items()
                }
                if latencies_by_mode
                else None
            ),
        }
        (self.out_dir / "distribution_summary.json").write_text(
            json.dumps(summary, indent=2, ensure_ascii=True) + "\n"
        )
        return summary
