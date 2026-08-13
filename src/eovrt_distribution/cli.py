"""eovrt-distribute: replay (DBE) y live (EBE) sobre el mismo Distributor."""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

from eovrt_distribution.channels.mqtt import MqttChannel
from eovrt_distribution.config import DistributionConfig
from eovrt_distribution.distributor import Distributor
from eovrt_distribution.policy import NotificationPolicy
from eovrt_distribution.sources import JsonlReplaySource


def _positive_finite_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed) or parsed <= 0:
        raise argparse.ArgumentTypeError("debe ser un número finito mayor que cero")
    return parsed


def _build(cfg: DistributionConfig, source, out_dir: str) -> Distributor:
    return Distributor(
        source=source,
        channel=MqttChannel(
            mode=cfg.channel.mode,  # type: ignore[arg-type]
            host=cfg.channel.host,
            port=cfg.channel.port,
            topic_prefix=cfg.channel.topic_prefix,
            qos=cfg.channel.qos,
        ),
        policy=NotificationPolicy(
            cooldown_ms=cfg.notification_policy.cooldown_ms,
            key_fields=tuple(cfg.notification_policy.key),
        ),
        out_dir=Path(out_dir),
        max_attempts=cfg.retry.max_attempts,
        retry_wait_ms=cfg.retry.wait_ms,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="eovrt-distribute")
    sub = parser.add_subparsers(dest="command", required=True)

    p_replay = sub.add_parser("replay", help="DBE: releer alerts.jsonl de una corrida")
    p_replay.add_argument("--alerts", required=True)
    p_replay.add_argument("--out-dir", required=True)
    p_replay.add_argument("--config", default=None)

    p_live = sub.add_parser("live", help="EBE: consumir el bus control.alert.v1.*")
    p_live.add_argument("--endpoint", required=True)
    p_live.add_argument("--out-dir", required=True)
    p_live.add_argument("--backfill", default=None)
    p_live.add_argument("--control-run-id", default=None)
    p_live.add_argument("--config", default=None)
    p_live.add_argument("--idle-timeout-ms", type=_positive_finite_float, default=None)

    args = parser.parse_args(argv)
    cfg = DistributionConfig.load(Path(args.config) if args.config else None)

    if args.command == "replay":
        alerts_path = Path(args.alerts)
        if not alerts_path.exists():
            print(f"no existe: {alerts_path}", file=sys.stderr)
            return 2
        source = JsonlReplaySource(alerts_path)
    else:
        from eovrt_distribution.transport.zmq_source import ZmqSource

        backfill_path = Path(args.backfill) if args.backfill else None
        if backfill_path is not None and not backfill_path.is_file():
            print(f"no existe: {backfill_path}", file=sys.stderr)
            return 2

        source = ZmqSource(
            endpoint=args.endpoint,
            backfill_path=backfill_path,
            idle_timeout_ms=args.idle_timeout_ms,
            control_run_id=args.control_run_id,
        )

    summary = _build(cfg, source, args.out_dir).run()
    print(json.dumps(summary, ensure_ascii=True))
    return 0
