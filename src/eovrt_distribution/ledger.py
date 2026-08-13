"""Idempotencia por (notification_id, channel); backing en notifications.jsonl."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

from eovrt_distribution.contracts.delivery import DeliveryRecord


class DeliveryLedger:
    def __init__(self, notifications_path: Path) -> None:
        self._path = Path(notifications_path)
        self._delivered: set[tuple[str, str]] = set()
        if self._path.exists():
            delivered_by_key: dict[tuple[str, str], dict] = {}
            for line in self._path.read_text().splitlines():
                if not line.strip():
                    continue
                row = json.loads(line)
                if row.get("outcome") == "delivered":
                    key = (row["notification_id"], row["channel"])
                    delivered_by_key[key] = row

            self._delivered = set(delivered_by_key.keys())
            self._rewrite_with_delivered_only(delivered_by_key)
            return

        self._path.parent.mkdir(parents=True, exist_ok=True)

    def _rewrite_with_delivered_only(self, delivered_by_key: dict[tuple[str, str], dict]) -> None:
        if not self._path.parent.exists():
            self._path.parent.mkdir(parents=True, exist_ok=True)

        fd, tmp_name = tempfile.mkstemp(
            dir=self._path.parent, prefix=self._path.name, suffix=".tmp"
        )
        tmp_path = Path(tmp_name)
        os.close(fd)
        try:
            with tmp_path.open("w", encoding="utf-8") as fh:
                for row in delivered_by_key.values():
                    fh.write(json.dumps(row, ensure_ascii=True))
                    fh.write("\n")
            tmp_path.replace(self._path)
        finally:
            try:
                tmp_path.unlink()
            except FileNotFoundError:
                pass
            except PermissionError:
                pass

    def seen(self, notification_id: str, channel: str) -> bool:
        return (notification_id, channel) in self._delivered

    def append(self, record: DeliveryRecord) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record.model_dump(mode="json"), ensure_ascii=True))
            fh.write("\n")
        if record.outcome == "delivered":
            self._delivered.add((record.notification_id, record.channel))
