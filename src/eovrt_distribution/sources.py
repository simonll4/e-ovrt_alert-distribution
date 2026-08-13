"""Fuentes de alertas: Direct (tests), JsonlReplay (DBE). Zmq vive en transport/."""

from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path


@dataclass
class SourcedAlert:
    alert: dict
    ts_publish_ms: float | None = None


class DirectSource:
    def __init__(self, alerts: list[dict | SourcedAlert]) -> None:
        self._alerts = alerts
        self.stats = {"read": 0, "skipped_malformed": 0}

    def __iter__(self) -> Iterator[SourcedAlert]:
        for item in self._alerts:
            self.stats["read"] += 1
            yield item if isinstance(item, SourcedAlert) else SourcedAlert(alert=item)


class JsonlReplaySource:
    def __init__(self, path: Path) -> None:
        self._path = Path(path)
        self.stats = {"read": 0, "skipped_malformed": 0}

    def __iter__(self) -> Iterator[SourcedAlert]:
        with self._path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    alert = json.loads(line)
                except json.JSONDecodeError:
                    self.stats["skipped_malformed"] += 1
                    continue
                self.stats["read"] += 1
                yield SourcedAlert(alert=alert)
