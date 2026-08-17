"""Idempotencia por (notification_id, channel); backing en notifications.jsonl.

Política de generaciones (append-only entre corridas, 92b §6):

- Al reabrir un `out_dir` ya usado, la generación vigente se **archiva íntegra** como
  `notifications.<n>.jsonl` (primer `n >= 1` libre). No hay compactación: ninguna fila
  se borra jamás, ni siquiera los `failed` / `suppressed_cooldown` / `dead_letter` de
  corridas anteriores.
- El set de deduplicación es **acumulativo sobre TODAS las generaciones** presentes en
  el directorio, no solo sobre la última: un `delivered` registrado en cualquier corrida
  previa (`.1`, `.2`, ...) sigue considerándose entregado y su alerta se clasifica como
  `skipped_duplicate`. Sin esta acumulación, un replay repetido sobre el mismo directorio
  volvería a publicar lo ya entregado.

Los consumidores del artefacto (`report.py`, `aggregate.py`) leen `notifications.jsonl`
por nombre exacto, de modo que siempre ven la generación en curso.
"""

from __future__ import annotations

import json
from pathlib import Path

from eovrt_distribution.contracts.delivery import DeliveryRecord


def archive_previous(path: Path) -> Path | None:
    """Renombra un artefacto previo a <stem>.<n>.jsonl.

    Nada se borra jamás: la generación anterior queda íntegra al lado de la vigente
    (92b §6).
    """
    path = Path(path)
    if not path.exists():
        return None
    n = 1
    while True:
        candidate = path.with_name(f"{path.stem}.{n}{path.suffix}")
        if not candidate.exists():
            path.rename(candidate)
            return candidate
        n += 1


def _generation_order(path: Path) -> tuple[int, int, str]:
    """Clave de orden numérico para `<stem>.<n><suffix>`.

    `sorted()` lexicográfico intercalaría `.1, .10, .11, .2`. Se ordena por el índice
    de generación; lo que no parsea como entero va al final, de forma estable por nombre.
    """
    tail = path.stem.rpartition(".")[2]
    if tail.isdigit():
        return (0, int(tail), "")
    return (1, 0, path.name)


class DeliveryLedger:
    def __init__(self, notifications_path: Path) -> None:
        """Archiva la generación previa y rehidrata los `delivered` ACUMULADOS.

        El set de deduplicación se reconstruye leyendo **todas** las generaciones
        archivadas del directorio (`notifications.<n>.jsonl`), en orden numérico de
        generación, no solo la última: una entrega de cualquier corrida anterior sigue
        contando como entregada. Nada se borra; ver el docstring del módulo.
        """
        self._path = Path(notifications_path)
        self._delivered: set[tuple[str, str]] = set()
        self._path.parent.mkdir(parents=True, exist_ok=True)
        if self._path.exists():
            archive_previous(self._path)

        archived = self._path.parent.glob(f"{self._path.stem}.*{self._path.suffix}")
        for path in sorted(archived, key=_generation_order):
            for line in path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                row = json.loads(line)
                if row.get("outcome") == "delivered":
                    self._delivered.add((row["notification_id"], row["channel"]))

    def seen(self, notification_id: str, channel: str) -> bool:
        return (notification_id, channel) in self._delivered

    def append(self, record: DeliveryRecord) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record.model_dump(mode="json"), ensure_ascii=True))
            fh.write("\n")
        if record.outcome == "delivered":
            self._delivered.add((record.notification_id, record.channel))
