# Alert Distribution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implementar `e-ovrt_alert-distribution`: consumidor desacoplado de alertas confirmadas del control-plane que aplica política de notificación (cooldown), entrega por MQTT y registra todo resultado de forma trazable e idempotente.

**Architecture:** Pipeline `source → policy → ledger → channel → records`. Dos fuentes (`JsonlReplaySource` para DBE, `ZmqSource` con backfill para EBE) alimentan un único `Distributor`. Contratos Pydantic (`control.notification.v1`, `control.delivery.v1`). Canal MQTT único con modos `dry_run` (default, sin I/O, cubre CI) y `live` (paho-mqtt, QoS 1). Salidas append-only por corrida: `notifications.jsonl`, `dead_letter.jsonl`, `distribution_summary.json`.

**Tech Stack:** Python 3.11, pydantic v2, pyzmq + msgpack (bus), PyYAML (config), paho-mqtt (extra opcional `[mqtt]`), pytest.

**Spec:** `docs/superpowers/specs/2026-07-18-alert-distribution-design.md` (en este repo). Normativa upstream: spec 45, ADR-005, ADR-011, doc 06 (repo `docs/` del workspace).

## Global Constraints

- **NUNCA crear un commit de git salvo pedido explícito del usuario en ese turno** (regla del workspace, `projects/CLAUDE.md`). Los pasos de este plan terminan en tests verdes, no en commit. Donde un ejecutor esperaría "Commit", detenerse y dejar el working tree listo.
- Python `>=3.11`; paquete `eovrt_distribution` con src-layout; entry point CLI `eovrt-distribute`.
- `paho-mqtt` es extra opcional `[mqtt]`: sin él, todo funciona salvo el modo `live` del canal (que falla con mensaje claro).
- Frontera estricta (ADR-011): jamás recalcular severidad, mutar estado de patrón ni crear alertas. El módulo solo consume, decide notificación y registra.
- La entrada es el `AlertEvent` real del control-plane (`control.alert.v1`): campos `alert_id`, `condition_id`, `source_id`, `severity`, `subject_key`, `pattern_id`, `control_run_id`, `media_run_id`, `experiment_id?`, `timestamp_ms?` (tiempo de media), `evidence?`. **No existe `confirmed_at_ms`.**
- `notification_id` = `sha1(alert_id)[:16]` — determinista, idempotencia por construcción.
- Outcomes válidos de `DeliveryRecord`: `delivered | failed | skipped_duplicate | dead_letter | suppressed_cooldown`.
- Cooldown: default `cooldown_ms: 30000`, clave `(condition_id, source_id)`, opera sobre `timestamp_ms` (tiempo de media) con fallback a wall-clock si es `None`. Toda supresión genera registro — nunca silenciosa.
- Retry: `max_attempts: 3`, espera fija `retry_wait_ms: 500` (valor fijado en este plan; spec lo dejaba abierto).
- Métrica `talert_notification_ms`: en live = `puback_wall_ms - ts_publish_ms` (del envelope de bus), `latency_mode: "live"`; en replay/dry_run = duración del envío, `latency_mode: "wall_clock_dbe"` (spec 40 §5).
- Bus (verificado contra `e-ovrt_control-plane/src/eovrt_control/transport/alert_bus.py`): publisher XPUB **bind** `tcp://0.0.0.0:5558` — el consumidor hace connect+SUB a los prefijos `control.alert.v1.` y `run.lifecycle.v1.`; mensajes de 2 frames `[topic-utf8, msgpack {schema_version:"bus.envelope.v1", topic, key, seq, ts_publish_ms, payload}]`, `payload` = bytes de la línea JSONL del AlertEvent. Huecos de `seq` se cuentan (`bus_dropped_events`), nunca se silencian. Trampa ZMQ no negociable: jamás cerrar un socket desde un hilo distinto al que lo creó mientras hay un `recv` en curso — parada cooperativa vía flag + `RCVTIMEO`.
- Credenciales MQTT solo por env (`EOVRT_MQTT_USERNAME`, `EOVRT_MQTT_PASSWORD`) — jamás en configs versionadas ni artefactos.

## File Structure

```
e-ovrt_alert-distribution/
├── pyproject.toml
├── README.md
├── .gitignore
├── configs/
│   └── example.yaml
├── src/eovrt_distribution/
│   ├── __init__.py
│   ├── contracts/
│   │   ├── __init__.py
│   │   ├── notification.py     # NotificationEnvelope + from_alert()
│   │   └── delivery.py         # DeliveryRecord + Outcome
│   ├── policy.py               # NotificationPolicy (cooldown ADR-011)
│   ├── ledger.py               # DeliveryLedger (idempotencia + writer JSONL)
│   ├── channels/
│   │   ├── __init__.py
│   │   ├── base.py             # Channel protocol + SendResult + ChannelError
│   │   └── mqtt.py             # MqttChannel dry_run|live
│   ├── sources.py              # DirectSource, JsonlReplaySource, SourcedAlert
│   ├── transport/
│   │   ├── __init__.py
│   │   ├── envelope.py         # decode bus.envelope.v1 (msgpack)
│   │   └── zmq_source.py       # ZmqSource (SUB, backfill, seq gaps, run_finished)
│   ├── distributor.py          # Distributor + DistributionSummary
│   ├── config.py               # DistributionConfig (YAML)
│   └── cli.py                  # eovrt-distribute replay|live
└── tests/
    ├── conftest.py             # fixtures: make_alert(), tmp run dirs
    ├── test_contracts.py
    ├── test_policy.py
    ├── test_ledger.py
    ├── test_channel_mqtt.py
    ├── test_distributor.py
    ├── test_sources.py
    ├── test_zmq_source.py
    ├── test_cli.py
    └── test_mqtt_live.py       # marcado integration (requiere broker)
```

---

### Task 1: Scaffolding del paquete

**Files:**
- Create: `pyproject.toml`, `.gitignore`, `README.md`, `src/eovrt_distribution/__init__.py`, `src/eovrt_distribution/contracts/__init__.py`, `src/eovrt_distribution/channels/__init__.py`, `src/eovrt_distribution/transport/__init__.py`, `tests/conftest.py`

**Interfaces:**
- Produces: paquete instalable `eovrt_distribution`; fixture `make_alert(**overrides) -> dict` que todos los tests posteriores usan.

- [x] **Step 1: Crear pyproject.toml**

```toml
[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[project]
name = "eovrt-alert-distribution"
version = "0.1.0"
description = "E-OVRT-VDP alert distribution: decoupled consumer of confirmed alerts (spec 45)"
requires-python = ">=3.11"
dependencies = [
    "pydantic>=2.5",
    "pyzmq>=25",
    "msgpack>=1.0",
    "PyYAML>=6.0",
]

[project.optional-dependencies]
mqtt = ["paho-mqtt>=2.0"]
dev = ["pytest>=8", "ruff>=0.4"]

[project.scripts]
eovrt-distribute = "eovrt_distribution.cli:main"

[tool.setuptools.packages.find]
where = ["src"]

[tool.pytest.ini_options]
testpaths = ["tests"]
markers = ["integration: requires external services (MQTT broker)"]
addopts = "-m 'not integration'"
```

- [x] **Step 2: Crear .gitignore y README.md**

`.gitignore`:
```
__pycache__/
*.egg-info/
.venv/
runs/
.pytest_cache/
.ruff_cache/
```

`README.md`:
```markdown
# e-ovrt_alert-distribution

Distribución de alertas de E-OVRT-VDP (spec 45). Consumidor desacoplado de
alertas ya confirmadas por el control-plane: las recibe (bus
`control.alert.v1.*` o `alerts.jsonl` en replay), aplica política de
notificación (cooldown, ADR-011), entrega por MQTT y registra intento y
resultado. Nunca recalcula severidad ni crea alertas.

Diseño: `docs/superpowers/specs/2026-07-18-alert-distribution-design.md`.

## Uso

    pip install -e ".[mqtt,dev]"
    eovrt-distribute replay --alerts <control-run>/alerts.jsonl --out-dir runs/d1
    eovrt-distribute live --endpoint tcp://127.0.0.1:5558 --backfill <control-run>/alerts.jsonl --out-dir runs/d2
```

- [x] **Step 3: Crear los `__init__.py`** (los cuatro, vacíos salvo el raíz)

`src/eovrt_distribution/__init__.py`:
```python
__version__ = "0.1.0"
```

- [x] **Step 4: Crear tests/conftest.py con la fábrica de alertas**

La forma imita la línea real de `alerts.jsonl` del control-plane (contrato `control.alert.v1`).

```python
import itertools

import pytest

_counter = itertools.count(1)


@pytest.fixture
def make_alert():
    def _make(**overrides) -> dict:
        i = next(_counter)
        alert = {
            "schema_version": "control.alert.v1",
            "event_type": "alert_event",
            "control_run_id": "cr-test",
            "media_run_id": "mr-test",
            "unit_id": f"u-{i:04d}",
            "source_id": "cam-01",
            "alert_id": f"00000000-0000-5000-8000-{i:012d}",
            "pattern_id": "pr01",
            "condition_id": "CR-01",
            "subject_key": f"subject-{i}",
            "severity": "high",
            "state": "open",
            "evidence": {},
            "timestamp_ms": 1000.0 * i,
            "experiment_id": "exp-test",
        }
        alert.update(overrides)
        return alert

    return _make
```

- [x] **Step 5: Instalar y verificar**

Run: `cd /home/simonll4/projects/e-ovrt_alert-distribution && python3.11 -m venv .venv && .venv/bin/pip install -e ".[dev]" -q && .venv/bin/python -c "import eovrt_distribution; print(eovrt_distribution.__version__)"`
Expected: `0.1.0`

Run: `.venv/bin/pytest -q`
Expected: `no tests ran` (exit code 5 — aceptable en este task; desde Task 2 siempre verde).

*(Sin commit — regla del workspace: solo a pedido explícito.)*

---

### Task 2: Contratos — NotificationEnvelope y DeliveryRecord

**Files:**
- Create: `src/eovrt_distribution/contracts/notification.py`, `src/eovrt_distribution/contracts/delivery.py`
- Test: `tests/test_contracts.py`

**Interfaces:**
- Consumes: dict de AlertEvent (fixture `make_alert`).
- Produces:
  - `NotificationEnvelope.from_alert(alert: dict, ts_publish_ms: float | None = None) -> NotificationEnvelope`
  - `notification_id_for(alert_id: str) -> str`
  - `DeliveryRecord(...)` con campos: `control_run_id, notification_id, alert_id, channel, mode, attempt, outcome, error, talert_notification_ms, latency_mode, attempted_at, delivered_at, experiment_id`
  - `Outcome = Literal["delivered", "failed", "skipped_duplicate", "dead_letter", "suppressed_cooldown"]`

- [x] **Step 1: Escribir tests que fallan**

`tests/test_contracts.py`:
```python
from eovrt_distribution.contracts.delivery import DeliveryRecord
from eovrt_distribution.contracts.notification import (
    NotificationEnvelope,
    notification_id_for,
)


def test_notification_id_is_deterministic_and_short():
    a = notification_id_for("00000000-0000-5000-8000-000000000001")
    b = notification_id_for("00000000-0000-5000-8000-000000000001")
    c = notification_id_for("00000000-0000-5000-8000-000000000002")
    assert a == b
    assert a != c
    assert len(a) == 16


def test_envelope_from_alert_maps_fields(make_alert):
    alert = make_alert(severity="medium")
    env = NotificationEnvelope.from_alert(alert, ts_publish_ms=1750000000000.0)
    assert env.schema_version == "control.notification.v1"
    assert env.event_type == "notification_envelope"
    assert env.alert_id == alert["alert_id"]
    assert env.notification_id == notification_id_for(alert["alert_id"])
    assert env.condition_id == "CR-01"
    assert env.source_id == "cam-01"
    assert env.severity == "medium"
    assert env.media_timestamp_ms == alert["timestamp_ms"]
    assert env.confirmed_wall_ms == 1750000000000.0
    assert env.experiment_id == "exp-test"
    assert alert["condition_id"] in env.summary_text
    assert alert["source_id"] in env.summary_text


def test_envelope_from_alert_replay_has_no_wall_clock(make_alert):
    env = NotificationEnvelope.from_alert(make_alert())
    assert env.confirmed_wall_ms is None


def test_delivery_record_rejects_unknown_outcome(make_alert):
    import pytest

    with pytest.raises(Exception):
        DeliveryRecord(
            control_run_id="cr",
            notification_id="n",
            alert_id="a",
            channel="mqtt",
            mode="dry_run",
            attempt=1,
            outcome="exploded",
            attempted_at="2026-07-18T00:00:00Z",
        )


def test_delivery_record_accepts_suppressed_cooldown():
    rec = DeliveryRecord(
        control_run_id="cr",
        notification_id="n",
        alert_id="a",
        channel="mqtt",
        mode="dry_run",
        attempt=0,
        outcome="suppressed_cooldown",
        attempted_at="2026-07-18T00:00:00Z",
    )
    assert rec.schema_version == "control.delivery.v1"
    assert rec.latency_mode is None
```

- [x] **Step 2: Verificar que fallan**

Run: `.venv/bin/pytest tests/test_contracts.py -q`
Expected: FAIL / error de import (`ModuleNotFoundError`).

- [x] **Step 3: Implementar contratos**

`src/eovrt_distribution/contracts/notification.py`:
```python
"""Contrato control.notification.v1 (doc 06 §6.1 + source_id + experiment_id)."""
from __future__ import annotations

import hashlib

from pydantic import BaseModel


def notification_id_for(alert_id: str) -> str:
    return hashlib.sha1(alert_id.encode("utf-8")).hexdigest()[:16]


class NotificationEnvelope(BaseModel):
    schema_version: str = "control.notification.v1"
    event_type: str = "notification_envelope"
    notification_id: str
    control_run_id: str
    media_run_id: str
    alert_id: str
    pattern_id: str
    condition_id: str
    source_id: str
    subject_key: str
    severity: str
    episode_state: str
    media_timestamp_ms: float | None = None
    confirmed_wall_ms: float | None = None  # ts_publish_ms del bus; None en replay
    evidence_ref: dict | None = None
    summary_text: str
    experiment_id: str | None = None

    @classmethod
    def from_alert(
        cls, alert: dict, ts_publish_ms: float | None = None
    ) -> "NotificationEnvelope":
        return cls(
            notification_id=notification_id_for(alert["alert_id"]),
            control_run_id=alert["control_run_id"],
            media_run_id=alert["media_run_id"],
            alert_id=alert["alert_id"],
            pattern_id=alert["pattern_id"],
            condition_id=alert["condition_id"],
            source_id=alert["source_id"],
            subject_key=alert["subject_key"],
            severity=alert["severity"],
            episode_state=alert.get("state", "open"),
            media_timestamp_ms=alert.get("timestamp_ms"),
            confirmed_wall_ms=ts_publish_ms,
            evidence_ref=alert.get("evidence"),
            summary_text=(
                f"[{alert['severity']}] {alert['condition_id']} confirmada en "
                f"{alert['source_id']} (sujeto {alert['subject_key']})"
            ),
            experiment_id=alert.get("experiment_id"),
        )
```

`src/eovrt_distribution/contracts/delivery.py`:
```python
"""Contrato control.delivery.v1 (doc 06 §6.2 + suppressed_cooldown + experiment_id)."""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel

Outcome = Literal[
    "delivered", "failed", "skipped_duplicate", "dead_letter", "suppressed_cooldown"
]


class DeliveryRecord(BaseModel):
    schema_version: str = "control.delivery.v1"
    event_type: str = "delivery_record"
    control_run_id: str
    notification_id: str
    alert_id: str
    channel: str
    mode: Literal["dry_run", "live"]
    attempt: int
    outcome: Outcome
    error: str | None = None
    talert_notification_ms: float | None = None
    latency_mode: Literal["live", "wall_clock_dbe"] | None = None
    attempted_at: str
    delivered_at: str | None = None
    experiment_id: str | None = None
```

- [x] **Step 4: Verificar que pasan**

Run: `.venv/bin/pytest tests/test_contracts.py -q`
Expected: `5 passed`

---

### Task 3: Ledger de idempotencia

**Files:**
- Create: `src/eovrt_distribution/ledger.py`
- Test: `tests/test_ledger.py`

**Interfaces:**
- Consumes: `DeliveryRecord` (Task 2).
- Produces: `DeliveryLedger(notifications_path: Path)` con:
  - `seen(notification_id: str, channel: str) -> bool` (solo cuenta `delivered`)
  - `append(record: DeliveryRecord) -> None` (escribe JSONL y actualiza el índice en memoria)
  - Al construirse, rehidrata el índice leyendo el archivo si existe.

- [x] **Step 1: Tests que fallan**

`tests/test_ledger.py`:
```python
import json

from eovrt_distribution.contracts.delivery import DeliveryRecord
from eovrt_distribution.ledger import DeliveryLedger


def _rec(outcome: str, nid: str = "n1") -> DeliveryRecord:
    return DeliveryRecord(
        control_run_id="cr",
        notification_id=nid,
        alert_id="a1",
        channel="mqtt",
        mode="dry_run",
        attempt=1,
        outcome=outcome,
        attempted_at="2026-07-18T00:00:00Z",
    )


def test_delivered_marks_seen(tmp_path):
    ledger = DeliveryLedger(tmp_path / "notifications.jsonl")
    assert not ledger.seen("n1", "mqtt")
    ledger.append(_rec("delivered"))
    assert ledger.seen("n1", "mqtt")
    assert not ledger.seen("n1", "telegram")


def test_non_delivered_outcomes_do_not_mark_seen(tmp_path):
    ledger = DeliveryLedger(tmp_path / "notifications.jsonl")
    ledger.append(_rec("failed"))
    ledger.append(_rec("suppressed_cooldown"))
    assert not ledger.seen("n1", "mqtt")


def test_rehydrates_from_existing_file(tmp_path):
    path = tmp_path / "notifications.jsonl"
    first = DeliveryLedger(path)
    first.append(_rec("delivered", nid="n9"))
    second = DeliveryLedger(path)
    assert second.seen("n9", "mqtt")


def test_appends_one_json_line_per_record(tmp_path):
    path = tmp_path / "notifications.jsonl"
    ledger = DeliveryLedger(path)
    ledger.append(_rec("delivered"))
    ledger.append(_rec("skipped_duplicate"))
    lines = path.read_text().strip().splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0])["outcome"] == "delivered"
```

- [x] **Step 2: Verificar que fallan**

Run: `.venv/bin/pytest tests/test_ledger.py -q`
Expected: FAIL (`ModuleNotFoundError`).

- [x] **Step 3: Implementar**

`src/eovrt_distribution/ledger.py`:
```python
"""Idempotencia por (notification_id, channel); backing en notifications.jsonl."""
from __future__ import annotations

import json
from pathlib import Path

from eovrt_distribution.contracts.delivery import DeliveryRecord


class DeliveryLedger:
    def __init__(self, notifications_path: Path) -> None:
        self._path = Path(notifications_path)
        self._delivered: set[tuple[str, str]] = set()
        if self._path.exists():
            for line in self._path.read_text().splitlines():
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
```

- [x] **Step 4: Verificar que pasan**

Run: `.venv/bin/pytest tests/test_ledger.py -q`
Expected: `4 passed`

---

### Task 4: Política de notificación (cooldown, ADR-011)

**Files:**
- Create: `src/eovrt_distribution/policy.py`
- Test: `tests/test_policy.py`

**Interfaces:**
- Consumes: `NotificationEnvelope` (Task 2).
- Produces: `NotificationPolicy(cooldown_ms: float = 30000.0, key_fields: tuple[str, ...] = ("condition_id", "source_id"))` con:
  - `allow(env: NotificationEnvelope, now_wall_ms: float) -> bool` — `True` = notificar (y arma la ventana); `False` = suprimir. Base de tiempo: `media_timestamp_ms` si está presente, si no `now_wall_ms`.

- [x] **Step 1: Tests que fallan**

`tests/test_policy.py`:
```python
from eovrt_distribution.contracts.notification import NotificationEnvelope
from eovrt_distribution.policy import NotificationPolicy


def _env(make_alert, **overrides) -> NotificationEnvelope:
    return NotificationEnvelope.from_alert(make_alert(**overrides))


def test_burst_same_condition_source_suppressed(make_alert):
    policy = NotificationPolicy(cooldown_ms=30000.0)
    first = _env(make_alert, timestamp_ms=1000.0)
    burst = _env(make_alert, timestamp_ms=5000.0, subject_key="otro-sujeto")
    assert policy.allow(first, now_wall_ms=0.0)
    assert not policy.allow(burst, now_wall_ms=0.0)  # coalesce entre sujetos


def test_reallows_after_window(make_alert):
    policy = NotificationPolicy(cooldown_ms=30000.0)
    assert policy.allow(_env(make_alert, timestamp_ms=1000.0), now_wall_ms=0.0)
    assert policy.allow(_env(make_alert, timestamp_ms=31001.0), now_wall_ms=0.0)


def test_different_key_not_suppressed(make_alert):
    policy = NotificationPolicy(cooldown_ms=30000.0)
    assert policy.allow(_env(make_alert, timestamp_ms=1000.0), now_wall_ms=0.0)
    assert policy.allow(
        _env(make_alert, timestamp_ms=2000.0, source_id="cam-02"), now_wall_ms=0.0
    )
    assert policy.allow(
        _env(make_alert, timestamp_ms=3000.0, condition_id="CR-02"), now_wall_ms=0.0
    )


def test_falls_back_to_wall_clock_without_media_time(make_alert):
    policy = NotificationPolicy(cooldown_ms=30000.0)
    assert policy.allow(_env(make_alert, timestamp_ms=None), now_wall_ms=100000.0)
    assert not policy.allow(_env(make_alert, timestamp_ms=None), now_wall_ms=110000.0)
    assert policy.allow(_env(make_alert, timestamp_ms=None), now_wall_ms=140001.0)


def test_zero_cooldown_never_suppresses(make_alert):
    policy = NotificationPolicy(cooldown_ms=0.0)
    assert policy.allow(_env(make_alert, timestamp_ms=1000.0), now_wall_ms=0.0)
    assert policy.allow(_env(make_alert, timestamp_ms=1000.0), now_wall_ms=0.0)
```

- [x] **Step 2: Verificar que fallan**

Run: `.venv/bin/pytest tests/test_policy.py -q`
Expected: FAIL (`ModuleNotFoundError`).

- [x] **Step 3: Implementar**

`src/eovrt_distribution/policy.py`:
```python
"""Política de notificación (ADR-011): el cooldown vive acá, no en el motor.

Clave de supresión (condition_id, source_id): para notificación asistiva lo
relevante es "esta condición en esta cámara ya fue avisada", independiente
del sujeto. Base de tiempo = media_timestamp_ms (coherente entre replay y
live); fallback a wall-clock si la alerta no trae tiempo de media.
"""
from __future__ import annotations

from eovrt_distribution.contracts.notification import NotificationEnvelope


class NotificationPolicy:
    def __init__(
        self,
        cooldown_ms: float = 30000.0,
        key_fields: tuple[str, ...] = ("condition_id", "source_id"),
    ) -> None:
        self.cooldown_ms = cooldown_ms
        self.key_fields = key_fields
        self._last_notified_ms: dict[tuple, float] = {}

    def _key(self, env: NotificationEnvelope) -> tuple:
        return tuple(getattr(env, field) for field in self.key_fields)

    def allow(self, env: NotificationEnvelope, now_wall_ms: float) -> bool:
        if self.cooldown_ms <= 0:
            return True
        t = env.media_timestamp_ms if env.media_timestamp_ms is not None else now_wall_ms
        key = self._key(env)
        last = self._last_notified_ms.get(key)
        if last is not None and (t - last) < self.cooldown_ms:
            return False
        self._last_notified_ms[key] = t
        return True
```

- [x] **Step 4: Verificar que pasan**

Run: `.venv/bin/pytest tests/test_policy.py -q`
Expected: `5 passed`

---

### Task 5: Canal MQTT (base + dry_run)

**Files:**
- Create: `src/eovrt_distribution/channels/base.py`, `src/eovrt_distribution/channels/mqtt.py`
- Test: `tests/test_channel_mqtt.py`

**Interfaces:**
- Consumes: `NotificationEnvelope` (Task 2).
- Produces:
  - `SendResult(ok: bool, error: str | None, puback_wall_ms: float | None)` (dataclass en `base.py`)
  - `ChannelError(Exception)` en `base.py`
  - `MqttChannel(mode: Literal["dry_run","live"] = "dry_run", host: str = "127.0.0.1", port: int = 1883, topic_prefix: str = "eovrt/alerts", qos: int = 1)` con:
    - `name: str = "mqtt"`
    - `topic_for(env) -> str` → `"eovrt/alerts/<severity>"`
    - `send(env: NotificationEnvelope) -> SendResult` — `dry_run`: sin I/O, siempre ok; `live`: se implementa en Task 10 (acá levanta `ChannelError` si falta paho o el modo live se invoca sin implementación).

- [x] **Step 1: Tests que fallan**

`tests/test_channel_mqtt.py`:
```python
from eovrt_distribution.channels.mqtt import MqttChannel
from eovrt_distribution.contracts.notification import NotificationEnvelope


def test_dry_run_send_ok_without_io(make_alert):
    channel = MqttChannel(mode="dry_run")
    env = NotificationEnvelope.from_alert(make_alert(severity="high"))
    result = channel.send(env)
    assert result.ok
    assert result.error is None
    assert channel.name == "mqtt"


def test_topic_includes_severity(make_alert):
    channel = MqttChannel(mode="dry_run")
    env = NotificationEnvelope.from_alert(make_alert(severity="medium"))
    assert channel.topic_for(env) == "eovrt/alerts/medium"


def test_dry_run_records_published_payloads(make_alert):
    channel = MqttChannel(mode="dry_run")
    env = NotificationEnvelope.from_alert(make_alert())
    channel.send(env)
    assert len(channel.dry_run_published) == 1
    topic, payload = channel.dry_run_published[0]
    assert topic == "eovrt/alerts/high"
    assert env.notification_id in payload
```

- [x] **Step 2: Verificar que fallan**

Run: `.venv/bin/pytest tests/test_channel_mqtt.py -q`
Expected: FAIL (`ModuleNotFoundError`).

- [x] **Step 3: Implementar**

`src/eovrt_distribution/channels/base.py`:
```python
from __future__ import annotations

from dataclasses import dataclass


class ChannelError(Exception):
    pass


@dataclass
class SendResult:
    ok: bool
    error: str | None = None
    puback_wall_ms: float | None = None
```

`src/eovrt_distribution/channels/mqtt.py`:
```python
"""Único canal de esta iteración (ADR-005). dry_run: sin I/O, cubre CI."""
from __future__ import annotations

import json
from typing import Literal

from eovrt_distribution.channels.base import ChannelError, SendResult
from eovrt_distribution.contracts.notification import NotificationEnvelope


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
        raise ChannelError(
            "modo live no disponible todavía (se implementa con paho-mqtt; "
            "instalar extra [mqtt])"
        )
```

- [x] **Step 4: Verificar que pasan**

Run: `.venv/bin/pytest tests/test_channel_mqtt.py -q`
Expected: `3 passed`

---

### Task 6: Fuentes básicas — SourcedAlert, DirectSource, JsonlReplaySource

**Files:**
- Create: `src/eovrt_distribution/sources.py`
- Test: `tests/test_sources.py`

**Interfaces:**
- Consumes: dicts de AlertEvent.
- Produces:
  - `SourcedAlert(alert: dict, ts_publish_ms: float | None)` (dataclass)
  - `DirectSource(alerts: list[dict])` — iterable de `SourcedAlert` con `ts_publish_ms=None`; para tests.
  - `JsonlReplaySource(path: Path)` — lee `alerts.jsonl` del control-plane línea a línea; ignora líneas vacías; `ts_publish_ms=None`.
  - Ambas implementan `__iter__() -> Iterator[SourcedAlert]` y exponen `stats: dict` (`{"read": int, "skipped_malformed": int}`; las líneas no parseables se cuentan, nunca abortan el replay).

- [x] **Step 1: Tests que fallan**

`tests/test_sources.py`:
```python
import json

from eovrt_distribution.sources import DirectSource, JsonlReplaySource


def test_direct_source_yields_all(make_alert):
    alerts = [make_alert(), make_alert()]
    src = DirectSource(alerts)
    got = list(src)
    assert [s.alert["alert_id"] for s in got] == [a["alert_id"] for a in alerts]
    assert all(s.ts_publish_ms is None for s in got)


def test_jsonl_replay_reads_lines_and_counts_malformed(tmp_path, make_alert):
    path = tmp_path / "alerts.jsonl"
    good1, good2 = make_alert(), make_alert()
    path.write_text(
        json.dumps(good1) + "\n" + "{esto no es json}\n" + "\n" + json.dumps(good2) + "\n"
    )
    src = JsonlReplaySource(path)
    got = list(src)
    assert [s.alert["alert_id"] for s in got] == [good1["alert_id"], good2["alert_id"]]
    assert src.stats == {"read": 2, "skipped_malformed": 1}
```

- [x] **Step 2: Verificar que fallan**

Run: `.venv/bin/pytest tests/test_sources.py -q`
Expected: FAIL (`ModuleNotFoundError`).

- [x] **Step 3: Implementar**

`src/eovrt_distribution/sources.py`:
```python
"""Fuentes de alertas: Direct (tests), JsonlReplay (DBE). Zmq vive en transport/."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator


@dataclass
class SourcedAlert:
    alert: dict
    ts_publish_ms: float | None = None


class DirectSource:
    def __init__(self, alerts: list[dict]) -> None:
        self._alerts = alerts
        self.stats = {"read": 0, "skipped_malformed": 0}

    def __iter__(self) -> Iterator[SourcedAlert]:
        for alert in self._alerts:
            self.stats["read"] += 1
            yield SourcedAlert(alert=alert)


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
```

- [x] **Step 4: Verificar que pasan**

Run: `.venv/bin/pytest tests/test_sources.py -q`
Expected: `2 passed`

---

### Task 7: Distributor — pipeline completo con summary

**Files:**
- Create: `src/eovrt_distribution/distributor.py`
- Test: `tests/test_distributor.py`

**Interfaces:**
- Consumes: `SourcedAlert`/fuentes (Task 6), `NotificationPolicy` (Task 4), `DeliveryLedger` (Task 3), `MqttChannel`/`SendResult` (Task 5), contratos (Task 2).
- Produces:
  - `Distributor(source, channel, policy, out_dir: Path, max_attempts: int = 3, retry_wait_ms: float = 500.0)`
  - `run() -> dict` — procesa toda la fuente y devuelve el summary (también escrito a `distribution_summary.json`).
  - Efectos en `out_dir`: `notifications.jsonl` (todo DeliveryRecord), `dead_letter.jsonl` (solo agotadas), `distribution_summary.json`.
  - Summary: `{"schema_version": "control.distribution_summary.v1", "control_run_id", "experiment_id", "channel", "mode", "counts": {<outcome>: int}, "source_stats": {...}, "talert_notification_ms": {"count", "min", "mean", "p95"} | None}`.
  - Orden del pipeline por alerta: envelope → **policy** (suppressed_cooldown) → **ledger** (skipped_duplicate) → **send con retry** (delivered | dead_letter). Registro de cada intento fallido intermedio como `failed`.

- [x] **Step 1: Tests que fallan**

`tests/test_distributor.py`:
```python
import json

from eovrt_distribution.channels.base import SendResult
from eovrt_distribution.channels.mqtt import MqttChannel
from eovrt_distribution.distributor import Distributor
from eovrt_distribution.ledger import DeliveryLedger
from eovrt_distribution.policy import NotificationPolicy
from eovrt_distribution.sources import DirectSource


class FailingChannel(MqttChannel):
    def __init__(self, fail_times: int) -> None:
        super().__init__(mode="dry_run")
        self._remaining = fail_times

    def send(self, env) -> SendResult:
        if self._remaining > 0:
            self._remaining -= 1
            return SendResult(ok=False, error="boom")
        return super().send(env)


def _run(tmp_path, alerts, channel=None, cooldown_ms=30000.0, max_attempts=3):
    dist = Distributor(
        source=DirectSource(alerts),
        channel=channel or MqttChannel(mode="dry_run"),
        policy=NotificationPolicy(cooldown_ms=cooldown_ms),
        out_dir=tmp_path,
        max_attempts=max_attempts,
        retry_wait_ms=0.0,
    )
    return dist.run()


def _outcomes(tmp_path):
    lines = (tmp_path / "notifications.jsonl").read_text().strip().splitlines()
    return [json.loads(line)["outcome"] for line in lines]


def test_happy_path_delivers_and_summarizes(tmp_path, make_alert):
    summary = _run(tmp_path, [make_alert(), make_alert(source_id="cam-02")])
    assert summary["counts"] == {"delivered": 2}
    assert summary["schema_version"] == "control.distribution_summary.v1"
    assert (tmp_path / "distribution_summary.json").exists()
    assert _outcomes(tmp_path) == ["delivered", "delivered"]


def test_burst_suppressed_by_cooldown_and_recorded(tmp_path, make_alert):
    alerts = [
        make_alert(timestamp_ms=1000.0),
        make_alert(timestamp_ms=2000.0, subject_key="otro"),
        make_alert(timestamp_ms=3000.0, subject_key="tercero"),
    ]
    summary = _run(tmp_path, alerts)
    assert summary["counts"] == {"delivered": 1, "suppressed_cooldown": 2}
    assert _outcomes(tmp_path) == [
        "delivered",
        "suppressed_cooldown",
        "suppressed_cooldown",
    ]


def test_rerun_is_fully_skipped_duplicate(tmp_path, make_alert):
    alerts = [make_alert(), make_alert(source_id="cam-02")]
    _run(tmp_path, alerts)
    summary = _run(tmp_path, alerts, cooldown_ms=0.0)
    assert summary["counts"] == {"skipped_duplicate": 2}


def test_retry_then_delivered(tmp_path, make_alert):
    summary = _run(tmp_path, [make_alert()], channel=FailingChannel(fail_times=2))
    assert summary["counts"] == {"failed": 2, "delivered": 1}
    assert _outcomes(tmp_path) == ["failed", "failed", "delivered"]


def test_exhausted_goes_to_dead_letter(tmp_path, make_alert):
    summary = _run(
        tmp_path, [make_alert()], channel=FailingChannel(fail_times=99), max_attempts=3
    )
    assert summary["counts"] == {"failed": 3, "dead_letter": 1}
    dead = (tmp_path / "dead_letter.jsonl").read_text().strip().splitlines()
    assert len(dead) == 1
    assert json.loads(dead[0])["outcome"] == "dead_letter"


def test_latency_mode_is_wall_clock_dbe_without_bus_timestamp(tmp_path, make_alert):
    _run(tmp_path, [make_alert()])
    row = json.loads((tmp_path / "notifications.jsonl").read_text().splitlines()[0])
    assert row["latency_mode"] == "wall_clock_dbe"
    assert row["talert_notification_ms"] is not None
```

- [x] **Step 2: Verificar que fallan**

Run: `.venv/bin/pytest tests/test_distributor.py -q`
Expected: FAIL (`ModuleNotFoundError`).

- [x] **Step 3: Implementar**

`src/eovrt_distribution/distributor.py`:
```python
"""Pipeline: source → policy → ledger → channel(retry) → records + summary."""
from __future__ import annotations

import json
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path

from eovrt_distribution.channels.mqtt import MqttChannel
from eovrt_distribution.contracts.delivery import DeliveryRecord
from eovrt_distribution.contracts.notification import NotificationEnvelope
from eovrt_distribution.ledger import DeliveryLedger
from eovrt_distribution.policy import NotificationPolicy


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _p95(values: list[float]) -> float:
    ordered = sorted(values)
    idx = max(0, round(0.95 * len(ordered)) - 1)
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
        counts: dict[str, int] = {}
        latencies: list[float] = []
        run_meta: dict = {}

        def record(rec: DeliveryRecord) -> None:
            ledger.append(rec)
            counts[rec.outcome] = counts.get(rec.outcome, 0) + 1
            if rec.talert_notification_ms is not None and rec.outcome == "delivered":
                latencies.append(rec.talert_notification_ms)
            if rec.outcome == "dead_letter":
                with dead_letter_path.open("a", encoding="utf-8") as fh:
                    fh.write(json.dumps(rec.model_dump(mode="json"), ensure_ascii=True))
                    fh.write("\n")

        for sourced in self.source:
            env = NotificationEnvelope.from_alert(
                sourced.alert, ts_publish_ms=sourced.ts_publish_ms
            )
            run_meta.setdefault("control_run_id", env.control_run_id)
            run_meta.setdefault("experiment_id", env.experiment_id)
            base = dict(
                control_run_id=env.control_run_id,
                notification_id=env.notification_id,
                alert_id=env.alert_id,
                channel=self.channel.name,
                mode=self.channel.mode,
                experiment_id=env.experiment_id,
            )

            if not self.policy.allow(env, now_wall_ms=time.time() * 1000.0):
                record(
                    DeliveryRecord(
                        **base, attempt=0, outcome="suppressed_cooldown",
                        attempted_at=_now_iso(),
                    )
                )
                continue

            if ledger.seen(env.notification_id, self.channel.name):
                record(
                    DeliveryRecord(
                        **base, attempt=0, outcome="skipped_duplicate",
                        attempted_at=_now_iso(),
                    )
                )
                continue

            delivered = False
            for attempt in range(1, self.max_attempts + 1):
                attempted_at = _now_iso()
                send_start_ms = time.time() * 1000.0
                result = self.channel.send(env)
                if result.ok:
                    end_ms = result.puback_wall_ms or time.time() * 1000.0
                    if env.confirmed_wall_ms is not None:
                        latency, mode = end_ms - env.confirmed_wall_ms, "live"
                    else:
                        latency, mode = end_ms - send_start_ms, "wall_clock_dbe"
                    record(
                        DeliveryRecord(
                            **base, attempt=attempt, outcome="delivered",
                            talert_notification_ms=latency, latency_mode=mode,
                            attempted_at=attempted_at, delivered_at=_now_iso(),
                        )
                    )
                    delivered = True
                    break
                record(
                    DeliveryRecord(
                        **base, attempt=attempt, outcome="failed",
                        error=result.error, attempted_at=attempted_at,
                    )
                )
                if attempt < self.max_attempts and self.retry_wait_ms > 0:
                    time.sleep(self.retry_wait_ms / 1000.0)

            if not delivered:
                record(
                    DeliveryRecord(
                        **base, attempt=self.max_attempts, outcome="dead_letter",
                        error="max_attempts exhausted", attempted_at=_now_iso(),
                    )
                )

        summary = {
            "schema_version": "control.distribution_summary.v1",
            "control_run_id": run_meta.get("control_run_id"),
            "experiment_id": run_meta.get("experiment_id"),
            "channel": self.channel.name,
            "mode": self.channel.mode,
            "counts": counts,
            "source_stats": dict(self.source.stats),
            "talert_notification_ms": (
                {
                    "count": len(latencies),
                    "min": min(latencies),
                    "mean": statistics.fmean(latencies),
                    "p95": _p95(latencies),
                }
                if latencies
                else None
            ),
        }
        (self.out_dir / "distribution_summary.json").write_text(
            json.dumps(summary, indent=2, ensure_ascii=True) + "\n"
        )
        return summary
```

- [x] **Step 4: Verificar que pasan (suite completa)**

Run: `.venv/bin/pytest -q`
Expected: todos verdes (`20 passed` aprox.).

---

### Task 8: Transport — decode de envelope y ZmqSource (live)

**Files:**
- Create: `src/eovrt_distribution/transport/envelope.py`, `src/eovrt_distribution/transport/zmq_source.py`
- Test: `tests/test_zmq_source.py`

**Interfaces:**
- Consumes: `SourcedAlert` (Task 6).
- Produces:
  - `decode_envelope(frames: list[bytes]) -> dict` — valida 2 frames y `schema_version == "bus.envelope.v1"`; devuelve `{"topic", "key", "seq", "ts_publish_ms", "payload" (dict ya parseado)}`. Levanta `ValueError` si malformado.
  - `ALERT_TOPIC_PREFIX = "control.alert.v1."`, `LIFECYCLE_TOPIC_PREFIX = "run.lifecycle.v1."`
  - `ZmqSource(endpoint: str, backfill_path: Path | None = None, recv_timeout_ms: int = 500, idle_timeout_ms: float | None = None)`:
    - `__iter__() -> Iterator[SourcedAlert]` — primero el backfill (dedupe posterior por `alert_id` contra el stream), después el stream hasta `run_finished` o `request_stop()`.
    - `request_stop() -> None` — parada cooperativa (flag; el socket se cierra en el MISMO hilo del recv-loop — trampa libzmq/SIGABRT).
    - `stats: dict` — `{"read", "skipped_malformed", "backfill_read", "bus_dropped_events", "duplicates_from_backfill"}`.
    - Reglas de `seq`: por topic, hueco = `bus_dropped_events += delta`; nunca silencioso.

- [x] **Step 1: Tests que fallan**

`tests/test_zmq_source.py` (publisher stub XPUB en el propio test, sobre `tcp://127.0.0.1:<puerto libre>`):
```python
import json
import threading
import time

import msgpack
import pytest
import zmq

from eovrt_distribution.transport.envelope import decode_envelope
from eovrt_distribution.transport.zmq_source import ZmqSource


def _envelope_frames(topic: str, seq: int, payload: dict, ts: float = 1750000000000.0):
    env = {
        "schema_version": "bus.envelope.v1",
        "topic": topic,
        "key": payload.get("source_id", ""),
        "seq": seq,
        "ts_publish_ms": ts,
        "payload": json.dumps(payload, ensure_ascii=True).encode(),
    }
    return [topic.encode(), msgpack.packb(env)]


def test_decode_envelope_roundtrip(make_alert):
    alert = make_alert()
    frames = _envelope_frames("control.alert.v1.cr-test", 0, alert)
    decoded = decode_envelope(frames)
    assert decoded["seq"] == 0
    assert decoded["ts_publish_ms"] == 1750000000000.0
    assert decoded["payload"]["alert_id"] == alert["alert_id"]


def test_decode_envelope_rejects_bad_schema():
    frames = [b"t", msgpack.packb({"schema_version": "otra.cosa"})]
    with pytest.raises(ValueError):
        decode_envelope(frames)


@pytest.fixture
def publisher():
    ctx = zmq.Context.instance()
    sock = ctx.socket(zmq.XPUB)
    port = sock.bind_to_random_port("tcp://127.0.0.1")
    yield sock, f"tcp://127.0.0.1:{port}"
    sock.close(0)


def _wait_subscriptions(sock, expected: int = 2) -> None:
    """El ZmqSource emite DOS suscripciones (alertas + lifecycle); publicar
    antes de que el XPUB procese ambas filtraría mensajes y colgaría el test."""
    for _ in range(expected):
        sock.recv()


def _run_source(source):
    got = []

    def consume():
        for item in source:
            got.append(item)

    thread = threading.Thread(target=consume)
    thread.start()
    return got, thread


def test_live_stream_until_run_finished(publisher, make_alert):
    sock, endpoint = publisher
    source = ZmqSource(endpoint=endpoint)
    got, thread = _run_source(source)
    _wait_subscriptions(sock)
    a1, a2 = make_alert(), make_alert()
    sock.send_multipart(_envelope_frames("control.alert.v1.cr-test", 0, a1))
    sock.send_multipart(_envelope_frames("control.alert.v1.cr-test", 1, a2))
    sock.send_multipart(
        _envelope_frames("run.lifecycle.v1.cr-test", 2, {"event": "run_finished"})
    )
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert [s.alert["alert_id"] for s in got] == [a1["alert_id"], a2["alert_id"]]
    assert got[0].ts_publish_ms == 1750000000000.0
    assert source.stats["bus_dropped_events"] == 0


def test_seq_gap_counted(publisher, make_alert):
    sock, endpoint = publisher
    source = ZmqSource(endpoint=endpoint)
    got, thread = _run_source(source)
    _wait_subscriptions(sock)
    sock.send_multipart(_envelope_frames("control.alert.v1.cr-test", 0, make_alert()))
    sock.send_multipart(_envelope_frames("control.alert.v1.cr-test", 3, make_alert()))
    sock.send_multipart(
        _envelope_frames("run.lifecycle.v1.cr-test", 4, {"event": "run_finished"})
    )
    thread.join(timeout=5)
    assert source.stats["bus_dropped_events"] == 2


def test_backfill_then_stream_dedupes_by_alert_id(publisher, tmp_path, make_alert):
    sock, endpoint = publisher
    early, late = make_alert(), make_alert()
    backfill = tmp_path / "alerts.jsonl"
    backfill.write_text(json.dumps(early) + "\n")
    source = ZmqSource(endpoint=endpoint, backfill_path=backfill)
    got, thread = _run_source(source)
    _wait_subscriptions(sock)
    sock.send_multipart(_envelope_frames("control.alert.v1.cr-test", 0, early))  # dup
    sock.send_multipart(_envelope_frames("control.alert.v1.cr-test", 1, late))
    sock.send_multipart(
        _envelope_frames("run.lifecycle.v1.cr-test", 2, {"event": "run_finished"})
    )
    thread.join(timeout=5)
    assert [s.alert["alert_id"] for s in got] == [early["alert_id"], late["alert_id"]]
    assert source.stats["duplicates_from_backfill"] == 1
    assert source.stats["backfill_read"] == 1


def test_request_stop_terminates_without_run_finished(publisher):
    sock, endpoint = publisher
    source = ZmqSource(endpoint=endpoint, recv_timeout_ms=50)
    got, thread = _run_source(source)
    _wait_subscriptions(sock)
    time.sleep(0.2)
    source.request_stop()
    thread.join(timeout=5)
    assert not thread.is_alive()
```

- [x] **Step 2: Verificar que fallan**

Run: `.venv/bin/pytest tests/test_zmq_source.py -q`
Expected: FAIL (`ModuleNotFoundError`).

- [x] **Step 3: Implementar**

`src/eovrt_distribution/transport/envelope.py`:
```python
"""bus.envelope.v1 — wire-compatible con el publisher del control-plane."""
from __future__ import annotations

import json

import msgpack

ALERT_TOPIC_PREFIX = "control.alert.v1."
LIFECYCLE_TOPIC_PREFIX = "run.lifecycle.v1."


def decode_envelope(frames: list[bytes]) -> dict:
    if len(frames) != 2:
        raise ValueError(f"esperados 2 frames, llegaron {len(frames)}")
    env = msgpack.unpackb(frames[1], raw=False)
    if not isinstance(env, dict) or env.get("schema_version") != "bus.envelope.v1":
        raise ValueError(f"envelope desconocido: {env!r:.120}")
    payload_raw = env["payload"]
    if isinstance(payload_raw, (bytes, bytearray)):
        payload = json.loads(payload_raw.decode("utf-8"))
    else:
        payload = json.loads(payload_raw)
    return {
        "topic": env["topic"],
        "key": env.get("key"),
        "seq": env["seq"],
        "ts_publish_ms": env.get("ts_publish_ms"),
        "payload": payload,
    }
```

`src/eovrt_distribution/transport/zmq_source.py`:
```python
"""ZmqSource: SUB al bus de alertas del control-plane, con backfill.

Parada cooperativa: request_stop() setea un flag; el recv-loop usa RCVTIMEO
y el socket SIEMPRE se crea y cierra en el hilo que itera — cerrar un socket
ZMQ desde otro hilo con un recv en curso aborta el proceso (SIGABRT).
"""
from __future__ import annotations

import threading
from pathlib import Path
from typing import Iterator

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
    ) -> None:
        self.endpoint = endpoint
        self.backfill_path = Path(backfill_path) if backfill_path else None
        self.recv_timeout_ms = recv_timeout_ms
        self.idle_timeout_ms = idle_timeout_ms
        self._stop = threading.Event()
        self.stats = {
            "read": 0,
            "skipped_malformed": 0,
            "backfill_read": 0,
            "bus_dropped_events": 0,
            "duplicates_from_backfill": 0,
        }

    def request_stop(self) -> None:
        self._stop.set()

    def __iter__(self) -> Iterator[SourcedAlert]:
        seen_alert_ids: set[str] = set()
        ctx = zmq.Context.instance()
        sock = ctx.socket(zmq.SUB)
        sock.setsockopt(zmq.RCVTIMEO, self.recv_timeout_ms)
        sock.connect(self.endpoint)
        sock.setsockopt_string(zmq.SUBSCRIBE, ALERT_TOPIC_PREFIX)
        sock.setsockopt_string(zmq.SUBSCRIBE, LIFECYCLE_TOPIC_PREFIX)
        try:
            if self.backfill_path and self.backfill_path.exists():
                replay = JsonlReplaySource(self.backfill_path)
                for sourced in replay:
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
                        break
                    continue
                idle_ms = 0.0
                try:
                    env = decode_envelope(frames)
                except (ValueError, KeyError):
                    self.stats["skipped_malformed"] += 1
                    continue
                topic, seq = env["topic"], env["seq"]
                prev = last_seq.get("_bus")
                if prev is not None and seq > prev + 1:
                    self.stats["bus_dropped_events"] += seq - prev - 1
                last_seq["_bus"] = seq
                if topic.startswith(LIFECYCLE_TOPIC_PREFIX):
                    if env["payload"].get("event") == "run_finished":
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
            sock.close(0)
```

- [x] **Step 4: Verificar que pasan (suite completa)**

Run: `.venv/bin/pytest -q`
Expected: todos verdes. Si `test_request_stop_terminates_without_run_finished` cuelga, el bug está en el manejo de `RCVTIMEO`/flag — no agregar cierre de socket desde otro hilo como "arreglo".

---

### Task 9: Config YAML + CLI (`eovrt-distribute replay|live`)

**Files:**
- Create: `src/eovrt_distribution/config.py`, `src/eovrt_distribution/cli.py`, `configs/example.yaml`
- Test: `tests/test_cli.py`

**Interfaces:**
- Consumes: todo lo anterior.
- Produces:
  - `DistributionConfig.load(path: Path | None) -> DistributionConfig` — defaults completos si `path` es None; campos: `notification_policy.cooldown_ms` (30000.0), `notification_policy.key` (`["condition_id","source_id"]`), `channel.mode` ("dry_run"), `channel.host/port/topic_prefix/qos`, `retry.max_attempts` (3), `retry.wait_ms` (500.0).
  - CLI `eovrt-distribute`:
    - `replay --alerts <path> --out-dir <dir> [--config <yaml>]`
    - `live --endpoint <tcp://...> --out-dir <dir> [--backfill <path>] [--config <yaml>] [--idle-timeout-ms <n>]`
    - Imprime el summary como JSON a stdout; exit 0 si terminó, 2 si la fuente no existe.

- [x] **Step 1: Tests que fallan**

`tests/test_cli.py`:
```python
import json

from eovrt_distribution.cli import main
from eovrt_distribution.config import DistributionConfig


def test_config_defaults():
    cfg = DistributionConfig.load(None)
    assert cfg.notification_policy.cooldown_ms == 30000.0
    assert cfg.channel.mode == "dry_run"
    assert cfg.retry.max_attempts == 3


def test_config_from_yaml(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text(
        "notification_policy:\n  cooldown_ms: 5000\nchannel:\n  mode: dry_run\n"
        "retry:\n  max_attempts: 2\n  wait_ms: 10\n"
    )
    cfg = DistributionConfig.load(p)
    assert cfg.notification_policy.cooldown_ms == 5000
    assert cfg.retry.max_attempts == 2


def test_cli_replay_end_to_end(tmp_path, make_alert, capsys):
    alerts = tmp_path / "alerts.jsonl"
    alerts.write_text(
        json.dumps(make_alert()) + "\n" + json.dumps(make_alert(source_id="cam-02")) + "\n"
    )
    out = tmp_path / "out"
    code = main(["replay", "--alerts", str(alerts), "--out-dir", str(out)])
    assert code == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["counts"] == {"delivered": 2}
    assert (out / "notifications.jsonl").exists()
    # re-ejecución: 100% skipped_duplicate (criterio de terminado del spec §7)
    code = main(["replay", "--alerts", str(alerts), "--out-dir", str(out)])
    assert code == 0
    summary2 = json.loads(capsys.readouterr().out)
    assert summary2["counts"] == {"skipped_duplicate": 2}


def test_cli_replay_missing_file_exits_2(tmp_path):
    assert main(["replay", "--alerts", str(tmp_path / "no.jsonl"), "--out-dir", str(tmp_path)]) == 2
```

- [x] **Step 2: Verificar que fallan**

Run: `.venv/bin/pytest tests/test_cli.py -q`
Expected: FAIL (`ModuleNotFoundError`).

- [x] **Step 3: Implementar**

`src/eovrt_distribution/config.py`:
```python
from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel


class PolicyConfig(BaseModel):
    cooldown_ms: float = 30000.0
    key: list[str] = ["condition_id", "source_id"]


class ChannelConfig(BaseModel):
    mode: str = "dry_run"
    host: str = "127.0.0.1"
    port: int = 1883
    topic_prefix: str = "eovrt/alerts"
    qos: int = 1


class RetryConfig(BaseModel):
    max_attempts: int = 3
    wait_ms: float = 500.0


class DistributionConfig(BaseModel):
    notification_policy: PolicyConfig = PolicyConfig()
    channel: ChannelConfig = ChannelConfig()
    retry: RetryConfig = RetryConfig()

    @classmethod
    def load(cls, path: Path | None) -> "DistributionConfig":
        if path is None:
            return cls()
        data = yaml.safe_load(Path(path).read_text()) or {}
        return cls.model_validate(data)
```

`src/eovrt_distribution/cli.py`:
```python
"""eovrt-distribute: replay (DBE) y live (EBE) sobre el mismo Distributor."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from eovrt_distribution.channels.mqtt import MqttChannel
from eovrt_distribution.config import DistributionConfig
from eovrt_distribution.distributor import Distributor
from eovrt_distribution.policy import NotificationPolicy
from eovrt_distribution.sources import JsonlReplaySource


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
    p_live.add_argument("--config", default=None)
    p_live.add_argument("--idle-timeout-ms", type=float, default=None)

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

        source = ZmqSource(
            endpoint=args.endpoint,
            backfill_path=Path(args.backfill) if args.backfill else None,
            idle_timeout_ms=args.idle_timeout_ms,
        )

    summary = _build(cfg, source, args.out_dir).run()
    print(json.dumps(summary, ensure_ascii=True))
    return 0
```

`configs/example.yaml`:
```yaml
# eovrt-distribute — config de ejemplo (sin credenciales: van por env)
notification_policy:
  cooldown_ms: 30000
  key: [condition_id, source_id]
channel:
  mode: dry_run          # live requiere extra [mqtt] y broker Mosquitto
  host: 127.0.0.1
  port: 1883
  topic_prefix: eovrt/alerts
  qos: 1
retry:
  max_attempts: 3
  wait_ms: 500
```

- [x] **Step 4: Verificar (suite completa + smoke del entry point)**

Run: `.venv/bin/pytest -q`
Expected: todos verdes.

Run: `.venv/bin/pip install -e . -q && .venv/bin/eovrt-distribute --help`
Expected: usage con subcomandos `replay` y `live`.

---

### Task 10: Canal MQTT live (paho) + dedup QoS 1

**Files:**
- Modify: `src/eovrt_distribution/channels/mqtt.py` (método `_send_live`)
- Test: agregar a `tests/test_channel_mqtt.py`; crear `tests/test_mqtt_live.py` (integration)

**Interfaces:**
- Consumes: `SendResult`, `ChannelError` (Task 5).
- Produces: `MqttChannel(mode="live")._send_live(topic, payload) -> SendResult` — conexión lazy (un cliente por canal, se conecta en el primer send), `publish(qos=1)` + `wait_for_publish(timeout=5)`; `puback_wall_ms` = wall-clock tras el PUBACK; errores → `SendResult(ok=False, error=...)` (el retry lo maneja el Distributor). Sin paho instalado → `ChannelError` con mensaje que nombra el extra `[mqtt]`.

- [x] **Step 1: Tests que fallan (unit, sin broker)**

Agregar a `tests/test_channel_mqtt.py`:
```python
def test_live_without_paho_raises_channel_error(monkeypatch, make_alert):
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name.startswith("paho"):
            raise ImportError("no paho")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    channel = MqttChannel(mode="live")
    env = NotificationEnvelope.from_alert(make_alert())
    import pytest as _pytest

    from eovrt_distribution.channels.base import ChannelError

    with _pytest.raises(ChannelError, match=r"\[mqtt\]"):
        channel.send(env)


def test_qos1_duplicate_delivery_deduped_by_ledger(tmp_path, make_alert):
    """QoS 1 puede duplicar: el mismo alert re-entregado debe terminar en
    skipped_duplicate por ledger (doc 07 D5.2) — criterio de terminado spec §7."""
    from eovrt_distribution.distributor import Distributor
    from eovrt_distribution.policy import NotificationPolicy
    from eovrt_distribution.sources import DirectSource

    alert = make_alert()
    dist = Distributor(
        source=DirectSource([alert, dict(alert)]),  # duplicado exacto
        channel=MqttChannel(mode="dry_run"),
        policy=NotificationPolicy(cooldown_ms=0.0),
        out_dir=tmp_path,
        retry_wait_ms=0.0,
    )
    summary = dist.run()
    assert summary["counts"] == {"delivered": 1, "skipped_duplicate": 1}
```

- [x] **Step 2: Verificar que fallan**

Run: `.venv/bin/pytest tests/test_channel_mqtt.py -q`
Expected: FAIL (`ChannelError` actual no menciona `[mqtt]` en live-path lazy; el test de dedup puede pasar ya — si pasa, confirmarlo y seguir).

- [x] **Step 3: Implementar `_send_live`**

Reemplazar `_send_live` en `src/eovrt_distribution/channels/mqtt.py`:
```python
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
                client = mqtt.Client(
                    callback_api_version=mqtt.CallbackAPIVersion.VERSION2
                )
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
        except Exception as exc:  # broker caído, DNS, auth: retry del Distributor
            return SendResult(ok=False, error=f"{type(exc).__name__}: {exc}")
```

- [x] **Step 4: Test de integración (broker real, marcado)**

`tests/test_mqtt_live.py`:
```python
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
```

- [x] **Step 5: Verificar**

Run: `.venv/bin/pytest -q`
Expected: todos verdes (los `integration` quedan deseleccionados por `addopts`).

Run (solo si hay broker local levantado; opcional en este task): `.venv/bin/pip install -e ".[mqtt]" -q && .venv/bin/pytest -m integration -q`
Expected: `1 passed` (o `skipped` si no hay paho/broker).

---

### Task 11: Validación end-to-end contra una corrida real (criterios de terminado)

**Files:**
- Ninguno nuevo — ejecución y verificación manual documentada.

**Interfaces:**
- Consumes: CLI completa (Task 9), corridas reales del control-plane en `/home/simonll4/projects/e-ovrt_control-plane/runs/` (o donde el usuario indique).

- [x] **Step 1: Localizar una corrida real con alertas**

Run: `ls /home/simonll4/projects/e-ovrt_control-plane/runs/*/alerts.jsonl 2>/dev/null | head -5`
Si no hay ninguna, pedirle al usuario una corrida (o generar una con el control-plane en replay según su CLAUDE.md) — no inventar datos.

- [x] **Step 2: Replay real (criterio §7.1 del spec)**

Run: `.venv/bin/eovrt-distribute replay --alerts <corrida>/alerts.jsonl --out-dir runs/validate-replay`
Expected: exit 0; `notifications.jsonl` con un registro por alerta (delivered/suppressed_cooldown según el contenido real); summary impreso.

Run de nuevo (mismo comando).
Expected: `counts` = 100% `skipped_duplicate` + los `suppressed_cooldown` que correspondan. Verificar con: `python3 -c "import json; s=json.load(open('runs/validate-replay/distribution_summary.json')); print(s['counts'])"`

- [x] **Step 3: Checklist de criterios de terminado del spec §7**

Marcar contra la evidencia:
- [x] Replay sobre corrida real + re-ejecución 100% `skipped_duplicate` (Step 2)
- [x] QoS 1 duplicado → dedup por ledger (test de Task 10)
- [x] Ráfaga misma condición-fuente → 1 notificación + `suppressed_cooldown` (tests de Tasks 4 y 7)
- [x] `experiment_id` en envelope y records (tests de Task 2; verificar en el JSONL real del Step 2)
- [x] Modo live EBE con backfill (tests de Task 8; el run EBE real requiere coordinación con el runner de experimental-setup — **fuera del alcance de este repo**, ver spec §8)
- [ ] `mosquitto_sub` en vivo + p95 en summary (requiere broker del compose de experimental-setup — coordinar con el usuario cuando toque la demo)

- [x] **Step 4: Suite final completa + lint**

Run: `.venv/bin/pytest -q && .venv/bin/ruff check src tests`
Expected: todo verde, sin findings.

*(Recordatorio final: el working tree queda listo; commits SOLO si el usuario los pide explícitamente.)*
