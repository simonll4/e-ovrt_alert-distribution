# Contratos y artefactos

## Cadena contractual

El servicio preserva la identidad de la alerta a través de contratos versionados:

```text
control.alert.v1
  └─ opcionalmente dentro de bus.envelope.v1
       ↓ NotificationEnvelope.from_alert
control.notification.v1
       ↓ intento de canal
control.delivery.v1 (uno o más)
       ↓ agregación por corrida
control.distribution_summary.v1
```

Los contratos Pydantic rechazan números no finitos en campos temporales. La configuración también
es estricta: claves desconocidas producen un error en vez de ignorarse silenciosamente.

## Entrada: `control.alert.v1`

El adaptador permite campos adicionales del productor, pero consume y valida los siguientes:

| Campo | Condición | Uso en distribución |
|---|---|---|
| `schema_version` | obligatorio, literal `control.alert.v1` | selección del contrato |
| `event_type` | obligatorio, literal `alert_event` | tipo de evento |
| `control_run_id` | obligatorio | trazabilidad de corrida |
| `media_run_id` | obligatorio | trazabilidad de percepción |
| `alert_id` | obligatorio | origen de `notification_id` e idempotencia |
| `pattern_id` | obligatorio | contexto de la alerta |
| `condition_id` | obligatorio | texto y clave de cooldown |
| `source_id` | obligatorio | texto, topic de bus y clave de cooldown |
| `subject_key` | obligatorio | texto y clave configurable de cooldown |
| `severity` | obligatorio | texto y sufijo del topic MQTT |
| `state` | opcional, default `open` | se copia como `episode_state` |
| `timestamp_ms` | opcional, finito | tiempo de media para cooldown |
| `evidence` | opcional, objeto | se copia como `evidence_ref` |
| `experiment_id` | opcional | correlación end-to-end |

Ejemplo mínimo válido:

```json
{
  "schema_version": "control.alert.v1",
  "event_type": "alert_event",
  "control_run_id": "ctrl-001",
  "media_run_id": "media-001",
  "alert_id": "alert-001",
  "pattern_id": "driver_distraction",
  "condition_id": "phone_use",
  "source_id": "camera-cabin",
  "subject_key": "driver",
  "severity": "high"
}
```

## Transporte live: `bus.envelope.v1`

ZeroMQ envía dos frames:

1. topic UTF-8;
2. objeto MessagePack `bus.envelope.v1`.

| Campo del envelope | Regla |
|---|---|
| `schema_version` | literal `bus.envelope.v1` |
| `topic` | string idéntico al primer frame |
| `key` | opcional; el publisher usa `source_id` |
| `seq` | entero no negativo y monotónico por corrida |
| `ts_publish_ms` | número finito o `null` |
| `payload` | JSON serializado como bytes o string; debe decodificar a objeto |

Los topics relevantes son:

- `control.alert.v1.<control_run_id>`: contiene la alerta;
- `run.lifecycle.v1.<control_run_id>`: contiene el sentinel `run_finished`.

Un envelope inválido no llega al núcleo: se cuenta como fuente malformada. Un salto de `seq` válido
se registra en `bus_dropped_events`.

## Salida de canal: `control.notification.v1`

| Campo | Origen |
|---|---|
| `schema_version` | literal `control.notification.v1` |
| `event_type` | literal `notification_envelope` |
| `notification_id` | SHA-1 truncado de `alert_id` |
| `control_run_id` | alerta |
| `media_run_id` | alerta |
| `alert_id` | alerta |
| `pattern_id` | alerta |
| `condition_id` | alerta |
| `source_id` | alerta |
| `subject_key` | alerta |
| `severity` | alerta |
| `episode_state` | `state` de la alerta |
| `media_timestamp_ms` | `timestamp_ms` de la alerta o `null` |
| `confirmed_wall_ms` | `ts_publish_ms` en bus live; `null` en replay/backfill |
| `evidence_ref` | `evidence` de la alerta o `null` |
| `summary_text` | texto derivado de severidad, condición, fuente y sujeto |
| `experiment_id` | alerta o `null` |

Ejemplo de payload MQTT:

```json
{
  "schema_version": "control.notification.v1",
  "event_type": "notification_envelope",
  "notification_id": "8a26d4f32142451a",
  "control_run_id": "ctrl-001",
  "media_run_id": "media-001",
  "alert_id": "alert-001",
  "pattern_id": "driver_distraction",
  "condition_id": "phone_use",
  "source_id": "camera-cabin",
  "subject_key": "driver",
  "severity": "high",
  "episode_state": "open",
  "media_timestamp_ms": null,
  "confirmed_wall_ms": null,
  "evidence_ref": null,
  "summary_text": "[high] phone_use confirmada en camera-cabin (sujeto driver)",
  "experiment_id": null
}
```

El `notification_id` del ejemplo es la derivación real de `alert-001`. El payload se publica en
`<topic_prefix>/high`.

## Registro: `control.delivery.v1`

Cada decisión o intento genera un registro independiente.

| Campo | Regla |
|---|---|
| `schema_version` | literal `control.delivery.v1` |
| `event_type` | literal `delivery_record` |
| `control_run_id` | corrida de la alerta |
| `notification_id` | identidad de notificación |
| `alert_id` | identidad de alerta para reporte |
| `channel` | `mqtt` en esta versión |
| `mode` | `dry_run` o `live` |
| `attempt` | 0 para decisiones sin envío; 1..N para intentos |
| `outcome` | enum documentado abajo |
| `error` | causa del fallo o `null` |
| `talert_notification_ms` | latencia solo para `delivered` |
| `latency_mode` | `live`, `wall_clock_dbe` o `null` |
| `attempted_at` | timestamp UTC ISO 8601 |
| `delivered_at` | timestamp UTC ISO 8601 solo en entrega |
| `experiment_id` | correlación opcional |

```json
{
  "schema_version": "control.delivery.v1",
  "event_type": "delivery_record",
  "control_run_id": "ctrl-001",
  "notification_id": "8a26d4f32142451a",
  "alert_id": "alert-001",
  "channel": "mqtt",
  "mode": "live",
  "attempt": 1,
  "outcome": "delivered",
  "error": null,
  "talert_notification_ms": 12.4,
  "latency_mode": "live",
  "attempted_at": "2026-08-13T10:00:00Z",
  "delivered_at": "2026-08-13T10:00:00.012400Z",
  "experiment_id": "exp_20260813T100000Z_demo"
}
```

### Outcomes

| Outcome | Significado | ¿Hubo intento MQTT? | ¿Marca idempotencia? |
|---|---|---:|---:|
| `delivered` | el dry-run terminó o MQTT confirmó publicación | sí, salvo I/O simulado | sí |
| `failed` | un intento no pudo entregar | sí | no |
| `skipped_duplicate` | ya existía un `delivered` para la misma clave | no | no; reutiliza la marca existente |
| `suppressed_cooldown` | la clave semántica estaba dentro de la ventana | no | no |
| `dead_letter` | se agotaron todos los intentos | sí, en registros `failed` previos | no |

Una alerta agotada produce N registros `failed` y luego un `dead_letter`; este último no sustituye
el detalle de los intentos.

## Resumen: `control.distribution_summary.v1`

El summary describe una ejecución completa:

| Campo | Contenido |
|---|---|
| `schema_version` | literal `control.distribution_summary.v1` |
| `control_run_id` | primera corrida observada o `null` si no hubo alerta válida |
| `experiment_id` | primer experimento observado o `null` |
| `channel` | `mqtt` |
| `mode` | `dry_run` o `live` del canal |
| `counts` | cantidad de records por outcome; solo incluye outcomes observados |
| `skipped_invalid_alerts` | JSON válidos que no cumplieron el contrato de alerta |
| `source_stats` | estadísticas específicas de la fuente |
| `talert_notification_ms` | agregados por modo de latencia o `null` |

```json
{
  "schema_version": "control.distribution_summary.v1",
  "control_run_id": "ctrl-001",
  "experiment_id": "exp_20260813T100000Z_demo",
  "channel": "mqtt",
  "mode": "live",
  "counts": {
    "delivered": 3,
    "suppressed_cooldown": 1
  },
  "skipped_invalid_alerts": 0,
  "source_stats": {
    "read": 4,
    "skipped_malformed": 0,
    "backfill_read": 0,
    "bus_dropped_events": 0,
    "duplicates_from_backfill": 0,
    "termination_reason": "run_finished"
  },
  "talert_notification_ms": {
    "live": {
      "count": 3,
      "min": 8.2,
      "mean": 10.6,
      "p95": 12.4
    }
  }
}
```

`JsonlReplaySource` solo informa `read` y `skipped_malformed`. `ZmqSource` agrega las estadísticas de
backfill, gaps y terminación mostradas en el ejemplo.

## Artefactos persistidos

El directorio `--out-dir` contiene:

### `notifications.jsonl`

Ledger de idempotencia e historial de la ejecución. Cada línea es un `control.delivery.v1`. Dentro
de una ejecución se agrega en orden de decisión; al reabrir el directorio la generación previa se
**archiva íntegra** como `notifications.<n>.jsonl` y los nuevos outcomes se escriben en un archivo
vigente nuevo. Ninguna fila se borra: no hay compactación.

La rehidratación de las claves ya entregadas recorre **todas** las generaciones del directorio, de
modo que un `delivered` de cualquier corrida previa sigue vigente para la deduplicación.

El reporte usa la última línea de cada `alert_id` como outcome terminal para la tabla de alertas, y
lee `notifications.jsonl` por nombre exacto: las generaciones archivadas no entran al reporte.

### `dead_letter.jsonl`

Vista especializada de los registros `dead_letter` de la ejecución actual. Solo se crea si existe al
menos una agotada. Al iniciar una ejecución sobre un directorio ya usado, la versión previa se
archiva como `dead_letter.<n>.jsonl` en vez de eliminarse. No es una cola con consumidor ni una
orden de reproceso.

### `distribution_summary.json`

Objeto JSON indentado con el resumen. Se escribe al final de un recorrido normal; la CLI imprime
el mismo objeto en una línea por stdout. El runner valida schema, forma, valores no negativos y
finitud de métricas antes de considerar exitosa la distribución. La verificación de equivalencia
exacta entre stdout y archivo existe sólo en el camino por CLI (fallback subprocess): en el camino
HTTP por default (ADR-020) no hay stdout — el runner pollea `GET /api/runs/{id}` y valida el
summary que sirve el servicio, que es el mismo objeto persistido.

## Ubicación consolidada

En una corrida orquestada, los tres artefactos viven en:

```text
runs/exp_<id>/distribution/
├── notifications.jsonl
├── notifications.1.jsonl      # solo si el directorio se reutilizó (generación previa)
├── dead_letter.jsonl          # solo si hubo agotadas
└── distribution_summary.json
```

La carpeta es hermana de `media/`, `control/` y `report/`; no pertenece al directorio interno de la
corrida del control-plane.

## Siguiente lectura

- [Integración con runner y webconsole](05-integracion-con-la-plataforma.md)
- [Decisiones y límites de las garantías](06-decisiones-y-limitaciones.md)
