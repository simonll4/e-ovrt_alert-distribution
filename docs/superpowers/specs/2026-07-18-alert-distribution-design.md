# Diseño: e-ovrt_alert-distribution

- **Fecha:** 2026-07-18
- **Estado:** Aprobado (adapta spec 45, ya "Escrito" y respaldado por ADRs)
- **Fuente normativa:** este documento no redefine nada — traduce a plan de
  implementación lo que ya está decidido en el repo `docs/` (workspace-level):
  - `docs/specs/45-distribucion-alertas.md` — spec recortado, autoridad principal
  - `docs/decisiones/adr-005-distribucion-mqtt-repo-propio.md` — repo propio,
    canal único MQTT, recorte de alcance
  - `docs/decisiones/adr-011-frontera-politica-alertas.md` — cooldown vive en
    distribución, no en el motor del control-plane
  - `docs/nucleo/06-diseno-distribucion-alertas.md` — diseño completo original
    (anexo E-06); lo excluido de esta iteración queda documentado ahí
  - `docs/specs/40-plataforma-etapa4-integrador.md` — convenciones de envelope
    de bus y métrica `t_alert-notification`
  - `docs/specs/41-control-plane.md` §7-8 — publisher del lado control-plane
    (`control.alert.v1.*`, ya implementado en
    `e-ovrt_control-plane/src/eovrt_control/transport/alert_bus.py`)

Cualquier conflicto entre este documento y spec 45 se resuelve a favor de
spec 45, **excepto** donde este documento corrige supuestos del spec contra el
código real del control-plane (verificado 2026-07-18, rama
`feature/control-service`): campos del `AlertEvent`, semántica de emisión del
motor, y estado real de las integraciones cruzadas. Esas correcciones están
marcadas como **[corrección]**.

**Nota de gobernanza:** spec 45 declaraba este repo "local, sin remote
GitHub"; el repo se creó en `github.com/simonll4/e-ovrt_alert-distribution`
por decisión del usuario (2026-07-18), que actualiza esa regla para este repo.

## 1. Qué es (y qué no)

Consumidor desacoplado de alertas **ya confirmadas** por el `PatternEngine`
del control-plane. Las recibe del bus `control.alert.v1.*` (modo live) o de
`alerts.jsonl` (modo replay), aplica política de notificación (cooldown), las
entrega por **MQTT**, y registra intento/resultado.

**Nunca**: recalcula severidad, muta estado de patrón, ni crea alertas
(frontera estricta, ADR-011 — el repo separado materializa esta frontera en
la estructura misma, no solo en el código).

**Recorte vs. doc 06 (E-06)**, válido para esta iteración: un solo canal
(MQTT); sin Telegram/webhook; sin dashboard propio (la webconsole muestra
outcomes, spec 44 §8); retry mínimo (N intentos + registro, sin backoff);
dead-letter simple (archivo de agotadas, sin comando de reproceso).

## 2. Estructura del repo

```
e-ovrt_alert-distribution/
├── pyproject.toml                # paquete eovrt_distribution; extra [mqtt] → paho-mqtt
├── src/eovrt_distribution/
│   ├── transport/                # PUB/SUB ZeroMQ + envelope — wire-compatible con
│   │                             #   los planos (nace acá; extracción futura mecánica)
│   ├── contracts/
│   │   ├── notification.py       # NotificationEnvelope (control.notification.v1)
│   │   └── delivery.py           # DeliveryRecord (control.delivery.v1)
│   ├── sources.py                # AlertStreamSource: JsonlReplay | Zmq (backfill)
│   ├── ledger.py                 # idempotencia (notification_id, channel)
│   ├── channels/mqtt.py          # único canal; dry_run | live
│   ├── distributor.py            # source → ledger → canal → registros
│   └── cli.py                    # eovrt-distribute (replay y live)
├── configs/
└── tests/
```

## 3. Contratos

Entrada real: el `AlertEvent` implementado
(`e-ovrt_control-plane/src/eovrt_control/contracts/alerts.py`,
`control.alert.v1`). Campos relevantes verificados: `alert_id` (uuid5
determinista sobre `control_run_id:media_run_id:unit_id:pattern_id:subject_key`),
`condition_id`, `source_id`, `severity`, `subject_key`, `experiment_id`
(propagado end-to-end desde config/API — no hace falta otra fuente),
`timestamp_ms` (tiempo de media, nullable), `alert_registered_ms` y
`first_evidence_ms` (monotónicos, **process-local, no comparables entre
procesos**).

- `NotificationEnvelope` (doc 06 §6.1 + `experiment_id`, spec 40 §2, **más
  [corrección] `source_id`**: doc 06 no lo incluía pero la clave de cooldown
  lo exige; el `AlertEvent` lo trae). `notification_id` determinista desde
  `alert_id` — idempotencia por construcción, no por lookup.
- `DeliveryRecord` (doc 06 §6.2 + `experiment_id`); `channel` fijo `"mqtt"`
  en esta iteración. **[corrección]** El enum `outcome` de doc 06 se extiende
  con `"suppressed_cooldown"` (lo exige ADR-011/spec 45; doc 06 no lo lista).
- **[corrección] Medición de `talert_notification_ms`:** el `AlertEvent` NO
  tiene `confirmed_at_ms` (spec 45 asumía un campo inexistente). Mapeo
  definido: en modo **live**, desde `ts_publish_ms` del envelope de bus
  (único instante wall-clock de la confirmación disponible) hasta el PUBACK.
  En modo **replay**, la resta contra tiempos de la corrida original no es
  significativa: se mide wall-clock del proceso de distribución y se etiqueta
  `wall_clock_dbe` (spec 40 §5, fila `t_alert-notification`).

## 4. Fuentes y modos

| Modo | Fuente | Uso |
|---|---|---|
| `replay` | `JsonlReplaySource` sobre `runs/<id>/alerts.jsonl` del control-plane | DBE, post-run; idempotente re-ejecutable |
| `live` | `ZmqSource` suscripta al prefijo `control.alert.v1.` (publisher ya implementado, spec 41 §8.6), con **backfill** desde `alerts.jsonl` al conectar | EBE; mismo distribuidor |

Detalles verificados del publisher (`transport/alert_bus.py`): socket
**XPUB con bind** en `alert_bus.endpoint` (default `tcp://0.0.0.0:5558`,
`enabled: false` por default) — el consumidor hace connect+subscribe.
Envelope msgpack `bus.envelope.v1` en 2 frames `[topic, envelope]`; `payload`
= la línea JSONL exacta del `AlertEvent`; `key` = `source_id`; `seq`
monotónico por corrida (se consume aun en fallo de envío → huecos = drops).

**[corrección] Fin de corrida:** el `ZmqSource` debe suscribirse también a
`run.lifecycle.v1.<control_run_id>` — el run cierra con el sentinel
`run_finished` en ese topic; spec 45 no lo mencionaba y sin él el modo live
no termina.

Reglas de bus de spec 40 §3.2 aplican: suscripción antes del run (lo ordena
el runner, spec 44); huecos de `seq` se registran, nunca se silencian; el log
es la verdad — por eso el backfill en modo live.

## 5. Canal MQTT

- `paho-mqtt` como extra opcional; sin él, solo `dry_run` disponible.
- Publica `NotificationEnvelope` (JSON) a topic `eovrt/alerts/<severity>`;
  QoS 1 — **el ledger es obligatorio** porque QoS 1 puede duplicar entregas
  (doc 07 D5.2).
- `dry_run` (default): construye el payload y lo registra sin I/O real — es
  lo que cubre CI. `live`: contra broker Mosquitto (compose de la
  plataforma en `e-ovrt_experimental-setup`).
- Credenciales/host se resuelven por env/config; nunca quedan en artefactos
  versionados.

## 6. Política de notificación, ledger, retry, observabilidad

**Cooldown (ADR-011) — la política de notificación vive en distribución.**
**[corrección]** La premisa exacta, según el código real del motor
(`engine/pattern_engine.py`): el motor emite un `AlertEvent` por **transición
a `confirmed`** (una alerta por episodio; re-alerta solo tras
resolve→re-confirm), no "por cada confirmación" como decía spec 45. Además el
motor conserva un cooldown propio opcional (`realert_cooldown_ms`, clave
`(pattern_id, subject_key)`, **apagado por default**) — el cooldown de
distribución es una capa **adicional y más gruesa** (clave
`(condition_id, source_id)`, coalesce entre sujetos), no un reemplazo. Con
los defaults de la plataforma (motor sin cooldown), distribución es la única
supresión activa, que es el espíritu de ADR-011. El distribuidor decide
cuáles alertas se convierten en notificación real:

```yaml
notification_policy:
  cooldown_ms: 30000        # no re-notificar la misma clave dentro de la ventana
  key: [condition_id, source_id]   # clave de supresión (default; puede sumar subject_key)
```

Una alerta suprimida genera `DeliveryRecord(outcome="suppressed_cooldown")`
— trazable, contada en el summary, nunca silenciosa. Valor inicial 30s,
calibrable. La clave es `(condition, source)`, no el sujeto: para
notificación asistiva lo relevante es "esta condición en esta cámara ya fue
avisada".

**Ledger:** clave `(notification_id, channel)`; backing = `DeliveryRecord`
con `outcome=delivered` en `notifications.jsonl` (append-only). Re-ejecución
segura → `skipped_duplicate`. Dos capas distintas y ambas necesarias: el
ledger deduplica **exactos** (misma alerta reenviada), el cooldown suprime
**semánticos** (alertas distintas de la misma condición-fuente demasiado
seguidas).

**Retry:** `max_attempts` default 3, espera fija corta (valor exacto queda
abierto — se fija al implementar el canal, sin bloquear el resto). Agotado →
`outcome: dead_letter` + línea en `dead_letter.jsonl`. Sin reproceso
automático en esta iteración.

**Salidas por corrida:** `notifications.jsonl`, `dead_letter.jsonl`,
`distribution_summary.json` (conteos por outcome + agregados
`t_alert-notification` min/mean/p95). El generador de reporte (spec 44 §4)
los incorpora a `report.json`.

## 7. Orden de implementación y criterios de terminado

Orden: contratos + ledger + canal en `dry_run` con `DirectSource` de test →
`JsonlReplaySource` (DBE end-to-end contra una corrida real) → `transport/` +
`ZmqSource` (live) → Mosquitto en el compose + modo `live`.

- [ ] `distribute` en replay sobre una corrida real: `notifications.jsonl`
      completo, re-ejecución 100% `skipped_duplicate`.
- [ ] Modo live consumiendo el bus de alertas de una corrida EBE, con
      backfill verificado (alertas previas a la suscripción no se pierden).
- [ ] Entrega MQTT real observada con `mosquitto_sub`; `t_alert-notification`
      p95 en `distribution_summary.json`.
- [ ] QoS 1 duplicado simulado → deduplicado por ledger (test).
- [ ] Ráfaga de re-alertas de la misma condición-fuente → una sola
      notificación + `suppressed_cooldown` registrados (test de la política
      ADR-011).
- [ ] `experiment_id` presente en envelope y records; el `report.json` de una
      corrida con distribución lo muestra integrado (spec 44).

## 8. Interfaces con otros repos

- **`e-ovrt_control-plane` (spec 41):** consume su publisher
  `control.alert.v1.*`; nunca toca el motor de patrones.
- **`e-ovrt_experimental-setup` (spec 44):** compose del broker Mosquitto;
  webconsole muestra outcomes (fase 2 de la vista de alertas).
  **[corrección] Dos integraciones acá son trabajo FUTURO en ese repo, no
  comportamiento existente:** (a) el generador de reporte implementado (spec
  44 §4) aún no incorpora `distribution_summary.json` a `report.json`
  (anticipado en ADR-014); (b) el runner live hoy espera **1** suscriptor
  (`wait_for_subscriber`, deuda C del doc 51) — sumar el distribuidor exige
  extender el ordenamiento subscribe-antes-del-run y el conteo esperado.
  Ninguna de las dos bloquea este repo (replay y dry_run no las necesitan),
  pero el modo live end-to-end sí depende de (b).
- **doc 06:** todo lo excluido de esta iteración (canales extra, backoff,
  dashboard propio, reproceso de dead-letter) queda diseñado ahí como anexo
  (E-06) — su incorporación futura no altera la semántica de la alerta
  (DA-13).

## 9. Puntos abiertos (no bloquean el plan de implementación)

- `summary_text` del envelope: config-driven vs. hardcoded — doc 06 lo deja
  abierto; se resuelve al implementar `contracts/notification.py`.
- Valor exacto de la espera fija de retry — se fija al implementar
  `channels/mqtt.py`.
- Comando de reproceso de dead-letter: explícitamente diferido a una
  iteración futura, no entra en el alcance de este repo por ahora.
- Interacción de las dos capas de cooldown si algún pattern set futuro activa
  `realert_cooldown_ms` en el motor: hoy es teórica (default apagado); si se
  activa, documentar qué capa suprime qué antes de calibrar.
- El control-plane consumido está en la rama `feature/control-service` (2
  commits sin pushear al 2026-07-18) — al integrar en vivo, verificar contra
  qué rama/estado se corre.
