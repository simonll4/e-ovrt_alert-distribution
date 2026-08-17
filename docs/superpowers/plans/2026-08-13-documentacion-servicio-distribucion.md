# Documentación integral del servicio de distribución Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publicar una documentación técnica autocontenida que explique el propósito, la arquitectura, la lógica y las fronteras del servicio de distribución de alertas en E-OVRT.

**Architecture:** `docs/README.md` será la portada de un conjunto conceptual dividido por responsabilidad. Cada documento tendrá una fuente canónica clara, diagramas Mermaid solo donde aporten relaciones o secuencias y enlaces relativos entre conceptos, mientras `README.md` de la raíz conservará el arranque rápido.

**Tech Stack:** Markdown CommonMark/GFM, Mermaid, Python 3.11, Pydantic 2, ZeroMQ, MessagePack y MQTT QoS 1.

## Global Constraints

- Documentar únicamente el comportamiento vigente verificado en código, configuración o pruebas.
- No modificar código, contratos, configuración ni comportamiento del servicio.
- No presentar Docker ni un daemon persistente como parte de la arquitectura vigente.
- Distinguir siempre replay/DBE de live/EBE y deduplicación de cooldown.
- Mantener `docs/superpowers/` como trazabilidad de diseño y no como documentación primaria.
- No tocar ni versionar `notas.txt`.

---

### Task 1: Portada, propósito y alcance

**Files:**
- Create: `docs/README.md`
- Create: `docs/01-proposito-y-alcance.md`
- Modify: `README.md`

**Interfaces:**
- Consumes: estructura aprobada en `docs/superpowers/specs/2026-08-13-documentacion-servicio-distribucion-design.md`.
- Produces: recorrido de lectura y definición canónica de la frontera del servicio.

- [x] **Step 1: Crear el índice documental**

Escribir una portada con audiencia, mapa de documentos, recorrido recomendado y fuentes normativas.

- [x] **Step 2: Documentar propósito y frontera**

Explicar la transformación `alerta confirmada → notificación`, responsabilidades y exclusiones, e incluir un diagrama Mermaid de contexto.

- [x] **Step 3: Enlazar desde la raíz**

Agregar a `README.md` un enlace visible a `docs/README.md` sin duplicar el contenido conceptual.

- [x] **Step 4: Verificar enlaces iniciales**

Run: `.venv/bin/python -c "from pathlib import Path; assert Path('docs/README.md').is_file(); assert Path('docs/01-proposito-y-alcance.md').is_file()"`

Expected: exit code 0.

### Task 2: Arquitectura y lógica

**Files:**
- Create: `docs/02-arquitectura.md`
- Create: `docs/03-logica-de-distribucion.md`

**Interfaces:**
- Consumes: `src/eovrt_distribution/{cli,config,distributor,ledger,policy,sources}.py`, `channels/` y `transport/`.
- Produces: modelo mental de componentes y flujo de decisiones.

- [x] **Step 1: Documentar arquitectura**

Describir CLI, fuentes, adaptación, política, ledger, canal y orquestador; incluir diagrama Mermaid de componentes y el modelo de proceso efímero por experimento.

- [x] **Step 2: Documentar el pipeline**

Describir en orden validación, idempotencia, cooldown, retry, entrega, dead letter, cierre y summary con un diagrama de flujo.

- [x] **Step 3: Documentar propiedades transversales**

Separar idempotencia exacta, supresión semántica, backfill, gaps de secuencia, terminación y métricas de latencia.

- [x] **Step 4: Contrastar símbolos documentados**

Run: `rg -n "class (Distributor|DeliveryLedger|NotificationPolicy|JsonlReplaySource|ZmqSource|MqttChannel)" src/eovrt_distribution`

Expected: todos los componentes nombrados aparecen en la implementación.

### Task 3: Contratos, artefactos e integración

**Files:**
- Create: `docs/04-contratos-y-artefactos.md`
- Create: `docs/05-integracion-con-la-plataforma.md`

**Interfaces:**
- Consumes: modelos `NotificationEnvelope`, `DeliveryRecord`, decodificador `bus.envelope.v1`, CLI y runner del repositorio hermano.
- Produces: referencia de schemas y secuencias DBE/EBE.

- [x] **Step 1: Documentar contratos mediante tablas**

Enumerar campos obligatorios, opcionales y derivados de entrada, transporte, notificación, delivery y summary.

- [x] **Step 2: Agregar ejemplos válidos**

Incluir ejemplos mínimos JSON de `control.alert.v1`, `control.notification.v1`, `control.delivery.v1` y `control.distribution_summary.v1`.

- [x] **Step 3: Documentar artefactos**

Explicar ciclo de vida, compactación y semántica de `notifications.jsonl`, `dead_letter.jsonl` y `distribution_summary.json`.

- [x] **Step 4: Documentar integración**

Explicar buses `:5557`/`:5558`, broker MQTT, runner, consolidación y webconsole; incluir secuencias Mermaid replay y live.

- [x] **Step 5: Validar ejemplos contra modelos**

Run: `.venv/bin/python -m pytest -q tests/test_contracts.py tests/test_cli.py`

Expected: contract and CLI tests pass.

### Task 4: Decisiones, limitaciones y glosario

**Files:**
- Create: `docs/06-decisiones-y-limitaciones.md`
- Create: `docs/glosario.md`

**Interfaces:**
- Consumes: garantías y límites observados en código y tests.
- Produces: interpretación precisa de lo que el servicio garantiza y de lo que queda fuera de alcance.

- [x] **Step 1: Documentar decisiones y trade-offs**

Cubrir QoS 1 más ledger local, retry fijo, cooldown en memoria, un canal, proceso por corrida y seguridad local.

- [x] **Step 2: Documentar limitaciones**

Aclarar ausencia de exactly-once distribuido, coordinación multi-proceso, reproceso automático, dashboard propio, canales alternativos y persistencia central.

- [x] **Step 3: Crear glosario enlazado**

Definir DBE, EBE, envelope, alert, notification, outcome, ledger, backfill, cooldown, PUBACK, dead letter, QoS y run ID.

### Task 5: Revisión y verificación integral

**Files:**
- Modify: todos los documentos creados en Tasks 1-4 si la revisión detecta inconsistencias.

**Interfaces:**
- Consumes: conjunto documental completo y árbol actual del repositorio.
- Produces: documentación coherente, navegable y validada.

- [x] **Step 1: Buscar placeholders y lenguaje obsoleto**

Run: `rg -n -i "TBD|TODO|implement later|fill in|por definir|imagen Docker propia|daemon persistente" docs/*.md README.md`

Expected: no placeholders; cualquier mención de Docker o daemon los identifica explícitamente como ausentes.

- [x] **Step 2: Validar enlaces Markdown relativos**

Ejecutar un script de solo lectura que extraiga enlaces locales de `README.md` y `docs/*.md` y compruebe que sus destinos existan.

- [x] **Step 3: Validar implementación**

Run: `.venv/bin/python -m pytest -q`

Expected: suite unitaria completa pasa; la integración externa permanece excluida por configuración de Pytest.

- [x] **Step 4: Validar estilo y diff**

Run: `.venv/bin/ruff check src tests && git diff --check && git status --short`

Expected: Ruff y diff limpios; solo aparecen los documentos previstos y el `notas.txt` preexistente.
