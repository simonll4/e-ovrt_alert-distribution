# Documentación integral del servicio de distribución — diseño

Fecha: 2026-08-13  
Repositorio: `e-ovrt_alert-distribution`  
Estado: aprobado para implementación

## 1. Objetivo

Crear en el propio repositorio una documentación técnica vigente que explique el propósito del
servicio de distribución dentro de E-OVRT, la lógica que gobierna cada alerta, su arquitectura y
sus fronteras con el resto de la plataforma. El lector debe poder comprender el sistema sin leer
primero el código ni reconstruir el estado actual a partir de especificaciones históricas.

La documentación se dirige principalmente a quienes diseñan, mantienen, integran o evalúan la
plataforma. Los comandos de instalación y operación aparecerán solo cuando ayuden a explicar una
interfaz o verificar el flujo; no constituirán el eje del conjunto.

## 2. Principios editoriales

- Describir únicamente comportamiento vigente y verificable en código o pruebas.
- Separar responsabilidades conceptuales de detalles de implementación.
- Explicar primero el lugar del servicio en la plataforma y luego profundizar en sus componentes.
- Distinguir explícitamente los modos `replay` y `live`, sin presentarlos como dos servicios.
- Diferenciar deduplicación exacta, supresión semántica por cooldown y retry de entrega.
- Exponer las garantías y los límites deliberados del servicio.
- Mantener las specs bajo `docs/superpowers/` como trazabilidad histórica, no como manual vigente.
- Usar diagramas Mermaid pequeños y texto Markdown versionable.
- Evitar duplicar bloques extensos: cada concepto tendrá un documento canónico y enlaces desde los
  demás.

## 3. Arquitectura de la documentación

`docs/README.md` será la portada y el recorrido recomendado. Enlazará los siguientes documentos:

1. `01-proposito-y-alcance.md`: función en E-OVRT, responsabilidades, entradas, salidas y límites.
2. `02-arquitectura.md`: contexto, componentes internos, dependencias y despliegue como proceso del
   host por experimento.
3. `03-logica-de-distribucion.md`: recorrido de una alerta y orden de decisiones del pipeline.
4. `04-contratos-y-artefactos.md`: schemas, topics, archivos persistidos, outcomes y métricas.
5. `05-integracion-con-la-plataforma.md`: relación con control-plane, runner, broker y webconsole;
   secuencias DBE y EBE.
6. `06-decisiones-y-limitaciones.md`: garantías, trade-offs, fallos visibles y capacidades fuera de
   alcance.
7. `glosario.md`: vocabulario y nombres de contrato usados en el conjunto.

El `README.md` de la raíz seguirá siendo la entrada rápida del paquete y enlazará `docs/README.md`
para la explicación completa.

## 4. Contenido por documento

### 4.1 Propósito y alcance

Definirá al distribuidor como consumidor desacoplado de alertas ya confirmadas por el
control-plane. Aclarará que transforma una alerta confirmada en un intento de notificación, aplica
política de entrega y conserva evidencia del resultado. También dejará explícito que no detecta,
no evalúa patrones, no confirma alertas, no recalcula severidad y no modifica el estado del motor.

Incluirá un diagrama de contexto que muestre:

`media-plane → control-plane → alert-distribution → MQTT → consumidor externo`, con el runner
coordinando la corrida y la webconsole leyendo artefactos consolidados.

### 4.2 Arquitectura

Describirá unidades con responsabilidades independientes:

- fuentes `JsonlReplaySource` y `ZmqSource`;
- adaptación `control.alert.v1 → control.notification.v1`;
- `NotificationPolicy` para cooldown;
- `DeliveryLedger` para idempotencia;
- `MqttChannel` para entrega `dry_run` o `live`;
- `Distributor` como orquestador del pipeline;
- CLI como frontera de proceso.

El documento explicará que el servicio se ejecuta como proceso efímero del host, uno por
experimento, y que MQTT es una dependencia externa. No describirá una imagen Docker ni un daemon
persistente porque no forman parte de la arquitectura vigente.

### 4.3 Lógica de distribución

Presentará el orden exacto de evaluación:

1. leer alerta desde replay, backfill o bus;
2. validar y crear el `NotificationEnvelope`;
3. omitir entradas inválidas y contabilizarlas;
4. comprobar idempotencia por `(notification_id, channel)`;
5. evaluar cooldown por la clave configurada;
6. intentar publicación hasta `max_attempts`;
7. registrar cada intento fallido;
8. registrar entrega y consumir el cooldown solo después del éxito;
9. producir dead letter al agotar intentos;
10. cerrar el canal y escribir el summary.

Explicará por separado:

- `notification_id` determinista derivado de `alert_id`;
- diferencia entre `skipped_duplicate` y `suppressed_cooldown`;
- por qué un fallo no consume la ventana de cooldown;
- compactación del ledger entre ejecuciones;
- deduplicación entre backfill y eventos live;
- detección de huecos de secuencia;
- finalización por `run_finished`, timeout o cancelación;
- medición `live` frente a `wall_clock_dbe`.

### 4.4 Contratos y artefactos

Documentará mediante tablas los campos de:

- entrada `control.alert.v1`;
- transporte `bus.envelope.v1`;
- salida MQTT `control.notification.v1`;
- registro `control.delivery.v1`;
- resumen `control.distribution_summary.v1`.

Cada campo se clasificará como obligatorio, opcional o derivado. Se describirán todos los outcomes
vigentes: `delivered`, `failed`, `skipped_duplicate`, `suppressed_cooldown` y `dead_letter`.

Los artefactos canónicos serán:

- `notifications.jsonl`, ledger e historial de la ejecución;
- `dead_letter.jsonl`, agotadas de la ejecución actual;
- `distribution_summary.json`, conteos, estadísticas de fuente y latencias.

Los ejemplos JSON serán mínimos, válidos y coherentes con los modelos Pydantic actuales.

### 4.5 Integración con la plataforma

Mostrará dos secuencias:

- DBE/replay: control termina, se consolida `alerts.jsonl`, se ejecuta distribución y se genera el
  reporte.
- EBE/live: el runner inicia control y el suscriptor de alertas antes de media; el distribuidor
  procesa backfill y bus hasta el fin de corrida.

Explicará la separación entre el bus media→control (`:5557`) y el bus control→distribución
(`:5558`), el topic MQTT por severidad, la inclusión de los artefactos en `report.json` y la
representación de outcomes en la webconsole.

### 4.6 Decisiones y limitaciones

Separará garantías verificables de aquello que no se ofrece:

- entrega MQTT QoS 1 con idempotencia local, no exactly-once distribuido;
- retry fijo, no backoff exponencial;
- un solo canal MQTT;
- dead-letter auditable, sin comando automático de reproceso;
- ledger local por directorio de salida, sin coordinación entre procesos concurrentes;
- cooldown en memoria durante una ejecución;
- sin dashboard propio, persistencia central ni daemon;
- credenciales solo por variables de entorno y broker ligado a loopback en el despliegue local.

### 4.7 Glosario

Definirá DBE, EBE, alerta confirmada, notificación, outcome, ledger, backfill, cooldown, PUBACK,
dead letter, QoS, source, envelope y run ID. Cada entrada enlazará al documento donde se explica su
comportamiento.

## 5. Diagramas

Se incluirán únicamente diagramas que reduzcan ambigüedad:

- contexto de plataforma en `01-proposito-y-alcance.md`;
- componentes y dependencias en `02-arquitectura.md`;
- flujo de decisión en `03-logica-de-distribucion.md`;
- secuencias replay y live en `05-integracion-con-la-plataforma.md`.

Los diagramas no repetirán tablas de contratos ni árboles de archivos que se entiendan mejor como
texto.

## 6. Verificación y criterio de terminado

- Todos los nombres, valores por defecto y reglas se contrastan con `src/`, `configs/example.yaml`
  y `pyproject.toml`.
- Los ejemplos JSON validan contra los modelos Pydantic o reproducen salidas reales de test.
- Los comandos publicados responden correctamente con el CLI instalado.
- Todos los enlaces Markdown relativos resuelven a archivos existentes.
- No quedan marcadores pendientes ni afirmaciones de funcionalidad futura como si estuviera
  implementada.
- `README.md` de la raíz enlaza la nueva portada sin duplicar la documentación conceptual.
- La suite, Ruff y `git diff --check` permanecen limpios después de los cambios documentales.

## 7. Fuera de alcance

Este trabajo no modifica código, contratos, configuración ni comportamiento. Tampoco agrega una
guía genérica de MQTT, manual de Docker, tutorial de despliegue distribuido, nuevos ADRs ni
documentación de componentes que pertenecen a otros repositorios. Esos componentes se describen
solo en la medida necesaria para explicar las fronteras del distribuidor.
