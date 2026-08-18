# Decisiones y limitaciones

## Garantías vigentes

### Frontera de dominio

El distribuidor solo consume alertas confirmadas. La validación contractual puede rechazar una
entrada, pero no corrige ni reinterpreta el resultado del control-plane. Esta frontera mantiene la
política de notificación separada de la política que confirma patrones.

### Mismo núcleo para replay y live

Los modos cambian la fuente y el origen temporal de la latencia, no las reglas de distribución.
Ambos usan el mismo adaptador, cooldown, ledger, retry, canal y formatos de salida. Esto reduce la
divergencia entre evaluación DBE y ejecución EBE.

### Evidencia por decisión e intento

Las supresiones y los duplicados no desaparecen: generan records con `attempt: 0`. Cada retry
fallido también se registra. El último outcome permite una vista resumida por alerta, mientras el
JSONL conserva la secuencia que lo produjo durante la ejecución actual.

### Idempotencia recuperable

Una entrega exitosa queda respaldada en disco por `(notification_id, channel)`. Al reabrir el mismo
directorio, la clave se recupera antes de procesar nuevas alertas. La reejecución exacta no vuelve a
publicar por decisión del ledger.

### Fallos de entrada aislados

JSON malformado, envelopes inválidos y alertas incompatibles se contabilizan sin abortar el resto
del lote o stream. Los fallos internos —I/O del ledger, configuración inválida o excepciones no
recuperables— no se disfrazan como un summary exitoso.

### Entrega confirmada por el canal

En MQTT live, `delivered` requiere que Paho informe la publicación confirmada dentro del timeout.
El instante del PUBACK cierra la métrica live. En `dry_run`, el éxito significa que el payload y el
topic se construyeron, no que existió una entrega externa.

## Decisiones y trade-offs

### MQTT QoS 1 más ledger local

QoS 1 ofrece entrega al menos una vez entre cliente y broker; puede producir duplicados. El ledger
evita que el propio proceso republique una alerta que ya considera entregada, pero un consumidor
debe seguir siendo tolerante a duplicados de red.

La alternativa exactly-once distribuida exigiría coordinación entre productor, broker y
consumidor, además de persistencia transaccional. No forma parte del recorte actual.

### Cooldown en memoria

El cooldown comienza vacío en cada proceso y usa tiempo de media cuando existe. Esto hace la
política determinista dentro de un replay y evita incorporar una base de estado compartida. Como
contrapartida, la supresión semántica no sobrevive a un reinicio ni se coordina entre procesos.

La idempotencia sí persiste porque resuelve otro problema: repetición exacta de una entrega.

### Retry fijo

`max_attempts` y `wait_ms` son explícitos y predecibles. La espera fija simplifica la medición y las
pruebas, pero no se adapta a caídas largas ni distribuye carga mediante jitter. Un broker que no se
recupere dentro de la ventana termina en dead letter.

### Ledger JSONL append-only por generaciones

JSONL mantiene auditoría legible y facilita la consolidación sin una base de datos. Al reabrir un
directorio, la generación previa se archiva como `notifications.<n>.jsonl` en vez de compactarse:
ninguna fila desaparece, y el costo es que el directorio crece con cada reejecución.

No es un journal transaccional multi-writer. Dos procesos que escriban simultáneamente el mismo
`--out-dir` pueden interferir; la plataforma asigna un directorio de distribución por experimento y
un solo proceso propietario.

### Un proceso por experimento

El runner inicia el CLI cuando la corrida lo solicita y lo termina con ella. Esto liga artefactos,
logs y estado a un experimento concreto y evita mantener un daemon adicional. El costo es que no
hay un servicio central con cola compartida o administración global.

**✎ 2026-08-18** (*decía "evita mantener un daemon adicional", como si no
existiera esa opción*): desde ADR-019 el daemon SÍ existe (`eovrt-distribute
serve`, servicio HTTP en `:8082`) como camino adicional, no como reemplazo.
Este subproceso-por-experimento sigue siendo el **default** del runner de la
webconsole (ADR-018, no derogada); el costo descrito arriba sigue aplicando a
ese camino. Lo que el servicio HTTP tampoco resuelve —una cola compartida o
administración global entre corridas— sigue siendo cierto: el servicio admite
**una corrida activa a la vez** (spec 45 §9.4), igual que el subproceso.

### Un canal

El único transporte implementado es MQTT. Mantener un canal único permite cerrar contratos,
idempotencia y observabilidad antes de introducir semánticas diferentes de Telegram, webhook o
correo. `channel` permanece en los records para conservar la frontera y permitir evolución futura,
pero esa evolución no está implementada.

## Alcance de la seguridad

- Las credenciales MQTT no aparecen en YAML: se leen de variables de entorno.
- La configuración de ejemplo apunta a `127.0.0.1`.
- El broker anónimo usado en laboratorio debe permanecer ligado a loopback.
- El runner omite stderr del subprocesso al construir errores para evitar filtrar configuración
  sensible.

El servicio no implementa TLS, rotación de secretos, autorización por topic ni almacenamiento
seguro de credenciales. Un despliegue fuera del host local debe aportar esas capacidades en el
broker y en la gestión del entorno.

## Limitaciones explícitas

| Área | No ofrecido actualmente |
|---|---|
| Canales | Telegram, webhook, email o fan-out multicanal |
| Entrega | exactly-once end-to-end |
| Retry | backoff exponencial, jitter o retry indefinido |
| Dead letter | comando de reproceso, scheduler o cola consumible |
| Estado | ledger central, coordinación multi-proceso o locks distribuidos |
| Cooldown | persistencia entre procesos o sincronización entre hosts |
| Operación | dashboard propio o imagen Docker propia (✎ 2026-08-18, corregido: esta fila decía también "API HTTP, daemon permanente" — ambos se ofrecen desde ADR-019 vía `eovrt-distribute serve`, `docs/specs/45-distribucion-alertas.md` §9; lo que sigue sin ofrecerse es el dashboard y la imagen Docker) |
| Seguridad | TLS y autorización administrados por el paquete |
| Recuperación live | retención propia del bus; el backfill requiere un archivo explícito |
| Métricas | mezcla de latencia DBE con latencia live |

## Consecuencias para consumidores

Un consumidor MQTT debería:

- validar `schema_version` y `event_type`;
- usar `notification_id` como clave idempotente propia;
- aceptar que QoS 1 puede repetir un mensaje;
- no inferir resolución de episodios a partir de ausencia de notificaciones;
- tratar `summary_text` como presentación y los campos estructurados como contrato;
- conservar su propia política de expiración o acknowledgement si la necesita.

El distribuidor confirma publicación al broker, no lectura ni acción humana del consumidor final.

## Consecuencias para operadores e integradores

- Cada corrida debe recibir un `--out-dir` exclusivo.
- Replay y live del distribuidor deben coincidir con el modo del control-plane al usar el runner.
- En live, el endpoint debe ser el bus de alertas, no el de detecciones.
- `dry_run` prueba el pipeline, pero no acredita disponibilidad MQTT.
- `dead_letter.jsonl` debe interpretarse junto con los records `failed` y el summary.
- `bus_dropped_events > 0` indica una discontinuidad de transporte que afecta la completitud
  observable, aunque el proceso termine correctamente.

## Evolución compatible

Agregar campos opcionales a los contratos o nuevos agregados al summary puede ser aditivo. Cambiar
la identidad, la clave idempotente, la semántica de un outcome o el origen de una latencia requiere
una nueva versión de schema y actualización coordinada del runner, reporte y webconsole.

## Lecturas relacionadas

- [Propósito y frontera](01-proposito-y-alcance.md)
- [Pipeline detallado](03-logica-de-distribucion.md)
- [Contratos y artefactos](04-contratos-y-artefactos.md)
