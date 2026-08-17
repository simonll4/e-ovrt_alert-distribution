# Glosario

## Alerta confirmada

Hecho de dominio `control.alert.v1` emitido por el control-plane cuando un patrón alcanza el estado
confirmado. Es la entrada del distribuidor, no una decisión que este servicio vuelva a calcular.

Véase [Propósito y alcance](01-proposito-y-alcance.md).

## Backfill

Lectura inicial de un `alerts.jsonl` antes de continuar con el bus live. Cubre eventos persistidos
que pudieron ocurrir antes de que el suscriptor estuviera listo. `ZmqSource` evita repetir por bus
los `alert_id` ya leídos en el backfill.

Véase [Lógica de distribución](03-logica-de-distribucion.md#8-backfill-y-bus-live).

## Cooldown

Ventana temporal durante la cual se suprimen alertas nuevas con la misma clave semántica. Por
defecto agrupa por condición y fuente. Solo una entrega exitosa inicia la ventana.

Véase [Supresión por cooldown](03-logica-de-distribucion.md#4-supresión-por-cooldown).

## DBE

Evaluación basada en datos (*data-based evaluation*). En este servicio corresponde al modo
`replay`, que procesa un archivo histórico después de la corrida. Su latencia de distribución se
etiqueta `wall_clock_dbe` y no representa latencia operacional live.

Véase [Integración DBE](05-integracion-con-la-plataforma.md#integración-dbe--replay).

## Dead letter

Resultado terminal de una notificación que agotó todos los intentos. Se registra como
`control.delivery.v1` y también en `dead_letter.jsonl`; no implica reproceso automático.

Véase [Publicación y retry](03-logica-de-distribucion.md#5-publicación-y-retry).

## EBE

Evaluación basada en eventos (*event-based evaluation*). Corresponde al modo `live`, en el que el
distribuidor consume el bus mientras la corrida está activa.

Véase [Integración EBE](05-integracion-con-la-plataforma.md#integración-ebe--live).

## Envelope

Contenedor versionado usado en el transporte ZeroMQ. `bus.envelope.v1` acompaña el payload con
topic, clave, secuencia e instante de publicación. No debe confundirse con
`NotificationEnvelope`, que es el mensaje de dominio publicado por MQTT.

Véase [Contratos y artefactos](04-contratos-y-artefactos.md#transporte-live-busenvelopev1).

## Idempotencia

Propiedad por la cual repetir la misma alerta no produce otra publicación después de una entrega
registrada. La clave es `(notification_id, channel)` y se recupera desde el ledger.

Véase [Deduplicación exacta](03-logica-de-distribucion.md#3-deduplicación-exacta).

## Ledger

Registro local `notifications.jsonl` que conserva `DeliveryRecord` y reconstruye las claves ya
entregadas leyendo todas las generaciones del directorio. Al comenzar una nueva ejecución sobre el
mismo directorio, la generación previa se archiva como `notifications.<n>.jsonl`; nada se borra.

Véase [Artefactos persistidos](04-contratos-y-artefactos.md#artefactos-persistidos).

## Notificación

Representación `control.notification.v1` derivada de una alerta confirmada y preparada para un
canal. Tiene identidad determinista, texto resumido y referencias a la corrida y evidencia.

Véase [Salida de canal](04-contratos-y-artefactos.md#salida-de-canal-controlnotificationv1).

## Outcome

Resultado de una decisión o intento de entrega: `delivered`, `failed`, `skipped_duplicate`,
`suppressed_cooldown` o `dead_letter`.

Véase [Outcomes](04-contratos-y-artefactos.md#outcomes).

## PUBACK

Confirmación MQTT de una publicación QoS 1. El canal live considera `delivered` cuando la librería
informa que la publicación fue confirmada; ese instante cierra `talert_notification_ms`.

Véase [Canal MQTT](02-arquitectura.md#canal-mqtt).

## QoS 1

Nivel MQTT de entrega al menos una vez. Reduce la pérdida entre cliente y broker, pero admite
duplicados; por eso la idempotencia sigue siendo obligatoria.

Véase [Decisiones y limitaciones](06-decisiones-y-limitaciones.md#mqtt-qos-1-más-ledger-local).

## Retry

Nuevo intento de publicación después de un `failed`. El servicio usa un máximo configurable y una
espera fija; cada intento queda registrado.

Véase [Publicación y retry](03-logica-de-distribucion.md#5-publicación-y-retry).

## Run ID

Identificador de una corrida. `media_run_id` rastrea percepción, `control_run_id` rastrea el motor
de patrones y `experiment_id` correlaciona los planos en el experimento paraguas.

Véase [Cadena contractual](04-contratos-y-artefactos.md#cadena-contractual).

## Source

Abstracción iterable que entrega `SourcedAlert`. Puede ser un JSONL, un bus ZeroMQ o una fuente
directa de pruebas. Sus estadísticas se incluyen en el summary.

Véase [Fuentes](02-arquitectura.md#fuentes).

## `talert_notification_ms`

Latencia de distribución registrada únicamente para entregas. Se separa por `live` y
`wall_clock_dbe` para impedir que una medición de replay se interprete como latencia operacional.

Véase [Latencia](03-logica-de-distribucion.md#6-latencia).
