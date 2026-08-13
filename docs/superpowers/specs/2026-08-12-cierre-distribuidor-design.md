# Cierre del distribuidor de alertas — diseño

Fecha: 2026-08-12
Repo: `e-ovrt_alert-distribution`
Normativa: ADR-016, ADR-011 y spec 45 del repositorio hermano `docs/`.

## 1. Objetivo y alcance

Cerrar las brechas propias del paquete que seguían abiertas después del relevamiento 114:
crecimiento acumulativo de `notifications.jsonl` entre re-ejecuciones (C2), entorno reproducible
en Python 3.11 (C4) y una historia Git verificable (C5). No se agregan canales,
dashboard, reproceso automático de dead letters ni nuevas políticas de notificación.

La integración con el runner y la webconsole se diseña en
`e-ovrt_experimental-setup/docs/superpowers/specs/2026-08-12-integracion-distribucion-alertas-design.md`.

## 2. Ledger acotado sin perder idempotencia

`notifications.jsonl` continúa siendo simultáneamente el ledger de entregas y el registro auditable
de la ejecución actual. Al construir `DeliveryLedger` sobre un archivo existente:

1. se leen todas las líneas válidas;
2. se conserva el último `DeliveryRecord(outcome="delivered")` de cada
   `(notification_id, channel)`;
3. el archivo se reescribe atómicamente con ese conjunto canónico antes de agregar resultados de la
   nueva ejecución;
4. los nuevos intentos, fallos, supresiones, duplicados y dead letters se agregan normalmente.

Así, tras cada reapertura, el tamaño queda acotado por las entregas únicas históricas más los
registros de una ejecución. La segunda ejecución conserva la propiedad actual: una alerta entregada
antes produce `skipped_duplicate`, porque la clave entregada se rehidrata antes de compactar.

La compactación usa un archivo temporal en el mismo directorio y `Path.replace()` para no dejar un
ledger parcial si el proceso cae durante la reescritura. Una línea JSON inválida no se descarta en
silencio: aborta la apertura del ledger igual que hoy, porque ese archivo es evidencia interna
producida por el propio paquete y su corrupción debe quedar visible.

`dead_letter.jsonl` describe la ejecución actual. Se trunca al comenzar `Distributor.run()`; si no
hay agotadas, queda ausente o vacío, pero nunca conserva agotadas de una ejecución anterior como si
pertenecieran a la actual.

## 3. Runtime Python 3.11

El paquete declara `requires-python = ">=3.11,<3.12"` y Ruff apunta a `py311`. El entorno local se
recrea con Python 3.11 y se reinstala con los extras `mqtt,dev`. La restricción es deliberada: evita
que CI, el entorno local y la imagen validen versiones distintas durante el cierre.

## 4. Despliegue como proceso del host

La topología vigente ejecuta media-plane, control-plane y el distribuidor como procesos del host.
El runner crea un `eovrt-distribute` por experimento y lo termina junto con la corrida; no se agrega
una imagen propia ni un daemon persistente. El broker MQTT continúa siendo una dependencia externa
y puede ejecutarse con aMQTT o Mosquitto ligado a loopback.

## 5. Pruebas y criterio de terminado

- Test unitario: un archivo con múltiples outcomes por clave se compacta a una entrega por clave.
- Test unitario: una clave entregada sigue respondiendo `seen=True` después de compactar.
- Test de CLI: tres replays sobre el mismo directorio no hacen crecer el archivo por la suma de las
  tres historias y siguen produciendo `skipped_duplicate`.
- Test del distribuidor: `dead_letter.jsonl` viejo no reaparece en una ejecución nueva exitosa.
- Suite completa, integración MQTT disponible y Ruff limpios bajo Python 3.11.
- Smoke DBE del ejecutable real descubierto por el runner y prueba live contra un broker MQTT local.

## 6. Compatibilidad y errores

No cambia ningún schema (`control.notification.v1`, `control.delivery.v1` ni
`control.distribution_summary.v1`) ni el nombre de los tres artefactos. La compactación ocurre solo
al iniciar una nueva ejecución sobre un directorio existente; la primera ejecución conserva todos
sus intentos. Un fallo de I/O o corrupción del ledger hace fallar el comando con salida no cero y no
se presenta un summary exitoso.
