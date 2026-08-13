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
    eovrt-distribute live --endpoint tcp://127.0.0.1:5558 --control-run-id <control-run-id> \
      --backfill <control-run>/alerts.jsonl --out-dir runs/d2

En live, `--endpoint` es el bus de alertas **control→distribución** (`alert_bus`,
default `:5558`), no el bus de detecciones media→control (`input.bus`, `:5557`).
El broker MQTT se configura aparte en `configs/example.yaml` (`channel.host` y
`channel.port`); usuario y contraseña van exclusivamente por
`EOVRT_MQTT_USERNAME` / `EOVRT_MQTT_PASSWORD`.

El despliegue vigente ejecuta este módulo como proceso del host, igual que los
planos media/control. No se mantiene una imagen Docker propia ni un daemon
persistente de distribución: el runner crea un proceso por experimento.
