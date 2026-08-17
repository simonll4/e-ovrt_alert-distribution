# e-ovrt_alert-distribution

Distribución de alertas de E-OVRT-VDP (spec 45). Consumidor desacoplado de
alertas ya confirmadas por el control-plane: las recibe (bus
`control.alert.v1.*` o `alerts.jsonl` en replay), aplica política de
notificación (cooldown, ADR-011), entrega por MQTT y registra intento y
resultado. Nunca recalcula severidad ni crea alertas.

Diseño: `docs/superpowers/specs/2026-07-18-alert-distribution-design.md`.

Documentación técnica vigente: [`docs/README.md`](docs/README.md). Explica el
propósito del servicio en la plataforma, su arquitectura, la lógica de
distribución, los contratos y sus límites.

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

## Tests

    .venv/bin/python -m pytest -q          # suite por defecto (sin integración)

`pyproject.toml` declara `addopts = "-m 'not integration'"`, así que el test que
publica contra un broker MQTT real (`tests/test_mqtt_live.py`) queda **excluido por
defecto** y aparece como `deselected`.

### Test de integración con broker

Levantar un broker anónimo en `127.0.0.1:1883` y correr el marcador `integration`:

    pip install amqtt   # broker MQTT 3.1.1 puro Python, sin apt/sudo
    amqtt -c ../e-ovrt_experimental-setup/infra/platform/mosquitto/amqtt.yaml
    # (o el binario mosquitto si está instalado)

    .venv/bin/python -m pytest -m integration -q

El YAML del broker vive en el repo hermano `e-ovrt_experimental-setup`; el comando
canónico está documentado en `infra/platform/README.md` de ese repo y se corre desde
su raíz (`amqtt -c infra/platform/mosquitto/amqtt.yaml`) — arriba figura la ruta
relativa equivalente desde este repo.

**Sin broker el test no falla: se saltea con causa** (`1 skipped`), citando el error
de conexión observado. El canal no propaga la excepción de red — la devuelve como
`SendResult(ok=False, error=...)` —, así que el test inspecciona `result.error`
además de capturar excepciones.
