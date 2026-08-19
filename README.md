# e-ovrt_alert-distribution

Distribución de alertas de E-OVRT-VDP (spec 45). Consumidor desacoplado de
alertas ya confirmadas por el control-plane: las recibe (bus
`control.alert.v1.*` o `alerts.jsonl` en replay), aplica política de
notificación (cooldown, ADR-011), entrega por MQTT y registra intento y
resultado. Nunca recalcula severidad ni crea alertas.

Diseño original (pre-servicio): `docs/superpowers/specs/2026-07-18-alert-distribution-design.md`.
El servicio HTTP y su acople con la webconsole están decididos en **ADR-019**
(`docs/decisiones/adr-019-servicio-http-distribucion.md`) y **ADR-020**
(`docs/decisiones/adr-020-http-como-unico-acople-de-distribucion.md`), en el
repo documental hermano `docs/`.

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

**El módulo también corre como servicio HTTP** (ADR-019, spec 45 §9 —
`docs/specs/45-distribucion-alertas.md` en el repo documental hermano `docs/`,
igual que las ADR), espejo del control-plane:

    pip install -e ".[service,dev]"
    eovrt-distribute serve --host 127.0.0.1 --port 8082

`POST /api/runs` dispara una corrida (`replay` o `live`) y devuelve su id; `GET
/api/runs/{id}` sirve el mismo `distribution_summary.json` que imprime el CLI;
`POST /api/runs/{id}/cancel` la detiene. Requiere el extra `service`
(fastapi/uvicorn) — sin él, `serve` falla con un mensaje explícito en vez de un
traceback. **Este es el camino por default del runner de la webconsole
(ADR-020)**: le habla al servicio por HTTP; el subproceso por experimento quedó
como **fallback operativo** (`EOVRT_CONSOLE_DISTRIBUTION_TRANSPORT=subprocess`)
y dejó de ser un patrón de acople. Setear `=http` es un no-op: ya es el default.

El despliegue vigente ejecuta este módulo como servicio del host en `:8082`,
igual que los planos media (`:8080`) y control (`:8081`); el CLI
(`replay`/`live`) se conserva para el camino offline y la ejecución directa.
Desde 2026-08-19 el repo también mantiene una imagen Docker propia
(`infra/docker/Dockerfile`) para el compose de la plataforma.

**✎ Historia (2026-08-18):** hasta esa fecha no existía daemon persistente y el
runner creaba un proceso por experimento (ADR-018). ADR-019 introdujo el
servicio HTTP y **ADR-020 derogó a ADR-018**: HTTP pasó a ser el default y el
subproceso quedó sólo como fallback operativo.

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
