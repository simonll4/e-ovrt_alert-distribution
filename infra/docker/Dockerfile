# e-ovrt_alert-distribution — imagen del servicio HTTP de distribución (ADR-019).
#
# Construir desde la raíz del repo (el contexto de build es la raíz):
#   docker build -f infra/docker/Dockerfile -t eovrt-alert-distribution .
#
# Notas operativas:
# - Los artefactos de una corrida se escriben en el `out_dir` del request
#   (`POST /api/runs`), que debe ser un filesystem compartido con el BFF —
#   el compose de la plataforma lo monta en rutas idénticas en ambos
#   contenedores. `EOVRT_DISTRIBUTION_RUNS_DIR` es cosmético (default del
#   directorio de corridas cuando el request no trae `out_dir`).
# - Las credenciales MQTT van EXCLUSIVAMENTE por entorno:
#   `EOVRT_MQTT_USERNAME` / `EOVRT_MQTT_PASSWORD`. Nunca en YAML ni en la imagen.

# requires-python = ">=3.11,<3.12" — pin duro, no subir a 3.12+.
FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml ./
COPY src/ ./src/
# configs/ no se empaqueta con pip (no es parte del paquete): hay que copiarla.
COPY configs/ ./configs/

# Extras obligatorios: `service` (fastapi+uvicorn — sin él `serve` sale con 1)
# y `mqtt` (paho-mqtt — sin él el modo live falla con ChannelError).
RUN pip install --no-cache-dir ".[mqtt,service]"

EXPOSE 8082

# GET /healthz vía stdlib urllib: python:3.11-slim no trae curl.
HEALTHCHECK --interval=10s --start-period=10s \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8082/healthz', timeout=5)"]

# `--host 0.0.0.0` es obligatorio: el default del CLI es 127.0.0.1,
# inalcanzable desde los contenedores hermanos del compose.
CMD ["eovrt-distribute", "serve", "--host", "0.0.0.0", "--port", "8082"]
