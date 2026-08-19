# Documentación del servicio de distribución de alertas

Este conjunto explica qué lugar ocupa `e-ovrt_alert-distribution` en E-OVRT, qué decisiones toma
sobre una alerta ya confirmada y qué garantías ofrece al resto de la plataforma. La referencia
parte del comportamiento vigente del código y sus pruebas; las especificaciones históricas de
[`superpowers/`](superpowers/) se conservan como trazabilidad del diseño.

## Recorrido recomendado

1. [Propósito y alcance](01-proposito-y-alcance.md): por qué existe el servicio y dónde termina su
   responsabilidad.
2. [Arquitectura](02-arquitectura.md): componentes, dependencias y modelo de ejecución.
3. [Lógica de distribución](03-logica-de-distribucion.md): cómo se decide el destino de cada
   alerta.
4. [Contratos y artefactos](04-contratos-y-artefactos.md): schemas, topics, outcomes y archivos.
5. [Integración con la plataforma](05-integracion-con-la-plataforma.md): secuencias replay y live.
6. [Decisiones y limitaciones](06-decisiones-y-limitaciones.md): garantías, trade-offs y alcance
   deliberado.
7. [Glosario](glosario.md): vocabulario compartido.

Para instalar el paquete o consultar el CLI, véase el [README de la raíz](../README.md).

## Idea central

El distribuidor recibe hechos que el control-plane ya confirmó. No vuelve a interpretar detecciones
ni patrones: decide si corresponde notificar, intenta entregar la notificación por MQTT y deja
evidencia auditable de cada resultado.

```text
alerta confirmada + política + historial de entrega
                         ↓
       notificación entregada, suprimida o agotada
```

La misma lógica se expone por tres superficies de ejecución:

- `replay` (DBE): consume un `alerts.jsonl` ya producido.
- `live` (EBE): consume el bus de alertas mientras la corrida está activa y puede completar el
  comienzo mediante backfill.
- `serve` (servicio HTTP en `:8082`, ADR-019): daemon de vida larga que dispara esas mismas
  corridas (`replay` o `live`) vía `POST /api/runs`; es el camino por default del runner de la
  webconsole (ADR-020).

Las tres superficies ejecutan el mismo `Distributor`, producen los mismos contratos y escriben los
mismos artefactos.

## Fuentes de autoridad

La documentación se contrastó con:

- `src/eovrt_distribution/`, implementación vigente;
- `tests/`, comportamiento de regresión;
- `configs/example.yaml`, configuración pública;
- `pyproject.toml`, runtime y dependencias;
- spec 45, ADR-005, ADR-011, ADR-016, ADR-019 y ADR-020 del repositorio hermano `docs/`;
- integración vigente del runner y del generador de reportes en
  `e-ovrt_experimental-setup`.

Ante una diferencia accidental entre esta explicación y una versión futura del código, los modelos
Pydantic y las pruebas del commit ejecutado son la referencia operativa. La documentación debe
actualizarse junto con cualquier cambio de contrato o comportamiento.
