"""Política de notificación (ADR-011): el cooldown vive acá, no en el motor.

Clave de supresión (condition_id, source_id): para notificación asistiva lo
relevante es "esta condición en esta cámara ya fue avisada", independiente
del sujeto.

Base de tiempo mixta. `_time()` devuelve el par `(base, timestamp)`: base
``"media"`` cuando la alerta trae `media_timestamp_ms` (coherente entre replay
y live: dos alertas del mismo video conservan su distancia temporal aunque el
replay ocurra mucho después) y base ``"wall"`` cuando no lo trae, usando el
wall-clock del momento del chequeo. La base se **registra junto al timestamp**
en `mark_notified`, y ante bases incomparables (la guardada difiere de la
actual) `is_suppressed` **nunca suprime**: restar wall-clock contra tiempo de
media daría una diferencia sin significado, y el sesgo elegido es dejar pasar
la notificación antes que suprimirla por una resta inválida.
"""

from __future__ import annotations

from eovrt_distribution.contracts.notification import NotificationEnvelope


class NotificationPolicy:
    def __init__(
        self,
        cooldown_ms: float = 30000.0,
        key_fields: tuple[str, ...] = ("condition_id", "source_id"),
    ) -> None:
        self.cooldown_ms = cooldown_ms
        self.key_fields = key_fields
        self._last_notified_ms: dict[tuple, tuple[str, float]] = {}

    def _key(self, env: NotificationEnvelope) -> tuple:
        return tuple(getattr(env, field) for field in self.key_fields)

    def _time(self, env: NotificationEnvelope, now_wall_ms: float) -> tuple[str, float]:
        """Devuelve (`base`, `timestamp`) usada para comparación de cooldown.

        Separamos explícitamente la base `media` y `wall`. Si una alerta llegó con
        `media_timestamp_ms`, ese timestamp gobierna todo el cooldown de esa señal.
        Si no llega, usamos el wall-clock del momento del check.
        """
        if env.media_timestamp_ms is not None:
            return "media", env.media_timestamp_ms
        return "wall", now_wall_ms

    def allow(self, env: NotificationEnvelope, now_wall_ms: float) -> bool:
        """Compatibilidad: comprueba y, si permite, consume la ventana."""
        if self.is_suppressed(env, now_wall_ms):
            return False
        self.mark_notified(env, now_wall_ms)
        return True

    def is_suppressed(self, env: NotificationEnvelope, now_wall_ms: float) -> bool:
        if self.cooldown_ms <= 0:
            return False
        base, t = self._time(env, now_wall_ms)
        key = self._key(env)
        last = self._last_notified_ms.get(key)
        if last is None:
            return False
        prior_base, last_t = last
        if prior_base != base:
            return False
        return (t - last_t) < self.cooldown_ms

    def mark_notified(self, env: NotificationEnvelope, now_wall_ms: float) -> None:
        if self.cooldown_ms <= 0:
            return
        key = self._key(env)
        self._last_notified_ms[key] = self._time(env, now_wall_ms)
