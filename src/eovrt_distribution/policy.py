"""Política de notificación (ADR-011): el cooldown vive acá, no en el motor.

Clave de supresión (condition_id, source_id): para notificación asistiva lo
relevante es "esta condición en esta cámara ya fue avisada", independiente
del sujeto. Base de tiempo = media_timestamp_ms (coherente entre replay y
live); fallback a wall-clock si la alerta no trae tiempo de media.
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
        self._last_notified_ms: dict[tuple, float] = {}

    def _key(self, env: NotificationEnvelope) -> tuple:
        return tuple(getattr(env, field) for field in self.key_fields)

    def allow(self, env: NotificationEnvelope, now_wall_ms: float) -> bool:
        """Compatibilidad: comprueba y, si permite, consume la ventana."""
        if self.is_suppressed(env, now_wall_ms):
            return False
        self.mark_notified(env, now_wall_ms)
        return True

    def is_suppressed(self, env: NotificationEnvelope, now_wall_ms: float) -> bool:
        if self.cooldown_ms <= 0:
            return False
        t = env.media_timestamp_ms if env.media_timestamp_ms is not None else now_wall_ms
        key = self._key(env)
        last = self._last_notified_ms.get(key)
        return last is not None and (t - last) < self.cooldown_ms

    def mark_notified(self, env: NotificationEnvelope, now_wall_ms: float) -> None:
        if self.cooldown_ms <= 0:
            return
        t = env.media_timestamp_ms if env.media_timestamp_ms is not None else now_wall_ms
        key = self._key(env)
        self._last_notified_ms[key] = t
