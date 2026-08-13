from eovrt_distribution.contracts.notification import NotificationEnvelope
from eovrt_distribution.policy import NotificationPolicy


def _env(make_alert, **overrides) -> NotificationEnvelope:
    return NotificationEnvelope.from_alert(make_alert(**overrides))


def test_burst_same_condition_source_suppressed(make_alert):
    policy = NotificationPolicy(cooldown_ms=30000.0)
    first = _env(make_alert, timestamp_ms=1000.0)
    burst = _env(make_alert, timestamp_ms=5000.0, subject_key="otro-sujeto")
    assert policy.allow(first, now_wall_ms=0.0)
    assert not policy.allow(burst, now_wall_ms=0.0)  # coalesce entre sujetos


def test_reallows_after_window(make_alert):
    policy = NotificationPolicy(cooldown_ms=30000.0)
    assert policy.allow(_env(make_alert, timestamp_ms=1000.0), now_wall_ms=0.0)
    assert policy.allow(_env(make_alert, timestamp_ms=31001.0), now_wall_ms=0.0)


def test_different_key_not_suppressed(make_alert):
    policy = NotificationPolicy(cooldown_ms=30000.0)
    assert policy.allow(_env(make_alert, timestamp_ms=1000.0), now_wall_ms=0.0)
    assert policy.allow(_env(make_alert, timestamp_ms=2000.0, source_id="cam-02"), now_wall_ms=0.0)
    assert policy.allow(
        _env(make_alert, timestamp_ms=3000.0, condition_id="CR-02"), now_wall_ms=0.0
    )


def test_falls_back_to_wall_clock_without_media_time(make_alert):
    policy = NotificationPolicy(cooldown_ms=30000.0)
    assert policy.allow(_env(make_alert, timestamp_ms=None), now_wall_ms=100000.0)
    assert not policy.allow(_env(make_alert, timestamp_ms=None), now_wall_ms=110000.0)
    assert policy.allow(_env(make_alert, timestamp_ms=None), now_wall_ms=140001.0)


def test_zero_cooldown_never_suppresses(make_alert):
    policy = NotificationPolicy(cooldown_ms=0.0)
    assert policy.allow(_env(make_alert, timestamp_ms=1000.0), now_wall_ms=0.0)
    assert policy.allow(_env(make_alert, timestamp_ms=1000.0), now_wall_ms=0.0)


def test_check_does_not_consume_cooldown_until_delivery_is_marked(make_alert):
    policy = NotificationPolicy(cooldown_ms=30000.0)
    first = _env(make_alert, timestamp_ms=1000.0)
    second = _env(make_alert, timestamp_ms=2000.0, subject_key="otro")

    assert policy.is_suppressed(first, now_wall_ms=0.0) is False
    assert policy.is_suppressed(second, now_wall_ms=0.0) is False
    policy.mark_notified(first, now_wall_ms=0.0)
    assert policy.is_suppressed(second, now_wall_ms=0.0) is True
