import itertools

import pytest

_counter = itertools.count(1)


@pytest.fixture
def make_alert():
    def _make(**overrides) -> dict:
        i = next(_counter)
        alert = {
            "schema_version": "control.alert.v1",
            "event_type": "alert_event",
            "control_run_id": "cr-test",
            "media_run_id": "mr-test",
            "unit_id": f"u-{i:04d}",
            "source_id": "cam-01",
            "alert_id": f"00000000-0000-5000-8000-{i:012d}",
            "pattern_id": "pr01",
            "condition_id": "CR-01",
            "subject_key": f"subject-{i}",
            "severity": "high",
            "state": "open",
            "evidence": {},
            "timestamp_ms": 1000.0 * i,
            "experiment_id": "exp-test",
        }
        alert.update(overrides)
        return alert

    return _make
