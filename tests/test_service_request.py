import pytest
from pydantic import ValidationError

from eovrt_distribution.service.run_request import DistributionRunRequest


def test_replay_valido() -> None:
    req = DistributionRunRequest(
        mode="replay", out_dir="/tmp/out", alerts_path="/tmp/alerts.jsonl", config_path="c.yaml"
    )
    assert req.mode == "replay"


def test_campo_desconocido_es_error() -> None:
    """extra='forbid': un typo no se ignora en silencio."""
    with pytest.raises(ValidationError):
        DistributionRunRequest(
            mode="replay", out_dir="/tmp/o", alerts_path="/a", config_path="c", modo="replay"
        )


def test_config_path_y_config_juntos_es_error() -> None:
    with pytest.raises(ValidationError):
        DistributionRunRequest(
            mode="replay", out_dir="/tmp/o", alerts_path="/a", config_path="c", config={"x": 1}
        )


def test_sin_ninguna_config_es_error() -> None:
    with pytest.raises(ValidationError):
        DistributionRunRequest(mode="replay", out_dir="/tmp/o", alerts_path="/a")


def test_replay_sin_alerts_path_es_error() -> None:
    with pytest.raises(ValidationError):
        DistributionRunRequest(mode="replay", out_dir="/tmp/o", config_path="c")


def test_live_sin_endpoint_es_error() -> None:
    with pytest.raises(ValidationError):
        DistributionRunRequest(mode="live", out_dir="/tmp/o", config_path="c")


def test_idle_timeout_no_positivo_es_error() -> None:
    with pytest.raises(ValidationError):
        DistributionRunRequest(
            mode="live", out_dir="/tmp/o", endpoint="tcp://127.0.0.1:5558",
            config_path="c", idle_timeout_ms=0,
        )
