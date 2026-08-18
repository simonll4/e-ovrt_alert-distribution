import sys
from unittest.mock import patch

from eovrt_distribution.cli import main


def test_serve_sin_extra_service_no_tira_traceback(monkeypatch, capsys) -> None:
    """T6: sin fastapi/uvicorn, `serve` debe fallar con mensaje, no con traceback crudo."""
    monkeypatch.setitem(sys.modules, "uvicorn", None)
    assert main(["serve"]) == 1
    err = capsys.readouterr().err
    assert "service" in err
    assert "ModuleNotFoundError" not in err


def test_serve_usa_8082_por_defecto() -> None:
    with patch("uvicorn.run") as run:
        assert main(["serve"]) == 0
    assert run.call_args.kwargs["port"] == 8082
    assert run.call_args.kwargs["host"] == "127.0.0.1"


def test_serve_acepta_puerto_explicito() -> None:
    with patch("uvicorn.run") as run:
        assert main(["serve", "--port", "9100"]) == 0
    assert run.call_args.kwargs["port"] == 9100


def test_replay_sigue_funcionando(tmp_path) -> None:
    """Guard de no-regresion: agregar `serve` no puede romper el CLI existente."""
    alerts = tmp_path / "a.jsonl"
    alerts.write_text("", encoding="utf-8")
    assert main(["replay", "--alerts", str(alerts), "--out-dir", str(tmp_path / "o")]) == 0
