"""Tests directos (unit) para `eovrt_distribution.service.run_ids`.

Por que existen ademas del test HTTP `test_run_id_con_traversal_da_404`:
a nivel HTTP, GET/DELETE/cancel de un `run_id` invalido y de un `run_id`
bien formado pero inexistente devuelven el MISMO 404 -- el fallback de
`RunManager` (`KeyError` sobre un dict) es seguro para CUALQUIER string, sea
o no un id valido. Confirmado a mano: comentar `require_valid_run_id(run_id)`
en `get_run` NO hace fallar `test_run_id_con_traversal_da_404` (el mismo 404
sale por el `except UnknownRunError` de todos modos). Un test end-to-end no
puede distinguir "el guard disparo" de "el guard no existia y el manager
fallo igual".

Estos tests SI distinguen: ejercitan `is_valid_run_id`/`require_valid_run_id`
directamente y fallan de inmediato si se comenta la validacion del regex
(confirmado a mano, ver el fix de la ronda 1 de la Tarea 4).
"""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from eovrt_distribution.service.run_ids import (
    is_valid_run_id,
    new_distribution_run_id,
    require_valid_run_id,
)


@pytest.mark.parametrize(
    "run_id",
    ["dist_20260101T000000Z_abcdef", "a", "A1_-", "run-123"],
)
def test_is_valid_run_id_acepta_ids_bien_formados(run_id: str) -> None:
    assert is_valid_run_id(run_id) is True


@pytest.mark.parametrize(
    "run_id",
    ["..", "../etc", "a/b", "a\\b", "", " ", "a b", "a.b"],
)
def test_is_valid_run_id_rechaza_traversal_y_separadores(run_id: str) -> None:
    assert is_valid_run_id(run_id) is False


def test_require_valid_run_id_levanta_404_para_id_invalido() -> None:
    with pytest.raises(HTTPException) as exc_info:
        require_valid_run_id("..")
    assert exc_info.value.status_code == 404


def test_require_valid_run_id_no_levanta_para_id_valido() -> None:
    require_valid_run_id("dist_20260101T000000Z_abcdef")  # no debe levantar nada


def test_new_distribution_run_id_siempre_pasa_is_valid_run_id() -> None:
    # Postcondicion declarada en el docstring de `new_distribution_run_id`.
    for _ in range(20):
        assert is_valid_run_id(new_distribution_run_id())
