"""Tests for UGO305 vs Table 8.3 weight builders."""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.analysis.electricity.d_85.disagg_weights import (
    build_ugo_col_table83_row_intersection_matrix,
    table83_go_weights,
)


def test_table83_go_weights_normalizes(mock_fba: pd.DataFrame) -> None:
    w = table83_go_weights(fba=mock_fba)
    assert pytest.approx(float(w.sum())) == 1.0
    assert w['221110'] > w['221121'] > w['221122']


def test_offdiag_matrix_rules_d1_d2(mock_fba: pd.DataFrame) -> None:
    w_ugo = pd.Series({'221110': 0.5, '221121': 0.3, '221122': 0.2})
    w_83 = table83_go_weights(fba=mock_fba)
    T = 1000.0
    M = build_ugo_col_table83_row_intersection_matrix(w_ugo, w_83, T)

    assert pytest.approx(float(M.sum().sum())) == T
    for code in ('221110', '221121', '221122'):
        assert pytest.approx(float(M[code].sum())) == float(w_ugo[code]) * T

    i, j, k = '221110', '221121', '221122'
    assert M.at[j, i] == 0.0
    assert M.at[k, i] == 0.0
    assert pytest.approx(M.at[i, i]) == w_ugo[i] * T


def test_offdiag_worked_example_totals() -> None:
    w_ugo = pd.Series({'221110': 0.5, '221121': 0.3, '221122': 0.2})
    w_83 = pd.Series({'221110': 0.86, '221121': 0.09, '221122': 0.05})
    T = 1000.0
    M = build_ugo_col_table83_row_intersection_matrix(w_ugo, w_83, T)
    assert M.loc['221121', '221110'] == 0.0
    assert pytest.approx(M.loc['221110', '221121']) == 0.3 * 0.86 * T
