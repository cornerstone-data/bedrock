"""Unit tests for 2017 redefinition ratio carry."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from bedrock.transform.iot.nowcast_redefinition_ratios import (
    ATOL,
    MARGINS_VALUE_COLUMNS,
    RedefinitionRatios,
    apply_redefinition_ratios,
    compute_redefinition_ratios,
    industry_gross_output,
    load_redefinition_ratios,
    write_redefinition_ratios,
)
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY as M

# Toy tables use million-USD scale so deltas clear ATOL ($0.5M).


def _f(value: object) -> float:
    return float(np.asarray(value, dtype=float).reshape(-1)[0])


def _margins(
    values: dict[tuple[str, str], dict[str, float]],
) -> pd.DataFrame:
    index = pd.MultiIndex.from_tuples(
        list(values.keys()), names=['industry_code', 'commodity_code']
    )
    frame = pd.DataFrame(0.0, index=index, columns=list(MARGINS_VALUE_COLUMNS))
    for key, cols in values.items():
        for col, val in cols.items():
            frame.loc[key, col] = val
    return frame


def _empty_bundle(
    V: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    industries = list(V.index)
    empty_u = pd.DataFrame(0.0, index=industries, columns=industries)
    empty_va = pd.DataFrame(0.0, index=['V001'], columns=industries)
    empty_m = _margins(
        {(str(industries[0]), str(industries[0])): {"Producers' Value": 0.0}}
    )
    return empty_u, empty_va, empty_m


def test_industry_gross_output_row_sums() -> None:
    V = pd.DataFrame(
        [[10.0 * M, 5.0 * M], [0.0, 20.0 * M]],
        index=['1111A0', '1111B0'],
        columns=['1111A0', '1111B0'],
    )
    go = industry_gross_output(V)
    assert _f(go.loc['1111A0']) == pytest.approx(15.0 * M)
    assert _f(go.loc['1111B0']) == pytest.approx(20.0 * M)


def test_apply_does_not_mutate_inputs() -> None:
    V = pd.DataFrame([[100.0 * M]], index=['1111A0'], columns=['1111A0'])
    U = pd.DataFrame([[50.0 * M]], index=['1111A0'], columns=['1111A0'])
    VA = pd.DataFrame([[10.0 * M]], index=['V001'], columns=['1111A0'])
    Uimp = U.copy()
    margins = _margins({('1111A0', '1111A0'): {"Producers' Value": 50.0 * M}})
    ratios = RedefinitionRatios(
        V=pd.DataFrame(columns=['industry', 'commodity', 'ratio']),
        U=pd.DataFrame(columns=['row_code', 'industry', 'ratio']),
        VA=pd.DataFrame(columns=['row_code', 'industry', 'ratio']),
        Uimp=pd.DataFrame(columns=['row_code', 'industry', 'ratio']),
        margins=pd.DataFrame(
            columns=[
                'industry_code',
                'commodity_code',
                'value_column',
                'amount',
                'scale',
            ]
        ),
    )
    V_id, U_id, VA_id, Uimp_id, m_id = id(V), id(U), id(VA), id(Uimp), id(margins)
    apply_redefinition_ratios(V, U, VA, Uimp, margins, ratios=ratios)
    assert id(V) == V_id and id(U) == U_id and id(VA) == VA_id
    assert id(Uimp) == Uimp_id and id(margins) == m_id
    assert _f(V.iloc[0, 0]) == 100.0 * M


def test_make_cellwise_diagonal_and_off_diagonal_no_dest_credit() -> None:
    """Make moves are deducted from source cells only (diagonal + off-diagonal)."""
    V_before = pd.DataFrame(
        [[100.0 * M, 20.0 * M], [0.0, 50.0 * M]],
        index=['1111A0', '1111B0'],
        columns=['1111A0', '1111B0'],
    )
    V_after = pd.DataFrame(
        [[90.0 * M, 10.0 * M], [0.0, 50.0 * M]],
        index=['1111A0', '1111B0'],
        columns=['1111A0', '1111B0'],
    )
    empty_u, empty_va, empty_m = _empty_bundle(V_before)
    ratios = compute_redefinition_ratios(
        V_before,
        empty_u,
        empty_va,
        empty_u,
        empty_m,
        V_after,
        empty_u,
        empty_va,
        empty_u,
        empty_m,
    )
    assert len(ratios.V) == 2
    V_out, *_ = apply_redefinition_ratios(
        V_before, empty_u, empty_va, empty_u, empty_m, ratios=ratios
    )
    assert _f(V_out.loc['1111A0', '1111A0']) == pytest.approx(90.0 * M)
    assert _f(V_out.loc['1111A0', '1111B0']) == pytest.approx(10.0 * M)
    assert _f(V_out.loc['1111B0', '1111A0']) == pytest.approx(0.0)
    assert _f(V_out.loc['1111B0', '1111B0']) == pytest.approx(50.0 * M)


def test_use_va_import_industry_columns_only() -> None:
    U_before = pd.DataFrame(
        [[100.0 * M, 40.0 * M]],
        index=['1111A0'],
        columns=['1111A0', 'F010'],
    )
    U_after = pd.DataFrame(
        [[80.0 * M, 40.0 * M]],
        index=['1111A0'],
        columns=['1111A0', 'F010'],
    )
    V = pd.DataFrame([[200.0 * M]], index=['1111A0'], columns=['1111A0'])
    empty_va = pd.DataFrame(0.0, index=['V001'], columns=['1111A0'])
    empty_m = _margins({('1111A0', '1111A0'): {"Producers' Value": 0.0}})
    ratios = compute_redefinition_ratios(
        V,
        U_before,
        empty_va,
        U_before,
        empty_m,
        V,
        U_after,
        empty_va,
        U_after,
        empty_m,
    )
    assert list(ratios.U['industry']) == ['1111A0']
    assert 'F010' not in set(ratios.U['industry'])
    _, U_out, _, Uimp_out, _ = apply_redefinition_ratios(
        V, U_before, empty_va, U_before, empty_m, ratios=ratios
    )
    assert _f(U_out.loc['1111A0', '1111A0']) == pytest.approx(80.0 * M)
    assert _f(U_out.loc['1111A0', 'F010']) == pytest.approx(40.0 * M)
    assert _f(Uimp_out.loc['1111A0', '1111A0']) == pytest.approx(80.0 * M)


def test_margins_industry_go_ratio_and_nonindustry_absolute() -> None:
    V = pd.DataFrame([[100.0 * M]], index=['1111A0'], columns=['1111A0'])
    empty_u = pd.DataFrame(0.0, index=['1111A0'], columns=['1111A0'])
    empty_va = pd.DataFrame(0.0, index=['V001'], columns=['1111A0'])
    m_before = _margins(
        {
            ('1111A0', '1111A0'): {
                "Producers' Value": 50.0 * M,
                'Wholesale': 10.0 * M,
            },
            ('F010', '1111A0'): {"Producers' Value": 20.0 * M},
        }
    )
    m_after = _margins(
        {
            ('1111A0', '1111A0'): {
                "Producers' Value": 40.0 * M,
                'Wholesale': 10.0 * M,
            },
            ('F010', '1111A0'): {"Producers' Value": 15.0 * M},
        }
    )
    ratios = compute_redefinition_ratios(
        V,
        empty_u,
        empty_va,
        empty_u,
        m_before,
        V,
        empty_u,
        empty_va,
        empty_u,
        m_after,
    )
    by_key = {
        (r.industry_code, r.commodity_code, r.value_column): r
        for r in ratios.margins.itertuples(index=False)
    }
    ind = by_key[('1111A0', '1111A0', "Producers' Value")]
    assert ind.scale == 'go_ratio'
    assert _f(ind.amount) == pytest.approx(0.1)
    fd = by_key[('F010', '1111A0', "Producers' Value")]
    assert fd.scale == 'absolute'
    assert _f(fd.amount) == pytest.approx(5.0 * M)

    V2 = pd.DataFrame([[200.0 * M]], index=['1111A0'], columns=['1111A0'])
    *_, m_out = apply_redefinition_ratios(
        V2, empty_u, empty_va, empty_u, m_before, ratios=ratios
    )
    assert _f(m_out.loc[('1111A0', '1111A0'), "Producers' Value"]) == pytest.approx(
        30.0 * M
    )
    assert _f(m_out.loc[('F010', '1111A0'), "Producers' Value"]) == pytest.approx(
        15.0 * M
    )


def test_x_none_uses_pre_mutation_make_go() -> None:
    V_before = pd.DataFrame([[100.0 * M]], index=['1111A0'], columns=['1111A0'])
    V_after = pd.DataFrame([[90.0 * M]], index=['1111A0'], columns=['1111A0'])
    empty_u, empty_va, empty_m = _empty_bundle(V_before)
    ratios = compute_redefinition_ratios(
        V_before,
        empty_u,
        empty_va,
        empty_u,
        empty_m,
        V_after,
        empty_u,
        empty_va,
        empty_u,
        empty_m,
    )
    V_out, *_ = apply_redefinition_ratios(
        V_before, empty_u, empty_va, empty_u, empty_m, ratios=ratios, x=None
    )
    assert _f(V_out.loc['1111A0', '1111A0']) == pytest.approx(90.0 * M)


def test_year_t_go_ratios_scale_absolute_margins_do_not() -> None:
    V = pd.DataFrame([[100.0 * M]], index=['1111A0'], columns=['1111A0'])
    empty_u = pd.DataFrame(0.0, index=['1111A0'], columns=['1111A0'])
    empty_va = pd.DataFrame(0.0, index=['V001'], columns=['1111A0'])
    m_before = _margins(
        {
            ('1111A0', '1111A0'): {"Producers' Value": 50.0 * M},
            ('F010', '1111A0'): {"Producers' Value": 20.0 * M},
        }
    )
    m_after = _margins(
        {
            ('1111A0', '1111A0'): {"Producers' Value": 40.0 * M},
            ('F010', '1111A0'): {"Producers' Value": 15.0 * M},
        }
    )
    ratios = compute_redefinition_ratios(
        V,
        empty_u,
        empty_va,
        empty_u,
        m_before,
        V,
        empty_u,
        empty_va,
        empty_u,
        m_after,
    )
    x_t = pd.Series({'1111A0': 300.0 * M})
    *_, m_out = apply_redefinition_ratios(
        V, empty_u, empty_va, empty_u, m_before, ratios=ratios, x=x_t
    )
    assert _f(m_out.loc[('1111A0', '1111A0'), "Producers' Value"]) == pytest.approx(
        20.0 * M
    )
    assert _f(m_out.loc[('F010', '1111A0'), "Producers' Value"]) == pytest.approx(
        15.0 * M
    )


def test_zero_go_stores_zero_ratio() -> None:
    V = pd.DataFrame([[0.0]], index=['1111A0'], columns=['1111A0'])
    U_before = pd.DataFrame([[10.0 * M]], index=['1111A0'], columns=['1111A0'])
    U_after = pd.DataFrame([[0.0]], index=['1111A0'], columns=['1111A0'])
    empty_va = pd.DataFrame(0.0, index=['V001'], columns=['1111A0'])
    empty_m = _margins({('1111A0', '1111A0'): {"Producers' Value": 0.0}})
    ratios = compute_redefinition_ratios(
        V,
        U_before,
        empty_va,
        U_before,
        empty_m,
        V,
        U_after,
        empty_va,
        U_after,
        empty_m,
    )
    assert _f(ratios.U.iloc[0]['ratio']) == 0.0


def test_write_load_roundtrip(tmp_path: Path) -> None:
    ratios = RedefinitionRatios(
        V=pd.DataFrame([{'industry': '1111A0', 'commodity': '1111A0', 'ratio': 0.1}]),
        U=pd.DataFrame([{'row_code': '1111A0', 'industry': '1111A0', 'ratio': 0.2}]),
        VA=pd.DataFrame([{'row_code': 'V001', 'industry': '1111A0', 'ratio': 0.05}]),
        Uimp=pd.DataFrame(
            [{'row_code': '1111A0', 'industry': '1111A0', 'ratio': 0.03}]
        ),
        margins=pd.DataFrame(
            [
                {
                    'industry_code': '1111A0',
                    'commodity_code': '1111A0',
                    'value_column': "Producers' Value",
                    'amount': 0.1,
                    'scale': 'go_ratio',
                }
            ]
        ),
    )
    write_redefinition_ratios(ratios, tmp_path)
    loaded = load_redefinition_ratios(tmp_path)
    assert _f(loaded.V.iloc[0]['ratio']) == pytest.approx(0.1)
    assert loaded.V.iloc[0]['industry'] == '1111A0'
    assert loaded.margins.iloc[0]['scale'] == 'go_ratio'


def test_atol_filters_dust() -> None:
    V = pd.DataFrame([[1_000_000.0 * M]], index=['1111A0'], columns=['1111A0'])
    U_before = pd.DataFrame([[ATOL / 2]], index=['1111A0'], columns=['1111A0'])
    U_after = pd.DataFrame([[0.0]], index=['1111A0'], columns=['1111A0'])
    empty_va = pd.DataFrame(0.0, index=['V001'], columns=['1111A0'])
    empty_m = _margins({('1111A0', '1111A0'): {"Producers' Value": 0.0}})
    ratios = compute_redefinition_ratios(
        V,
        U_before,
        empty_va,
        U_before,
        empty_m,
        V,
        U_after,
        empty_va,
        U_after,
        empty_m,
    )
    assert ratios.U.empty
