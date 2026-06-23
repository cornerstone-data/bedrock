"""Tests for d_85 scenario figures (analysis-only)."""

from __future__ import annotations

from pathlib import Path
from typing import cast

import matplotlib
import pandas as pd
import pytest

matplotlib.use('Agg')

from bedrock.analysis.electricity.d_85.figure_data import (
    build_diagonal_intersection_matrix,
    market_clearing_gaps_table,
    step2_intersection_matrix,
)
from bedrock.analysis.electricity.d_85.figures import (
    build_decision_figures,
    plot_figure_a_pr3_scenario_map,
    plot_figure_c_step2_intersection_matrices,
)
from bedrock.analysis.electricity.d_85.scenario_types import (
    DisaggScenarioResult,
    ScenarioWeights,
)
from bedrock.utils.schemas.cornerstone_schemas import ELECTRICITY_DISAGG_SECTORS


def test_diagonal_intersection_preserves_total() -> None:
    w = pd.Series({'221110': 0.5, '221121': 0.3, '221122': 0.2})
    matrix = build_diagonal_intersection_matrix(w, 1000.0)
    assert pytest.approx(float(matrix.sum().sum())) == 1000.0
    assert matrix.loc['221121', '221110'] == 0.0


def test_step2_baseline_is_diagonal() -> None:
    from bedrock.analysis.electricity.d_85.disagg_weights import (  # noqa: PLC0415
        table83_go_weights,
        ugo305_go_weights,
    )

    t = 500.0
    base = step2_intersection_matrix('baseline', total=t)
    w = ugo305_go_weights()
    for code in ELECTRICITY_DISAGG_SECTORS:
        assert base.loc[code, code] == pytest.approx(float(w[code]) * t)
        assert float(base.loc[code].drop(code).sum()) == pytest.approx(0.0)

    prod = step2_intersection_matrix('t8.3_production_diag', total=t)
    w83 = table83_go_weights()
    assert prod.loc['221110', '221110'] == pytest.approx(float(w83['221110']) * t)


def test_offdiag_matrix_has_zero_gen_off_diagonal() -> None:
    matrix = step2_intersection_matrix('t8.3_production_offdiag', total=1000.0)
    assert matrix.loc['221121', '221110'] == 0.0
    assert matrix.loc['221122', '221110'] == 0.0
    assert cast(float, matrix.at['221110', '221121']) > 0.0


def _stub_scenario(
    name: str, gaps: dict[str, float], *, metrics_only: bool
) -> DisaggScenarioResult:
    codes = list(ELECTRICITY_DISAGG_SECTORS)
    w = pd.Series({c: 1 / 3 for c in codes})
    weights = ScenarioWeights(
        w_make_intersection=w,
        w_use_intersection=None,
        w_column_steps=w,
        w_row_uniform=w,
        w_row_by_column=None,
        intersection_3x3=None,
    )
    empty = pd.DataFrame(0.0, index=codes, columns=codes)
    q = pd.Series({c: 100.0 for c in codes})
    x = q.copy()
    for code, gap in gaps.items():
        empty.loc[code, codes[0]] = float(q[code]) + gap
    return DisaggScenarioResult(
        name=name,
        weights=weights,
        V=empty,
        Udom=empty,
        Uimp=pd.DataFrame(0.0, index=codes, columns=codes),
        VA=pd.DataFrame(0.0, index=['V00100'], columns=codes),
        Y=pd.DataFrame(0.0, index=codes, columns=['F01000']),
        q=q,
        x=x,
        metrics_only=metrics_only,
    )


def test_market_clearing_gaps_table_includes_decision5() -> None:
    d3 = {
        'baseline': _stub_scenario('baseline', {'221110': -1e6}, metrics_only=False),
        't8.3_production_diag': _stub_scenario(
            't8.3_production_diag', {'221110': 5e9}, metrics_only=False
        ),
        't8.3_production_offdiag': _stub_scenario(
            't8.3_production_offdiag', {'221110': 1e10}, metrics_only=True
        ),
        't8.3_purchased_power_diag': _stub_scenario(
            't8.3_purchased_power_diag', {'221110': 4e9}, metrics_only=False
        ),
        't8.3_purchased_power_offdiag': _stub_scenario(
            't8.3_purchased_power_offdiag', {'221110': 9e9}, metrics_only=True
        ),
    }
    d5 = {
        'baseline': d3['baseline'],
        'p24_2017': _stub_scenario('p24_2017', {'221110': -5e9}, metrics_only=False),
        'p24_target': _stub_scenario(
            'p24_target', {'221110': -5.1e9}, metrics_only=False
        ),
    }
    table = market_clearing_gaps_table(d3, d5)
    assert set(table['scenario_id']) == {
        'baseline',
        't8.3_production_diag',
        't8.3_production_offdiag',
        't8.3_purchased_power_diag',
        't8.3_purchased_power_offdiag',
        'p24_2017',
        'p24_target',
    }
    assert table.loc[
        (table['scenario_id'] == 'p24_2017') & (table['sector'] == '221110'),
        'market_clearing_gap_b',
    ].iloc[0] == pytest.approx(-5.0)


def test_build_decision_figures_writes_pngs(tmp_path: Path) -> None:
    d3 = {
        'baseline': _stub_scenario('baseline', {'221110': 0.0}, metrics_only=False),
        't8.3_production_diag': _stub_scenario(
            't8.3_production_diag', {'221110': 1e9}, metrics_only=False
        ),
        't8.3_production_offdiag': _stub_scenario(
            't8.3_production_offdiag', {'221110': 2e9}, metrics_only=True
        ),
        't8.3_purchased_power_diag': _stub_scenario(
            't8.3_purchased_power_diag', {'221110': 1e9}, metrics_only=False
        ),
        't8.3_purchased_power_offdiag': _stub_scenario(
            't8.3_purchased_power_offdiag', {'221110': 2e9}, metrics_only=True
        ),
    }
    d5 = {
        'baseline': d3['baseline'],
        'p24_2017': _stub_scenario('p24_2017', {'221110': -1e9}, metrics_only=False),
        'p24_target': _stub_scenario(
            'p24_target', {'221110': -1e9}, metrics_only=False
        ),
    }
    paths = build_decision_figures(d3, d5, output_dir=tmp_path)
    assert len(paths) == 3
    for path in paths.values():
        assert path.exists()
        assert path.stat().st_size > 1000


def test_plot_figure_a_and_c_smoke() -> None:
    plot_figure_a_pr3_scenario_map()
    plot_figure_c_step2_intersection_matrices()
