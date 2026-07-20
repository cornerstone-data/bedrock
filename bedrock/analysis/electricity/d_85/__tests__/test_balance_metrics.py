"""Tests for balance and VA metrics."""

from __future__ import annotations

import pandas as pd

from bedrock.analysis.electricity.d_85.balance_metrics import (
    compute_balance_metrics,
    va_summary_by_scenario,
)
from bedrock.analysis.electricity.d_85.scenario_types import (
    DisaggScenarioResult,
    ScenarioWeights,
)


def _minimal_result(*, metrics_only: bool = False) -> DisaggScenarioResult:
    codes = ['221110', '221121', '221122']
    idx = pd.Index(codes, name='sector')
    V = pd.DataFrame(0.0, index=idx, columns=idx)
    for code, val in zip(codes, (100.0, 20.0, 80.0)):
        V.at[code, code] = val
    Udom = pd.DataFrame(0.0, index=idx, columns=idx)
    Uimp = Udom.copy()
    VA = pd.DataFrame(
        {'221110': [10.0], '221121': [2.0], '221122': [8.0]}, index=['V00100']
    )
    Y = pd.DataFrame(0.0, index=idx, columns=['F01000'])
    q = pd.Series({'221110': 100.0, '221121': 20.0, '221122': 80.0})
    x = q.copy()
    w = pd.Series({'221110': 0.5, '221121': 0.1, '221122': 0.4})
    weights = ScenarioWeights(w, None, w, w, None, None)
    return DisaggScenarioResult(
        name='test',
        weights=weights,
        V=V,
        Udom=Udom,
        Uimp=Uimp,
        VA=VA,
        Y=Y,
        q=q,
        x=x,
        metrics_only=metrics_only,
    )


def test_compute_balance_metrics_shape() -> None:
    metrics = compute_balance_metrics(_minimal_result())
    assert set(metrics['sector']) == {'221110', '221121', '221122'}
    assert (metrics['qx_relative_error'] == 0.0).all()


def test_va_summary_flags_negative() -> None:
    result = _minimal_result()
    result.VA.loc['V00200', '221110'] = -5.0
    summary = va_summary_by_scenario(result)
    gen = summary.loc[summary['sector'] == '221110'].iloc[0]
    assert int(gen['negative_va_count']) >= 1
