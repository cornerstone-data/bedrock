"""Tests for scenario EF pipeline."""

from __future__ import annotations

from unittest import mock

import pandas as pd

from bedrock.analysis.electricity.d_85.scenario_ef_pipeline import (
    _apply_electricity_q_overrides,
    derive_Aq_from_scenario,
    scenario_vnorm,
)
from bedrock.analysis.electricity.d_85.scenario_types import (
    DisaggScenarioResult,
    ScenarioWeights,
)


def _tiny_scenario() -> DisaggScenarioResult:
    codes = ['221110', '221121', '221122', '212100']
    idx = pd.Index(codes, name='sector')
    V = pd.DataFrame(0.0, index=idx, columns=idx)
    V.at['221110', '221110'] = 100.0
    V.at['221121', '221121'] = 20.0
    V.at['221122', '221122'] = 80.0
    Udom = pd.DataFrame(0.0, index=idx, columns=idx)
    Uimp = Udom.copy()
    VA = pd.DataFrame(
        {'221110': [5.0], '221121': [1.0], '221122': [4.0]}, index=['V00100']
    )
    Y = pd.DataFrame(0.0, index=idx, columns=['F01000'])
    q = pd.Series({'221110': 100.0, '221121': 20.0, '221122': 80.0, '212100': 0.0})
    x = q.copy()
    w = pd.Series({'221110': 0.5, '221121': 0.1, '221122': 0.4})
    weights = ScenarioWeights(w, None, w, w, None, None)
    return DisaggScenarioResult('tiny', weights, V, Udom, Uimp, VA, Y, q, x)


def test_derive_Aq_from_scenario_shapes() -> None:
    with mock.patch(
        'bedrock.analysis.electricity.d_85.scenario_ef_pipeline.validate_cornerstone'
    ):
        aq = derive_Aq_from_scenario(_tiny_scenario())
    assert aq.Adom.shape == aq.Aimp.shape
    assert len(aq.scaled_q) >= 3


def test_scenario_vnorm_matches_v_index() -> None:
    scen = _tiny_scenario()
    vnorm = scenario_vnorm(scen)
    assert list(vnorm.index) == list(scen.V.index)
    assert list(vnorm.columns) == list(scen.V.columns)


@mock.patch(
    'bedrock.analysis.electricity.d_85.scenario_ef_pipeline._summary_utilities_ratio'
)
def test_apply_q_overrides(util_mock: mock.Mock) -> None:
    util_mock.return_value = 1.0
    q = pd.Series({'221110': 100.0, '221121': 20.0, '221122': 80.0})
    overrides = {'221110': 1.2, '221121': 0.9, '221122': 1.1}
    out = _apply_electricity_q_overrides(q, overrides, io_year=2022, detail_year=2017)
    assert out['221110'] == 120.0
    assert out['221121'] == 18.0
