"""Tests for Decision 7 UGO305 scaling simulation."""

from __future__ import annotations

from unittest import mock

import pandas as pd
import pytest

from bedrock.analysis.electricity.d_85.scaling_scenarios import (
    build_anchored_overrides,
    build_ugo305_detail_ratios,
)


@mock.patch('bedrock.analysis.electricity.d_85.scaling_scenarios._go_levels_for_year')
def test_build_ugo305_detail_ratios(go_mock: mock.Mock) -> None:
    go_mock.side_effect = [
        pd.Series({'221110': 100.0, '221121': 10.0, '221122': 50.0}),
        pd.Series({'221110': 110.0, '221121': 11.0, '221122': 55.0}),
    ]
    ratios = build_ugo305_detail_ratios(base_year=2017, target_year=2022)
    assert ratios['221110'] == pytest.approx(1.1)
    assert ratios['221121'] == pytest.approx(1.1)


@mock.patch(
    'bedrock.analysis.electricity.d_85.scenario_ef_pipeline._summary_utilities_ratio'
)
def test_anchored_preserves_weighted_mean(util_mock: mock.Mock) -> None:
    util_mock.return_value = 1.2
    ratios = pd.Series({'221110': 1.1, '221121': 0.9, '221122': 1.0})
    w = pd.Series({'221110': 0.5, '221121': 0.2, '221122': 0.3})
    overrides = build_anchored_overrides(ratios, w_base=w)
    weighted = sum(overrides[k] * w[k] for k in w.index) / w.sum()
    assert weighted == pytest.approx(1.2)
