"""Tests for chained dispersion math."""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.analysis.electricity_disagg_diagnostics.dispersion import (
    DISPERSION_TOL,
    compute_chained_dispersion,
)


def test_no_oscillation_combined_equals_sum_steps() -> None:
    footing = pd.Series({'a': 10.0, 'b': 20.0})
    step1 = pd.Series({'a': 12.0, 'b': 20.0})
    step2 = pd.Series({'a': 12.0, 'b': 18.0})
    step3 = pd.Series({'a': 15.0, 'b': 18.0})
    result = compute_chained_dispersion(
        footing,
        [step1, step2, step3],
        ['s1', 's2', 's3'],
    )
    assert result.combined_mmt == pytest.approx(
        sum(result.step_values_mmt), abs=DISPERSION_TOL
    )
    assert not result.show_offsetting_bar


def test_oscillation_sum_steps_exceeds_combined() -> None:
    footing = pd.Series({'a': 0.0, 'b': 0.0})
    step1 = pd.Series({'a': 10.0, 'b': 0.0})
    step2 = pd.Series({'a': 0.0, 'b': 0.0})
    step3 = pd.Series({'a': 0.0, 'b': 0.0})
    result = compute_chained_dispersion(
        footing,
        [step1, step2, step3],
        ['up', 'down', 'flat'],
    )
    assert sum(result.step_values_mmt) > result.combined_mmt + DISPERSION_TOL
    assert result.show_offsetting_bar
