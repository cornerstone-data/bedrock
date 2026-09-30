"""The manufacturing expense seed's survey path across survey changes (#1053)."""

from __future__ import annotations

import numpy as np
import pandas as pd

from bedrock.analysis.nowcasting.inputs_structure import (
    SMOOTHED_EXPENSE_KINDS,
    smoothed_kind_block,
    smoothed_survey_path,
)
from bedrock.utils.config.usa_config import get_usa_config

YEARS = list(range(2017, 2025))


def _path(values: list[float]) -> pd.Series:
    return pd.Series(values, index=YEARS, dtype=float)


def _churn(path: pd.Series) -> float:
    return float(np.log(path).diff().abs().sum())


def test_census_years_stay_exact() -> None:
    out = smoothed_survey_path(_path([1.9, 11.0, 2.3, 4.5, 8.5, 12.0, 16.9, 23.8]))
    assert out[2017] == 1.9
    assert out[2022] == 12.0


def test_a_survey_change_spike_is_damped() -> None:
    """``33641A``'s fuel path: census 1.9, ASM 11.0 then 2.3, census 12.0."""
    raw = _path([1.9, 11.0, 2.3, 4.5, 8.5, 12.0, 16.9, 23.8])
    out = smoothed_survey_path(raw)
    assert _churn(out.loc[2017:2022]) < 0.5 * _churn(raw.loc[2017:2022])
    assert out[2018] < 3.0


def test_a_steady_asm_path_meets_both_censuses_without_a_break() -> None:
    """ASM reads 2x the census level throughout: benchmarking removes the wedge."""
    raw = _path([10.0, 20.0, 20.4, 20.8, 21.2, 10.8, 11.0, 11.2])
    out = smoothed_survey_path(raw)
    steps = out.loc[2017:2022].pct_change().dropna()
    assert steps.abs().max() < 0.05


def test_aies_years_chain_on_their_own_change() -> None:
    raw = _path([10.0, 10.0, 10.0, 10.0, 10.0, 12.0, 30.0, 33.0])
    out = smoothed_survey_path(raw)
    assert out[2023] == 12.0
    assert np.isclose(out[2024], 12.0 * 33.0 / 30.0)


def test_an_unobserved_industry_is_untouched() -> None:
    raw = _path([np.nan] * 8)
    out = smoothed_survey_path(raw)
    assert out.isna().all()


def test_missing_asm_years_interpolate_between_the_censuses() -> None:
    raw = _path([10.0, np.nan, np.nan, np.nan, np.nan, 20.0, np.nan, np.nan])
    out = smoothed_survey_path(raw)
    assert out.loc[2017:2022].is_monotonic_increasing
    assert 10.0 < out[2019] < 20.0


def test_electricity_keeps_its_raw_path_and_the_switch_is_on() -> None:
    assert 'CSTELEC' not in SMOOTHED_EXPENSE_KINDS
    assert 'CSTFU' in SMOOTHED_EXPENSE_KINDS
    assert get_usa_config().smooth_manufacturing_expense_path is True


def test_block_keeps_each_years_total_and_the_census_years() -> None:
    """Smoothing works on shares, so the raw total (a 2020 dip, a 2022 peak) stays."""
    block = pd.DataFrame(
        {
            year: [small, 100.0 * scale]
            for year, small, scale in zip(
                YEARS,
                [1.9, 11.0, 2.3, 4.5, 8.5, 12.0, 16.9, 23.8],
                [1.0, 1.05, 1.1, 0.8, 1.2, 1.6, 1.3, 1.2],
            )
        },
        index=['33641A', 'big'],
    )
    out = smoothed_kind_block(block)
    assert np.allclose(out.sum(axis=0), block.sum(axis=0))
    assert np.allclose(out[2017], block[2017])
    assert np.allclose(out[2022], block[2022])
    assert _churn(out.loc['33641A', 2017:2022]) < _churn(block.loc['33641A', 2017:2022])
