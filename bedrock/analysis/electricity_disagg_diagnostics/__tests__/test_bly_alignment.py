"""Tests for sum-preserving BLy alignment."""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.analysis.electricity_disagg_diagnostics.bly import align_bly_pair
from bedrock.analysis.electricity_disagg_diagnostics.dispersion import (
    DISPERSION_TOL,
    pairwise_dispersion,
)
from bedrock.utils.schemas.cornerstone_schemas import (
    ELECTRICITY_AGGREGATE_SECTOR,
    ELECTRICITY_DISAGG_SECTORS,
)


def test_footing_final_schema_only_near_zero_dispersion() -> None:
    footing = pd.Series({ELECTRICITY_AGGREGATE_SECTOR: 100.0})
    final = pd.Series(
        {
            ELECTRICITY_DISAGG_SECTORS[0]: 34.0,
            ELECTRICITY_DISAGG_SECTORS[1]: 4.0,
            ELECTRICITY_DISAGG_SECTORS[2]: 62.0,
        }
    )
    assert pairwise_dispersion(footing, final) == pytest.approx(0.0, abs=DISPERSION_TOL)


def test_children_only_side_keeps_aggregate_zero() -> None:
    a = pd.Series({ELECTRICITY_AGGREGATE_SECTOR: 0.0, ELECTRICITY_DISAGG_SECTORS[0]: 10.0})
    b = pd.Series(
        {
            ELECTRICITY_DISAGG_SECTORS[0]: 34.0,
            ELECTRICITY_DISAGG_SECTORS[1]: 4.0,
            ELECTRICITY_DISAGG_SECTORS[2]: 62.0,
        }
    )
    _, b_aligned = align_bly_pair(a, b)
    assert float(b_aligned[ELECTRICITY_AGGREGATE_SECTOR]) == 0.0


def test_both_present_zeros_aggregate() -> None:
    a = pd.Series(
        {
            ELECTRICITY_AGGREGATE_SECTOR: 50.0,
            ELECTRICITY_DISAGG_SECTORS[0]: 10.0,
        }
    )
    b = pd.Series(
        {
            ELECTRICITY_AGGREGATE_SECTOR: 99.0,
            ELECTRICITY_DISAGG_SECTORS[0]: 34.0,
            ELECTRICITY_DISAGG_SECTORS[1]: 4.0,
            ELECTRICITY_DISAGG_SECTORS[2]: 62.0,
        }
    )
    a_aligned, b_aligned = align_bly_pair(a, b)
    assert float(a_aligned[ELECTRICITY_AGGREGATE_SECTOR]) == 0.0
    assert float(b_aligned[ELECTRICITY_AGGREGATE_SECTOR]) == 0.0


def test_alignment_preserves_total_mass() -> None:
    a = pd.Series({'1111A0': 1.0, ELECTRICITY_AGGREGATE_SECTOR: 100.0})
    b = pd.Series(
        {
            '1111A0': 1.0,
            ELECTRICITY_DISAGG_SECTORS[0]: 34.0,
            ELECTRICITY_DISAGG_SECTORS[1]: 4.0,
            ELECTRICITY_DISAGG_SECTORS[2]: 62.0,
        }
    )
    a_aligned, b_aligned = align_bly_pair(a, b)
    assert float(a_aligned.sum()) == pytest.approx(float(a.sum()))
    assert float(b_aligned.sum()) == pytest.approx(float(b.sum()))
