"""Tests for #1040 median-coverage Hybrid mode freeze."""

from __future__ import annotations

from bedrock.transform.ghg.facility_coverage import median_freeze_modes_from_tables


def test_median_freeze_modes_from_tables_facility_vs_mecs_and_recipe() -> None:
    # Median A = 0.75 < 0.8 -> MECS; median B = 0.85 >= 0.8 -> recipe facility mode.
    coverage = {
        2017: {'A': 0.70, 'B': 0.90, 'C': 0.95},
        2018: {'A': 0.75, 'B': 0.85, 'C': 0.96},
        2019: {'A': 0.90, 'B': 0.82, 'C': 0.94},
    }
    native = {
        2019: {
            'A': 'facility_floor',
            'B': 'facility_vector',
            'C': 'keep_prior',  # recipe says MECS but median clears -> floor
        },
    }
    out = median_freeze_modes_from_tables(
        coverage, native, recipe_year=2019, min_coverage=0.8
    )
    assert out['A'] == 'keep_prior'
    assert out['B'] == 'facility_vector'
    assert out['C'] == 'facility_floor'


def test_median_freeze_modes_missing_sector_is_mecs() -> None:
    coverage = {2017: {'X': 0.5}, 2018: {'X': 0.6}}
    out = median_freeze_modes_from_tables(
        coverage, {}, recipe_year=2017, min_coverage=0.8
    )
    assert out['X'] == 'keep_prior'
