"""Tests for #1060: process-subpart emissions in the coverage denominator."""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.transform.ghg.facility_coverage import (
    ATTRIBUTION_MIN_COVERAGE,
    attribution_mode,
    coverage_from_components,
)


def _components() -> tuple[pd.Series, pd.Series, pd.Series, pd.Series]:
    # cement-like: little subpart C, kiln fuel and calcination under subpart H;
    # boiler-like: all combustion under subpart C, no process subparts.
    ghgrp = pd.Series({'327310': 2.0, '311221': 15.0})
    nei_below = pd.Series({'327310': 0.0, '311221': 0.5})
    nei_above = pd.Series({'327310': 0.0, '311221': 0.0})
    process = pd.Series({'327310': 62.0, '999999': 4.0})
    return ghgrp, nei_below, nei_above, process


def test_process_subparts_lower_coverage_and_send_sector_to_mecs() -> None:
    out = coverage_from_components(*_components())
    assert out.loc['327310', 'coverage'] == pytest.approx(2.0 / 64.0)
    assert out.loc['327310', 'process_subparts_Mt'] == pytest.approx(62.0)
    mode = attribution_mode(float(out.loc['327310', 'coverage']), 'vector')
    assert mode == 'keep_prior'


def test_combustion_only_sector_keeps_the_old_coverage() -> None:
    out = coverage_from_components(*_components())
    assert out.loc['311221', 'coverage'] == pytest.approx(15.0 / 15.5)
    assert out.loc['311221', 'coverage'] >= ATTRIBUTION_MIN_COVERAGE
    assert out.loc['311221', 'process_subparts_Mt'] == 0.0


def test_process_only_sector_without_combustion_is_dropped() -> None:
    out = coverage_from_components(*_components())
    assert '999999' not in out.index


def test_total_and_unresolved_stay_combustion_only() -> None:
    ghgrp, nei_below, _, process = _components()
    nei_above = pd.Series({'311221': 1.5})
    out = coverage_from_components(ghgrp, nei_below, nei_above, process)
    assert out.loc['311221', 'total_Mt'] == pytest.approx(17.0)
    assert out.loc['311221', 'unresolved'] == pytest.approx(1.5 / 17.0)
    assert out.loc['327310', 'total_Mt'] == pytest.approx(2.0)
