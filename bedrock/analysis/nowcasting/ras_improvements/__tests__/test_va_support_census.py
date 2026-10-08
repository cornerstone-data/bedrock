"""Hermetic tests for #808 VA support census classifiers (no GRAS)."""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.analysis.nowcasting.ras_improvements.va_support_census import (
    INDUSTRY_CSV_COLUMNS,
    SUMMARY_CSV_COLUMNS,
    VA_OPEN_SUPPORT_INDUSTRIES,
    b2_candidate_cells,
    classify_b1_gate,
    t18_skip_gate,
)
from bedrock.utils.economic.balance.mask import SutMask


def test_t18_skip_gate_order() -> None:
    assert (
        t18_skip_gate(free_v00300=False, abs_sum_seed_offsets=10.0) == 'v00300_frozen'
    )
    assert t18_skip_gate(free_v00300=True, abs_sum_seed_offsets=0.0) == 'abs_sum_zero'
    assert t18_skip_gate(free_v00300=True, abs_sum_seed_offsets=1.0) == 'none'


def test_classify_b1_gate_first_match() -> None:
    assert (
        classify_b1_gate(
            free_v00100=None, t1_minus_t18_usd_m=0.0, t18_abs=100.0, year_error=True
        )
        == 'needs_decision'
    )
    assert (
        classify_b1_gate(free_v00100=False, t1_minus_t18_usd_m=0.0, t18_abs=100.0)
        == 'needs_decision'
    )
    assert (
        classify_b1_gate(free_v00100=True, t1_minus_t18_usd_m=0.0, t18_abs=40.0)
        == 'skip_immaterial'
    )
    # Small gap, material T18 → allow_b1
    assert (
        classify_b1_gate(free_v00100=True, t1_minus_t18_usd_m=10.0, t18_abs=2000.0)
        == 'allow_b1'
    )
    # Large gap → needs_decision
    assert (
        classify_b1_gate(free_v00100=True, t1_minus_t18_usd_m=1500.0, t18_abs=2000.0)
        == 'needs_decision'
    )


def test_schema_column_locks() -> None:
    assert 't1_minus_t18_usd_m' in INDUSTRY_CSV_COLUMNS
    assert 'b1_gate' in SUMMARY_CSV_COLUMNS
    assert '814000' not in VA_OPEN_SUPPORT_INDUSTRIES
    assert '531HSO' in VA_OPEN_SUPPORT_INDUSTRIES


def test_b2_candidate_cells_toy_mask(monkeypatch: pytest.MonkeyPatch) -> None:
    index = ['c1', 'c2', 'V00100']
    columns = ['531HSO', '814000', 'other']
    structural = pd.DataFrame(
        [[True, True, True], [False, True, True], [True, True, True]],
        index=index,
        columns=columns,
    )
    mask = SutMask(
        structural_zero=structural,
        fixed_value=pd.DataFrame(False, index=index, columns=columns),
        sign_lock=pd.DataFrame(0, index=index, columns=columns, dtype=int),
    )
    monkeypatch.setattr(
        'bedrock.analysis.nowcasting.ras_improvements.va_support_census.balance_commodities',
        lambda: ('c1', 'c2'),
    )
    # Only 531HSO is in VA_OPEN_SUPPORT; c1 True, c2 False → 1 candidate
    assert b2_candidate_cells(mask) == 1
