"""Tests for Cornerstone-frame BEA use and Make tables."""

from __future__ import annotations

import pytest

from bedrock.extract.allocation.bea import load_bea_make_table, load_bea_use_table
from bedrock.utils.taxonomy.cornerstone.commodities import COMMODITIES
from bedrock.utils.taxonomy.cornerstone.industries import (
    INDUSTRIES,
    WASTE_DISAGG_INDUSTRIES,
)

_APPLIANCE_SUBS = ("335221", "335222", "335224", "335228")
_WASTE_KIDS = tuple(WASTE_DISAGG_INDUSTRIES["562000"])


@pytest.mark.eeio_integration
def test_load_bea_use_table_cornerstone_shape() -> None:
    """Table has Cornerstone industry rows + PCE row."""
    load_bea_use_table.cache_clear()
    table = load_bea_use_table()
    industry_rows = [i for i in table.index if i in INDUSTRIES]
    assert len(industry_rows) == len(INDUSTRIES)
    assert "221200" in table.columns
    assert table.shape[0] >= len(INDUSTRIES)
    assert table.shape[1] > 0


@pytest.mark.eeio_integration
def test_load_bea_make_table_cornerstone_frame() -> None:
    """Make has the Cornerstone 405 schema used by allocation."""
    load_bea_make_table.cache_clear()
    make = load_bea_make_table()
    assert list(make.index) == list(INDUSTRIES)
    assert list(make.columns) == list(COMMODITIES)
    for code in _APPLIANCE_SUBS:
        assert code not in make.index
        assert code not in make.columns
    assert "335220" in make.index
    assert "331313" in make.index
    assert "33131B" in make.index
    assert "562000" not in make.index
    for code in _WASTE_KIDS:
        assert code in make.index
