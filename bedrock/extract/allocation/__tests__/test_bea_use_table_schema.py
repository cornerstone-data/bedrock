"""Tests for schema-aligned BEA use table (load_bea_use_table)."""

from __future__ import annotations

import pytest

from bedrock.extract.allocation.bea import load_bea_use_table
from bedrock.utils.taxonomy.cornerstone.industries import INDUSTRIES


@pytest.mark.eeio_integration
def test_load_bea_use_table_cornerstone_shape() -> None:
    """Table should have Cornerstone industry rows + PCE row."""
    load_bea_use_table.cache_clear()
    table = load_bea_use_table()
    industry_rows = [i for i in table.index if i in INDUSTRIES]
    assert len(industry_rows) == len(INDUSTRIES)
    assert "221200" in table.columns
    assert table.shape[0] >= len(INDUSTRIES)
    assert table.shape[1] > 0
