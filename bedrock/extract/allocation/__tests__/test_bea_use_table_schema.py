"""Tests for schema-aligned BEA use table (load_bea_use_table)."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from bedrock.extract.allocation.bea import (
    _load_bea_use_table_cached,
    load_bea_use_table,
)
from bedrock.utils.config.usa_config import USAConfig
from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from bedrock.utils.taxonomy.cornerstone.industries import INDUSTRIES


def _clear_use_table_cache() -> None:
    _load_bea_use_table_cached.cache_clear()


@pytest.mark.eeio_integration
def test_load_bea_use_table_ceda_shape() -> None:
    """With CEDA config, table has CEDA v7 industry rows + PCE row."""
    _clear_use_table_cache()
    config = USAConfig(use_cornerstone_2026_model_schema=False)
    with patch("bedrock.extract.allocation.bea.get_usa_config", return_value=config):
        table = load_bea_use_table()
    # Rows = CEDA industries + one PCE row
    industry_rows = [i for i in table.index if i in CEDA_V7_SECTORS]
    assert len(industry_rows) == len(CEDA_V7_SECTORS)
    assert "221200" in table.columns
    assert table.shape[0] >= len(CEDA_V7_SECTORS)
    assert table.shape[1] > 0


@pytest.mark.eeio_integration
def test_load_bea_use_table_cornerstone_shape() -> None:
    """With Cornerstone config, table has Cornerstone industry rows + PCE row."""
    _clear_use_table_cache()
    config = USAConfig(use_cornerstone_2026_model_schema=True)
    with patch("bedrock.extract.allocation.bea.get_usa_config", return_value=config):
        table = load_bea_use_table()
    industry_rows = [i for i in table.index if i in INDUSTRIES]
    assert len(industry_rows) == len(INDUSTRIES)
    assert "221200" in table.columns
    assert table.shape[0] >= len(INDUSTRIES)
    assert table.shape[1] > 0


@pytest.mark.eeio_integration
def test_load_bea_use_table_cache_per_schema() -> None:
    """CEDA and Cornerstone calls return different shapes (cache keyed by schema)."""
    _clear_use_table_cache()
    config_ceda = USAConfig(use_cornerstone_2026_model_schema=False)
    config_cs = USAConfig(use_cornerstone_2026_model_schema=True)
    with patch("bedrock.extract.allocation.bea.get_usa_config", return_value=config_ceda):
        table_ceda = load_bea_use_table()
    with patch("bedrock.extract.allocation.bea.get_usa_config", return_value=config_cs):
        table_cs = load_bea_use_table()
    assert table_ceda.shape[0] != table_cs.shape[0]
    assert set(table_ceda.index) != set(table_cs.index)
