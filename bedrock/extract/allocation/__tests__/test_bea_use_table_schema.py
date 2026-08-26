"""Tests for Cornerstone-frame BEA use and Make tables."""

from __future__ import annotations

from typing import cast

import numpy as np
import pandas as pd
import pytest

from bedrock.extract.allocation.bea import load_bea_make_table, load_bea_use_table
from bedrock.transform.eeio.derived_cornerstone import _derive_cornerstone_V_baseline
from bedrock.utils.taxonomy.cornerstone.commodities import COMMODITIES
from bedrock.utils.taxonomy.cornerstone.industries import (
    INDUSTRIES,
    WASTE_DISAGG_INDUSTRIES,
)

_APPLIANCE_SUBS = ("335221", "335222", "335224", "335228")
_WASTE_KIDS = tuple(WASTE_DISAGG_INDUSTRIES["562000"])
# Codes present in both frames whose Make mass is not 1:1 (aluminum lump,
# government-enterprise fold-ins).
_NON_1TO1_SHARED = frozenset({"331313", "221100", "485000"})


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
    """Make is the 405 Cornerstone baseline V, not CEDA v7 or disagg V."""
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

    baseline = _derive_cornerstone_V_baseline()
    pd.testing.assert_frame_equal(make, baseline)


@pytest.mark.eeio_integration
def test_make_table_1to1_row_sums_match_ceda_v7() -> None:
    """Shared 1:1 sectors keep the same Make row mass as ``derive_2017_V_usa``.

    Trap codes (appliances, aluminum, waste, gov-enterprise fold-ins) are
    excluded; those frames differ by construction. Retire this test with
    ``derive_2017_V_usa``.
    """
    from bedrock.transform.eeio.derived_2017 import derive_2017_V_usa  # noqa: PLC0415
    from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS  # noqa: PLC0415

    load_bea_make_table.cache_clear()
    cs = load_bea_make_table()
    v7 = derive_2017_V_usa()
    shared = sorted(
        (set(INDUSTRIES) & set(CEDA_V7_SECTORS)).difference(_NON_1TO1_SHARED)
    )
    cs_rows = cs.loc[shared].sum(axis=1)
    v7_rows = v7.loc[shared].sum(axis=1)
    rel = (cs_rows - v7_rows).abs() / v7_rows.replace(0.0, np.nan)
    rel = rel.fillna(0.0)
    worst = rel.nlargest(5)
    assert (
        rel.max() < 1e-8
    ), f"1:1 Make row sums diverge; worst relative diffs:\n{worst.to_string()}"
    # Live Make lookups besides household appliances.
    for code in ("333415", "326140", "326150"):
        assert cs.at[code, code] == pytest.approx(
            cast(float, v7.at[code, code]), rel=1e-8
        )
