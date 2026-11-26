"""The MECS child split uses census-year Use tables, not each nowcast year's (#1053).

The 2022 census Use is blended with the 2017 benchmark Use before 2022 (#1210).
"""

from __future__ import annotations

import re
from pathlib import Path

_ENERGY_DIR = Path(__file__).resolve().parents[1]


def test_mecs_split_anchored_on_census_year() -> None:
    for year in range(2017, 2025):
        path = _ENERGY_DIR / f'Energy_manufacturing_national_nowcast_{year}.yaml'
        text = path.read_text(encoding='utf-8')
        anchor = int(
            re.search(r'mecs_anchor_year: &mecs_anchor_year (\d{4})', text).group(1)  # type: ignore[union-attr]
        )
        split = int(re.search(r'split_year: &split_year (\d{4})', text).group(1))  # type: ignore[union-attr]
        # Every year splits on 2022 Use blended with the 2017 benchmark Use, so
        # the split has no step at 2021. The quantity anchor stays 2022.
        assert anchor == 2022, path.name
        assert split == 2022, path.name
        nowcast = text.split('Nowcast_use:', 1)[1].split('_mecs_annual_path:', 1)[0]
        assert 'year: *split_year' in nowcast, path.name
        # The Use year itself is never the nowcast year (inventory_year is).
        assert not re.search(r'(?m)^\s+year: \*use_year', nowcast), path.name
        assert 'EIA_MECS blend_census_use_years' in nowcast, path.name
        assert 'inventory_year: *use_year' in nowcast, path.name
        assert 'early_year: 2017' in nowcast, path.name
