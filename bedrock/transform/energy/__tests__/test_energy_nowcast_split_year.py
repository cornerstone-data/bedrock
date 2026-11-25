"""The MECS child split uses a census-year Use table, not each nowcast year's (#1053)."""

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
        # Inventory years on the 2018 MECS survey split on the 2017 benchmark
        # Use; 2021+ (MECS 2022 on main) split on 2022. The quantity anchor stays 2022.
        assert anchor == 2022, path.name
        assert split == (2017 if year <= 2020 else 2022), path.name
        nowcast = text.split('Nowcast_use:', 1)[1].split('_mecs_annual_path:', 1)[0]
        assert 'year: *split_year' in nowcast, path.name
        assert 'year: *use_year' not in nowcast, path.name
