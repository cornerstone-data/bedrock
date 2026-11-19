"""The MECS child split uses census Use 2022 with the MECS 2022 anchor (#1053 / #1210)."""

from __future__ import annotations

import re
from pathlib import Path

_ENERGY_DIR = Path(__file__).resolve().parents[1]


def test_mecs_split_anchored_on_census_year() -> None:
    for year in range(2017, 2025):
        path = _ENERGY_DIR / f'Energy_manufacturing_national_nowcast_{year}.yaml'
        text = path.read_text(encoding='utf-8')
        mecs = int(re.search(r'mecs_anchor_year: &mecs_anchor_year (\d{4})', text).group(1))  # type: ignore[union-attr]
        split = int(re.search(r'split_year: &split_year (\d{4})', text).group(1))  # type: ignore[union-attr]
        assert split == mecs, path.name
        nowcast = text.split('Nowcast_use:', 1)[1].split('_mecs_annual_path:', 1)[0]
        assert 'year: *split_year' in nowcast, path.name
        assert 'year: *use_year' not in nowcast, path.name
