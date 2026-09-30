"""The MECS child split uses a census-year Use table, not each nowcast year's (#1053)."""

from __future__ import annotations

import re
from pathlib import Path

_ENERGY_DIR = Path(__file__).resolve().parents[1]


def test_mecs_split_anchored_on_census_year() -> None:
    for year in range(2017, 2025):
        path = _ENERGY_DIR / f'Energy_manufacturing_national_nowcast_{year}.yaml'
        text = path.read_text(encoding='utf-8')
        mecs = int(re.search(r'mecs_year: &mecs_year (\d{4})', text).group(1))  # type: ignore[union-attr]
        split = int(re.search(r'split_year: &split_year (\d{4})', text).group(1))  # type: ignore[union-attr]
        # MECS 2018 splits on the 2017 benchmark Use, MECS 2022 on 2022's.
        assert split == (2017 if mecs == 2018 else 2022), path.name
        block = text.split('_attribution_sources:', 1)[1].split('source_names:', 1)[0]
        assert 'year: *split_year' in block, path.name
        assert 'year: *use_year' not in block, path.name
