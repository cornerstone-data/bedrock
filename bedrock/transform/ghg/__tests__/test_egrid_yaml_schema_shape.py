"""Schema-shape checks for dual 2024 eGRID Cornerstone GHG overlays."""

from __future__ import annotations

from pathlib import Path

import pytest

_GHG_DIR = Path(__file__).resolve().parents[1]

_PUBLISHED_2024 = _GHG_DIR / 'GHG_national_Cornerstone_2024_egrid.yaml'
_NOWCAST_2024 = _GHG_DIR / 'GHG_national_Cornerstone_nowcast_2024_egrid.yaml'


@pytest.mark.parametrize(
    ('path', 'parent_include'),
    [
        (_PUBLISHED_2024, '!include:GHG_national_Cornerstone_2024.yaml'),
        (_NOWCAST_2024, '!include:GHG_national_Cornerstone_nowcast_2024.yaml'),
    ],
)
def test_egrid_2024_yaml_post_854_schema_shape(
    path: Path,
    parent_include: str,
) -> None:
    text = path.read_text(encoding='utf-8')
    assert parent_include in text

    # industry_spec nest uses naics: (not bare NAICS_6 at the wrong level)
    assert 'industry_spec:' in text
    assert 'naics:' in text
    # Wrong-level bare marker from pre-#854 overlays.
    assert 'industry_spec:\n    NAICS_6:' not in text
    assert 'industry_spec:\n  NAICS_6:' not in text

    # activity_schema uses nested naics year/hierarchy (not scalar NAICS_2017_Code)
    assert 'activity_schema:' in text
    assert 'NAICS_2017_Code' not in text
    assert 'year: 2017' in text
    assert 'hierarchy: flat' in text
    # Nested under activity_schema → naics
    assert 'activity_schema:\n        naics:' in text
