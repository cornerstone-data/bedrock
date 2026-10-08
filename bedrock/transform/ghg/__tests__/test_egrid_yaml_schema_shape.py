"""Schema-shape checks for eGRID Cornerstone GHG method YAMLs / catalog."""

from __future__ import annotations

from pathlib import Path

from bedrock.utils.config.common import get_catalog_info

_GHG_DIR = Path(__file__).resolve().parents[1]
_CATALOG = (
    Path(__file__).resolve().parents[3] / 'utils' / 'config' / 'source_catalog.yaml'
)

_PUBLISHED_2024 = _GHG_DIR / 'GHG_national_Cornerstone_2024_egrid.yaml'
_NOWCAST_BASE_2024 = _GHG_DIR / 'GHG_national_Cornerstone_nowcast_2024.yaml'


def test_egrid_catalog_activity_schema() -> None:
    info = get_catalog_info('EPA_eGRID_electric')
    assert info.get('data_format') == 'FBS_outside_flowsa'
    assert info.get('activity_schema') == {'naics': {'year': 2017, 'hierarchy': 'flat'}}
    catalog_text = _CATALOG.read_text(encoding='utf-8')
    assert 'EPA_eGRID_electric:' in catalog_text
    assert 'NAICS_2017_Code' not in catalog_text


def test_nowcast_2024_base_uses_catalog_egrid() -> None:
    text = _NOWCAST_BASE_2024.read_text(encoding='utf-8')
    assert 'EPA_eGRID_electric:' in text
    assert 'UMD_GHGIA_T_3_7' not in text
    assert 'Electric Power Sector: Electric Power' not in text
    # Static schema lives in source_catalog, not the method YAML.
    assert 'activity_schema:' not in text
    assert 'data_format: FBS_outside_flowsa' not in text
    assert 'eGRID: *ghgi_year' in text


def test_published_2024_egrid_overlay_uses_catalog() -> None:
    text = _PUBLISHED_2024.read_text(encoding='utf-8')
    assert '!include:GHG_national_Cornerstone_2024.yaml' in text
    assert 'industry_spec:' not in text
    assert 'activity_schema:' not in text
    assert 'data_format: FBS_outside_flowsa' not in text
    assert 'EPA_eGRID_electric:' in text
