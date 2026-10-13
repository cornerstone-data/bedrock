"""VIUS construction truck sector mapping (Phase 0 repro + crosswalk guards).

Documents the ~45 Mt silent drop on the pinned transportation GHG FBS when
NAICS ``236`` expands to ``236115`` and is dropped on ``INDUSTRIES`` reindex.
Crosswalk tests lock VIUS/UMD construction rows to BEA ``23`` (#1093 pattern).
"""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.transform.allocation import derived
from bedrock.utils.config.usa_config import USAConfig
from bedrock.utils.mapping.sectormapping import get_activitytosector_mapping
from bedrock.utils.taxonomy.cornerstone.industries import INDUSTRIES

PINNED_BUGGY_TRANSPORT_FBS = (
    'GHG_national_Cornerstone_nowcast_facilities_transportation_2024_'
    'v0.5.0_011f41f.parquet'
)
TRANSPORT_FBS_BASE = 'GHG_national_Cornerstone_nowcast_facilities_transportation_2024'

VIUS_CONSTRUCTION_ACTIVITIES = (
    'Construction--Residential',
    'Construction--Non-residential',
    'Other Construction',
)

UMD_TRUCK_ACTIVITIES = (
    'Light-Duty Trucks',
    'Medium- and Heavy-Duty Trucks',
    'Medium- and Heavy-Duty Trucks - Distillate Fuel Oil',
)

_CONSTRUCTION_NAICS_PREFIXES = ('236', '237', '238')
_BAD_CONSTRUCTION_NAICS = frozenset({'2361', '2362', '237', '238'})


def _co2_flow_mask(fbs: pd.DataFrame) -> pd.Series:
    return fbs['Flowable'].isin(['Carbon dioxide', 'CO2'])


def _sector_starts_with(sectors: pd.Series, prefixes: tuple[str, ...]) -> pd.Series:
    as_str = sectors.astype(str)
    mask = pd.Series(False, index=sectors.index)
    for prefix in prefixes:
        mask |= as_str.str.startswith(prefix)
    return mask


def _sum_co2_kg(fbs: pd.DataFrame, sector_mask: pd.Series) -> float:
    return float(fbs.loc[sector_mask & _co2_flow_mask(fbs), 'FlowAmount'].sum())


def _kg_co2_lost_on_industry_reindex(mapped_fbs: pd.DataFrame) -> float:
    """CO2 kg on sectors that ``E_usa.reindex(INDUSTRIES)`` would zero out."""
    co2 = mapped_fbs.loc[_co2_flow_mask(mapped_fbs)]
    by_sector = co2.groupby('SectorProducedBy', as_index=False)['FlowAmount'].sum()
    industry_codes = {str(sector) for sector in INDUSTRIES}
    pre = float(by_sector['FlowAmount'].sum())
    post = float(
        by_sector.loc[
            by_sector['SectorProducedBy'].astype(str).isin(industry_codes),
            'FlowAmount',
        ].sum()
    )
    return pre - post


@pytest.mark.eeio_integration
def test_pinned_transport_fbs_construction_co2_drops_on_reindex(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Phase 0: pinned parquet holds ~45 Mt on NAICS 236* that reindex drops."""
    monkeypatch.setattr(
        derived,
        'get_usa_config',
        lambda: USAConfig.model_validate(
            {'cornerstone_ghg_fbs_filename': PINNED_BUGGY_TRANSPORT_FBS}
        ),
    )
    fbs = derived._load_cornerstone_ghg_fbs_from_gcs(base_name=TRANSPORT_FBS_BASE)
    sectors = fbs['SectorProducedBy'].astype(str)
    raw_construction = _sum_co2_kg(
        fbs, _sector_starts_with(sectors, _CONSTRUCTION_NAICS_PREFIXES)
    )
    assert (
        raw_construction > 40e9
    ), f'expected >40 Mt CO2 on NAICS 236*/237*/238*; got {raw_construction / 1e9:.2f} Mt'

    mapped = derived.map_fbs_sectors_to_model_schema(fbs)
    mapped_sectors = mapped['SectorProducedBy'].astype(str)
    assert _sum_co2_kg(mapped, mapped_sectors == '236115') > 35e9

    lost_kg = _kg_co2_lost_on_industry_reindex(mapped)
    assert (
        lost_kg > 35e9
    ), f'expected >35 Mt CO2 lost on INDUSTRIES reindex; got {lost_kg / 1e9:.2f} Mt'

    industry_codes = {str(sector) for sector in INDUSTRIES}
    assert '236115' not in industry_codes
    on_236115 = _sum_co2_kg(mapped, mapped_sectors == '236115')
    assert (
        lost_kg >= on_236115 * 0.9
    ), 'most reindex loss is unmappable 236115, not 233*'


def test_vius_construction_activities_map_to_bea_23() -> None:
    mapping = get_activitytosector_mapping('BTS_VIUS')
    for activity in VIUS_CONSTRUCTION_ACTIVITIES:
        rows = mapping[mapping['Activity'] == activity]
        assert len(rows) == 1, f'{activity} should have one crosswalk row'
        row = rows.iloc[0]
        assert row['SectorSourceName'] == 'BEA_2017_Code'
        assert str(row['Sector']) == '23'
        assert str(row['Sector']) not in _BAD_CONSTRUCTION_NAICS


@pytest.mark.parametrize('activity', UMD_TRUCK_ACTIVITIES)
def test_umd_truck_activities_use_single_bea_23_not_naics_construction(
    activity: str,
) -> None:
    mapping = get_activitytosector_mapping('UMD_GHGIA_transportation')
    rows = mapping[mapping['Activity'] == activity]
    bea_23 = rows[
        (rows['SectorSourceName'] == 'BEA_2017_Code')
        & (rows['Sector'].astype(str) == '23')
    ]
    assert len(bea_23) == 1, f'{activity} should have exactly one BEA 23 row'
    naics_construction = rows[rows['Sector'].astype(str).isin(_BAD_CONSTRUCTION_NAICS)]
    assert naics_construction.empty, (
        f'{activity} still lists NAICS construction rows: '
        f'{naics_construction["Sector"].tolist()}'
    )
