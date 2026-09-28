"""Facility coverage bands for attribution gating (#928).

``coverage`` is GHGRP combustion floor / (floor + NEI-only below the GHGRP
threshold). Attribution may use facility weights only where that share clears
:data:`ATTRIBUTION_MIN_COVERAGE`; the downward (vector) gate stays at coverage
>= 0.95 and unresolved <= 0.05.

NEI CO2 ends at 2022 (#932). For later inventory years, pass ``nei_year=2022``
so the coverage map is carried forward until #970.
"""

from __future__ import annotations

import logging
from typing import Any, cast

import numpy as np
import pandas as pd
import stewi

from bedrock.extract.stewifbs.facility_combustion import (
    FACILITY_SCOPE_PREFIXES,
    GHGRP_FLOW_MAP,
    build_facility_combustion,
    facility_sectors,
)
from bedrock.transform.ghg import ghgrp_subpart_w
from bedrock.utils.config.common import load_crosswalk
from bedrock.utils.emissions.gwp import GWP100_AR6_CEDA
from bedrock.utils.mapping.location import BEA_ECONOMIC_TERRITORY

logger = logging.getLogger(__name__)

#: GHGRP reporting threshold, 25,000 t CO2e, in kg (stewi units).
GHGRP_THRESHOLD_KG = 25_000 * 1_000

COMBUSTION_FUEL_CLASSES = ('purchased', 'self_supplied')

#: Attribution gate for #928 / #965 -- facility vs MECS/Use.
ATTRIBUTION_MIN_COVERAGE = 0.8

#: Downward (vector) gate: high coverage and low unresolved share.
VECTOR_COVERAGE_FLOOR = 0.95
VECTOR_UNRESOLVED_CEILING = 0.05

#: Below this facility combustion total (Mt), a sector cannot be tested for
#: the vector exception and stays a floor.
MIN_MT_FOR_VECTOR_TEST = 0.5


def _naics_to_bea_detail() -> pd.Series:
    """NAICS 2017 -> BEA detail for unambiguous codes only."""
    pairs = (
        load_crosswalk('NAICS_to_BEA_Crosswalk_2017')[
            ['NAICS_2017_Code', 'BEA_2017_Detail_Code']
        ]
        .dropna()
        .drop_duplicates()
    )
    fanout = pairs.groupby('NAICS_2017_Code')['BEA_2017_Detail_Code'].nunique()
    unique = pairs[pairs['NAICS_2017_Code'].isin(fanout[fanout == 1].index)]
    return unique.set_index('NAICS_2017_Code')['BEA_2017_Detail_Code']


def _bea_of(naics: object, to_bea: pd.Series) -> str | None:
    text = str(naics)
    lookup = set(to_bea.index)
    for length in (6, 5, 4, 3, 2):
        prefix = text[:length]
        if prefix in lookup:
            return str(to_bea[prefix])
    return None


def bea_detail_for_naics(naics: object) -> str | None:
    """Map a NAICS code to an unambiguous BEA detail, or None."""
    return _bea_of(naics, _naics_to_bea_detail())


def _drop_outside_geography(
    frame: pd.DataFrame, amount: str, label: str, year: int
) -> pd.DataFrame:
    keep = frame['State'].isin(BEA_ECONOMIC_TERRITORY)
    dropped = frame[~keep]
    if not dropped.empty:
        logger.info(
            '%s %d: dropping %.2f Mt over %d facilities outside BEA territory.',
            label,
            year,
            dropped[amount].sum() / 1e9,
            dropped['FacilityID'].nunique(),
        )
    return frame[keep]


def ghgrp_subpart_C_by_sector(year: int) -> pd.Series:
    """GHGRP subpart C combustion by BEA detail, Mt CO2e, for one year."""
    gwp = {str(k): float(v) for k, v in GWP100_AR6_CEDA.items()}
    to_bea = _naics_to_bea_detail()
    flows = stewi.getInventory(
        'GHGRP', year=year, stewiformat='flowbyprocess', download_if_missing=True
    )
    combustion = flows[
        (flows['Process'] == 'C') & flows['FlowName'].isin(GHGRP_FLOW_MAP)
    ].copy()
    combustion['CO2e'] = combustion['FlowAmount'] * combustion['FlowName'].map(
        GHGRP_FLOW_MAP
    ).map(gwp)
    facilities = stewi.getInventoryFacilities('GHGRP', year, download_if_missing=True)[
        ['FacilityID', 'NAICS', 'State']
    ]
    combustion = combustion.merge(facilities, on='FacilityID', how='left')
    combustion['sector'] = combustion['NAICS'].map(lambda n: _bea_of(n, to_bea))
    combustion = _drop_outside_geography(combustion, 'CO2e', 'GHGRP subpart C', year)
    resolved = combustion.dropna(subset=['sector'])
    out = resolved.groupby('sector')['CO2e'].sum() / 1e9
    return out.drop(index='221100', errors='ignore')


def ghgrp_subpart_W_by_sector(year: int) -> pd.Series:
    """Subpart W combustion by BEA detail, Mt CO2e, for one year."""
    burned = ghgrp_subpart_w.subpart_W_combustion((year,))
    if burned.empty:
        return pd.Series(dtype=float)
    year_rows = burned[burned['year'] == year]
    if year_rows.empty:
        return pd.Series(dtype=float)
    _nei_sectors, ghgrp_sectors = facility_sectors(year, year)
    placed = (
        year_rows.groupby('FacilityID')['CO2e']
        .sum()
        .reset_index()
        .join(ghgrp_sectors, on='FacilityID')
    )
    unknown = placed['State'].isna()
    placed = _drop_outside_geography(placed[~unknown], 'CO2e', 'subpart W', year)
    resolved = placed.dropna(subset=['sector'])
    out = resolved.groupby('sector')['CO2e'].sum() / 1e9
    return out.drop(index='221100', errors='ignore')


def ghgrp_combustion_floor(year: int) -> pd.Series:
    """Subpart C + subpart W combustion floor by BEA sector, Mt CO2e."""
    subpart_c = ghgrp_subpart_C_by_sector(year)
    subpart_w = ghgrp_subpart_W_by_sector(year)
    if subpart_w.empty:
        return subpart_c
    return subpart_c.add(subpart_w, fill_value=0.0)


def facility_coverage_bands(
    year: int,
    *,
    nei_year: int | None = None,
    min_Mt: float = MIN_MT_FOR_VECTOR_TEST,
    coverage_floor: float = VECTOR_COVERAGE_FLOOR,
    unresolved_ceiling: float = VECTOR_UNRESOLVED_CEILING,
) -> pd.DataFrame:
    """Per-sector coverage, unresolved share, and floor/vector verdict.

    *nei_year* defaults to *year*; pass 2022 when building 2023/2024 methods.
    """
    year = int(year)
    nei_year = int(nei_year if nei_year is not None else year)
    union = build_facility_combustion(
        year,
        nei_year=nei_year,
        sector_prefixes=FACILITY_SCOPE_PREFIXES,
        exclude_sectors=('221100',),
    )
    floor = ghgrp_combustion_floor(year)

    nei = union[
        (union['source'] == 'NEI') & union['fuel_class'].isin(COMBUSTION_FUEL_CLASSES)
    ]
    per_facility = nei.groupby(['FacilityID', 'sector'])['CO2e'].sum().reset_index()
    big = per_facility['CO2e'] > GHGRP_THRESHOLD_KG

    out = pd.DataFrame(
        {
            'ghgrp_Mt': floor,
            'nei_below_Mt': per_facility[~big].groupby('sector')['CO2e'].sum() / 1e9,
            'nei_above_Mt': per_facility[big].groupby('sector')['CO2e'].sum() / 1e9,
        }
    ).fillna(0.0)
    out['total_Mt'] = out.sum(axis=1)
    out = out[out['total_Mt'] > 0]
    out['coverage'] = out['ghgrp_Mt'] / (out['ghgrp_Mt'] + out['nei_below_Mt'])
    out['unresolved'] = out['nei_above_Mt'] / out['total_Mt']
    testable = out['total_Mt'] >= min_Mt
    out['verdict'] = np.where(
        testable
        & (out['coverage'] >= coverage_floor)
        & (out['unresolved'] <= unresolved_ceiling),
        'vector',
        'floor',
    )
    out = out.rename_axis('sector').reset_index()
    out['year'] = year
    out['nei_year'] = nei_year
    return out


def attribution_mode(
    coverage: float,
    verdict: str,
    *,
    min_coverage: float = ATTRIBUTION_MIN_COVERAGE,
) -> str:
    """``facility_vector`` / ``facility_floor`` / ``keep_prior`` for one sector."""
    if not np.isfinite(coverage) or coverage < min_coverage:
        return 'keep_prior'
    if verdict == 'vector':
        return 'facility_vector'
    return 'facility_floor'


def modes_by_sector(
    bands: pd.DataFrame,
    *,
    min_coverage: float = ATTRIBUTION_MIN_COVERAGE,
) -> dict[str, str]:
    """BEA sector -> attribution mode from a :func:`facility_coverage_bands` table."""
    out: dict[str, str] = {}
    for row in bands.itertuples(index=False):
        sector = str(cast(Any, row.sector))
        coverage = float(cast(Any, row.coverage))
        verdict = str(cast(Any, row.verdict))
        out[sector] = attribution_mode(coverage, verdict, min_coverage=min_coverage)
    return out
