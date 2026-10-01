"""Facility coverage bands for attribution gating (#928, #1040, #1060).

``coverage`` is GHGRP combustion floor / (floor + NEI-only below the GHGRP
threshold + GHGRP emissions reported outside the combustion subparts). The
last term (#1060) is what the sector's facilities report under source-category
subparts (cement H, lime S, glass N, iron and steel Q, refineries Y, ...).
Those subparts report kiln and furnace fuel together with process emissions,
so the fuel in them is invisible to the facility fuel shares; a sector whose
reports sit mostly there keeps the MECS shares. Attribution may use facility
weights only where that share clears
:data:`ATTRIBUTION_MIN_COVERAGE`; the downward (vector) gate stays at coverage
>= 0.95 and unresolved <= 0.05.

Hybrid production modes (#1040) use :func:`modes_median_freeze`: one
facility-vs-MECS side per sector from median coverage over
:data:`MODE_FREEZE_YEARS`, with floor vs vector taken from native modes in
:data:`MODE_RECIPE_YEAR`. Per-year native modes remain available via
:func:`modes_by_sector` for diagnostics.

NEI CO2 ends at 2022 (#932). For later inventory years, pass ``nei_year=2022``
so the coverage map is carried forward until #970.
"""

from __future__ import annotations

import logging
import os
from functools import lru_cache
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd
import stewi

from bedrock.extract.stewifbs.facility_combustion import (
    FACILITY_SCOPE_PREFIXES,
    GHGRP_EXCLUDED_SUBPARTS,
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

#: Span over which #1040 freezes one Hybrid mode per sector (median coverage).
MODE_FREEZE_YEARS = tuple(range(2017, 2025))
#: Native floor/vector recipe year when median coverage clears the gate.
MODE_RECIPE_YEAR = 2022
#: Last NEI inventory year with CO2 (#932); later method years reuse it.
NEI_LAST_YEAR = 2022

FACILITY_MODES = frozenset({'facility_floor', 'facility_vector'})
MECS_MODE = 'keep_prior'

#: GHGRP subparts whose fuel the facility fuel shares can see: general
#: stationary combustion (C) and oil and gas combustion (W).
COMBUSTION_SUBPARTS = frozenset({'C', 'W'})

#: Version of the coverage definition; part of the bands cache filename, so a
#: definition change never reuses bands cached under the old one.
COVERAGE_DEFINITION_VERSION = (
    2  # 2: process-subpart emissions in the denominator (#1060)
)


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


def ghgrp_process_subparts_by_sector(year: int) -> pd.Series:
    """GHGRP emissions outside the combustion subparts by BEA detail, Mt CO2e.

    Everything a sector's facilities report under subparts other than C and W
    (and the excluded power subpart D): process emissions plus any kiln or
    furnace fuel those subparts report with them (#1060).
    """
    gwp = {str(k): float(v) for k, v in GWP100_AR6_CEDA.items()}
    to_bea = _naics_to_bea_detail()
    flows = stewi.getInventory(
        'GHGRP', year=year, stewiformat='flowbyprocess', download_if_missing=True
    )
    other = flows[
        ~flows['Process'].isin(COMBUSTION_SUBPARTS | GHGRP_EXCLUDED_SUBPARTS)
        & flows['FlowName'].isin(GHGRP_FLOW_MAP)
    ].copy()
    other['CO2e'] = other['FlowAmount'] * other['FlowName'].map(GHGRP_FLOW_MAP).map(gwp)
    facilities = stewi.getInventoryFacilities('GHGRP', year, download_if_missing=True)[
        ['FacilityID', 'NAICS', 'State']
    ]
    other = other.merge(facilities, on='FacilityID', how='left')
    other['sector'] = other['NAICS'].map(lambda n: _bea_of(n, to_bea))
    other = _drop_outside_geography(other, 'CO2e', 'GHGRP process subparts', year)
    resolved = other.dropna(subset=['sector'])
    out = resolved.groupby('sector')['CO2e'].sum() / 1e9
    return out.drop(index='221100', errors='ignore')


def coverage_from_components(
    ghgrp_Mt: pd.Series,
    nei_below_Mt: pd.Series,
    nei_above_Mt: pd.Series,
    process_subparts_Mt: pd.Series,
) -> pd.DataFrame:
    """Coverage and unresolved share from the per-sector components, Mt CO2e.

    ``coverage`` = GHGRP combustion / (GHGRP combustion + NEI below the GHGRP
    threshold + GHGRP process-subpart emissions). ``unresolved`` = NEI above
    the threshold not matched to GHGRP / combustion total. ``total_Mt`` (the
    vector-test size) stays combustion only. Sectors with no facility
    combustion are dropped, as before.
    """
    out = pd.DataFrame(
        {
            'ghgrp_Mt': ghgrp_Mt,
            'nei_below_Mt': nei_below_Mt,
            'nei_above_Mt': nei_above_Mt,
        }
    ).fillna(0.0)
    out['total_Mt'] = out.sum(axis=1)
    out = out[out['total_Mt'] > 0]
    out['process_subparts_Mt'] = process_subparts_Mt.reindex(out.index).fillna(0.0)
    out['coverage'] = out['ghgrp_Mt'] / (
        out['ghgrp_Mt'] + out['nei_below_Mt'] + out['process_subparts_Mt']
    )
    out['unresolved'] = out['nei_above_Mt'] / out['total_Mt']
    return out


def ghgrp_subpart_W_by_sector(year: int, *, nei_year: int | None = None) -> pd.Series:
    """Subpart W combustion by BEA detail, Mt CO2e, for one year.

    *nei_year* is only used because :func:`facility_sectors` loads both
    inventories; pass the coverage NEI year (e.g. 2022 for 2023/24) so a
    missing NEI_<ghgrp_year> facility file is not requested.
    """
    burned = ghgrp_subpart_w.subpart_W_combustion((year,))
    if burned.empty:
        return pd.Series(dtype=float)
    year_rows = burned[burned['year'] == year]
    if year_rows.empty:
        return pd.Series(dtype=float)
    roster_nei = int(nei_year if nei_year is not None else year)
    _nei_sectors, ghgrp_sectors = facility_sectors(roster_nei, year)
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


def ghgrp_combustion_floor(year: int, *, nei_year: int | None = None) -> pd.Series:
    """Subpart C + subpart W combustion floor by BEA sector, Mt CO2e."""
    subpart_c = ghgrp_subpart_C_by_sector(year)
    subpart_w = ghgrp_subpart_W_by_sector(year, nei_year=nei_year)
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
    floor = ghgrp_combustion_floor(year, nei_year=nei_year)

    nei = union[
        (union['source'] == 'NEI') & union['fuel_class'].isin(COMBUSTION_FUEL_CLASSES)
    ]
    per_facility = nei.groupby(['FacilityID', 'sector'])['CO2e'].sum().reset_index()
    big = per_facility['CO2e'] > GHGRP_THRESHOLD_KG

    out = coverage_from_components(
        floor,
        per_facility[~big].groupby('sector')['CO2e'].sum() / 1e9,
        per_facility[big].groupby('sector')['CO2e'].sum() / 1e9,
        ghgrp_process_subparts_by_sector(year),
    )
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


def nei_year_for_method(method_year: int) -> int:
    """NEI inventory year for a method year (hold at 2022 after NEI CO2 ends)."""
    return NEI_LAST_YEAR if int(method_year) > NEI_LAST_YEAR else int(method_year)


def _coverage_bands_cache_dir() -> Path:
    override = os.environ.get('BEDROCK_FACILITY_COVERAGE_CACHE', '').strip()
    root = (
        Path(override)
        if override
        else (Path.home() / '.cache' / 'bedrock' / 'facility_coverage_bands')
    )
    root.mkdir(parents=True, exist_ok=True)
    return root


def load_or_build_coverage_bands(
    year: int,
    *,
    nei_year: int | None = None,
    refresh: bool = False,
) -> pd.DataFrame:
    """:func:`facility_coverage_bands` with an on-disk parquet cache."""
    year = int(year)
    nei_year = int(nei_year if nei_year is not None else nei_year_for_method(year))
    path = _coverage_bands_cache_dir() / (
        f'bands_v{COVERAGE_DEFINITION_VERSION}_{year}_nei{nei_year}.parquet'
    )
    if path.exists() and not refresh:
        return pd.read_parquet(path)
    bands = facility_coverage_bands(year, nei_year=nei_year)
    bands.to_parquet(path, index=False)
    logger.info('Cached coverage bands %s', path)
    return bands


def median_freeze_modes_from_tables(
    coverage_by_year: dict[int, dict[str, float]],
    native_modes: dict[int, dict[str, str]],
    *,
    recipe_year: int,
    min_coverage: float = ATTRIBUTION_MIN_COVERAGE,
) -> dict[str, str]:
    """One Hybrid mode per sector from median coverage (#1040 A2).

    Facility vs MECS comes from median coverage over the span vs
    *min_coverage*. Floor vs vector comes from native modes in *recipe_year*
    when that mode is facility_*; otherwise ``facility_floor``.
    """
    years = sorted(coverage_by_year)
    sectors: set[str] = set()
    for y in years:
        sectors |= set(coverage_by_year[y])
        sectors |= set(native_modes.get(y, {}))

    late_modes = native_modes.get(int(recipe_year), {})
    frozen: dict[str, str] = {}
    for sector in sectors:
        covs = [
            coverage_by_year[y][sector]
            for y in years
            if sector in coverage_by_year[y]
            and np.isfinite(coverage_by_year[y][sector])
        ]
        if not covs:
            frozen[sector] = MECS_MODE
            continue
        med = float(np.median(covs))
        if med < min_coverage:
            frozen[sector] = MECS_MODE
            continue
        late = late_modes.get(sector, 'facility_floor')
        frozen[sector] = late if late in FACILITY_MODES else 'facility_floor'
    return frozen


@lru_cache(maxsize=8)
def _modes_median_freeze_cached(
    years: tuple[int, ...],
    recipe_year: int,
    min_coverage: float,
) -> tuple[tuple[str, str], ...]:
    coverage_by_year: dict[int, dict[str, float]] = {}
    native_modes: dict[int, dict[str, str]] = {}
    for y in years:
        bands = load_or_build_coverage_bands(y, nei_year=nei_year_for_method(y))
        coverage_by_year[y] = {
            str(cast(Any, row.sector)): float(cast(Any, row.coverage))
            for row in bands.itertuples(index=False)
        }
        if y == recipe_year:
            native_modes[y] = modes_by_sector(bands, min_coverage=min_coverage)

    frozen = median_freeze_modes_from_tables(
        coverage_by_year,
        native_modes,
        recipe_year=recipe_year,
        min_coverage=min_coverage,
    )
    logger.info(
        'Median-freeze modes (years=%s-%s, recipe=%s, min_coverage=%.2f): %s',
        min(years),
        max(years),
        recipe_year,
        min_coverage,
        pd.Series(frozen).value_counts().to_dict(),
    )
    return tuple(sorted(frozen.items()))


def modes_median_freeze(
    years: tuple[int, ...] = MODE_FREEZE_YEARS,
    *,
    recipe_year: int = MODE_RECIPE_YEAR,
    min_coverage: float = ATTRIBUTION_MIN_COVERAGE,
    refresh: bool = False,
) -> dict[str, str]:
    """Frozen Hybrid modes for the facility FBS span (#1040 A2).

    Builds (or loads cached) coverage bands for each year in *years*, takes
    native modes in *recipe_year* for floor vs vector, and returns one mode
    map applied to every method year.
    """
    years = tuple(int(y) for y in years)
    recipe_year = int(recipe_year)
    min_coverage = float(min_coverage)
    if recipe_year not in years:
        raise ValueError(f'recipe_year {recipe_year} not in years {years}')
    if refresh:
        _modes_median_freeze_cached.cache_clear()
        for y in years:
            load_or_build_coverage_bands(
                y, nei_year=nei_year_for_method(y), refresh=True
            )
    return dict(_modes_median_freeze_cached(years, recipe_year, min_coverage))
