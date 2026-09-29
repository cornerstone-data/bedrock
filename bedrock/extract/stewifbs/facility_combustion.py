"""GHGRP/NEI facility combustion as an FBS attribution source.

Combines stewi GHGRP and NEI (FRS prefer-GHGRP), classifies fuel via
:mod:`bedrock.transform.ghg.ghgrp_subpart_w` and NEI SCCs, and assigns BEA
detail sectors for method YAML selection on ``Flowable``.

This code includes:

- **Mobile SCCs dropped:** NEI ``Process`` codes whose first two digits are
  ``22`` (aircraft at airports and other mobile point sources) are excluded.
  SCC is gone after aggregation, so this cannot be done in YAML
  ``exclusion_fields``.
- **NEI ``fuel_class`` / Flowable from SCC only for 2021+**
  (``NEI_FUEL_CLASS_FIRST_YEAR``). Before 2021 the SCC coding parked almost
  all on-site CO2 on process branches, so those digits are not read for fuel.
  Pre-2021 method years keep **that year's NEI levels** and assign NEI
  ``Flowable`` from **2021/2022 twin shares** (FacilityID mean, else NAICS-6,
  else sector). Twins are **NEI-only** — not applied to GHGRP (process /
  fugitive subpart mass must not be painted as Coal/Petroleum combustion).
- **Prefer GHGRP** on ``FRS_ID``; NEI-only facilities fill the residual.
  Same-site address fallback recovers links FRS missed.
- **CO2e for weights:** GHGRP CO2/CH4/N2O collapsed with IPCC AR5 100-year
  GWPs so multi-gas facilities can share with NEI CO2 on the same basis as
  UMD GHGIA (AR5; see
  https://ghgi.cgs.umd.edu/Web%20Content/Chapters/GHGIA_FullReport_2026.pdf).
  Prefer-GHGRP ``Flowable`` stays the bare fuel name (``Petroleum``,
  ``Natural Gas``, ``Coal``).
- **Share-weight rows:** GHGRP lease/plant natural gas is appended for YAML
  selection only as ``Flowable: Natural Gas - lease and plant`` (FBS has no
  ``Description`` field). Those tonnes are excluded from the logged prefer-GHGRP
  total.
- **Electric power** is dropped in this module if ``exclude_sectors``
  from the method YAML lists ``221100``.
"""

from __future__ import annotations

import re
from functools import lru_cache
from typing import Any

import facilitymatcher
import numpy as np
import pandas as pd
import stewi
from facilitymatcher import colocation

from bedrock.analysis.time_series_B_matrix.B_change_diagnostics import (
    SPLIT_BASIS_NAICS6,
    SPLIT_BASIS_NONE,
    SPLIT_BASIS_SECTOR,
    _apply_combustion_process_share,
)
from bedrock.transform.ghg import ghgrp_subpart_w
from bedrock.utils.config.common import load_crosswalk
from bedrock.utils.emissions.gwp import GWP100_AR5
from bedrock.utils.logging.flowsa_log import log
from bedrock.utils.mapping.location import filter_to_model_geography

ONSITE_SCC_BRANCHES = ('1', '2', '3')
COMBUSTION_SCC_BRANCHES = ('1', '2')
MOBILE_SCC_PREFIX = '22'
GHGRP_EXCLUDED_SUBPARTS = frozenset({'D'})
PROCESS_GAS_SCC_LEVEL3 = '007'
FACILITY_SCOPE_PREFIXES = ('21', '22', '31', '32', '33')
NEI_FUEL_CLASS_FIRST_YEAR = 2021
_FUEL_FLOWABLES = ('Coal', 'Natural Gas', 'Petroleum', 'Other')

# GWP100_AR5.
GHGRP_FLOW_MAP = {
    'Carbon Dioxide': 'CO2',
    'Methane': 'CH4',
    'Nitrous Oxide': 'N2O',
}

_FUEL_TYPE_TO_FLOWABLE = (
    (
        re.compile(
            r'natural gas|field gas|process gas|pipeline|methane', re.IGNORECASE
        ),
        'Natural Gas',
    ),
    (
        re.compile(r'coal|coke|lignite|anthracite|bituminous', re.IGNORECASE),
        'Coal',
    ),
    (
        re.compile(
            r'distillate|diesel|fuel oil|gasoline|kerosene|petroleum|'
            r'propane|lpg|still gas|naphtha',
            re.IGNORECASE,
        ),
        'Petroleum',
    ),
)

# EPA SCC Level 3 (chars 4-6) → Flowable by major category (char 1).
_NEI_SCC_FUEL_EXTERNAL = {
    '001': 'Coal',
    '002': 'Coal',
    '003': 'Coal',
    '004': 'Petroleum',
    '005': 'Petroleum',
    '006': 'Natural Gas',
    '007': 'Natural Gas',  # process gas; fuel_class still self_supplied
    '012': 'Petroleum',  # LPG
}
_NEI_SCC_FUEL_INTERNAL = {
    '001': 'Petroleum',  # distillate / diesel
    '002': 'Natural Gas',
    '003': 'Petroleum',  # gasoline
    '004': 'Petroleum',
    '005': 'Petroleum',  # LPG
}


#: Concordance columns other than 2017 that facility NAICS may be reported in.
#: GHGRP 2017 carries 2007 and 2012 codes (331111, 211111); GHGRP and NEI from
#: 2022 carry 2022 codes (322120, 212115).
OTHER_NAICS_VINTAGES = (
    'NAICS_2022_Code',
    'NAICS_2012_Code',
    'NAICS_2007_Code',
    'NAICS_2002_Code',
)


@lru_cache(maxsize=1)
def naics_to_2017() -> dict[str, str]:
    """Recode 6-digit NAICS from other vintages to one NAICS 2017 code.

    The facility FBS routes emit facility NAICS on a NAICS 2017 schema. A code
    from another vintage matches no 2017 code there: ``322120`` (NAICS 2022,
    paper mills) is neither 2017's ``322121`` nor ``322122``. The rollup to
    ``industry_spec`` then keeps it as its own key, and it gets no share.

    An existing 2017 code is never recoded. Where one code maps to several 2017
    codes, the lowest is used, but only when all of them map to the same BEA
    detail. All such cases so far also roll up to the same ``industry_spec``
    key, so the choice doesn't move a share. A code whose 2017 codes span
    several BEA details is left as reported.
    """
    concordance = load_crosswalk('NAICS_Year_Concordance')
    current = set(concordance['NAICS_2017_Code'].dropna().str.strip())
    to_bea = (
        load_crosswalk('NAICS_to_BEA_Crosswalk_2017')[
            ['NAICS_2017_Code', 'BEA_2017_Detail_Code']
        ]
        .dropna()
        .drop_duplicates()
        .groupby('NAICS_2017_Code')['BEA_2017_Detail_Code']
        .agg(frozenset)
    )
    recode: dict[str, str] = {}
    for column in OTHER_NAICS_VINTAGES:
        pairs = concordance[[column, 'NAICS_2017_Code']].dropna()
        pairs = pairs[pairs[column].str.len() == 6]
        for code, group in pairs.groupby(column)['NAICS_2017_Code']:
            code = str(code).strip()
            if code in current or code in recode:
                continue
            targets = sorted(set(group.str.strip()))
            beas = {b for t in targets for b in to_bea.get(t, frozenset())}
            if len(beas) == 1:
                recode[code] = targets[0]
    return recode


def recode_naics_to_2017(naics: pd.Series) -> pd.Series:
    """Apply :func:`naics_to_2017` to a NAICS column; other codes unchanged."""
    text = naics.astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    recode = naics_to_2017()
    return text.map(lambda c: recode.get(c, c))


def facility_sectors(
    nei_year: int, ghgrp_year: int
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """NAICS rolled up to one BEA detail code, for the NEI year and the GHGRP year.

    Facility NAICS from other vintages are recoded to NAICS 2017 first
    (:func:`naics_to_2017`), so the ``NAICS`` column is on the 2017 schema the
    FBS routes emit.
    """
    crosswalk = (
        load_crosswalk('NAICS_to_BEA_Crosswalk_2017')[
            ['NAICS_2017_Code', 'BEA_2017_Detail_Code']
        ]
        .dropna()
        .drop_duplicates()
    )
    fanout = crosswalk.groupby('NAICS_2017_Code')['BEA_2017_Detail_Code'].nunique()
    to_bea = crosswalk[
        crosswalk['NAICS_2017_Code'].isin(fanout[fanout == 1].index)
    ].set_index('NAICS_2017_Code')['BEA_2017_Detail_Code']
    naics_lookup = set(to_bea.index)

    nei_facilities = stewi.getInventoryFacilities(
        'NEI', nei_year, download_if_missing=True
    )[['FacilityID', 'NAICS', 'State']].assign(inventory='NEI')
    ghgrp_facilities = stewi.getInventoryFacilities(
        'GHGRP', ghgrp_year, download_if_missing=True
    )[['FacilityID', 'NAICS', 'State']].assign(inventory='GHGRP')
    facilities = pd.concat([nei_facilities, ghgrp_facilities], ignore_index=True)
    reported = (
        facilities['NAICS'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    )
    recoded = reported.isin(naics_to_2017().keys())
    facilities['NAICS'] = recode_naics_to_2017(facilities['NAICS'])
    if recoded.any():
        log.info(
            'facility_sectors (NEI %d, GHGRP %d): recoded %d facilities from '
            'other NAICS vintages to NAICS 2017: %s',
            nei_year,
            ghgrp_year,
            int(recoded.sum()),
            reported[recoded].value_counts().head(8).to_dict(),
        )
    naics = facilities['NAICS'].astype(str)
    sector = pd.Series(pd.NA, index=facilities.index, dtype='object')
    for length in (6, 5, 4, 3, 2):
        prefix = naics.str[:length]
        # Longer prefixes are tried first, so a 6-digit hit is not overwritten.
        hit = sector.isna() & prefix.isin(naics_lookup)
        sector = sector.mask(hit, prefix.map(to_bea))
    facilities['sector'] = sector
    nei_sectors = facilities.loc[
        facilities['inventory'] == 'NEI', ['FacilityID', 'NAICS', 'State', 'sector']
    ].set_index('FacilityID')
    ghgrp_sectors = facilities.loc[
        facilities['inventory'] == 'GHGRP', ['FacilityID', 'NAICS', 'State', 'sector']
    ].set_index('FacilityID')
    return nei_sectors, ghgrp_sectors


def nei_onsite_co2(
    nei_raw: pd.DataFrame, nei_sectors: pd.DataFrame, *, use_scc: bool
) -> pd.DataFrame:
    """On-site NEI CO2. SCC fuel labels only when *use_scc* (NEI year 2021+)."""
    process = nei_raw['Process'].astype(str)
    nei_raw = nei_raw[
        (nei_raw['FlowName'] == 'Carbon Dioxide')
        & process.str[0].isin(ONSITE_SCC_BRANCHES)
        & (process.str[:2] != MOBILE_SCC_PREFIX)
    ]
    process = nei_raw['Process'].astype(str)
    if use_scc:
        branch = process.str[0]
        level3 = process.str[3:6]
        nei_raw = nei_raw.assign(
            fuel_class=np.where(
                ~branch.isin(COMBUSTION_SCC_BRANCHES),
                'process',
                np.where(
                    level3 == PROCESS_GAS_SCC_LEVEL3, 'self_supplied', 'purchased'
                ),
            ),
            Flowable='Other',
        )
        external = branch == '1'
        nei_raw.loc[external, 'Flowable'] = (
            level3.loc[external].map(_NEI_SCC_FUEL_EXTERNAL).fillna('Other')
        )
        internal = branch == '2'
        nei_raw.loc[internal, 'Flowable'] = (
            level3.loc[internal].map(_NEI_SCC_FUEL_INTERNAL).fillna('Other')
        )
    else:
        nei_raw = nei_raw.assign(fuel_class='unclassified', Flowable='Other')
    return (
        nei_raw.groupby(['FacilityID', 'fuel_class', 'Flowable'])['FlowAmount']
        .sum()
        .rename('CO2e')
        .reset_index()
        .join(nei_sectors, on='FacilityID')
        .assign(source='NEI')
    )


def _flowable_share_frame(labeled: pd.DataFrame) -> pd.DataFrame:
    """FacilityID × Flowable shares (rows sum to 1)."""
    wide = (
        labeled.groupby(['FacilityID', 'Flowable'])['CO2e']
        .sum()
        .unstack(fill_value=0)
        .reindex(columns=list(_FUEL_FLOWABLES), fill_value=0)
    )
    return wide.div(wide.sum(axis=1).replace(0.0, np.nan), axis=0).fillna(0.0)


def _group_mean_shares(
    facility_shares: pd.DataFrame, weight: pd.Series, group: pd.Series
) -> pd.DataFrame:
    """Mass-weighted mean Flowable shares by group key."""
    frame = facility_shares.join(weight.rename('_w'), how='inner').join(
        group.rename('_g'), how='inner'
    )
    frame = frame[frame['_g'].notna() & (frame['_w'] > 0)]
    if frame.empty:
        return pd.DataFrame(columns=list(_FUEL_FLOWABLES))
    num = (
        frame[list(_FUEL_FLOWABLES)]
        .multiply(frame['_w'], axis=0)
        .groupby(frame['_g'])
        .sum()
    )
    den = frame.groupby('_g')['_w'].sum().replace(0.0, np.nan)
    return num.div(den, axis=0).fillna(0.0)


def _scc_labeled_nei(year: int) -> pd.DataFrame:
    """NEI on-site CO2 with SCC Flowable labels for one twin anchor year."""
    raw = stewi.getInventory(
        'NEI', year, stewiformat='flowbyprocess', download_if_missing=True
    )
    if raw is None or getattr(raw, 'empty', True):
        raise ValueError(f'no NEI flowbyprocess for twin year {year}')
    sectors, _ = facility_sectors(year, year)
    return nei_onsite_co2(raw, sectors, use_scc=True)


@lru_cache(maxsize=1)
def _fuel_flowable_share_tables() -> dict[str, Any]:
    """2021/2022 Flowable twin tables (same anchors / cascade as the diagnostics
    combustion/process backcast).

    Levels are never taken from these tables — only shares. Facility shares are
    the mean of 2021 and 2022 where both exist, else 2021 alone; NAICS-6 and
    sector means are over the both-anchor set.
    """
    rows_a = _scc_labeled_nei(2021)
    rows_b = _scc_labeled_nei(2022)
    shares_a = _flowable_share_frame(rows_a)
    shares_b = _flowable_share_frame(rows_b)
    both_ids = shares_a.index.intersection(shares_b.index)
    facility = shares_a.copy()
    if len(both_ids):
        cols = list(_FUEL_FLOWABLES)
        facility.loc[both_ids, cols] = (
            shares_a.loc[both_ids, cols] + shares_b.loc[both_ids, cols]
        ).to_numpy() / 2.0

    meta_a = rows_a.drop_duplicates('FacilityID').set_index('FacilityID')[
        ['NAICS', 'sector']
    ]
    totals_a = rows_a.groupby('FacilityID')['CO2e'].sum()
    totals_b = rows_b.groupby('FacilityID')['CO2e'].sum()
    both_meta = meta_a.reindex(both_ids)
    both_shares = facility.reindex(both_ids)
    both_weight = (
        totals_a.reindex(both_ids).fillna(0.0) + totals_b.reindex(both_ids).fillna(0.0)
    ) / 2.0
    return {
        'facility': facility,
        'twin_both': set(both_ids),
        'twin_anchor_a': set(shares_a.index),
        'naics': _group_mean_shares(
            both_shares, both_weight, both_meta['NAICS'].astype(str).str[:6]
        ),
        'sector': _group_mean_shares(both_shares, both_weight, both_meta['sector']),
    }


def _lookup_flowable_share_row(
    facility_id: Any,
    naics: Any,
    sector: Any,
    tables: dict[str, Any],
) -> pd.Series | None:
    """FacilityID → NAICS-6 → sector cascade (test / simple path)."""
    fac = tables['facility']
    if facility_id in fac.index:
        return fac.loc[facility_id]
    n6 = str(naics)[:6] if pd.notna(naics) else ''
    if n6 and n6 in tables['naics'].index:
        return tables['naics'].loc[n6]
    if pd.notna(sector) and sector in tables['sector'].index:
        return tables['sector'].loc[sector]
    return None


def apply_pre2021_fuel_flowable_shares(
    rows: pd.DataFrame,
    tables: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Split ``Flowable='Other'`` using 2021/2022 twin shares; keep levels.

    Reuses :func:`bedrock.analysis.time_series_B_matrix.B_change_diagnostics._apply_combustion_process_share`
    for the FacilityID → NAICS-6 → sector cascade when full twin tables are
    present. Does not touch lease/plant or already-labeled fuels.
    """
    if rows.empty or not (rows['Flowable'].astype(str) == 'Other').any():
        return rows
    tables = tables or _fuel_flowable_share_tables()
    kept = rows.loc[rows['Flowable'].astype(str) != 'Other']
    other = rows.loc[rows['Flowable'].astype(str) == 'Other']

    # Production tables include twin membership sets; unit tests pass bare frames.
    if 'twin_both' in tables:
        keys = other.drop_duplicates('FacilityID').set_index('FacilityID')
        prior = pd.DataFrame(
            {
                'naics6': keys['NAICS'].map(
                    lambda v: str(v)[:6] if pd.notna(v) else pd.NA
                ),
                'sector': keys['sector'],
            },
            index=keys.index,
        )
        graded = _apply_combustion_process_share(
            prior,
            twin_both=tables['twin_both'],
            twin_anchor_a=tables['twin_anchor_a'],
            facility_share=pd.Series(1.0, index=tables['facility'].index),
            naics_share=pd.Series(1.0, index=tables['naics'].index),
            sector_share=pd.Series(1.0, index=tables['sector'].index),
            basis_mean_facility='facility',
            basis_anchor_a_facility='facility',
        )
        share_rows = []
        for fid_key, basis in graded['split_basis'].items():
            fid = str(fid_key)
            if basis == 'facility' and fid in tables['facility'].index:
                share_rows.append(tables['facility'].loc[fid].rename(fid))
            elif basis == SPLIT_BASIS_NAICS6:
                n6 = graded['naics6'].loc[fid]
                if pd.notna(n6) and n6 in tables['naics'].index:
                    share_rows.append(tables['naics'].loc[n6].rename(fid))
            elif basis == SPLIT_BASIS_SECTOR:
                sec = graded['sector'].loc[fid]
                if pd.notna(sec) and sec in tables['sector'].index:
                    share_rows.append(tables['sector'].loc[sec].rename(fid))
            elif basis == SPLIT_BASIS_NONE:
                continue
        by_fac = (
            pd.DataFrame(share_rows)
            if share_rows
            else pd.DataFrame(columns=list(_FUEL_FLOWABLES))
        )
    else:
        by_fac = pd.DataFrame(columns=list(_FUEL_FLOWABLES))

    records: list[Any] = []
    for row in other.to_dict('records'):
        fid = row['FacilityID']
        if fid in by_fac.index:
            shares = by_fac.loc[fid]
        else:
            shares = _lookup_flowable_share_row(
                fid, row.get('NAICS'), row.get('sector'), tables
            )
        if shares is None:
            records.append(row)
            continue
        co2e = float(row['CO2e'])
        for fuel in _FUEL_FLOWABLES:
            w = float(shares[fuel])
            if w <= 0:
                continue
            piece = {**row, 'Flowable': fuel, 'CO2e': co2e * w}
            if fuel != 'Other':
                piece['fuel_class'] = 'purchased'
            records.append(piece)
    parts = [kept] if not kept.empty else []
    if records:
        parts.append(pd.DataFrame(records))
    return pd.concat(parts, ignore_index=True) if parts else rows.iloc[0:0].copy()


def ghgrp_fuel_labels(
    ghgrp: pd.DataFrame,
    nei: pd.DataFrame,
    per_facility: pd.Series,
    subpart_c: pd.Series,
    year: int,
    *,
    use_scc: bool,
) -> pd.DataFrame:
    """GHGRP rows with a fuel label: subpart W, self-supplied plants, then NEI shares."""
    labeled_ghgrp_rows: list[pd.DataFrame] = []
    burned = ghgrp_subpart_w.subpart_W_combustion((year,))
    subpart_w_facilities: set[str] = set()
    if not burned.empty:
        text = burned['fuel_type'].astype(str)
        flowable = pd.Series('Other', index=burned.index)
        for pattern, name in _FUEL_TYPE_TO_FLOWABLE:
            flowable = flowable.mask(
                text.str.contains(pattern, na=False) & flowable.eq('Other'),
                name,
            )
        burned = burned.assign(Flowable=flowable)
        subpart_w = (
            burned.groupby(['FacilityID', 'fuel_class', 'Flowable'])['CO2e']
            .sum()
            .reset_index()
        )
        other_co2e = (
            per_facility.reindex(subpart_w['FacilityID'].unique()).fillna(0.0)
            - subpart_w.groupby('FacilityID')['CO2e'].sum()
        ).clip(lower=0.0)
        labeled_ghgrp_rows.append(subpart_w)
        if (other_co2e > 0).any():
            labeled_ghgrp_rows.append(
                other_co2e[other_co2e > 0]
                .rename('CO2e')
                .reset_index()
                .assign(fuel_class='process', Flowable='Other')
            )
        subpart_w_facilities = set(subpart_w['FacilityID'])

    self_supplied_plants = (
        ghgrp_subpart_w.self_supplying_facilities(year) - subpart_w_facilities
    )
    if self_supplied_plants:
        plant_fuel = subpart_c.reindex(sorted(self_supplied_plants)).dropna()
        other_co2e = (
            per_facility.reindex(plant_fuel.index).fillna(0.0) - plant_fuel
        ).clip(lower=0.0)
        labeled_ghgrp_rows.append(
            plant_fuel.rename('CO2e')
            .reset_index()
            .assign(fuel_class='self_supplied', Flowable='Natural Gas')
        )
        if (other_co2e > 0).any():
            labeled_ghgrp_rows.append(
                other_co2e[other_co2e > 0]
                .rename('CO2e')
                .reset_index()
                .assign(fuel_class='process', Flowable='Other')
            )
    labeled_ghgrp = (
        pd.concat(labeled_ghgrp_rows, ignore_index=True)
        if labeled_ghgrp_rows
        else pd.DataFrame(columns=['FacilityID', 'fuel_class', 'Flowable', 'CO2e'])
    )
    labeled_facilities = (
        set(labeled_ghgrp['FacilityID']) if not labeled_ghgrp.empty else set()
    )
    if not labeled_ghgrp.empty:
        labeled_ghgrp = labeled_ghgrp.merge(
            ghgrp.drop(columns=['CO2e', 'fuel_class', 'Flowable'], errors='ignore'),
            on='FacilityID',
            how='inner',
        ).assign(source='GHGRP')
    remainder = ghgrp[~ghgrp['FacilityID'].isin(labeled_facilities)]
    # SCC years, or pre-2021 after twin Flowables were applied to *nei*.
    nei_has_fuels = not nei.empty and (nei['Flowable'].astype(str) != 'Other').any()
    if (use_scc or nei_has_fuels) and not remainder.empty:
        nei_by_frs = (
            nei.dropna(subset=['FRS_ID'])
            .groupby(['FRS_ID', 'fuel_class', 'Flowable'])['CO2e']
            .sum()
            .reset_index()
        )
        if not nei_by_frs.empty:
            totals = nei_by_frs.groupby('FRS_ID')['CO2e'].transform('sum')
            shares = nei_by_frs.assign(share=nei_by_frs['CO2e'] / totals)[
                ['FRS_ID', 'fuel_class', 'Flowable', 'share']
            ]
            merged = remainder.merge(
                shares, on='FRS_ID', how='inner', suffixes=('_old', '')
            )
            if not merged.empty:
                shared = merged.assign(CO2e=merged['CO2e'] * merged['share']).drop(
                    columns=['share', 'fuel_class_old', 'Flowable_old'],
                    errors='ignore',
                )
                unmatched = remainder[~remainder['FRS_ID'].isin(set(merged['FRS_ID']))]
                remainder = pd.concat([shared, unmatched], ignore_index=True)
    ghgrp_out = pd.concat(
        [p for p in (labeled_ghgrp, remainder) if p is not None and not p.empty],
        ignore_index=True,
    )
    return ghgrp_out[ghgrp_out['CO2e'] > 0]


def build_facility_combustion(
    year: int,
    *,
    nei_year: int | None = None,
    sector_prefixes: tuple[str, ...] | None = None,
    exclude_sectors: tuple[str, ...] | None = None,
    keep_flowables: tuple[str, ...] | None = None,
) -> pd.DataFrame:
    """GHGRP ∪ NEI facility combustion with ``fuel_class`` and ``Flowable``.

    *year* is the GHGRP year; *nei_year* defaults to *year* (use 2022 for
    2023/2024 NEI hold). See module docstring for mobile SCC, fuel_class gate, and
    prefer-GHGRP rules. ``exclude_sectors`` is applied only when passed (the
    facilities YAML lists electric power).

    Also appends GHGRP lease/plant **share-weight** rows for YAML as
    ``Flowable: Natural Gas - lease and plant``. Those tonnes are not added into
    the logged union total. Prefer-GHGRP rows keep bare fuel ``Flowable`` names.
    """
    year = int(year)
    nei_year = int(nei_year if nei_year is not None else year)
    use_scc = nei_year >= NEI_FUEL_CLASS_FIRST_YEAR
    if sector_prefixes is None:
        sector_prefixes = FACILITY_SCOPE_PREFIXES

    nei_raw = stewi.getInventory(
        'NEI', nei_year, stewiformat='flowbyprocess', download_if_missing=True
    )
    flows = stewi.getInventory(
        'GHGRP', year, stewiformat='flowbyprocess', download_if_missing=True
    )
    if nei_raw is None or getattr(nei_raw, 'empty', True):
        raise ValueError(f'no NEI flowbyprocess for {nei_year}')
    if flows is None or getattr(flows, 'empty', True):
        raise ValueError(f'no GHGRP flowbyprocess for {year}')

    nei_sectors, ghgrp_sectors = facility_sectors(nei_year, year)
    nei = nei_onsite_co2(nei_raw, nei_sectors, use_scc=use_scc)
    if not use_scc:
        # Same-year levels; Flowable from 2021/2022 twin shares.
        nei = apply_pre2021_fuel_flowable_shares(nei)

    gwp = {str(k): float(v) for k, v in GWP100_AR5.items()}
    flows = flows[
        ~flows['Process'].isin(GHGRP_EXCLUDED_SUBPARTS)
        & flows['FlowName'].isin(GHGRP_FLOW_MAP)
    ].copy()
    flows['CO2e'] = flows['FlowAmount'] * flows['FlowName'].map(GHGRP_FLOW_MAP).map(gwp)
    per_facility = flows.groupby('FacilityID')['CO2e'].sum()
    subpart_c = flows[flows['Process'] == 'C'].groupby('FacilityID')['CO2e'].sum()
    ghgrp = (
        per_facility.reset_index()
        .join(ghgrp_sectors, on='FacilityID')
        .assign(
            source='GHGRP',
            fuel_class='unclassified',
            Flowable='Other',
        )
    )

    matches = facilitymatcher.get_matches_for_inventories(['NEI', 'GHGRP'])
    ghgrp_frs = (
        matches[matches['Source'] == 'GHGRP']
        .drop_duplicates('FacilityID')
        .set_index('FacilityID')['FRS_ID']
    )
    nei_frs = (
        matches[matches['Source'] == 'NEI']
        .drop_duplicates('FacilityID')
        .set_index('FacilityID')['FRS_ID']
    )
    ghgrp['FRS_ID'] = ghgrp['FacilityID'].map(ghgrp_frs)
    nei['FRS_ID'] = nei['FacilityID'].map(nei_frs)

    # Same-address sites the FRS match missed. Roster year follows each
    # inventory (GHGRP *year*, NEI *nei_year*) so carried-forward NEI years
    # (e.g. 2023/24 → NEI 2022) do not request a missing NEI facility file.
    linked = set(nei['FRS_ID'].dropna())
    left = ghgrp[ghgrp['FRS_ID'].notna() & ~ghgrp['FRS_ID'].isin(linked)]
    covered_before = set(ghgrp['FRS_ID'].dropna())
    right = nei[nei['FRS_ID'].isna() | ~nei['FRS_ID'].isin(covered_before)]
    relabelled = pd.Series(dtype='object')
    if not left.empty and not right.empty:
        address_rows = []
        for inventory, ids in (
            ('GHGRP', left['FacilityID']),
            ('NEI', right['FacilityID']),
        ):
            roster_year = nei_year if inventory == 'NEI' else year
            roster = stewi.getInventoryFacilities(
                inventory, roster_year, download_if_missing=True
            )
            if roster is None:
                raise ValueError(f'no {inventory} facility roster for {roster_year}')
            out = pd.DataFrame(
                {
                    'FacilityID': roster['FacilityID'].astype(str),
                    'State': roster['State'],
                    'address': colocation.normalize_address(roster['Address']),
                    'tokens': colocation.normalize_name(roster['FacilityName']).map(
                        colocation.name_tokens
                    ),
                    'sector3': roster['NAICS'].fillna('').astype(str).str[:3],
                }
            )
            address_rows.append(
                out[
                    out['address'].str.match(r'^\d')
                    & out['FacilityID'].isin(set(ids.astype(str)))
                ]
            )
        pairs = address_rows[0].merge(
            address_rows[1],
            on=['State', 'address'],
            suffixes=('_g', '_n'),
        )
        if not pairs.empty:
            shares_token = [
                bool(a & b) for a, b in zip(pairs['tokens_g'], pairs['tokens_n'])
            ]
            pairs = pairs[
                pd.Series(shares_token, index=pairs.index)
                | (
                    (pairs['sector3_g'] == pairs['sector3_n'])
                    & (pairs['sector3_g'] != '')
                )
            ]
            frs_of_ghgrp = ghgrp.drop_duplicates('FacilityID').set_index('FacilityID')[
                'FRS_ID'
            ]
            pairs = pairs.assign(FRS_ID=pairs['FacilityID_g'].map(frs_of_ghgrp))
            relabelled = (
                pairs.dropna(subset=['FRS_ID']).groupby('FacilityID_n')['FRS_ID'].min()
            )
    if not relabelled.empty:
        nei['FRS_ID'] = nei['FacilityID'].map(relabelled).fillna(nei['FRS_ID'])

    ghgrp_out = ghgrp_fuel_labels(
        ghgrp, nei, per_facility, subpart_c, year, use_scc=use_scc
    )

    covered = set(ghgrp_out['FRS_ID'].dropna())
    nei_only = nei[nei['FRS_ID'].isna() | ~nei['FRS_ID'].isin(covered)]
    union = pd.concat([ghgrp_out, nei_only], ignore_index=True)
    union = union[union['sector'].notna() & (union['CO2e'] > 0)]
    if exclude_sectors:
        union = union[~union['sector'].astype(str).isin(exclude_sectors)]
    union = filter_to_model_geography(
        union.rename(columns={'CO2e': 'FlowAmount'}),
        label=f'facility_combustion {year}',
        amount_col='FlowAmount',
    ).rename(columns={'FlowAmount': 'CO2e'})
    union = union[union['sector'].astype(str).str.strip().str[:2].isin(sector_prefixes)]
    if keep_flowables is not None:
        union = union[
            union['Flowable'].astype(str).isin({str(f) for f in keep_flowables})
        ]
    union = union.assign(year=year)
    union_mt = float(union['CO2e'].sum()) / 1e9

    # Lease and plant natural gas, for YAML selection only. Not part of the
    # prefer-GHGRP level total. Distinct Flowable; FBS has no Description column.
    lease_plant_fuel = ghgrp_subpart_w.lease_and_plant_fuel((year,), {year: subpart_c})
    if not lease_plant_fuel.empty:
        lease_plant = (
            lease_plant_fuel.groupby('FacilityID', as_index=False)['CO2e']
            .sum()
            .join(ghgrp_sectors, on='FacilityID')
            .assign(
                source='GHGRP',
                fuel_class='lease and plant',
                Flowable='Natural Gas - lease and plant',
                year=year,
            )
        )
        lease_plant = lease_plant[lease_plant['CO2e'] > 0]
        if exclude_sectors:
            lease_plant = lease_plant[
                ~lease_plant['sector'].astype(str).isin(exclude_sectors)
            ]
        lease_plant = lease_plant[
            lease_plant['sector'].notna()
            & lease_plant['sector']
            .astype(str)
            .str.strip()
            .str[:2]
            .isin(sector_prefixes)
        ]
        for col in set(union.columns) - set(lease_plant.columns):
            lease_plant[col] = np.nan
        union = pd.concat([union, lease_plant[union.columns]], ignore_index=True)

    n_facilities = int(union['FacilityID'].nunique())
    log.info(
        f'Facility combustion {year}: {union_mt:.1f} Mt prefer-GHGRP over '
        f'{n_facilities} facilities (share-weight rows excluded from Mt)'
    )
    return union.reset_index(drop=True)
