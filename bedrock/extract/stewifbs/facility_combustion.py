"""GHGRP/NEI facility combustion as an FBS attribution source.

Combines stewi GHGRP and NEI (FRS prefer-GHGRP), classifies fuel via
:mod:`bedrock.transform.ghg.ghgrp_subpart_w` and NEI SCCs, and assigns BEA
detail sectors for method YAML selection on ``Flowable``.

Rules documented for callers / method authors:

- **Mobile SCCs dropped:** NEI ``Process`` codes whose first two digits are
  ``22`` (aircraft at airports and other mobile point sources) are excluded.
  SCC is gone after aggregation, so this cannot be done in YAML
  ``exclusion_fields``.
- **NEI ``fuel_class`` / Flowable from SCC only for 2021+**
  (``NEI_FUEL_CLASS_FIRST_YEAR``). Before 2021 the SCC coding parked almost
  all on-site CO2 on process branches; those years keep NEI levels as
  ``unclassified`` / ``Other`` and do not impute NEI shares onto GHGRP.
  GHGRP subpart W / plant-segment classification is unchanged in every year.
- **Prefer GHGRP** on ``FRS_ID``; NEI-only facilities fill the residual.
  Same-site address fallback recovers links FRS missed.
- **CO2e for weights:** GHGRP CO2/CH4/N2O collapsed with AR6 CEDA GWPs so
  multi-gas facilities can share with NEI CO2. Prefer-GHGRP ``Flowable`` stays
  the bare fuel name (``Petroleum``, ``Natural Gas``, ``Coal``).
- **Share-weight rows:** GHGRP lease/plant natural gas is appended for YAML
  selection only as ``Flowable: Natural Gas - lease and plant`` (FBS has no
  ``Description`` field). Those tonnes are excluded from the logged prefer-GHGRP
  total.
"""

from __future__ import annotations

import re

import facilitymatcher
import numpy as np
import pandas as pd
import stewi
from facilitymatcher import colocation

from bedrock.transform.ghg import ghgrp_subpart_w
from bedrock.utils.config.common import load_crosswalk
from bedrock.utils.emissions.gwp import GWP100_AR6_CEDA
from bedrock.utils.logging.flowsa_log import log
from bedrock.utils.mapping.location import filter_to_model_geography

ONSITE_SCC_BRANCHES = ('1', '2', '3')
COMBUSTION_SCC_BRANCHES = ('1', '2')
MOBILE_SCC_PREFIX = '22'
GHGRP_EXCLUDED_SUBPARTS = frozenset({'D'})
PROCESS_GAS_SCC_LEVEL3 = '007'
FACILITY_SCOPE_PREFIXES = ('21', '22', '31', '32', '33')
NEI_FUEL_CLASS_FIRST_YEAR = 2021

GHGRP_FLOW_MAP = {
    'Carbon Dioxide': 'CO2',
    'Methane': 'CH4_fossil',
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


def _facility_sectors(inventory: str, year: int) -> pd.DataFrame:
    pairs = (
        load_crosswalk('NAICS_to_BEA_Crosswalk_2017')[
            ['NAICS_2017_Code', 'BEA_2017_Detail_Code']
        ]
        .dropna()
        .drop_duplicates()
    )
    fanout = pairs.groupby('NAICS_2017_Code')['BEA_2017_Detail_Code'].nunique()
    to_bea = pairs[pairs['NAICS_2017_Code'].isin(fanout[fanout == 1].index)].set_index(
        'NAICS_2017_Code'
    )['BEA_2017_Detail_Code']
    lookup = set(to_bea.index)

    def bea_of(naics: object) -> str | None:
        text = str(naics)
        for length in (6, 5, 4, 3, 2):
            if text[:length] in lookup:
                return str(to_bea[text[:length]])
        return None

    facilities = stewi.getInventoryFacilities(
        inventory, year, download_if_missing=True
    )[['FacilityID', 'NAICS', 'State']]
    facilities['sector'] = facilities['NAICS'].map(bea_of)
    return facilities.set_index('FacilityID')


def _flowable_from_fuel_type(label: object) -> str:
    text = str(label)
    for pattern, flowable in _FUEL_TYPE_TO_FLOWABLE:
        if pattern.search(text):
            return flowable
    return 'Other'


def _frs_map(matches: pd.DataFrame, source: str) -> pd.Series:
    rows = matches[matches['Source'] == source].drop_duplicates('FacilityID')
    return rows.set_index('FacilityID')['FRS_ID']


def _same_site_frs(ghgrp: pd.DataFrame, nei: pd.DataFrame, year: int) -> pd.Series:
    """NEI FacilityID → GHGRP FRS_ID for same-address sites FRS missed (#925)."""

    def keys(inventory: str, ids: pd.Series) -> pd.DataFrame:
        facilities = stewi.getInventoryFacilities(
            inventory, year, download_if_missing=True
        )
        out = pd.DataFrame(
            {
                'FacilityID': facilities['FacilityID'].astype(str),
                'State': facilities['State'],
                'address': colocation.normalize_address(facilities['Address']),
                'tokens': colocation.normalize_name(facilities['FacilityName']).map(
                    colocation.name_tokens
                ),
                'sector3': facilities['NAICS'].fillna('').astype(str).str[:3],
            }
        )
        return out[
            out['address'].str.match(r'^\d')
            & out['FacilityID'].isin(set(ids.astype(str)))
        ]

    linked = set(nei['FRS_ID'].dropna())
    left = ghgrp[ghgrp['FRS_ID'].notna() & ~ghgrp['FRS_ID'].isin(linked)]
    covered = set(ghgrp['FRS_ID'].dropna())
    right = nei[nei['FRS_ID'].isna() | ~nei['FRS_ID'].isin(covered)]
    if left.empty or right.empty:
        return pd.Series(dtype='object')

    pairs = keys('GHGRP', left['FacilityID']).merge(
        keys('NEI', right['FacilityID']),
        on=['State', 'address'],
        suffixes=('_g', '_n'),
    )
    if pairs.empty:
        return pd.Series(dtype='object')

    shares_token = [bool(a & b) for a, b in zip(pairs['tokens_g'], pairs['tokens_n'])]
    pairs = pairs[
        pd.Series(shares_token, index=pairs.index)
        | ((pairs['sector3_g'] == pairs['sector3_n']) & (pairs['sector3_g'] != ''))
    ]
    frs_of_ghgrp = ghgrp.drop_duplicates('FacilityID').set_index('FacilityID')['FRS_ID']
    pairs = pairs.assign(FRS_ID=pairs['FacilityID_g'].map(frs_of_ghgrp))
    return pairs.dropna(subset=['FRS_ID']).groupby('FacilityID_n')['FRS_ID'].min()


def _classify_nei(
    nei: pd.DataFrame, sectors: pd.DataFrame, *, use_scc: bool
) -> pd.DataFrame:
    """Aggregate on-site NEI CO2; SCC fuel labels only when *use_scc* (#926)."""
    if use_scc:
        process = nei['Process'].astype(str)
        branch = process.str[0]
        level3 = process.str[3:6]
        nei = nei.assign(
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
        nei.loc[external, 'Flowable'] = (
            level3.loc[external].map(_NEI_SCC_FUEL_EXTERNAL).fillna('Other')
        )
        internal = branch == '2'
        nei.loc[internal, 'Flowable'] = (
            level3.loc[internal].map(_NEI_SCC_FUEL_INTERNAL).fillna('Other')
        )
        keys = ['FacilityID', 'fuel_class', 'Flowable']
    else:
        nei = nei.assign(fuel_class='unclassified', Flowable='Other')
        keys = ['FacilityID', 'fuel_class', 'Flowable']

    return (
        nei.groupby(keys)['FlowAmount']
        .sum()
        .rename('CO2e')
        .reset_index()
        .join(sectors, on='FacilityID')
        .assign(source='NEI')
    )


def _ghgrp_with_flowable(
    year: int, per_facility: pd.Series, subpart_c: pd.Series
) -> pd.DataFrame:
    """Subpart W (fuel_type → Flowable) plus gas-processing plant self-supply."""
    parts: list[pd.DataFrame] = []
    burned = ghgrp_subpart_w.subpart_W_combustion((year,))
    classified: set[str] = set()
    if not burned.empty:
        burned = burned.assign(
            Flowable=burned['fuel_type'].map(_flowable_from_fuel_type)
        )
        w_fuel = (
            burned.groupby(['FacilityID', 'fuel_class', 'Flowable'])['CO2e']
            .sum()
            .reset_index()
        )
        rest = (
            per_facility.reindex(w_fuel['FacilityID'].unique()).fillna(0.0)
            - w_fuel.groupby('FacilityID')['CO2e'].sum()
        ).clip(lower=0.0)
        parts.append(w_fuel)
        if (rest > 0).any():
            parts.append(
                rest[rest > 0]
                .rename('CO2e')
                .reset_index()
                .assign(fuel_class='process', Flowable='Other')
            )
        classified = set(w_fuel['FacilityID'])

    plants = ghgrp_subpart_w.self_supplying_facilities(year) - classified
    if plants:
        plant_fuel = subpart_c.reindex(sorted(plants)).dropna()
        rest = (per_facility.reindex(plant_fuel.index).fillna(0.0) - plant_fuel).clip(
            lower=0.0
        )
        parts.append(
            plant_fuel.rename('CO2e')
            .reset_index()
            .assign(fuel_class='self_supplied', Flowable='Natural Gas')
        )
        if (rest > 0).any():
            parts.append(
                rest[rest > 0]
                .rename('CO2e')
                .reset_index()
                .assign(fuel_class='process', Flowable='Other')
            )

    if not parts:
        return pd.DataFrame(columns=['FacilityID', 'fuel_class', 'Flowable', 'CO2e'])
    return pd.concat(parts, ignore_index=True)


def _apply_nei_shares(ghgrp: pd.DataFrame, nei: pd.DataFrame) -> pd.DataFrame:
    """Split remaining GHGRP totals by matched NEI fuel_class × Flowable shares."""
    if ghgrp.empty:
        return ghgrp
    nei_by_frs = (
        nei.dropna(subset=['FRS_ID'])
        .groupby(['FRS_ID', 'fuel_class', 'Flowable'])['CO2e']
        .sum()
        .reset_index()
    )
    if nei_by_frs.empty:
        return ghgrp

    totals = nei_by_frs.groupby('FRS_ID')['CO2e'].transform('sum')
    shares = nei_by_frs.assign(share=nei_by_frs['CO2e'] / totals)[
        ['FRS_ID', 'fuel_class', 'Flowable', 'share']
    ]
    merged = ghgrp.merge(shares, on='FRS_ID', how='inner', suffixes=('_old', ''))
    if merged.empty:
        return ghgrp

    shared = merged.assign(CO2e=merged['CO2e'] * merged['share']).drop(
        columns=['share', 'fuel_class_old', 'Flowable_old'], errors='ignore'
    )
    unmatched = ghgrp[~ghgrp['FRS_ID'].isin(set(merged['FRS_ID']))]
    return pd.concat([shared, unmatched], ignore_index=True)


def lease_and_plant_total_mmt(year: int) -> float:
    """GHGRP lease/plant fuel total in MMT CO2e for inventory carve levels."""
    year = int(year)
    flows = stewi.getInventory(
        'GHGRP', year, stewiformat='flowbyprocess', download_if_missing=True
    )
    if flows is None or getattr(flows, 'empty', True):
        return 0.0
    gwp = {str(k): float(v) for k, v in GWP100_AR6_CEDA.items()}
    combustion = flows[
        (flows['Process'] == 'C') & flows['FlowName'].isin(GHGRP_FLOW_MAP)
    ].copy()
    combustion['CO2e'] = combustion['FlowAmount'] * combustion['FlowName'].map(
        GHGRP_FLOW_MAP
    ).map(gwp)
    subpart_c = combustion.groupby('FacilityID')['CO2e'].sum()
    carved = ghgrp_subpart_w.lease_and_plant_fuel((year,), {year: subpart_c})
    if carved.empty:
        return 0.0
    return float(carved['CO2e'].sum()) / 1e9


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
    prefer-GHGRP rules. Optional filters come from the FBS method YAML.

    Also appends GHGRP lease/plant **share-weight** rows for YAML as
    ``Flowable: Natural Gas - lease and plant``. Those tonnes are not added into
    the logged union total. Prefer-GHGRP rows keep bare fuel ``Flowable`` names.
    """
    year = int(year)
    nei_year = int(nei_year if nei_year is not None else year)
    use_scc = nei_year >= NEI_FUEL_CLASS_FIRST_YEAR
    if sector_prefixes is None:
        sector_prefixes = FACILITY_SCOPE_PREFIXES
    if exclude_sectors is None:
        exclude_sectors = ('221100',)

    # --- load ---
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

    process = nei_raw['Process'].astype(str)
    nei_raw = nei_raw[
        (nei_raw['FlowName'] == 'Carbon Dioxide')
        & process.str[0].isin(ONSITE_SCC_BRANCHES)
        & (process.str[:2] != MOBILE_SCC_PREFIX)
    ]
    nei = _classify_nei(nei_raw, _facility_sectors('NEI', nei_year), use_scc=use_scc)

    gwp = {str(k): float(v) for k, v in GWP100_AR6_CEDA.items()}
    flows = flows[
        ~flows['Process'].isin(GHGRP_EXCLUDED_SUBPARTS)
        & flows['FlowName'].isin(GHGRP_FLOW_MAP)
    ].copy()
    flows['CO2e'] = flows['FlowAmount'] * flows['FlowName'].map(GHGRP_FLOW_MAP).map(gwp)
    per_facility = flows.groupby('FacilityID')['CO2e'].sum()
    subpart_c = flows[flows['Process'] == 'C'].groupby('FacilityID')['CO2e'].sum()
    ghgrp_sectors = _facility_sectors('GHGRP', year)
    ghgrp = (
        per_facility.reset_index()
        .join(ghgrp_sectors, on='FacilityID')
        .assign(
            source='GHGRP',
            fuel_class='unclassified',
            Flowable='Other',
        )
    )

    # --- FRS match (+ same-site fallback) ---
    matches = facilitymatcher.get_matches_for_inventories(['NEI', 'GHGRP'])
    ghgrp['FRS_ID'] = ghgrp['FacilityID'].map(_frs_map(matches, 'GHGRP'))
    nei['FRS_ID'] = nei['FacilityID'].map(_frs_map(matches, 'NEI'))
    relabelled = _same_site_frs(ghgrp, nei, year)
    if not relabelled.empty:
        nei['FRS_ID'] = nei['FacilityID'].map(relabelled).fillna(nei['FRS_ID'])

    # --- classify GHGRP; NEI shares on the remainder when SCC fuel is defined ---
    said = _ghgrp_with_flowable(year, per_facility, subpart_c)
    classified_ids = set(said['FacilityID']) if not said.empty else set()
    if not said.empty:
        said = said.merge(
            ghgrp.drop(columns=['CO2e', 'fuel_class', 'Flowable'], errors='ignore'),
            on='FacilityID',
            how='inner',
        ).assign(source='GHGRP')
    remainder = ghgrp[~ghgrp['FacilityID'].isin(classified_ids)]
    if use_scc:
        remainder = _apply_nei_shares(remainder, nei)
    ghgrp_out = pd.concat(
        [p for p in (said, remainder) if p is not None and not p.empty],
        ignore_index=True,
    )
    ghgrp_out = ghgrp_out[ghgrp_out['CO2e'] > 0]

    # --- prefer-GHGRP union ---
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

    # Lease/plant share-weight rows only (not part of the prefer-GHGRP level total).
    # Distinct Flowable so YAML can select them; FBS has no Description column.
    lease = ghgrp_subpart_w.lease_and_plant_fuel((year,), {year: subpart_c})
    if not lease.empty:
        weights = (
            lease.groupby('FacilityID', as_index=False)['CO2e']
            .sum()
            .join(ghgrp_sectors, on='FacilityID')
            .assign(
                source='GHGRP',
                fuel_class='lease and plant',
                Flowable='Natural Gas - lease and plant',
                year=year,
            )
        )
        weights = weights[weights['CO2e'] > 0]
        if exclude_sectors:
            weights = weights[~weights['sector'].astype(str).isin(exclude_sectors)]
        weights = weights[
            weights['sector'].notna()
            & weights['sector'].astype(str).str.strip().str[:2].isin(sector_prefixes)
        ]
        for col in set(union.columns) - set(weights.columns):
            weights[col] = np.nan
        union = pd.concat([union, weights[union.columns]], ignore_index=True)

    n_facilities = int(union['FacilityID'].nunique())
    log.info(
        f'Facility combustion {year}: {union_mt:.1f} Mt prefer-GHGRP over '
        f'{n_facilities} facilities (share-weight rows excluded from Mt)'
    )
    return union.reset_index(drop=True)
