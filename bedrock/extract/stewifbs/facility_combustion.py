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
- **Electric power** is dropped in this module if ``exclude_sectors``
  from the method YAML lists ``221100``.
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


def facility_sectors(
    nei_year: int, ghgrp_year: int
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """NAICS rolled up to one BEA detail code, for the NEI year and the GHGRP year."""
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
    if use_scc and not remainder.empty:
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

    gwp = {str(k): float(v) for k, v in GWP100_AR6_CEDA.items()}
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

    # Same-address sites the FRS match missed. Both inventories are read at the
    # GHGRP year, matching the previous match step.
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
            roster = stewi.getInventoryFacilities(
                inventory, year, download_if_missing=True
            )
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
