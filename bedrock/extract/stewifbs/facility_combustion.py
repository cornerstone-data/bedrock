"""GHGRP/NEI facility combustion as an FBS attribution source.

Combines stewi GHGRP and NEI (FRS prefer-GHGRP), classifies fuel via
:mod:`bedrock.transform.ghg.ghgrp_subpart_w` and NEI SCCs, and assigns BEA
detail sectors for method YAML selection on ``Flowable``.
"""

from __future__ import annotations

import re

import facilitymatcher
import numpy as np
import pandas as pd
import stewi

from bedrock.transform.ghg import ghgrp_subpart_w
from bedrock.utils.config.common import load_crosswalk
from bedrock.utils.emissions.gwp import GWP100_AR6_CEDA
from bedrock.utils.logging.flowsa_log import log
from bedrock.utils.mapping.location import filter_to_model_geography

ONSITE_SCC_BRANCHES = ('1', '2', '3')
COMBUSTION_SCC_BRANCHES = ('1', '2')
GHGRP_EXCLUDED_SUBPARTS = frozenset({'D'})
PROCESS_GAS_SCC_LEVEL3 = '007'
FACILITY_SCOPE_PREFIXES = ('21', '22', '31', '32', '33')

GHGRP_FLOW_MAP = {
    'Carbon Dioxide': 'CO2',
    'Methane': 'CH4_fossil',
    'Nitrous Oxide': 'N2O',
}

# Subpart W fuel_type → T_3_11 Flowable (same idea as ghgrp_subpart_w).
_FUEL_TYPE_TO_FLOWABLE = (
    (
        re.compile(r'natural gas|field gas|process gas|pipeline|methane', re.I),
        'Natural Gas',
    ),
    (re.compile(r'coal|coke|lignite|anthracite|bituminous', re.I), 'Coal'),
    (
        re.compile(
            r'distillate|diesel|fuel oil|gasoline|kerosene|petroleum|'
            r'propane|lpg|still gas|naphtha',
            re.I,
        ),
        'Petroleum',
    ),
)

# EPA SCC Level 3 (chars 4-6) → Flowable by major category (char 1).
# Branch 3 (industrial processes) uses a different Level-3 space — leave Other.
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


def build_facility_combustion(
    year: int,
    *,
    nei_year: int | None = None,
    sector_prefixes: tuple[str, ...] | None = None,
    exclude_sectors: tuple[str, ...] | None = None,
    keep_flowables: tuple[str, ...] | None = None,
) -> pd.DataFrame:
    """GHGRP ∪ NEI facility combustion with fuel_class and Flowable.

    *year* is the GHGRP year; *nei_year* defaults to *year* (use 2022 for
    2023/2024 NEI hold). Optional filters come from the FBS method YAML.
    """
    year = int(year)
    nei_year = int(nei_year if nei_year is not None else year)
    prefixes = (
        sector_prefixes if sector_prefixes is not None else FACILITY_SCOPE_PREFIXES
    )
    drop_sectors = exclude_sectors if exclude_sectors is not None else ('221100',)
    sectors_nei = _facility_sectors('NEI', nei_year)
    nei = stewi.getInventory(
        'NEI', nei_year, stewiformat='flowbyprocess', download_if_missing=True
    )
    if nei is None:
        raise ValueError(
            f'stewi returned no NEI flowbyprocess for {nei_year}; '
            f'cannot build facility combustion'
        )
    flows = stewi.getInventory(
        'GHGRP', year, stewiformat='flowbyprocess', download_if_missing=True
    )
    if flows is None:
        raise ValueError(
            f'stewi returned no GHGRP flowbyprocess for {year}. '
            f'Public stewi serves GHGRP through 2023; 2024 needs a local FOIA '
            f'build under stewi local_path.'
        )
    nei = nei[
        (nei['FlowName'] == 'Carbon Dioxide')
        & nei['Process'].astype(str).str[0].isin(ONSITE_SCC_BRANCHES)
    ].copy()
    branch = nei['Process'].astype(str).str[0]
    nei['fuel_class'] = np.where(
        ~branch.isin(COMBUSTION_SCC_BRANCHES),
        'process',
        np.where(
            nei['Process'].astype(str).str[3:6] == PROCESS_GAS_SCC_LEVEL3,
            'self_supplied',
            'purchased',
        ),
    )
    scc = nei['Process'].astype(str)
    level3 = scc.str[3:6]
    nei['Flowable'] = 'Other'
    external = branch == '1'
    nei.loc[external, 'Flowable'] = (
        level3.loc[external].map(_NEI_SCC_FUEL_EXTERNAL).fillna('Other')
    )
    internal = branch == '2'
    nei.loc[internal, 'Flowable'] = (
        level3.loc[internal].map(_NEI_SCC_FUEL_INTERNAL).fillna('Other')
    )
    nei = (
        nei.groupby(['FacilityID', 'fuel_class', 'Flowable'])['FlowAmount']
        .sum()
        .rename('CO2e')
        .reset_index()
        .join(sectors_nei, on='FacilityID')
        .assign(source='NEI', fuel_class_known=True, fuel_class_basis='NEI SCC')
    )

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
        .join(_facility_sectors('GHGRP', year), on='FacilityID')
        .assign(
            source='GHGRP',
            fuel_class='unclassified',
            fuel_class_known=False,
            fuel_class_basis='',
            Flowable='Other',
        )
    )

    matches = facilitymatcher.get_matches_for_inventories(['NEI', 'GHGRP'])

    def frs_of(source: str) -> pd.Series:
        rows = matches[matches['Source'] == source].drop_duplicates('FacilityID')
        return rows.set_index('FacilityID')['FRS_ID']

    ghgrp['FRS_ID'] = ghgrp['FacilityID'].map(frs_of('GHGRP'))
    nei['FRS_ID'] = nei['FacilityID'].map(frs_of('NEI'))

    burned = ghgrp_subpart_w.subpart_W_combustion((year,))
    reported_parts: list[pd.DataFrame] = []
    if not burned.empty:
        burned = burned.copy()
        burned['Flowable'] = burned['fuel_type'].map(_flowable_from_fuel_type)
        w_fuel = (
            burned.groupby(['FacilityID', 'fuel_class', 'Flowable'])['CO2e']
            .sum()
            .reset_index()
        )
        rest = (
            per_facility.reindex(w_fuel['FacilityID'].unique()).fillna(0.0)
            - w_fuel.groupby('FacilityID')['CO2e'].sum()
        ).clip(lower=0.0)
        process_rest = (
            rest[rest > 0]
            .rename('CO2e')
            .to_frame()
            .reset_index()
            .assign(fuel_class='process', Flowable='Other')
        )
        reported_parts.append(
            pd.concat([w_fuel, process_rest], ignore_index=True).assign(
                fuel_class_basis='GHGRP subpart W fuel'
            )
        )

    classified_ids: set[str] = set()
    if reported_parts:
        classified_ids = set(reported_parts[0]['FacilityID'])

    plants = ghgrp_subpart_w.self_supplying_facilities(year) - classified_ids
    if plants:
        plant_fuel = subpart_c.reindex(sorted(plants)).dropna()
        rest = (per_facility.reindex(plant_fuel.index).fillna(0.0) - plant_fuel).clip(
            lower=0.0
        )
        plant_block = pd.concat(
            [
                plant_fuel.rename('CO2e')
                .to_frame()
                .reset_index()
                .assign(fuel_class='self_supplied', Flowable='Natural Gas'),
                rest[rest > 0]
                .rename('CO2e')
                .to_frame()
                .reset_index()
                .assign(fuel_class='process', Flowable='Other'),
            ],
            ignore_index=True,
        ).assign(fuel_class_basis='GHGRP segment')
        reported_parts.append(plant_block)
        classified_ids |= set(plant_block['FacilityID'])

    pieces: list[pd.DataFrame] = []
    if reported_parts:
        reported = pd.concat(reported_parts, ignore_index=True)
        said = reported.merge(
            ghgrp.drop(
                columns=[
                    'CO2e',
                    'fuel_class',
                    'fuel_class_known',
                    'fuel_class_basis',
                    'Flowable',
                ],
                errors='ignore',
            ),
            on='FacilityID',
            how='inner',
        ).assign(fuel_class_known=True, source='GHGRP')
        pieces.append(said)
        ghgrp = ghgrp[~ghgrp['FacilityID'].isin(classified_ids)]

    # NEI fuel-class × Flowable share onto remaining GHGRP totals.
    nei_by_frs = (
        nei.dropna(subset=['FRS_ID'])
        .groupby(['FRS_ID', 'fuel_class', 'Flowable'])['CO2e']
        .sum()
        .reset_index()
    )
    if not nei_by_frs.empty and not ghgrp.empty:
        totals = nei_by_frs.groupby('FRS_ID')['CO2e'].transform('sum')
        nei_by_frs = nei_by_frs.assign(share=nei_by_frs['CO2e'] / totals)
        merged = ghgrp.merge(
            nei_by_frs[['FRS_ID', 'fuel_class', 'Flowable', 'share']],
            on='FRS_ID',
            how='inner',
            suffixes=('_old', ''),
        )
        if not merged.empty:
            pieces.append(
                merged.assign(
                    CO2e=merged['CO2e'] * merged['share'],
                    fuel_class_basis='NEI SCC share',
                    fuel_class_known=True,
                ).drop(
                    columns=['share', 'fuel_class_old', 'Flowable_old'], errors='ignore'
                )
            )
            matched_frs = set(merged['FRS_ID'])
            pieces.append(ghgrp[~ghgrp['FRS_ID'].isin(matched_frs)])
        else:
            pieces.append(ghgrp)
    else:
        pieces.append(ghgrp)

    ghgrp_out = pd.concat([p for p in pieces if not p.empty], ignore_index=True)
    ghgrp_out = ghgrp_out[ghgrp_out['CO2e'] > 0]

    covered = set(ghgrp_out['FRS_ID'].dropna())
    nei_only = nei[nei['FRS_ID'].isna() | ~nei['FRS_ID'].isin(covered)]

    union = pd.concat([ghgrp_out, nei_only], ignore_index=True)
    union = union[union['sector'].notna() & (union['CO2e'] > 0)]
    if drop_sectors:
        union = union[~union['sector'].astype(str).isin(drop_sectors)]
    union = filter_to_model_geography(
        union.rename(columns={'CO2e': 'FlowAmount'}),
        label=f'facility_combustion {year}',
        amount_col='FlowAmount',
    ).rename(columns={'FlowAmount': 'CO2e'})
    union = union[union['sector'].astype(str).str.strip().str[:2].isin(prefixes)]
    if keep_flowables is not None:
        keep = {str(f) for f in keep_flowables}
        union = union[union['Flowable'].astype(str).isin(keep)]
    union = union.assign(year=year)
    log.info(
        f'Facility combustion {year}: {union["CO2e"].sum() / 1e9:.1f} Mt over '
        f'{union["FacilityID"].nunique()} facilities, '
        f'{union["sector"].nunique()} BEA sectors'
    )
    return union.reset_index(drop=True)
