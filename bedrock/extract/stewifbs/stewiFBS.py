# stewiFBS.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Functions to access data from stewi and stewicombo for use in flowbysector

These functions are called if referenced in flowbysectormethods as
data_format FBS_outside_flowsa with the function specified in FBS_datapull_fxn
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import facilitymatcher
import numpy as np
import pandas as pd
import stewi
import stewicombo
from esupy.processed_data_mgmt import read_source_metadata
from stewicombo.globals import addChemicalMatches, compile_metadata, set_stewicombo_meta

from bedrock.extract.flowbyactivity import FlowByActivity
from bedrock.extract.stewifbs.facility_combustion import build_facility_combustion
from bedrock.transform.flowbyfunctions import assign_fips_location_system
from bedrock.transform.flowbysector import FlowBySector
from bedrock.utils.config.settings import process_adjustmentpath
from bedrock.utils.logging.flowsa_log import log
from bedrock.utils.mapping import sector as naics_mapping
from bedrock.utils.mapping.location import (
    apply_county_FIPS,
    filter_to_model_geography,
    update_geoscale,
)
from bedrock.utils.mapping.sectormapping import get_activitytosector_mapping

InventoryDict = dict[str, str]


def stewicombo_to_sector(
    config: dict[str, Any],
    full_name: str,
    external_config_path: str | None = None,
    **_kwargs: Any,
) -> FlowBySector:
    """
    Returns emissions from stewicombo in fbs format, requires stewi >= 0.9.5
    :param config: which may contain the following elements:
        local_inventory_name: (optional) a string naming the file from which to
                source a pregenerated stewicombo file stored locally (e.g.,
                'CAP_HAP_national_2017_v0.9.7_5cf36c0.parquet' or
                'CAP_HAP_national_2017')
        inventory_dict: a dictionary of inventory types and years (e.g.,
                {'NEI':'2017', 'TRI':'2017'})
        compartments: list of compartments to include (e.g., 'water', 'air',
                'soil'), use None to include all compartments
        functions: list of functions (str) to call for additional processing
    :param method: dictionary, FBS method
    :param external_config_path, str, optional path to an FBS method outside
        flowsa repo
    :return: FlowBySector object
    """
    inventory_name = config.get('local_inventory_name')
    config['full_name'] = full_name

    df: pd.DataFrame | None = None
    if inventory_name is not None:
        df = stewicombo.getInventory(inventory_name, download_if_missing=True)
    if df is None:
        # run stewicombo to combine inventories, filter for LCI, remove overlap
        log.info('generating inventory in stewicombo')
        df = stewicombo.combineFullInventories(
            config['inventory_dict'],
            filter_for_LCI=True,
            remove_overlap=True,
            compartments=config.get('compartments'),
        )

    if df is None:
        # Inventories not found for stewicombo, return empty FBS
        return FlowBySector(pd.DataFrame(), convert_df_to_flowby=True)

    facility_mapping = extract_facility_data(config['inventory_dict'])

    # merge dataframes to assign facility information based on facility IDs
    df = df.drop(columns=['SRS_CAS', 'SRS_ID', 'FacilityIDs_Combined']).merge(
        facility_mapping.loc[:, facility_mapping.columns != 'NAICS'],
        how='inner',
        on='FacilityID',
    )

    all_NAICS = obtain_NAICS_from_facility_matcher(
        list(config['inventory_dict'].keys())
    )

    df = assign_naics_to_stewicombo(df, all_NAICS, facility_mapping)

    if 'reassign_process_to_sectors' in config:
        df = reassign_process_to_sectors(
            df,
            config['inventory_dict']['NEI'],
            config['reassign_process_to_sectors'],
            external_config_path,
        )

    return prepare_stewi_fbs(df, config)


def stewi_to_sector(
    config: dict[str, Any],
    full_name: str,
    external_config_path: str | None = None,
    **_kwargs: Any,
) -> FlowBySector:
    """
    Returns emissions from stewi in fbs format, requires stewi >= 0.9.5
    :param config: which may contain the following elements:
        inventory_dict: a dictionary of inventory types and years (e.g.,
                {'NEI':'2017', 'TRI':'2017'})
        compartments: list of compartments to include (e.g., 'water', 'air',
                'soil'), use None to include all compartments
        functions: list of functions (str) to call for additional processing
    :return: FlowBySector object
    """
    _ = (external_config_path, _kwargs)
    # determine if fxns specified in FBS method yaml
    functions: list[str] = config.get('functions', [])
    config['full_name'] = full_name

    # run stewi to generate inventory and filter for LCI
    df = pd.DataFrame()
    for database, year in config['inventory_dict'].items():
        inv = (
            stewi.getInventory(
                database,
                year,
                filters=['filter_for_LCI', 'US_States_only'],
                download_if_missing=True,
            )
            .assign(Year=year)
            .assign(Source=database)
        )
        df = pd.concat([df, inv], ignore_index=True)
    compartments = config.get('compartments')
    if compartments:
        # Subset based on primary compartment
        df = df[df['Compartment'].str.split('/', expand=True)[0].isin(compartments)]
    facility_mapping = extract_facility_data(config['inventory_dict'])
    # Convert NAICS to string (first to int to avoid decimals)
    facility_mapping['NAICS'] = facility_mapping['NAICS'].astype(int).astype(str)

    # merge dataframes to assign facility information based on facility IDs
    df = df.merge(facility_mapping, how='left', on='FacilityID')
    fbs = prepare_stewi_fbs(df, config)

    for function in functions:
        fbs = getattr(sys.modules[__name__], function)(fbs)

    return fbs


# Stewi facility ``Plant primary fuel`` uses eGRID PLPRMFL codes; map to PLFUELCT
# categories before NAICS crosswalk (EPA eGRID code lookup, plant primary fuel table).
_EGRID_PLPRMFL_TO_PLFUELCT: dict[str, str] = {
    'AB': 'BIOMASS',
    'BFG': 'OFSL',
    'BIT': 'COAL',
    'BLQ': 'BIOMASS',
    'COG': 'COAL',
    'DFO': 'OIL',
    'GEO': 'GEOTHERMAL',
    'JF': 'OIL',
    'KER': 'OIL',
    'LFG': 'BIOMASS',
    'LIG': 'COAL',
    'MSW': 'BIOMASS',
    'MWH': 'OTHF',
    'NG': 'GAS',
    'NUC': 'NUCLEAR',
    'OBG': 'BIOMASS',
    'OBL': 'BIOMASS',
    'OBS': 'BIOMASS',
    'OG': 'OFSL',
    'OTH': 'OTHF',
    'PC': 'OIL',
    'PRG': 'OTHF',
    'PUR': 'OTHF',
    'RC': 'COAL',
    'RFO': 'OIL',
    'SGC': 'COAL',
    'SUB': 'COAL',
    'SUN': 'SOLAR',
    'TDF': 'OFSL',
    'WAT': 'HYDRO',
    'WC': 'COAL',
    'WDL': 'BIOMASS',
    'WDS': 'BIOMASS',
    'WH': 'OTHF',
    'WND': 'WIND',
    'WO': 'OIL',
}


def _egrid_plprmfl_to_plfuelct(fuel: str) -> str | None:
    key = str(fuel).strip().upper()
    if key in _EGRID_PLPRMFL_TO_PLFUELCT:
        return _EGRID_PLPRMFL_TO_PLFUELCT[key]
    if key in _EGRID_PLPRMFL_TO_PLFUELCT.values():
        return key
    return None


def load_egrid_emissions_via_stewi(year: str | int) -> pd.DataFrame:
    """Load stewi eGRID flow-by-facility emissions with facility location and fuel."""
    year_str = str(year)
    try:
        df = stewi.getInventory('eGRID', year_str, download_if_missing=True)
        facilities = stewi.getInventoryFacilities(
            'eGRID', year_str, download_if_missing=True
        )
        if facilities is None:
            raise TypeError('eGRID facility inventory missing after download')
    except TypeError:
        # download_if_missing=True looks for data on EPA server, so generate locally
        log.info(
            f'eGRID {year_str} not available via download; '
            'regenerating with download_if_missing=False'
        )
        df = stewi.getInventory('eGRID', year_str, download_if_missing=False)
        facilities = stewi.getInventoryFacilities(
            'eGRID', year_str, download_if_missing=False
        )

    facilities = (
        facilities[['FacilityID', 'State', 'County', 'Plant primary fuel']]
        .drop_duplicates(subset='FacilityID', keep='first')
        # Cut to BEA's economic territory here, while State is still a
        # two-letter code -- apply_county_FIPS overwrites it with a full name.
        .pipe(filter_to_model_geography, label=f'eGRID {year_str}')
        .pipe(apply_county_FIPS)
    )
    merged = df.merge(facilities, how='inner', on='FacilityID')
    orphans = df.loc[~df['FacilityID'].isin(facilities['FacilityID'])]
    if not orphans.empty:
        log.warning(
            'eGRID %s: %d emission rows over %d facilities have no facility '
            'record and are dropped',
            year_str,
            len(orphans),
            orphans['FacilityID'].nunique(),
        )
    return merged


def assign_naics_from_egrid_fuel(
    df: pd.DataFrame,
    mapping_name: str,
    *,
    external_config_path: str | None = None,
) -> pd.DataFrame:
    """Map eGRID primary fuel (PLPRMFL or PLFUELCT) to target NAICS (2017) codes."""
    if 'PrimaryFuelCategory' not in df.columns:
        if 'Plant primary fuel' not in df.columns:
            raise KeyError(
                'eGRID dataframe must include Plant primary fuel from stewi facilities'
            )
        df = df.assign(
            PrimaryFuelCategory=df['Plant primary fuel'].map(_egrid_plprmfl_to_plfuelct)
        )
    else:
        df = df.assign(
            PrimaryFuelCategory=df['PrimaryFuelCategory'].map(
                _egrid_plprmfl_to_plfuelct
            )
        )
    crosswalk = get_activitytosector_mapping(mapping_name, external_config_path)[
        ['Activity', 'Sector']
    ].drop_duplicates(subset=['Activity'])
    merged = df.merge(
        crosswalk,
        left_on='PrimaryFuelCategory',
        right_on='Activity',
        how='left',
    )
    unmapped_mask = merged['Sector'].isna() & merged['PrimaryFuelCategory'].notna()
    if unmapped_mask.any():
        unmapped_fuels = sorted(
            merged.loc[unmapped_mask, 'PrimaryFuelCategory'].unique()
        )
        log.warning(
            'eGRID primary fuel categories without NAICS mapping in %s: %s',
            mapping_name,
            unmapped_fuels,
        )
    return (
        merged.assign(NAICS=merged['Sector'])
        .drop(columns=['Activity', 'Sector'], errors='ignore')
        .dropna(subset=['NAICS'])
    )


def _subset_stewi_include_flow_names(
    df: pd.DataFrame, flow_names: list[str] | tuple[str, ...]
) -> pd.DataFrame:
    """Keep stewi rows whose ``FlowName`` is in ``include_flow_names`` (method yaml)."""
    if 'FlowName' not in df.columns:
        raise KeyError(
            'Stewi dataframe must include FlowName before include_flow_names filter'
        )
    allowed = frozenset(flow_names)
    out = df.loc[df['FlowName'].isin(allowed)]
    if out.empty:
        log.warning(
            'No stewi rows after include_flow_names filter: %s',
            sorted(allowed),
        )
    return out


def egrid_to_sector(
    config: dict[str, Any],
    full_name: str,
    external_config_path: str | None = None,
    **_kwargs: Any,
) -> FlowBySector:
    """
    Build a national FBS from stewi eGRID plant-level air emissions.

    Loads ``eGRID`` via stewi, then assigns NAICS from primary fuel category.
    """
    _ = _kwargs
    config['full_name'] = full_name
    mapping_name = config.get('activity_to_sector_mapping', 'EPA_eGRID')
    inventory_dict: InventoryDict = config['inventory_dict']
    if len(inventory_dict) != 1 or 'eGRID' not in inventory_dict:
        raise ValueError(
            "egrid_to_sector expects inventory_dict with a single 'eGRID' year entry"
        )
    egrid_year = inventory_dict['eGRID']

    df = load_egrid_emissions_via_stewi(egrid_year)

    df = assign_naics_from_egrid_fuel(
        df, mapping_name, external_config_path=external_config_path
    )
    df = df.assign(
        Year=int(config.get('year', egrid_year)),
        Source='eGRID',
        Class='Chemicals',
    )
    return prepare_stewi_fbs(df, config)


def facility_combustion_to_sector(
    config: dict[str, Any],
    full_name: str,
    external_config_path: str | None = None,
    **_kwargs: Any,
) -> FlowBySector:
    """
    Returns GHGRP/NEI facility combustion weights in FBS format for attribution.

    Builds via :func:`~bedrock.extract.stewifbs.facility_combustion.build_facility_combustion`
    (prefer-GHGRP ∪ NEI-only; drops mobile SCC ``22*``; NEI SCC fuel labels
    only from 2021+). Prefer-GHGRP ``Flowable`` is the bare fuel name; lease/plant
    share weights use ``Natural Gas - lease and plant``.

    :param config: may include:
        inventory_dict: GHGRP and optional NEI years (e.g. ``{'GHGRP':'2023',
            'NEI':'2022'}``)
        year: method year written on output rows
        sector_prefixes: optional 2-digit sector parents to keep
        exclude_sectors: optional detail codes to drop
        keep_flowables: optional Flowable labels to keep
    :param full_name: FBS name
    :param external_config_path: unused; accepted for FBS_datapull signature
    :return: FlowBySector with facility combustion weights
    """
    _ = (external_config_path, _kwargs)
    config = dict(config)
    config['full_name'] = full_name
    inventory_dict: InventoryDict = config['inventory_dict']
    if 'GHGRP' not in inventory_dict:
        raise ValueError(
            'facility_combustion_to_sector requires inventory_dict with GHGRP'
        )
    ghgrp_year = int(inventory_dict['GHGRP'])
    nei_year = int(inventory_dict.get('NEI', ghgrp_year))
    year = int(config.get('year', ghgrp_year))

    def _str_tuple(key: str) -> tuple[str, ...] | None:
        raw = config.get(key)
        if raw is None:
            return None
        return tuple(str(x) for x in raw)

    union = build_facility_combustion(
        ghgrp_year,
        nei_year=nei_year,
        sector_prefixes=_str_tuple('sector_prefixes'),
        exclude_sectors=_str_tuple('exclude_sectors'),
        keep_flowables=_str_tuple('keep_flowables'),
    )

    # Emit facility NAICS (not BEA). UMD inventory maps to NAICS; proportional
    # attribution joins on PrimarySector, so BEA weights match nothing and zero
    # the activity set. Prefix/exclude filters above still use BEA ``sector``.
    facility = (
        union.rename(columns={'CO2e': 'FlowAmount'})
        .assign(
            SectorProducedBy=lambda d: d['NAICS']
            .astype(str)
            .str.replace(r'\.0$', '', regex=True),
            SectorConsumedBy=np.nan,
            Class='Energy',
            Context='emission/air',
            Unit='kg',
            FlowType='ELEMENTARY_FLOW',
            Year=year,
            Location='00000',
            LocationSystem='FIPS',
            MetaSources='GHGRP_NEI',
            SectorSourceName=f'NAICS_{config.get("target_schema_year", 2017)}_Code',
        )
        .loc[
            :,
            [
                'Flowable',
                'Class',
                'Context',
                'Unit',
                'FlowType',
                'FlowAmount',
                'Year',
                'Location',
                'LocationSystem',
                'SectorProducedBy',
                'SectorConsumedBy',
                'MetaSources',
                'SectorSourceName',
            ],
        ]
    )
    fbs = FlowBySector(
        facility, full_name=full_name, config=config, convert_df_to_flowby=True
    )
    fbs.config.update({'data_format': 'FBS'})
    return fbs


def _naics_weight_frame(
    weights: pd.DataFrame,
    *,
    year: int,
    flowable: str,
    target_schema_year: int = 2017,
) -> pd.DataFrame:
    """FBS-shaped rows from NAICS attribution shares.

    ``Unit='share'``: ``FlowAmount`` is a dimensionless fraction within this
    table (sums to 1), for proportional attribution only — not inventory mass.
    NAICS codes sit on ``SectorConsumedBy`` (same side as MECS energy/money
    FBS and TECHNOSPHERE primary-sector logic). ``MetaSources`` should already
    be set per row from the coverage mode.
    """
    if 'MetaSources' not in weights.columns:
        raise ValueError('_naics_weight_frame requires MetaSources on weights')
    return weights.assign(
        SectorProducedBy=np.nan,
        SectorConsumedBy=lambda d: d['NAICS']
        .astype(str)
        .str.replace(r'\.0$', '', regex=True),
        Flowable=flowable,
        Class='Energy',
        Context=np.nan,
        Unit='share',
        FlowType='TECHNOSPHERE_FLOW',
        Year=year,
        Location='00000',
        LocationSystem='FIPS',
        SectorSourceName=f'NAICS_{target_schema_year}_Code',
    ).loc[
        :,
        [
            'Flowable',
            'Class',
            'Context',
            'Unit',
            'FlowType',
            'FlowAmount',
            'Year',
            'Location',
            'LocationSystem',
            'SectorProducedBy',
            'SectorConsumedBy',
            'MetaSources',
            'SectorSourceName',
        ],
    ]


def _amounts_to_shares(amounts: pd.Series) -> pd.Series:
    """Normalize non-negative amounts to shares; zeros if the total is empty."""
    total = float(amounts.sum())
    if total <= 0:
        return amounts.astype(float) * 0.0
    return amounts.astype(float) / total


def _naics_amounts_at_industry_spec(
    amounts: pd.DataFrame,
    *,
    config: dict[str, Any],
    flowable: str,
    year: int,
    target_schema_year: int,
) -> pd.DataFrame:
    """Roll ``NAICS``/``FlowAmount`` rows to method ``industry_spec`` targets.

    Uses :meth:`FlowBySector.sector_aggregation` (same path as FBS attribution
    sources) so facility/MECS digit lengths match inventory join keys before
    share blending.
    """
    from bedrock.transform.ghg.facility_coverage import (  # noqa: PLC0415
        bea_detail_for_naics,
    )

    if 'industry_spec' not in config:
        raise ValueError(
            'hybrid_facility_mecs_to_sector requires industry_spec on the '
            'method config for NAICS rollup'
        )
    frame = _naics_weight_frame(
        amounts.assign(MetaSources='rollup'),
        year=year,
        flowable=flowable,
        target_schema_year=target_schema_year,
    )
    # Temporary MetaSources only to satisfy the weight-frame schema.
    fbs = FlowBySector(
        frame,
        full_name='_hybrid_rollup',
        config=dict(config),
        convert_df_to_flowby=True,
    )
    rolled = pd.DataFrame(fbs.sector_aggregation())
    out = (
        rolled.rename(columns={'SectorConsumedBy': 'NAICS'})
        .groupby('NAICS', as_index=False)
        .agg(FlowAmount=('FlowAmount', 'sum'))
    )
    out['NAICS'] = out['NAICS'].astype(str)
    out['sector'] = out['NAICS'].map(bea_detail_for_naics)
    return out


def _hybrid_shares_for_flowable(
    *,
    flowable: str,
    mecs_class: str | None,
    facility_union: pd.DataFrame,
    mecs: pd.DataFrame,
    mecs_method: str,
    modes: dict[str, str],
    config: dict[str, Any],
    year: int,
    target_schema_year: int,
    min_coverage: float,
) -> pd.DataFrame:
    """Blend one fuel's facility + MECS amounts into renormalized NAICS shares."""
    facility = facility_union[facility_union['Flowable'].astype(str) == flowable].copy()
    facility['NAICS'] = (
        facility['NAICS'].astype(str).str.replace(r'\.0$', '', regex=True)
    )
    fac_w = _naics_amounts_at_industry_spec(
        facility.groupby('NAICS', as_index=False).agg(FlowAmount=('CO2e', 'sum')),
        config=config,
        flowable=flowable,
        year=year,
        target_schema_year=target_schema_year,
    )

    mecs_fuel = mecs[mecs['Flowable'].astype(str) == flowable]
    if mecs_class is not None:
        mecs_fuel = mecs_fuel[mecs_fuel['Class'].astype(str) == str(mecs_class)]
    if mecs_fuel.empty:
        raise ValueError(
            f'No MECS rows for Flowable={flowable!r} class={mecs_class!r} '
            f'in {mecs_method}'
        )
    mecs_w = _naics_amounts_at_industry_spec(
        mecs_fuel.assign(NAICS=mecs_fuel['SectorConsumedBy'].astype(str))
        .groupby('NAICS', as_index=False)
        .agg(FlowAmount=('FlowAmount', 'sum')),
        config=config,
        flowable=flowable,
        year=year,
        target_schema_year=target_schema_year,
    )

    fac_by = fac_w.set_index('NAICS')
    mecs_by = mecs_w.set_index('NAICS')
    fac_share = _amounts_to_shares(fac_by['FlowAmount'])
    mecs_share = _amounts_to_shares(mecs_by['FlowAmount'])
    all_naics = sorted(set(fac_by.index) | set(mecs_by.index))
    meta_by_mode = {
        'keep_prior': mecs_method,
        'facility_vector': 'GHGRP_NEI',
        'facility_floor': f'GHGRP_NEI;{mecs_method}',
    }
    rows: list[dict[str, Any]] = []
    for naics in all_naics:
        fac_amt = float(fac_share.get(naics, 0.0))
        mecs_amt = float(mecs_share.get(naics, 0.0))
        if naics in fac_by.index and pd.notna(fac_by.at[naics, 'sector']):
            bea = str(fac_by.at[naics, 'sector'])
        elif naics in mecs_by.index and pd.notna(mecs_by.at[naics, 'sector']):
            bea = str(mecs_by.at[naics, 'sector'])
        else:
            continue
        mode = modes.get(bea, 'keep_prior')
        if mode == 'keep_prior':
            weight = mecs_amt
        elif mode == 'facility_vector':
            weight = fac_amt
        else:
            weight = max(fac_amt, mecs_amt)
        if weight <= 0:
            continue
        rows.append(
            {
                'NAICS': naics,
                'FlowAmount': weight,
                'MetaSources': meta_by_mode[mode],
            }
        )
    if not rows:
        raise ValueError(
            f'hybrid_facility_mecs_to_sector produced no weights for '
            f'{flowable!r} at min_coverage={min_coverage}'
        )
    weights = pd.DataFrame(rows)
    weights['FlowAmount'] = _amounts_to_shares(weights['FlowAmount'])
    share_sum = float(weights['FlowAmount'].sum())
    if abs(share_sum - 1.0) > 1e-9:
        raise ValueError(
            f'hybrid shares for {flowable!r} sum to {share_sum}, expected 1.0'
        )
    log.info(
        'Hybrid %s attribution: %d NAICS shares after industry_spec rollup '
        '(min_coverage=%.2f, share_sum=%.6f)',
        flowable,
        len(weights),
        min_coverage,
        share_sum,
    )
    return _naics_weight_frame(
        weights,
        year=year,
        flowable=flowable,
        target_schema_year=target_schema_year,
    )


def hybrid_facility_mecs_to_sector(
    config: dict[str, Any],
    full_name: str,
    external_config_path: str | None = None,
    **_kwargs: Any,
) -> FlowBySector:
    """Facility + MECS hybrid attribution weights under the #928 residual rule.

    Output is one attribution-share FBS (``Unit='share'``) with one
    ``Flowable`` per configured fuel. Each fuel's ``FlowAmount`` sums to 1
    independently — not inventory emissions. Facility CO2e and MECS native
    units are each rolled to the method ``industry_spec`` via
    :meth:`FlowBySector.sector_aggregation`, converted to NAICS shares, then
    blended. NAICS codes sit on ``SectorConsumedBy``.

    ``GHGRP_NEI_Facilities`` remains kg CO2e on ``SectorProducedBy``; MECS
    ``Energy_manufacturing_national_nowcast_*`` is unchanged. This builder only
    reads them to form per-NAICS shares for proportional attribution of
    table 3-11.

    Config keys (in addition to those of :func:`facility_combustion_to_sector`):

    - ``flowables``: mapping of bare fuel name → MECS ``Class``
      (``Natural Gas`` / ``Coal`` → ``Energy``; ``Petroleum`` → ``Money``)
    - ``mecs_method``: Energy manufacturing FBS method stem for the year
    - ``min_coverage``: attribution gate (default 0.8)
    - ``industry_spec`` / ``target_schema_year``: inherited from the FBS method

    Modes are the #1040 median-coverage freeze
    (:func:`~bedrock.transform.ghg.facility_coverage.modes_median_freeze`):
    one facility-vs-MECS side per sector from median 2017-2024 coverage vs
    ``min_coverage``, with floor vs vector from 2022 native modes.
    ``keep_prior`` uses MECS share; ``facility_vector`` uses facility share;
    ``facility_floor`` uses max(facility share, MECS share). Each fuel's
    blended table is renormalized to sum to 1. ``MetaSources`` is
    ``mecs_method`` / ``GHGRP_NEI`` / ``GHGRP_NEI;<mecs_method>`` by mode.
    Lease/plant and still gas stay on :func:`facility_combustion_to_sector`
    (no MECS fallback).
    """
    from bedrock.transform.ghg.facility_coverage import (  # noqa: PLC0415
        ATTRIBUTION_MIN_COVERAGE,
        modes_median_freeze,
    )

    _ = (external_config_path, _kwargs)
    config = dict(config)
    config['full_name'] = full_name
    inventory_dict: InventoryDict = config['inventory_dict']
    if 'GHGRP' not in inventory_dict:
        raise ValueError(
            'hybrid_facility_mecs_to_sector requires inventory_dict with GHGRP'
        )
    flowables_cfg = config.get('flowables')
    mecs_method = str(config.get('mecs_method') or '')
    if not isinstance(flowables_cfg, dict) or not flowables_cfg or not mecs_method:
        raise ValueError(
            'hybrid_facility_mecs_to_sector requires flowables '
            '(fuel -> MECS Class) and mecs_method'
        )
    target_schema_year = int(config.get('target_schema_year', 2017))
    ghgrp_year = int(inventory_dict['GHGRP'])
    nei_year = int(inventory_dict.get('NEI', ghgrp_year))
    year = int(config.get('year', ghgrp_year))
    min_coverage = float(config.get('min_coverage', ATTRIBUTION_MIN_COVERAGE))

    def _str_tuple(key: str) -> tuple[str, ...] | None:
        raw = config.get(key)
        if raw is None:
            return None
        return tuple(str(x) for x in raw)

    keep_flowables = _str_tuple('keep_flowables') or tuple(
        str(f) for f in flowables_cfg
    )
    union = build_facility_combustion(
        ghgrp_year,
        nei_year=nei_year,
        sector_prefixes=_str_tuple('sector_prefixes'),
        exclude_sectors=_str_tuple('exclude_sectors'),
        keep_flowables=keep_flowables,
    )
    modes = modes_median_freeze(min_coverage=min_coverage)
    log.info(
        'Hybrid median-freeze modes (min_coverage=%.2f): %s',
        min_coverage,
        pd.Series(modes).value_counts().to_dict(),
    )

    mecs_fbs = FlowBySector.return_FBS(
        method=mecs_method, download_sources_ok=True, download_fbs_ok=True
    )
    mecs = pd.DataFrame(mecs_fbs)

    frames: list[pd.DataFrame] = []
    for flowable, mecs_class in flowables_cfg.items():
        frames.append(
            _hybrid_shares_for_flowable(
                flowable=str(flowable),
                mecs_class=None if mecs_class is None else str(mecs_class),
                facility_union=union,
                mecs=mecs,
                mecs_method=mecs_method,
                modes=modes,
                config=config,
                year=year,
                target_schema_year=target_schema_year,
                min_coverage=min_coverage,
            )
        )
    frame = pd.concat(frames, ignore_index=True)
    fbs = FlowBySector(
        frame, full_name=full_name, config=config, convert_df_to_flowby=True
    )
    fbs.config.update({'data_format': 'FBS'})
    return fbs


def reassign_process_to_sectors(
    df: pd.DataFrame,
    year: str,
    file_list: list[str],
    external_config_path: str | None = None,
) -> pd.DataFrame:
    """
    Reassigns emissions from a specific process or SCC and NAICS combination
    to a new NAICS.

    :param df: a dataframe of emissions and mapped faciliites from stewicombo
    :param year: year as str
    :param file_list: list, one or more names of csv files in
        process_adjustmentpath
    :param external_config_path, str, optional path to an FBS method outside
        flowsa repo
    :return: df
    """
    df_adj = pd.DataFrame()
    for file in file_list:
        fpath: Path = process_adjustmentpath / f'{file}.csv'
        if external_config_path:
            f_out_path = (
                Path(external_config_path) / 'process_adjustments' / f'{file}.csv'
            )
            if f_out_path.is_file():
                fpath = f_out_path
        log.debug(f'modifying processes from {fpath}')
        df_adj0 = pd.read_csv(fpath, dtype='str')
        df_adj = pd.concat([df_adj, df_adj0], ignore_index=True)

    # Eliminate duplicate adjustments
    df_adj = df_adj.drop_duplicates()
    if (
        sum(df_adj.duplicated(subset=['source_naics', 'source_process'], keep=False))
        > 0
    ):
        log.warning('duplicate process adjustments')
        df_adj = df_adj.drop_duplicates(subset=['source_naics', 'source_process'])

    # obtain and prepare SCC dataset
    keep_sec_cntx = bool(any('/' in s for s in df.Compartment.unique()))
    df_fbp = stewi.getInventory(
        'NEI',
        year,
        stewiformat='flowbyprocess',
        download_if_missing=True,
        keep_sec_cntx=keep_sec_cntx,
    )
    df_fbp = df_fbp[df_fbp['Process'].isin(df_adj['source_process'])]
    df_fbp = (
        df_fbp.assign(Source='NEI')
        .pipe(addChemicalMatches)
        .pipe(stewicombo.overlaphandler.remove_NEI_overlaps, SCC=True)
        .drop(columns=['_CompartmentPrimary'], errors='ignore')
    )

    # merge in NAICS data
    facility_df = (
        df.filter(['FacilityID', 'NAICS', 'Location'])
        .reset_index(drop=True)
        .drop_duplicates(keep='first')
    )
    df_fbp = df_fbp.merge(facility_df, how='left', on='FacilityID')
    df_fbp['Year'] = year

    # TODO: expand naics list in scc file to include child naics automatically
    df_fbp = df_fbp.merge(
        df_adj,
        how='inner',
        left_on=['NAICS', 'Process'],
        right_on=['source_naics', 'source_process'],
    )

    # subtract emissions by SCC from specific facilities
    df_emissions = (
        df_fbp.groupby(['FacilityID', 'FlowName', 'Compartment'])
        .agg({'FlowAmount': 'sum'})
        .rename(columns={'FlowAmount': 'Emissions'})
    )
    df = (
        df.merge(df_emissions, how='left', on=['FacilityID', 'FlowName', 'Compartment'])
        .assign(Emissions=lambda x: x['Emissions'].fillna(value=0))
        .assign(FlowAmount=lambda x: x['FlowAmount'] - x['Emissions'])
        .drop(columns=['Emissions'])
    )

    # add back in emissions under the correct target NAICS
    df_fbp = df_fbp.drop(
        columns=[
            'Process',
            'NAICS',
            'source_naics',
            'source_process',
            'ProcessType',
            'SRS_CAS',
            'SRS_ID',
        ]
    ).rename(columns={'target_naics': 'NAICS'})
    return pd.concat([df, df_fbp], ignore_index=True)


def extract_facility_data(inventory_dict: InventoryDict) -> pd.DataFrame:
    """
    Returns df of facilities from each inventory in inventory_dict,
    including FIPS code
    :param inventory_dict: a dictionary of inventory types and years (e.g.,
                {'NEI':'2017', 'TRI':'2017'})
    :return: df
    """
    facilities_list: list[pd.DataFrame] = []
    # load facility data from stewi output directory, keeping only the
    # facility IDs, and geographic information
    for database, year in inventory_dict.items():
        facilities = stewi.getInventoryFacilities(
            database, year, download_if_missing=True
        )
        facilities = facilities[['FacilityID', 'State', 'County', 'NAICS']]
        if len(facilities[facilities.duplicated(subset='FacilityID', keep=False)]) > 0:
            log.debug(
                f'Duplicate facilities in {database}_{year} - keeping first listed'
            )
            facilities = facilities.drop_duplicates(subset='FacilityID', keep='first')
        facilities_list.append(facilities)

    facility_mapping = pd.concat(facilities_list, ignore_index=True)
    return facility_mapping.pipe(apply_county_FIPS)


def obtain_NAICS_from_facility_matcher(inventory_list: list[str]) -> pd.DataFrame:
    """
    Returns dataframe of all facilities with included in inventory_list with
    their first or primary NAICS.
    :param inventory_list: a list of inventories (e.g., ['NEI', 'TRI'])
    :return: df
    """
    # Access NAICS From facility matcher and assign based on FRS_ID
    all_NAICS = facilitymatcher.get_FRS_NAICSInfo_for_facility_list(
        frs_id_list=None,
        inventories_of_interest_list=inventory_list,
        download_if_missing=True,
    )
    return all_NAICS.query('PRIMARY_INDICATOR == "PRIMARY"').drop(
        columns=['PRIMARY_INDICATOR']
    )


def assign_naics_to_stewicombo(
    df: pd.DataFrame,
    all_NAICS: pd.DataFrame,
    facility_mapping: pd.DataFrame,
) -> pd.DataFrame:
    """
    Apply naics to combined inventory preferentially using FRS_ID.
    When FRS_ID does not provide unique NAICS, then use NAICS assigned by
    inventory source
    :param df: combined inventory from stewicombo
    :param all_NAICS: df of NAICS by FRS_ID
    :param facility_mapping: df of NAICS by Facility_ID
    """
    # first merge in NAICS by FRS, but only where the FRS has a single NAICS
    df = df.merge(
        all_NAICS[~all_NAICS.duplicated(subset=['FRS_ID', 'Source'], keep=False)],
        how='left',
        on=['FRS_ID', 'Source'],
    )

    # next use NAICS from inventory sources
    return (
        df.merge(
            facility_mapping[['FacilityID', 'NAICS']],
            how='left',
            on='FacilityID',
            suffixes=(None, '_y'),
        )
        .assign(NAICS=lambda x: x['NAICS'].fillna(x['NAICS_y']))
        .drop(columns=['NAICS_y'])
        .query('NAICS != "None"')
    )


def prepare_stewi_fbs(df_load: pd.DataFrame, config: dict[str, Any]) -> FlowBySector:
    """
    Prepare stewi or stewicombo emissions as FBS.

    Optional method keys (same pattern as ``compartments`` for compartment filter):

    - ``include_flow_names``: list of stewi ``FlowName`` values to keep; omitted
      means all flows. Popped before ``prepare_fbs`` so it is not reapplied on
      FBS-shaped data. Prefer this over ``selection_fields`` on stewi sources.
    """
    include_flow_names = config.pop('include_flow_names', None)
    config.pop('selection_fields', None)

    if include_flow_names is not None:
        df_load = _subset_stewi_include_flow_names(df_load, include_flow_names)

    inventory_dict = config['inventory_dict']
    config['fedefl_mapping'] = [x for x in inventory_dict if x != 'RCRAInfo']
    config['drop_unmapped_rows'] = True
    if 'year' not in config:
        config['year'] = df_load['Year'][0]

    # Alias legacy stewi key so CRHW (and peers) using target_naics_year still load.
    if 'target_schema_year' not in config and 'target_naics_year' in config:
        config['target_schema_year'] = config['target_naics_year']

    activity_schema = f"NAICS_{config['activity_schema']['naics']['year']}_Code"

    prepared = df_load.pipe(update_geoscale, config['geoscale']).rename(
        columns={'NAICS': 'ActivityProducedBy', 'Source': 'SourceName'}
    )
    if (
        'ActivityConsumedBy' not in prepared.columns
        or prepared['ActivityConsumedBy'].isna().all()
    ):
        prepared = prepared.assign(ActivityConsumedBy=np.nan)

    fbs = FlowByActivity(
        prepared.assign(Class='Chemicals')
        .pipe(
            naics_mapping.convert_naics_year,
            f"NAICS_{config['target_schema_year']}_Code",
            activity_schema,
            config['full_name'],
        )
        .assign(
            FlowType=lambda x: np.where(
                x['SourceName'] == 'RCRAInfo', 'WASTE_FLOW', 'ELEMENTARY_FLOW'
            )
        )
        .pipe(assign_fips_location_system, config['year'])
        # ^^ Consider upating this old function
        .drop(
            columns=[
                'FacilityID',
                'FRS_ID',
                'State',
                'County',
                'Plant primary fuel',
                'PrimaryFuelCategory',
                'fuel_class',
                'sector',
                'source',
                'year',
            ],
            errors='ignore',
        )
        .dropna(subset=['Location'])
        .reset_index(drop=True),
        full_name=config.get('full_name'),
        config=config,
        convert_df_to_flowby=True,
    ).prepare_fbs()

    fbs.config.update({'data_format': 'FBS'})
    return fbs


def add_stewi_metadata(inventory_dict: InventoryDict) -> dict[str, Any]:
    """
    Access stewi metadata for generating FBS metdata file
    :param inventory_dict: a dictionary of inventory types and years (e.g.,
                {'NEI':'2017', 'TRI':'2017'})
    :return: combined dictionary of metadata from each inventory
    """
    return compile_metadata(inventory_dict)


def add_stewicombo_metadata(inventory_name: str) -> dict[str, Any]:
    """Access locally stored stewicombo metadata by filename"""
    return read_source_metadata(
        stewicombo.globals.paths, set_stewicombo_meta(inventory_name)
    )


if __name__ == '__main__':
    import bedrock

    fbs = bedrock.transform.flowbysector.FlowBySector.generateFlowBySector(
        'CRHW_national_2017'
    )
    # fbs = bedrock.transform.flowbysector.FlowBySector.generateFlowBySector('TRI_DMR_state_2017')
