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
from bedrock.transform.flowbyfunctions import assign_fips_location_system
from bedrock.transform.flowbysector import FlowBySector
from bedrock.utils.config.settings import process_adjustmentpath
from bedrock.utils.logging.flowsa_log import log
from bedrock.utils.mapping import naics as naics_mapping
from bedrock.utils.mapping.location import apply_county_FIPS, update_geoscale
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
    df = stewi.getInventory('eGRID', year_str, download_if_missing=True)
    facilities = stewi.getInventoryFacilities(
        'eGRID', year_str, download_if_missing=True
    )
    facilities = (
        facilities[['FacilityID', 'State', 'County', 'Plant primary fuel']]
        .drop_duplicates(subset='FacilityID', keep='first')
        .pipe(lambda d: apply_county_FIPS(d, unmatched='national'))
    )
    return df.merge(facilities, how='left', on='FacilityID')


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

    activity_schema_raw = config['activity_schema']
    if isinstance(activity_schema_raw, str):
        activity_schema: str = activity_schema_raw
    else:
        activity_schema = config.get('activity_schema', {}).get(config['year'])

    fbs = FlowByActivity(
        df_load.pipe(update_geoscale, config['geoscale'])
        # ^^ update location to appropriate geoscale prior to aggregating
        .rename(columns={'NAICS': 'ActivityProducedBy', 'Source': 'SourceName'})
        .assign(Class='Chemicals')
        .assign(ActivityConsumedBy=np.nan)
        .pipe(
            naics_mapping.convert_naics_year,
            f"NAICS_{config['target_naics_year']}_Code",
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
        .drop(columns=['FacilityID', 'FRS_ID', 'State', 'County'], errors='ignore')
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
