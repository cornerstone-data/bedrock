# EIA_MECS.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
MANUFACTURING ENERGY CONSUMPTION SURVEY (MECS)
https://www.eia.gov/consumption/manufacturing/data/2014/
Last updated: 8 Sept. 2020
"""
import io
import os
from typing import Any, List, TypeVar

import numpy as np
import pandas as pd
import stewi
from requests import Response

from bedrock.extract.eia.EIA_CBECS_Land import calculate_total_facility_land_area
from bedrock.extract.flowbyactivity import FlowByActivity, getFlowByActivity
from bedrock.extract.generateflowbyactivity import generateFlowByActivity
from bedrock.transform.flowbyclean import (
    define_parentincompletechild_descendants,
    load_prepare_clean_source,
)
from bedrock.transform.flowbyfunctions import assign_fips_location_system
from bedrock.utils.config.common import (
    WITHDRAWN_KEYWORD,
    get_catalog_info,
    load_crosswalk,
)
from bedrock.utils.economic.units import (
    HEATING_OIL_MMBTU_PER_GALLON,
    PROPANE_MMBTU_PER_GALLON,
)
from bedrock.utils.io.gcp import (
    download_extract_input_from_gcs_if_not_exists,
    load_from_gcs,
)
from bedrock.utils.io.gcp_paths import gcs_extract_input_path
from bedrock.utils.io.local_extract_input_data import local_extract_input_dir
from bedrock.utils.logging.flowsa_log import log
from bedrock.utils.mapping.location import (
    US_FIPS,
    assign_census_regions,
    get_region_and_division_codes,
)


def eia_mecs_URL_helper(
    *, build_url: str, config: dict[str, Any], year: str, **_kwrags: dict[str, Any]
) -> List[str]:
    """
    This helper function uses the "build_url" input from generateflowbyactivity.py,
    which is a base url for data imports that requires parts of the url
    text string to be replaced with info specific to the data year. This
    function does not parse the data, only modifies the urls from which data
    is obtained.
    :param build_url: string, base url
    :param config: dictionary, items in FBA method yaml
    :param args: dictionary, arguments specified when running generateflowbyactivity.py
        generateflowbyactivity.py ('year' and 'source')
    :return: list, urls to call, concat, parse, format into
    Flow-By-Activity format
    """
    # initiate url list
    urls = []

    # for all tables listed in the source config file...
    for table in config['tables']:
        # start with build url
        url = build_url
        # replace '__year__' in build url
        url = url.replace('__year__', year)
        # 2014 files are in .xlsx format; 2010 files are in .xls format
        if year == '2010':
            url = url[:-1]
        # replace '__table__' in build url
        url = url.replace('__table__', table)
        # add to list of urls
        urls.append(url)

    return urls


def eia_mecs_land_call(
    *, resp: Response, source: str, year: str, **_: dict[str, Any]
) -> pd.DataFrame:
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param args: dictionary, arguments specified when running
        generateflowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    buf = io.BytesIO(resp.content)
    df = _eia_mecs_land_read_from_excel(buf, year)
    table = os.path.basename(str(resp.url))
    with open(os.path.join(local_extract_input_dir(source, year), table), "wb") as f:
        f.write(resp.content)
    return df


def _eia_mecs_land_read_from_excel(source: str | io.BytesIO, year: str) -> pd.DataFrame:
    """Read Table 9.1 / RSE 9.1 from a local xlsx path or buffer (same as HTTP)."""
    if isinstance(source, io.BytesIO):
        source.seek(0)
    df_raw_data = pd.read_excel(source, sheet_name='Table 9.1')
    if isinstance(source, io.BytesIO):
        source.seek(0)
    df_raw_rse = pd.read_excel(source, sheet_name='RSE 9.1')
    if year == "2014":
        # skip rows and remove extra rows at end of dataframe
        df_rse = pd.DataFrame(df_raw_rse.loc[12:93]).reindex()
        df_data = pd.DataFrame(df_raw_data.loc[16:97]).reindex()
        df_description = pd.DataFrame(df_raw_data.loc[16:97]).reindex()
        # pull first 7 columns
        df_rse = df_rse.iloc[:, 0:7]
        df_data = df_data.iloc[:, 0:7]
        df_description = df_description.iloc[:, 0:7]

        df_description.columns = [
            "NAICS Code(a)",
            "Subsector and Industry",
            "Approximate Enclosed Floorspace of All "
            "Buildings Onsite (million sq ft)",
            "Establishments(b) (counts)",
            "Average Enclosed Floorspace per Establishment (sq ft)",
            "Approximate Number of All Buildings Onsite (counts)",
            "Average Number of Buildings Onsite per Establishment (counts)",
        ]
        df_data.columns = [
            "NAICS Code(a)",
            "Subsector and Industry",
            "Approximate Enclosed Floorspace of All "
            "Buildings Onsite (million sq ft)",
            "Establishments(b) (counts)",
            "Average Enclosed Floorspace per Establishment (sq ft)",
            "Approximate Number of All Buildings Onsite (counts)",
            "Average Number of Buildings Onsite per Establishment (counts)",
        ]
        df_rse.columns = [
            "NAICS Code(a)",
            "Subsector and Industry",
            "Approximate Enclosed Floorspace of All Buildings Onsite "
            "(million sq ft)",
            "Establishments(b) (counts)",
            "Average Enclosed Floorspace per Establishment (sq ft)",
            "Approximate Number of All Buildings Onsite (counts)",
            "Average Number of Buildings Onsite per Establishment (counts)",
        ]

        # Drop unused columns
        df_description = df_description.drop(
            columns=[
                "Approximate Enclosed Floorspace of All Buildings Onsite "
                "(million sq ft)",
                "Establishments(b) (counts)",
                "Average Enclosed Floorspace per Establishment (sq ft)",
                "Approximate Number of All Buildings Onsite (counts)",
                "Average Number of Buildings Onsite per Establishment " "(counts)",
            ]
        )

        df_data = df_data.drop(columns=["Subsector and Industry"])
        df_rse = df_rse.drop(columns=["Subsector and Industry"])
    else:
        df_rse = pd.DataFrame(df_raw_rse.loc[14:97]).reindex()
        df_data = pd.DataFrame(df_raw_data.loc[16:99]).reindex()
        df_description = pd.DataFrame(df_raw_data.loc[16:99]).reindex()
        df_description.columns = [
            "NAICS Code(a)",
            "Subsector and Industry",
            "Approximate Enclosed Floorspace of All "
            "Buildings Onsite (million sq ft)",
            "Establishments(b) (counts)",
            "Average Enclosed Floorspace per Establishment (sq ft)",
            "Approximate Number of All Buildings Onsite (counts)",
            "Average Number of Buildings Onsite per Establishment (counts)",
        ]
        df_data.columns = [
            "NAICS Code(a)",
            "Subsector and Industry",
            "Approximate Enclosed Floorspace of All Buildings Onsite "
            "(million sq ft)",
            "Establishments(b) (counts)",
            "Average Enclosed Floorspace per Establishment (sq ft)",
            "Approximate Number of All Buildings Onsite (counts)",
            "Average Number of Buildings Onsite per Establishment (counts)",
        ]
        df_rse.columns = [
            "NAICS Code(a)",
            "Subsector and Industry",
            "Approximate Enclosed Floorspace of All Buildings Onsite "
            "(million sq ft)",
            "Establishments(b) (counts)",
            "Average Enclosed Floorspace per Establishment (sq ft)",
            "Approximate Number of All Buildings Onsite (counts)",
            "Average Number of Buildings Onsite per Establishment (counts)",
        ]
        # Drop unused columns
        df_description = df_description.drop(
            columns=[
                "Approximate Enclosed Floorspace of All Buildings Onsite "
                "(million sq ft)",
                "Establishments(b) (counts)",
                "Average Enclosed Floorspace per Establishment (sq ft)",
                "Approximate Number of All Buildings Onsite (counts)",
                "Average Number of Buildings Onsite per Establishment " "(counts)",
            ]
        )
        df_data = df_data.drop(columns=["Subsector and Industry"])
        df_rse = df_rse.drop(columns=["Subsector and Industry"])

    df_data = df_data.melt(
        id_vars=["NAICS Code(a)"], var_name="FlowName", value_name="FlowAmount"
    )
    df_rse = df_rse.melt(
        id_vars=["NAICS Code(a)"], var_name="FlowName", value_name="Spread"
    )

    df = pd.merge(df_data, df_rse)
    df = pd.merge(df, df_description)

    return df


def eia_mecs_land_parse(
    *, df_list: List[pd.DataFrame], year: str, **_: dict[str, Any]
) -> pd.DataFrame:
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    df_array = []
    for dataframes in df_list:

        dataframes = dataframes.rename(columns={'NAICS Code(a)': 'ActivityConsumedBy'})
        dataframes = dataframes.rename(
            columns={'Subsector and Industry': 'Description'}
        )
        dataframes.loc[dataframes.Description == "Total", "ActivityConsumedBy"] = (
            "31-33"
        )
        unit = []
        for index, row in dataframes.iterrows():
            if row["FlowName"] == "Establishments(b) (counts)":
                row["FlowName"] = "Establishments (counts)"
            flow_name_str = row["FlowName"]
            flow_name_array = flow_name_str.split("(")
            row["FlowName"] = flow_name_array[0]
            unit_text = flow_name_array[1]
            unit_text_array = unit_text.split(")")
            if unit_text_array[0] == "counts":
                unit.append("p")
            else:
                unit.append(unit_text_array[0])
            ACB = row["ActivityConsumedBy"]
            ACB_str = str(ACB).strip()
            row["ActivityConsumedBy"] = ACB_str
        df_array.append(dataframes)
    df = pd.concat(df_array, sort=False)

    # trim whitespace associated with Activity
    df['Description'] = df['Description'].str.strip()

    # add manufacturing to end of description if missing
    df['Description'] = df['Description'].apply(
        lambda x: x + ' Manufacturing' if not x.endswith('Manufacturing') else x
    )

    # replace withdrawn code
    df.loc[df['FlowAmount'] == "Q", 'FlowAmount'] = WITHDRAWN_KEYWORD
    df.loc[df['FlowAmount'] == "N", 'FlowAmount'] = WITHDRAWN_KEYWORD
    df["Class"] = 'Land'
    df["SourceName"] = 'EIA_MECS_Land'
    df['Year'] = year
    df["Compartment"] = 'ground'
    df['MeasureofSpread'] = "RSE"
    df['Location'] = US_FIPS
    df['Unit'] = unit
    df = assign_fips_location_system(df, year)
    df['FlowType'] = "ELEMENTARY_FLOW"
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp

    # modify flowname
    df['FlowName'] = df['FlowName'].str.strip()

    return df


def eia_mecs_land_load_gcs(**kwargs: Any) -> pd.DataFrame:
    """For each url the file gets download and stored locally from gcs"""
    path = download_extract_input_from_gcs_if_not_exists(kwargs)
    return _eia_mecs_land_read_from_excel(path, str(kwargs["year"]))


def eia_mecs_energy_load_gcs(**kwargs: Any) -> pd.DataFrame:
    """For each url the file gets download and stored locally from gcs"""
    path = download_extract_input_from_gcs_if_not_exists(kwargs)
    df_raw_data = pd.read_excel(path, sheet_name=0, header=None)
    df_raw_rse = pd.read_excel(path, sheet_name=1, header=None)
    return _eia_clean_mecs_energy(
        df_raw_data, df_raw_rse, year=kwargs["year"], config=kwargs["config"]
    )


def eia_mecs_energy_call(
    *,
    resp: Response | None,
    source: str,
    year: str,
    config: dict[str, Any],
    **_kwargs: dict[str, Any],
) -> pd.DataFrame:
    """
    Convert response for calling url to pandas dataframe, begin
    parsing df into FBA format
    :param resp: df, response from url call
    :param year: year
    :param config: dictionary, items in FBA method yaml
    :return: pandas dataframe of original source data
    """
    if resp is None:
        raise ValueError("Response cannot be None")
    # read raw data from url into dataframe
    # (include both Sheet 1 (data) and Sheet 2 (relative standard errors))
    df_raw_data = pd.read_excel(io.BytesIO(resp.content), sheet_name=0, header=None)
    df_raw_rse = pd.read_excel(io.BytesIO(resp.content), sheet_name=1, header=None)

    table = os.path.basename(str(resp.url))
    with open(os.path.join(local_extract_input_dir(source, year), table), "wb") as f:
        f.write(resp.content)

    df = _eia_clean_mecs_energy(df_raw_data, df_raw_rse, year, config)

    return df


def _eia_clean_mecs_energy(
    df_raw_data: pd.DataFrame,
    df_raw_rse: pd.DataFrame,
    year: str,
    config: dict[str, Any],
) -> pd.DataFrame:
    """Performs minimal pre-processing on the raw EIA tables."""
    # load dictionary containing information about each energy table
    # (the .yaml includes information such as column names, units, and
    # which rows to grab)
    table_dict = config['table_dict']

    # retrieve table name from cell A3 of Excel file
    table = df_raw_data.iloc[2][0]
    # drop the table description (retain only table name)
    table = table.split('    ')[0]
    table = table.split('  ')[0]

    # for each of the census regions...
    # - grab the appropriate rows and columns
    # - add column names
    # - "unpivot" dataframe from wide format to long format
    # - add columns denoting census region, relative standard error, units
    # - concatenate census region data into master dataframe
    df_data = pd.DataFrame()

    for region in table_dict[year][table]['regions']:

        # grab relevant columns
        # (this is a necessary step because code was retaining some seemingly
        # blank columns) determine number of columns in table, based on
        # number of column names
        num_cols = len(table_dict[year][table]['col_names'])
        # keep only relevant columns
        df_raw_data = df_raw_data.iloc[:, 0:num_cols]
        df_raw_rse = df_raw_rse.iloc[:, 0:num_cols]

        # grab relevant rows
        # get indices for relevant rows
        grab_rows = table_dict[year][table]['regions'][region]
        grab_rows_rse = table_dict[year][table]['rse_regions'][region]
        # keep only relevant rows
        df_data_region = pd.DataFrame(
            df_raw_data.loc[grab_rows[0] - 1 : grab_rows[1] - 1]
        ).reindex()
        df_rse_region = pd.DataFrame(
            df_raw_rse.loc[grab_rows_rse[0] - 1 : grab_rows_rse[1] - 1]
        ).reindex()

        # assign column names
        # if table name ends in 1, the column names are pulled from the table dict unchanged
        if table[-1] == '1':
            df_data_region.columns = table_dict[year][table]['col_names']
            df_rse_region.columns = table_dict[year][table]['col_names']
        # if table name ends in 2, the units must be stripped from the column names listed in the table dict
        if table[-1] in ['2', '0', '5', '6', '7']:
            df_data_region.columns = [
                name.split(' | ', 2)[0] for name in table_dict[year][table]['col_names']
            ]
            df_rse_region.columns = [
                name.split(' | ', 2)[0] for name in table_dict[year][table]['col_names']
            ]

        skip_cols = [c for c in df_data_region.columns if str(c).startswith('SKIP')]
        if skip_cols:
            df_data_region = df_data_region.drop(columns=skip_cols)
            df_rse_region = df_rse_region.drop(columns=skip_cols)

        if table[-1] == '5':
            major_name = ""
            df_data_region = df_data_region.dropna()
            energy_sources_data = []
            for _, row in df_data_region.iterrows():
                name = row["Energy Source"]
                if "    " in str(name):
                    minor_name = str(name).replace('    ', '')
                    if "Biomass Total" == minor_name:
                        major_name = minor_name
                        minor_name = ""
                    name = str(major_name) + " " + str(minor_name)
                else:
                    major_name = str(row["Energy Source"])
                energy_sources_data.append(name)
            df_data_region['Energy Source'] = energy_sources_data
            df_data_region = df_data_region.rename(
                columns={'Energy Source': 'FlowName', 'Total First Use': 'FlowAmount'}
            )
            df_data_region['NAICS Code'] = 'All Purposes'
            major_name = ""
            df_rse_region = df_rse_region.dropna()
            energy_sources_rse = []
            for _, row in df_rse_region.iterrows():
                name = row["Energy Source"]
                if "    " in str(name):
                    minor_name = str(name).replace('    ', '')
                    if "Biomass Total" == minor_name:
                        major_name = minor_name
                        minor_name = ""
                    name = str(major_name) + " " + str(minor_name)
                else:
                    major_name = str(row["Energy Source"])
                energy_sources_rse.append(name)
            df_rse_region['Energy Source'] = energy_sources_rse
            df_rse_region = df_rse_region.rename(
                columns={'Energy Source': 'FlowName', 'Total First Use': 'Spread'}
            )
        else:
            # "unpivot" dataframe from wide format to long format
            # ('NAICS code' and 'Subsector and Industry' are identifier variables)
            # (all other columns are value variables)
            df_data_region = pd.melt(
                df_data_region,
                id_vars=list(df_data_region.columns[0:2]),
                value_vars=list(df_data_region.columns[2:]),
                var_name='FlowName',
                value_name='FlowAmount',
            )
            df_rse_region = pd.melt(
                df_rse_region,
                id_vars=list(df_rse_region.columns[0:2]),
                value_vars=list(df_rse_region.columns[2:]),
                var_name='FlowName',
                value_name='Spread',
            )

        # add relative standard error data
        df_data_region = pd.merge(df_data_region, df_rse_region)
        # add census region
        df_data_region['Location'] = region
        df_data_region['Table Name'] = table

        # add units
        # if table name ends in 1, units must be extracted from flow names
        if table[-1] == '1':
            flow_name_array = df_data_region['FlowName'].str.split('\s+\|+\s')
            df_data_region['Unit'] = flow_name_array.str[1]
            df_data_region['FlowName'] = flow_name_array.str[0]
        # if table name ends in 2, units are 'trillion Btu'
        elif table[-1] in ['2', '5', '6']:
            df_data_region['Unit'] = 'Trillion Btu'
            if table[-3] == '7':  # Table 7.2
                df_data_region['Unit'] = 'USD / million btu'
        elif str(table).rstrip().endswith('7.7'):
            # YAML col_names are the three electricity columns only (million kWh).
            # Full EIA 7.7 also has NG (bcf) and steam (billion Btu); those are
            # not extracted. Other Table 7.x do not hit this branch (7.1 / 7.2 /
            # 7.10 dispatch on a different last character).
            df_data_region['Unit'] = 'million kWh'
        elif table[-1] == '0':
            df_data_region['Unit'] = 'million USD'

        data_type = table_dict[year][table]['data_type']
        if data_type == 'nonfuel consumption':
            df_data_region['Class'] = 'Other'
        elif data_type == 'fuel consumption':
            df_data_region['Class'] = 'Energy'
        elif data_type == 'money':
            df_data_region['Class'] = 'Money'

        # remove extra spaces before 'Subsector and Industry' descriptions
        if table[-1] != '5':
            df_data_region['Subsector and Industry'] = df_data_region[
                'Subsector and Industry'
            ].str.lstrip(' ')
        else:
            df_data_region['Subsector and Industry'] = "Total"

        # concatenate census region data with master dataframe
        df_data = pd.concat([df_data, df_data_region])

    return df_data


def eia_mecs_energy_parse(
    *, df_list: List[pd.DataFrame], source: str, year: str, **_kwargs: dict[str, Any]
) -> pd.DataFrame:
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year
    :param source: source
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    # concatenate dataframe list into single dataframe
    df = pd.concat(df_list, sort=True)

    # rename columns to match standard flowbyactivity format
    df = df.rename(
        columns={'NAICS Code': 'ActivityConsumedBy', 'Table Name': 'Description'}
    )
    df.loc[df['Subsector and Industry'] == 'Total', 'ActivityConsumedBy'] = '31-33'
    df = df.drop(columns='Subsector and Industry')
    df['ActivityConsumedBy'] = df['ActivityConsumedBy'].str.strip()
    # add hardcoded data
    df["SourceName"] = source
    df["Compartment"] = None
    df['FlowType'] = 'TECHNOSPHERE_FLOW'
    df['Year'] = year
    df['MeasureofSpread'] = "RSE"
    # assign location codes and location system
    df.loc[df['Location'] == 'Total United States', 'Location'] = US_FIPS
    df = assign_fips_location_system(df, year)
    df = assign_census_regions(df)
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp

    # drop rows that reflect subtotals (only necessary in 2014)
    df = df.dropna(subset=['ActivityConsumedBy'])

    df = df.assign(
        FlowAmount=df.FlowAmount.mask(
            df.FlowAmount.str.isnumeric() == False, np.nan  # noqa: E712
        ),
        Suppressed=df.FlowAmount.where(
            df.FlowAmount.str.isnumeric() == False, np.nan  # noqa: E712
        ),
        Spread=df.Spread.mask(df.Spread.str.isnumeric() == False, np.nan),  # noqa: E712
    )

    return df


def estimate_suppressed_mecs_energy(
    fba: FlowByActivity, **_kwargs: Any
) -> FlowByActivity:
    '''
    Rough first pass at an estimation method, for testing purposes. This
    will drop rows with 'D' or 'Q' values, on the grounds that as far as I can
    tell we don't have any more information for them than we do for any
    industry without its own line item in the MECS anyway. '*' is for value
    less than 0.5 Trillion Btu and will be assumed to be 0.25 Trillion Btu
    '''
    if 'Suppressed' not in fba.columns:
        log.warning(
            'The current MECS dataframe does not contain data '
            'on estimation method and so suppressed data will '
            'not be assessed.'
        )
        return fba
    dropped = fba.query('Suppressed not in ["D", "Q"]')
    unsuppressed = dropped.assign(
        FlowAmount=dropped.FlowAmount.mask(dropped.Suppressed == '*', 0.25)
    )

    return unsuppressed.drop(columns='Suppressed')


DF = TypeVar('DF', bound=pd.DataFrame)

#: MECS survey years the pipeline builds (EIA_MECS_Energy.yaml), searched
#: nearest-first when a suppressed feedstock cell is filled from another
#: survey (#1200).
MECS_SURVEY_YEARS = (2018, 2022)
#: MECS nonfuel (feedstock) table, the basis for non-energy use (#1200).
MECS_FEEDSTOCK_TABLE = 'Table 2.1'
SUPPRESSED_CODES = ('D', 'Q')
#: ``Suppressed`` marker for a cell filled here; not ``D``/``Q`` so
#: :func:`estimate_suppressed_mecs_energy` keeps it.
GHGRP_FILL_MARKER = 'GHGRP'


def ghgrp_co2_by_naics(year: int) -> pd.Series:
    '''GHGRP CO2 (kg) by facility NAICS for *year*, all subparts but D
    (electric power). Empty if stewi has no inventory for the year.'''
    try:
        flows = stewi.getInventory(
            'GHGRP', year, stewiformat='flowbyprocess', download_if_missing=True
        )
        facilities = stewi.getInventoryFacilities(
            'GHGRP', year, download_if_missing=True
        )
    except Exception as err:  # noqa: BLE001 - a missing year leaves ratio 1
        log.warning(f'GHGRP {year} unavailable for MECS fill: {err}')
        return pd.Series(dtype=float)
    if flows is None or facilities is None:
        return pd.Series(dtype=float)
    flows = flows[(flows['FlowName'] == 'Carbon Dioxide') & (flows['Process'] != 'D')]
    flows = flows.merge(
        facilities[['FacilityID', 'NAICS']], on='FacilityID', how='left'
    )
    naics = flows['NAICS'].astype(str).str.replace(r'\.0$', '', regex=True)
    return flows.groupby(naics)['FlowAmount'].sum()


def _ghgrp_ratio(
    code: str, ghgrp: dict[int, pd.Series], year: int, other: int
) -> float:
    '''GHGRP CO2 of facilities under NAICS *code*, *year* over *other*; 1.0
    when either year has none.'''
    totals = []
    for y in (year, other):
        series = ghgrp.get(y, pd.Series(dtype=float))
        totals.append(
            float(series[series.index.astype(str).str.startswith(code)].sum())
        )
    if totals[0] <= 0 or totals[1] <= 0:
        return 1.0
    return totals[0] / totals[1]


def _parent_remainder(
    national: pd.DataFrame, code: str, flow: str, filled: dict[tuple[str, str], float]
) -> float | None:
    '''Room left in *code*'s nearest reported parent for *flow*: the parent's
    value less every other child it lists (and fills already made under it).
    None when no numeric parent exists.'''
    rows = national[national['FlowName'] == flow]
    numeric = rows[~rows['Suppressed'].isin(SUPPRESSED_CODES)]
    values: dict[str, float] = dict(
        zip(numeric['ActivityConsumedBy'], numeric['FlowAmount'].astype(float))
    )
    codes = set(rows['ActivityConsumedBy'])
    prefixes = [c for c in codes if c != code and code.startswith(c) and c.isdigit()]
    if not prefixes:
        return None
    parent = max(prefixes, key=len)
    if parent not in values:
        return None

    def nearest_parent(c: str) -> str | None:
        found = [p for p in codes if p != c and c.startswith(p) and p.isdigit()]
        return max(found, key=len) if found else None

    siblings = [c for c in codes if c != code and nearest_parent(c) == parent]
    used = sum(filled.get((c, flow), values.get(c, 0.0)) for c in siblings)
    return values[parent] - used


def fill_suppressed_feedstock(
    fba: DF,
    year: int,
    other_surveys: dict[int, pd.DataFrame],
    ghgrp: dict[int, pd.Series],
) -> DF:
    '''Fill suppressed (``D``/``Q``) national MECS feedstock cells (#1200).

    Each such Table 2.1 cell takes the value the nearest other survey reports
    for the same NAICS and fuel, scaled by the ratio of that NAICS's GHGRP CO2
    between the two years, and capped at the room left in its nearest reported
    parent. Cells no other survey reports stay suppressed. Filled cells carry
    ``Suppressed`` = :data:`GHGRP_FILL_MARKER`.

    :param other_surveys: Table 2.1 national rows of other survey years
    :param ghgrp: GHGRP CO2 by NAICS, by year (:func:`ghgrp_co2_by_naics`)
    '''
    fba = fba.copy()
    mask = (fba['Description'] == MECS_FEEDSTOCK_TABLE) & (fba['Location'] == US_FIPS)
    national = fba[mask]
    targets = national[
        national['Suppressed'].isin(SUPPRESSED_CODES)
        & (national['FlowName'] != 'Total')
        & national['ActivityConsumedBy'].astype(str).str.isdigit()
    ]
    order = sorted(other_surveys, key=lambda y: (abs(y - year), -y))
    filled: dict[tuple[str, str], float] = {}
    # Deepest codes first so a parent's room accounts for its filled children.
    for idx in sorted(
        targets.index, key=lambda i: -len(str(targets.at[i, 'ActivityConsumedBy']))
    ):
        code = str(targets.at[idx, 'ActivityConsumedBy'])
        flow = str(targets.at[idx, 'FlowName'])
        for other in order:
            ref = other_surveys[other]
            hit = ref[
                (ref['ActivityConsumedBy'] == code)
                & (ref['FlowName'] == flow)
                & (ref['Unit'].astype(str) == str(targets.at[idx, 'Unit']))
                & ~ref['Suppressed'].isin(SUPPRESSED_CODES)
            ]
            if hit.empty:
                continue
            ratio = _ghgrp_ratio(code, ghgrp, year, other)
            # '*' (under 0.5) counts as 0.25, as estimate_suppressed_mecs_energy
            base = (
                0.25
                if hit['Suppressed'].iloc[0] == '*'
                else float(hit['FlowAmount'].astype(float).iloc[0])
            )
            value = base * ratio
            room = _parent_remainder(national, code, flow, filled)
            if room is not None:
                value = min(value, max(room, 0.0))
            filled[(code, flow)] = value
            break
    if filled:
        log.info(
            f'MECS {year} Table 2.1: filled {len(filled)} suppressed feedstock '
            f'cells from other MECS x GHGRP CO2 ratios'
        )
    for (code, flow), value in filled.items():
        rows = mask & (fba['ActivityConsumedBy'] == code) & (fba['FlowName'] == flow)
        fba.loc[rows, 'FlowAmount'] = value
        fba.loc[rows, 'Suppressed'] = GHGRP_FILL_MARKER
    return fba


def fill_suppressed_mecs_feedstock_with_ghgrp(
    fba: FlowByActivity, **_kwargs: Any
) -> FlowByActivity:
    '''
    clean_fba_before_activity_sets, ahead of
    :func:`estimate_suppressed_mecs_energy`. Fills suppressed national Table
    2.1 (feedstock) cells from the other MECS surveys scaled by GHGRP CO2,
    instead of letting them drop to zero (#1200, #1196).
    '''
    if 'Suppressed' not in fba.columns:
        return fba
    year = int(fba.config['year'])
    other_surveys: dict[int, pd.DataFrame] = {}
    for other in MECS_SURVEY_YEARS:
        if other == year:
            continue
        ref = getFlowByActivity('EIA_MECS_Energy', other)
        other_surveys[other] = pd.DataFrame(
            ref[
                (ref['Description'] == MECS_FEEDSTOCK_TABLE)
                & (ref['Location'] == US_FIPS)
            ]
        )
    ghgrp = {y: ghgrp_co2_by_naics(y) for y in (year, *other_surveys)}
    return fill_suppressed_feedstock(fba, year, other_surveys, ghgrp)


#: Gallons of distillate fuel oil per trillion Btu (0.138 MMBtu per gallon).
GALLONS_DISTILLATE_PER_TBTU = 1e6 / 0.138


def mecs_distillate_to_gallons(fba: FlowByActivity, **_kwargs: Any) -> FlowByActivity:
    '''
    clean_fba. Converts MECS Table 3.2 distillate fuel oil from
    trillion Btu to gallons, so manufacturing sits in the same unit as the EIA
    and Census distillate volumes it is weighed against.
    '''
    tbtu = fba['Unit'].str.lower() == 'trillion btu'
    if not tbtu.all():
        raise ValueError('mecs_distillate_to_gallons expects trillion Btu rows only')
    return fba.assign(
        FlowAmount=fba['FlowAmount'] * GALLONS_DISTILLATE_PER_TBTU, Unit='gal'
    )


def keep_chemical_manufacturing(fba: FlowByActivity, **_kwargs: Any) -> FlowByActivity:
    '''Keep MECS NAICS 325 (chemicals) and descendants. CEDA NEU NG is chemicals-only.'''
    return fba.query("ActivityConsumedBy.str.startswith('325')").reset_index(drop=True)


def apply_mecs_annual_share_path(fba: FlowByActivity, **_kwargs: Any) -> FlowByActivity:
    '''
    clean_fba_w_sec: 2022-anchored annual path for MECS amounts (#1210 / #1318).

    Runs after map_to_sectors on parent-incomplete-child remainders. One weight
    per (ActivityConsumedBy, Flowable). A survey cell is published only when
    snapshotted Suppressed is null; D, Q, *, GHGRP, and any other flag are
    unpublished for the level rule and for RSE (their variance is 0). Filled
    and * amounts still enter the parent/child split so a withheld child is
    subtracted from its parent (e.g. 2018 plastics HGL). Both sides published
    with matching published children use the RSE lambda path from the 2022
    share toward 2018 (inventory years before the anchor). One side missing,
    or the prior published a child that the mapped set does not carry (folded
    into the parent, e.g. 3121), takes that published side as a flat level in
    every inventory year (including 2022+). Neither published keeps the mapped
    row. Shares renormalize to the mapped fuel total.

    Prior codes are recoded to the anchor NAICS vintage and summed when several
    land on one code. Published 2022 Hydrogen is folded into the Petroleum share
    numerator only; the fuel total stays mapped Other. A group_id that fans out
    to several sectors, all published in the prior survey for that fuel, is
    split into one group_id per sector by those amounts (e.g. 336111/336112), so
    Use attribution passes it through.
    '''
    params = fba.config['clean_parameter']
    inventory_year = int(params['inventory_year'])
    anchor_year = int(params['anchor_year'])
    prior_year = int(params['prior_year'])
    span = anchor_year - prior_year
    ((source_name, source_cfg),) = fba.config['clean_source'].items()
    naics_by_year = get_catalog_info(source_name)['activity_schema']['naics']['by_year']
    concordance = (
        load_crosswalk('NAICS_Year_Concordance')[
            [
                f'NAICS_{naics_by_year[prior_year]}_Code',
                f'NAICS_{naics_by_year[anchor_year]}_Code',
            ]
        ]
        .dropna()
        .astype(str)
        .apply(lambda col: col.str.strip())
        .drop_duplicates()
    )
    concordance.columns = pd.Index(['prior', 'anchor'])
    n_targets = concordance.groupby('prior')['anchor'].nunique()
    split_codes = set(n_targets.index[n_targets > 1])
    recode = {
        p: a
        for p, a in zip(concordance['prior'], concordance['anchor'])
        if p != a and p not in split_codes
    }

    amounts: dict[int, dict[tuple[str, str], float]] = {}
    variances: dict[int, dict[tuple[str, str], float]] = {}
    descendants: dict[int, dict[tuple[str, str], frozenset[str]]] = {}
    published: dict[int, set[tuple[str, str]]] = {}
    prior_for_sectors: FlowByActivity | None = None
    for year in (anchor_year, prior_year):
        filled = fill_suppressed_mecs_feedstock_with_ghgrp(
            FlowByActivity.return_FBA(
                full_name=source_name,
                year=year,
                config={
                    **get_catalog_info(source_name),
                    **{
                        k: v
                        for k, v in fba.config.items()
                        if k
                        not in (
                            'clean_fba',
                            'clean_fba_w_sec',
                            'clean_source',
                            'clean_parameter',
                        )
                    },
                    **source_cfg,
                    'year': year,
                },
            )
        )
        flags = filled.select_by_fields()
        supp_by_key = dict(
            zip(
                zip(
                    flags['ActivityConsumedBy'].astype(str),
                    flags['FlowName'].astype(str),
                ),
                flags['Suppressed'],
            )
        )
        raw = (
            estimate_suppressed_mecs_energy(filled)
            .select_by_fields()
            .convert_units_and_flows()
            .reset_index(drop=True)
        )
        raw_keys = list(
            zip(raw['ActivityConsumedBy'].astype(str), raw['Flowable'].astype(str))
        )
        if len(set(raw_keys)) != len(raw_keys):
            raise ValueError(
                f'{source_name} {year}: more than one row per MECS code and fuel'
            )
        # Keep filled/* in parent/child amounts; unpublished only for level/RSE.
        pub_mask = np.array([pd.isna(supp_by_key.get(k)) for k in raw_keys], dtype=bool)
        rse = pd.to_numeric(raw['Spread'], errors='coerce').fillna(0.0) / 100
        raw_var = {
            k: float(v if pub else 0.0)
            for k, v, pub in zip(
                raw_keys, (rse * raw['FlowAmount'].astype(float)) ** 2, pub_mask
            )
        }
        if raw.empty:
            piece_amt, piece_var, desc = {}, {}, {}
        else:
            survey = define_parentincompletechild_descendants(
                raw.reset_index(names='group_id').assign(
                    group_total=lambda x: x.FlowAmount
                )
            )
            keys = list(
                zip(
                    survey['ActivityConsumedBy'].astype(str),
                    survey['Flowable'].astype(str),
                )
            )
            desc = {
                k: frozenset(str(d).split())
                for k, d in zip(keys, survey['descendants'])
            }
            piece_amt = dict(zip(keys, survey['FlowAmount'].astype(float)))
            piece_var = {
                (c, f): raw_var[(c, f)]
                + sum(
                    raw_var.get((d, f), 0.0)
                    for d in ds
                    if not any(d != o and d.startswith(o) for o in ds)
                )
                for (c, f), ds in desc.items()
            }
        pub_keys = {k for k in piece_amt if pd.isna(supp_by_key.get(k))}
        if year == prior_year:
            pub_raw = raw.loc[pub_mask]
            prior_for_sectors = pub_raw.copy() if not pub_raw.empty else None
        if year == anchor_year:
            amounts[year], variances[year], descendants[year] = (
                piece_amt,
                piece_var,
                desc,
            )
            published[year] = pub_keys
            continue
        split = sorted({c for c, _ in piece_amt if c in split_codes})
        if split:
            raise ValueError(
                f'{source_name} {year}: NAICS {naics_by_year[prior_year]} codes '
                f'{", ".join(split)} split across several NAICS '
                f'{naics_by_year[anchor_year]} codes; no one-to-many recode'
            )
        amounts[year], variances[year], descendants[year] = {}, {}, {}
        published[year] = set()
        for (c, f), amt in piece_amt.items():
            key = (recode.get(c, c), f)
            amounts[year][key] = amounts[year].get(key, 0.0) + amt
            variances[year][key] = variances[year].get(key, 0.0) + piece_var[(c, f)]
            descendants[year][key] = descendants[year].get(key, frozenset()) | {
                recode.get(d, d) for d in desc[(c, f)]
            }
            if (c, f) in pub_keys:
                published[year].add(key)
        merged = sorted({recode[c] for c, _ in piece_amt if c in recode})
        if merged:
            log.info(
                f'{source_name} {year}: NAICS {naics_by_year[prior_year]} codes '
                f'recoded to {naics_by_year[anchor_year]} for {", ".join(merged)}'
            )

    split = sorted(set(fba['ActivityConsumedBy'].astype(str)) & split_codes)
    if split:
        raise ValueError(
            f'{fba.full_name}: mapped NAICS codes {", ".join(split)} split across '
            f'several NAICS {naics_by_year[anchor_year]} codes; no one-to-many recode'
        )
    groups = fba.drop_duplicates('group_id')[
        ['group_id', 'ActivityConsumedBy', 'Flowable', 'group_total']
    ].assign(
        ActivityConsumedBy=lambda x: x['ActivityConsumedBy']
        .astype(str)
        .map(lambda c: recode.get(c, c)),
        Flowable=lambda x: x['Flowable'].astype(str),
    )
    if groups.duplicated(['ActivityConsumedBy', 'Flowable']).any():
        raise ValueError(
            f'{fba.full_name}: more than one group_id per MECS code and fuel'
        )

    factor: dict[Any, float] = {}
    level_reasons: dict[str, list[str]] = {}
    lambdas: list[str] = []
    for flow_key, by_flow in groups.groupby('Flowable'):
        flow = str(flow_key)
        mapped_amt = {
            str(c): float(a)
            for c, a in zip(by_flow['ActivityConsumedBy'], by_flow['group_total'])
        }
        fuel_total = float(sum(mapped_amt.values()))
        if fuel_total <= 0:
            continue
        # Petroleum: fold published 2022 Hydrogen into the share numerator only.
        a_amt = dict(mapped_amt)
        if flow == 'Petroleum':
            for code in list(a_amt):
                h2 = amounts[anchor_year].get((code, 'Hydrogen'), 0.0)
                if (code, 'Hydrogen') in published[anchor_year]:
                    variances[anchor_year][(code, 'Petroleum')] = variances[
                        anchor_year
                    ].get((code, 'Petroleum'), 0.0) + variances[anchor_year].get(
                        (code, 'Hydrogen'), 0.0
                    )
                if (code, 'Petroleum') not in published[anchor_year]:
                    a_amt[code] = h2
                else:
                    a_amt[code] = a_amt[code] + h2
        a_sum = float(sum(a_amt.values()))
        p_amt = {c: amounts[prior_year].get((c, flow), 0.0) for c in a_amt}
        # Children the prior published that the mapped activity set does not
        # carry as their own row fold into the parent for the prior level.
        for code in a_amt:
            for d in descendants[prior_year].get((code, flow), frozenset()):
                if d in a_amt or (d, flow) not in amounts[prior_year]:
                    continue
                p_amt[code] = p_amt[code] + amounts[prior_year][(d, flow)]
        p_sum = float(sum(p_amt.values()))
        prior_only = sorted(
            c
            for (c, f), v in amounts[prior_year].items()
            if f == flow and c not in a_amt and v > 0
        )
        if prior_only:
            log.warning(
                f'{fba.full_name} MECS path {flow}: prior-only pieces not added: '
                f'{", ".join(prior_only)}'
            )
        weights: dict[str, float] = {}
        for code, a_share_amt in a_amt.items():
            a_pub = (code, flow) in published[anchor_year] or (
                flow == 'Petroleum'
                and (code, 'Hydrogen') in published[anchor_year]
                and a_share_amt > 0
            )
            p_pub = (code, flow) in published[prior_year] or any(
                (d, flow) in published[prior_year]
                for d in descendants[prior_year].get((code, flow), frozenset())
                if d not in a_amt
            )
            sa = a_share_amt / a_sum if a_sum else 0.0
            desc_a = descendants[anchor_year].get((code, flow), frozenset())
            desc_p = descendants[prior_year].get((code, flow), frozenset())
            # Prior only when a prior-published child is absent from the mapped
            # set and folds into the parent (e.g. 3121); extras that already
            # have their own mapped rows do not force prior (e.g. chemical 325).
            prior_extra = desc_p - desc_a
            prior_finer = bool(prior_extra) and any(d not in a_amt for d in prior_extra)
            if mapped_amt[code] <= 0 and not (flow == 'Petroleum' and a_share_amt > 0):
                reason = 'zero in anchor'
                weights[code] = 0.0
            elif a_pub and p_pub and not prior_finer:
                if inventory_year >= anchor_year:
                    reason = 'anchor level'
                    weights[code] = sa
                else:
                    reason = ''
                    sp = p_amt[code] / p_sum if p_sum else 0.0
                    noise = (
                        variances[anchor_year].get((code, flow), 0.0) / a_sum**2
                        + variances[prior_year].get((code, flow), 0.0) / p_sum**2
                    )
                    lam = max(0.0, 1.0 - noise / (sp - sa) ** 2) if sp != sa else 0.0
                    w = lam * min(anchor_year - inventory_year, span) / span
                    weights[code] = (
                        sa * (sp / sa) ** w if sa > 0 and sp > 0 else sa + w * (sp - sa)
                    )
                    if max(sa, sp) >= 0.01:
                        lambdas.append(f'{flow} {code} {lam:.2f}')
            elif p_pub and (not a_pub or prior_finer):
                reason = (
                    'prior level; children differ'
                    if prior_finer
                    else 'prior level; anchor unpublished'
                )
                weights[code] = p_amt[code] / p_sum if p_sum else 0.0
            elif a_pub:
                reason = 'anchor level; prior unpublished'
                weights[code] = sa
            else:
                reason = 'neither published'
                weights[code] = mapped_amt[code] / fuel_total
            if reason:
                level_reasons.setdefault(reason, []).append(code)
        w_sum = float(sum(weights.values()))
        for gid, code, a in zip(
            by_flow['group_id'], by_flow['ActivityConsumedBy'], by_flow['group_total']
        ):
            factor[gid] = (
                fuel_total * weights[code] / w_sum / float(a) if a and w_sum else 1.0
            )

    log.info(
        f'{fba.full_name} MECS path {inventory_year} (anchor {anchor_year}, '
        f'prior {prior_year}) level: '
        + '; '.join(
            f'{reason}: {", ".join(sorted(codes))}'
            for reason, codes in level_reasons.items()
        )
        or 'none'
    )
    log.info(
        f'{fba.full_name} MECS path {inventory_year} lambda (share >= 1%): '
        + (', '.join(lambdas) or 'none')
    )
    scale = fba['group_id'].map(factor).fillna(1.0)
    out = fba.assign(
        FlowAmount=fba['FlowAmount'] * scale, group_total=fba['group_total'] * scale
    )

    # One-to-many fan-out (e.g. 336110 -> 336111/336112): when 2018 published
    # every sector, give each its own group_id so Use attribution keeps the split.
    if (
        prior_for_sectors is None
        or prior_for_sectors.empty
        or 'SectorConsumedBy' not in out.columns
    ):
        return out
    sector_amt = (
        prior_for_sectors.assign(
            ActivityConsumedBy=lambda x: x['ActivityConsumedBy'].astype(str),
            Flowable=lambda x: x['Flowable'].astype(str),
        )
        .groupby(['ActivityConsumedBy', 'Flowable'], sort=False)['FlowAmount']
        .sum()
    )
    out = out.reset_index(drop=True)
    next_id = int(out['group_id'].max()) + 1
    for gid, sub in out.groupby('group_id'):
        sectors = sub['SectorConsumedBy'].astype(str).tolist()
        if len(sectors) <= 1 or len(set(sectors)) != len(sectors):
            continue
        flow = str(sub['Flowable'].iloc[0])
        prior_parts = {s: float(sector_amt.get((s, flow), 0.0)) for s in sectors}
        if min(prior_parts.values()) <= 0:
            continue
        activity_total = float(sub['group_total'].iloc[0])
        part_sum = float(sum(prior_parts.values()))
        for idx, sector in zip(sub.index, sectors):
            amount = activity_total * prior_parts[sector] / part_sum
            out.at[idx, 'FlowAmount'] = amount
            out.at[idx, 'group_total'] = amount
            out.at[idx, 'ActivityConsumedBy'] = sector
            out.at[idx, 'group_id'] = next_id
            next_id += 1
        log.info(
            f'{fba.full_name} MECS path {inventory_year}: split group_id '
            f'{str(gid)} ({flow}) by {prior_year}: '
            + ', '.join(f'{s}={prior_parts[s] / part_sum:.3f}' for s in sectors)
        )
    return out


def keep_industrial_petroleum_consumers(
    fba: FlowByActivity, **_kwargs: Any
) -> FlowByActivity:
    '''
    clean_fba on petrol activity set. Keep ag/mining/construction/manufacturing
    and 221200 consumers.
    '''
    sector_col = (
        'SectorConsumedBy'
        if 'SectorConsumedBy' in fba.columns
        else 'ActivityConsumedBy'
    )
    sector = fba[sector_col].astype(str)
    keep = sector.str.startswith(('11', '21', '23', '31', '32', '33')) | sector.isin(
        frozenset({'221200'})
    )

    return fba.loc[keep].reset_index(drop=True)


def multiply_bea_by_mecs_petroleum_energy_fraction(
    fba: FlowByActivity, **_kwargs: Any
) -> FlowByActivity:
    '''
    clean_fba_after_attribution. Industrial petrol weights are BEA 324110
    purchases times MECS Other energy fraction Table 3.1 / (Table 2.1 + Table 3.1).
    MECS ratios are mapped via Cornerstone_2025 and joined on SectorConsumedBy;
    sectors with no MECS line use 1.0.
    '''
    clean_source = fba.config['clean_source']
    if isinstance(clean_source, str):
        name, src_config = clean_source, {}
    else:
        ((name, src_config),) = clean_source.items()
    year = int(src_config['year'])
    target_year = fba.config['target_schema_year']
    mecs = FlowByActivity(
        getFlowByActivity(name, year),
        full_name=name,
        config={
            **get_catalog_info(name),
            **src_config,
            'year': year,
            'activity_to_sector_mapping': fba.config['activity_to_sector_mapping'],
            'target_schema_year': target_year,
            'industry_spec': fba.config['industry_spec'],
        },
    )
    mecs = (
        mecs.function_socket('estimate_suppressed')
        .select_by_fields()
        .convert_units_and_flows()
        .reset_index(drop=True)
        .reset_index(names='group_id')
        .assign(group_total=lambda x: x.FlowAmount)
    )
    sector_col = 'SectorConsumedBy'
    t21 = (
        mecs.query("Description == 'Table 2.1'")
        .map_to_sectors(target_year=target_year)
        .groupby(sector_col)['FlowAmount']
        .sum()
    )
    t31 = (
        mecs.query("Description == 'Table 3.1'")
        .map_to_sectors(target_year=target_year)
        .groupby(sector_col)['FlowAmount']
        .sum()
    )
    idx = t21.index.union(t31.index)
    t21 = t21.reindex(idx).fillna(0)
    t31 = t31.reindex(idx).fillna(0)
    den = t21 + t31
    ratios = (t31 / den).where(den != 0).fillna(1.0)

    bea_sector_col = (
        'SectorConsumedBy'
        if 'SectorConsumedBy' in fba.columns
        else 'ActivityConsumedBy'
    )
    matched = fba[bea_sector_col].astype(str).map(ratios).fillna(1.0)
    return fba.assign(FlowAmount=fba['FlowAmount'] * matched)


def household_petroleum_transport_fraction(config: dict[str, Any]) -> float:
    '''
    Share of PCE gasoline-and-other-energy that is transport rather than
    residential heat oil/propane. Same formula CEDA uses on F01000.
    '''
    clean_parameter = config['clean_parameter']
    year = int(clean_parameter.get('year', config['year']))
    pce_cfg = clean_parameter['pce']
    propane_cfg = clean_parameter['propane_price']
    heat_oil_cfg = clean_parameter['heating_oil_price']
    heat_fuels = clean_parameter['residential_heat_fuels']

    pce_tbl = load_from_gcs(
        name=pce_cfg['file'],
        sub_bucket=gcs_extract_input_path(pce_cfg['extract_input']),
        local_dir=local_extract_input_dir(pce_cfg['extract_input']),
        loader=lambda pth: pd.read_csv(
            pth,
            skiprows=int(pce_cfg.get('skiprows', 3)),
            index_col=int(pce_cfg.get('index_col', 1)),
        )
        .dropna()
        .drop(columns=list(pce_cfg.get('drop_columns', ['Line']))),
    )
    pce_tbl.index = pce_tbl.index.str.strip()
    pce_tbl.columns = pce_tbl.columns.astype(int)
    pce_year = int(max(int(c) for c in pce_tbl.columns if int(c) <= year))
    pce = float(pce_tbl.loc[pce_cfg['line'], pce_year])

    prices: dict[str, float] = {}
    for key, price_cfg in (
        ('propane_price', propane_cfg),
        ('heating_oil_price', heat_oil_cfg),
    ):
        skiprows = int(price_cfg.get('skiprows', 4))
        price_tbl = load_from_gcs(
            name=price_cfg['file'],
            sub_bucket=gcs_extract_input_path(price_cfg['extract_input']),
            local_dir=local_extract_input_dir(price_cfg['extract_input']),
            loader=lambda pth: pd.read_csv(pth, skiprows=skiprows),
        )
        month_col = price_cfg.get('month_column', 'Month')
        value_col = price_cfg['value_column']
        price_tbl['Year'] = price_tbl[month_col].str.extract(r'(\d{4})').astype(int)
        by_year = price_tbl.groupby('Year')[value_col].mean()
        years = [y for y in by_year.index if y <= year] or list(by_year.index)
        prices[key] = float(by_year[int(max(years))])

    clean_source = config['clean_source']
    if isinstance(clean_source, str):
        name, src_config = clean_source, {}
    else:
        ((name, src_config),) = clean_source.items()
    annex_year = int(src_config.get('year', year))
    annex = FlowByActivity(
        getFlowByActivity(name, annex_year),
        full_name=name,
        config={**get_catalog_info(name), **src_config, 'year': annex_year},
    ).select_by_fields()

    kerosene_lpg = float(
        annex.loc[
            annex['FlowName'].isin(list(heat_fuels['propane_priced'])), 'FlowAmount'
        ].sum()
    )
    distillate = float(
        annex.loc[
            annex['FlowName'].isin(list(heat_fuels['heating_oil_priced'])),
            'FlowAmount',
        ].sum()
    )
    res_heat = (kerosene_lpg * (prices['propane_price'] / PROPANE_MMBTU_PER_GALLON)) + (
        distillate * (prices['heating_oil_price'] / HEATING_OIL_MMBTU_PER_GALLON)
    )
    return (pce - res_heat) / pce


def scale_household_petroleum_to_transport_share(
    fba: FlowByActivity, **_kwargs: Any
) -> FlowByActivity:
    '''
    clean_fba_after_attribution. Scale F01000 petroleum by the transport share
    of PCE gasoline-and-other-energy (residential heat oil/propane removed).
    '''
    scale = household_petroleum_transport_fraction(fba.config)
    mask = fba['ActivityConsumedBy'] == 'F01000'
    if 'SectorConsumedBy' in fba.columns:
        mask = mask | (fba['SectorConsumedBy'] == 'F01000')
    out = fba.copy()
    out.loc[mask, 'FlowAmount'] = out.loc[mask, 'FlowAmount'] * scale
    return out


def clean_mapped_mecs_energy_fba_to_state(
    fba: FlowByActivity, **_: Any
) -> FlowByActivity:
    """
    clean_fba_w_sec fxn that replicates drop_parentincompletechild_descendants but
    also updates regions to states for state models.
    """
    fba = update_regions_to_states(fba)
    return fba


def update_regions_to_states(
    fba: FlowByActivity, download_sources_ok: bool = True
) -> FlowByActivity:
    """
    Propogates regions to all states to enable for use in state methods.
    Allocates sectors across states based on employment.
    clean_allocation_fba_w_sec fxn
    """
    fba_load = fba.copy()
    log.info('Updating census regions to states')

    region_map = get_region_and_division_codes()
    region_map = region_map[['Region', 'State_FIPS']].drop_duplicates()
    region_map.loc[:, 'State_FIPS'] = region_map['State_FIPS'].apply(
        lambda x: x.ljust(3 + len(x), '0') if len(x) < 5 else x
    )

    # Allocate MECS based on employment FBS
    hlp = load_prepare_clean_source(fba, download_sources_ok=download_sources_ok)

    # For each region, generate ratios across states for a given sector
    hlp = hlp.merge(region_map, how='left', left_on='Location', right_on='State_FIPS')
    hlp['Allocation'] = hlp['FlowAmount'] / hlp.groupby(
        ['Region', 'SectorProducedBy']
    ).FlowAmount.transform('sum')

    fba = FlowByActivity(
        pd.merge(
            fba.rename(columns={'Location': 'Region'}),
            (
                hlp[['Region', 'Location', 'SectorProducedBy', 'Allocation']].rename(
                    columns={'SectorProducedBy': 'SectorConsumedBy'}
                )
            ),
            how='left',
            on=['Region', 'SectorConsumedBy'],
        )
    )
    fba = (
        fba.assign(FlowAmount=lambda x: x['FlowAmount'] * x['Allocation'])
        .assign(LocationSystem='FIPS_2015')
        .drop(columns=['Allocation', 'Region'])
    )

    # Rest group_id and group_total
    fba = (
        fba.drop(columns=['group_id', 'group_total'])
        .reset_index(drop=True)
        .reset_index(names='group_id')
        .assign(group_total=fba.FlowAmount)
    )

    # Check for data loss
    if (abs(1 - (sum(fba['FlowAmount']) / sum(fba_load['FlowAmount'])))) > 0.0005:
        log.warning('Data loss upon census region mapping')

    return fba


def mecs_land_fba_cleanup(fba: FlowByActivity, **_: Any) -> FlowByActivity:
    """
    Modify the EIA MECS Land FBA
    :param fba: df, EIA MECS Land FBA format
    :return: df, EA MECS Land FBA
    """
    # calculate the land area in addition to building footprint
    fba = calculate_total_facility_land_area(fba)

    return fba


if __name__ == "__main__":
    generateFlowByActivity(source='EIA_MECS_Energy', year=2018)
    fba = getFlowByActivity('EIA_MECS_Energy', 2018)
