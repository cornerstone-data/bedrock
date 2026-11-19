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
from bedrock.transform.flowbyclean import load_prepare_clean_source
from bedrock.transform.flowbyfunctions import assign_fips_location_system
from bedrock.utils.config.common import WITHDRAWN_KEYWORD, get_catalog_info
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
        unit = str(targets.at[idx, 'Unit'])
        for other in order:
            ref = other_surveys[other]
            hit = ref[
                (ref['ActivityConsumedBy'] == code)
                & (ref['FlowName'] == flow)
                & (ref['Unit'].astype(str) == unit)
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
            log.info(
                f'MECS {year} Table 2.1 {code} {flow}: suppressed '
                f'{targets.at[idx, "Suppressed"]} filled with {value:.1f} {unit} '
                f'from MECS {other} x GHGRP CO2 ratio {ratio:.3f}'
                + (f' (parent room {room:.1f})' if room is not None else '')
            )
            break
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
    clean_fba: 2022-anchored flat+λ annual path for MECS sector amounts (#1210).

    Primary *fba* is the anchor survey (already suppress-filled). Prior survey
    is loaded only from YAML ``clean_source`` (must match ``prior_year``).
    Years, span, ``after_anchor``, and ``lambda_zero_flownames`` come from
    ``clean_parameter`` — nothing hard-coded. ``path`` must be ``flat``.
    '''
    params = fba.config['clean_parameter']
    inventory_year = int(params['inventory_year'])
    anchor_year = int(params['anchor_year'])
    prior_year = int(params['prior_year'])
    span = int(params.get('path_span', anchor_year - prior_year))
    if span <= 0:
        raise ValueError(
            f'apply_mecs_annual_share_path path_span must be > 0, got {span}'
        )
    after_anchor = str(params.get('after_anchor', 'hold'))
    if after_anchor not in ('hold', 'follow'):
        raise ValueError(
            f'apply_mecs_annual_share_path after_anchor must be hold or follow, '
            f'got {after_anchor!r}'
        )
    path_mode = str(params.get('path', 'flat'))
    if path_mode != 'flat':
        raise ValueError(
            f'apply_mecs_annual_share_path only supports path=flat, got {path_mode!r}'
        )
    # flat path: after_anchor hold|follow both keep share_a for t >= anchor.
    lambda_zero_flownames = {
        str(x) for x in params.get('lambda_zero_flownames', ('Other', 'Petroleum'))
    }

    clean_source = fba.config['clean_source']
    if isinstance(clean_source, str):
        raise ValueError(
            'apply_mecs_annual_share_path requires clean_source as a mapping '
            'with year (prior survey FBA declared in YAML)'
        )
    ((prior_name, prior_cfg),) = clean_source.items()
    prior_cfg_year = int(prior_cfg['year'])
    if prior_cfg_year != prior_year:
        raise ValueError(
            f'clean_source year {prior_cfg_year} != clean_parameter.prior_year '
            f'{prior_year}'
        )

    flow_col = 'Flowable' if 'Flowable' in fba.columns else 'FlowName'
    code_col = 'ActivityConsumedBy'

    prior = FlowByActivity(
        getFlowByActivity(prior_name, prior_year),
        full_name=prior_name,
        config={
            **get_catalog_info(prior_name),
            **prior_cfg,
            'year': prior_year,
            'activity_to_sector_mapping': fba.config.get('activity_to_sector_mapping'),
            'target_schema_year': fba.config.get('target_schema_year'),
            'industry_spec': fba.config.get('industry_spec'),
        },
    )
    # Fill then capture suppress flags before estimate drops Suppressed.
    prior = fill_suppressed_mecs_feedstock_with_ghgrp(prior)
    prior_flags = prior[[code_col, 'FlowName', 'Suppressed']].copy()
    prior = estimate_suppressed_mecs_energy(prior)
    prior = prior.select_by_fields(
        selection_fields=prior_cfg.get('selection_fields'),
        exclusion_fields=prior_cfg.get('exclusion_fields'),
    )
    # Match primary column name after FlowName→Flowable / selection renames.
    if flow_col == 'Flowable' and 'Flowable' not in prior.columns:
        prior = prior.rename(columns={'FlowName': 'Flowable'})
    prior_flow = 'Flowable' if 'Flowable' in prior.columns else 'FlowName'

    # Map prior suppress flags through the same FlowName renames as selection.
    sel_flows = (prior_cfg.get('selection_fields') or {}).get('FlowName')
    flag_flow = prior_flags['FlowName'].astype(str)
    if isinstance(sel_flows, dict):
        flag_flow = flag_flow.replace(sel_flows)
    prior_flags = prior_flags.assign(_flow=flag_flow)
    prior_supp = {
        (str(r[code_col]), str(r['_flow'])): r['Suppressed']
        for _, r in prior_flags.iterrows()
    }

    prior_amt = (
        prior.groupby([prior[code_col].astype(str), prior[prior_flow].astype(str)])[
            'FlowAmount'
        ]
        .sum()
        .astype(float)
    )

    out = fba.copy()
    new_amounts = out['FlowAmount'].astype(float).copy()

    for flow_key, idx in out.groupby(
        out[flow_col].astype(str), sort=False
    ).groups.items():
        flow = str(flow_key)
        anchor_rows = out.loc[idx]
        a_amt = (
            anchor_rows.groupby(anchor_rows[code_col].astype(str))['FlowAmount']
            .sum()
            .astype(float)
        )
        # Only NAICS present on the current activity-set FBA (e.g. chem-only NG).
        codes = [str(c) for c in sorted(a_amt.index)]
        a_sum = float(a_amt.sum())
        p_vals = {
            c: (
                float(prior_amt.loc[(c, flow)]) if (c, flow) in prior_amt.index else 0.0
            )
            for c in codes
        }
        p_sum = float(sum(p_vals.values()))
        share_a = {c: (float(a_amt.loc[c]) / a_sum if a_sum else 0.0) for c in codes}
        share_p = {c: (p_vals[c] / p_sum if p_sum else 0.0) for c in codes}

        weights: dict[str, float] = {}
        for code in codes:
            sa = share_a[code]
            sp = share_p[code]
            supp = prior_supp.get((code, flow))
            if flow in lambda_zero_flownames:
                lam = 0.0
                reason = f'FlowName {flow} in lambda_zero_flownames'
            elif supp in SUPPRESSED_CODES or supp == GHGRP_FILL_MARKER or supp == '*':
                lam = 0.0
                reason = f'prior Suppressed={supp!r}'
            elif sa <= 0 or sp <= 0:
                lam = 0.0
                reason = 'missing/zero anchor or prior amount'
            else:
                lam = 1.0
                reason = 'both numeric'
            if lam == 0.0 and sa > 0:
                log.info(
                    f'MECS annual path λ=0 for {code} {flow} '
                    f'(inventory {inventory_year}): {reason}'
                )
            # flat path: path(t)=1, path(prior)=1
            if sa <= 0:
                weights[code] = 0.0
                continue
            if inventory_year >= anchor_year:
                weights[code] = sa
            else:
                adjust = (sp / sa) if sa else 1.0
                if adjust <= 0:
                    adjust = 1.0
                w = lam * min(anchor_year - inventory_year, span) / span
                weights[code] = sa * (adjust**w)

        w_sum = float(sum(weights.values()))
        if w_sum <= 0 or a_sum <= 0:
            continue
        # Preserve this flow's total on the anchor FBA (attribution weights).
        scale = {c: a_sum * (weights[c] / w_sum) for c in codes}
        for i in idx:
            code = str(out.at[i, code_col])
            # Split group total across duplicate rows for the same NAICS×flow.
            n_dup = int((anchor_rows[code_col].astype(str) == code).sum())
            new_amounts.at[i] = scale.get(code, 0.0) / n_dup if n_dup else 0.0

    return out.assign(FlowAmount=new_amounts)


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

    When ``clean_parameter`` has inventory/anchor/prior years, Other amounts are
    put on the #1210 flat+λ annual path before the fraction (YAML declares prior
    via ``prior_year``; ``clean_source`` year is the anchor survey).
    '''
    clean_source = fba.config['clean_source']
    if isinstance(clean_source, str):
        name, src_config = clean_source, {}
    else:
        ((name, src_config),) = clean_source.items()
    year = int(src_config['year'])
    target_year = fba.config['target_schema_year']
    params = fba.config.get('clean_parameter') or {}
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
    if {'inventory_year', 'anchor_year', 'prior_year'} <= set(params):
        prior_year = int(params['prior_year'])
        path_cfg = {
            **mecs.config,
            'clean_source': {
                name: {
                    **{k: v for k, v in src_config.items() if k != 'year'},
                    'year': prior_year,
                }
            },
            'clean_parameter': {
                **params,
                'path': params.get('path', 'flat'),
                'after_anchor': params.get('after_anchor', 'hold'),
                'lambda_zero_flownames': params.get(
                    'lambda_zero_flownames', ['Other', 'Petroleum']
                ),
            },
        }
        mecs = FlowByActivity(pd.DataFrame(mecs), full_name=name, config=path_cfg)
        mecs = fill_suppressed_mecs_feedstock_with_ghgrp(mecs)
        mecs = estimate_suppressed_mecs_energy(mecs)
        mecs = mecs.select_by_fields()
        mecs = apply_mecs_annual_share_path(mecs)
        mecs = (
            mecs.convert_units_and_flows()
            .reset_index(drop=True)
            .reset_index(names='group_id')
            .assign(group_total=lambda x: x.FlowAmount)
        )
    else:
        mecs = (
            mecs.function_socket('estimate_suppressed')
            .select_by_fields()
            .convert_units_and_flows()
            .reset_index(drop=True)
            .reset_index(names='group_id')
            .assign(group_total=lambda x: x.FlowAmount)
        )
    sector_col = 'SectorConsumedBy'
    desc_col = 'Description'
    t21 = (
        mecs.query(f"{desc_col} == 'Table 2.1'")
        .map_to_sectors(target_year=target_year)
        .groupby(sector_col)['FlowAmount']
        .sum()
    )
    t31 = (
        mecs.query(f"{desc_col} == 'Table 3.1'")
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
