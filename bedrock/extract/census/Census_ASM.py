# Census_ASM.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Pulls Census Annual Survey of Manufacturers
--year = 'year' e.g. 2015
"""
import json

import pandas as pd

from bedrock.transform.flowbyfunctions import assign_fips_location_system
from bedrock.utils.mapping.location import US_FIPS


def asm_URL_helper(*, build_url, year, **_):
    """
    This helper function uses the "build_url" input from generateflowbyactivity.py,
    which is a base url for data imports that requires parts of the url text
    string to be replaced with info specific to the data year. This function
    does not parse the data, only modifies the urls from which data
    is obtained.
    :param build_url: string, base url
    :param year: year
    :return: list, urls to call, concat, parse, format into
        Flow-By-Activity format
    """
    urls_census = []
    # This section gets the census data by county instead of by state.
    # This is only for years 2010 and 2011. This is done because the State
    # query that gets all counties returns too many results and errors out.

    url = build_url
    # url = url.replace("%3A%2A", ":*")
    urls_census.append(url)

    return urls_census


def asm_call(*, resp, **_):
    """
    Convert response for calling url to pandas dataframe, begin
        parsing df into FBA format
    :param resp: df, response from url call
    :return: pandas dataframe of original source data
    """
    cbp_json = json.loads(resp.text)
    # convert response to dataframe
    df_census = pd.DataFrame(data=cbp_json[1 : len(cbp_json)], columns=cbp_json[0])
    return df_census


def asm_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    # concat dataframes
    df = pd.concat(df_list, sort=False)

    df = df.rename(
        columns={
            'NAICS2017': 'ActivityProducedBy',
            'RCPTOT': 'FlowAmount',
            'YEAR': 'Year',
        }
    )

    df['Location'] = US_FIPS
    df['FlowName'] = 'Sales'
    df['Unit'] = 'Thousand USD'
    df['Class'] = 'Money'

    # add location system based on year of data
    df = assign_fips_location_system(df, year)
    # hard code data
    df['SourceName'] = 'Census_ASM'
    # Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    df['Compartment'] = None
    return df


def asm_pxi_URL_helper(*, build_url, year, **_):
    """
    The ASM value-of-shipments endpoint, one url per year.

    Unlike :func:`asm_URL_helper` the year is a ``time=`` predicate rather than
    part of the path, so it is substituted here.
    """
    return [build_url.replace('__year__', str(year))]


def asm_pxi_parse(*, df_list, year, **_):
    """
    Format ASM industry x product value of shipments into an FBA.

    ``ActivityProducedBy`` is the NAICS industry and ``FlowName`` the 2017 NAPCS
    collection code, matching :mod:`Census_EC_PxI` so one product -> commodity
    concordance serves both.

    ⚠️ **Every NAICS level is kept**, 3- through 6-digit plus the ``31-33``
    all-manufacturing row. The rollups are not redundant here: 57% of six-digit
    cells publish as zero under disclosure suppression, and a five-digit parent
    minus its published children is the only available control for putting that
    value back. Filtering to six digits at extract time would throw the control
    away and silently understate every commodity.
    """
    df = pd.concat(df_list, sort=False)

    df = df.rename(
        columns={
            'NAICS2017': 'ActivityProducedBy',
            'NAPCS2017': 'FlowName',
            'NAPCSDOL': 'FlowAmount',
        }
    )
    df['FlowAmount'] = pd.to_numeric(df['FlowAmount'], errors='coerce')
    df['Year'] = year
    df['Location'] = US_FIPS
    # f.o.b. plant, net of discounts, excluding freight and excise taxes - so
    # basic value rather than producer. Verified against the published 2017
    # table: tobacco and distilleries, where excise is ~75% of value, both build
    # to 0.99 of basic and 0.56 of producer.
    df['Unit'] = 'Thousand USD'
    df['Class'] = 'Money'
    df['FlowType'] = 'TECHNOSPHERE_FLOW'

    df = assign_fips_location_system(df, year)
    df['SourceName'] = 'Census_ASM_PxI'
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    df['Compartment'] = None
    return df
