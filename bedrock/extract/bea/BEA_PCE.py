# BEA_PCE.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
""" """

import json
from typing import Any

import numpy as np
import pandas as pd
from requests import Response

from bedrock.transform.flowbyfunctions import assign_fips_location_system
from bedrock.utils.mapping.location import get_state_FIPS


def bea_pce_url_helper(
    *, build_url: str, config: dict[str, Any], **_: Any
) -> list[str]:
    """
    This helper function uses the "build_url" input from generateflowbyactivity.py,
    which is a base url for data imports that requires parts of the url text
    string to be replaced with info specific to the data year. This function
    does not parse the data, only modifies the urls from which data is
    obtained.
    :param build_url: string, base url
    :param config: dictionary, items in FBA method yaml
    :return: list, urls to call, concat, parse, format into Flow-By-Activity
        format
    """
    urls = []
    for state in get_state_FIPS()['FIPS']:
        url1 = build_url.replace('__stateFIPS__', state)
        for table in config['tables']:
            url = url1.replace('__table__', table)
            urls.append(url)

    return urls


def bea_pce_call(*, resp: Response, **_: Any) -> pd.DataFrame:
    """
    Convert response for calling url to pandas dataframe,
    begin parsing df into FBA format
    :param resp: df, response from url call
    :return: pandas dataframe of original source data
    """
    try:
        json_load = json.loads(resp.text)
        df = pd.DataFrame(data=json_load['BEAAPI']['Results']['Data'])
    except:  # noqa: E722
        df = pd.DataFrame()
    finally:
        return df


def bea_pce_parse(*, df_list: list[pd.DataFrame], year: int, **_: Any) -> pd.DataFrame:
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    # Concat dataframes
    df = pd.concat(df_list, ignore_index=True)

    df = (
        df.rename(
            columns={
                'GeoFips': 'Location',
                'TimePeriod': 'Year',
                'CL_UNIT': 'Unit',
                'Description': 'ActivityProducedBy',
                'Code': 'Description',
            }
        )
        .assign(FlowAmount=lambda x: x['DataValue'].astype(float))
        .assign(FlowName='Personal consumption expenditures')
        .drop(columns=['UNIT_MULT', 'GeoName', 'DataValue'], errors='ignore')
    )

    df['Unit'] = np.where(
        df['Description'].str.startswith('SAPCE2'), 'Dollars / p', df['Unit']
    )

    # add location system based on year of data
    df = assign_fips_location_system(df, year)
    # add hard code data
    df['SourceName'] = 'BEA_PCE'
    df['Class'] = 'Money'
    # Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    df['Compartment'] = None
    df['FlowType'] = "ELEMENTARY_FLOW"

    return df


if __name__ == "__main__":
    from bedrock.extract.flowbyactivity import getFlowByActivity
    from bedrock.extract.generateflowbyactivity import generateFlowByActivity

    generateFlowByActivity(source='BEA_PCE', year=2023)
    fba = pd.DataFrame()
    for y in range(2023, 2024):
        fba = pd.concat(
            [fba, getFlowByActivity('BEA_PCE', y)],
            ignore_index=True,
        )
