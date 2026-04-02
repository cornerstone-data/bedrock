# BLS_CES.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Pulls Consumer Expenditure Survey data from Bureau of Labor Statistics.
"""

from __future__ import annotations

import itertools as it
import json
from collections import OrderedDict
from typing import Any, cast

import pandas as pd
from esupy.remote import make_url_request

from bedrock.utils.config.common import load_env_file_key
from bedrock.utils.config.settings import externaldatapath


def read_ces_item_codes() -> pd.DataFrame:
    # https://download.bls.gov/pub/time.series/cx/cx.item
    df = pd.read_csv(externaldatapath / 'ces_items.csv')
    df = df.query('selectable == "T"')
    # TODO: add units directly to this file?
    return df


def bls_ces_call(config: dict[str, Any], year: str | int) -> list[pd.DataFrame]:
    """ """
    headers = {'Content-type': 'application/json'}
    api_key = load_env_file_key('API_Key', config['api_name'])
    series = read_ces_item_codes()['item_code']
    series_dict0 = OrderedDict(config['series'])
    series_dict0['item'] = list(series)
    series_dict = OrderedDict(
        (k, series_dict0[k])
        for k in (
            'prefix',
            'seasonal',
            'item',
            'demographics',
            'characteristics',
            'process',
        )
    )

    combinations = it.product(*(series_dict[Name] for Name in series_dict))
    series_list = [''.join(x) for x in list(combinations)]
    df_list = []
    # Do this in chunks of 50 per API limits
    for i in range(0, len(series_list), 50):
        x = i
        short_series = series_list[x : x + 50]

        data = json.dumps(
            {
                'seriesid': short_series,
                'startyear': 2004,
                'endyear': 2022,
                'registrationkey': api_key,
            }
        )

        response = make_url_request(
            url=config['base_url'], method='POST', data=data, headers=headers
        )

        json_data = json.loads(response.content)
        for series in json_data['Results']['series']:
            series_data = series['data']
            df = pd.DataFrame(
                data=series_data[0 : len(series_data)],
                columns=cast(Any, series_data[0]),
            )
            df['series'] = series['seriesID']
            df_list.append(df)
    return df_list


def bls_ces_parse(
    *,
    df_list: list[pd.DataFrame],
    config: dict[str, Any],
    year: str | int,
    **_: Any,
) -> pd.DataFrame:
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    df_list = bls_ces_call(config, year)
    # Concat dataframes
    df = pd.concat(df_list, sort=False)
    series_df = read_ces_item_codes()
    # assign units using subcategory_code
    series_df['Unit'] = 'USD'  # default value as USD
    series_df.loc[series_df.subcategory_code.isin(['CONSUNIT', 'TITLECU']), 'Unit'] = (
        'Thousand p'
    )
    series_df.loc[
        (series_df.subcategory_code == 'TITLECU')
        & (series_df.item_code.isin(['INCBFTAX', 'INCAFTAX'])),
        'Unit',
    ] = 'Thousand USD'
    series_df.loc[series_df.subcategory_code == 'TITLEPD', 'Unit'] = 'Percent'
    substrs = config['series']['demographics']

    def extract_substring(s: str) -> str:
        start_index = 3  # Starting from the 4th letter (index 3)
        end_index = min(s.find(end) for end in substrs if end in s)
        # ^ Ending before demographics substring
        return s[start_index:end_index]

    df = (
        df.assign(region=lambda x: x['series'].str[-3:].str[:2])  # 16th and 17th
        .assign(code=lambda x: x['series'].apply(extract_substring))
        .merge(
            series_df.filter(['item_code', 'item_text', 'Unit']).rename(
                columns={'item_code': 'code'}
            ),
            how='left',
            on='code',
        )
        .assign(value=lambda x: x['value'].replace('-', 0).astype(float))
        .rename(
            columns={
                'year': 'Year',
                'value': 'FlowAmount',
                'item_text': 'FlowName',
                'series': 'Description',
                'region': 'Location',
            }
        )
        .drop(columns=['period', 'periodName', 'latest', 'code', 'footnotes'])
    )

    # hard code data for flowsa format
    df['LocationSystem'] = 'BLS Regions'
    df['FlowType'] = 'TECHNOSPHERE_FLOW'
    df['Class'] = 'Money'
    df.loc[~df.Unit.str.contains('USD'), 'Class'] = 'Other'
    df['ActivityConsumedBy'] = 'Households'
    df['SourceName'] = 'BLS_CES'
    # Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    df['Compartment'] = None

    return df


if __name__ == '__main__':
    from bedrock.extract import flowbyactivity
    from bedrock.extract.generateflowbyactivity import generateFlowByActivity

    generateFlowByActivity(source='BLS_CES', year='2017-2019')
    fba = flowbyactivity.getFlowByActivity('BLS_CES', year=2017)
