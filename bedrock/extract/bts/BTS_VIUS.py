# BTS_VIUS.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Pulls BTS/Census VIUS national estimates with New Hampshire.
Not state level data, as the state level data
source does not include New Hampshire - can be addressed later
"""
import re
from io import BytesIO
from typing import Any

import numpy as np
import pandas as pd

from bedrock.transform.flowbyfunctions import assign_fips_location_system
from bedrock.utils.mapping.location import US_FIPS


def vius_call(*, resp: Any, config: dict[str, Any], **_: Any) -> list[pd.DataFrame]:
    """
    Convert url based data to pandas dataframe,
    begin parsing df into FBA format
    :param resp: df, response from url call
    :return: pandas dataframe of original source data
    """
    df_list = []
    for sheet in config['sheets']:
        df = pd.read_excel(BytesIO(resp.content), sheet_name=sheet, header=3).assign(
            sheet=sheet
        )
        df_list.append(df)

    return df_list


def vius_parse(
    *, df_list: list[pd.DataFrame], source: str, year: str, **_: Any
) -> pd.DataFrame:
    """
    Combine, parse, and format data
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    frames = []
    for df in df_list:
        df = df.copy()
        df.columns = [
            ' '.join(str(c).replace('\xa0', ' ').replace('\n', ' ').split())
            for c in df.columns
        ]
        # whitespace
        for c in df.columns:
            if df[c].dtype == object:
                df[c] = (
                    df[c]
                    .astype(str)
                    .str.replace('\xa0', ' ', regex=False)
                    .str.replace(r'\s+', ' ', regex=True)
                    .str.strip()
                )
                df[c] = df[c].replace({'nan': np.nan, 'None': np.nan})

        df = df.dropna(subset=['Geographic Area Name']).reset_index(drop=True)
        sheet = df['sheet'].iloc[0]

        if sheet == 'Table M2E':
            df = df.loc[
                df['Meaning of Primary characteristic code'] == 'Kind of Business'
            ].reset_index(drop=True)
            primary_val = df['Meaning of Primary value code']
            df['ActivityConsumedBy'] = primary_val.where(primary_val != 'X')
            df['ActivityProducedBy'] = df['Meaning of Business use code']
            fuel = pd.Series(np.nan, index=df.index)

        elif sheet == 'Table M2C':
            df = df.loc[
                (df['Meaning of Primary characteristic code'] == 'Fuel Type')
                & (df['Meaning of Secondary characteristic code'] == 'X')
                & (df['Meaning of Secondary value code'] == 'X')
            ].reset_index(drop=True)
            primary_val = df['Meaning of Primary value code']
            df['ActivityConsumedBy'] = df['Meaning of Business use code']
            df['ActivityProducedBy'] = df['Meaning of Body type code']
            fuel = primary_val.where(primary_val != 'X')

        else:
            continue

        df['Description'] = sheet
        id_cols = [
            'ActivityConsumedBy',
            'ActivityProducedBy',
            'Description',
            'Geographic Area Name',
        ]
        estimate_cv_pairs = [
            (
                'Number of vehicles (thousands)',
                'Coefficient of variation for number of vehicles (%)',
            ),
            (
                'Vehicle miles1 (millions)',
                'Coefficient of variation for vehicle miles (%)',
            ),
            (
                'Average miles per vehicle (thousands)',
                'Coefficient of variation for average miles per vehicle (%)',
            ),
        ]
        for est_col, cv_col in estimate_cv_pairs:
            tmp = df[id_cols].copy()
            tmp['FlowAmount'] = df[est_col]
            tmp['Spread'] = df[cv_col]
            m = re.match(r'^(.*?)\s*\(([^)]+)\)\s*$', est_col)
            assert m is not None
            flow_name = re.sub(r'\d+$', '', m.group(1)).strip()
            tmp['Unit'] = m.group(2).strip()
            tmp['FlowName'] = np.where(
                fuel.notna(),
                flow_name + ' - ' + fuel.astype(str),
                flow_name,
            )
            frames.append(tmp)

    df = pd.concat(frames, sort=False).reset_index(drop=True)

    # set suppressed values to 0 but mark as suppressed
    suppressed = df.FlowAmount.astype(str).str.strip().isin(['S', 'Z', 'D'])
    df = df.assign(
        Suppressed=np.where(suppressed, df.FlowAmount.astype(str).str.strip(), np.nan),
        FlowAmount=np.where(suppressed, 0, df.FlowAmount),
    )
    df['FlowAmount'] = pd.to_numeric(
        df.FlowAmount.astype(str).str.replace(',', '', regex=False),
        errors='coerce',
    ).fillna(0)
    df['Spread'] = pd.to_numeric(
        df['Spread'].astype(str).str.replace(',', '', regex=False),
        errors='coerce',
    )
    df['MeasureofSpread'] = 'Coefficient of Variation'

    df['Location'] = US_FIPS
    df = assign_fips_location_system(df, year)

    df['Class'] = 'Other'
    df['SourceName'] = source
    df['Year'] = int(year)
    df['FlowType'] = 'TECHNOSPHERE_FLOW'
    df['DataReliability'] = 5  # temp
    df['DataCollection'] = 5  # temp

    return df.drop(columns=['Geographic Area Name'], errors='ignore')
