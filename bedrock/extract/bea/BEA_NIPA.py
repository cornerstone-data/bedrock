# BEA_NIPA.py (bedrock)
# !/usr/bin/env python3
# coding=utf-8

"""
Supporting functions for National Income and Product Accounts from BEA.
"""

import os
import re
import zipfile
from typing import Any

import pandas as pd
from bedrock.utils.io.gcp import download_extract_input_from_gcs_if_not_exists
from bedrock.utils.io.local_extract_input_data import local_extract_input_dir
from bedrock.utils.mapping.location import US_FIPS
from bedrock.transform.flowbyfunctions import assign_fips_location_system

#: BEA publishes the whole national accounts as one archive, all years and all
#: tables, so there is one file to cache rather than one per year.
FLAT_FILES_ZIP = 'FlatFiles.ZIP'


def flat_files_local_path(source: str = 'BEA_NIPA') -> str:
    """Where the flat-file archive is cached: ``extract/input_data/BEA_NIPA/``."""
    return os.path.join(local_extract_input_dir(source, year=None), FLAT_FILES_ZIP)


def _read_tables_from_zip(zip_path: str, config: dict[str, Any]) -> list[pd.DataFrame]:
    """Read the ``files`` named in the yaml out of the archive."""
    with zipfile.ZipFile(zip_path) as zip_file:
        return [
            pd.read_csv(zip_file.open(filename)).rename(
                columns={
                    '%SeriesCode': 'SeriesCode',
                    'TableId:LineNo': 'Table_and_Line',
                }
            )
            for filename in zip_file.namelist()
            if filename in config['files']
        ]


def bea_nipa_call(
    *, resp: Any, source: str, config: dict[str, Any], **_: Any
) -> list[pd.DataFrame]:
    """Cache the downloaded archive under extract-input, then read it.

    Only reached with ``extract_data_from_raw_sources: True``; writing the
    archive down rather than parsing it out of memory is what lets every later
    run go through :func:`bea_nipa_load_gcs` instead of back to BEA.
    """
    local_path = flat_files_local_path(source)
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open(local_path, 'wb') as f:
        f.write(resp.content)
    return _read_tables_from_zip(local_path, config)


def bea_nipa_load_gcs(**kwargs: Any) -> list[pd.DataFrame]:
    """Load the archive from the local cache, or GCS extract-input if missing."""
    source = str(kwargs['source'])
    local_path = flat_files_local_path(source)
    if not os.path.exists(local_path):
        download_extract_input_from_gcs_if_not_exists(
            # the archive is not year-specific, so it sits directly under
            # extract/input-data/BEA_NIPA/ rather than in a year subfolder
            {**kwargs, 'year': None},
            local_dir=os.path.dirname(local_path),
            object_name=FLAT_FILES_ZIP,
        )
    if not os.path.exists(local_path):
        raise FileNotFoundError(
            f'{FLAT_FILES_ZIP} is neither cached at {local_path} nor available '
            f'from gs://cornerstone-default/extract/input-data/{source}/. Set '
            f'extract_data_from_raw_sources: True in {source}.yaml to fetch it '
            f'from BEA and cache it, then upload it so others need not.'
        )
    return _read_tables_from_zip(local_path, kwargs['config'])


def bea_nipa_parse(
    *,
    df_list: list[pd.DataFrame],
    source: str,
    year: int,
    config: dict[str, Any],
    **_: Any,
) -> pd.DataFrame:
    """
    Parse BEA data for GrossOutput, Make, and Use tables
    :param source:
    :param year:
    :return:
    """
    for df in df_list:
        if 'TableTitle' in df:
            tables = df
        elif 'Value' in df:
            data = df
            data['Value'] = data['Value'].str.replace(',', '').astype(float) * 1000000
        elif 'SeriesLabel' in df:
            series = df

    def extract_series_by_table(table: str) -> pd.DataFrame:
        series1 = series.query('Table_and_Line.str.contains(@table)').reset_index(
            drop=True
        )
        # Split the strings by '|'
        series1['Table_and_Line'] = series1['Table_and_Line'].str.split('|')
        # Explode the lists into separate rows
        df = series1.explode('Table_and_Line')
        df = df.query('Table_and_Line.str.contains(@table)').reset_index(drop=True)
        df['TableId'] = df['Table_and_Line'].str.split(':', expand=True)[0]
        df['Line'] = df['Table_and_Line'].str.split(':', expand=True)[1].astype('int')
        df = df.drop(columns=['Table_and_Line'])
        df = df.merge(tables, on='TableId', how='left', validate='m:1')
        return df.reset_index(drop=True)

    def generate_data_table(table: str) -> pd.DataFrame:
        series = extract_series_by_table(table)
        series1_wide = (
            series.merge(data.query('Period > 2011'), how='left', on='SeriesCode')
            # .pivot_table(index=[c for c in series.columns if c not in ['Period', 'Value']],
            #          columns='Period', values='Value', aggfunc='mean')
            #         # use 'mean' in case of errors in duplicates
            # .reset_index()
            .sort_values(by='Line')
        )
        return series1_wide

    df = pd.DataFrame()
    df = pd.concat(
        [generate_data_table(c) for c in config['tables']], ignore_index=True
    )
    df = df.drop(
        columns=['SeriesCodeParents', 'DefaultScale', 'CalculationType', 'MetricName']
    )

    df = (
        df.assign(
            Description=lambda x: x['TableId']
            + ': '
            + x['SeriesCode']
            + ' - '
            + x['Line'].astype(str)
        )
        .assign(Year=lambda x: x['Period'].astype('Int64').astype(str))
        .rename(columns={'SeriesLabel': 'ActivityProducedBy', 'Value': 'FlowAmount'})
        .assign(
            # BEA's SeriesLabel occasionally carries its own trailing footnote-reference
            # number, e.g. "Accommodations (104)". That number is unrelated to Table/Line
            # (which are already tracked separately via Description) and has no counterpart
            # in other BEA tables (e.g. PCE Bridge category names), so it is dropped here.
            ActivityProducedBy=lambda x: x['ActivityProducedBy'].str.replace(
                r'(?:\s*\(\d+\))+$', '', regex=True
            )
            # BEA sometimes pads slash-separated terms with spaces (e.g. "Cosmetic /
            # perfumes / bath / nail preparations"), while other BEA tables (e.g. PCE
            # Bridge category names) write the same term slash-tight. Normalize so
            # names agree across tables.
            .str.replace(r'\s*/\s*', '/', regex=True)
            # NIPA abbreviates "not elsewhere classified" as "n.e.c." (e.g. "Electrical
            # equipment, n.e.c."), while other BEA tables (e.g. PEQ Bridge category
            # names) spell it out ("Electrical equipment, not elsewhere classified").
            # Expand so names agree across tables.
            .str.replace(r'\bn\.e\.c\.', 'not elsewhere classified', regex=True)
        )
    )

    # columns relevant to all BEA data
    df['SourceName'] = source
    df['FlowName'] = 'USD'
    df['ActivityConsumedBy'] = ''  # set something here?
    df['Compartment'] = ''  # set something here?
    df['Class'] = 'Money'
    df['FlowType'] = 'TECHNOSPHERE_FLOW'
    df['Location'] = US_FIPS
    df = assign_fips_location_system(df, 2024)
    df['Unit'] = 'USD'
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp

    return df


def extract_table_info(fba: pd.DataFrame, **_: Any) -> pd.DataFrame:
    """ """
    # extract table info for easier parsing
    fba[['Table', 'Code_Line']] = fba['Description'].str.split(': ', expand=True)
    fba[['Code', 'Line']] = fba['Code_Line'].str.split(' - ', expand=True)
    fba = (
        fba.assign(Line=lambda x: x['Line'].astype(int)).drop(columns=['Code_Line'])
        # .sort_values(by=['Table', 'Line'])
    )
    return fba


def drop_unassigned(fba: pd.DataFrame, **_: Any) -> pd.DataFrame:
    """clean_fba_w_sec fxn"""
    # Because ACB is assigned in the method yaml, need to drop those that don't
    # have an original APB assignment in the mapping file
    fba = fba[~fba['SectorProducedBy'].isna()]

    return fba


if __name__ == '__main__':
    from bedrock.extract.generateflowbyactivity import generateFlowByActivity
    from bedrock.extract.flowbyactivity import getFlowByActivity

    generateFlowByActivity(source='BEA_NIPA', year='2022-2024')
    fba = pd.DataFrame()
    for y in range(2022, 2025):
        fba = pd.concat([fba, getFlowByActivity('BEA_NIPA', y)], ignore_index=True)

    # extract table info for easier parsing
    fba[['Table', 'Code_Line']] = fba['Description'].str.split(': ', expand=True)
    fba[['Code', 'Line']] = fba['Code_Line'].str.split(' - ', expand=True)
    fba = (
        fba.assign(Line=lambda x: x['Line'].astype(int))
        .drop(columns=['Code_Line'])
        .sort_values(by=['Table', 'Line'])
    )
