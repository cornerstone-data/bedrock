# Census_SAS.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
U.S. Census Service Annual Survey
"""

import os
from typing import Any

import numpy as np
import pandas as pd

from bedrock.transform.flowbyfunctions import assign_fips_location_system
from bedrock.utils.io.gcp import download_extract_input_from_gcs_if_not_exists
from bedrock.utils.io.local_extract_input_data import local_extract_input_dir
from bedrock.utils.mapping.location import US_FIPS


def sas_workbook_filename(config: dict) -> str:
    """The workbook's published filename, e.g. ``sas-22.xlsx``.

    Taken from the configured url so the cached copy always carries the vintage
    it came from - when Census publishes ``sas-23``, changing the url is enough
    to make the cache name follow.
    """
    return os.path.basename(str(config['url']['base_url']))


def sas_local_path(source: str, config: dict) -> str:
    """Where the workbook is cached: ``extract/input_data/Census_SAS/``."""
    return os.path.join(
        local_extract_input_dir(source, year=None), sas_workbook_filename(config)
    )


def _read_sheets(path: str, config: dict) -> list:
    """Read the sheets named in the yaml out of the workbook."""
    return [
        pd.read_excel(path, sheet_name=sheet, header=4).assign(sheet=f'{sheet}: {name}')
        for sheet, name in config['sheets'].items()
    ]


def census_sas_call(*, resp, source='Census_SAS', config=None, **_):
    """Cache the downloaded workbook under extract-input, then read it.

    Only reached with ``extract_data_from_raw_sources: True``. One workbook
    carries every year and every table, so it is cached once rather than per
    year - the same shape as BEA_NIPA's archive.
    """
    local_path = sas_local_path(source, config)
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open(local_path, 'wb') as f:
        f.write(resp.content)
    return _read_sheets(local_path, config)


def census_sas_load_gcs(**kwargs: Any) -> list:
    """Load the workbook from the local cache, or GCS extract-input if missing."""
    source = str(kwargs['source'])
    config = kwargs['config']
    local_path = sas_local_path(source, config)
    filename = sas_workbook_filename(config)
    if not os.path.exists(local_path):
        download_extract_input_from_gcs_if_not_exists(
            # one workbook for all years, so it sits directly under
            # extract/input-data/Census_SAS/ rather than in a year subfolder
            {**kwargs, 'year': None},
            local_dir=os.path.dirname(local_path),
            object_name=filename,
        )
    if not os.path.exists(local_path):
        raise FileNotFoundError(
            f'{filename} is neither cached at {local_path} nor available from '
            f'gs://cornerstone-default/extract/input-data/{source}/. Set '
            f'extract_data_from_raw_sources: True in {source}.yaml to fetch it '
            f'from Census and cache it, then upload it so others need not.'
        )
    return _read_sheets(local_path, config)


def census_sas_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    df = pd.concat(df_list, sort=False)
    value_vars = [
        c for c in df.columns if "Estimate" in c or "Coefficient of Variation" in c
    ]
    id_vars = [c for c in df.columns if c not in value_vars]
    df = (
        df.dropna(subset=['Item'])
        .melt(id_vars=id_vars, value_vars=value_vars)
        .assign(Year=lambda x: x['variable'].str[0:4])
        .assign(var=lambda x: x['variable'].str[5:])
        .drop(columns='variable')
        .pivot_table(
            columns=['var'], index=id_vars + ['Year'], values='value', aggfunc='sum'
        )
        .reset_index()
        .query('`Tax Status` == "All Establishments"')
        .rename(
            columns={
                'NAICS': 'ActivityConsumedBy',
                'Item': 'FlowName',
                'sheet': 'Description',
                'Coefficient of Variation': 'Spread',
                'Estimate': 'FlowAmount',
            }
        )
        .drop(columns=['Employer Status', 'Tax Status', 'NAICS Description'])
    )

    # Revenue is *produced* by the industry; expense is *consumed* by it. The
    # parse chain above lands every sheet's NAICS in ActivityConsumedBy, which
    # is right for the expense tables and wrong for the revenue ones - Table 2's
    # pipeline margin items and Table 8's truck commodity groups both have to
    # sit in ActivityProducedBy to read like every other margin source
    # (Census_AWTS, Census_ARTS, Census_AIES all place the margin-producing
    # sector there).
    is_revenue = df['Description'].str.startswith(('Table 2', 'Table 8'))
    df['ActivityProducedBy'] = np.where(is_revenue, df['ActivityConsumedBy'], None)
    df['ActivityConsumedBy'] = np.where(is_revenue, None, df['ActivityConsumedBy'])

    # set suppressed values to 0 but mark as suppressed
    # otherwise set non-numeric to nan
    df = df.assign(
        Suppressed=np.where(
            df.FlowAmount.str.strip().isin(["S", "Z", "D"]),
            df.FlowAmount.str.strip(),
            np.nan,
        ),
        FlowAmount=np.where(
            df.FlowAmount.str.strip().isin(["S", "Z", "D"]), 0, df.FlowAmount
        ),
    )
    df = df.assign(
        Suppressed=np.where(
            df.FlowAmount.str.endswith('(s)') == True, '(s)', df.Suppressed
        ),
        FlowAmount=np.where(
            df.FlowAmount.str.endswith('(s)') == True,
            df.FlowAmount.str.replace(',', '').str[:-3],
            df.FlowAmount,
        ),
    )

    df['Class'] = 'Money'
    df['SourceName'] = 'Census_SAS'
    # millions of dollars
    df['FlowAmount'] = df['FlowAmount'].astype(float) * 1000000
    df['Spread'] = pd.to_numeric(df['Spread'], errors='coerce')
    df['MeasureofSpread'] = 'Coefficient of Variation'
    df['Unit'] = 'USD'
    df['FlowType'] = "ELEMENTARY_FLOW"
    df['Compartment'] = None
    df['Location'] = US_FIPS
    df = assign_fips_location_system(df, 2024)
    # Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5

    return df


if __name__ == "__main__":
    import bedrock

    bedrock.extract.generateflowbyactivity.main(source='Census_SAS', year='2013-2022')
    fba = bedrock.extract.flowbyactivity.getFlowByActivity('Census_SAS', 2022)
