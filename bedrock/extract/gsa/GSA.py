"""GSA Federal Fleet Report (FFR) open data — Table 5-1 worldwide fuel."""

from __future__ import annotations

import os
from io import BytesIO
from typing import Any
from urllib.parse import quote, unquote, urlparse

import numpy as np
import pandas as pd
import xlrd

from bedrock.transform.flowbyfunctions import assign_fips_location_system
from bedrock.utils.io.gcp import download_extract_input_from_gcs_if_not_exists
from bedrock.utils.io.local_extract_input_data import local_extract_input_dir
from bedrock.utils.logging.flowsa_log import log
from bedrock.utils.mapping.location import US_FIPS

# Filenames on the GSA library page are irregular by year.
_FFR_FILES: dict[int, str] = {
    2017: 'FY2017Federal_Fleet_Data1.xlsx',
    2018: 'FY_2018_Federal_Fleet_Data_Set_8-14-2019.xlsx',
    2019: 'FY2019FederalFleetReportFinal.xlsx',
    2020: 'FY2020FederalFleetReport.xlsx',
    2021: 'Travel_Transportation_and_Asset_Mgmt/FY2021FFROpenDataSet.xls',
    2022: 'FY2022FFROpenDataSet.xlsx',
    2023: 'FY2023FFROpenDataSet.xlsx',
    2024: 'Fiscal Year 2024 FFR Open Data Set.xls',
    2025: 'Fiscal Year 2025 FFR Open Data Set.xlsx',
}


def gsa_ffr_url_helper(
    *, year: str | int, config: dict[str, Any], **_kwargs: Any
) -> list[str]:
    y = int(year)
    if y not in _FFR_FILES:
        raise ValueError(f'No GSA FFR open-data URL mapped for FY{y}')
    # Quote path segments but keep slashes for nested 2021 path.
    rel = '/'.join(quote(p, safe='') for p in _FFR_FILES[y].split('/'))
    return [config['url']['base_url'] + rel]


def _gsa_ffr_read_sheet(raw: bytes, sheet: str) -> pd.DataFrame:
    # xlsx vs legacy xls (FY2021/2024).
    if raw[:2] == b'PK':
        return pd.read_excel(BytesIO(raw), sheet_name=sheet, header=None)
    book = xlrd.open_workbook(file_contents=raw)
    sh = book.sheet_by_name(sheet)
    return pd.DataFrame(
        [[sh.cell_value(r, c) for c in range(sh.ncols)] for r in range(sh.nrows)]
    )


def gsa_ffr_call(
    *,
    resp: Any,
    config: dict[str, Any],
    url: str | None = None,
    source: str,
    year: str,
    **_kwargs: Any,
) -> list[pd.DataFrame]:
    raw = resp.content
    # Unquote so spaces in FY2024/2025 filenames match human-readable uploads.
    table = unquote(os.path.basename(urlparse(url or '').path))
    with open(os.path.join(local_extract_input_dir(source, year), table), 'wb') as f:
        f.write(raw)
    return [
        _gsa_ffr_read_sheet(raw, sheet).assign(sheet=sheet)
        for sheet in config['sheets']
    ]


def gsa_ffr_load_gcs(**kwargs: Any) -> list[pd.DataFrame]:
    """Load GSA FFR sheets from GCS extract-input (or local cache)"""
    object_name = unquote(os.path.basename(urlparse(str(kwargs.get('url', ''))).path))
    path = download_extract_input_from_gcs_if_not_exists(
        kwargs, object_name=object_name
    )
    with open(path, 'rb') as f:
        raw = f.read()
    return [
        _gsa_ffr_read_sheet(raw, sheet).assign(sheet=sheet)
        for sheet in kwargs['config']['sheets']
    ]


def gsa_ffr_parse(
    *, df_list: list[pd.DataFrame], source: str, year: str, **_kwargs: Any
) -> pd.DataFrame:
    for df in df_list:
        if 'sheet' in df.columns and not df.empty and df['sheet'].iloc[0] != '5-1':
            log.warning(
                f"GSA FFR sheet {df['sheet'].iloc[0]!r} differs from '5-1'; "
                f'check layout — parse only handles Table 5-1 '
            )
    raw = df_list[0].drop(columns=['sheet'], errors='ignore')
    header: list[str] | None = None
    records: list[dict[str, Any]] = []
    for _, series in raw.iterrows():
        vals = list(series.values)
        while vals and (
            vals[0] is None
            or (isinstance(vals[0], float) and np.isnan(vals[0]))
            or (isinstance(vals[0], str) and vals[0].strip() == '')
        ):
            vals = vals[1:]
        if not vals or vals[0] is None or vals[0] == '':
            continue
        label = str(vals[0]).strip()
        if label == 'Department or Agency':
            header = [str(v).strip() if v is not None else '' for v in vals]
            continue
        if header is None:
            continue
        # Skip blank / footnote-only trailing rows.
        if label.startswith('*') or label.lower().startswith('note'):
            continue
        for i, fuel in enumerate(header[1:], start=1):
            if not fuel or fuel == 'Total':
                continue
            amt = vals[i] if i < len(vals) else None
            if amt is None or amt == '':
                continue
            try:
                amount = float(amt)
            except (TypeError, ValueError):
                continue
            records.append(
                {
                    'ActivityConsumedBy': label,
                    'FlowName': fuel,
                    'FlowAmount': amount,
                }
            )

    df = pd.DataFrame.from_records(records)
    if df.empty:
        raise ValueError(f'GSA_FFR Table 5-1 parse produced no rows for FY{year}')

    df['ActivityProducedBy'] = np.nan
    df['Description'] = 'Table 5-1: Worldwide Fuel Consumption'
    # FFR reports motor fuels in gasoline-gallon equivalents.
    df['Unit'] = 'GGE'
    df['Class'] = 'Energy'
    df['SourceName'] = source
    df['Year'] = int(year)
    df['FlowType'] = 'TECHNOSPHERE_FLOW'
    df['Location'] = US_FIPS
    df = assign_fips_location_system(df, year)
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    return df
