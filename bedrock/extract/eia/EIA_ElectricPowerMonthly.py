# EIA_ElectricPowerMonthly.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
U.S. EIA Electric Power Monthly (Tables 5.1 and 5.2 only).
https://www.eia.gov/electricity/monthly/

Layout validation (August 2026 edition, edition_month 202608):
- Stable URLs: ``xls/table_5_01.xlsx``, ``xls/table_5_02.xlsx`` under the monthly site.
- Sheet shape: Period column; sector columns Residential / Commercial / Industrial /
  Transportation / All Sectors. Blocks: Annual Totals (year rows), then ``Year YYYY``
  month rows (January..December or through the data month), then Year to Date and
  Rolling 12 Months. Only month rows under ``Year YYYY`` are parsed.
- 5.1 units: thousand MWh (scale 1000 -> MWh). 5.2 units: million dollars (scale 1e6).
- Archive: keep the two XLS files under ``{edition_month}/`` for local/GCS.
- Same-months Jan-Aug 2026 vs 2025 retail price change ~+6.45% (level ~137.7 on
  Annual 2025 index 129.33), matching the #1320 ballpark.
- Month is stored in Description as ``; month=N`` (FBA schema has no Month field).
"""

from __future__ import annotations

import os
import re
from typing import Any

import pandas as pd

from bedrock.transform.flowbyfunctions import assign_fips_location_system
from bedrock.utils.io.gcp import download_extract_input_from_gcs_if_not_exists
from bedrock.utils.io.local_extract_input_data import local_extract_input_dir
from bedrock.utils.mapping.location import US_FIPS

_YEAR_BLOCK_RE = re.compile(r'^Year\s+(\d{4})$', re.IGNORECASE)
_MONTH_NUMBER = {
    'january': 1,
    'february': 2,
    'march': 3,
    'april': 4,
    'may': 5,
    'june': 6,
    'july': 7,
    'august': 8,
    'sept': 9,
    'september': 9,
    'october': 10,
    'november': 11,
    'december': 12,
}
_SECTORS = ('Residential', 'Commercial', 'Industrial', 'Transportation')


def electric_power_monthly_url_helper(
    *, build_url: str, config: dict[str, Any], **_: Any  # noqa: ARG001
) -> list[str]:
    """Expand ``__table__`` to one URL per configured workbook.

    ``config`` is required by the flowsa url_replace signature.
    """
    return [build_url.replace('__table__', table_key) for table_key in config['tables']]


def _table_filename(table_key: str) -> str:
    return table_key if table_key.endswith('.xlsx') else f'{table_key}.xlsx'


def _edition_local_dir(*, source: str, config: dict[str, Any]) -> str:
    return local_extract_input_dir(source, year=str(config['edition_month']))


def _table_key_from_path(path_or_url: str) -> str:
    name = os.path.basename(str(path_or_url).split('?')[0])
    return name.removesuffix('.xlsx')


def _read_table(path: str, table_key: str) -> pd.DataFrame:
    raw = pd.read_excel(path, header=None)
    return raw.assign(_table_key=table_key)


def electric_power_monthly_call(
    *, resp: Any, source: str, config: dict[str, Any], url: str, **_: Any
) -> pd.DataFrame:
    """Cache one table XLS under the edition folder and return its raw frame."""
    table_key = _table_key_from_path(url)
    local_dir = _edition_local_dir(source=source, config=config)
    os.makedirs(local_dir, exist_ok=True)
    local_path = os.path.join(local_dir, _table_filename(table_key))
    with open(local_path, 'wb') as f:
        f.write(resp.content)
    return _read_table(local_path, table_key)


def electric_power_monthly_load_gcs(**kwargs: Any) -> pd.DataFrame:
    """Load one table XLS from local cache, or GCS extract-input if missing."""
    config = kwargs['config']
    source = str(kwargs['source'])
    url = str(kwargs.get('url', ''))
    table_key = _table_key_from_path(url)
    filename = _table_filename(table_key)
    local_dir = _edition_local_dir(source=source, config=config)
    local_path = os.path.join(local_dir, filename)
    if not os.path.exists(local_path):
        gcs_kwargs = {
            **kwargs,
            'year': str(config['edition_month']),
            'url': filename,
        }
        local_path = download_extract_input_from_gcs_if_not_exists(
            gcs_kwargs, local_dir=local_dir, object_name=filename
        )
    return _read_table(local_path, table_key)


def _base_fba_rows(
    *,
    table_cfg: dict[str, Any],
    source: str,
    rows: list[dict[str, Any]],
) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    scale = float(table_cfg.get('flow_amount_scale', 1))
    df['FlowAmount'] = pd.to_numeric(df['FlowAmount'], errors='coerce') * scale
    df = df.dropna(subset=['FlowAmount'])
    df['SourceName'] = source
    df['Unit'] = table_cfg['unit']
    df['Class'] = table_cfg['class']
    df['FlowType'] = table_cfg['flow_type']
    df['Location'] = US_FIPS
    df['Compartment'] = None
    df['DataReliability'] = 5  # temporary
    df['DataCollection'] = 5  # temporary
    # Month lives in Description (FBA schema has no Month column).
    df['Description'] = [
        f"{table_cfg['table_label']}; month={int(m)}" for m in df['Month']
    ]
    df = df.drop(columns=['Month'])
    df['ActivityProducedBy'] = 'Total Electric Industry'
    return df


def _parse_monthly_sector(
    df: pd.DataFrame, table_cfg: dict[str, Any], source: str
) -> pd.DataFrame:
    """Parse Tables 5.1 / 5.2 monthly sector blocks (skip annual / YTD / rolling)."""
    header_row = None
    for i in range(len(df)):
        cell = df.iloc[i, 0]
        if pd.notna(cell) and str(cell).strip().lower() == 'period':
            header_row = i
            break
    if header_row is None:
        return pd.DataFrame()

    sectors: list[tuple[int, str]] = []
    for col in range(1, df.shape[1]):
        label = df.iloc[header_row, col]
        if pd.isna(label):
            continue
        name = str(label).replace('\n', ' ').strip()
        if name in _SECTORS:
            sectors.append((col, name))
    if not sectors:
        return pd.DataFrame()

    flow_name = table_cfg.get('flow_name', table_cfg['table_label'])
    rows: list[dict[str, Any]] = []
    year: int | None = None
    for i in range(header_row + 1, len(df)):
        label = df.iloc[i, 0]
        if pd.isna(label):
            continue
        text = str(label).strip()
        if not text or text.startswith('See '):
            break
        year_match = _YEAR_BLOCK_RE.match(text)
        if year_match:
            year = int(year_match.group(1))
            continue
        if text.lower().startswith('year to date') or text.lower().startswith(
            'rolling'
        ):
            year = None
            continue
        if text.lower() == 'annual totals' or text.isdigit():
            continue
        month = _MONTH_NUMBER.get(text.lower())
        if month is None or year is None:
            continue
        for col, sector in sectors:
            val = df.iloc[i, col]
            if pd.isna(val):
                continue
            rows.append(
                {
                    'Year': year,
                    'Month': month,
                    'FlowName': flow_name,
                    'ActivityConsumedBy': sector,
                    'FlowAmount': val,
                }
            )
    return _base_fba_rows(table_cfg=table_cfg, source=source, rows=rows)


_LAYOUT_PARSERS = {
    'monthly_sector': _parse_monthly_sector,
}


def electric_power_monthly_parse(
    *,
    df_list: list[pd.DataFrame],
    source: str,
    year: str | None,
    config: dict[str, Any],
    **_: Any,
) -> pd.DataFrame:
    """Parse configured Electric Power Monthly tables into Flow-By-Activity format."""
    frames = []
    for raw in df_list:
        table_key = raw['_table_key'].iloc[0]
        table_cfg = config['tables'][table_key]
        parser = _LAYOUT_PARSERS[table_cfg['layout']]
        frames.append(parser(raw.drop(columns=['_table_key']), table_cfg, source))

    df = pd.concat([f for f in frames if len(f)], ignore_index=True)
    if len(df) == 0:
        return df

    df['Year'] = df['Year'].astype(str)
    fips_year = int(df['Year'].astype(int).max()) if year is None else int(year)
    df = assign_fips_location_system(df, fips_year)
    return df


if __name__ == '__main__':
    from pathlib import Path

    from bedrock.extract.flowbyactivity import getFlowByActivity
    from bedrock.extract.generateflowbyactivity import generateFlowByActivity
    from bedrock.utils.config.settings import FBA_DIR

    generateFlowByActivity(source='EIA_ElectricPowerMonthly', year='2025-2026')
    combined = pd.concat(
        [getFlowByActivity('EIA_ElectricPowerMonthly', y) for y in (2025, 2026)],
        ignore_index=True,
    )
    out_path = Path(FBA_DIR) / 'EIA_ElectricPowerMonthly_2025-2026_review.csv'
    combined.to_csv(out_path, index=False)
    print(f'Wrote {len(combined)} rows to {out_path}')
