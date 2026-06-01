# EIA_ElectricPowerAnnual.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
U.S. EIA Electric Power Annual (edition ZIP of table workbooks).
https://www.eia.gov/electricity/annual/
"""

import os
import re
import zipfile
from typing import Any

import pandas as pd

from bedrock.transform.flowbyfunctions import assign_fips_location_system
from bedrock.utils.io.gcp import download_extract_input_from_gcs_if_not_exists
from bedrock.utils.io.local_extract_input_data import local_extract_input_dir
from bedrock.utils.mapping.location import US_FIPS, get_state_FIPS

_YEAR_RE = re.compile(r'^\d{4}$')
_YEAR_HEADER_RE = re.compile(r'^Year\s+(\d{4})$', re.IGNORECASE)

_CENSUS_DIVISIONS = frozenset(
    {
        'New England',
        'Middle Atlantic',
        'East North Central',
        'West North Central',
        'South Atlantic',
        'East South Central',
        'West South Central',
        'Mountain',
        'Pacific Contiguous',
        'Pacific Noncontiguous',
    }
)


def electric_power_annual_url_helper(
    *, build_url: str, config: dict[str, Any], **_: Any
) -> list[str]:
    """Resolve download URL from ``edition_year`` (publication ZIP), not data ``years``."""
    return [build_url.replace('__edition_year__', str(config['edition_year']))]


def _zip_basename(config: dict[str, Any]) -> str:
    """Local ZIP file name for the configured publication edition."""
    return f"epa-{config['edition_year']}.zip"


def _table_zip_name(table_key: str) -> str:
    return table_key if table_key.endswith('.xlsx') else f'{table_key}.xlsx'


def _read_tables_from_zip(zip_path: str, config: dict[str, Any]) -> list[pd.DataFrame]:
    """Read each configured workbook from the edition ZIP into a raw dataframe."""
    with zipfile.ZipFile(zip_path) as zf:
        names = set(zf.namelist())
        frames = []
        for table_key, table_cfg in config['tables'].items():
            zip_name = _table_zip_name(table_key)
            if zip_name not in names:
                continue
            raw = pd.read_excel(zf.open(zip_name), header=None)
            frames.append(raw.assign(_table_key=table_key))
        return frames


def _edition_local_zip_path(*, source: str, config: dict[str, Any]) -> str:
    """Path to the cached edition ZIP under ``extract/input_data``."""
    return os.path.join(
        local_extract_input_dir(source, year=None), _zip_basename(config)
    )


def electric_power_annual_call(
    *, resp: Any, source: str, config: dict[str, Any], **_: Any
) -> list[pd.DataFrame]:
    """Download edition ZIP, cache under extract-input, and read table workbooks."""
    local_path = _edition_local_zip_path(source=source, config=config)
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open(local_path, 'wb') as f:
        f.write(resp.content)
    return _read_tables_from_zip(local_path, config)


def electric_power_annual_load_gcs(**kwargs: Any) -> list[pd.DataFrame]:
    """Load edition ZIP from local cache, or GCS extract-input if missing."""
    config = kwargs['config']
    source = str(kwargs['source'])
    local_path = _edition_local_zip_path(source=source, config=config)
    if os.path.exists(local_path):
        return _read_tables_from_zip(local_path, config)
    zip_name = os.path.basename(local_path)
    local_dir = os.path.dirname(local_path)
    gcs_kwargs = {**kwargs, 'year': None, 'url': zip_name}
    path = download_extract_input_from_gcs_if_not_exists(
        gcs_kwargs, local_dir=local_dir, object_name=zip_name
    )
    return _read_tables_from_zip(path, config)


def _is_year_label(value: Any) -> bool:
    """Return True when a cell value is a four-digit calendar year."""
    if pd.isna(value):
        return False
    text = str(value).strip().split('.')[0]
    return bool(_YEAR_RE.match(text))


def _first_matching_row(df: pd.DataFrame, marker: str, *, col: int = 0) -> int | None:
    """Return the first row index where column ``col`` equals ``marker``, or None."""
    matches = df.index[df[col].astype(str).str.strip() == marker]
    if len(matches) == 0:
        return None
    return int(matches[0])


def _base_fba_rows(
    *,
    table_cfg: dict[str, Any],
    source: str,
    rows: list[dict[str, Any]],
) -> pd.DataFrame:
    """Apply shared Flow-By-Activity fields and unit scaling to parsed row dicts."""
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
    if 'Location' not in df.columns:
        df['Location'] = US_FIPS
    df['Compartment'] = None
    df['DataReliability'] = 5  # temporary
    df['DataCollection'] = 5  # temporary
    df['Description'] = table_cfg['table_label']
    return df


def _sector_by_provider_activities(
    table_cfg: dict[str, Any], *, sector: str, provider: str | None
) -> tuple[str | None, str | None]:
    """Map sector and provider to ActivityProducedBy and ActivityConsumedBy per yaml."""
    role_values = {'sector': sector, 'provider': provider}
    produced = role_values.get(table_cfg.get('activity_produced_by', 'sector'))
    consumed = role_values.get(table_cfg.get('activity_consumed_by', 'provider'))
    return produced, consumed


def _state_fips_lookup() -> dict[str, str]:
    return {
        str(row.State).strip().lower(): str(row.FIPS)
        for row in get_state_FIPS('2015').itertuples(index=False)
    }


def _parse_sector_by_provider(
    df: pd.DataFrame, table_cfg: dict[str, Any], source: str
) -> pd.DataFrame:
    """Parse Tables 2.2, 2.3, and 2.4 (sales, revenue, or prices by sector and provider)."""
    header_row = _first_matching_row(df, 'Year')
    if header_row is None:
        return pd.DataFrame()
    sectors = [
        str(c).replace('\n', ' ').strip()
        for c in df.iloc[header_row, 1:].tolist()
        if pd.notna(c) and str(c).strip()
    ]
    flow_name = table_cfg.get('flow_name', table_cfg['table_label'])
    rows: list[dict[str, Any]] = []
    provider: str | None = None
    for i in range(header_row + 1, len(df)):
        label = df.iloc[i, 0]
        if pd.isna(label):
            continue
        label = str(label).strip()
        if not label or label.startswith('N/A'):
            break
        if _is_year_label(label):
            year = int(str(label).strip().split('.')[0])
            for j, sector in enumerate(sectors):
                val = df.iloc[i, j + 1]
                if pd.notna(val):
                    activity_produced, activity_consumed = (
                        _sector_by_provider_activities(
                            table_cfg, sector=sector, provider=provider
                        )
                    )
                    rows.append(
                        {
                            'Year': year,
                            'FlowName': flow_name,
                            'ActivityProducedBy': activity_produced,
                            'ActivityConsumedBy': activity_consumed,
                            'FlowAmount': val,
                        }
                    )
        else:
            provider = label
    return _base_fba_rows(table_cfg=table_cfg, source=source, rows=rows)


def _parse_state_sector_price(
    df: pd.DataFrame, table_cfg: dict[str, Any], source: str
) -> pd.DataFrame:
    """Parse Table 2.10 (state average retail price by customer sector).

    Same Activity fields as Table 2.4: provider and sector; Location is state FIPS.
    Workbook columns are typically two recent years only (e.g. 2023 and 2024).
    """
    sector_row = _first_matching_row(df, 'Residential', col=1)
    year_row = _first_matching_row(df, 'Year 2024', col=1)
    if sector_row is None or year_row is None:
        return pd.DataFrame()
    state_fips = _state_fips_lookup()
    col_meta: list[tuple[int, str, int]] = []
    current_sector: str | None = None
    for col in range(1, df.shape[1]):
        sector_cell = df.iloc[sector_row, col]
        if pd.notna(sector_cell) and str(sector_cell).strip():
            current_sector = str(sector_cell).replace('\n', ' ').strip()
        year_cell = df.iloc[year_row, col]
        if current_sector is None or pd.isna(year_cell):
            continue
        year_match = _YEAR_HEADER_RE.match(str(year_cell).strip())
        if year_match is None:
            continue
        col_meta.append((col, current_sector, int(year_match.group(1))))

    flow_name = table_cfg.get('flow_name', table_cfg['table_label'])
    rows: list[dict[str, Any]] = []
    for i in range(year_row + 1, len(df)):
        label = df.iloc[i, 0]
        if pd.isna(label):
            continue
        state = str(label).replace('\n', ' ').strip()
        if not state or state.startswith('See ') or state.startswith('Source:'):
            break
        if state in _CENSUS_DIVISIONS or state == 'U.S. Total':
            continue
        location = state_fips.get(state.lower())
        if location is None:
            continue
        for col, sector, year in col_meta:
            val = df.iloc[i, col]
            if pd.isna(val):
                continue
            text = str(val).strip()
            if not text or text == '--':
                continue
            provider = table_cfg.get('provider', 'Total Electric Industry')
            activity_produced, activity_consumed = _sector_by_provider_activities(
                table_cfg, sector=sector, provider=provider
            )
            rows.append(
                {
                    'Year': year,
                    'FlowName': flow_name,
                    'Location': location,
                    'ActivityProducedBy': activity_produced,
                    'ActivityConsumedBy': activity_consumed,
                    'FlowAmount': val,
                }
            )
    return _base_fba_rows(table_cfg=table_cfg, source=source, rows=rows)


def _trade_flow_name(metric: str) -> str | None:
    """Map import/export column headers to standard electricity flow names."""
    metric_lower = metric.lower()
    if 'import' in metric_lower:
        return 'electricity imports'
    if 'export' in metric_lower:
        return 'electricity exports'
    return None


def _parse_electricity_trade(
    df: pd.DataFrame, table_cfg: dict[str, Any], source: str
) -> pd.DataFrame:
    """Parse Table 2.14 (Canada/Mexico imports and exports by year)."""
    year_row = _first_matching_row(df, 'Year')
    if year_row is None:
        return pd.DataFrame()
    group_row = year_row - 1
    col_meta: list[tuple[str, str]] = []
    current_country = ''
    for col in range(1, df.shape[1]):
        group = df.iloc[group_row, col]
        metric = df.iloc[year_row, col]
        if pd.notna(group) and str(group).strip():
            current_country = str(group).replace('\n', ' ').strip()
        if pd.notna(metric) and str(metric).strip():
            if 'u.s. total' in current_country.lower():
                continue
            flow_name = _trade_flow_name(str(metric).strip())
            if flow_name is not None:
                col_meta.append((current_country, flow_name))

    rows: list[dict[str, Any]] = []
    for i in range(year_row + 1, len(df)):
        label = df.iloc[i, 0]
        if not _is_year_label(label):
            break
        year = int(str(label).strip().split('.')[0])
        for col, (country, flow_name) in enumerate(col_meta, start=1):
            val = df.iloc[i, col]
            if pd.notna(val):
                if flow_name == 'electricity imports':
                    activity_produced, activity_consumed = country, None
                else:
                    activity_produced, activity_consumed = None, country
                rows.append(
                    {
                        'Year': year,
                        'FlowName': flow_name,
                        'Location': country,
                        'ActivityProducedBy': activity_produced,
                        'ActivityConsumedBy': activity_consumed,
                        'FlowAmount': val,
                    }
                )
    return _base_fba_rows(table_cfg=table_cfg, source=source, rows=rows)


# EIA uses different column titles for the same small-scale PV series (Table 3.6 vs 3.x.B).
_GENERATION_FUEL_ALIASES: dict[str, str] = {
    'Estimated Small Scale Solar Photovoltaic Generation': 'Estimated Solar Photovoltaic',
}


def _canonical_generation_fuel(fuel: str) -> str:
    name = fuel.strip()
    return _GENERATION_FUEL_ALIASES.get(name, name)


def _generation_fuel_name(fuel: str) -> str:
    """Fuel label for FlowName; distinguishes utility-scale vs small-scale solar."""
    name = _canonical_generation_fuel(fuel)
    lower = name.lower()
    if 'estimated' in lower:
        return f'{name}, small scale'
    if name == 'Solar' or lower in ('solar photovoltaic', 'solar thermal'):
        return f'{name}, utility scale'
    return name


def _generation_flow_name(base: str, fuel: str) -> str:
    return f'{base}, {_generation_fuel_name(fuel)}'


def _include_generation_column(fuel: str, table_cfg: dict[str, Any]) -> bool:
    """Return False for columns listed in ``exclude_columns`` to avoid double-counting."""
    excluded = {str(c).strip() for c in table_cfg.get('exclude_columns', [])}
    return fuel.strip() not in excluded


def _parse_generation_by_fuel(
    df: pd.DataFrame, table_cfg: dict[str, Any], source: str
) -> pd.DataFrame:
    """Parse Tables 3.1.A-3.6 (annual net generation by fuel and sector)."""
    header_row = _first_matching_row(df, 'Period')
    if header_row is None:
        return pd.DataFrame()
    fuels = [
        str(c).replace('\n', ' ').strip()
        for c in df.iloc[header_row, 1:].tolist()
        if pd.notna(c) and str(c).strip()
    ]
    annual_row = _first_matching_row(df, 'Annual Totals')
    if annual_row is None:
        return pd.DataFrame()
    start = annual_row + 1
    flow_name = table_cfg.get('flow_name', table_cfg['table_label'])
    sector = table_cfg.get('sector')
    rows: list[dict[str, Any]] = []
    for i in range(start, len(df)):
        label = df.iloc[i, 0]
        if not _is_year_label(label):
            break
        year = int(str(label).strip().split('.')[0])
        for j, fuel in enumerate(fuels):
            if not _include_generation_column(fuel, table_cfg):
                continue
            val = df.iloc[i, j + 1]
            if pd.notna(val):
                rows.append(
                    {
                        'Year': year,
                        'FlowName': _generation_flow_name(flow_name, fuel),
                        'ActivityProducedBy': sector,
                        'ActivityConsumedBy': None,
                        'FlowAmount': val,
                    }
                )
    df_out = _base_fba_rows(table_cfg=table_cfg, source=source, rows=rows)
    if note := table_cfg.get('aggregation_note'):
        df_out['Description'] = f"{table_cfg['table_label']}. {note}"
    return df_out


def _clean_accounting_label(line: str) -> str:
    return line.lstrip('.').strip()


def _accounting_section(clean_line: str, section: str | None) -> str | None:
    """Track revenue vs expense section while scanning Table 8.3 line items."""
    lower = clean_line.lower()
    if clean_line == 'Utility Operating Revenues':
        return 'revenue'
    if clean_line == 'Utility Operating Expenses':
        return 'expenses'
    if 'net utility operating income' in lower:
        return None
    return section


_ACCOUNTING_SECTION_HEADERS = frozenset(
    {'Utility Operating Revenues', 'Utility Operating Expenses'}
)


def _accounting_flow_name(clean_line: str, section: str | None) -> str:
    """Build FlowName with revenue/expense prefix except for section header rows."""
    if clean_line in _ACCOUNTING_SECTION_HEADERS:
        return clean_line
    if section == 'revenue':
        return f'revenue: {clean_line}'
    if section == 'expenses':
        return f'expenses: {clean_line}'
    return clean_line


def _year_column_blocks(df: pd.DataFrame) -> list[tuple[int, list[tuple[int, int]]]]:
    """Table 8.3 repeats year headers (e.g. 2014-2019, then 2020-2024)."""
    blocks: list[tuple[int, list[tuple[int, int]]]] = []
    for i in range(len(df)):
        if str(df.iloc[i, 0]).strip() != 'Description':
            continue
        year_cols: list[tuple[int, int]] = []
        for col in range(1, df.shape[1]):
            val = df.iloc[i, col]
            if pd.notna(val) and _is_year_label(str(val).strip().split('.')[0]):
                year_cols.append((col, int(str(val).strip().split('.')[0])))
        if year_cols:
            blocks.append((i, year_cols))
    return blocks


def _parse_years_as_columns(
    df: pd.DataFrame, table_cfg: dict[str, Any], source: str
) -> pd.DataFrame:
    """Parse Table 8.3 (utility revenue and expense line items with years as columns)."""
    blocks = _year_column_blocks(df)
    if not blocks:
        return pd.DataFrame()

    use_prefix = table_cfg.get('revenue_expense_flow_prefix', False)
    activity_produced = table_cfg.get('activity_produced_by', table_cfg['table_label'])
    rows: list[dict[str, Any]] = []
    for block_idx, (year_row, year_cols) in enumerate(blocks):
        next_year_row = (
            blocks[block_idx + 1][0] if block_idx + 1 < len(blocks) else len(df)
        )
        section: str | None = None
        for i in range(year_row + 1, next_year_row):
            line = df.iloc[i, 0]
            if pd.isna(line):
                continue
            line = str(line).strip()
            if not line or line.startswith('Source:') or line.startswith('Note'):
                break
            clean = _clean_accounting_label(line)
            if not clean:
                continue
            if use_prefix:
                section = _accounting_section(clean, section)
                flow_name = _accounting_flow_name(clean, section)
            else:
                flow_name = clean
            for col, year in year_cols:
                val = df.iloc[i, col]
                if pd.notna(val):
                    rows.append(
                        {
                            'Year': year,
                            'FlowName': flow_name,
                            'ActivityProducedBy': activity_produced,
                            'ActivityConsumedBy': None,
                            'FlowAmount': val,
                        }
                    )
    df = _base_fba_rows(table_cfg=table_cfg, source=source, rows=rows)
    if note := table_cfg.get('aggregation_note'):
        df['Description'] = f"{table_cfg['table_label']}. {note}"
    return df


_LAYOUT_PARSERS = {
    'sector_by_provider': _parse_sector_by_provider,
    'state_sector_price': _parse_state_sector_price,
    'electricity_trade': _parse_electricity_trade,
    'generation_by_fuel': _parse_generation_by_fuel,
    'years_as_columns': _parse_years_as_columns,
}


def electric_power_annual_parse(
    *,
    df_list: list[pd.DataFrame],
    source: str,
    year: str | None,
    config: dict[str, Any],
    **_: Any,
) -> pd.DataFrame:
    """Parse configured Electric Power Annual tables into Flow-By-Activity format."""
    frames = []
    for raw in df_list:
        table_key = raw['_table_key'].iloc[0]
        table_cfg = config['tables'][table_key]
        parser = _LAYOUT_PARSERS[table_cfg['layout']]
        frames.append(parser(raw.drop(columns=['_table_key']), table_cfg, source))

    df = pd.concat([f for f in frames if len(f)], ignore_index=True)
    if len(df) == 0:
        return df

    # call_all_years filters with str year labels in generateflowbyactivity
    df['Year'] = df['Year'].astype(str)
    fips_year = int(df['Year'].astype(int).max()) if year is None else int(year)
    df = assign_fips_location_system(df, fips_year)
    return df


if __name__ == '__main__':
    from pathlib import Path

    from bedrock.extract.flowbyactivity import getFlowByActivity
    from bedrock.extract.generateflowbyactivity import generateFlowByActivity
    from bedrock.utils.config.settings import FBA_DIR

    generateFlowByActivity(source='EIA_ElectricPowerAnnual', year='2014-2024')
    combined = pd.concat(
        [getFlowByActivity('EIA_ElectricPowerAnnual', y) for y in range(2014, 2025)],
        ignore_index=True,
    )
    out_path = Path(FBA_DIR) / 'EIA_ElectricPowerAnnual_2014-2024_review.csv'
    combined.to_csv(out_path, index=False)
    print(f'Wrote {len(combined)} rows to {out_path}')
