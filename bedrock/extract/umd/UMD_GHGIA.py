# UMD_GHGIA.py (flowsa)
# University of Maryland GHG Inventory and Analysis — https://ghgi.cgs.umd.edu/
#
# Structure mirrors ``EPA_GHGI.py``: ``umd_ghgia_load`` returns one dataframe per table with
# ``SourceName`` = ``UMD_GHGIA_T_<chapter>_<table>``; ``umd_ghgia_parse`` returns a list of parsed
# tables so ``generateFlowByActivity`` writes one parquet per ``SourceName``.
#
# Raw CSVs live under extract/input-data/UMD_GHGIA/<UMD_GHGIA_INPUT_RELEASE_DIR_YEAR>/.
# Discovery prefers local ``bedrock/extract/input_data``; GCS listing fills gaps.
# Each file is opened via ``load_from_gcs`` (download only if missing locally).

from __future__ import annotations

import os
import re
from typing import Any, List, Optional, cast

import numpy as np
import pandas as pd

from bedrock.extract.epa.EPA_GHGI import (
    DROP_COLS,
    YEARS,
    series_separate_name_and_units,
    strip_char,
)
from bedrock.transform.flowbyfunctions import assign_fips_location_system
from bedrock.utils.io.gcp import load_from_gcs
from bedrock.utils.io.gcp_paths import gcs_extract_input_path
from bedrock.utils.io.local_extract_input_data import local_dir_for_gcs_sub_bucket
from bedrock.utils.logging.flowsa_log import log

UMD_SOURCE_PREFIX = 'UMD_GHGIA_T_'


def umd_source_name(table_id: str) -> str:
    """FBA / parquet stem suffix for one GHGIA table file (cf. ``EPA_GHGI_T_*``)."""

    return f'{UMD_SOURCE_PREFIX}{table_id.replace("-", "_")}'


def _table_id_from_source_name(source_name: str) -> str:
    if not str(source_name).startswith(UMD_SOURCE_PREFIX):
        raise ValueError(
            f'Expected SourceName to start with {UMD_SOURCE_PREFIX!r}, got {source_name!r}'
        )
    return str(source_name)[len(UMD_SOURCE_PREFIX) :].replace('_', '-')


# Folder under extract/input-data/UMD_GHGIA/... on GCS and under extract/input_data (inventory years are columns).
UMD_GHGIA_INPUT_RELEASE_DIR_YEAR = '2024'

TABLE_ID_RE = re.compile(r'Table\s+([345]-\d+[a-z]?)\.csv$', re.IGNORECASE)


def _skip_auxiliary_umd_csv_basename(basename: str) -> bool:
    """Skip figure exports and boxed sidebar CSVs (not inventory tables)."""

    b = basename.lower()
    return 'figure' in b or 'box' in b


def _drop_trailing_unnamed_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Drop columns whose names contain ``Unnamed``, except column 0 (row labels / activity).

    ``DROP_COLS`` removes ``Unnamed: 0`` at load time; remaining headers may still be
    ``Unnamed: 1``, etc. EPA-style ``_get_unnamed_cols`` would drop those too and erase the
    activity column UMD spreadsheets use as the first column.
    """
    if df.shape[1] == 0:
        return df
    first = df.columns[0]
    to_drop = [c for c in df.columns if 'Unnamed' in str(c) and not c == first]
    return df.drop(columns=to_drop, errors='ignore')


def _strip_leading_utf8_bom_mojibake(s: str) -> str:
    """Remove UTF-8 BOM whether decoded correctly (\\ufeff) or as latin-1 (ï»¿)."""

    # utf-8-sig decoding already drops U+FEFF; keep for callers on legacy strings / mixed paths.
    stripped = s.lstrip('\ufeff')
    while stripped.startswith('ï»¿'):
        stripped = stripped.removeprefix('ï»¿').lstrip()
    return stripped


def _peek_csv_head_lines(path: str, n: int = 5) -> str:
    """First lines of CSV for title inference. Use utf-8-sig so BOM is not mangled."""
    buf: list[str] = []
    try:
        with open(path, encoding='utf-8-sig', errors='replace') as f:
            for _ in range(n):
                buf.append(f.readline())
    except OSError:
        return ''
    return _strip_leading_utf8_bom_mojibake(''.join(buf))


def _chapter_tables(config: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Flatten nested ``Tables`` (by chapter); legacy ``Chapter3Tables`` merges on top."""

    out: dict[str, dict[str, Any]] = {}
    tables = config.get('Tables')
    if isinstance(tables, dict):
        for chapter_tables in tables.values():
            if not isinstance(chapter_tables, dict):
                continue
            for tid, meta in chapter_tables.items():
                tid_s = str(tid)
                out[tid_s] = dict(meta) if isinstance(meta, dict) else {}
    legacy = config.get('Chapter3Tables')
    if isinstance(legacy, dict):
        for tid, meta in legacy.items():
            tid_s = str(tid)
            base = out.get(tid_s, {})
            if isinstance(meta, dict):
                merged = dict(base)
                merged.update(dict(meta))
                out[tid_s] = merged
            elif tid_s not in out:
                out[tid_s] = {}
    return out


def _layout_hint_from_yaml_lists(
    table_id: str, config: dict[str, Any]
) -> Optional[str]:
    """Map ``fuel_sector`` / ``fuel_vehicle`` / ``ghg_fuel`` / ``no_nesting`` lists → layout name."""

    tid = str(table_id)
    fs = {str(x) for x in (config.get('fuel_sector') or [])}
    fv = {str(x) for x in (config.get('fuel_vehicle') or [])}
    gf = {str(x) for x in (config.get('ghg_fuel') or [])}
    nn = {str(x) for x in (config.get('no_nesting') or [])}
    if tid in fs:
        return 'fuel_sector'
    if tid in fv:
        return 'fuel_vehicle'
    if tid in gf:
        return 'ghg_sections'
    if tid in nn:
        return 'standard_wide'
    return None


def _local_umd_csv_pairs(local_root: str) -> list[tuple[str, str]]:
    """Relative paths (under local_root) and table ids for Table [345]-*.csv (exclude figure/box)."""
    out: list[tuple[str, str]] = []
    if not os.path.isdir(local_root):
        return out
    for root, _, files in os.walk(local_root):
        for fname in files:
            if not fname.endswith('.csv') or _skip_auxiliary_umd_csv_basename(fname):
                continue
            m = TABLE_ID_RE.match(fname)
            if not m:
                continue
            full = os.path.join(root, fname)
            rel = os.path.relpath(full, local_root).replace('\\', '/')
            out.append((rel, m.group(1)))
    out.sort(key=lambda x: x[0])
    return out


def _iter_umd_csv_paths(source: str) -> list[tuple[str, str]]:
    """Discover Chapter 3–5 CSVs: prefer local extract/input_data mirror; else list GCS.

    Matches EPA GHGI behavior: ``local_dir_for_gcs_sub_bucket`` + ``load_from_gcs``
    (download only when the path does not exist locally).
    """
    sub_bucket = (
        gcs_extract_input_path(source, UMD_GHGIA_INPUT_RELEASE_DIR_YEAR)
        .strip('/')
        .replace('\\', '/')
    )
    local_root = local_dir_for_gcs_sub_bucket(sub_bucket)

    pairs = _local_umd_csv_pairs(local_root)
    if pairs:
        log.info(
            'UMD GHGIA: discovered %s Table [345]-*.csv files under local cache %s',
            len(pairs),
            local_root,
        )
        return pairs

    log.info(
        'UMD GHGIA: no local Chapter 3–5 CSVs under %s — listing GCS gs://cornerstone-default/%s/',
        local_root,
        sub_bucket,
    )
    out: list[tuple[str, str]] = []
    try:
        from google.cloud import storage

        client = storage.Client()
        bucket = client.bucket('cornerstone-default')
        prefix = sub_bucket + '/'
        for blob in bucket.list_blobs(prefix=prefix):
            if not blob.name.endswith('.csv'):
                continue
            base = os.path.basename(blob.name)
            if _skip_auxiliary_umd_csv_basename(base):
                continue
            m = TABLE_ID_RE.match(base)
            if not m:
                continue
            rel = blob.name[len(prefix) :].replace('\\', '/')
            out.append((rel, m.group(1)))
    except Exception as exc:
        log.warning('UMD GHGIA: GCS listing failed (%s)', exc)

    out.sort(key=lambda x: x[0])
    return out


def _primary_flow_from_title(title: str) -> Optional[str]:
    u = title.upper()
    if re.search(r'\bN2O\b', u):
        return 'N2O'
    if re.search(r'\bCH4\b', u):
        return 'CH4'
    if re.search(r'\bCO2\b', u):
        return 'CO2'
    return None


def _infer_unit_from_title(title: str) -> str:
    u = title.upper()
    if 'METRIC TONS' in u and 'MMT' not in u:
        return 'MT CO2e'
    return 'MMT CO2e'


def _infer_layout(
    table_id: str,
    df: pd.DataFrame,
    title: str,
    chapter_tables: dict[str, dict[str, Any]],
    config: dict[str, Any],
) -> str:
    overrides = chapter_tables
    if table_id in overrides and overrides[table_id].get('layout'):
        return cast(str, overrides[table_id]['layout'])

    listed = _layout_hint_from_yaml_lists(table_id, config)
    if listed:
        return listed

    title_u = title.upper()
    col0 = df.iloc[:, 0].dropna().astype(str).map(lambda x: strip_char(str(x)))
    up = col0.str.upper().str.strip()
    if up.isin({'CO2', 'CH4', 'N2O', 'N20'}).any():
        return 'ghg_sections'

    if 'PETROLEUM SYSTEMS' in title_u:
        return 'systems_segments'
    if 'NATURAL GAS SYSTEMS' in title_u:
        return 'systems_segments'
    if 'INTERNATIONAL BUNKER' in title_u or table_id == '3-102':
        return 'multi_chem'

    if (
        'MOBILE COMBUSTION' in title_u
        or 'TRANSPORTATION END-USE' in title_u
        or table_id in {'3-8', '3-9', '3-10'}
    ):
        return 'fuel_vehicle'

    if table_id.startswith(('4-', '5-')):
        return 'standard_wide'

    return 'fuel_sector'


def _table_meta(
    table_id: str,
    title: str,
    layout: str,
    chapter_tables: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    overrides_all = chapter_tables
    ov = dict(overrides_all.get(table_id, {}))
    flow = ov.get('flow') or _primary_flow_from_title(title)
    unit = ov.get('unit') or _infer_unit_from_title(title)
    desc = ov.get('desc') or _compact_desc(title, table_id)
    meta = {
        'class': ov.get('class', 'Chemicals'),
        'compartment': ov.get('compartment', 'air'),
        'unit': unit,
        'flow': flow,
        'desc': desc,
        'data_reliability': ov.get('data_reliability', 5),
        'activity': ov.get('activity'),
        'melt_var': ov.get('melt_var', 'FlowName'),
        'nested_fuel_sector': ov.get('nested_fuel_sector'),
        'layout': layout,
    }
    if layout in ('systems_segments', 'multi_chem', 'ghg_sections'):
        meta['flow'] = flow  # may be None for multi_chem / ghg_sections
    return meta


def _apply_inventory_year_templates(meta: dict[str, Any], inventory_year: str) -> None:
    """Expand __year__ in YAML-driven strings (e.g. NEU activity templates on table 3-14)."""
    for key in ('activity', 'desc'):
        val = meta.get(key)
        if isinstance(val, str) and '__year__' in val:
            meta[key] = val.replace('__year__', inventory_year)


def _compact_desc(title: str, table_id: str) -> str:
    for line in title.splitlines():
        s = _strip_leading_utf8_bom_mojibake(line.strip())
        if s and 'Table' in s:
            return s[:500]
    return f'Table {table_id} (UMD GHGIA)'


def _flatten_3_25_columns(df: pd.DataFrame) -> pd.DataFrame:
    new_headers: list[str] = []
    for col in df.columns:
        new_header = 'Unnamed: 0'
        if 'Unnamed' not in col[0]:
            if 'Unnamed' not in col[1]:
                new_header = f'{col[0]} {col[1]}'
            else:
                new_header = col[0]
        else:
            new_header = col[1]
        new_headers.append(new_header)
    df.columns = pd.Index(new_headers)
    return df


def _load_one_dataframe(
    rel: str,
    table_id: str,
    sub_bucket: str,
    local_base: str,
    year: str,
    chapter_tables: dict[str, dict[str, Any]],
) -> pd.DataFrame:
    """Load Chapter 3 CSVs: same pipeline as EPA GHGI — cache under extract/input_data, then ``load_from_gcs``."""
    ov_sk = chapter_tables.get(table_id, {}).get('skiprows')

    def loader(pth: str) -> pd.DataFrame:
        head = _peek_csv_head_lines(pth)
        if chapter_tables.get(table_id, {}).get('two_row_header'):
            raw = pd.read_csv(
                pth,
                skiprows=int(ov_sk) if ov_sk is not None else 1,
                header=[0, 1],
                encoding='utf-8-sig',
                thousands=',',
            )
            raw = _flatten_3_25_columns(raw)
        else:
            skip = int(ov_sk) if ov_sk is not None else 1
            raw = pd.read_csv(
                pth,
                skiprows=skip,
                encoding='utf-8-sig',
                thousands=',',
                header=0,
            )
        if table_id == '3-8':
            cols = [c[:4] for c in list(raw.columns[1:])]
            raw = raw.rename(columns=dict(zip(raw.columns[1:], cols)))
        raw.attrs['umd_title'] = head
        return raw

    df = load_from_gcs(
        name=rel.replace('\\', '/'),
        sub_bucket=sub_bucket,
        local_dir=local_base,
        loader=loader,
    )
    years_to_drop = YEARS.copy()
    if year in years_to_drop:
        years_to_drop.remove(year)
    df = df.drop(columns=(DROP_COLS + years_to_drop), errors='ignore')
    df['SourceName'] = umd_source_name(table_id)
    return df


def umd_ghgia_load(**kwargs: Any) -> List[pd.DataFrame]:
    """Load each ``Table *.csv`` into its own dataframe with ``SourceName`` set (cf. ``ghg_load_gcs``).

    Wired like ``EPA_GHGI``: ``extract_data_from_raw_sources: False`` + ``gcs_fxn`` so HTTP is not used;
    paths mirror ``extract/input-data/`` on GCS under ``bedrock/extract/input_data``. Listing prefers an
    existing local tree; ``load_from_gcs`` downloads only when a file path is missing locally.
    """
    inventory_year = str(kwargs['year'])
    source = str(kwargs['source'])
    config = kwargs['config']
    chapter_tables = _chapter_tables(config)

    sub_bucket = (
        gcs_extract_input_path(source, UMD_GHGIA_INPUT_RELEASE_DIR_YEAR)
        .strip('/')
        .replace('\\', '/')
    )
    local_base = local_dir_for_gcs_sub_bucket(sub_bucket)

    pairs = _iter_umd_csv_paths(source)
    if not pairs:
        log.warning(
            'UMD GHGIA: no Table [345]-*.csv files found under prefix %s', sub_bucket
        )
        return []

    log.info(
        'UMD GHGIA: staging prefix extract/input-data/%s/%s/ — inventory year %s '
        '(local cache %s)',
        source,
        UMD_GHGIA_INPUT_RELEASE_DIR_YEAR,
        inventory_year,
        local_base,
    )

    dfs: list[pd.DataFrame] = []
    for rel, tid in pairs:
        if inventory_year in ('2023', '2024') and tid == '3-25b':
            continue
        try:
            df = _load_one_dataframe(
                rel, tid, sub_bucket, local_base, inventory_year, chapter_tables
            )
            if df is not None and len(df.columns) > 1:
                dfs.append(df)
        except Exception as exc:
            log.error('UMD GHGIA: failed to load %s (%s): %s', rel, tid, exc)

    if not dfs:
        log.error(
            'UMD GHGIA: no tables loaded for inventory %s %s — check gcloud auth and '
            'that CSV files exist under gs://cornerstone-default/%s/',
            source,
            inventory_year,
            sub_bucket,
        )

    return dfs


def _apply_flow_amount_cleanup(df: pd.DataFrame) -> pd.DataFrame:
    try:
        flow_stripped = df['FlowAmount'].astype(str).str.strip()
        df['Suppressed'] = flow_stripped.where(flow_stripped == '+', np.nan)
        df['FlowAmount'] = (
            df['FlowAmount'].astype(str).str.replace(',', '').infer_objects(copy=False)
        )
        df['FlowAmount'] = df['FlowAmount'].replace('+', '0').infer_objects(copy=False)
        df['FlowAmount'] = pd.to_numeric(df['FlowAmount'], errors='coerce')
        df = df.dropna(subset='FlowAmount')
    except (AttributeError, KeyError):
        df = df.dropna(subset='FlowAmount')
    return df


def _title_case_fuel_activity(label: str) -> str:
    s = strip_char(label)
    parts = re.split(r'(\s+|-)', s)
    out: list[str] = []
    for p in parts:
        if p.isspace() or p == '-' or not p:
            out.append(p)
            continue
        if p.upper() in ('LPG', 'CO2', 'CH4', 'N2O'):
            out.append(p.upper())
            continue
        out.append(p[:1].upper() + p[1:] if len(p) > 1 else p.upper())
    return ''.join(out)


# Sector rows nested under a fuel-type parent (Table 3-4 ``Fuel Type/Sector`` column).
FUEL_TYPE_SECTOR_CHILD_LABELS: tuple[str, ...] = (
    'Electric Power',
    'Industrial',
    'Commercial',
    'Residential',
    'U.S. Territories',
    'Transportation',
    'Exploration',
    'Production (Total)',
    'Refining',
    'Crude Oil Transportation',
    'Cropland',
    'Grassland',
)

UMD_NESTED_PARENT_FUEL_ALIASES: dict[str, str] = {
    'GEOTHERMALA': 'Geothermal',
    'GEOTHERMAL A': 'Geothermal',
}


def _header_indicates_nested_fuel_sector(header: str) -> bool:
    """True when column A documents combined fuel-type + sector nesting (e.g. Table 3-4)."""
    h = str(header).strip().lower().replace('\\', '/')
    if 'unnamed' in h:
        return False
    return 'fuel' in h and 'sector' in h


def _normalize_nested_parent_fuel_label(label: str) -> str:
    """Strip footnotes and known UMD suffix markers from fuel-type header rows."""
    s = strip_char(label).strip()
    ul = ' '.join(s.split()).upper()
    return UMD_NESTED_PARENT_FUEL_ALIASES.get(ul, s)


def _apply_nested_fuel_sector_labels(df: pd.DataFrame) -> pd.DataFrame:
    """Expand hierarchical ``Fuel Type/Sector`` rows into ``Coal Residential``-style activities."""
    sector_by_upper = {
        ' '.join(s.split()).upper(): s for s in FUEL_TYPE_SECTOR_CHILD_LABELS
    }

    drop_idx: list[Any] = []
    current_fuel_display = ''

    for idx in df.index:
        raw = df.at[idx, 'ActivityProducedBy']
        if pd.isna(raw):
            drop_idx.append(idx)
            continue
        label = strip_char(str(raw)).strip()
        ul = ' '.join(label.split()).upper()

        if not ul:
            drop_idx.append(idx)
            continue
        if ul.startswith('TOTAL'):
            drop_idx.append(idx)
            continue
        if ul.startswith(('NO ', 'NO(')):
            drop_idx.append(idx)
            continue
        if ul.startswith('NOTE:'):
            drop_idx.append(idx)
            continue
        if re.match(r'^[a-z]\s+Although\b', label):
            drop_idx.append(idx)
            continue

        sector_hit = sector_by_upper.get(ul)
        if sector_hit:
            if not current_fuel_display:
                drop_idx.append(idx)
                continue
            df.at[idx, 'ActivityProducedBy'] = f'{current_fuel_display} {sector_hit}'
            continue

        current_fuel_display = _title_case_fuel_activity(
            _normalize_nested_parent_fuel_label(label)
        )
        drop_idx.append(idx)

    return df.drop(index=drop_idx, errors='ignore')


def _drop_parse_helpers_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Remove staging columns before layout parsing (keep ``SourceName``)."""

    if '_TableId' in df.columns:
        return df.drop(columns=['_TableId'])
    return df


def _parse_fuel_sector_like(
    df: pd.DataFrame,
    meta: dict[str, Any],
    *,
    mode: str,
    source_name: str,
) -> pd.DataFrame:
    """mode: 'sector' (3-7-like) or 'vehicle' (3-13-like)."""
    df = _drop_trailing_unnamed_cols(df)
    header0 = str(df.columns[0])
    df = df.rename(columns={df.columns[0]: 'ActivityProducedBy'})
    nested_fuel_sector = False
    if mode == 'sector':
        nf_cfg = meta.get('nested_fuel_sector')
        if nf_cfg is True:
            nested_fuel_sector = True
        elif nf_cfg is False:
            nested_fuel_sector = False
        else:
            nested_fuel_sector = _header_indicates_nested_fuel_sector(header0)

    if nested_fuel_sector:
        df = _apply_nested_fuel_sector_labels(df)

    df['ActivityConsumedBy'] = 'None'
    df['FlowType'] = 'ELEMENTARY_FLOW'
    df['Location'] = '00000'
    df['SourceName'] = source_name

    id_vars = [
        'SourceName',
        'ActivityConsumedBy',
        'ActivityProducedBy',
        'FlowType',
        'Location',
    ]
    df = df.melt(id_vars=id_vars, var_name='Year', value_name='FlowAmount')
    df = _apply_flow_amount_cleanup(df)

    df['Unit'] = meta.get('unit')
    df['FlowName'] = meta.get('flow')
    df['Class'] = meta.get('class')
    df['Description'] = meta.get('desc')
    df['Compartment'] = meta.get('compartment')
    df['Year'] = df['Year'].astype(str)
    df = df[df['Year'].isin([meta.get('_filter_year')])]

    if nested_fuel_sector:
        return df

    activity_subtotal_sector = list(FUEL_TYPE_SECTOR_CHILD_LABELS)
    activity_subtotal_fuel = [
        'Gasoline',
        'Distillate Fuel Oil',
        'Jet Fuel',
        'Aviation Gasoline',
        'Residual Fuel Oil',
        'Natural Gas',
        'LPG',
        'Electricity',
        'Fuel Type/Vehicle Type',
        'Diesel On-Road',
        'Alternative Fuel On-Road',
        'Non-Road',
        'Gasoline On-Road',
    ]
    activity_subtotal = (
        activity_subtotal_sector if mode == 'sector' else activity_subtotal_fuel
    )

    apbe_value = ''
    after_total = False
    for row_idx, row in df.iterrows():
        apb_value = strip_char(cast(str, row['ActivityProducedBy']))
        if apb_value in activity_subtotal or after_total:
            apbe_value = apb_value
            df.loc[row_idx, 'ActivityProducedBy'] = f'All activities {apbe_value}'
        else:
            apb_txt = apb_value
            df.loc[row_idx, 'ActivityProducedBy'] = f'{apb_txt} {apbe_value}'
        if apb_value.startswith('Total'):
            df = df.drop(row_idx)
            after_total = True

    return df


def _parse_systems_segments(
    df: pd.DataFrame, meta: dict[str, Any], *, source_name: str
) -> pd.DataFrame:
    df = _drop_trailing_unnamed_cols(df)
    df = df.rename(columns={df.columns[0]: 'ActivityProducedBy'})
    df['ActivityConsumedBy'] = 'None'
    df['FlowType'] = 'ELEMENTARY_FLOW'
    df['Location'] = '00000'
    df['SourceName'] = source_name

    id_vars = [
        'SourceName',
        'ActivityConsumedBy',
        'ActivityProducedBy',
        'FlowType',
        'Location',
    ]
    df = df.melt(id_vars=id_vars, var_name='Year', value_name='FlowAmount')
    df = _apply_flow_amount_cleanup(df)

    df['Unit'] = meta.get('unit')
    df['FlowName'] = meta.get('flow')
    df['Class'] = meta.get('class')
    df['Description'] = meta.get('desc')
    df['Compartment'] = meta.get('compartment')
    fy = meta.get('_filter_year')
    df['Year'] = df['Year'].astype(str)
    df = df[df['Year'].isin([fy])]

    bool_apb = False
    apbe_value = ''
    flow_name_list = [
        'Explorationb',
        'Production',
        'Processing',
        'Transmission and Storage',
        'Distribution',
        'Post-Meter',
        'Crude Oil Transportation',
        'Refining',
        'Exploration',
        'Mobile AC',
        'Refrigerated Transport',
        'Comfort Cooling for Trains and Buses',
    ]
    start_activity = cast(str, meta.get('flow'))
    for row_idx, row in df.iterrows():
        apb_value = cast(str, row['ActivityProducedBy'])
        if apb_value.strip() in flow_name_list:
            apbe_value = apb_value
            if apbe_value == 'Explorationb':
                apbe_value = 'Exploration'
            df.loc[row_idx, 'FlowName'] = start_activity
            df.loc[row_idx, 'ActivityProducedBy'] = apbe_value
            bool_apb = True
        else:
            if bool_apb:
                df.loc[row_idx, 'FlowName'] = start_activity
                apb_txt = strip_char(cast(str, df.loc[row_idx, 'ActivityProducedBy']))
                if apb_txt == 'Gathering and Boostingc':
                    apb_txt = 'Gathering and Boosting'
                df.loc[row_idx, 'ActivityProducedBy'] = f'{apbe_value} - {apb_txt}'
            else:
                apb_txt = strip_char(cast(str, df.loc[row_idx, 'ActivityProducedBy']))
                df.loc[row_idx, 'ActivityProducedBy'] = f'{apb_txt} {apbe_value}'
        if apb_value.strip() == 'Total' or apb_value.strip().startswith('Total'):
            df = df.drop(row_idx)

    return df


def _parse_multi_chem(
    df: pd.DataFrame, meta: dict[str, Any], *, source_name: str
) -> pd.DataFrame:
    df = _drop_trailing_unnamed_cols(df)
    df = df.rename(columns={df.columns[0]: 'ActivityProducedBy'})
    df['ActivityConsumedBy'] = 'None'
    df['FlowType'] = 'ELEMENTARY_FLOW'
    df['Location'] = '00000'
    df['SourceName'] = source_name

    id_vars = [
        'SourceName',
        'ActivityConsumedBy',
        'ActivityProducedBy',
        'FlowType',
        'Location',
    ]
    df = df.melt(id_vars=id_vars, var_name='Year', value_name='FlowAmount')
    df = _apply_flow_amount_cleanup(df)

    df['Unit'] = meta.get('unit')
    df['Class'] = meta.get('class')
    df['Description'] = meta.get('desc')
    df['Compartment'] = meta.get('compartment')
    fy = meta.get('_filter_year')
    df['Year'] = df['Year'].astype(str)
    df = df[df['Year'].isin([fy])]

    bool_apb = False
    bool_lulucf = False
    apbe_value = ''
    flow_name_list = [
        'CO2',
        'CH4',
        'N2O',
        'NF3',
        'HFCs',
        'PFCs',
        'SF6',
        'CH4 a',
        'N2O b',
        'CO',
        'NOx',
    ]
    for row_idx, row in df.iterrows():
        apb_value = strip_char(cast(str, row['ActivityProducedBy']))
        if 'CH4' in apb_value:
            apb_value = 'CH4'
        elif 'N2O' in apb_value and apb_value != 'N2O from Product Uses':
            apb_value = 'N2O'
        elif 'CO2' in apb_value:
            apb_value = 'CO2'

        if apb_value in flow_name_list:
            if bool_lulucf:
                df = df.drop(row_idx)
            else:
                apbe_value = apb_value
                df.loc[row_idx, 'FlowName'] = apbe_value
                df.loc[row_idx, 'ActivityProducedBy'] = 'All activities'
                bool_apb = True
        elif apb_value.startswith('LULUCF'):
            df.loc[row_idx, 'FlowName'] = 'CO2e'
            df.loc[row_idx, 'ActivityProducedBy'] = strip_char(apb_value)
            bool_lulucf = True
        elif apb_value.startswith(('Total', 'Net')):
            df = df.drop(row_idx)
        else:
            apb_txt = strip_char(cast(str, df.loc[row_idx, 'ActivityProducedBy']))
            df.loc[row_idx, 'ActivityProducedBy'] = apb_txt
            if bool_apb:
                df.loc[row_idx, 'FlowName'] = apbe_value

    return df


def _parse_non_energy_rows(
    df: pd.DataFrame, meta: dict[str, Any], *, source_name: str
) -> pd.DataFrame:
    df = _drop_trailing_unnamed_cols(df)
    df = df.rename(columns={df.columns[0]: 'ActivityProducedBy'})
    df['ActivityConsumedBy'] = 'None'
    df['FlowType'] = 'ELEMENTARY_FLOW'
    df['Location'] = '00000'
    df['SourceName'] = source_name

    id_vars = [
        'SourceName',
        'ActivityConsumedBy',
        'ActivityProducedBy',
        'FlowType',
        'Location',
    ]
    df = df.melt(id_vars=id_vars, var_name='Year', value_name='FlowAmount')
    df = _apply_flow_amount_cleanup(df)

    df['Unit'] = meta.get('unit')
    df['FlowName'] = meta.get('flow')
    df['Class'] = meta.get('class')
    df['Description'] = meta.get('desc')
    df['Compartment'] = meta.get('compartment')
    fy = meta.get('_filter_year')
    df['Year'] = df['Year'].astype(str)
    df = df[df['Year'].isin([fy])]

    apbe_value = ''
    flow_name_list = ['Industry', 'Transportation', 'U.S. Territories']
    for row_idx, row in df.iterrows():
        unit = cast(str, row['Unit'])
        if unit.strip() == 'MMT  CO2':
            df.loc[row_idx, 'Unit'] = 'MMT CO2e'
        if cast(str, df.loc[row_idx, 'Unit']).strip() != 'MMT CO2e':
            df = df.drop(row_idx)
            continue
        df.loc[row_idx, 'FlowName'] = meta.get('flow')
        apb_value = ' '.join(cast(str, row['ActivityProducedBy']).split())
        apb_value = apb_value.replace('°', '')
        if apb_value in flow_name_list:
            apbe_value = apb_value
            df.loc[row_idx, 'ActivityProducedBy'] = f'{apbe_value} All activities'
        else:
            apb_txt = strip_char(apb_value)
            df.loc[row_idx, 'ActivityProducedBy'] = f'{apbe_value} {apb_txt}'
        if apb_value == 'Total' or apb_value == 'Total ':
            df = df.drop(row_idx)

    return df


def _parse_non_energy_melt(
    df: pd.DataFrame, meta: dict[str, Any], *, source_name: str
) -> pd.DataFrame:
    """Wide NEU-style table (two-row CSV header): melt flow/type columns (EPA GHGI ``ghg_parse`` 3-25 pattern)."""
    df = _drop_trailing_unnamed_cols(df)
    df = df.rename(columns={df.columns[0]: 'ActivityProducedBy'})
    df['ActivityConsumedBy'] = 'None'
    df['FlowType'] = 'ELEMENTARY_FLOW'
    df['Location'] = '00000'
    df['SourceName'] = source_name

    id_vars = [
        'SourceName',
        'ActivityConsumedBy',
        'ActivityProducedBy',
        'FlowType',
        'Location',
    ]
    # EPA uses var_name FlowName then overwrites contents; never drop that column. Use a temp
    # name so ``drop`` cannot remove FlowName after ``series_separate_name_and_units``.
    _melt_label_col = '__umd_non_energy_melt_col__'
    df = df.melt(id_vars=id_vars, var_name=_melt_label_col, value_name='FlowAmount')
    df = _apply_flow_amount_cleanup(df)

    act_tpl = meta.get('activity') or 'Adjusted Non-Energy Use Fossil Fuel - __type__'
    flow_raw = df[_melt_label_col]
    name_unit = series_separate_name_and_units(
        flow_raw, str(act_tpl), str(meta.get('unit'))
    )
    df['FlowName'] = name_unit['names']
    df['Unit'] = name_unit['units']
    df.drop(columns=[_melt_label_col], inplace=True)
    df['Year'] = meta.get('_filter_year')

    df['Class'] = meta.get('class')
    df['Description'] = meta.get('desc')
    df['Compartment'] = meta.get('compartment')
    return df


def _parse_ghg_sections(
    df: pd.DataFrame, meta: dict[str, Any], *, source_name: str
) -> pd.DataFrame:
    df = _drop_trailing_unnamed_cols(df)
    label_col = df.columns[0]
    year_cols = [c for c in df.columns[1:] if str(c).strip() in YEARS]
    if not year_cols:
        year_cols = [
            c
            for c in df.columns[1:]
            if re.fullmatch(r'(19|20)\d{2}', str(c).strip() or '')
        ]

    fy = str(meta.get('_filter_year'))
    records: list[dict[str, Any]] = []
    current_flow: Optional[str] = None

    for _, row in df.iterrows():
        raw_label = row[label_col]
        if pd.isna(raw_label):
            continue
        label = strip_char(str(raw_label))
        ul = label.upper().strip()
        if ul in {'CO2', 'CH4', 'N2O', 'N20'}:
            current_flow = 'N2O' if ul == 'N20' else ul
            continue
        if current_flow is None:
            continue
        if ul.startswith('TOTAL') or label.startswith('Total'):
            continue

        activity = _title_case_fuel_activity(label)
        for yc in year_cols:
            if str(yc).strip() != fy:
                continue
            amt = row[yc]
            records.append(
                {
                    'ActivityProducedBy': activity,
                    'ActivityConsumedBy': 'None',
                    'FlowName': current_flow,
                    'Year': fy,
                    'FlowAmount': amt,
                    'FlowType': 'ELEMENTARY_FLOW',
                    'Location': '00000',
                    'SourceName': source_name,
                    'Unit': meta.get('unit'),
                    'Class': meta.get('class'),
                    'Description': meta.get('desc'),
                    'Compartment': meta.get('compartment'),
                }
            )

    out = pd.DataFrame.from_records(records)
    if len(out):
        out = _apply_flow_amount_cleanup(out)
    return out


def _standard_wide_year_melt(
    df: pd.DataFrame, meta: dict[str, Any], *, source_name: str
) -> pd.DataFrame:
    """Activity column + year columns only (no sector / multi-chem reshaping)."""
    df = _drop_trailing_unnamed_cols(df)
    df = df.rename(columns={df.columns[0]: 'ActivityProducedBy'})
    df['ActivityConsumedBy'] = 'None'
    df['FlowType'] = 'ELEMENTARY_FLOW'
    df['Location'] = '00000'
    df['SourceName'] = source_name

    id_vars = [
        'SourceName',
        'ActivityConsumedBy',
        'ActivityProducedBy',
        'FlowType',
        'Location',
    ]
    df = df.melt(id_vars=id_vars, var_name='Year', value_name='FlowAmount')
    df = _apply_flow_amount_cleanup(df)

    df['Unit'] = meta.get('unit')
    df['FlowName'] = meta.get('flow')
    df['Class'] = meta.get('class')
    df['Description'] = meta.get('desc')
    df['Compartment'] = meta.get('compartment')
    fy = str(meta.get('_filter_year'))
    df['Year'] = df['Year'].astype(str)
    df = df[df['Year'].isin([fy])]
    for row_idx, row in df.iterrows():
        df.loc[row_idx, 'ActivityProducedBy'] = strip_char(
            cast(str, row['ActivityProducedBy'])
        )
    return df


def _finalize_common(df: pd.DataFrame, meta: dict[str, Any]) -> pd.DataFrame:
    df['DataReliability'] = meta.get('data_reliability', 5)
    df['DataCollection'] = 1
    df['MeasureofSpread'] = 'None'
    df['DistributionType'] = 'None'
    df['LocationSystem'] = 'None'
    fy = str(meta.get('_filter_year'))
    df = assign_fips_location_system(df, fy)

    df['ActivityProducedBy'] = df['ActivityProducedBy'].astype(str).str.strip()
    df['ActivityConsumedBy'] = df['ActivityConsumedBy'].astype(str).str.strip()
    df['FlowName'] = df['FlowName'].astype(str).str.strip()

    df.loc[
        df['ActivityProducedBy'].str.contains('U.S. Territor')
        | df['ActivityConsumedBy'].str.contains('U.S. Territor'),
        'Location',
    ] = '99000'

    df.drop(df.loc[df['ActivityProducedBy'] == 'Total'].index, inplace=True)
    df.drop(df.loc[df['FlowName'] == 'Total'].index, inplace=True)
    df = df.loc[:, ~df.columns.duplicated()]
    df['FlowAmount'] = df['FlowAmount'].replace(',', '', regex=True)
    return df


def _parse_single_table_chunk(
    df: pd.DataFrame, year: str, config: dict[str, Any]
) -> pd.DataFrame:
    """Parse one loaded table into FBA layout (cf. a single iteration of ``EPA_GHGI.ghg_parse``)."""

    if 'SourceName' not in df.columns:
        log.warning('UMD GHGIA: skipping table without SourceName column')
        return pd.DataFrame()

    source_name = str(df['SourceName'].iloc[0])
    table_id = _table_id_from_source_name(source_name)
    dfp = _drop_parse_helpers_columns(df)

    title = str(dfp.attrs.get('umd_title', ''))
    chapter_tables = _chapter_tables(config)
    layout_eff = _infer_layout(table_id, dfp, title, chapter_tables, config)

    meta = _table_meta(table_id, title, layout_eff, chapter_tables)
    _apply_inventory_year_templates(meta, year)
    meta['_filter_year'] = year

    if layout_eff == 'ghg_sections':
        parsed = _parse_ghg_sections(dfp, meta, source_name=source_name)
    elif layout_eff == 'fuel_vehicle':
        parsed = _parse_fuel_sector_like(
            dfp, meta, mode='vehicle', source_name=source_name
        )
    elif layout_eff == 'fuel_sector':
        parsed = _parse_fuel_sector_like(
            dfp, meta, mode='sector', source_name=source_name
        )
    elif layout_eff == 'systems_segments':
        parsed = _parse_systems_segments(dfp, meta, source_name=source_name)
    elif layout_eff == 'multi_chem':
        parsed = _parse_multi_chem(dfp, meta, source_name=source_name)
    elif layout_eff == 'non_energy_rows':
        parsed = _parse_non_energy_rows(dfp, meta, source_name=source_name)
    elif layout_eff == 'non_energy_melt':
        parsed = _parse_non_energy_melt(dfp, meta, source_name=source_name)
    elif layout_eff == 'standard_wide':
        parsed = _standard_wide_year_melt(dfp, meta, source_name=source_name)
    else:
        if not meta.get('flow'):
            log.warning(
                'UMD GHGIA: cannot parse table %s (layout=%s, no flow in title/metadata)',
                table_id,
                layout_eff,
            )
            return pd.DataFrame()
        log.info(
            'UMD GHGIA: table %s using standard wide-year melt (layout=%s)',
            table_id,
            layout_eff,
        )
        parsed = _standard_wide_year_melt(dfp, meta, source_name=source_name)

    if parsed is None or len(parsed.index) == 0:
        log.warning('UMD GHGIA: no rows after parsing table %s', table_id)
        return pd.DataFrame()

    parsed = _finalize_common(parsed, meta)
    return parsed


def umd_ghgia_parse(
    *,
    df_list: List[pd.DataFrame],
    year: str,
    config: dict[str, Any],
    **_kwargs: Any,
) -> List[pd.DataFrame]:
    """Parse and format each UMD GHGIA table (mirrors ``EPA_GHGI.ghg_parse`` — returns a list per table)."""

    cleaned_list: list[pd.DataFrame] = []
    for df in df_list:
        if 'SourceName' not in df.columns:
            log.warning('UMD GHGIA: skipping dataframe without SourceName')
            continue
        source_name = str(df['SourceName'].iloc[0])
        log.info('Processing %s', source_name)
        chunk = _parse_single_table_chunk(df, str(year), config)
        if len(chunk.index):
            cleaned_list.append(chunk)
        else:
            log.warning('UMD GHGIA: empty result for %s', source_name)

    return cleaned_list


__all__ = ['UMD_SOURCE_PREFIX', 'umd_source_name', 'umd_ghgia_load', 'umd_ghgia_parse']
