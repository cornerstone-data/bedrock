"""Import downloaded diagnostics workbooks into the shared parquet cache."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from bedrock.analysis.electricity_disagg_diagnostics.manifest import Manifest
from bedrock.utils.validation.analysis.bly_plots import TAB_BLY
from bedrock.utils.validation.analysis.fetch import _cache_path, _coerce_numeric

logger = logging.getLogger(__name__)

REQUIRED_TABS = (TAB_BLY, 'config_summary')


def local_workbook_path(local_dir: Path, config_name: str) -> Path:
    """Resolve ``{config_name}.xlsx`` (or ``.xls``) under *local_dir*."""
    for ext in ('.xlsx', '.xls'):
        candidate = local_dir / f'{config_name}{ext}'
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(
        f'No local workbook for config {config_name!r} in {local_dir}. '
        f'Expected {config_name}.xlsx (Google Sheets → Download → Microsoft Excel).'
    )


def _prepare_local_tab(df: pd.DataFrame, tab: str) -> pd.DataFrame:
    """Normalize Excel exports so parquet cache matches live Sheets dtypes."""
    if tab == 'config_summary':
        out = df.copy()
        for col in out.columns:
            out[col] = out[col].map(lambda x: '' if pd.isna(x) else str(x))
        return out
    out = _coerce_numeric(df)
    if 'index' in out.columns:
        out['index'] = out['index'].map(lambda x: '' if pd.isna(x) else str(x))
    return out


def import_workbook_to_cache(
    xlsx_path: Path,
    sheet_id: str,
    *,
    tabs: tuple[str, ...] = REQUIRED_TABS,
) -> None:
    """Read selected tabs from a downloaded workbook into ``fetch`` parquet cache."""
    available = pd.ExcelFile(xlsx_path, engine='openpyxl').sheet_names
    missing = [tab for tab in tabs if tab not in available]
    if missing:
        raise ValueError(
            f'{xlsx_path.name} is missing tab(s) {missing}. '
            f'Found: {available}. Export the full diagnostics workbook from Google Sheets.'
        )
    for tab in tabs:
        df = _prepare_local_tab(
            pd.read_excel(xlsx_path, sheet_name=tab, engine='openpyxl'),
            tab,
        )
        path = _cache_path(sheet_id, tab)
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path)
        logger.info('Cached %s -> %s', tab, path)


def seed_cache_from_local_dir(manifest: Manifest, local_dir: Path) -> None:
    """Import all manifest workbooks from *local_dir* into the parquet cache."""
    jobs: list[tuple[str, str]] = [(manifest.footing.config, manifest.footing.sheet_id)]
    jobs.extend((step.config, step.sheet_id) for step in manifest.steps)
    if manifest.final.sheet_id != manifest.steps[-1].sheet_id:
        jobs.append((manifest.final.config, manifest.final.sheet_id))

    seen: set[str] = set()
    for config_name, sheet_id in jobs:
        if sheet_id in seen:
            continue
        seen.add(sheet_id)
        xlsx_path = local_workbook_path(local_dir, config_name)
        print(f'  import {xlsx_path.name} -> cache/{sheet_id}/')
        import_workbook_to_cache(xlsx_path, sheet_id)
