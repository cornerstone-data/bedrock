"""Fetch + cache Google Sheet diagnostics tabs as typed DataFrames.

Each tab is fetched once via ``read_sheet_tab``, numeric columns are
coerced (Sheets returns all-string cells), and the result is cached as
parquet next to this package. Subsequent calls skip the network unless
``refresh=True``.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from bedrock.utils.io.gcp import read_sheet_tab

logger = logging.getLogger(__name__)

CACHE_ROOT = Path(__file__).resolve().parent / ".cache"


def _cache_path(sheet_id: str, tab: str) -> Path:
    safe_tab = tab.replace("/", "_")
    return CACHE_ROOT / sheet_id / f"{safe_tab}.parquet"


def _coerce_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """Convert columns that parse cleanly as numeric; leave the rest as strings.

    Sheets returns every cell as a string. Coerce column-wise so EF values
    and counts become numeric, but preserve identifier columns (sector
    codes, country codes, sector_name) as strings.

    A column is coerced only when *every* non-null value parses as numeric.
    This is deliberate: NAICS sectors are often mixed (``111200`` alongside
    ``33131B``), and a permissive threshold would silently convert the
    majority-digit codes to floats and drop the alphanumeric ones.
    """
    out = df.copy()
    for col in out.columns:
        non_null = out[col].notna() & (out[col].astype(str).str.len() > 0)
        if not non_null.any():
            continue
        coerced = pd.to_numeric(out[col][non_null], errors="coerce")
        if coerced.notna().all():
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def load_tab(
    sheet_id: str,
    tab: str,
    *,
    refresh: bool = False,
) -> pd.DataFrame:
    """Return a tab as a typed DataFrame, using the parquet cache when present."""
    path = _cache_path(sheet_id, tab)
    if path.exists() and not refresh:
        logger.info(f"Cache hit: {path}")
        return pd.read_parquet(path)

    logger.info(f"Fetching sheet {sheet_id!r} tab {tab!r}")
    df = _coerce_numeric(read_sheet_tab(sheet_id, tab))
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path)
    logger.info(f"Cached: {path}")
    return df


def load_tabs(
    sheet_id: str,
    tabs: list[str],
    *,
    refresh: bool = False,
) -> dict[str, pd.DataFrame]:
    """Load a set of tabs; any missing or unreadable tab raises."""
    return {tab: load_tab(sheet_id, tab, refresh=refresh) for tab in tabs}


def load_tabs_optional(
    sheet_id: str,
    tabs: list[str],
    *,
    refresh: bool = False,
) -> dict[str, pd.DataFrame | None]:
    """Like ``load_tabs`` but tolerates missing tabs, returning None for each."""
    result: dict[str, pd.DataFrame | None] = {}
    for tab in tabs:
        try:
            result[tab] = load_tab(sheet_id, tab, refresh=refresh)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Sheet tab {tab!r} not loaded: {e}")
            result[tab] = None
    return result
