"""eGRID inputs for electricity disaggregation (stewi inventories and workbook sheets)."""

from __future__ import annotations

import functools
from collections.abc import Iterable
from pathlib import Path

import pandas as pd
import stewi.exceptions
from stewi.egrid import OUTPUT_PATH, _config, download_eGRID, extract_eGRID_excel
from stewi.formats import StewiFormat
from stewi.globals import MWh_MJ, read_inventory
from stewi.globals import config as stewi_config

DEFAULT_YEAR_START = 2016
DEFAULT_YEAR_END = 2024


def egrid_inventory_years(year_start: int, year_end: int) -> list[int]:
    """Calendar years with stewi eGRID source config in [year_start, year_end]."""
    keys = stewi_config()['databases']['eGRID']
    configured = sorted(int(k) for k in keys if str(k).isdigit())
    return [y for y in configured if year_start <= y <= year_end]


def _require_egrid_year(year: int) -> str:
    year_str = str(year)
    if year_str not in _config:
        raise stewi.exceptions.InventoryNotAvailableError(
            inv='eGRID',
            year=year_str,
        )
    return year_str


def ensure_egrid_workbook(year: int, *, download_if_missing: bool = True) -> Path:
    """Return the local eGRID workbook path for a stewi-configured year."""
    year_str = _require_egrid_year(year)
    path = OUTPUT_PATH / _config[year_str]['file_name']
    if not path.is_file():
        if not download_if_missing:
            msg = f'eGRID workbook not found for {year}: {path}'
            raise FileNotFoundError(msg)
        download_eGRID(year_str)
    if not path.is_file():
        msg = f'eGRID workbook not found for {year} after download: {path}'
        raise FileNotFoundError(msg)
    return path


def _find_column(df: pd.DataFrame, substring: str) -> str:
    matches = [c for c in df.columns if substring in str(c)]
    if not matches:
        msg = f'No column containing {substring!r} in GGL sheet; got {list(df.columns)}'
        raise ValueError(msg)
    return str(matches[0])


def load_egrid_ggl(
    year: int,
    *,
    download_if_missing: bool = True,
) -> pd.DataFrame:
    """Grid gross loss and estimated losses by interconnect region for one inventory year."""
    year_str = _require_egrid_year(year)
    if download_if_missing:
        ensure_egrid_workbook(year, download_if_missing=True)
    raw = extract_eGRID_excel(year_str, 'GGL', index='field')
    return _normalize_ggl(raw)


def _normalize_ggl(raw: pd.DataFrame) -> pd.DataFrame:
    region_col = _find_column(raw, 'interconnect power grids')
    est_col = _find_column(raw, 'Estimated losses (MWh)')
    loss_col = _find_column(raw, 'Grid gross loss')
    year_col = next(
        (c for c in ('Data Year', 'Data year') if c in raw.columns),
        None,
    )
    if year_col is None:
        msg = f'GGL sheet missing Data Year column; got {list(raw.columns)}'
        raise ValueError(msg)

    out = pd.DataFrame(
        {
            'year': pd.to_numeric(raw[year_col], errors='coerce').astype('Int64'),
            'region': raw[region_col].astype(str).str.strip(),
            'estimated_losses_mwh': pd.to_numeric(raw[est_col], errors='coerce'),
            'grid_gross_loss': pd.to_numeric(raw[loss_col], errors='coerce'),
        }
    )
    if out['year'].isna().any():
        raise ValueError('GGL sheet has non-numeric Data Year values')
    return out.astype({'year': int})


def grid_loss_by_region_by_year(
    year_start: int = DEFAULT_YEAR_START,
    year_end: int = DEFAULT_YEAR_END,
    *,
    years: Iterable[int] | None = None,
    download_if_missing: bool = True,
) -> pd.DataFrame:
    """Stacked GGL rows for each inventory year (long format: year, region, losses)."""
    if years is None:
        year_list = egrid_inventory_years(year_start, year_end)
    else:
        year_list = sorted(years)

    frames = [
        load_egrid_ggl(year, download_if_missing=download_if_missing)
        for year in year_list
    ]
    return pd.concat(frames, ignore_index=True)


def load_egrid_flowbyfacility(
    year: int,
    *,
    download_if_missing: bool = True,
) -> pd.DataFrame:
    """Return stewi eGRID flow-by-facility parquet for *year* without getInventory filters.

    Uses ``read_inventory`` so plant net generation matches the stored inventory
    (sum of PLNT / US ``USNGENAN``). ``getInventory`` re-aggregates on read and drops
    non-positive ``FlowAmount`` rows, which raises the US electricity total.
    """
    _require_egrid_year(year)
    inv = read_inventory(
        'eGRID',
        year,
        StewiFormat.FLOWBYFACILITY,
        download_if_missing=download_if_missing,
    )
    if inv is None:
        msg = f'eGRID flow-by-facility inventory not available for {year}'
        raise FileNotFoundError(msg)
    return inv


def _net_generation_mj(flowbyfacility: pd.DataFrame) -> float:
    """Sum Electricity (net generation) across a stewi eGRID flowbyfacility table, in MJ."""
    gen = flowbyfacility.loc[flowbyfacility['FlowName'] == 'Electricity', 'FlowAmount']
    if gen.empty:
        msg = (
            "eGRID flow-by-facility has no 'Electricity' rows "
            '(plant annual net generation)'
        )
        raise ValueError(msg)
    units = flowbyfacility.loc[
        flowbyfacility['FlowName'] == 'Electricity', 'Unit'
    ].unique()
    if len(units) != 1 or units[0] != 'MJ':
        msg = f"unexpected units for 'Electricity': {units.tolist()}"
        raise ValueError(msg)
    return float(gen.sum())


def us_total_net_generation_mwh(
    year: int,
    *,
    download_if_missing: bool = True,
) -> float:
    """Sum US plant annual net generation (MWh); matches PLNT / US workbook totals."""
    inv = load_egrid_flowbyfacility(year, download_if_missing=download_if_missing)
    return _net_generation_mj(inv) / MWh_MJ


def us_total_net_generation_by_year(
    year_start: int = DEFAULT_YEAR_START,
    year_end: int = DEFAULT_YEAR_END,
    *,
    years: Iterable[int] | None = None,
    download_if_missing: bool = True,
) -> pd.Series[float]:
    """US net generation by inventory year (values in MWh, index = year)."""
    if years is None:
        year_list = egrid_inventory_years(year_start, year_end)
    else:
        year_list = sorted(years)

    totals: dict[int, float] = {}
    for year in year_list:
        totals[year] = us_total_net_generation_mwh(
            year, download_if_missing=download_if_missing
        )
    return pd.Series(totals, dtype=float, name='net_generation_mwh')


# ---------------------------------------------------------------------------
# EIA Electric Power Annual helpers for EIA-anchored G/T/D
# ---------------------------------------------------------------------------

_TABLE_2_2_KEYS: tuple[str, ...] = (
    'Residential',
    'Commercial',
    'Industrial',
    'Transportation',
    'Direct Use',
    'Total End Use',
)
_TABLE_3_1_TOTAL_PRODUCER = 'Total (all sectors)'


def _epa_fba(year: int) -> pd.DataFrame:
    from bedrock.extract.flowbyactivity import getFlowByActivity  # noqa: PLC0415

    return getFlowByActivity('EIA_ElectricPowerAnnual', year)


def _table_mask(df: pd.DataFrame, year: int, table_fragment: str) -> pd.Series:
    desc = df['Description'].astype(str)
    return (df['Year'] == year) & desc.str.contains(table_fragment, na=False)


@functools.cache
def eia_table_2_2_end_use_mwh(year: int) -> dict[str, float]:
    """EIA Table 2.2 sales + Direct Use + Total End Use, MWh.

    Do not require ActivityProducedBy == 'Total Electric Industry' for every
    key: Direct Use / Total End Use may be other provider rows.
    """
    df = _epa_fba(year)
    table = df.loc[_table_mask(df, year, 'Table 2.2')]
    out: dict[str, float] = {}
    for key in _TABLE_2_2_KEYS:
        rows = table.loc[table['ActivityConsumedBy'] == key]
        if rows.empty:
            raise ValueError(f'Table 2.2 missing {key!r} for year {year}')
        tei = rows.loc[rows['ActivityProducedBy'] == 'Total Electric Industry']
        if not tei.empty:
            out[key] = float(tei['FlowAmount'].iloc[0])
        else:
            out[key] = float(rows['FlowAmount'].sum())
    if out['Total End Use'] <= 0:
        raise ValueError(f'Table 2.2 Total End Use non-positive for year {year}')
    if 'Direct Use' not in out:
        raise ValueError(f'Table 2.2 missing Direct Use for year {year}')
    return out


def _export_mwh_from_fba(df: pd.DataFrame, year: int) -> float | None:
    mask = (
        (df['Year'] == year)
        & (df['FlowName'].astype(str) == 'electricity exports')
        & df['Description'].astype(str).str.contains('Table 2.14', na=False)
    )
    sub = df.loc[mask]
    if sub.empty:
        return None
    loc = sub['Location'].astype(str)
    keep = loc.str.contains('Canada', case=False, na=False) | loc.str.contains(
        'Mexico', case=False, na=False
    )
    rows = sub.loc[keep]
    if rows.empty:
        return None
    return float(rows['FlowAmount'].sum())


@functools.cache
def eia_table_2_14_export_mwh(year: int) -> float:
    """Canada + Mexico electricity exports from EIA Table 2.14, MWh.

    ``epa_02_14`` uses ``flow_amount_scale: 1`` (not Table 3.1's 1000).
    If *year* is missing, use the latest available 2.14 year and log it.
    """
    import logging  # noqa: PLC0415

    logger = logging.getLogger(__name__)
    try:
        df = _epa_fba(year)
        val = _export_mwh_from_fba(df, year)
        if val is not None:
            return val
    except Exception:
        df = None
    for fallback in range(year - 1, 2013, -1):
        try:
            fb = _epa_fba(fallback)
        except Exception:
            continue
        val = _export_mwh_from_fba(fb, fallback)
        if val is not None:
            logger.info(
                'Table 2.14 year %s missing; using latest available year %s',
                year,
                fallback,
            )
            return val
    raise ValueError(f'Table 2.14 Canada+Mexico exports missing for year {year}')


@functools.cache
def eia_table_3_1_total_mwh(year: int) -> float:
    """EIA Table 3.1.A + 3.1.B all-sector net generation, MWh.

    Filter ``ActivityProducedBy == 'Total (all sectors)'`` and sum FlowAmount
    (extract already drops double-count columns and applies scale 1000).
    """
    df = _epa_fba(year)
    mask = (
        (df['Year'] == year)
        & (df['ActivityProducedBy'] == _TABLE_3_1_TOTAL_PRODUCER)
        & df['Description'].astype(str).str.contains('Table 3.1', na=False)
    )
    sub = df.loc[mask]
    if sub.empty:
        raise ValueError(
            f'Table 3.1 Total (all sectors) missing for year {year} '
            f'(2017 eGRID scale has no fallback)'
        )
    total = float(sub['FlowAmount'].sum())
    if total <= 0:
        raise ValueError(f'Table 3.1 total non-positive for year {year}')
    return total


@functools.cache
def egrid_mwh_for_io_year(year: int, *, download_if_missing: bool = True) -> float:
    """Plant-net eGRID MWh for an IO-account year.

    2017 has no stewi eGRID inventory: ``eGRID_2016 * (EIA 3.1_2017 / 3.1_2016)``.
    Other years use ``us_total_net_generation_mwh``. Do not add GGL losses.
    """
    if year == 2017:
        egrid_2016 = us_total_net_generation_mwh(
            2016, download_if_missing=download_if_missing
        )
        t31_2017 = eia_table_3_1_total_mwh(2017)
        t31_2016 = eia_table_3_1_total_mwh(2016)
        if t31_2016 <= 0:
            raise ValueError('EIA Table 3.1 2016 total is non-positive')
        return float(egrid_2016 * (t31_2017 / t31_2016))
    return us_total_net_generation_mwh(year, download_if_missing=download_if_missing)
