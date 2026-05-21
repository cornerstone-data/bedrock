"""eGRID inputs for electricity disaggregation (stewi inventories and workbook sheets)."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

import pandas as pd
import stewi
import stewi.exceptions
from stewi.egrid import OUTPUT_PATH, _config, download_eGRID, extract_eGRID_excel
from stewi.globals import MWh_MJ
from stewi.globals import config as stewi_config

DEFAULT_YEAR_START = 2016
DEFAULT_YEAR_END = 2024


def egrid_inventory_years(year_start: int, year_end: int) -> list[int]:
    """Calendar years with stewi eGRID source config in [year_start, year_end]."""
    keys = stewi_config()["databases"]["eGRID"]
    configured = sorted(int(k) for k in keys if str(k).isdigit())
    return [y for y in configured if year_start <= y <= year_end]


def _require_egrid_year(year: int) -> str:
    year_str = str(year)
    if year_str not in _config:
        raise stewi.exceptions.InventoryNotAvailableError(
            inv="eGRID",
            year=year_str,
        )
    return year_str


def ensure_egrid_workbook(year: int, *, download_if_missing: bool = True) -> Path:
    """Return the local eGRID workbook path for a stewi-configured year."""
    year_str = _require_egrid_year(year)
    path = OUTPUT_PATH / _config[year_str]["file_name"]
    if not path.is_file():
        if not download_if_missing:
            msg = f"eGRID workbook not found for {year}: {path}"
            raise FileNotFoundError(msg)
        download_eGRID(year_str)
    if not path.is_file():
        msg = f"eGRID workbook not found for {year} after download: {path}"
        raise FileNotFoundError(msg)
    return path


def _find_column(df: pd.DataFrame, substring: str) -> str:
    matches = [c for c in df.columns if substring in str(c)]
    if not matches:
        msg = f"No column containing {substring!r} in GGL sheet; got {list(df.columns)}"
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
    raw = extract_eGRID_excel(year_str, "GGL", index="field")
    return _normalize_ggl(raw)


def _normalize_ggl(raw: pd.DataFrame) -> pd.DataFrame:
    region_col = _find_column(raw, "interconnect power grids")
    est_col = _find_column(raw, "Estimated losses (MWh)")
    loss_col = _find_column(raw, "Grid gross loss")
    year_col = "Data Year" if "Data Year" in raw.columns else _find_column(raw, "Year")

    out = pd.DataFrame(
        {
            "year": pd.to_numeric(raw[year_col], errors="coerce").astype("Int64"),
            "region": raw[region_col].astype(str).str.strip(),
            "estimated_losses_mwh": pd.to_numeric(raw[est_col], errors="coerce"),
            "grid_gross_loss": pd.to_numeric(raw[loss_col], errors="coerce"),
        }
    )
    if out["year"].isna().any():
        raise ValueError("GGL sheet has non-numeric Data Year values")
    return out.astype({"year": int})


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
    """Return stewi eGRID flow-by-facility inventory for *year*."""
    return stewi.getInventory(
        "eGRID",
        year,
        stewiformat="flowbyfacility",
        download_if_missing=download_if_missing,
    )


def _net_generation_mj(flowbyfacility: pd.DataFrame) -> float:
    """Sum Electricity (net generation) across a stewi eGRID flowbyfacility table, in MJ."""
    gen = flowbyfacility.loc[flowbyfacility["FlowName"] == "Electricity", "FlowAmount"]
    if gen.empty:
        msg = (
            "eGRID flow-by-facility has no 'Electricity' rows "
            "(plant annual net generation)"
        )
        raise ValueError(msg)
    units = flowbyfacility.loc[
        flowbyfacility["FlowName"] == "Electricity", "Unit"
    ].unique()
    if len(units) != 1 or units[0] != "MJ":
        msg = f"unexpected units for 'Electricity': {units.tolist()}"
        raise ValueError(msg)
    return float(gen.sum())


def us_total_net_generation_mwh(
    year: int,
    *,
    download_if_missing: bool = True,
) -> float:
    """Sum US plant annual net generation (MWh) from stewi eGRID for *year*."""
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
    return pd.Series(totals, dtype=float, name="net_generation_mwh")
