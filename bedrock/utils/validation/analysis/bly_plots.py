"""BLy sector stacked-bar data prep + CLI options.

Consumed by ``diagnostics_plots`` as part of the diagnostics figure suite. Tab
layout matches ``calculate_national_accounting_balance_diagnostics`` output.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar

import click
import pandas as pd

from bedrock.utils.taxonomy.cornerstone.commodities import WASTE_DISAGG_COMMODITIES

F = TypeVar("F", bound=Callable[..., Any])

TAB_BLY = "BLy_new_vs_BLy_old"
SECTOR_COLUMN = "index"
VALUE_COLUMN = "BLy_new - BLy_old (MtCO2e)"
WASTE_AGGREGATE_SECTOR = "562000"
DEFAULT_GROUP_SMALL_THRESHOLD = 3.0


def bly_plot_options(func: F) -> F:
    """BLy figure options (compose with ``common_options`` on the umbrella CLI)."""
    return click.option(
        "--bly-group-small-threshold",
        type=float,
        default=DEFAULT_GROUP_SMALL_THRESHOLD,
        show_default=True,
        help=(
            "BLy stacked bar: roll sectors with |Δ Mt CO2e| below this into "
            "Other Increase / Other Decrease. Use 0 to show every sector."
        ),
    )(func)


def combine_waste_diffs(df: pd.DataFrame, *, aggregate_sector: str) -> pd.DataFrame:
    disagg_sectors = set(WASTE_DISAGG_COMMODITIES.get(aggregate_sector, []))
    if not disagg_sectors:
        return df

    sectors_present = set(df["sector"])
    if aggregate_sector not in sectors_present:
        return df
    if not (sectors_present & disagg_sectors):
        return df

    waste_mask = df["sector"].eq(aggregate_sector) | df["sector"].isin(disagg_sectors)
    combined_value = df.loc[waste_mask, "value"].sum()
    non_waste_df = df.loc[~waste_mask].copy()
    combined_df = pd.DataFrame([{"sector": aggregate_sector, "value": combined_value}])
    return pd.concat([non_waste_df, combined_df], ignore_index=True)


def group_small_changes(df: pd.DataFrame, *, threshold: float) -> pd.DataFrame:
    if threshold <= 0:
        return df

    df = df.copy()
    small = df["value"].abs() < threshold
    if not small.any():
        return df

    large_df = df.loc[~small].copy()
    small_df = df.loc[small].copy()

    other_increase = small_df.loc[small_df["value"] > 0, "value"].sum()
    other_decrease = small_df.loc[small_df["value"] < 0, "value"].sum()

    suffix = f" (|Δ| < {threshold:g} MMT)"
    grouped_rows: list[dict[str, str | float]] = []
    if other_increase != 0:
        grouped_rows.append(
            {"sector": f"Other Increase{suffix}", "value": other_increase}
        )
    if other_decrease != 0:
        grouped_rows.append(
            {"sector": f"Other Decrease{suffix}", "value": other_decrease}
        )

    if not grouped_rows:
        return large_df

    grouped_df = pd.DataFrame(grouped_rows)
    return pd.concat([large_df, grouped_df], ignore_index=True)


def build_sector_stack_frame(
    tab: pd.DataFrame,
    *,
    sector_column: str = SECTOR_COLUMN,
    value_column: str = VALUE_COLUMN,
    group_small_threshold: float = DEFAULT_GROUP_SMALL_THRESHOLD,
) -> pd.DataFrame:
    """Normalize a ``BLy_new_vs_BLy_old`` tab to ``sector`` / ``value`` for plotting."""
    missing = [c for c in (sector_column, value_column) if c not in tab.columns]
    if missing:
        raise ValueError(f"BLy tab missing columns {missing}")

    df = tab[[sector_column, value_column]].rename(
        columns={sector_column: "sector", value_column: "value"}
    )
    df["sector"] = df["sector"].astype(str)
    df["value"] = pd.to_numeric(df["value"], errors="raise")
    df = combine_waste_diffs(df, aggregate_sector=WASTE_AGGREGATE_SECTOR)
    df = group_small_changes(df, threshold=group_small_threshold)
    return df
