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
DEFAULT_MAX_SECTORS = 0


def bly_plot_options(func: F) -> F:
    """BLy figure options (compose with ``common_options`` on the umbrella CLI)."""
    func = click.option(
        "--bly-max-sectors",
        type=int,
        default=DEFAULT_MAX_SECTORS,
        show_default=True,
        help=(
            "BLy stacked bar: keep at most this many named sectors (top-|Δ|); "
            "roll the rest into Other Increase / Other Decrease. Use 0 for no cap."
        ),
    )(func)
    func = click.option(
        "--bly-group-small-threshold",
        type=float,
        default=DEFAULT_GROUP_SMALL_THRESHOLD,
        show_default=True,
        help=(
            "BLy stacked bar: roll sectors with |Δ Mt CO2e| below this into "
            "Other Increase / Other Decrease. Use 0 to show every sector."
        ),
    )(func)
    return func


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


def bucket_small_and_overflow(
    df: pd.DataFrame,
    *,
    threshold: float,
    max_sectors: int,
) -> pd.DataFrame:
    """Roll sectors with |Δ| < threshold AND any overflow past top-N into Other buckets.

    Named sectors kept are the top ``max_sectors`` by |value| among those with
    |value| ≥ threshold. Everything else (sub-threshold or rank > max_sectors)
    is summed into ``Other Increase`` / ``Other Decrease`` rows, with a label
    suffix describing the combined rule.
    """
    df = df.copy()
    abs_val = df["value"].abs()

    below_threshold = (
        abs_val < threshold if threshold > 0 else pd.Series(False, index=df.index)
    )
    eligible = df.loc[~below_threshold].copy()
    if max_sectors > 0 and len(eligible) > max_sectors:
        eligible_ranked = eligible.reindex(
            eligible["value"].abs().sort_values(ascending=False).index
        )
        kept = eligible_ranked.head(max_sectors)
        overflow = eligible_ranked.tail(len(eligible_ranked) - max_sectors)
    else:
        kept = eligible
        overflow = df.iloc[0:0]

    rolled = pd.concat([df.loc[below_threshold], overflow], ignore_index=True)
    if rolled.empty:
        return kept.reset_index(drop=True)

    pos = rolled.loc[rolled["value"] > 0, "value"]
    neg = rolled.loc[rolled["value"] < 0, "value"]

    suffix = f"\n(|Δ| < {threshold:g} MMT)" if below_threshold.any() else ""

    rolled_rows: list[dict[str, str | float]] = []
    if len(pos) > 0 and pos.sum() != 0:
        rolled_rows.append(
            {"sector": f"Other Increase{suffix}", "value": float(pos.sum())}
        )
    if len(neg) > 0 and neg.sum() != 0:
        rolled_rows.append(
            {"sector": f"Other Decrease{suffix}", "value": float(neg.sum())}
        )

    if not rolled_rows:
        return kept.reset_index(drop=True)

    grouped_df = pd.DataFrame(rolled_rows)
    return pd.concat([kept, grouped_df], ignore_index=True)


def build_sector_stack_frame(
    tab: pd.DataFrame,
    *,
    sector_column: str = SECTOR_COLUMN,
    value_column: str = VALUE_COLUMN,
    group_small_threshold: float = DEFAULT_GROUP_SMALL_THRESHOLD,
    max_sectors: int = DEFAULT_MAX_SECTORS,
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
    df = bucket_small_and_overflow(
        df, threshold=group_small_threshold, max_sectors=max_sectors
    )
    return df
