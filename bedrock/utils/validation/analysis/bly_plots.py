"""BLy sector stacked-bar data prep + CLI options.

Consumed by ``diagnostics_plots`` as part of the diagnostics figure suite. Tab
layout matches ``calculate_national_accounting_balance_diagnostics`` output.

Cross-sheet step / span helpers (``bly_step_delta``, ``bly_span_delta``) compare
live ``BLy_new`` across diagnostics sheets — pathway totals
(``diag(d) @ L @ y``), not inventory E / direct D.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, TypeVar

import click
import pandas as pd

from bedrock.utils.taxonomy.cornerstone.commodities import WASTE_DISAGG_COMMODITIES
from bedrock.utils.validation.analysis.fetch import load_tab
from bedrock.utils.validation.analysis.release_v0_3_progression import ProgressionSheet
from bedrock.utils.validation.analysis.release_v0_v05_us_waterfall_groups import (
    FINAL_V05_USEEIO,
    G2_METHODS,
    G3_DATA,
    G4_NOWCAST,
)

F = TypeVar("F", bound=Callable[..., Any])

logger = logging.getLogger(__name__)

TAB_BLY = "BLy_new_vs_BLy_old"
TAB_N = "N_and_diffs"
SECTOR_COLUMN = "index"
VALUE_COLUMN = "BLy_new - BLy_old (MtCO2e)"
BLY_NEW_COLUMN = "BLy_new (MtCO2e)"
WASTE_AGGREGATE_SECTOR = "562000"
DEFAULT_GROUP_SMALL_THRESHOLD = 3.0
DEFAULT_MAX_SECTORS = 0
DEFAULT_LABEL_MAX_LEN = 42
# Other-bucket floor for weighted-avg N contributions (kg CO2e / USD).
DEFAULT_WEIGHTED_N_GROUP_SMALL_THRESHOLD = 1e-4


@dataclass(frozen=True)
class BlyStepPair:
    """Named adjacent (or span-endpoint) rung pair for cross-sheet BLy Δ."""

    key: str
    label: str
    before: ProgressionSheet
    after: ProgressionSheet


V05_US_BLY_STEP_PAIRS: dict[str, BlyStepPair] = {
    "data": BlyStepPair(
        key="data",
        label="Data (G2→G3)",
        before=G2_METHODS,
        after=G3_DATA,
    ),
    "nowcast": BlyStepPair(
        key="nowcast",
        label="Nowcast (G3→G4)",
        before=G3_DATA,
        after=G4_NOWCAST,
    ),
    "facility": BlyStepPair(
        key="facility",
        label="Facility GHG (G4→FINAL)",
        before=G4_NOWCAST,
        after=FINAL_V05_USEEIO,
    ),
}

DEFAULT_V05_US_PAIR_KEYS: tuple[str, ...] = ("nowcast", "facility")


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
    threshold_unit: str = "MMT",
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

    suffix = (
        f"\n(|Δ| < {threshold:g} {threshold_unit})" if below_threshold.any() else ""
    )

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
    threshold_unit: str = "MMT",
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
        df,
        threshold=group_small_threshold,
        max_sectors=max_sectors,
        threshold_unit=threshold_unit,
    )
    return df


def bly_new_by_sector(sheet_id: str, *, refresh: bool = False) -> pd.DataFrame:
    """Live ``BLy_new`` per sector (MMT), with ``sector_name`` when ``N_and_diffs`` has it."""
    bly = load_tab(sheet_id, TAB_BLY, refresh=refresh)
    if BLY_NEW_COLUMN not in bly.columns:
        raise ValueError(
            f"BLy tab missing {BLY_NEW_COLUMN!r}; columns={list(bly.columns)}"
        )
    out = bly[[SECTOR_COLUMN, BLY_NEW_COLUMN]].rename(
        columns={SECTOR_COLUMN: "sector", BLY_NEW_COLUMN: "bly_new_mmt"}
    )
    out["sector"] = out["sector"].astype(str)
    out["bly_new_mmt"] = pd.to_numeric(out["bly_new_mmt"], errors="raise")

    try:
        names = load_tab(sheet_id, TAB_N, refresh=refresh)[
            [SECTOR_COLUMN, "sector_name"]
        ].drop_duplicates(SECTOR_COLUMN)
        names = names.rename(columns={SECTOR_COLUMN: "sector"})
        names["sector"] = names["sector"].astype(str)
        out = out.merge(names, on="sector", how="left")
    except Exception as exc:  # noqa: BLE001 — names are best-effort for labels
        logger.info("sector_name join skipped for sheet %s: %s", sheet_id, exc)

    return out


def bly_step_delta(
    before_sheet_id: str,
    after_sheet_id: str,
    *,
    refresh: bool = False,
) -> pd.DataFrame:
    """Per-sector ``BLy_new(after) − BLy_new(before)`` as ``sector`` / ``value`` (+ name)."""
    before = bly_new_by_sector(before_sheet_id, refresh=refresh).rename(
        columns={"bly_new_mmt": "bly_before"}
    )
    after = bly_new_by_sector(after_sheet_id, refresh=refresh).rename(
        columns={"bly_new_mmt": "bly_after"}
    )
    name_col = (
        after[["sector", "sector_name"]]
        if "sector_name" in after.columns
        else (
            before[["sector", "sector_name"]]
            if "sector_name" in before.columns
            else None
        )
    )
    merged = before[["sector", "bly_before"]].merge(
        after[["sector", "bly_after"]], on="sector", how="inner"
    )
    merged["value"] = merged["bly_after"] - merged["bly_before"]
    if name_col is not None:
        merged = merged.merge(name_col, on="sector", how="left")
    return merged


def bly_span_delta(
    pairs: Sequence[BlyStepPair],
    *,
    refresh: bool = False,
) -> pd.DataFrame:
    """Combined bar across ordered pairs: ``BLy_new(last.after) − BLy_new(first.before)``.

    When pairs chain (``after[i].config_name == before[i+1].config_name``), per-sector
    span Δ equals the sum of adjacent step Δs. Opposing moves within the span cancel.
    """
    if not pairs:
        raise ValueError("bly_span_delta requires at least one BlyStepPair")

    for idx in range(len(pairs) - 1):
        left = pairs[idx].after.config_name
        right = pairs[idx + 1].before.config_name
        if left != right:
            logger.warning(
                "BLy step pairs do not chain at index %s: %s.after=%s vs %s.before=%s; "
                "span still uses first.before → last.after but step Δs will not "
                "telescope to the span",
                idx,
                pairs[idx].key,
                left,
                pairs[idx + 1].key,
                right,
            )

    return bly_step_delta(
        pairs[0].before.sheet_id,
        pairs[-1].after.sheet_id,
        refresh=refresh,
    )


def n_new_by_sector(sheet_id: str, *, refresh: bool = False) -> pd.DataFrame:
    """Live ``N_new`` per sector from ``N_and_diffs``, with ``sector_name`` when present."""
    df = load_tab(sheet_id, TAB_N, refresh=refresh)
    if "N_new" not in df.columns:
        raise ValueError(f"N_and_diffs missing N_new; columns={list(df.columns)}")
    out = df[[SECTOR_COLUMN, "N_new"]].rename(
        columns={SECTOR_COLUMN: "sector", "N_new": "n_new"}
    )
    out["sector"] = out["sector"].astype(str)
    out["n_new"] = pd.to_numeric(out["n_new"], errors="raise")
    out = out.drop_duplicates("sector", keep="first")
    if "sector_name" in df.columns:
        names = df[[SECTOR_COLUMN, "sector_name"]].drop_duplicates(SECTOR_COLUMN)
        names = names.rename(columns={SECTOR_COLUMN: "sector"})
        names["sector"] = names["sector"].astype(str)
        out = out.merge(names, on="sector", how="left")
    return out


def weighted_n_step_delta(
    before_sheet_id: str,
    after_sheet_id: str,
    *,
    q: pd.Series | None = None,
    refresh: bool = False,
) -> pd.DataFrame:
    """Per-sector contribution to Δ of q-weighted average N: ``(ΔN · q) / Σq``.

    Uses canonical v0.5 ``scaled_q_USA`` when ``q`` is omitted. Net of ``value``
    equals the waterfall step ``Σ(N·q)/Σq`` after − before on the same q.
    """
    if q is None:
        from bedrock.utils.validation.waterfall_progression import (  # noqa: PLC0415
            load_canonical_v0_5_q,
        )

        q = load_canonical_v0_5_q()
    q = q.astype(float).copy()
    q.index = q.index.astype(str)

    before = n_new_by_sector(before_sheet_id, refresh=refresh).rename(
        columns={"n_new": "n_before"}
    )
    after = n_new_by_sector(after_sheet_id, refresh=refresh).rename(
        columns={"n_new": "n_after"}
    )
    merged = before[["sector", "n_before"]].merge(
        after[["sector", "n_after"]], on="sector", how="inner"
    )
    if "sector_name" in after.columns:
        merged = merged.merge(after[["sector", "sector_name"]], on="sector", how="left")
    elif "sector_name" in before.columns:
        merged = merged.merge(
            before[["sector", "sector_name"]], on="sector", how="left"
        )

    q_aligned = q.reindex(merged["sector"]).to_numpy()
    n_before = merged["n_before"].to_numpy(dtype=float)
    n_after = merged["n_after"].to_numpy(dtype=float)
    mask = (
        pd.notna(q_aligned) & (q_aligned != 0) & pd.notna(n_before) & pd.notna(n_after)
    )
    merged = merged.loc[mask].copy()
    q_vals = q.reindex(merged["sector"]).to_numpy(dtype=float)
    denom = float(q_vals.sum())
    if denom == 0.0:
        raise ValueError(
            "canonical q has zero sum on sectors overlapping both N sheets"
        )
    merged["q"] = q_vals
    merged["value"] = (merged["n_after"] - merged["n_before"]) * merged["q"] / denom
    return merged


def weighted_n_span_delta(
    pairs: Sequence[BlyStepPair],
    *,
    q: pd.Series | None = None,
    refresh: bool = False,
) -> pd.DataFrame:
    """Combined weighted-N bar: first.before → last.after (same chaining rules as BLy)."""
    if not pairs:
        raise ValueError("weighted_n_span_delta requires at least one BlyStepPair")

    for idx in range(len(pairs) - 1):
        left = pairs[idx].after.config_name
        right = pairs[idx + 1].before.config_name
        if left != right:
            logger.warning(
                "Weighted-N step pairs do not chain at index %s: %s.after=%s vs "
                "%s.before=%s; span still uses first.before → last.after",
                idx,
                pairs[idx].key,
                left,
                pairs[idx + 1].key,
                right,
            )

    return weighted_n_step_delta(
        pairs[0].before.sheet_id,
        pairs[-1].after.sheet_id,
        q=q,
        refresh=refresh,
    )


def stack_frame_from_delta(
    delta: pd.DataFrame,
    *,
    group_small_threshold: float = DEFAULT_GROUP_SMALL_THRESHOLD,
    max_sectors: int = DEFAULT_MAX_SECTORS,
    label_max_len: int = DEFAULT_LABEL_MAX_LEN,
    threshold_unit: str = "MMT",
) -> pd.DataFrame:
    """Bucket a step/span delta frame and label sectors as ``code name`` for plotting."""
    if "sector" not in delta.columns or "value" not in delta.columns:
        raise ValueError("delta must have 'sector' and 'value' columns")

    tab = pd.DataFrame(
        {
            SECTOR_COLUMN: delta["sector"].astype(str),
            VALUE_COLUMN: delta["value"],
        }
    )
    frame = build_sector_stack_frame(
        tab,
        group_small_threshold=group_small_threshold,
        max_sectors=max_sectors,
        threshold_unit=threshold_unit,
    )

    if "sector_name" not in delta.columns:
        return frame

    name_map = (
        delta.assign(sector=delta["sector"].astype(str))
        .drop_duplicates("sector")
        .set_index("sector")["sector_name"]
        .to_dict()
    )

    def _label(sector: str) -> str:
        normalized = sector.strip().lower()
        if normalized.startswith(("other increase", "other decrease")):
            return sector
        name = name_map.get(sector)
        if name is None or (isinstance(name, float) and pd.isna(name)):
            return sector
        return f"{sector} {name}"[:label_max_len]

    frame = frame.copy()
    frame["sector"] = frame["sector"].map(_label)
    return frame


def resolve_v05_us_pairs(pair_keys: Sequence[str]) -> tuple[BlyStepPair, ...]:
    """Resolve ordered ``V05_US_BLY_STEP_PAIRS`` keys; raise on unknown names."""
    resolved: list[BlyStepPair] = []
    for key in pair_keys:
        try:
            resolved.append(V05_US_BLY_STEP_PAIRS[key])
        except KeyError as exc:
            known = ", ".join(sorted(V05_US_BLY_STEP_PAIRS))
            raise ValueError(f"unknown BLy step pair {key!r}; known: {known}") from exc
    return tuple(resolved)
