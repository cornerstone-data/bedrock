#!/usr/bin/env python3
"""Plot a single stacked bar of sector changes from a BLy diagnostics CSV.

The script reads the diagnostics sector column (`index`) and the BLy delta
column (`BLy_new - BLy_old (MtCO2e)`), then applies two preprocessing steps
before plotting:
  1. If both `562000` and disaggregated waste sectors are present, combine them
     into a single `562000` value.
  2. Group sectors with small absolute changes into `Other Increase` or
     `Other Decrease`.

Positive values are stacked upward from zero and negative values are stacked
downward from zero. Within each side, the largest-magnitude values are placed
closest to zero so segment sizes decrease away from the center line.

Edit the script-level settings below, then run:
  python bedrock/pipeline/visualization/plot_sector_stacked_net_change.py
"""

from __future__ import annotations

import pathlib

import matplotlib.pyplot as plt
import pandas as pd

from bedrock.utils.taxonomy.cornerstone.commodities import WASTE_DISAGG_COMMODITIES

# -----------------------------
# User settings
# -----------------------------
# TODO: Instead of reading a local CSV, load directly from Google Drive based on
# a diagnostics run and read the `BLy_new_vs_BLy_old` tab.
CSV_PATH = pathlib.Path("scratch/BLy_diff_v02.csv")
OUTPUT_PATH: pathlib.Path | None = None
SECTOR_COLUMN = "index"
VALUE_COLUMN = "BLy_new - BLy_old (MtCO2e)"
WASTE_AGGREGATE_SECTOR = "562000"
GROUP_SMALL_CHANGES_THRESHOLD = 3.0
TITLE = "Sector contributions to net change"
YLABEL = "Gross change (MMT CO2e)"
FIGSIZE = (5.0, 7.0)
BAR_WIDTH = 0.8
LABEL_FONT_SIZE = 7
NET_MARKER_SIZE = 80
CALLOUT_LINE_COLOR = "0.6"
CALLOUT_LINE_WIDTH = 0.7
CALLOUT_X_OFFSETS = (0.8, 1.0, 1.2)


def load_sector_values(csv_path: pathlib.Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    missing_columns = [
        column for column in [SECTOR_COLUMN, VALUE_COLUMN] if column not in df.columns
    ]
    if missing_columns:
        raise ValueError(f"Missing required columns in {csv_path}: {missing_columns}")

    df = df[[SECTOR_COLUMN, VALUE_COLUMN]].copy()
    df.columns = ["sector", "value"]
    df["sector"] = df["sector"].astype(str)
    df["value"] = pd.to_numeric(df["value"], errors="raise")
    df = combine_waste_diffs(df, aggregate_sector=WASTE_AGGREGATE_SECTOR)
    df = group_small_changes(df, threshold=GROUP_SMALL_CHANGES_THRESHOLD)
    return df


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

    grouped_rows: list[dict[str, str | float]] = []
    if other_increase != 0:
        grouped_rows.append({"sector": "Other Increase", "value": other_increase})
    if other_decrease != 0:
        grouped_rows.append({"sector": "Other Decrease", "value": other_decrease})

    if not grouped_rows:
        return large_df

    grouped_df = pd.DataFrame(grouped_rows)
    return pd.concat([large_df, grouped_df], ignore_index=True)


def order_stack(df: pd.DataFrame, *, positive: bool) -> pd.DataFrame:
    ordered = df.copy()
    sector_normalized = ordered["sector"].str.strip().str.lower()

    if positive:
        ordered["_is_other_end"] = sector_normalized.eq("other increase")
        ordered = ordered.sort_values(
            by=["_is_other_end", "value"],
            ascending=[True, False],
        )
    else:
        ordered["_is_other_end"] = sector_normalized.eq("other decrease")
        ordered = ordered.sort_values(
            by=["_is_other_end", "value"],
            ascending=[True, True],
        )

    return ordered.drop(columns="_is_other_end")


def plot_stacked_net_change(
    df: pd.DataFrame,
    *,
    title: str,
    ylabel: str,
    figsize: tuple[float, float],
) -> tuple[plt.Figure, plt.Axes]:
    pos = order_stack(df[df["value"] > 0], positive=True)
    neg = order_stack(df[df["value"] < 0], positive=False)
    label_side_index = 0

    fig, ax = plt.subplots(figsize=figsize)

    pos_bottom = 0.0
    neg_bottom = 0.0
    total_span = max(pos["value"].sum() - neg["value"].sum(), 1.0)
    inside_label_threshold = total_span * 0.08

    def add_segment_label(*, sector: str, value: float, bottom: float) -> None:
        nonlocal label_side_index
        center_y = bottom + value / 2
        label = sector
        sector_normalized = sector.strip().lower()

        if sector_normalized in {"other increase", "other decrease"}:
            va = "top" if value > 0 else "bottom"
            y_text = (
                bottom + value - 0.02 * total_span
                if value > 0
                else bottom + value + 0.02 * total_span
            )
            ax.text(
                0,
                y_text,
                label,
                ha="center",
                va=va,
                fontsize=LABEL_FONT_SIZE,
            )
            return

        if abs(value) >= inside_label_threshold:
            ax.text(
                0,
                center_y,
                label,
                ha="center",
                va="center",
                fontsize=LABEL_FONT_SIZE,
            )
            return

        is_right_side = label_side_index % 2 == 0
        x_offset = CALLOUT_X_OFFSETS[(label_side_index // 2) % len(CALLOUT_X_OFFSETS)]
        x_text = x_offset if is_right_side else -x_offset
        ha = "left" if is_right_side else "right"
        label_side_index += 1
        ax.annotate(
            label,
            xy=(0, center_y),
            xytext=(x_text, center_y),
            ha=ha,
            va="center",
            fontsize=LABEL_FONT_SIZE,
            arrowprops={
                "arrowstyle": "-",
                "lw": CALLOUT_LINE_WIDTH,
                "color": CALLOUT_LINE_COLOR,
                "shrinkA": 0,
                "shrinkB": 0,
            },
        )

    for _, row in pos.iterrows():
        ax.bar(
            0,
            row["value"],
            bottom=pos_bottom,
            width=BAR_WIDTH,
        )
        add_segment_label(sector=row["sector"], value=row["value"], bottom=pos_bottom)
        pos_bottom += row["value"]

    for _, row in neg.iterrows():
        ax.bar(
            0,
            row["value"],
            bottom=neg_bottom,
            width=BAR_WIDTH,
        )
        add_segment_label(sector=row["sector"], value=row["value"], bottom=neg_bottom)
        neg_bottom += row["value"]

    net_total = df["value"].sum()
    ax.scatter(
        0,
        net_total,
        s=NET_MARKER_SIZE,
        color="black",
        zorder=5,
    )
    ax.annotate(
        "Net total",
        xy=(0, net_total),
        xytext=(0.55, net_total),
        ha="left",
        va="center",
        fontsize=LABEL_FONT_SIZE,
        arrowprops={
            "arrowstyle": "-",
            "lw": CALLOUT_LINE_WIDTH,
            "color": CALLOUT_LINE_COLOR,
            "shrinkA": 0,
            "shrinkB": 0,
        },
    )

    ax.axhline(0, color="black", linewidth=1)
    ax.set_xticks([0])
    ax.set_xticklabels([f"Net change = {net_total:.3g}"])
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    max_callout_x = max(CALLOUT_X_OFFSETS, default=1.2)
    ax.set_xlim(-(max_callout_x + 0.4), max_callout_x + 0.4)
    fig.tight_layout()
    return fig, ax


def main() -> None:
    df = load_sector_values(CSV_PATH)
    fig, _ = plot_stacked_net_change(
        df,
        title=TITLE,
        ylabel=YLABEL,
        figsize=FIGSIZE,
    )

    if OUTPUT_PATH is not None:
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(OUTPUT_PATH, dpi=300, bbox_inches="tight")
        print(f"Saved figure to: {OUTPUT_PATH}")

    plt.show()


if __name__ == "__main__":
    main()
