"""Cross-sheet BLy step / span gross-change stacked bars.

Compares live ``BLy_new`` across waterfall diagnostics sheets so sector
redistributions that cancel in weighted-average N are visible as gross MMT
moves. BLy is pathway total (``diag(d) @ L @ y``), not inventory E / direct D.

Layouts:
  - ``steps`` — one stacked bar per ``--pair`` (shared y-scale)
  - ``combined`` — single bar for first.before → last.after
  - ``both`` — steps panel PNG and combined PNG (default)

Usage:
    uv run python -m bedrock.utils.validation.analysis.bly_step_gross
    uv run python -m bedrock.utils.validation.analysis.bly_step_gross \\
        --ladder v05_us --pair nowcast --pair facility --layout both
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Literal

import click
import matplotlib.pyplot as plt
import pandas as pd

from bedrock.utils.validation.analysis.bly_plots import (
    DEFAULT_GROUP_SMALL_THRESHOLD,
    DEFAULT_V05_US_PAIR_KEYS,
    V05_US_BLY_STEP_PAIRS,
    BlyStepPair,
    bly_span_delta,
    bly_step_delta,
    resolve_v05_us_pairs,
    stack_frame_from_delta,
)
from bedrock.utils.validation.analysis.diagnostics_plots import bly_figsize
from bedrock.utils.validation.analysis.plotting import (
    plot_stacked_net_change,
    save_and_close,
    setup_mpl,
)

logger = logging.getLogger(__name__)

Layout = Literal["steps", "combined", "both"]

DEFAULT_OUT_DIR = Path(__file__).resolve().parent / "output" / "bly_step_gross"
# Multi-panel callouts need a named-sector cap; in-sheet BLy default is uncapped.
DEFAULT_STEP_MAX_SECTORS = 12
TITLE_STAMP = "[v0.5 US · BLy step gross]"


def _pair_keys_slug(pairs: tuple[BlyStepPair, ...]) -> str:
    return "_".join(p.key for p in pairs)


def _step_title(pair: BlyStepPair) -> str:
    return f"{TITLE_STAMP}\n{pair.label}"


def _combined_title(pairs: tuple[BlyStepPair, ...]) -> str:
    keys = " + ".join(p.key for p in pairs)
    start = pairs[0].before.step_label
    end = pairs[-1].after.step_label
    return f"{TITLE_STAMP}\nCombined ({keys})\n{start} → {end}"


def _write_delta_csv(delta: pd.DataFrame, path: Path) -> None:
    cols = [
        c
        for c in ("sector", "sector_name", "bly_before", "bly_after", "value")
        if c in delta.columns
    ]
    (
        delta[cols]
        .sort_values("value", key=lambda s: s.abs(), ascending=False)
        .to_csv(path, index=False)
    )
    print(f"Wrote: {path}")


def plot_steps_panel(
    pairs: tuple[BlyStepPair, ...],
    *,
    out_path: Path,
    group_small_threshold: float,
    max_sectors: int,
    refresh: bool,
    write_csv: bool,
) -> None:
    n = len(pairs)
    panel_w, panel_h = bly_figsize(max_sectors)
    fig, axes = plt.subplots(
        1,
        n,
        figsize=(n * max(panel_w, 7.5), panel_h),
        squeeze=False,
        layout="constrained",
    )
    axes_list = list(axes[0])
    frames = []
    for ax, pair in zip(axes_list, pairs, strict=True):
        delta = bly_step_delta(
            pair.before.sheet_id, pair.after.sheet_id, refresh=refresh
        )
        if write_csv:
            _write_delta_csv(
                delta, out_path.parent / f"bly_step_gross_{pair.key}_deltas.csv"
            )
        frame = stack_frame_from_delta(
            delta,
            group_small_threshold=group_small_threshold,
            max_sectors=max_sectors,
        )
        frames.append(frame)
        plot_stacked_net_change(
            ax,
            frame,
            title=_step_title(pair),
            ylabel="Gross change (MMT CO2e)",
        )

    y0 = min(ax.get_ylim()[0] for ax in axes_list)
    y1 = max(ax.get_ylim()[1] for ax in axes_list)
    for ax in axes_list:
        ax.set_ylim(y0, y1)

    save_and_close(fig, out_path)


def plot_combined(
    pairs: tuple[BlyStepPair, ...],
    *,
    out_path: Path,
    group_small_threshold: float,
    max_sectors: int,
    refresh: bool,
    write_csv: bool,
) -> None:
    delta = bly_span_delta(pairs, refresh=refresh)
    if write_csv:
        slug = _pair_keys_slug(pairs)
        _write_delta_csv(
            delta, out_path.parent / f"bly_step_gross_{slug}_combined_deltas.csv"
        )
    frame = stack_frame_from_delta(
        delta,
        group_small_threshold=group_small_threshold,
        max_sectors=max_sectors,
    )
    _, panel_h = bly_figsize(max_sectors)
    fig, ax = plt.subplots(figsize=(max(bly_figsize(max_sectors)[0], 7.5), panel_h))
    plot_stacked_net_change(
        ax,
        frame,
        title=_combined_title(pairs),
        ylabel="Gross change (MMT CO2e)",
    )
    fig.tight_layout()
    save_and_close(fig, out_path)


@click.command()
@click.option(
    "--ladder",
    type=click.Choice(["v05_us"], case_sensitive=False),
    default="v05_us",
    show_default=True,
    help="Waterfall sheet registry to draw pair endpoints from.",
)
@click.option(
    "--pair",
    "pair_keys",
    multiple=True,
    type=click.Choice(sorted(V05_US_BLY_STEP_PAIRS), case_sensitive=False),
    help=(
        "Repeatable step pair key. Default for v05_us: "
        + ", ".join(DEFAULT_V05_US_PAIR_KEYS)
    ),
)
@click.option(
    "--layout",
    type=click.Choice(["steps", "combined", "both"], case_sensitive=False),
    default="both",
    show_default=True,
)
@click.option(
    "--out-dir",
    type=click.Path(file_okay=False, path_type=Path),
    default=DEFAULT_OUT_DIR,
    show_default=True,
)
@click.option(
    "--refresh", is_flag=True, help="Re-fetch sheet tabs (skip parquet cache)."
)
@click.option(
    "--write-csv/--no-write-csv",
    default=True,
    show_default=True,
    help="Write per-pair / combined sector Δ CSVs next to the PNGs.",
)
@click.option(
    "--bly-max-sectors",
    type=int,
    default=DEFAULT_STEP_MAX_SECTORS,
    show_default=True,
    help=(
        "Keep at most this many named sectors (top-|Δ|); "
        "roll the rest into Other Increase / Other Decrease. Use 0 for no cap."
    ),
)
@click.option(
    "--bly-group-small-threshold",
    type=float,
    default=DEFAULT_GROUP_SMALL_THRESHOLD,
    show_default=True,
    help=(
        "Roll sectors with |Δ Mt CO2e| below this into "
        "Other Increase / Other Decrease. Use 0 to show every sector."
    ),
)
def main(
    ladder: str,
    pair_keys: tuple[str, ...],
    layout: str,
    out_dir: Path,
    refresh: bool,
    write_csv: bool,
    bly_max_sectors: int,
    bly_group_small_threshold: float,
) -> None:
    del ladder  # only v05_us registered today; kept for forward-compatible CLI
    setup_mpl()
    out_dir.mkdir(parents=True, exist_ok=True)

    keys = pair_keys or DEFAULT_V05_US_PAIR_KEYS
    pairs = resolve_v05_us_pairs(keys)
    slug = _pair_keys_slug(pairs)
    layout_norm = layout.lower()

    if layout_norm in ("steps", "both"):
        plot_steps_panel(
            pairs,
            out_path=out_dir / f"bly_step_gross_v05_us_{slug}_steps_panel.png",
            group_small_threshold=bly_group_small_threshold,
            max_sectors=bly_max_sectors,
            refresh=refresh,
            write_csv=write_csv,
        )
    if layout_norm in ("combined", "both"):
        plot_combined(
            pairs,
            out_path=out_dir / f"bly_step_gross_v05_us_{slug}_combined.png",
            group_small_threshold=bly_group_small_threshold,
            max_sectors=bly_max_sectors,
            refresh=refresh,
            write_csv=write_csv,
        )


if __name__ == "__main__":
    main()
