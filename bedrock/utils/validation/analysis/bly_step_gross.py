"""Cross-sheet step / span gross-change stacked bars for waterfall rungs.

Two metrics (``--metric``):

- ``bly`` — live ``BLy_new`` pathway totals (MMT). Redistributions that cancel in
  weighted-average N show as opposing sector MMT moves.
- ``weighted_n`` — sector contributions ``(ΔN · q) / Σq`` (kg/USD) using canonical
  v0.5 ``scaled_q_USA``. Stack **net equals** the q-weighted AVG N waterfall step.

Layouts:
  - ``steps`` — one stacked bar per ``--pair`` (shared y-scale)
  - ``combined`` — single bar for first.before → last.after
  - ``both`` — steps panel PNG and combined PNG (default)

Usage:
    uv run python -m bedrock.utils.validation.analysis.bly_step_gross
    uv run python -m bedrock.utils.validation.analysis.bly_step_gross \\
        --metric weighted_n --pair nowcast --pair facility --layout both
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import click
import matplotlib.pyplot as plt
import pandas as pd

from bedrock.utils.validation.analysis.bly_plots import (
    DEFAULT_GROUP_SMALL_THRESHOLD,
    DEFAULT_V05_US_PAIR_KEYS,
    DEFAULT_WEIGHTED_N_GROUP_SMALL_THRESHOLD,
    V05_US_BLY_STEP_PAIRS,
    BlyStepPair,
    bly_span_delta,
    bly_step_delta,
    resolve_v05_us_pairs,
    stack_frame_from_delta,
    weighted_n_span_delta,
    weighted_n_step_delta,
)
from bedrock.utils.validation.analysis.diagnostics_plots import bly_figsize
from bedrock.utils.validation.analysis.plotting import (
    plot_stacked_net_change,
    save_and_close,
    setup_mpl,
)

logger = logging.getLogger(__name__)

Layout = Literal["steps", "combined", "both"]
Metric = Literal["bly", "weighted_n", "both"]

DEFAULT_OUT_DIR = Path(__file__).resolve().parent / "output" / "bly_step_gross"
# Multi-panel callouts need a named-sector cap; in-sheet BLy default is uncapped.
DEFAULT_STEP_MAX_SECTORS = 12

DeltaFn = Callable[..., pd.DataFrame]


@dataclass(frozen=True)
class MetricSpec:
    key: str
    title_stamp: str
    ylabel: str
    net_label_unit: str
    threshold_unit: str
    default_group_small_threshold: float
    file_prefix: str
    csv_value_cols: tuple[str, ...]
    step_delta: DeltaFn
    span_delta: DeltaFn


METRIC_SPECS: dict[str, MetricSpec] = {
    "bly": MetricSpec(
        key="bly",
        title_stamp="[v0.5 US · BLy step gross]",
        ylabel="Gross change (MMT CO2e)",
        net_label_unit="MMT CO2e",
        threshold_unit="MMT",
        default_group_small_threshold=DEFAULT_GROUP_SMALL_THRESHOLD,
        file_prefix="bly_step_gross",
        csv_value_cols=("sector", "sector_name", "bly_before", "bly_after", "value"),
        step_delta=bly_step_delta,
        span_delta=bly_span_delta,
    ),
    "weighted_n": MetricSpec(
        key="weighted_n",
        title_stamp="[v0.5 US · weighted-N step gross]",
        ylabel="Contribution to weighted-avg N (kg/USD)",
        net_label_unit="kg/USD",
        threshold_unit="kg/USD",
        default_group_small_threshold=DEFAULT_WEIGHTED_N_GROUP_SMALL_THRESHOLD,
        file_prefix="weighted_n_step_gross",
        csv_value_cols=(
            "sector",
            "sector_name",
            "n_before",
            "n_after",
            "q",
            "value",
        ),
        step_delta=weighted_n_step_delta,
        span_delta=weighted_n_span_delta,
    ),
}


def _pair_keys_slug(pairs: tuple[BlyStepPair, ...]) -> str:
    return "_".join(p.key for p in pairs)


def _step_title(spec: MetricSpec, pair: BlyStepPair) -> str:
    return f"{spec.title_stamp}\n{pair.label}"


def _combined_title(spec: MetricSpec, pairs: tuple[BlyStepPair, ...]) -> str:
    keys = " + ".join(p.key for p in pairs)
    start = pairs[0].before.step_label
    end = pairs[-1].after.step_label
    return f"{spec.title_stamp}\nCombined ({keys})\n{start} → {end}"


def _write_delta_csv(delta: pd.DataFrame, path: Path, cols: tuple[str, ...]) -> None:
    keep = [c for c in cols if c in delta.columns]
    (
        delta[keep]
        .sort_values("value", key=lambda s: s.abs(), ascending=False)
        .to_csv(path, index=False)
    )
    print(f"Wrote: {path}")


def plot_steps_panel(
    pairs: tuple[BlyStepPair, ...],
    *,
    spec: MetricSpec,
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
    for ax, pair in zip(axes_list, pairs, strict=True):
        delta = spec.step_delta(
            pair.before.sheet_id, pair.after.sheet_id, refresh=refresh
        )
        if write_csv:
            _write_delta_csv(
                delta,
                out_path.parent / f"{spec.file_prefix}_{pair.key}_deltas.csv",
                spec.csv_value_cols,
            )
        frame = stack_frame_from_delta(
            delta,
            group_small_threshold=group_small_threshold,
            max_sectors=max_sectors,
            threshold_unit=spec.threshold_unit,
        )
        plot_stacked_net_change(
            ax,
            frame,
            title=_step_title(spec, pair),
            ylabel=spec.ylabel,
            net_label_unit=spec.net_label_unit,
        )

    y0 = min(ax.get_ylim()[0] for ax in axes_list)
    y1 = max(ax.get_ylim()[1] for ax in axes_list)
    for ax in axes_list:
        ax.set_ylim(y0, y1)

    save_and_close(fig, out_path)


def plot_combined(
    pairs: tuple[BlyStepPair, ...],
    *,
    spec: MetricSpec,
    out_path: Path,
    group_small_threshold: float,
    max_sectors: int,
    refresh: bool,
    write_csv: bool,
) -> None:
    delta = spec.span_delta(pairs, refresh=refresh)
    if write_csv:
        slug = _pair_keys_slug(pairs)
        _write_delta_csv(
            delta,
            out_path.parent / f"{spec.file_prefix}_{slug}_combined_deltas.csv",
            spec.csv_value_cols,
        )
    frame = stack_frame_from_delta(
        delta,
        group_small_threshold=group_small_threshold,
        max_sectors=max_sectors,
        threshold_unit=spec.threshold_unit,
    )
    _, panel_h = bly_figsize(max_sectors)
    fig, ax = plt.subplots(figsize=(max(bly_figsize(max_sectors)[0], 7.5), panel_h))
    plot_stacked_net_change(
        ax,
        frame,
        title=_combined_title(spec, pairs),
        ylabel=spec.ylabel,
        net_label_unit=spec.net_label_unit,
    )
    fig.tight_layout()
    save_and_close(fig, out_path)


def _emit_metric(
    pairs: tuple[BlyStepPair, ...],
    *,
    spec: MetricSpec,
    layout: str,
    out_dir: Path,
    group_small_threshold: float | None,
    max_sectors: int,
    refresh: bool,
    write_csv: bool,
) -> None:
    threshold = (
        group_small_threshold
        if group_small_threshold is not None
        else spec.default_group_small_threshold
    )
    slug = _pair_keys_slug(pairs)
    if layout in ("steps", "both"):
        plot_steps_panel(
            pairs,
            spec=spec,
            out_path=out_dir / f"{spec.file_prefix}_v05_us_{slug}_steps_panel.png",
            group_small_threshold=threshold,
            max_sectors=max_sectors,
            refresh=refresh,
            write_csv=write_csv,
        )
    if layout in ("combined", "both"):
        plot_combined(
            pairs,
            spec=spec,
            out_path=out_dir / f"{spec.file_prefix}_v05_us_{slug}_combined.png",
            group_small_threshold=threshold,
            max_sectors=max_sectors,
            refresh=refresh,
            write_csv=write_csv,
        )


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
    "--metric",
    type=click.Choice(["bly", "weighted_n", "both"], case_sensitive=False),
    default="bly",
    show_default=True,
    help="bly = pathway BLy MMT; weighted_n = (ΔN·q)/Σq kg/USD (waterfall-aligned).",
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
    default=None,
    help=(
        "Roll sectors with |Δ| below this into Other buckets. "
        "Default: 3.0 (MMT) for bly, 1e-4 (kg/USD) for weighted_n."
    ),
)
def main(
    ladder: str,
    pair_keys: tuple[str, ...],
    metric: str,
    layout: str,
    out_dir: Path,
    refresh: bool,
    write_csv: bool,
    bly_max_sectors: int,
    bly_group_small_threshold: float | None,
) -> None:
    del ladder  # only v05_us registered today; kept for forward-compatible CLI
    setup_mpl()
    out_dir.mkdir(parents=True, exist_ok=True)

    keys = pair_keys or DEFAULT_V05_US_PAIR_KEYS
    pairs = resolve_v05_us_pairs(keys)
    layout_norm = layout.lower()
    metric_norm = metric.lower()
    metric_keys = ("bly", "weighted_n") if metric_norm == "both" else (metric_norm,)

    for key in metric_keys:
        _emit_metric(
            pairs,
            spec=METRIC_SPECS[key],
            layout=layout_norm,
            out_dir=out_dir,
            group_small_threshold=bly_group_small_threshold,
            max_sectors=bly_max_sectors,
            refresh=refresh,
            write_csv=write_csv,
        )


if __name__ == "__main__":
    main()
