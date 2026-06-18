"""Overlay per-sector N/D % diff distributions from multiple diagnostics Sheets.

Each input Sheet contributes one semi-transparent histogram series per EF kind,
so two (or more) model versions can be compared on a single axis — e.g. v0.2 vs
v0.3, each measured as ``% diff vs the CEDA v0 baseline``. Percent-diff columns
are normalized across the two storage conventions seen in diagnostics Sheets
(percent-strings like ``"-6.86%"`` vs bare fractions like ``"0.0155"``), so runs
written at different times overlay correctly.

This lives in the validation analysis package (not under a-matrix analysis) so it
is reusable for general validation/visualization. It reads the same
``N_and_diffs`` / ``D_and_diffs`` tabs that ``generate_diagnostics`` writes.

Usage:
    python -m bedrock.utils.validation.analysis.overlay_ef_hist \\
        --series "v0.2=1pCSgLD14lmrQg3OtfHvnK4lQrFtqiavrjCt-C3R_bSw" \\
        --series "v0.3=1_DMZHI-vhHFrk7Bvd6Bx8Wmc5-3K3IQa3ylLN4tEHb0" \\
        --out-dir bedrock/utils/validation/v0.3/output
"""

from __future__ import annotations

import logging
from pathlib import Path

import click
import matplotlib.pyplot as plt
import pandas as pd

from bedrock.utils.io.gcp import read_sheet_tab

from .plotting import (
    normalize_pct_diff_to_percent,
    overlay_pct_diff_histogram,
    save_and_close,
    setup_mpl,
)

logger = logging.getLogger(__name__)

EF_KIND_LABEL = {'N': 'total EF (N)', 'D': 'direct EF (D)'}


def _series_for_kind(sheet_id: str, ef_kind: str) -> 'pd.Series[float]':
    """Return the per-sector pct-diff series (in percent) for one Sheet/kind."""
    tab = f'{ef_kind}_and_diffs'
    col = f'{ef_kind}_perc_diff'
    df = read_sheet_tab(sheet_id, tab)
    if col not in df.columns:
        raise KeyError(f'{tab!r} in {sheet_id} has no {col!r}; got {list(df.columns)}')
    return normalize_pct_diff_to_percent(df[col]).dropna()


def _parse_series(series: tuple[str, ...]) -> dict[str, str]:
    """Parse ``label=sheet_id`` pairs into an ordered mapping."""
    out: dict[str, str] = {}
    for item in series:
        if '=' not in item:
            raise click.BadParameter(f"--series must be 'label=sheet_id', got {item!r}")
        label, sheet_id = item.split('=', 1)
        out[label.strip()] = sheet_id.strip()
    return out


@click.command()
@click.option(
    '--series',
    multiple=True,
    required=True,
    help="Repeatable 'label=sheet_id' pair, one per model version to overlay.",
)
@click.option(
    '--out-dir',
    type=click.Path(file_okay=False, path_type=Path),
    required=True,
    help='Directory to write ef_overlay_hist_{N,D}.png into.',
)
@click.option('--title', default=None, help='Optional title prefix.')
def main(series: tuple[str, ...], out_dir: Path, title: str | None) -> None:
    setup_mpl(font_size=14)
    label_to_sheet = _parse_series(series)
    out_dir.mkdir(parents=True, exist_ok=True)

    for ef_kind in ('N', 'D'):
        series_by_label = {}
        for label, sheet_id in label_to_sheet.items():
            try:
                series_by_label[label] = _series_for_kind(sheet_id, ef_kind)
            except Exception as e:  # noqa: BLE001
                logger.warning('Skipping %s for %s: %s', label, ef_kind, e)
        if not series_by_label:
            logger.warning('No data for %s; skipping.', ef_kind)
            continue
        fig, ax = plt.subplots(figsize=(11, 6.5))
        kind_label = EF_KIND_LABEL[ef_kind]
        versions = ' vs '.join(series_by_label)
        suptitle = (
            f'{title + " — " if title else ""}{kind_label} per-sector % diff '
            f'vs CEDA v0 — {versions}'
        )
        overlay_pct_diff_histogram(
            ax,
            series_by_label,
            xlabel='EF change vs CEDA v0 baseline (%)',
            title=suptitle,
        )
        fig.tight_layout()
        out = out_dir / f'ef_overlay_hist_{ef_kind}.png'
        save_and_close(fig, out)
        print(f'Wrote: {out}')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    main()
