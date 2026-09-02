"""Industry output from the stored nowcast MUTs vs the BEA gross-output series.

Ours: row sums of the newest stored ``Nowcast_Detail_Make_before_redef``
parquet per year - producer prices, the Step 6 driver's product. 2017 is the
anchor year and uses the published before-redefinitions Make.

Theirs, twice: ``derive_gross_output_before_redefinition`` - the published BEA
detail gross-output series the repository currently consumes - and the
EC-adjusted panel the Step 5 balance actually imposed
(``detail_gross_output_panel``). The two comparisons separate deliberate
2022 Economic Census conditioning (#724) from balance drift: a year can sit
2% off the published series while sitting 0.2% from the panel it was asked to
hit, and both numbers are needed to read the table.

Feeds the "industry output vs the BEA series" section of
``progress_report.md``. Run with no arguments to print the table; pass
``--worst YEAR`` to list a year's largest per-industry divergences.
"""

from __future__ import annotations

import argparse
import sys
import typing as ta
from pathlib import Path

import pandas as pd

from bedrock.extract.iot.io_2017 import load_2017_V_before_redef_usa
from bedrock.transform.iot.derived_gross_industry_output import (
    USA_GROSS_INDUSTRY_OUTPUT_YEARS,
    derive_gross_output_before_redefinition,
)
from bedrock.transform.iot.derived_intermediate_and_value_added import (
    detail_gross_output_panel,
)
from bedrock.utils.config.settings import FBS_DIR

_B = 1e9
YEARS = tuple(range(2017, 2024))

#: Divergences below both of these are reported as agreement, not difference.
RELATIVE_FLOOR = 0.01
ABSOLUTE_FLOOR_USD = 100e6


def nowcast_industry_output(year: int, directory: Path | None = None) -> pd.Series:
    """Row sums of the year's stored before-redefinitions Make table. USD."""
    if year == 2017:
        make = load_2017_V_before_redef_usa()
        out = make.sum(axis=1)
        out.index = pd.Index([str(i) for i in make.index])
        return out
    where = Path(directory) if directory is not None else Path(FBS_DIR)
    matches = sorted(
        where.glob(f'Nowcast_Detail_Make_before_redef_{year}_*.parquet'),
        key=lambda p: p.stat().st_mtime,
    )
    if not matches:
        raise FileNotFoundError(
            f'no stored Make table for {year} in {where}; '
            'run bedrock.transform.iot.nowcast_mut first'
        )
    return pd.read_parquet(matches[-1]).sum(axis=1)


def per_industry_differences(year: int) -> pd.DataFrame:
    """Per-industry nowcast vs published BEA, one row per scored industry. USD.

    Industries the published series carries below :data:`ABSOLUTE_FLOOR_USD`
    are left out: a percent difference over a near-zero base is noise, not
    signal.
    """
    ours = nowcast_industry_output(year)
    bea = derive_gross_output_before_redefinition(
        ta.cast(USA_GROSS_INDUSTRY_OUTPUT_YEARS, year)
    )
    bea.index = pd.Index([str(i) for i in bea.index])
    bea = bea.reindex(ours.index).fillna(0.0)
    frame = pd.DataFrame({'nowcast': ours, 'bea': bea})
    frame = frame[frame['bea'].abs() >= ABSOLUTE_FLOOR_USD]
    frame['diff'] = frame['nowcast'] - frame['bea']
    frame['diff_%'] = 100 * frame['diff'] / frame['bea']
    return frame


def comparison_table() -> pd.DataFrame:
    """One row per year: totals, distribution statistics, the panel column."""
    panel = detail_gross_output_panel(ec_adjusted=True) * 1e6  # $M -> USD
    rows = []
    for year in YEARS:
        ours = nowcast_industry_output(year)
        imposed = panel[year].astype(float)
        imposed.index = pd.Index([str(i) for i in imposed.index])
        imposed = imposed.reindex(ours.index).fillna(0.0)

        bea = derive_gross_output_before_redefinition(
            ta.cast(USA_GROSS_INDUSTRY_OUTPUT_YEARS, year)
        )
        bea.index = pd.Index([str(i) for i in bea.index])
        bea = bea.reindex(ours.index).fillna(0.0)

        scored = per_industry_differences(year)
        material = (scored['diff_%'].abs() > 5.0) & (
            scored['diff'].abs() > ABSOLUTE_FLOOR_USD
        )
        worst = scored['diff_%'].abs().idxmax()
        rows.append(
            {
                'year': year,
                'nowcast_$B': ours.sum() / _B,
                'bea_go_$B': bea.sum() / _B,
                'total_diff_%': 100 * (ours.sum() - bea.sum()) / bea.sum(),
                'weighted_diff_%': 100 * (ours - bea).abs().sum() / bea.sum(),
                'median_|diff|_%': scored['diff_%'].abs().median(),
                'max_|diff|_%': scored['diff_%'].abs().max(),
                'max_industry': worst,
                'industries_>5%': int(material.sum()),
                'vs_imposed_%': 100 * (ours - imposed).abs().sum() / imposed.sum(),
            }
        )
    return pd.DataFrame(rows).set_index('year')


def worst_industries(year: int, count: int = 10) -> pd.DataFrame:
    """The year's largest absolute divergences vs the published series."""
    ours = nowcast_industry_output(year)
    bea = derive_gross_output_before_redefinition(
        ta.cast(USA_GROSS_INDUSTRY_OUTPUT_YEARS, year)
    )
    bea.index = pd.Index([str(i) for i in bea.index])
    bea = bea.reindex(ours.index).fillna(0.0)
    diff = ours - bea
    order = diff.abs().sort_values(ascending=False).head(count).index
    return pd.DataFrame(
        {
            'nowcast_$B': ours[order] / _B,
            'bea_go_$B': bea[order] / _B,
            'diff_$B': diff[order] / _B,
            'diff_%': 100 * diff[order] / bea[order].where(bea[order] != 0.0),
        }
    )


#: Figure colors - the repo-standard dataviz reference palette, light mode.
_POS = '#2a78d6'  # nowcast above the published series
_NEG = '#e34948'  # nowcast below
_INK = '#0b0b0b'
_INK_2 = '#52514e'
_MUTED = '#898781'
_GRID = '#e1e0d9'
_SURFACE = '#fcfcfb'


def figure(out_path: Path, dpi: int = 110, bar_year: int = 2023) -> Path:
    """Two panels: the per-industry spread by year, and *bar_year*'s movers.

    Left: every scored industry's percent difference vs the published series,
    one jittered column per year - the shape of the table's distribution
    columns. Right: *bar_year*'s largest absolute divergences as diverging
    bars, dollars on the axis and the percent in the label.
    """
    import matplotlib  # noqa: PLC0415

    matplotlib.use('Agg')
    import matplotlib.pyplot as plt  # noqa: PLC0415
    import numpy as np  # noqa: PLC0415

    fig, (left, right) = plt.subplots(
        1, 2, figsize=(12.6, 5.2), width_ratios=[1.15, 1.0], facecolor=_SURFACE
    )

    rng = np.random.default_rng(11)
    for position, year in enumerate(YEARS):
        scored = per_industry_differences(year)
        x = position + rng.uniform(-0.17, 0.17, len(scored))
        colors = np.where(scored['diff_%'] >= 0, _POS, _NEG)
        left.scatter(x, scored['diff_%'], s=9, c=colors, alpha=0.45, linewidths=0)
    left.axhline(0, color=_MUTED, linewidth=0.8)
    for bound in (5, -5):
        left.axhline(bound, color=_GRID, linewidth=0.8, linestyle=(0, (4, 3)))
    left.text(len(YEARS) - 0.45, 5.4, '±5%', color=_MUTED, fontsize=8, ha='right')
    left.set_xticks(range(len(YEARS)), [str(y) for y in YEARS])
    left.set_ylabel('difference vs published BEA GO, % of industry', color=_INK_2)

    movers = worst_industries(bar_year, count=14).iloc[::-1]
    bar_colors = [_POS if v >= 0 else _NEG for v in movers['diff_$B']]
    right.barh(movers.index, movers['diff_$B'], color=bar_colors, height=0.62)
    right.axvline(0, color=_MUTED, linewidth=0.8)
    span = float(movers['diff_$B'].abs().max())
    right.set_xlim(-1.35 * span, 1.35 * span)
    for code, row in movers.iterrows():
        inside = row['diff_$B'] >= 0
        right.text(
            row['diff_$B'] + (1.2 if inside else -1.2),
            code,
            f'{row["diff_%"]:+.1f}%',
            va='center',
            ha='left' if inside else 'right',
            color=_INK_2,
            fontsize=8,
        )
    right.set_xlabel(f'difference vs published BEA GO, $B ({bar_year})', color=_INK_2)

    for axis in (left, right):
        axis.set_facecolor(_SURFACE)
        for side in ('top', 'right'):
            axis.spines[side].set_visible(False)
        for side in ('left', 'bottom'):
            axis.spines[side].set_color(_GRID)
        axis.tick_params(colors=_MUTED, labelsize=9)
        axis.grid(axis='y' if axis is left else 'x', color=_GRID, linewidth=0.6)
        axis.set_axisbelow(True)
    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=dpi, facecolor=_SURFACE)
    plt.close(fig)
    return out_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        '--worst',
        type=int,
        metavar='YEAR',
        help='also list this year\'s largest per-industry divergences',
    )
    parser.add_argument(
        '--figure',
        action='store_true',
        help='write images/go_vs_nowcast_mut.png (the progress-report figure)',
    )
    parser.add_argument('--dpi', type=int, default=110)
    args = parser.parse_args(argv)

    pd.set_option('display.width', 160)
    print(comparison_table().round(2).to_string())
    if args.worst is not None:
        print(f'\nlargest divergences, {args.worst}:')
        print(worst_industries(args.worst).round(1).to_string())
    if args.figure:
        out = figure(
            Path(__file__).parent / 'images' / 'go_vs_nowcast_mut.png',
            dpi=args.dpi,
        )
        print(f'wrote {out}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
