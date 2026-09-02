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
from pathlib import Path

import pandas as pd

from bedrock.extract.iot.io_2017 import load_2017_V_before_redef_usa
from bedrock.transform.iot.derived_gross_industry_output import (
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


def comparison_table() -> pd.DataFrame:
    """One row per year: totals, weighted difference, and the panel column."""
    panel = detail_gross_output_panel(ec_adjusted=True) * 1e6  # $M -> USD
    rows = []
    for year in YEARS:
        ours = nowcast_industry_output(year)
        bea = derive_gross_output_before_redefinition(year)
        bea.index = pd.Index([str(i) for i in bea.index])
        bea = bea.reindex(ours.index).fillna(0.0)
        imposed = panel[year].astype(float)
        imposed.index = pd.Index([str(i) for i in imposed.index])
        imposed = imposed.reindex(ours.index).fillna(0.0)

        diff = ours - bea
        relative = diff.abs() / bea.where(bea != 0.0)
        material = (relative > 0.05) & (diff.abs() > ABSOLUTE_FLOOR_USD)
        rows.append(
            {
                'year': year,
                'nowcast_$B': ours.sum() / _B,
                'bea_go_$B': bea.sum() / _B,
                'total_diff_%': 100 * (ours.sum() - bea.sum()) / bea.sum(),
                'weighted_diff_%': 100 * diff.abs().sum() / bea.sum(),
                'industries_>5%': int(material.sum()),
                'vs_imposed_%': 100 * (ours - imposed).abs().sum() / imposed.sum(),
            }
        )
    return pd.DataFrame(rows).set_index('year')


def worst_industries(year: int, count: int = 10) -> pd.DataFrame:
    """The year's largest absolute divergences vs the published series."""
    ours = nowcast_industry_output(year)
    bea = derive_gross_output_before_redefinition(year)
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        '--worst',
        type=int,
        metavar='YEAR',
        help='also list this year\'s largest per-industry divergences',
    )
    args = parser.parse_args(argv)

    pd.set_option('display.width', 140)
    print(comparison_table().round(2).to_string())
    if args.worst is not None:
        print(f'\nlargest divergences, {args.worst}:')
        print(worst_industries(args.worst).round(1).to_string())
    return 0


if __name__ == '__main__':
    sys.exit(main())
