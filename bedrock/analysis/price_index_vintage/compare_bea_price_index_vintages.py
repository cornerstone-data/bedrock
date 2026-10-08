"""How far the derived industry price index moves from BEA's 2025Q2 to 2026Q2 release.

Both vintages go through :func:`derive_industry_price_index`, so the comparison
is of what bedrock would actually consume, not of the raw sheets. Two things
change between them:

- **Revisions** (2012-2024): BEA's annual update restates published detail.
- **2025 re-sourcing**: under 2025Q2 the 2025 column is the mean of 2025Q1-Q2
  from the *summary* quarterly table mapped down to detail; under 2026Q2 it is
  published *detail* annual. This is a change of source, not just a revision.

Revisions are reported as ``new / old - 1``. The aggregate is weighted by
2024 detail gross output (UGO305-A, 2026Q2) so large industries count more.
Because inflation factors are ratios, the last table also reports the change
in the 2017 -> year factor, which is what the nowcast price carry uses.

Usage::

    uv run python -m bedrock.analysis.price_index_vintage.compare_bea_price_index_vintages

Outputs (under ``output/``):
  revision_by_sector_year.csv   sector x year revision, both vintages' levels
  revision_summary_by_year.csv  distribution of revisions per year
  factor_change_from_2017.csv   change in PI[year]/PI[2017] per sector
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from bedrock.extract.iot.gdp import load_go_detail
from bedrock.transform.iot.derived_price_index import derive_industry_price_index
from bedrock.transform.iot.helpers import map_detail_table

OLD, NEW = '2025Q2', '2026Q2'
FACTOR_BASE_YEAR = 2017
WEIGHT_YEAR = 2024
OUT_DIR = Path(__file__).parent / 'output'


def _go_weights() -> pd.Series:
    """2024 gross output by detail code; duplicated codes summed."""
    go = map_detail_table(load_go_detail(NEW))
    return go.groupby('sector_code')[str(WEIGHT_YEAR)].sum().astype(float)


def _summarise(revision: pd.DataFrame, weights: pd.Series) -> pd.DataFrame:
    w = weights.reindex(revision.index).fillna(0.0)
    rows = []
    for year in revision.columns:
        r = revision[year]
        rows.append(
            {
                'year': year,
                'go_weighted_mean_pct': 100 * float(np.average(r, weights=w)),
                'median_pct': 100 * r.median(),
                'p10_pct': 100 * r.quantile(0.1),
                'p90_pct': 100 * r.quantile(0.9),
                'max_abs_pct': 100 * r.abs().max(),
                'share_abs_gt_1pct': (r.abs() > 0.01).mean(),
                'share_abs_gt_5pct': (r.abs() > 0.05).mean(),
            }
        )
    return pd.DataFrame(rows).set_index('year')


def main() -> None:
    old = derive_industry_price_index(OLD)
    new = derive_industry_price_index(NEW)
    years = [y for y in old.columns if y in new.columns]
    old, new = old[years].astype(float), new[years].astype(float)
    weights = _go_weights()

    revision = new / old - 1
    summary = _summarise(revision, weights)

    factor_years = [y for y in years if y > FACTOR_BASE_YEAR]
    factor_old = old[factor_years].div(old[FACTOR_BASE_YEAR], axis=0)
    factor_new = new[factor_years].div(new[FACTOR_BASE_YEAR], axis=0)
    factor_change = factor_new / factor_old - 1
    factor_summary = _summarise(factor_change, weights)

    OUT_DIR.mkdir(exist_ok=True)
    long = (
        pd.concat(
            {
                'pi_old': old.stack(),
                'pi_new': new.stack(),
                'revision': revision.stack(),
            },
            axis=1,
        )
        .rename_axis(['sector_code', 'year'])
        .reset_index()
    )
    long.to_csv(OUT_DIR / 'revision_by_sector_year.csv', index=False)
    summary.to_csv(OUT_DIR / 'revision_summary_by_year.csv')
    factor_change.to_csv(OUT_DIR / 'factor_change_from_2017.csv')

    pd.set_option('display.width', 160, 'display.precision', 2)
    print(f'\nPrice index level revision, {NEW} vs {OLD} ({len(old)} sectors)')
    print(summary)
    print(f'\nChange in PI[year]/PI[{FACTOR_BASE_YEAR}] inflation factor')
    print(factor_summary)
    for year in (2024, 2025):
        top = revision[year].abs().sort_values(ascending=False).head(10).index
        print(f'\nLargest {year} level revisions')
        print(
            pd.DataFrame(
                {
                    'old': old.loc[top, year],
                    'new': new.loc[top, year],
                    'revision_pct': 100 * revision.loc[top, year],
                    f'go_{WEIGHT_YEAR}_$bn': weights.reindex(top) / 1e3,
                }
            )
        )


if __name__ == '__main__':
    main()
