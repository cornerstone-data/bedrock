"""
How wrong is a frozen 2017 commodity mix, and where? (Step 4a, #570)

Carries the published 2017 detail Supply block's commodity mix forward onto
*observed* detail industry output, aggregates to BEA summary, and scores the
result against the published annual summary Supply table's ``T007`` column
(Total Commodity Output, published 2017-2024).

⚠️ **This is a diagnostic, not a construction.** The point is to size the
correction that a frozen mix leaves behind, because that correction is exactly
the amount of detail that would be flattened if the summary gap were closed by a
uniform within-group ratio. Big correction means the detail movement has to come
from detail-level indicators instead.

The benchmark replay #570 specifies cannot answer this: under a ported mix, 2017
reproduces itself by construction. The annual summary table is the only
non-circular target, and it covers every nowcast year.

Run: ``uv run python bedrock/analysis/nowcasting/frozen_mix_diagnostic.py``
"""

from __future__ import annotations

import argparse

import numpy as np
import pandas as pd

from bedrock.extract.iot.io_2017 import (
    _load_2017_detail_supply_use_usa,
    _load_usa_summary_sut,
)
from bedrock.transform.iot.derived_gross_industry_output import derive_gross_output
from bedrock.utils.taxonomy.mappings.bea_v2017_commodity__bea_v2017_summary import (
    load_bea_v2017_commodity_to_bea_v2017_summary,
)

#: The Supply tables are published in millions; ``derive_gross_output`` returns
#: dollars. Scoring one against the other without this is a factor-of-1e6 error
#: that looks like a 100,000,000% miss rather than a unit bug.
DOLLARS_TO_MILLIONS = 1e6

DEFAULT_YEARS = (2018, 2019, 2020, 2021, 2022, 2023, 2024)


#: Trailing aggregate columns of the Supply table, which are not industries.
#: ⚠️ Selecting industry columns by code *length* silently swallows ``'TRADE '``
#: - the label carries a trailing space, so it is six characters like a BEA
#: detail code. That injects the whole trade margin column into the block and
#: inflates margin-heavy commodities many-fold (apparel by 16x), while leaving
#: the economy-wide total close enough to look plausible. Match names, not
#: shapes.
TRAILING_COLUMNS = frozenset(
    {
        'Commodity Description',
        'T007',
        'MCIF',
        'MADJ',
        'T013',
        'TRADE',
        'TRANS',
        'T014',
        'MDTY',
        'TOP',
        'SUB',
        'T015',
        'T016',
    }
)


def detail_mix() -> tuple[pd.DataFrame, pd.Series]:
    """The 2017 detail block's commodity mix per industry, and its column sums."""
    supply = _load_2017_detail_supply_use_usa('Supply_detail')
    supply.columns = [str(c).strip() for c in supply.columns]
    industries = [c for c in supply.columns if c not in TRAILING_COLUMNS]
    block = supply[industries].apply(pd.to_numeric, errors='coerce').fillna(0.0)
    block = block[block.index.astype(str).str.len() == 6]
    column_totals = block.sum(axis=0)
    return block / column_totals.replace(0, np.nan), column_totals


def detail_to_summary() -> dict[str, str]:
    return {
        detail: summary[0]
        for detail, summary in load_bea_v2017_commodity_to_bea_v2017_summary().items()
        if summary
    }


def score_year(year: int, mix: pd.DataFrame, industries: list[str]) -> pd.DataFrame:
    """Frozen-mix commodity output vs published summary ``T007``, per group."""
    output = derive_gross_output(year, 'before') / DOLLARS_TO_MILLIONS
    output.index = output.index.astype(str)
    built_detail = (mix[industries] * output.reindex(industries).values).sum(axis=1)

    summary = _load_usa_summary_sut('Supply_summary', year)
    published = pd.to_numeric(summary['T007'], errors='coerce').dropna()

    built = built_detail.groupby(built_detail.index.map(detail_to_summary())).sum()
    scored = pd.concat(
        [built.rename('built'), published.rename('published')], axis=1
    ).dropna()
    scored = scored[scored['published'] > 0]
    scored['pct'] = (scored['built'] / scored['published'] - 1) * 100
    return scored


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--years', type=int, nargs='*', default=list(DEFAULT_YEARS))
    parser.add_argument(
        '--worst', type=int, default=8, help='how many worst groups to show'
    )
    parser.add_argument(
        '--check-2017-margins',
        action='store_true',
        help='report how far published detail gross output sits from the 2017 '
        'block column sums - the two margins must agree before either can '
        'constrain a build',
    )
    args = parser.parse_args()

    mix, column_totals = detail_mix()
    output_2017 = derive_gross_output(2017, 'before') / DOLLARS_TO_MILLIONS
    output_2017.index = output_2017.index.astype(str)
    industries = [i for i in mix.columns if i in output_2017.index]

    if args.check_2017_margins:
        ratio = output_2017.reindex(industries) / column_totals.reindex(industries)
        print('2017 row margin: published gross output vs the block it should sum to')
        print(
            f'  block  {column_totals.sum() / 1e6:>8,.3f} tn over {len(column_totals)}'
        )
        print(f'  GO     {output_2017.sum() / 1e6:>8,.3f} tn over {len(output_2017)}')
        print(
            f'  per-industry ratio: median {ratio.median():.4f}, '
            f'p05 {ratio.quantile(0.05):.3f}, p95 {ratio.quantile(0.95):.3f}\n'
        )

    for year in args.years:
        scored = score_year(year, mix, industries)
        weight = scored['published'] / scored['published'].sum()
        print(
            f'== {year}: {len(scored)} groups | '
            f'total {scored["built"].sum() / scored["published"].sum():.4f} | '
            f'wtd mean |err| {(weight * scored["pct"].abs()).sum():.2f}% | '
            f'max |err| {scored["pct"].abs().max():.1f}%'
        )
        worst = scored.sort_values('pct', key=abs, ascending=False).head(args.worst)
        print(
            worst[['published', 'pct']]
            .assign(published=lambda d: (d['published'] / 1e6).round(2))
            .round(1)
            .to_string(),
            end='\n\n',
        )


if __name__ == '__main__':
    main()
