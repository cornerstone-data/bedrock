"""How fast does an industry's input structure go stale, and does inflation fix it?

Step 3 (#497) seeds the Use table's intermediate block from the 2017 detail Use
SUT and carries it to 2018-2025 on a commodity price index.  Step 5 then holds
**both** margins of that block: the industry column to gross output (hard, T1)
and the commodity row to ``T016 = T019`` (hard, T11).  A biproportional balance
with both margins fixed keeps nothing of the seed but its *structure* -- so the
only question worth measuring about Step 3 is how good the carried structure is,
and the only metric that isolates it is one computed **on column shares with the
column total given**.

That metric is the index of dissimilarity::

    d_j = 0.5 * sum_c | s_hat[c, j] - s[c, j] |

the share of industry ``j``'s intermediate dollars sitting on the wrong
commodity.  Reported dollar-weighted across industries, so a column is worth what
it spends.

Three measurements
------------------

``--drift`` (default)
    Published **summary** Use SUT, 2017 against 2018-2024.  One benchmark
    vintage, no revision seam, exactly the nowcast horizon.  Answers *how fast
    does frozen 2017 structure decay*.

``--inflation``
    The same years, scoring the frozen structure against the same structure
    carried on a commodity price index.  Answers *does #497's inflation step
    earn its place*.

``--holdout``
    The 2012 benchmark detail Use table carried to 2017 and scored against the
    published 2017 detail table -- the only **out-of-sample, detail-level,
    both-ends-observed** version of the same question.  Mirrors
    :mod:`~.mix_holdout_test`, which does this for Step 4a's commodity mix.

⚠️ **The summary reference is not ground truth.**  BEA's annual summary SUT is
itself an estimate built from annual indicators over a carried-forward benchmark
structure.  Wherever BEA also froze structure, "frozen" wins here by
construction, and the drift below is a floor.  ``--holdout`` is the
non-circular check: both 2012 and 2017 are Economic-Census-anchored best-*level*
estimates, so neither was carried from the other.

⚠️ **The two references sit in different spaces.**  The summary SUT is
before-redefinitions at purchaser value, which is Step 3's own space; the 2012
detail table is available only after redefinitions at producer value
(``CEDA6IO.xlsx``, see :mod:`~.mix_holdout_test`).  The holdout is therefore an
analogue of Step 3's object, not that object.

Run::

    uv run python -m bedrock.analysis.nowcasting.intermediate_structure_drift
    uv run python -m bedrock.analysis.nowcasting.intermediate_structure_drift --all
"""

from __future__ import annotations

import argparse
import typing as ta

import numpy as np
import pandas as pd

from bedrock.extract.iot.io_2012 import load_2012_UR_usa
from bedrock.extract.iot.io_2017 import (
    _load_2017_detail_supply_use_usa,
    _load_usa_summary_sut,
    load_2017_Utot_after_redef_usa,
)
from bedrock.transform.iot.derived_price_index import derive_industry_price_index
from bedrock.utils.taxonomy.mappings.bea_v2017_commodity__bea_v2017_summary import (
    load_bea_v2017_commodity_to_bea_v2017_summary,
)

#: Years of the published summary Use SUT after the benchmark.
DRIFT_YEARS = (2018, 2019, 2020, 2021, 2022, 2023, 2024)

#: The five detail codes that changed between the 2012 and 2017 benchmarks.
#: ``33391A`` was renumbered; the four ``3352xx`` motor/generator codes merged.
#: Same map as :mod:`~.mix_holdout_test`.
RENAME = {
    '33391A': '333914',
    '335221': '335220',
    '335222': '335220',
    '335224': '335220',
    '335228': '335220',
}


def summary_intermediate(year: int) -> pd.DataFrame:
    """Commodity x industry intermediate block of the published summary Use SUT."""
    use = _load_usa_summary_sut('Use_SUT_summary', year)  # type: ignore[arg-type]
    use.index = use.index.astype(str)
    use.columns = use.columns.astype(str)
    # 'IOCode' is the workbook's header row, not a commodity; T005 and T001 are
    # the first margin row and column, so everything above and left of them is
    # the interior.
    first_margin_row = int(ta.cast(int, use.index.get_loc('T005')))
    first_margin_column = int(ta.cast(int, use.columns.get_loc('T001')))
    rows = [r for r in use.index[:first_margin_row] if r != 'IOCode']
    columns = list(use.columns[1:first_margin_column])
    return use.loc[rows, columns].astype(float)


def column_shares(block: pd.DataFrame) -> pd.DataFrame:
    """Each column normalised to its own total.  Empty columns stay zero."""
    total = block.sum(axis=0)
    return block.div(total.where(total != 0, np.nan), axis=1).fillna(0.0)


def dissimilarity(
    estimate: pd.DataFrame, actual: pd.DataFrame, weights: pd.Series
) -> tuple[float, pd.Series]:
    """Dollar-weighted index of dissimilarity, and the per-column series."""
    per_column = (estimate - actual).abs().sum(axis=0) / 2.0
    total = float(weights.sum())
    weighted = float((per_column * weights).sum() / total) if total else float('nan')
    return weighted, per_column


def _align(left: pd.DataFrame, right: pd.DataFrame) -> tuple[list[str], list[str]]:
    rows = [r for r in left.index if r in right.index]
    columns = [c for c in left.columns if c in right.columns]
    return rows, columns


def summary_price_index(year: int) -> pd.Series:
    """Detail industry price index aggregated to BEA summary, output-weighted.

    ``derive_industry_price_index`` is bedrock's own 2012-2025 detail industry
    PI (BEA underlying detail ``UGO304-A``, topped up from the summary quarterly
    series for the latest years).  Weights are 2017 detail commodity output
    ``T007``, which is the mass each detail code carries inside its summary
    parent.
    """
    price_index = derive_industry_price_index()
    price_index.index = price_index.index.astype(str)
    supply = _load_2017_detail_supply_use_usa('Supply_detail')
    supply.columns = supply.columns.str.strip()
    output = supply['T007'].astype(float)
    detail_to_summary = {
        str(code): (parents[0] if isinstance(parents, list) else str(parents))
        for code, parents in load_bea_v2017_commodity_to_bea_v2017_summary().items()
    }
    group = pd.Series(
        {code: detail_to_summary.get(code) for code in price_index.index}
    ).dropna()
    weight = output.reindex(group.index).fillna(0.0)
    level = price_index[year].reindex(group.index)
    numerator = (level * weight).groupby(group).sum()
    denominator = weight.groupby(group).sum()
    return (numerator / denominator.replace(0, np.nan)).fillna(
        level.groupby(group).mean()
    )


def drift() -> pd.DataFrame:
    """Frozen 2017 summary structure scored against every published later year."""
    benchmark = summary_intermediate(2017)
    records = []
    for year in DRIFT_YEARS:
        actual = summary_intermediate(year)
        rows, columns = _align(benchmark, actual)
        weights = actual.loc[rows, columns].sum(axis=0)
        score, _ = dissimilarity(
            column_shares(benchmark.loc[rows, columns]),
            column_shares(actual.loc[rows, columns]),
            weights,
        )
        records.append(
            {
                'year': year,
                'dissimilarity': score,
                'intermediate_$M': float(weights.sum()),
            }
        )
    return pd.DataFrame(records).set_index('year')


def inflation() -> pd.DataFrame:
    """Frozen structure against the same structure carried on a price index."""
    benchmark = summary_intermediate(2017)
    base_pi = summary_price_index(2017)
    records = []
    for year in DRIFT_YEARS:
        actual = summary_intermediate(year)
        rows, columns = _align(benchmark, actual)
        observed = column_shares(actual.loc[rows, columns])
        frozen = column_shares(benchmark.loc[rows, columns])
        ratio = (summary_price_index(year) / base_pi).reindex(rows).fillna(1.0)
        carried = column_shares(frozen.mul(ratio, axis=0))
        weights = actual.loc[rows, columns].sum(axis=0)
        frozen_score, _ = dissimilarity(frozen, observed, weights)
        carried_score, _ = dissimilarity(carried, observed, weights)
        records.append(
            {
                'year': year,
                'frozen': frozen_score,
                'inflated': carried_score,
                'improvement_%': 100 * (frozen_score - carried_score) / frozen_score,
            }
        )
    return pd.DataFrame(records).set_index('year')


def _detail_2012_and_2017() -> tuple[pd.DataFrame, pd.DataFrame]:
    """2012 and 2017 detail Use interiors on one shared commodity x industry frame."""
    use_2012 = load_2012_UR_usa()
    use_2012.index = use_2012.index.astype(str)
    use_2012.columns = use_2012.columns.astype(str)
    use_2012 = use_2012.rename(index=RENAME, columns=RENAME)
    # groupby on both axes: the 3352xx merge maps four codes onto one
    use_2012 = use_2012.groupby(level=0).sum().T.groupby(level=0).sum().T
    use_2017 = load_2017_Utot_after_redef_usa()
    use_2017.index = use_2017.index.astype(str)
    use_2017.columns = use_2017.columns.astype(str)
    rows, columns = _align(use_2012, use_2017)
    return use_2012.loc[rows, columns], use_2017.loc[rows, columns]


def holdout() -> pd.DataFrame:
    """2012 detail structure carried to 2017, with and without inflation."""
    use_2012, use_2017 = _detail_2012_and_2017()
    price_index = derive_industry_price_index()
    price_index.index = price_index.index.astype(str)
    ratio = (price_index[2017] / price_index[2012]).rename(index=RENAME)
    ratio = ratio.groupby(level=0).mean().reindex(use_2012.index).fillna(1.0)

    observed = column_shares(use_2017)
    frozen = column_shares(use_2012)
    carried = column_shares(frozen.mul(ratio, axis=0))
    weights = use_2017.sum(axis=0)
    frozen_score, frozen_columns = dissimilarity(frozen, observed, weights)
    carried_score, carried_columns = dissimilarity(carried, observed, weights)
    improved = int((carried_columns < frozen_columns).sum())
    return pd.DataFrame(
        [
            {
                'variant': 'frozen 2012 structure',
                'dissimilarity': frozen_score,
                'columns_improved': f'- / {len(frozen_columns)}',
            },
            {
                'variant': '+ commodity inflation',
                'dissimilarity': carried_score,
                'columns_improved': f'{improved} / {len(carried_columns)}',
            },
        ]
    ).set_index('variant')


def where(year: int = 2024, top: int = 15) -> pd.DataFrame:
    """Which summary industry columns carry the drift, by dollars misplaced."""
    benchmark = summary_intermediate(2017)
    actual = summary_intermediate(year)
    names = _load_usa_summary_sut('Use_SUT_summary', year).loc['IOCode']  # type: ignore[arg-type]
    rows, columns = _align(benchmark, actual)
    weights = actual.loc[rows, columns].sum(axis=0)
    _, per_column = dissimilarity(
        column_shares(benchmark.loc[rows, columns]),
        column_shares(actual.loc[rows, columns]),
        weights,
    )
    table = pd.DataFrame(
        {
            'name': [str(names.get(c))[:38] for c in columns],
            'dissimilarity': per_column,
            'column_$M': weights,
            'misplaced_$M': per_column * weights,
        }
    )
    return table.sort_values('misplaced_$M', ascending=False).head(top)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--drift', action='store_true', help='summary 2017 to 2018-2024'
    )
    parser.add_argument('--inflation', action='store_true', help='does #497 help?')
    parser.add_argument('--holdout', action='store_true', help='2012 to 2017, detail')
    parser.add_argument('--where', action='store_true', help='which columns drift')
    parser.add_argument('--all', action='store_true', help='every measurement')
    args = parser.parse_args()
    chosen = args.drift or args.inflation or args.holdout or args.where

    if args.all or args.drift or not chosen:
        print('\nFrozen 2017 input structure vs the published summary Use SUT')
        print('(share of a column of dollars sitting on the wrong commodity)\n')
        print(drift().round(4).to_string())
    if args.all or args.inflation:
        print('\nDoes carrying on a commodity price index help?\n')
        print(inflation().round(4).to_string())
    if args.all or args.holdout:
        print('\n2012 detail structure carried to 2017, scored on the 2017 benchmark\n')
        print(holdout().round(4).to_string())
    if args.all or args.where:
        print('\nWhere the 2024 drift sits\n')
        print(where().round(3).to_string())
    print()


if __name__ == '__main__':
    main()
