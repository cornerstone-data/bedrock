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

Five measurements
-----------------

``--drift`` (default)
    Published **summary** Use SUT, 2017 against 2018-2024.  One benchmark
    vintage, no revision seam, exactly the nowcast horizon.  Answers *how fast
    does frozen 2017 structure decay*.

``--inflation``
    The same years, scoring the frozen structure against the same structure
    carried on a commodity price index.  Answers *does #497's inflation step
    earn its place*.

``--holdout``
    The **benchmark detail SUT panel** -- 2007, 2012 and 2017, each carried to a
    later benchmark and scored there.  This is the load-bearing measurement:
    purchaser value, before redefinitions, BEA detail, all three years on the
    2017 code basis in one frame, which is Step 3's estimand exactly rather than
    an analogue of it.  Every span is out of sample at both ends, since each
    benchmark is its own Economic-Census-anchored *best-level* estimate.  It also
    reports what aggregating to summary hides, and fits the ``theta`` exponent on
    the price ratio.  Mirrors :mod:`~.mix_holdout_test`, which does the same for
    Step 4a's commodity mix.

``--where``
    Which columns carry the drift, at summary for 2024 and at detail for
    2012 -> 2017.  Summary hides about a third of the error and hides it
    unevenly, so the two rankings differ.

``--revision``
    The same year read from **both** summary Use vintages.  Nothing in it is
    drift: it is BEA restating a structure it had already published, and it is
    the noise floor under every year-on-year number above.

⚠️ **Every summary measurement reads one vintage.**  ``io_2017``'s loader picks
the workbook by year, which is right for FBA consumers and wrong for a module
that differences years against each other -- it would put a seam between 2022 and
2023 in the middle of ``--drift``'s series, and make ``--where``'s 2024 ranking a
2017 base from one workbook against a 2024 from another.  So every summary read
here goes to :data:`CURRENT_SUMMARY_USE` directly.  ``--revision`` measures what
that seam was worth.

⚠️ **The summary reference is not ground truth.**  BEA's annual summary SUT is
itself an estimate built from annual indicators over a carried-forward benchmark
structure.  Wherever BEA also froze structure, "frozen" wins there by
construction, and ``--drift`` is a floor.  ``--holdout`` is the non-circular
check, and it is also the one that reaches BEA detail.

⚠️ **The benchmark panel has no extractor yet.**  It arrives as a local drop of
``SUPPLY-USE_2026-08-24.zip`` in the ``USA_AllTablesSUP`` cache directory;
``io_2017`` still maps ``Use_SUT_detail`` to the single-year 2017 workbook.  See
:func:`benchmark_detail_intermediate`.

Run::

    uv run python -m bedrock.analysis.nowcasting.intermediate_structure_drift
    uv run python -m bedrock.analysis.nowcasting.intermediate_structure_drift --all
"""

from __future__ import annotations

import argparse
import typing as ta
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd

from bedrock.extract.iot.io_2017 import (
    GCS_USA_SUP_DIR,
    LOCAL_USA_SUP_DIR,
    _load_2017_detail_supply_use_usa,
)
from bedrock.transform.iot.derived_price_index import derive_industry_price_index
from bedrock.utils.io.gcp import load_from_gcs
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES
from bedrock.utils.taxonomy.mappings.bea_v2017_commodity__bea_v2017_summary import (
    load_bea_v2017_commodity_to_bea_v2017_summary,
)
from bedrock.utils.taxonomy.mappings.bea_v2017_industry__bea_v2017_summary import (
    load_bea_v2017_industry_to_bea_v2017_summary,
)

#: Years of the published summary Use SUT after the benchmark.
DRIFT_YEARS = (2018, 2019, 2020, 2021, 2022, 2023, 2024)

#: The two summary Use vintages.  ``io_2017._load_usa_summary_sut`` pins the
#: workbook by year -- 2017-2022 from the legacy release, 2023-2024 from the
#: current one -- so that published FBAs do not move under BEA's revisions.  That
#: is right for FBA consumers and wrong here: this module differences years
#: against each other, so reading through it would put a vintage seam in the
#: middle of every series.  Everything below reads :data:`CURRENT_SUMMARY_USE`,
#: and ``--revision`` measures what the other vintage would have contributed.
CURRENT_SUMMARY_USE = 'Use_Tables_Supply-Use_Framework_1997-2024_Summary.xlsx'
LEGACY_SUMMARY_USE = 'Use_Tables_Supply-Use_Framework_2017-2022_Summary.xlsx'

#: Years both vintages publish, which is what ``--revision`` can compare.
REVISION_YEARS = (2017, 2018, 2019, 2020, 2021, 2022)

#: The benchmark detail SUT panel: three years, one code basis, one frame.
BENCHMARK_YEAR = ta.Literal[2007, 2012, 2017]
BENCHMARK_YEARS: tuple[BENCHMARK_YEAR, ...] = (2007, 2012, 2017)
BENCHMARK_SPANS: tuple[tuple[BENCHMARK_YEAR, BENCHMARK_YEAR], ...] = (
    (2007, 2012),
    (2012, 2017),
    (2007, 2017),
)
BENCHMARK_SUT_ARCHIVE = 'SUPPLY-USE_2026-08-24.zip'

#: ``derive_industry_price_index`` starts here, so 2007 spans carry no carry.
PRICE_INDEX_START = 2012

#: Exponent on the price ratio.  1.0 is #497 as written; 0.0 is a frozen ``A``.
THETA_GRID = (0.0, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5)


def summary_use(year: int, workbook: str = CURRENT_SUMMARY_USE) -> pd.DataFrame:
    """A year's sheet of a summary Use SUT workbook, indexed by row code.

    Deliberately not ``io_2017._load_usa_summary_sut``: that picks the workbook
    by year, and this module needs the vintage held fixed across years.  See
    :data:`CURRENT_SUMMARY_USE`.
    """
    use = load_from_gcs(
        name=workbook,
        sub_bucket=GCS_USA_SUP_DIR,
        local_dir=LOCAL_USA_SUP_DIR,
        loader=lambda pth: pd.read_excel(
            pth, sheet_name=str(year), skiprows=5, dtype={'Unnamed: 0': str}
        ),
    )
    use = use.set_index(use.columns[0])
    use.index = use.index.astype(str).str.strip()
    use.columns = use.columns.astype(str).str.strip()
    return use


def summary_intermediate(
    year: int, workbook: str = CURRENT_SUMMARY_USE
) -> pd.DataFrame:
    """Commodity x industry intermediate block of the published summary Use SUT."""
    use = summary_use(year, workbook)
    # 'IOCode' is the workbook's header row, not a commodity; T005 and T001 are
    # the first margin row and column, so everything above and left of them is
    # the interior.
    first_margin_row = int(ta.cast(int, use.index.get_loc('T005')))
    first_margin_column = int(ta.cast(int, use.columns.get_loc('T001')))
    rows = [r for r in use.index[:first_margin_row] if r != 'IOCode']
    columns = list(use.columns[1:first_margin_column])
    # '...' marks a withheld cell, and blanks are structural zeros.
    return use.loc[rows, columns].apply(pd.to_numeric, errors='coerce').fillna(0.0)


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


def revision(top: int = 10) -> tuple[pd.DataFrame, pd.DataFrame]:
    """The same year read from both vintages: how big is BEA's own revision?

    Nothing here is drift.  Both sides are the *same year*, so a non-zero score
    is BEA restating a structure it had already published -- the noise floor
    under every year-on-year number this module reports, and the reason
    everything else reads one workbook.  Returns the year series and the columns
    that move most at the last overlapping year.
    """
    records = []
    for year in REVISION_YEARS:
        current = summary_intermediate(year, CURRENT_SUMMARY_USE)
        legacy = summary_intermediate(year, LEGACY_SUMMARY_USE)
        rows, columns = _align(current, legacy)
        weights = current.loc[rows, columns].sum(axis=0)
        score, _ = dissimilarity(
            column_shares(legacy.loc[rows, columns]),
            column_shares(current.loc[rows, columns]),
            weights,
        )
        records.append(
            {
                'year': year,
                'revision': score,
                'intermediate_$M': float(weights.sum()),
            }
        )
    series = pd.DataFrame(records).set_index('year')

    last = REVISION_YEARS[-1]
    current, legacy = (
        summary_intermediate(last, CURRENT_SUMMARY_USE),
        summary_intermediate(last, LEGACY_SUMMARY_USE),
    )
    names = summary_use(last).loc['IOCode']
    rows, columns = _align(current, legacy)
    weights = current.loc[rows, columns].sum(axis=0)
    _, per_column = dissimilarity(
        column_shares(legacy.loc[rows, columns]),
        column_shares(current.loc[rows, columns]),
        weights,
    )
    columns_frame = pd.DataFrame(
        {
            'name': [str(names.get(c))[:38] for c in columns],
            f'revision_{last}': per_column,
            'column_$M': weights,
            'restated_$M': per_column * weights,
        }
    ).sort_values('restated_$M', ascending=False)
    return series, columns_frame.head(top)


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


def benchmark_detail_intermediate(year: BENCHMARK_YEAR) -> pd.DataFrame:
    """Intermediate block of the detail Use SUT for a benchmark year, in $M.

    ``Use_SUT_Detail.xlsx`` inside ``SUPPLY-USE_2026-08-24.zip`` carries **2007,
    2012 and 2017 on one sheet each, all on the 2017 code basis and all in the
    same 413 x 424 frame** -- purchaser value, before redefinitions, BEA detail.
    That is Step 3's estimand exactly, three times, so the holdout below scores
    the thing being built rather than an analogue of it.

    ⚠️ **Not wired to GCS yet.**  ``io_2017`` maps ``Use_SUT_detail`` to the
    single-year ``Use_SUT_Framework_2017_DET.xlsx``; this zip is a local drop in
    the same cache directory and has no extractor.  Promoting it to a proper
    year-parameterised loader is its own task -- until then this reads the local
    file and says so if it is missing.
    """
    archive = Path(LOCAL_USA_SUP_DIR) / BENCHMARK_SUT_ARCHIVE
    if not archive.exists():
        raise FileNotFoundError(
            f'{archive} not found.  The 2007/2012/2017 detail SUT panel is a '
            'local drop with no extractor yet; see this module docstring.'
        )
    with (
        zipfile.ZipFile(archive) as bundle,
        bundle.open('Use_SUT_Detail.xlsx') as sheet,
    ):
        frame = (
            pd.read_excel(sheet, sheet_name=str(year), skiprows=5, dtype={'Code': str})
            .set_index('Code')
            .fillna(0)
        )
    frame.columns = frame.columns.astype(str)
    return frame.reindex(
        index=list(USA_2017_COMMODITY_CODES), columns=list(USA_2017_INDUSTRY_CODES)
    ).astype(float)


def _to_summary(block: pd.DataFrame) -> pd.DataFrame:
    """Aggregate a detail commodity x industry block to BEA summary on both axes."""

    def first(parents: object) -> str:
        return str(parents[0]) if isinstance(parents, list) else str(parents)

    commodity = {
        str(code): first(parents)
        for code, parents in load_bea_v2017_commodity_to_bea_v2017_summary().items()
    }
    industry = {
        str(code): first(parents)
        for code, parents in load_bea_v2017_industry_to_bea_v2017_summary().items()
    }
    rows = block.groupby(pd.Series({c: commodity.get(c, c) for c in block.index})).sum()
    return (
        rows.T.groupby(pd.Series({c: industry.get(c, c) for c in rows.columns})).sum().T
    )


def holdout() -> pd.DataFrame:
    """Each benchmark structure carried to a later benchmark, detail and summary.

    ``theta`` is the exponent on the price ratio that minimises the score:
    ``theta = 1`` is #497 as written, ``theta = 0`` is a frozen ``A``.  Only
    spans starting at 2012 or later carry one -- ``derive_industry_price_index``
    begins at 2012.
    """
    blocks = {year: benchmark_detail_intermediate(year) for year in BENCHMARK_YEARS}
    price_index = derive_industry_price_index()
    price_index.index = price_index.index.astype(str)
    commodities = list(USA_2017_COMMODITY_CODES)

    records = []
    for base, target in BENCHMARK_SPANS:
        seed, actual = blocks[base], blocks[target]
        weights = actual.sum(axis=0)
        observed = column_shares(actual)
        frozen = column_shares(seed)
        detail_score, frozen_columns = dissimilarity(frozen, observed, weights)
        summary_score, _ = dissimilarity(
            column_shares(_to_summary(seed)),
            column_shares(_to_summary(actual)),
            _to_summary(actual).sum(axis=0),
        )
        record: dict[str, object] = {
            'span': f'{base} -> {target}',
            'detail': detail_score,
            'summary': summary_score,
            'hidden_by_summary_%': 100 * (1 - summary_score / detail_score),
        }
        if base >= PRICE_INDEX_START:
            ratio = (
                (price_index[target] / price_index[base])
                .reindex(commodities)
                .fillna(1.0)
            )
            carried, carried_columns = column_shares(frozen.mul(ratio, axis=0)), None
            inflated_score, carried_columns = dissimilarity(carried, observed, weights)
            scored = {
                theta: dissimilarity(
                    column_shares(frozen.mul(ratio**theta, axis=0)), observed, weights
                )[0]
                for theta in THETA_GRID
            }
            record |= {
                'inflated': inflated_score,
                'inflation_%': 100 * (detail_score - inflated_score) / detail_score,
                'columns_improved': f'{int((carried_columns < frozen_columns).sum())}'
                f' / {len(frozen_columns)}',
                'best_theta': min(scored, key=lambda t: scored[t]),
            }
        records.append(record)
    return pd.DataFrame(records).set_index('span')


def where(year: int = 2024, top: int = 15) -> pd.DataFrame:
    """Which summary industry columns carry the drift, by dollars misplaced."""
    benchmark = summary_intermediate(2017)
    actual = summary_intermediate(year)
    names = summary_use(year).loc['IOCode']
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


def where_detail(
    base: BENCHMARK_YEAR = 2012, target: BENCHMARK_YEAR = 2017, top: int = 15
) -> pd.DataFrame:
    """The same picture at BEA detail, off the benchmark SUT panel.

    Worth having beside :func:`where`: summary hides roughly a third of the
    error (see :func:`holdout`), and it hides it unevenly, so the two rankings
    are not the same ranking.
    """
    seed = benchmark_detail_intermediate(base)
    actual = benchmark_detail_intermediate(target)
    names = _detail_descriptions(target)
    weights = actual.sum(axis=0)
    _, per_column = dissimilarity(column_shares(seed), column_shares(actual), weights)
    table = pd.DataFrame(
        {
            'name': [str(names.get(c))[:38] for c in actual.columns],
            'dissimilarity': per_column,
            'column_$M': weights,
            'misplaced_$M': per_column * weights,
        }
    )
    return table.sort_values('misplaced_$M', ascending=False).head(top)


def _detail_descriptions(year: BENCHMARK_YEAR) -> pd.Series:
    """``code -> description`` off the same sheet, for readable output."""
    use = _load_2017_detail_supply_use_usa('Use_SUT_detail')
    _ = year  # descriptions are the 2017 code book in every sheet
    return use['Commodity Description']


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--drift', action='store_true', help='summary 2017 to 2018-2024'
    )
    parser.add_argument('--inflation', action='store_true', help='does #497 help?')
    parser.add_argument(
        '--holdout', action='store_true', help='benchmark to benchmark, detail'
    )
    parser.add_argument('--where', action='store_true', help='which columns drift')
    parser.add_argument(
        '--revision', action='store_true', help='same year, both vintages'
    )
    parser.add_argument('--all', action='store_true', help='every measurement')
    args = parser.parse_args()
    chosen = args.drift or args.inflation or args.holdout or args.where or args.revision

    if args.all or args.drift or not chosen:
        print('\nFrozen 2017 input structure vs the published summary Use SUT')
        print('(share of a column of dollars sitting on the wrong commodity)\n')
        print(drift().round(4).to_string())
    if args.all or args.inflation:
        print('\nDoes carrying on a commodity price index help?\n')
        print(inflation().round(4).to_string())
    if args.all or args.holdout:
        print('\nBenchmark detail SUT carried forward, scored on the later benchmark')
        print('(purchaser value, before redefinitions - Step 3 estimand exactly)\n')
        print(holdout().round(4).to_string())
    if args.all or args.where:
        print('\nWhere the 2024 drift sits, summary\n')
        print(where().round(3).to_string())
        print('\nWhere the 2012 -> 2017 drift sits, detail\n')
        print(where_detail().round(3).to_string())
    if args.all or args.revision:
        series, columns = revision()
        print('\nThe same year read from both summary Use vintages')
        print('(not drift - BEA restating a structure it had already published)\n')
        print(series.round(4).to_string())
        print(f'\nWhich columns BEA restated most, {REVISION_YEARS[-1]}\n')
        print(columns.round(3).to_string())
    print()


if __name__ == '__main__':
    main()
