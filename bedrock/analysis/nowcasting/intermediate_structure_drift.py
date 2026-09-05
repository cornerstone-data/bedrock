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

Six measurements
----------------

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

``--theta``
    Fits the price-ratio exponent with and without the **margin leg** of the
    purchaser deflator.  ⚠️ Answered: the margin leg does not move ``theta``, so
    the low summary ``theta`` is not a missing-deflator artefact.  See
    :func:`theta`.

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

✅ **The benchmark panel has an extractor.**  ``io_2017``'s
``load_benchmark_detail_U_intermediate_usa`` reads 2007, 2012 and 2017 off the
published panel, GCS-backed like every other table here, and its 2017 sheet is
checked cell for cell against the single-year workbook.  See
:func:`benchmark_detail_intermediate`.

Run::

    uv run python -m bedrock.analysis.nowcasting.intermediate_structure_drift
    uv run python -m bedrock.analysis.nowcasting.intermediate_structure_drift --all
"""

from __future__ import annotations

import argparse
import itertools
import typing as ta

import numpy as np
import pandas as pd

from bedrock.extract.iot.io_2017 import (
    GCS_USA_SUP_DIR,
    LOCAL_USA_SUP_DIR,
    _load_2017_detail_supply_use_usa,
    load_benchmark_detail_U_intermediate_usa,
)
from bedrock.transform.iot.derived_price_index import derive_industry_price_index
from bedrock.transform.iot.nowcast_intermediate import (
    INTERMEDIATE_YEARS,
    PRICE_SURGE,
    benchmark_intermediate,
    derive_intermediate_use,
    intermediate_column_control,
    reproduction_check,
)
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
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

#: The Supply side of the same vintage, which carries the margin and tax legs.
CURRENT_SUMMARY_SUPPLY = 'Supply_Tables_1997-2024_Summary.xlsx'

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

#: ``derive_industry_price_index`` starts here, so 2007 spans carry no carry.
PRICE_INDEX_START = 2012

#: Every year both the summary Use SUT and the price index cover, which is the
#: span universe :func:`regime` fits on -- 78 base/target pairs.
REGIME_YEARS = tuple(range(PRICE_INDEX_START, 2025))

#: Exponent on the price ratio.  1.0 is #497 as written; 0.0 is a frozen ``A``.
#:
#: ⚠️ **The grid runs negative deliberately.**  It used to start at 0.0, which
#: censored the summary panel: 2023 and 2024 both pinned to the floor and were
#: read as "inflation contributes nothing".  They do not -- they fit -0.25 and
#: -0.50, meaning the frozen structure scores *better* when commodity shares are
#: moved **against** their price movement.  A floor of 0.0 cannot represent that
#: and silently reports it as 0.0.
THETA_GRID = (
    -1.0,
    -0.75,
    -0.5,
    -0.25,
    0.0,
    0.25,
    0.5,
    0.75,
    1.0,
    1.25,
    1.5,
)


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


def summary_supply(year: int) -> pd.DataFrame:
    """A year's sheet of the summary Supply SUT, indexed by commodity code."""
    supply = load_from_gcs(
        name=CURRENT_SUMMARY_SUPPLY,
        sub_bucket=GCS_USA_SUP_DIR,
        local_dir=LOCAL_USA_SUP_DIR,
        loader=lambda pth: pd.read_excel(
            pth, sheet_name=str(year), skiprows=5, dtype={'Unnamed: 0': str}
        ),
    )
    supply = supply.set_index(supply.columns[0])
    supply.index = supply.index.astype(str).str.strip()
    supply.columns = supply.columns.astype(str).str.strip()
    return supply


def summary_margin_rate(year: int) -> pd.Series:
    """``mu_c``: trade and transport margins over **producer** value, by commodity.

    ⚠️ The denominator is producer value, ``T013 + T015``, not basic ``T013``.
    BEA gross output is at producers' prices, so the price index carried in
    :func:`inflation` already contains the product-tax layer; taking the rate
    over basic value would double-count that wedge.  See §Margins.2 of
    ``intermediate_estimation_plan.md``.

    ``T016 = T013 + T014 + T015`` -- ``T014`` is the margins alone, not a
    running subtotal.
    """
    supply = summary_supply(year)
    rows = [r for r in supply.index if r != 'IOCode' and not r.startswith('T0')]
    parts = (
        supply.reindex(rows)[['T013', 'T014', 'T015']]
        .apply(pd.to_numeric, errors='coerce')
        .dropna(how='all')
    )
    producer = parts['T013'] + parts['T015']
    return parts['T014'] / producer.where(producer != 0, np.nan)


def summary_margin_factor(year: int, base: int = 2017) -> pd.Series:
    """``(1 + mu(t)) / (1 + mu(base))``, the margin leg of the purchaser deflator.

    ⚠️ **Margin suppliers are held at 1.0.**  For a trade or transport commodity
    ``T014`` is large and negative -- its margin is allocated away onto the goods
    it carries, which is why the columns net to zero -- so ``mu`` runs to -0.94
    (``42``), -0.99 (``486``), -0.88 (``482``) and ``1 + mu`` is a near-zero
    denominator.  Those rows carry almost no dollars in the purchaser-priced
    intermediate block anyway, for the same reason their ``mu`` is negative.
    """
    now, then = summary_margin_rate(year), summary_margin_rate(base)
    factor = (1.0 + now) / (1.0 + then)
    receiving = (then > 0) & (now > -1)
    return factor.where(receiving, 1.0).replace([np.inf, -np.inf], 1.0).fillna(1.0)


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


def theta(years: tuple[int, ...] = DRIFT_YEARS) -> pd.DataFrame:
    """Fit ``theta`` with and without the margin leg -- the discriminating test.

    §Inflation reads the summary panel's low ``theta`` as substitution under
    relative-price dispersion.  But a **missing deflator term** produces the same
    symptom, and the term that is missing is known: a cell of this block is at
    purchaser value and the price index carried against it is a producer-value
    one (§Margins.2).  So the two readings are separable by experiment rather
    than by argument:

    * if adding the margin leg pulls ``theta`` **toward** the detail panel's
      1.00, the gap was the deflator;
    * if ``theta`` stays low, the substitution reading stands and the margin leg
      is a second-order correction to a term that is wrong for another reason.

    Scored per year so the answer can be read against the price regime rather
    than averaged across it.
    """
    benchmark = summary_intermediate(2017)
    base_pi = summary_price_index(2017)
    records = []
    for year in years:
        actual = summary_intermediate(year)
        rows, columns = _align(benchmark, actual)
        observed = column_shares(actual.loc[rows, columns])
        frozen = column_shares(benchmark.loc[rows, columns])
        weights = actual.loc[rows, columns].sum(axis=0)
        price = (summary_price_index(year) / base_pi).reindex(rows).fillna(1.0)
        margin = summary_margin_factor(year).reindex(rows).fillna(1.0)

        def best(ratio: pd.Series) -> tuple[float, float]:
            # THETA_GRID runs negative, so a zero ratio would raise the whole
            # fit to infinity rather than harmlessly to zero.  No year has one
            # today; this keeps a future one from poisoning the fit silently.
            ratio = ratio.where(ratio > 0, 1.0)
            scored = {
                t: dissimilarity(
                    column_shares(frozen.mul(ratio**t, axis=0)), observed, weights
                )[0]
                for t in THETA_GRID
            }
            fitted = min(scored, key=lambda t: scored[t])
            return fitted, scored[fitted]

        price_theta, price_score = best(price)
        both_theta, both_score = best(price * margin)
        records.append(
            {
                'year': year,
                'theta_price_only': price_theta,
                'score_at_theta': price_score,
                'theta_with_margin': both_theta,
                'score_at_theta_margin': both_score,
                'margin_moves_theta': both_theta - price_theta,
            }
        )
    return pd.DataFrame(records).set_index('year')


def regime(price_years: tuple[int, ...] = REGIME_YEARS) -> pd.DataFrame:
    """Fit ``theta`` on every non-nested summary span, and see what predicts it.

    ⚠️ **Every span in :func:`theta` starts at 2017**, so span length, cumulative
    inflation, price dispersion and accumulated structural drift all move
    together with the calendar and none of them can be told from the others.
    That is why §Inflation's "``theta`` is a function of relative-price
    dispersion in the span" could be named but not tested.

    The summary Use SUT publishes 1997-2024 and ``derive_industry_price_index``
    reaches back to 2012, so spans with **different base years, different
    lengths and different inflation** are free -- 78 of them.  On those the
    regressors come apart, and the answer is not the one that was expected:

    ==============================  =====
    predictor of the fitted theta   R^2
    ==============================  =====
    crosses the 2021-22 surge       0.613
    cumulative price level          0.525
    elapsed years                   0.142
    relative-price **dispersion**   0.014
    ==============================  =====

    ✅ **The regime reading survives and the dispersion reading does not.**
    Dispersion explains 1.4% of the variance and its coefficient has the *wrong
    sign*; elapsed time explains 14%.  A single binary -- does the span cross
    2021-22 -- explains 61%, and adding elapsed years to it moves its
    coefficient to 0.002 and its R^2 not at all.  Holding length fixed, spans
    that cross the surge fit theta 0.0-0.5 and spans that do not fit 0.7-0.9, at
    every length from one to nine years.

    ⚠️ **And the choice is worth little where it matters most.**  The median gain
    of the best theta over a frozen ``A`` is **5.44%** of the score off the
    surge and **0.59%** across it.  Every year this build targets from 2022 on
    crosses it.
    """
    records = []
    for base, target in itertools.combinations(price_years, 2):
        seed_block, actual = summary_intermediate(base), summary_intermediate(target)
        rows, columns = _align(seed_block, actual)
        observed = column_shares(actual.loc[rows, columns])
        frozen = column_shares(seed_block.loc[rows, columns])
        weights = actual.loc[rows, columns].sum(axis=0)
        price = (
            (summary_price_index(target) / summary_price_index(base))
            .reindex(rows)
            .fillna(1.0)
        )
        margin = summary_margin_factor(target, base).reindex(rows).fillna(1.0)
        mass = seed_block.loc[rows, columns].sum(axis=1)

        def best(ratio: pd.Series) -> tuple[float, float]:
            ratio = ratio.where(ratio > 0, 1.0)
            scored = {
                t: dissimilarity(
                    column_shares(frozen.mul(ratio**t, axis=0)), observed, weights
                )[0]
                for t in THETA_GRID
            }
            fitted = min(scored, key=lambda t: scored[t])
            return fitted, scored[fitted]

        fitted_theta, score = best(price)
        with_margin, _ = best(price * margin)
        # A ratio of exactly 1.0 scores the same at every theta, so this is the
        # frozen-A score rather than a fit.
        _, frozen_score = best(price * 0 + 1.0)
        records.append(
            {
                'base': base,
                'target': target,
                'elapsed': target - base,
                'crosses_surge': base <= PRICE_SURGE[0] and target >= PRICE_SURGE[1],
                'theta': fitted_theta,
                'theta_with_margin': with_margin,
                'gain_over_frozen_%': 100 * (frozen_score - score) / frozen_score,
                **_price_spread(price, mass),
            }
        )
    return pd.DataFrame(records)


def _price_spread(ratio: pd.Series, mass: pd.Series) -> dict[str, float]:
    """The level and the spread of a span's log price ratio, dollar-weighted."""
    keep = (ratio > 0) & mass.notna() & (mass > 0)
    log = np.log(ratio[keep])
    weight = mass[keep] / mass[keep].sum()
    mean = float((log * weight).sum())
    return {
        'mean_log_ratio': mean,
        'sd_log_ratio': float(np.sqrt(float((weight * (log - mean) ** 2).sum()))),
    }


def regime_summary(spans: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """``regime``'s two readable cuts: what predicts theta, and what it is worth."""

    def r_squared(columns: list[str]) -> float:
        design = np.column_stack([np.ones(len(spans)), spans[columns].to_numpy(float)])
        target = spans['theta'].to_numpy(float)
        beta, *_ = np.linalg.lstsq(design, target, rcond=None)
        residual = float(((target - design @ beta) ** 2).sum())
        return 1 - residual / float(((target - target.mean()) ** 2).sum())

    models = {
        'crosses the 2021-22 surge': ['crosses_surge'],
        'cumulative price level': ['mean_log_ratio'],
        'elapsed years': ['elapsed'],
        'relative-price dispersion': ['sd_log_ratio'],
        'surge + elapsed': ['crosses_surge', 'elapsed'],
        'level + dispersion': ['mean_log_ratio', 'sd_log_ratio'],
    }
    fits = pd.DataFrame(
        {'R2': {name: r_squared(columns) for name, columns in models.items()}}
    )
    worth = (
        spans.groupby('crosses_surge')
        .agg(
            spans=('theta', 'size'),
            theta_mean=('theta', 'mean'),
            theta_min=('theta', 'min'),
            theta_max=('theta', 'max'),
            median_gain_over_frozen_pct=('gain_over_frozen_%', 'median'),
        )
        .round(3)
    )
    return fits.round(3), worth


def benchmark_detail_intermediate(year: BENCHMARK_YEAR) -> pd.DataFrame:
    """Intermediate block of the detail Use SUT for a benchmark year, in $M.

    BEA's benchmark detail SUT panel carries **2007, 2012 and 2017 on one sheet
    each, all on the 2017 code basis and all in the same 413 x 424 frame** --
    purchaser value, before redefinitions, BEA detail.  That is Step 3's
    estimand exactly, three times, so the holdout below scores the thing being
    built rather than an analogue of it.

    ``io_2017.load_benchmark_detail_U_intermediate_usa`` returns it in USD on
    the model index; this module works in $M against the summary panel, so it
    is divided back and left on the raw codes.
    """
    block = (
        load_benchmark_detail_U_intermediate_usa(year) / MILLION_CURRENCY_TO_CURRENCY
    )
    block.index = pd.Index(list(USA_2017_COMMODITY_CODES))
    block.columns = pd.Index(list(USA_2017_INDUSTRY_CODES))
    return block


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


def column_control(years: tuple[int, ...] = DRIFT_YEARS) -> pd.DataFrame:
    """Score Step 3's column control against the published summary ``T005``.

    The control is ``GO_producer - VAPRO`` per detail industry, both read off
    :mod:`bedrock.transform.iot.derived_intermediate_and_value_added`. Nothing
    observed exists at detail after 2017, but the *summary* Use SUT publishes
    ``T005`` for every year, so aggregating the detail control to summary checks
    that the 191-line to detail allocation adds back the way it should.

    ``level_%`` is the economy-wide error and ``spread_%`` is the dollar-weighted
    mean absolute error across summary industries.

    ⚠️ **This is a consistency check, not an independent validation.** BEA's
    ``UII205-A``/``UVA205-A`` and the summary Use SUT's ``T005`` are the same
    underlying estimate published two ways, so agreement confirms the mapping,
    the allocation and the residual construction -- not the source. Read it as
    "the control is now BEA's own ``T005``" rather than "the control was tested
    against something else".

    Measured 2026-08-25: ``level_%`` within 0.00007% and ``spread_%`` within
    0.00023% in every year 2018-2024, worst summary industry 0.003%. The
    superseded frozen-ratio seed scored 0.2-2.3% and 2.5-8.0% on the same two
    columns, with ``GSLG`` 18.3% low at 2022.
    """
    industry_to_summary = {
        str(code): (parents[0] if isinstance(parents, list) else str(parents))
        for code, parents in load_bea_v2017_industry_to_bea_v2017_summary().items()
    }
    group = pd.Series(
        {code: industry_to_summary.get(code) for code in USA_2017_INDUSTRY_CODES}
    )
    records = []
    for year in years:
        built = (
            intermediate_column_control(year).groupby(group).sum()
            / MILLION_CURRENCY_TO_CURRENCY
        )
        published = pd.to_numeric(
            ta.cast('pd.Series', summary_use(year).loc['T005']), errors='coerce'
        ).reindex(built.index)
        shared = published.notna()
        error = built[shared] - published[shared]
        total = float(published[shared].sum())
        worst = ta.cast(str, error.abs().idxmax())
        records.append(
            {
                'year': year,
                'built_$M': float(built[shared].sum()),
                'published_$M': total,
                'level_%': 100 * (float(built[shared].sum()) / total - 1),
                'spread_%': 100 * float(error.abs().sum()) / total,
                'worst': worst,
                'worst_%': 100 * float(error[worst]) / float(published[worst]),
            }
        )
    return pd.DataFrame(records).set_index('year')


def seed(theta: float | None = None) -> pd.DataFrame:
    """The built Step 3 block, year by year, against a frozen 2017 level.

    ``theta`` defaults to ``nowcast_intermediate.default_theta`` for the span,
    which is what the build ships; pass ``THETA_497`` for #497 as written.

    ⚠️ **This is not a score.** Nothing observed exists at detail after 2017, so
    all this reports is that the block is levelled, signed and shaped the way it
    should be. The structure is scored on the summary panel by :func:`drift` and
    :func:`inflation`, and the plumbing is checked by
    :func:`~bedrock.transform.iot.nowcast_intermediate.reproduction_check`.
    """
    frozen = float(benchmark_intermediate().to_numpy().sum())
    records = []
    for year in INTERMEDIATE_YEARS:
        block = derive_intermediate_use(year, theta=theta)
        total = float(block.to_numpy().sum())
        records.append(
            {
                'year': year,
                'intermediate_$B': total / 1e9,
                'vs_frozen_2017_%': 100 * (total / frozen - 1),
                'negative_cells': int((block.to_numpy() < 0).sum()),
                'empty_columns': int((block.abs().sum(axis=0) == 0).sum()),
            }
        )
    return pd.DataFrame(records).set_index('year')


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
    parser.add_argument(
        '--theta', action='store_true', help='fit theta, with and without margins'
    )
    parser.add_argument(
        '--regime',
        action='store_true',
        help='fit theta on all 78 non-nested spans; what predicts it',
    )
    parser.add_argument(
        '--control', action='store_true', help='score the column control'
    )
    parser.add_argument('--seed', action='store_true', help='the built Step 3 block')
    parser.add_argument('--all', action='store_true', help='every measurement')
    args = parser.parse_args()
    chosen = any(
        (
            args.drift,
            args.inflation,
            args.holdout,
            args.where,
            args.revision,
            args.theta,
            args.regime,
            args.control,
            args.seed,
        )
    )

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
    if args.all or args.theta:
        print('\nFitting theta, with and without the margin leg of the deflator')
        print('(does the missing purchaser-price term explain the low theta?)\n')
        print(theta().round(4).to_string())
    if args.all or args.regime:
        spans = regime()
        fits, worth = regime_summary(spans)
        print('\nFitting theta on every non-nested summary span, 2012-2024')
        print(f'({len(spans)} spans, so span length and inflation come apart)\n')
        print(fits.to_string())
        print('\nand what the choice is worth\n')
        print(worth.to_string())
    if args.all or args.control:
        print("\nStep 3's column control against the published summary T005")
        print('(level_% is economy-wide; spread_% is weighted MAE by industry)\n')
        print(column_control().round(3).to_string())
    if args.all or args.seed:
        print('\nThe built Step 3 block, against a frozen 2017 level\n')
        print(seed().round(3).to_string())
        print('\n2017 reproduction of the published interior\n')
        print(reproduction_check().to_string())
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
