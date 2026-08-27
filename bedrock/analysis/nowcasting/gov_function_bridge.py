"""Can Census government finances put a commodity mix into the ``G*`` columns?

#578's rescoped question, and the go/no-go the plan sequences S5 behind.

The government columns are the worst-drifting in the Use table -- ``S00500``
0.383, ``S00600`` 0.308, ``GSLGO`` 0.221, ``GSLGH`` 0.207, ``GSLGE`` 0.164 over
one five-year benchmark span.  Their column *level* is already observed (Step 3
takes ``GO - VAPRO``, see :mod:`~bedrock.transform.iot.nowcast_intermediate`),
so a source that supplies only a total supplies nothing.  What Census
``timeseries/govslocalfin`` uniquely offers is **function x object** -- education,
highways, police, hospitals, utilities -- and the plan's premise was that
function is a plausible bridge to commodity mix, because a highway department and
a school district buy different things.

The model that premise implies, for function shares ``w`` and within-function
commodity mixes ``m``::

    s_c(t) = sum_f w_f(t) * m_{c|f}

⚠️ **That model has an exact ceiling, and it is the finding.**  With ``m`` held
fixed, the commodity-mix movement the bridge can produce is bounded by the
*function*-mix movement itself::

    d(s(t), s(0)) = 0.5 * sum_c |sum_f dw_f * m_{c|f}|
                 <= 0.5 * sum_f |dw_f| * sum_c m_{c|f}
                  = 0.5 * sum_f |dw_f| = d(w(t), w(0))

since each ``m_{c|f}`` sums to 1 over commodities.  The bound is attained only if
the function mixes are mutually **disjoint**.  So the whole question reduces to
two measurable quantities: how far the function mix moves, and how different
government functions really are in what they buy.  Neither needs the bridge to
exist to be measured, which is what makes this a go/no-go rather than a build.

❌ The verdict is **no-go**
--------------------------

**1. The function mix barely moves** (:func:`function_mix_drift`).  Dissimilarity
of the state-and-local function mix against 2017 is **0.0464 at 2022** -- 4.6% of
government dollars changed function over five years -- and 0.0385 by 2024.  On the
current-operations basis it is **0.0121** across 2022-2024.  That 0.0464 is the
ceiling on a five-year span, against a **0.201** dollar-weighted commodity drift
in the same columns over a comparable span.

**2. Government functions are not disjoint** (:func:`function_dissimilarity`).
Measured on BEA's own government columns at 2017, the mean pairwise dissimilarity
is **0.639** -- they overlap by 36%, because every function buys utilities,
professional services, repair and supplies.  The realistic ceiling is therefore
about ``0.0464 * 0.639 = 0.030``, or **15% of what has to be explained**, and
that still credits the model with a perfect bridge.

**3. Reweighting real functions delivers 2.4%** (:func:`reweighting_test`).  BEA's
three general state-and-local columns *are* a function split -- education,
hospitals and health, everything else -- so their weights and their within-function
commodity mixes are both observed, at 2012 and 2017.  Their weight movement over
that span is **0.0453**, statistically the same size as ``govslocalfin``'s 0.0464.
Holding each column's 2012 mix and moving only the weights cuts the frozen error
from 0.1946 to **0.1899**, a gain of **+2.4%**.  ⚠️ **97.6% of the drift in those
columns is movement *within* a function**, which no function reweighting can see.

⚠️ Two independent defects, either of which would also sink it
--------------------------------------------------------------

**Current Operations by function does not exist before 2022** (:func:`coverage`).
The intermediate-consumption object -- ``Current Operations`` net of the
``Salaries and Wages`` exhibit -- is published **by function only for 2022-2024**.
For 2017-2021 it is a single economy-wide number, so there is **no 2017
observation to anchor the bridge at the seed year**, and the annual movement the
source was wanted for is exactly what it does not carry.  The row count jump the
source note flagged, 137 codes to 232 at 2022, *is* this defect.

The only continuously published function series is **Total Expenditure**, which
mixes in salaries, capital outlay, assistance and subsidies, and interest.
⚠️ **Its error as a proxy is larger than the signal it would carry**
(:func:`object_proxy`): total-expenditure and current-operations function mixes
differ by **0.062** in every year both exist, against the 0.0385 of total function
movement 2017-2024 that the proxy is being asked to transmit.

**`govslocalfin` is state and local only** (:func:`reach`).  Federal is 43.1% of
the government block's misplaced dollars, and the single worst-drifting column in
the entire Use table -- ``S00500``, 83,851 $M misplaced at 0.383 -- is federal and
out of reach.  State and local reaches 164,683 $M of 289,330 $M, **56.9%**.

What this does and does not close
---------------------------------

✅ It closes the **function bridge** as a route to the ``G*`` commodity mix.  The
bound in :func:`reweighting_test` is not specific to ``govslocalfin``'s function
list: a finer function split does not help, because disaggregating functions does
not move more dollars between them -- the 33-function mix moves 0.0464 where BEA's
3-function split moves 0.0453.

❌ It does **not** close #578's underlying problem.  The ``G*`` columns still
drift, the drift is still within-function, and nothing here sources it.  What it
rules out is the one bridge the plan had named.

⚠️ Two by-products of the pull survive the no-go and belong to other steps.
``Salaries and Wages`` is the government ``V00100`` Step 2 wants, and
``Capital Outlay`` by function -- which *is* published 2017-2024, unlike current
operations -- is the natural check on the final-demand government investment
columns.  Neither is Step 3's and neither is claimed here.

Run::

    uv run python -m bedrock.analysis.nowcasting.gov_function_bridge --all
    uv run python -m bedrock.analysis.nowcasting.gov_function_bridge --check
"""

import argparse
import json
import urllib.request
from pathlib import Path

import numpy as np
import pandas as pd

from bedrock.analysis.nowcasting.intermediate_structure_drift import (
    BENCHMARK_YEAR,
    benchmark_detail_intermediate,
    column_shares,
    dissimilarity,
)
from bedrock.utils.config.common import load_env_file_key

#: ``timeseries/govslocalfin`` publishes 2017-2024.  It is **not** nested under
#: ``timeseries/govs``; that path 404s.
GOVSLOCALFIN = 'https://api.census.gov/data/timeseries/govslocalfin'

#: Years the source covers.
GOVS_YEARS = tuple(range(2017, 2025))

#: ``GOVTYPE`` for state and local combined, which is the only tier that lines up
#: with BEA's ``GSLG*`` industries.
STATE_AND_LOCAL = '001'

#: Cached pull, under the gitignored analysis output directory.
_CACHE = 'govslocalfin.parquet'

#: Every ``AGG_DESC`` label for direct expenditure starts with this.
_PREFIX = 'Expenditure - Direct Expenditure - '

#: BEA detail government industries, split by whether a state-and-local source
#: can reach them at all.
FEDERAL = ('S00500', 'S00600', 'S00101', 'S00102')
STATE_LOCAL = ('GSLGE', 'GSLGH', 'GSLGO', 'S00201', 'S00202', 'S00203')

#: The three general state-and-local columns, which are themselves a function
#: split of state and local government: education, hospitals and health, rest.
SLG_GENERAL = ('GSLGE', 'GSLGH', 'GSLGO')

#: The benchmark span the detail commodity mix is observed over.
BENCHMARK_SPAN: tuple[BENCHMARK_YEAR, BENCHMARK_YEAR] = (2012, 2017)

#: The object whose function detail begins only at 2022.
CURRENT_OPERATIONS = 'Current Operations'

#: The object published by function for every year, and the reason that is not
#: a substitute -- it carries salaries, capital outlay, subsidies and interest.
TOTAL_EXPENDITURE = 'Total Expenditure'


def _output_dir() -> Path:
    path = Path(__file__).parent / 'output'
    path.mkdir(exist_ok=True)
    return path


def fetch(year: int) -> pd.DataFrame:
    """One year of ``govslocalfin``, all government types, as published.

    ``AGG_DESC`` is a bare code (``LF0094``); the human label lives in the
    ``AGG_DESC_LABEL`` attribute and has to be asked for explicitly.  Amounts are
    **$ thousands**, like the other Census API sources in the build.
    """
    key = load_env_file_key('api_key', 'Census')
    fields = 'AGG_DESC,AGG_DESC_LABEL,AMOUNT,GOVTYPE,GOVTYPE_LABEL'
    url = f'{GOVSLOCALFIN}?get={fields}&for=us:*&time={year}&key={key}'
    with urllib.request.urlopen(url, timeout=120) as response:
        payload = json.loads(response.read().decode('utf-8'))
    frame = pd.DataFrame(payload[1:], columns=payload[0])
    frame['YEAR'] = year
    frame['AMOUNT'] = frame.AMOUNT.astype('int64')
    return frame


def govslocalfin(refresh: bool = False) -> pd.DataFrame:
    """Every published year, cached under ``output/``.

    The cache is a convenience, not a snapshot contract -- ``--refresh`` re-pulls.
    """
    cache = _output_dir() / _CACHE
    if cache.exists() and not refresh:
        return pd.read_parquet(cache)
    frame = pd.concat([fetch(year) for year in GOVS_YEARS], ignore_index=True)
    frame.to_parquet(cache)
    return frame


def _function_object(label: str) -> tuple[str, str] | None:
    """Split a direct-expenditure ``AGG_DESC`` label into (function, object).

    Returns ``None`` for anything that is not a function leaf -- the
    all-function totals, the revenue block, and the ``Education`` parent tier
    that would double-count its three leaves.
    """
    if not label.startswith(_PREFIX):
        return None
    rest = label[len(_PREFIX) :]
    for head in ('General Expenditure - ', 'Utility Expenditure - '):
        if rest.startswith(head):
            rest = rest[len(head) :]
            break
    else:
        if not rest.startswith('Liquor Stores'):
            return None
    parts = [part.strip() for part in rest.split(' - ')]
    if len(parts) < 2:
        return None
    function, obj = ' / '.join(parts[:-1]), parts[-1]
    if function.startswith('Education / '):
        function = function[len('Education / ') :]
    if function == 'Education':
        return None
    # 'Total Expenditures' and 'Total Expenditure' are the same object.
    return function, TOTAL_EXPENDITURE if obj.startswith('Total Expenditure') else obj


def function_object_frame(refresh: bool = False) -> pd.DataFrame:
    """State-and-local direct expenditure as function x object x year, in $B."""
    frame = govslocalfin(refresh)
    frame = frame[frame.GOVTYPE == STATE_AND_LOCAL].copy()
    split = frame.AGG_DESC_LABEL.map(_function_object)
    frame = frame[split.notna()].copy()
    frame['function'] = [pair[0] for pair in split[split.notna()]]
    frame['object'] = [pair[1] for pair in split[split.notna()]]
    frame['amount_B'] = frame.AMOUNT / 1e6
    return frame[['YEAR', 'function', 'object', 'amount_B']]


def mix(frame: pd.DataFrame, obj: str, year: int) -> pd.Series:
    """Function shares of one object in one year."""
    slice_ = frame[(frame.object == obj) & (frame.YEAR == year)]
    total = slice_.set_index('function').amount_B
    total = total[total > 0]
    return total / total.sum()


def _dissimilarity(left: pd.Series, right: pd.Series) -> float:
    shared = left.index.intersection(right.index)
    return float(0.5 * (left[shared] - right[shared]).abs().sum())


def coverage(refresh: bool = False) -> pd.DataFrame:
    """WARNING: Which objects carry a function split, by year -- the anchoring defect.

    ``Current Operations`` is the object Step 3 needs and it is **absent by
    function for 2017-2021**, so the bridge cannot be anchored at the seed year.
    """
    frame = function_object_frame(refresh)
    table = frame.pivot_table(
        index='YEAR', columns='object', values='function', aggfunc='nunique'
    )
    return table.fillna(0).astype(int)


def function_mix_drift(refresh: bool = False) -> pd.DataFrame:
    """WARNING: The ceiling: how far the function mix moves, on each available basis.

    Every number here is an upper bound on the commodity-mix movement a
    function-reweighting bridge could deliver, before any allowance for the fact
    that government functions buy overlapping things.
    """
    frame = function_object_frame(refresh)
    rows = []
    base = mix(frame, TOTAL_EXPENDITURE, 2017)
    for year in GOVS_YEARS[1:]:
        rows.append(
            {
                'basis': TOTAL_EXPENDITURE,
                'span': f'2017 -> {year}',
                'functions': len(mix(frame, TOTAL_EXPENDITURE, year)),
                'dissimilarity': _dissimilarity(
                    base, mix(frame, TOTAL_EXPENDITURE, year)
                ),
            }
        )
    base = mix(frame, CURRENT_OPERATIONS, 2022)
    for year in (2023, 2024):
        rows.append(
            {
                'basis': CURRENT_OPERATIONS,
                'span': f'2022 -> {year}',
                'functions': len(mix(frame, CURRENT_OPERATIONS, year)),
                'dissimilarity': _dissimilarity(
                    base, mix(frame, CURRENT_OPERATIONS, year)
                ),
            }
        )
    return pd.DataFrame(rows)


def object_proxy(refresh: bool = False) -> pd.DataFrame:
    """WARNING: Is ``Total Expenditure`` a usable stand-in for ``Current Operations``?

    It is the only function series published for every year, so the bridge would
    have to ride on it.  The comparison is only possible for 2022-2024.
    """
    frame = function_object_frame(refresh)
    rows = []
    for year in (2022, 2023, 2024):
        current = mix(frame, CURRENT_OPERATIONS, year)
        total = mix(frame, TOTAL_EXPENDITURE, year)
        rows.append(
            {'year': year, 'cross_object_dissimilarity': _dissimilarity(current, total)}
        )
    return pd.DataFrame(rows)


def reach() -> pd.DataFrame:
    """WARNING: Gate 1: what share of the government block a state-and-local source sees.

    Misplaced dollars are the detail commodity-mix dissimilarity over
    :data:`BENCHMARK_SPAN` times the column's own intermediate total, so a column
    is worth what it spends.
    """
    base, target = BENCHMARK_SPAN
    seed, actual = (
        benchmark_detail_intermediate(base),
        benchmark_detail_intermediate(target),
    )
    weights = actual.sum(axis=0)
    _, per_column = dissimilarity(column_shares(seed), column_shares(actual), weights)
    codes = list(FEDERAL) + list(STATE_LOCAL)
    table = pd.DataFrame(
        {
            'tier': ['federal'] * len(FEDERAL) + ['state+local'] * len(STATE_LOCAL),
            'dissimilarity': per_column[codes],
            'column_$M': weights[codes],
            'misplaced_$M': (per_column * weights)[codes],
        }
    )
    return table.sort_values('misplaced_$M', ascending=False)


def function_dissimilarity() -> pd.DataFrame:
    """How different government functions are in what they actually buy.

    BEA's five general government columns at detail, scored pairwise on their
    2017 commodity shares.  This is the multiplier on the :func:`function_mix_drift`
    ceiling: at dissimilarity 1.0 the functions are disjoint and the bound is
    attained, at 0.0 reweighting them changes nothing at all.
    """
    shares = column_shares(benchmark_detail_intermediate(BENCHMARK_SPAN[1]))
    codes = list(SLG_GENERAL) + ['S00500', 'S00600']
    table = pd.DataFrame(index=codes, columns=codes, dtype=float)
    for row in codes:
        for column in codes:
            table.loc[row, column] = 0.5 * (shares[row] - shares[column]).abs().sum()
    return table


def reweighting_test() -> dict[str, float]:
    """WARNING: The bound, realised on real functions with real within-function mixes.

    BEA's three general state-and-local columns are a function split whose
    weights *and* commodity mixes are both observed at 2012 and 2017.  Freeze
    each mix at 2012, move only the weights to 2017, and score the aggregate --
    this is exactly what a perfect function bridge would do, with no bridge
    needed to do it.
    """
    base, target = BENCHMARK_SPAN
    early, late = (
        benchmark_detail_intermediate(base),
        benchmark_detail_intermediate(target),
    )
    codes = list(SLG_GENERAL)
    weight_early, weight_late = early[codes].sum(axis=0), late[codes].sum(axis=0)
    mix_early = column_shares(early)[codes]

    actual = late[codes].sum(axis=1) / late[codes].sum().sum()
    frozen = (mix_early * weight_early).sum(axis=1) / weight_early.sum()
    reweighted = (mix_early * weight_late).sum(axis=1) / weight_late.sum()

    frozen_error = float(0.5 * (frozen - actual).abs().sum())
    reweighted_error = float(0.5 * (reweighted - actual).abs().sum())
    return {
        'weight_movement': float(
            0.5
            * (weight_late / weight_late.sum() - weight_early / weight_early.sum())
            .abs()
            .sum()
        ),
        'frozen_error': frozen_error,
        'reweighted_error': reweighted_error,
        'gain': (frozen_error - reweighted_error) / frozen_error,
    }


def observed_drift() -> pd.Series:
    """Detail commodity-mix drift of the three state-and-local columns, and its
    dollar-weighted average -- what a bridge would have to explain."""
    base, target = BENCHMARK_SPAN
    early, late = (
        benchmark_detail_intermediate(base),
        benchmark_detail_intermediate(target),
    )
    weights = late.sum(axis=0)
    _, per_column = dissimilarity(column_shares(early), column_shares(late), weights)
    codes = list(SLG_GENERAL)
    drift = per_column[codes]
    weighted = float((drift * weights[codes]).sum() / weights[codes].sum())
    return pd.concat([drift, pd.Series({'dollar_weighted': weighted})])


def _money(value: float) -> str:
    return f'{value:,.0f}'


def main(
    reach_: bool = False,
    coverage_: bool = False,
    movement: bool = False,
    bound: bool = False,
    all_: bool = False,
    check: bool = False,
    refresh: bool = False,
) -> int:
    if all_ or check:
        reach_ = coverage_ = movement = bound = True
    if not any((reach_, coverage_, movement, bound)):
        reach_ = coverage_ = movement = bound = True
    failed = 0

    if reach_:
        print('=== GATE 1  what a state-and-local source can reach ===')
        table = reach()
        print(table.to_string(float_format=lambda x: f'{x:,.3f}'))
        tiers = table.groupby('tier')[['column_$M', 'misplaced_$M']].sum()
        print()
        print(tiers.to_string(float_format=_money))
        total = float(table['misplaced_$M'].sum())
        slg = float(tiers['misplaced_$M']['state+local'])
        print(
            f'\nstate+local reaches {_money(slg)} $M of {_money(total)} $M '
            f'misplaced -- {slg / total:.1%}'
        )
        print(
            'WARNING: the worst-drifting column in the whole Use table, S00500 at '
            '0.383, is federal and out of reach'
        )
        if check and not 0.55 < slg / total < 0.59:
            print('REGRESSED: state+local reach moved off 56.9%')
            failed += 1

    if coverage_:
        print('\n=== GATE 2a  which objects carry a function split ===')
        table = coverage(refresh)
        print(table.to_string())
        early = table.loc[2017].get(CURRENT_OPERATIONS, 0)
        late = table.loc[2024].get(CURRENT_OPERATIONS, 0)
        print(
            f'\nWARNING: Current Operations by function: {early} functions in 2017, '
            f'{late} in 2024.'
        )
        print(
            '   There is NO 2017 observation to anchor the bridge at the seed '
            'year, and the\n   only continuous function series is Total '
            'Expenditure, which is not the object.'
        )
        proxy = object_proxy(refresh)
        print()
        print(proxy.to_string(index=False, float_format=lambda x: f'{x:.4f}'))
        worst = proxy.cross_object_dissimilarity.max()
        print(
            f'WARNING: the proxy error ({worst:.4f}) is LARGER than the whole 2017-2024 '
            'function movement it\n   would have to transmit (0.0385).'
        )
        if check and not (early == 0 and late == 33):
            print('REGRESSED: Current Operations function coverage moved')
            failed += 1
        if check and not 0.055 < worst < 0.070:
            print('REGRESSED: total-expenditure proxy error moved off 0.062-0.064')
            failed += 1

    if movement:
        print('\n=== GATE 2b  how far the function mix moves -- the CEILING ===')
        table = function_mix_drift(refresh)
        print(table.to_string(index=False, float_format=lambda x: f'{x:.4f}'))
        five_year = float(table[table.span == '2017 -> 2022'].dissimilarity.iloc[0])
        print(
            f'\nWARNING: 4.6% of government dollars changed function over five years. '
            f'That {five_year:.4f}\n   is an EXACT upper bound on what a function '
            'bridge can move the commodity mix.'
        )
        if check and not 0.044 < five_year < 0.049:
            print('REGRESSED: five-year function movement moved off 0.0464')
            failed += 1

    if bound:
        print('\n=== GATE 3  the ceiling, against what has to be explained ===')
        pairwise = function_dissimilarity()
        print('pairwise dissimilarity of BEA government columns, 2017:')
        print(pairwise.to_string(float_format=lambda x: f'{x:.3f}'))
        values = pairwise.values
        overlap = float(values[~np.eye(len(values), dtype=bool)].mean())
        print(
            f'\nmean off-diagonal {overlap:.3f} -- government functions overlap by '
            f'{1 - overlap:.0%},\nso the ceiling is really 0.0464 * {overlap:.3f} = '
            f'{0.0464 * overlap:.3f}.'
        )

        drift = observed_drift()
        print('\nobserved detail commodity drift, 2012 -> 2017:')
        print(drift.to_string(float_format=lambda x: f'{x:.3f}'))
        must_explain = float(drift['dollar_weighted'])
        print(
            f'\n-> the bridge could explain at most '
            f'{0.0464 * overlap / must_explain:.1%} of it, crediting it with a '
            'perfect within-function mix.'
        )

        result = reweighting_test()
        print('\nand what real function reweighting actually delivers:')
        print(
            f'  function-weight movement 2012->2017 : '
            f'{result["weight_movement"]:.4f}   '
            f"(govslocalfin's 33 functions move 0.0464 -- no finer split helps)"
        )
        print(f'  frozen 2012 mix, scored at 2017     : {result["frozen_error"]:.4f}')
        print(
            f'  function-reweighted, scored at 2017 : {result["reweighted_error"]:.4f}'
        )
        print(f'  -> gain {result["gain"]:+.2%} of the frozen error')
        print(
            f'\nNO-GO. {1 - result["gain"]:.1%} of the drift in these columns is '
            'movement WITHIN a function,\n   which no function reweighting can see.'
        )
        if check and not 0.015 < result['gain'] < 0.035:
            print('REGRESSED: reweighting gain moved off +2.4%')
            failed += 1
        if check and not 0.60 < overlap < 0.68:
            print('REGRESSED: function overlap moved off 0.639')
            failed += 1

    if check:
        print(
            f'\n{"NO-GO: " + str(failed) + " check(s) failed" if failed else "OK: all checks pass"}'
        )
        return 1 if failed else 0
    return 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--reach',
        dest='reach_',
        action='store_true',
        help='gate 1: federal vs state+local',
    )
    parser.add_argument(
        '--coverage',
        dest='coverage_',
        action='store_true',
        help='gate 2a: which objects carry a function split, and the proxy error',
    )
    parser.add_argument(
        '--movement',
        action='store_true',
        help='gate 2b: how far the function mix moves',
    )
    parser.add_argument(
        '--bound',
        action='store_true',
        help='gate 3: the ceiling and the reweighting test',
    )
    parser.add_argument('--all', dest='all_', action='store_true', help='every gate')
    parser.add_argument(
        '--check',
        action='store_true',
        help='exit non-zero if one of the documented findings has regressed',
    )
    parser.add_argument(
        '--refresh', action='store_true', help='re-pull govslocalfin rather than cache'
    )
    raise SystemExit(main(**vars(parser.parse_args())))
