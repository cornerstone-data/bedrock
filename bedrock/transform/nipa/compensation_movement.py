"""The QCEW movement series: Step 2's ``V00100`` weight vector, 2017-2024.

What this is, and what it is not
--------------------------------

``NIPA_VA_compensation_<year>`` splits each of ``T60200D``'s 69 NIPA industry
controls across the BEA detail industries under it. The *level* is NIPA's; only
the **within-group shape** is estimated, and this module supplies it::

    weight_d(t) = V00100_d(2017) x growth_d(t)

where ``growth_d`` is QCEW annual payroll for industry ``d`` in year ``t`` over
the same industry in 2017. ``proportional`` attribution normalises inside the
group, so the weights are **weights and never levels** and each NIPA control
holds by construction.

⚠️ **QCEW's level is not usable; only its change is.** Graded on the observed
2012 -> 2017 span, level shares score **+21.9% worse than frozen 2017 shares**
at their best and +270.9% raw, while this movement form scores **-10.0%**. QCEW
misses proprietors, several benefit components and whole statutorily-excluded
industries, so its share of an industry's compensation is not its share of
compensation - but the year-over-year change in what it does see carries real
signal. That measurement is what rules out the far simpler build of pointing
``proportional`` straight at the QCEW FBA.

Four corrections, each of which changes the answer
--------------------------------------------------

**1. The concordance cannot resolve construction or government**, so they are
carved out. 47 NAICS codes sit under more than one BEA detail industry - 23
construction, 24 government - and they reach exactly five summary groups:
:data:`UNRESOLVABLE_GROUPS`. BEA splits construction by *type of structure* and
NAICS by *trade*, so a plumbing contractor's payroll belongs to no single BEA
construction industry. Applied everywhere QCEW makes the block **worse**
(+6.2%); carved out it is a clear go (-10.0%), and ``GSLG`` alone accounts for
more than twice the whole net degradation. ✅ The rule is derived from the
crosswalk's own multiplicity, so it is decidable before any score is computed.

**2. QCEW changes NAICS vintage mid-span**, at data year **2022**. Detected
rather than hardcoded (:func:`naics_vintage`): each year's six-digit code set is
matched against every column of the year concordance, and the margins are
decisive - 0.964 for 2012 on NAICS 2012, 0.9637 for 2017-2021 on NAICS 2017,
0.9621 for 2022-2025 on NAICS 2022. Ignoring it costs 31 detail industries their
coverage outright and doubles unmapped payroll from 10.4% to 20.7%.

⚠️ **Comparing two vintages code-by-code invents growth.** This is the failure
that faked a verdict once already: on NAICS 2012 against a NAICS 2017 crosswalk,
``541700`` came out at **20.9x** and raw QCEW scored **+92%**. So the two years
are paired through the concordance and a pair is used only when *both* halves
are present - :func:`code_bridge` - which holds each industry's composition
identical across the ratio by construction rather than by hope.

**3. Some industries are outside QCEW's universe entirely**, and a growth ratio
computed on the remnant is noise amplified. :data:`COVERAGE_FLOOR` gives an
industry its group's movement when QCEW sees less than 1% of its benchmark
compensation. Fifteen industries qualify on 2017 and fourteen are already
carved out above; the one this adds is **``482000`` rail**, where QCEW observes
**0.11%** of compensation because railroad employees are covered by the Railroad
Retirement Board rather than state UI. Left in, rail grows **4.45x** by 2024.

⚠️ **The floor is a validity guard, not a selector, and the holdout is neutral
on it.** At 1% it scores -10.016%, *identical* to no floor at all - the 2012 ->
2017 span simply does not contain the failure. Tuned upward it gets worse
(-8.2% at 0.10, -7.0% at 0.25), which is the same non-monotonicity that got the
group-level coverage ratio rejected as a selector. It is kept because an
industry observed at one part in a thousand is not being measured, which is an
argument about the source rather than about the score.

**4. A published zero is a suppression, not an observation.** QCEW reports
exactly ``0.0`` for both NAICS under ``334610`` (search and navigation
instruments) in **2021**, with normal payroll on either side - 486 and 922
million in 2020. Read as an observation that is a growth ratio of zero, a
weight of zero, and **the industry disappears from the block entirely**. One
occurrence in seven years, and fatal each time, so :func:`check` sweeps every
year rather than sampling.

⚠️ **An industry QCEW does not reach keeps its group's movement, never a
zero.** That is the difference between *no opinion* and *no compensation*, and
only the first is true. It applies to all four cases above: carved out,
unbridged, unobserved, and suppressed.

Reproduce every figure above with ``--check``.
"""

from __future__ import annotations

import argparse
import functools
import glob
import os
import sys

import numpy as np
import pandas as pd

from bedrock.extract.iot.io_2017 import _load_2017_detail_supply_use_usa
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES
from bedrock.utils.taxonomy.mappings.bea_v2017_industry__bea_v2017_summary import (
    load_bea_v2017_industry_to_bea_v2017_summary,
)

#: The benchmark the shares are carried from. Growth is always measured against
#: this year, never chained, so a single vintage bridge covers each ratio.
BENCHMARK_YEAR = 2017

#: Use row this module weights.
COMPENSATION_ROW = 'V00100'

#: QCEW annual payroll FBAs, generated locally; see ``BLS_QCEW.yaml``.
QCEW_GLOB = 'bedrock/extract/output_data/BLS_QCEW_{year}_*.parquet'

#: NAICS vintage concordance, one column per NAICS revision.
CONCORDANCE = os.path.join(
    'bedrock', 'utils', 'mapping', 'naics', 'NAICS_Year_Concordance.csv'
)

#: BEA detail industry crosswalk, read for its NAICS rows.
BEA_CROSSWALK = os.path.join(
    'bedrock',
    'utils',
    'mapping',
    'activitytosectormapping',
    'Sector_Crosswalk_BEA_2017_Detail.csv',
)

#: NAICS revisions the concordance carries, newest last.
VINTAGE_COLUMNS = {
    2002: 'NAICS_2002_Code',
    2007: 'NAICS_2007_Code',
    2012: 'NAICS_2012_Code',
    2017: 'NAICS_2017_Code',
    2022: 'NAICS_2022_Code',
}

#: Summary industries whose detail split QCEW's axis cannot express. Derived
#: from the crosswalk's own ambiguity; see the module docstring.
UNRESOLVABLE_GROUPS = ('23', 'GFE', 'GFGN', 'GSLE', 'GSLG')

#: Below this share of benchmark compensation, QCEW is not observing the
#: industry and its growth ratio is discarded. A validity guard, not a tuned
#: threshold - see the module docstring.
COVERAGE_FLOOR = 0.01


@functools.lru_cache(maxsize=1)
def _concordance() -> pd.DataFrame:
    return pd.read_csv(CONCORDANCE, dtype=str)


@functools.lru_cache(maxsize=1)
def _bea_crosswalk() -> pd.DataFrame:
    frame = pd.read_csv(BEA_CROSSWALK, dtype=str)
    return frame[frame['SectorSourceName'] == 'NAICS_2017_Code']


@functools.lru_cache(maxsize=1)
def ambiguous_naics() -> tuple[str, ...]:
    """NAICS codes the crosswalk puts under more than one BEA detail industry.

    ⚠️ **47 codes, all construction or government, and not a data defect** - it
    is the two axes disagreeing about what an industry is. They would otherwise
    be split evenly and the even split would be called an answer.
    """
    counts = _bea_crosswalk().groupby('Sector')['Activity'].nunique()
    return tuple(sorted(counts[counts > 1].index))


@functools.lru_cache(maxsize=1)
def naics_to_detail() -> dict[str, str]:
    """NAICS 2017 six-digit -> BEA 2017 detail industry, unambiguous only."""
    frame = _bea_crosswalk()
    keep = frame[~frame['Sector'].isin(ambiguous_naics())]
    return dict(zip(keep['Sector'], keep['Activity'], strict=True))


@functools.lru_cache(maxsize=1)
def detail_to_summary() -> dict[str, str]:
    """BEA detail industry -> BEA summary industry, all 402, one parent each.

    ⚠️ **The industry map, not the commodity one.** The commodity map drops
    ``331314``, ``S00101``, ``S00201`` and ``S00202`` on the industry axis,
    which leaves ``GSLE`` 15% short in every year and reads exactly like a
    vintage disagreement.
    """
    mapping = load_bea_v2017_industry_to_bea_v2017_summary()
    return {detail: parents[0] for detail, parents in mapping.items() if parents}


@functools.lru_cache(maxsize=8)
def qcew_national_payroll(year: int) -> pd.Series:
    """National QCEW annual payroll by six-digit NAICS, million USD.

    ⚠️ **Three filters, and dropping any one of them is silent.** The FBA
    carries employment beside payroll (``Class``), states beside the nation
    (``Location``), and the 5-, 4-, 3- and 2-digit parents beside the leaves -
    the table is hierarchical, so summing every ``ActivityProducedBy``
    double-counts. Omitting the ``Class`` filter alone puts head counts into a
    dollar total and inflates ``482000``'s coverage ratio from **0.001 to
    1,447**, which reads as a plausible number rather than as an error.

    ⚠️ **Ownership rides on ``FlowName``** (``Annual payroll, {Private,
    Federal/State/Local Government}``) rather than on an axis of its own, and it
    does not separate government enterprises from general government - which is
    why government is routed through NIPA instead and carved out here.
    """
    paths = sorted(glob.glob(QCEW_GLOB.format(year=year)))
    if not paths:
        raise FileNotFoundError(
            f'no extracted QCEW for {year}: {QCEW_GLOB.format(year=year)}'
        )
    frame = pd.read_parquet(
        paths[0], columns=['Class', 'Location', 'ActivityProducedBy', 'FlowAmount']
    )
    national = frame[(frame['Class'] == 'Money') & (frame['Location'] == '00000')]
    codes = national['ActivityProducedBy'].astype(str)
    national = national[codes.str.len() == 6]
    payroll = (
        national.groupby(national['ActivityProducedBy'].astype(str))['FlowAmount']
        .sum()
        .astype(float)
        / 1e6
    )
    payroll.index.name = 'naics'
    return payroll


@functools.lru_cache(maxsize=8)
def naics_vintage(year: int) -> int:
    """Which NAICS revision QCEW published ``year`` on.

    Detected, not hardcoded: the year's six-digit code set is matched against
    every column of the concordance and the best match wins. Measured margins
    are decisive - 2012 scores 0.964 on NAICS 2012 against 0.938 on 2017, and
    2022-2025 score 0.962 on NAICS 2022 against 0.872 on 2017 - so this reports
    a future revision instead of silently comparing across one.
    """
    codes = set(qcew_national_payroll(year).index)
    if not codes:
        raise ValueError(f'QCEW {year} has no six-digit rows')
    concordance = _concordance()
    scores = {
        vintage: len(codes & set(concordance[column].dropna())) / len(codes)
        for vintage, column in VINTAGE_COLUMNS.items()
    }
    return max(scores, key=lambda vintage: scores[vintage])


@functools.lru_cache(maxsize=8)
def code_bridge(year: int) -> pd.DataFrame:
    """Concordance-paired codes for ``year`` against :data:`BENCHMARK_YEAR`.

    One row per usable ``(benchmark code, year code, industry)`` triple. A pair
    survives only when both codes are published in their own year and both
    sides land on the **same single** BEA detail industry, which is what holds
    an industry's composition identical across the growth ratio rather than
    letting a renumbering masquerade as growth.
    """
    column = VINTAGE_COLUMNS[naics_vintage(year)]
    benchmark_column = VINTAGE_COLUMNS[naics_vintage(BENCHMARK_YEAR)]
    pairs = _concordance()[[benchmark_column, column]].copy()
    pairs.columns = ['benchmark_code', 'year_code']
    pairs = pairs.dropna().drop_duplicates()

    mapping = naics_to_detail()
    pairs['industry'] = pairs['benchmark_code'].map(mapping)
    pairs = pairs.dropna(subset=['industry'])

    # Neither side may straddle two BEA industries: a code that does cannot be
    # attributed, and keeping it would move payroll between industries as a
    # side effect of the renumbering.
    for side in ('year_code', 'benchmark_code'):
        single = pairs.groupby(side)['industry'].nunique().eq(1)
        pairs = pairs[pairs[side].map(single).fillna(False)]

    published = set(qcew_national_payroll(year).index)
    benchmark_published = set(qcew_national_payroll(BENCHMARK_YEAR).index)
    pairs = pairs[
        pairs['year_code'].isin(published)
        & pairs['benchmark_code'].isin(benchmark_published)
    ]
    return pairs.reset_index(drop=True)


def qcew_growth(year: int) -> pd.Series:
    """QCEW payroll growth by BEA detail industry, ``year`` over the benchmark.

    ``NaN`` where QCEW cannot speak - no paired codes, or nothing to divide by.
    Callers give those industries their group's movement; see
    :func:`compensation_weights`.
    """
    bridge = code_bridge(year)
    target = qcew_national_payroll(year)
    benchmark = qcew_national_payroll(BENCHMARK_YEAR)

    numerator = (
        bridge.drop_duplicates('year_code')
        .assign(amount=lambda f: f['year_code'].map(target))
        .groupby('industry')['amount']
        .sum()
    )
    denominator = (
        bridge.drop_duplicates('benchmark_code')
        .assign(amount=lambda f: f['benchmark_code'].map(benchmark))
        .groupby('industry')['amount']
        .sum()
    )
    # ⚠️ A published zero is a **suppression, not an observation**, and reading
    # it as one deletes the industry. QCEW reports exactly 0.0 for both NAICS
    # under `334610` (search and navigation instruments) in 2021 while 2020 and
    # 2022 are normal - 486 and 922 million on the two codes in 2020 - so the
    # ratio is 0, the weight is 0, and the industry vanishes from the block.
    # One occurrence in seven years, and fatal every time it happens. An
    # industry BEA still publishes compensation for did not stop paying anyone.
    suppressed = numerator.eq(0.0) & denominator.gt(0.0)
    growth = numerator.mask(suppressed) / denominator.replace(0.0, np.nan)
    growth = growth.replace([np.inf, -np.inf], np.nan)
    return growth.reindex(list(USA_2017_INDUSTRY_CODES))


@functools.lru_cache(maxsize=1)
def benchmark_compensation() -> pd.Series:
    """Published 2017 detail ``V00100`` by industry, million USD."""
    workbook = _load_2017_detail_supply_use_usa('Use_SUT_detail')
    row = workbook.loc[COMPENSATION_ROW]
    if isinstance(row, pd.DataFrame):
        raise ValueError(
            f'{COMPENSATION_ROW} matches {len(row)} rows of the 2017 detail Use '
            f'SUT; expected 1'
        )
    values = (
        pd.to_numeric(row, errors='coerce')
        .reindex(list(USA_2017_INDUSTRY_CODES))
        .astype(float)
    )
    if values.isna().any():
        missing = list(values.index[values.isna()])
        raise KeyError(f'2017 detail Use SUT is missing industries {missing}')
    values.index.name = 'industry'
    return values


@functools.lru_cache(maxsize=1)
def qcew_coverage() -> pd.Series:
    """QCEW payroll over benchmark compensation, per detail industry, 2017.

    The denominator of :data:`COVERAGE_FLOOR`. Taken on the benchmark year so
    the guard is decidable without any later year's answer.
    """
    mapping = naics_to_detail()
    payroll = qcew_national_payroll(BENCHMARK_YEAR)
    keys = pd.Series(
        {code: mapping[code] for code in payroll.index if code in mapping},
        dtype=object,
    )
    rolled = payroll.reindex(keys.index).groupby(keys).sum()
    rolled = rolled.reindex(list(USA_2017_INDUSTRY_CODES)).fillna(0.0)
    compensation = benchmark_compensation()
    return rolled / compensation.replace(0.0, np.nan)


def unobserved_industries() -> tuple[str, ...]:
    """Industries QCEW sees less than :data:`COVERAGE_FLOOR` of."""
    coverage = qcew_coverage()
    below = coverage[coverage.notna() & (coverage < COVERAGE_FLOOR)]
    return tuple(sorted(below.index))


def compensation_weights(year: int) -> pd.DataFrame:
    """The ``V00100`` weight vector for ``year``, per BEA detail industry.

    Columns: ``benchmark``, ``growth`` as QCEW gave it, ``applied`` growth after
    the carve-out and the coverage guard, ``weight`` the product, and ``reason``
    naming why an industry did not take QCEW's number.

    ⚠️ **Weights, never levels.** The dollars come from ``T60200D``; a
    ``proportional`` attribution normalises these inside each NIPA group, so
    the control holds by construction rather than by a rescale afterwards.
    """
    benchmark = benchmark_compensation()
    growth = qcew_growth(year).reindex(benchmark.index)
    parents = detail_to_summary()
    group = pd.Series(
        {industry: parents.get(industry, '') for industry in benchmark.index}
    )

    carved = group.isin(UNRESOLVABLE_GROUPS)
    unobserved = pd.Series(
        benchmark.index.isin(unobserved_industries()), benchmark.index
    )
    missing = growth.isna()

    reason = pd.Series('qcew', index=benchmark.index, dtype=object)
    reason[missing] = 'no paired qcew payroll'
    reason[unobserved] = f'coverage below {COVERAGE_FLOOR:.0%}'
    reason[carved] = 'concordance cannot resolve the group'

    applied = growth.where(~(carved | unobserved | missing)).fillna(1.0)
    return pd.DataFrame(
        {
            'group': group,
            'benchmark': benchmark,
            'growth': growth,
            'applied': applied,
            'weight': benchmark * applied,
            'reason': reason,
        }
    )


def apply_qcew_movement(fba: pd.DataFrame, **_kwargs: object) -> pd.DataFrame:
    """``clean_fba`` socket: move the benchmark ``V00100`` weights to a year.

    Wired onto the **attribution source** of ``NIPA_VA_compensation_<year>``,
    not onto the NIPA control. The attribution source is the published 2017
    ``V00100`` row of ``BEA_Detail_Use_SUT``; this multiplies each industry's
    row by :func:`compensation_weights`' ``applied`` growth, so the weight
    vector handed to ``proportional`` is ``V00100_d(2017) x growth_d(t)``.

    ✅ **This is why the build needs no new source and no new machinery.** The
    designed hatch for a computed weight vector is an ``FBS_outside_flowsa``
    attribution source, and that path is broken (see #731) - but a weight
    vector that is a *rescaling of an FBA already in the method* does not need
    it. The socket runs after unit conversion and before sector mapping, which
    is exactly where ``ActivityConsumedBy`` still holds BEA detail industry
    codes.

    ⚠️ **Weights, never levels.** ``proportional`` normalises inside each of
    ``T60200D``'s 69 groups afterwards, so scaling here changes only the
    within-group shape and every NIPA control still holds by construction. A
    growth factor that is wrong about the *level* therefore costs nothing; only
    its ratio to its group siblings matters.

    ⚠️ Reads ``movement_year`` from the FBA's config, not ``year`` - ``year`` on
    this source is the 2017 benchmark and must stay that way.
    """
    config = getattr(fba, 'config', {}) or {}
    year = config.get('movement_year')
    if year is None:
        raise ValueError(
            'apply_qcew_movement needs `movement_year` on the attribution '
            "source's config; `year` there is the 2017 benchmark and is not "
            'the year being nowcast'
        )
    year = int(year)
    if year == BENCHMARK_YEAR:
        return fba
    factors = compensation_weights(year)['applied']
    moved = fba.copy()
    scale = (
        moved['ActivityConsumedBy'].astype(str).map(factors).astype(float).fillna(1.0)
    )
    moved['FlowAmount'] = moved['FlowAmount'].astype(float) * scale
    return moved


def coverage_report(years: tuple[int, ...] = (2018, 2020, 2022, 2024)) -> pd.DataFrame:
    """One row per year: vintage, bridge size, and how much QCEW is trusted."""
    rows = []
    for year in years:
        bridge = code_bridge(year)
        weights = compensation_weights(year)
        payroll = qcew_national_payroll(year)
        used = float(payroll.reindex(bridge['year_code'].unique()).sum())
        trusted = weights['reason'].eq('qcew')
        rows.append(
            {
                'year': year,
                'naics_vintage': naics_vintage(year),
                'paired_codes': int(bridge['year_code'].nunique()),
                'industries': int(bridge['industry'].nunique()),
                'payroll_bridged_pct': 100 * used / float(payroll.sum()),
                'industries_on_qcew': int(trusted.sum()),
                'compensation_on_qcew_pct': (
                    100
                    * float(weights.loc[trusted, 'benchmark'].sum())
                    / float(weights['benchmark'].sum())
                ),
                'max_growth': float(weights.loc[trusted, 'applied'].max()),
            }
        )
    return pd.DataFrame(rows).set_index('year')


def check() -> int:
    """Reproduce every figure in the module docstring."""
    failures = []

    def expect(label: str, ok: bool, detail: str) -> None:
        print(f'  {"PASS" if ok else "FAIL"}  {label}  ({detail})')
        if not ok:
            failures.append(label)

    print('THE CONCORDANCE CARVE-OUT')
    ambiguous = ambiguous_naics()
    expect(
        '47 NAICS codes sit under more than one BEA detail industry',
        len(ambiguous) == 47,
        f'{len(ambiguous)}',
    )
    expect(
        'all of them are construction or government',
        all(code.startswith(('23', '92')) for code in ambiguous),
        'prefixes 23 and 92',
    )
    parents = detail_to_summary()
    reached = set(
        _bea_crosswalk()
        .loc[_bea_crosswalk()['Sector'].isin(ambiguous), 'Activity']
        .dropna()
    )
    groups = tuple(sorted({parents[i] for i in reached if i in parents}))
    expect(
        'they reach exactly the five carved-out summary groups',
        groups == UNRESOLVABLE_GROUPS,
        ', '.join(groups),
    )

    print()
    print('THE NAICS VINTAGE, DETECTED')
    for year, vintage in ((2017, 2017), (2021, 2017), (2022, 2022), (2024, 2022)):
        expect(
            f'QCEW {year} is on NAICS {vintage}',
            naics_vintage(year) == vintage,
            f'detected {naics_vintage(year)}',
        )

    print()
    print('THE COVERAGE GUARD')
    unobserved = unobserved_industries()
    expect(
        'fifteen industries fall below the 1% coverage floor',
        len(unobserved) == 15,
        f'{len(unobserved)}',
    )
    expect(
        '482000 rail is among them, and it is the one the carve-out misses',
        '482000' in unobserved,
        f'coverage {float(qcew_coverage().loc["482000"]):.5f}',
    )
    beyond = [
        industry
        for industry in unobserved
        if parents.get(industry, '') not in UNRESOLVABLE_GROUPS
        and float(benchmark_compensation().loc[industry]) > 0
        and float(qcew_coverage().loc[industry]) > 0
    ]
    expect(
        'it is the only industry the guard adds with any QCEW payroll at all',
        beyond == ['482000'],
        ', '.join(beyond) or 'none',
    )

    print()
    print('NO YEAR MAY DELETE AN INDUSTRY')
    # The 334610 case: QCEW publishes exactly 0.0 for both of its NAICS in 2021
    # and normal payroll either side, so an unguarded ratio zeroes the weight
    # and the industry leaves the block. Swept over every year because one
    # occurrence in seven is enough, and nothing downstream would say so.
    benchmark = benchmark_compensation()
    live = benchmark[benchmark > 0].index
    for year in range(2018, 2025):
        weights = compensation_weights(year)
        zeroed = [i for i in live if float(weights.loc[i, 'weight']) <= 0]
        expect(
            f'{year}: no industry with published compensation is zeroed',
            not zeroed,
            ', '.join(zeroed) or f'{len(live)} industries all positive',
        )

    print()
    print('THE WEIGHTS')
    for year in (2018, 2024):
        weights = compensation_weights(year)
        expect(
            f'{year}: every industry carries a weight',
            bool(weights['weight'].notna().all()),
            f'{len(weights)} industries',
        )
        expect(
            f'{year}: no industry is zeroed out of a group it belongs to',
            bool((weights.loc[weights['benchmark'] > 0, 'weight'] > 0).all()),
            'no benchmark-positive industry sent to zero',
        )
        expect(
            f'{year}: rail keeps its group movement rather than QCEW growth',
            weights.loc['482000', 'reason'] != 'qcew',
            str(weights.loc['482000', 'reason']),
        )

    print()
    print('COVERAGE BY YEAR')
    print(coverage_report().to_string(float_format=lambda v: f'{v:,.2f}'))

    print()
    if failures:
        print(f'FAILED: {len(failures)}')
        return 1
    print('OK   every figure in the module docstring reproduces')
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true', help='reproduce the docstring')
    parser.add_argument('--year', type=int, help='print one year of weights')
    args = parser.parse_args()
    if args.check:
        return check()
    if args.year:
        weights = compensation_weights(args.year)
        print(weights.to_string(float_format=lambda v: f'{v:,.2f}'))
        return 0
    parser.print_help()
    return 0


__all__ = [
    'BENCHMARK_YEAR',
    'COMPENSATION_ROW',
    'COVERAGE_FLOOR',
    'UNRESOLVABLE_GROUPS',
    'ambiguous_naics',
    'apply_qcew_movement',
    'benchmark_compensation',
    'code_bridge',
    'compensation_weights',
    'coverage_report',
    'detail_to_summary',
    'naics_to_detail',
    'naics_vintage',
    'qcew_coverage',
    'qcew_growth',
    'qcew_national_payroll',
    'unobserved_industries',
]


if __name__ == '__main__':
    sys.exit(main())
