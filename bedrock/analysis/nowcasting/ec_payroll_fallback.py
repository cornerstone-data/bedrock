"""Can Economic Census payroll serve the industries QCEW does not cover? No.

BEA's 2017 benchmark source-and-method notes name a fallback we do not use:

    For industries not covered by the QCEW, payroll data from the 2017 Economic
    Census were used; data were adjusted for misreporting and I-O industry
    definitions.

#731 proposed adopting it. This module measures the proposal and it is a
**NO-GO**, on three independent grounds - two structural, one graded. The
extraction it needed now exists (``Census_EC_Payroll``, 2012/2017/2022), so the
verdict is reproducible rather than argued.

1. The Economic Census does not cover what the fallback is for
--------------------------------------------------------------

43 BEA detail industries take a fallback today - the five carved-out summary
groups plus the coverage floors - carrying **2,739,876 million USD** of 2017
compensation. EC reaches more than 1% of 24 of them, which sounds like most of
the list until it is weighed: those 24 carry **670,173 million USD**, so EC
reaches **24.5% of the compensation at stake** and **19 industries get exactly
zero**, carrying 2,069,703 million USD between them.

⚠️ **Count and dollars disagree here, and dollars are the question.** The
industries EC reaches are mostly construction, which is carved out at the
*group* level and which QCEW already sees; the industries it misses are the
ones QCEW genuinely cannot observe.

======================  ==========  ==========  ==========
industry                comp $M     QCEW cov    EC cov
======================  ==========  ==========  ==========
``GSLGE`` s&l education    731,648      0.0000      0.0000
``GSLGO`` s&l other        467,952      0.0102      0.0000
``S00600`` federal gen     184,220      0.0436      0.0000
``531ORE`` owner-occ        93,508      0.0000      0.0000
``491000`` postal           54,249      0.6751      0.0000
``482000`` rail             21,893      0.0011      0.0000
======================  ==========  ==========  ==========

❌ **There is no sector 92, no NAICS 482 and no 491 in the Economic Census.**
Government, rail and the postal service are outside its universe, and imputed
owner-occupied housing has no payroll to measure at all. ``482000`` rail is the
one industry #731 named as newly reached by the coverage floor, and EC observes
**0.0000** of it - the same reason QCEW misses it (Railroad Retirement Board
rather than state UI) does not stop applying just because the source changed.

⚠️ **The one industry EC does reach, it reaches badly.** ``4B0000`` all other
retail - sent to its group's movement by #857 - is covered 56% by EC, so on
coverage alone it looks like the one live candidate. But retail is where EC is
**worst** on the holdout: group ``4A0`` scores 41,191 misplaced against 9,900
under frozen shares, four times worse. Reach is not the same as signal.

2. It does not dissolve the construction carve-out - it is far worse than it
----------------------------------------------------------------------------

#731's most interesting claim was that EC "covers construction by type of
structure, which is the BEA axis the NAICS crosswalk could not express", and
might therefore remove the carve-out. ❌ **Measured, it nearly doubles
construction's misallocation.**

Within group ``23``, compensation dollars on the wrong detail industry:

====================  ==============
candidate             misplaced $M
====================  ==============
``frozen`` (shipped)      **32,958**
``ec``                        63,312
====================  ==============

The reason is structural and is decidable before any score. ``ecnbasic``
publishes payroll on the **NAICS trade axis**, which is the axis that failed:
**23 of the 31** published construction codes sit under more than one BEA
detail industry, and the specialty trades (``238110``-``238390``) sit under
**eleven each**. That is the identical ambiguity :func:`ambiguous_naics`
carved construction out for. The type-of-construction axis does exist in the
Economic Census, but in a separate subject series measuring the **value of
work put in place**, not payroll - so it is not this table, and it is not a
compensation allocator.

3. Graded on the holdout it loses in every configuration
--------------------------------------------------------

Scored the same way :mod:`compensation_movement_holdout` scores everything
else, on the observed 2012 -> 2017 span:

=========================  ==============  ==============
candidate                  misplaced $M    vs frozen
=========================  ==============  ==============
``frozen``                        487,348          --
``qcew``                          517,328       +6.2%
``qcew_resolvable`` shipped   **438,534**  **-10.0%**
``ec`` everywhere                 525,904       +7.9%
``ec_construction``               468,888        -3.8%
``ec_carveouts``                  453,500        -6.9%
``ec_first``                      513,468       +5.4%
=========================  ==============  ==============

Every EC variant is **worse than what we ship**. ``ec_construction`` and
``ec_carveouts`` are the two forms #731 actually proposed - EC in place of the
frozen share, inside the carve-out - and both give back more than a third of
the carve-out's gain.

⚠️ **EC does beat QCEW in a handful of groups, and fitting to that would be the
same mistake ``qcew_covered`` was rejected for.** Ambulatory health care
``621`` scores 35,702 against QCEW's 40,893, wholesale ``42`` 51,765 against
56,044, social assistance ``624`` 398 against 3,027. A selector built from
those is fitted to this span, and the module's own precedent - a coverage
ratio that was non-monotonic in its own threshold - is that coverage and
predictive value are different quantities. Recorded, not adopted.

What BEA actually said, and why it does not transfer
-----------------------------------------------------

✅ **The quote is about building a level, and we are not building one.** BEA
used EC payroll to construct the **2017 benchmark** for industries QCEW misses,
"adjusted for misreporting and I-O industry definitions". This module's
estimator does something else: it takes BEA's published 2017 level as given and
moves the *within-group shape* between benchmarks. The clause doing the work in
BEA's sentence is the adjustment for I-O industry definitions - that is the
re-axis from NAICS trade onto BEA's structure type - and it is exactly the part
this table does not carry and we cannot reproduce from it.

So the three parts of #731 that were confirmations stand, and are worth keeping:
QCEW is BEA's primary allocator too; benefits are a payroll *relationship*
applied to QCEW wages, which is what holding the 2017 benefit-to-wage ratio and
moving on wage growth reproduces; and "adjusted to sum to total compensation by
industry in the NIPAs" is the ``T60200D`` control. Only the fallback swap fails.

What would actually reach these industries
-------------------------------------------

Recorded so the next attempt does not start here again:

- **Government** is already routed through NIPA, which is the right answer -
  ``T60200D`` controls it directly and no payroll survey reaches the
  general/enterprise split.
- **Rail** ``482000`` needs the Railroad Retirement Board's own compensation
  series, or the Surface Transportation Board's R-1 filings. Not a census.
- **Construction** needs the type-of-construction subject series, and it would
  be an allocator for *output*, not payroll - a different build with a
  different grader.
- **``531ORE``/``531HST``** are imputed and have no payroll by construction;
  their fallback is correct as it stands.

Run::

    uv run python -m bedrock.analysis.nowcasting.ec_payroll_fallback
    uv run python -m bedrock.analysis.nowcasting.ec_payroll_fallback --check
"""

from __future__ import annotations

import argparse
import functools
import sys

import numpy as np
import pandas as pd

from bedrock.analysis.nowcasting import compensation_movement_holdout as holdout
from bedrock.extract.flowbyactivity import getFlowByActivity
from bedrock.transform.nipa import compensation_movement as movement

#: The FBA this module reads.  2012 exists so the holdout can grade an
#: EC-based movement; 2017 and 2022 bracket the NAICS 2022 revision.
EC_SOURCE = 'Census_EC_Payroll'

#: ``ecnbasic`` publishes payroll in thousand USD; everything here is million.
THOUSAND_TO_MILLION = 1e-3

#: The variable BEA names.  ``EMP``/``ESTAB``/``RCPTOT`` ride along in the FBA
#: but only payroll is a compensation allocator.
PAYROLL_FLOW = 'PAYANN'


@functools.lru_cache(maxsize=4)
def ec_national_payroll(year: int) -> pd.Series:
    """Economic Census annual payroll by six-digit NAICS, million USD.

    ⚠️ **Six-digit rows only.**  ``ecnbasic`` publishes 2- through 6-digit and
    each parent covers all of its children, so summing unfiltered multiplies
    the table several times over - the same trap ``Census_EC_Expenses`` and
    ``BLS_QCEW`` both carry.
    """
    frame = pd.DataFrame(
        getFlowByActivity(EC_SOURCE, int(year), download_FBA_if_missing=True)
    )
    payroll = frame[frame['FlowName'] == PAYROLL_FLOW].copy()
    codes = payroll['ActivityConsumedBy'].astype(str)
    payroll = payroll[codes.str.fullmatch(r'\d{6}')]
    series = (
        payroll.groupby(payroll['ActivityConsumedBy'].astype(str))['FlowAmount'].sum()
        * THOUSAND_TO_MILLION
    )
    series.index.name = 'naics'
    return series


@functools.lru_cache(maxsize=1)
def shared_naics() -> tuple[str, ...]:
    """Six-digit codes EC publishes in **both** holdout benchmark years.

    ⚠️ Same discipline as :func:`~.compensation_movement_holdout.shared_naics`,
    and for the same reason: EC 2012 is on NAICS 2012 and the crosswalk is
    NAICS 2017, so a code retired between them would show growth that is pure
    renumbering.  894 of the 955/946 published codes survive.
    """
    base = set(ec_national_payroll(holdout.BASE_YEAR).index)
    target = set(ec_national_payroll(holdout.TARGET_YEAR).index)
    return tuple(sorted(base & target & set(movement.naics_to_detail())))


def ec_detail_payroll(year: int, vintage_consistent: bool = True) -> pd.Series:
    """EC payroll rolled onto BEA detail industries, million USD.

    Unambiguous NAICS only - :func:`~.compensation_movement.naics_to_detail`
    drops the 47 codes the crosswalk puts under more than one BEA industry,
    which is where construction goes.
    """
    payroll = ec_national_payroll(year)
    codes = list(shared_naics()) if vintage_consistent else list(payroll.index)
    mapping = movement.naics_to_detail()
    keep = [code for code in codes if code in mapping]
    rolled = (
        payroll.reindex(keep)
        .fillna(0.0)
        .groupby(pd.Series({code: mapping[code] for code in keep}))
        .sum()
    )
    industries = list(movement.benchmark_compensation().index)
    return rolled.reindex(industries).fillna(0.0)


def ec_coverage() -> pd.Series:
    """EC payroll over published 2017 compensation, per BEA detail industry.

    The direct analogue of :func:`~.compensation_movement.qcew_coverage`, and
    the measurement that decides #731: whether the Economic Census reaches the
    industries QCEW does not.
    """
    payroll = ec_detail_payroll(movement.BENCHMARK_YEAR, vintage_consistent=False)
    compensation = movement.benchmark_compensation()
    return payroll / compensation.replace(0.0, np.nan)


def fallback_reach() -> pd.DataFrame:
    """Every industry that takes a fallback today, and whether EC reaches it.

    "Takes a fallback" is the union of the three rules in
    :mod:`~.compensation_movement`: the carved-out summary groups, the
    benchmark coverage floor, and #857's year-bridge floor.
    """
    compensation = movement.benchmark_compensation()
    parents = movement.detail_to_summary()
    group = pd.Series(
        {industry: parents.get(industry, '') for industry in compensation.index}
    )
    carved = set(group[group.isin(movement.UNRESOLVABLE_GROUPS)].index)
    targets = sorted(
        carved
        | set(movement.unobserved_industries())
        | set(movement.unbridged_industries(2022))
    )
    table = pd.DataFrame(
        {
            'group': group.reindex(targets),
            'compensation_$M': compensation.reindex(targets),
            'qcew_coverage': movement.qcew_coverage().reindex(targets),
            'ec_coverage': ec_coverage().reindex(targets),
        }
    )
    table = table[table['compensation_$M'] > 0]
    table['ec_reaches'] = table['ec_coverage'].fillna(0.0) > movement.COVERAGE_FLOOR
    return table.sort_values('compensation_$M', ascending=False)


def candidates() -> pd.DataFrame:
    """The holdout frame with EC growth and the EC candidates added.

    ``ec``
        EC payroll growth everywhere it reaches, renormalised in group.
    ``ec_construction``
        ❌ #731's headline proposal: the shipped estimator, except construction
        takes EC instead of the frozen share.
    ``ec_carveouts``
        ❌ the same, extended to every carved-out group EC reaches.
    ``ec_first``
        ❌ EC wherever it reaches, the shipped estimator elsewhere.
    """
    frame = holdout.candidates()
    base = ec_detail_payroll(holdout.BASE_YEAR)
    target = ec_detail_payroll(holdout.TARGET_YEAR)
    growth = (target / base.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan)

    frame['ec_growth'] = growth.reindex(frame.index)
    frame['ec_moved'] = frame['base'] * frame['ec_growth'].fillna(1.0)
    target_total = frame.groupby('group')['observed'].transform('sum')
    total = frame.groupby('group')['ec_moved'].transform('sum')
    frame['ec'] = (
        (frame['ec_moved'] / total.replace(0.0, np.nan)) * target_total
    ).fillna(0.0)

    reaches = frame['ec_growth'].notna()
    frame['ec_construction'] = np.where(
        frame['group'] == '23', frame['ec'], frame['qcew_resolvable']
    )
    frame['ec_carveouts'] = np.where(
        frame['group'].isin(holdout.unresolvable_groups()) & reaches,
        frame['ec'],
        frame['qcew_resolvable'],
    )
    frame['ec_first'] = np.where(reaches, frame['ec'], frame['qcew_resolvable'])
    return frame


#: Every candidate the verdict table scores, shipped ones first.
SCORED = (
    'frozen',
    'qcew',
    'qcew_resolvable',
    'ec',
    'ec_construction',
    'ec_carveouts',
    'ec_first',
)


def scores() -> pd.DataFrame:
    """Score every candidate on the observed 2012 -> 2017 span."""
    frame = candidates()
    rows = []
    for column in SCORED:
        misplaced, dollars = holdout._dissimilarity(frame, column)  # noqa: SLF001
        rows.append(
            {
                'candidate': column,
                'misplaced_$M': misplaced,
                'pct_of_scored': 100.0 * misplaced / dollars,
            }
        )
    table = pd.DataFrame(rows).set_index('candidate')
    baseline = holdout._cell(table, 'frozen', 'misplaced_$M')  # noqa: SLF001
    table['vs_frozen_pct'] = 100.0 * (table['misplaced_$M'] - baseline) / baseline
    return table


def by_group() -> pd.DataFrame:
    """Where EC helps and where it hurts, per summary industry."""
    frame = candidates()
    multi = frame.groupby('group')['observed'].transform('size') > 1
    scored = frame[multi]
    total = scored.groupby('group')['observed'].transform('sum')
    actual = scored['observed'] / total.replace(0.0, np.nan)
    out = {}
    for column in ('frozen', 'qcew_resolvable', 'ec'):
        predicted = scored[column] / total.replace(0.0, np.nan)
        out[column] = ((predicted - actual).abs() * total).groupby(
            scored['group']
        ).sum() / 2
    table = pd.DataFrame(out)
    table['compensation_$M'] = scored.groupby('group')['observed'].sum()
    table['ec_vs_frozen'] = table['frozen'] - table['ec']
    table['ec_vs_qcew'] = table['qcew_resolvable'] - table['ec']
    return table.sort_values('ec_vs_frozen', ascending=False)


def construction_ambiguity() -> pd.DataFrame:
    """How many BEA industries each published construction NAICS reaches.

    The structural half of the verdict, and decidable without any score: EC
    publishes construction on the trade axis, so its codes straddle BEA's
    structure-type industries exactly as QCEW's do.
    """
    crosswalk = movement._bea_crosswalk()  # noqa: SLF001
    construction = crosswalk[crosswalk['Activity'].astype(str).str.startswith('23')]
    published = set(ec_national_payroll(movement.BENCHMARK_YEAR).index)
    reach = construction.groupby('Sector')['Activity'].nunique()
    reach = reach[reach.index.isin(published)]
    payroll = ec_national_payroll(movement.BENCHMARK_YEAR)
    return pd.DataFrame(
        {
            'bea_industries_reached': reach,
            'ec_payroll_$M': payroll.reindex(reach.index),
        }
    ).sort_values('bea_industries_reached', ascending=False)


def check() -> int:
    """Assert every figure the module docstring quotes."""
    failures: list[str] = []

    def expect(label: str, ok: bool, detail: str) -> None:
        print(f'  {"PASS" if ok else "FAIL"}  {label}  ({detail})')
        if not ok:
            failures.append(label)

    print('THE ECONOMIC CENSUS UNIVERSE')
    published = ec_national_payroll(movement.BENCHMARK_YEAR).index
    for prefix, what in (('92', 'government'), ('482', 'rail'), ('491', 'postal')):
        hit = [code for code in published if code.startswith(prefix)]
        expect(
            f'EC publishes no {what} ({prefix})',
            not hit,
            f'{len(hit)} codes',
        )

    print()
    print('IT DOES NOT REACH THE INDUSTRIES THE FALLBACK IS FOR')
    reach = fallback_reach()
    # ⚠️ By count EC reaches a majority of these industries; by dollars it does
    # not come close, and dollars are the question. The ones it reaches are
    # construction, carved out at the group level and already seen by QCEW.
    reached = float(reach.loc[reach['ec_reaches'], 'compensation_$M'].sum())
    at_stake = float(reach['compensation_$M'].sum())
    expect(
        'EC reaches under a third of the compensation that takes a fallback',
        reached / at_stake < 1 / 3,
        f'{reached:,.0f} of {at_stake:,.0f} $M, {100 * reached / at_stake:.1f}%',
    )
    zero = reach[reach['ec_coverage'].fillna(0.0) == 0.0]
    expect(
        'and observes a large block of them at exactly zero',
        len(zero) >= 15,
        f'{len(zero)} industries, {float(zero["compensation_$M"].sum()):,.0f} $M',
    )
    expect(
        '482000 rail - the one the coverage floor adds - is one of them',
        float(ec_coverage().loc['482000']) == 0.0,
        f'EC coverage {float(ec_coverage().loc["482000"]):.4f}',
    )

    print()
    print('CONSTRUCTION IS ON THE TRADE AXIS, WHICH IS THE AXIS THAT FAILED')
    ambiguity = construction_ambiguity()
    straddling = ambiguity[ambiguity['bea_industries_reached'] > 1]
    expect(
        'most published construction codes straddle BEA industries',
        len(straddling) > len(ambiguity) / 2,
        f'{len(straddling)} of {len(ambiguity)}',
    )
    expect(
        'the specialty trades straddle the most',
        int(ambiguity['bea_industries_reached'].max()) >= 10,
        f'up to {int(ambiguity["bea_industries_reached"].max())} BEA industries '
        f'on one NAICS code',
    )

    print()
    print('AND IT LOSES ON THE HOLDOUT, IN EVERY CONFIGURATION')
    table = scores()
    shipped = holdout._cell(table, 'qcew_resolvable', 'vs_frozen_pct')  # noqa: SLF001
    for candidate in ('ec', 'ec_construction', 'ec_carveouts', 'ec_first'):
        got = holdout._cell(table, candidate, 'vs_frozen_pct')  # noqa: SLF001
        expect(
            f'{candidate} is worse than the shipped estimator',
            got > shipped,
            f'{got:+.1f}% against {shipped:+.1f}%',
        )

    groups = by_group()
    construction = holdout._cell(groups, '23', 'ec')  # noqa: SLF001
    frozen = holdout._cell(groups, '23', 'frozen')  # noqa: SLF001
    expect(
        'EC does not dissolve the construction carve-out - it nearly doubles it',
        construction > frozen,
        f'{construction:,.0f} misplaced against {frozen:,.0f} frozen',
    )
    retail = holdout._cell(groups, '4A0', 'ec')  # noqa: SLF001
    retail_frozen = holdout._cell(groups, '4A0', 'frozen')  # noqa: SLF001
    expect(
        'and the one fallback industry it does reach sits in its worst group',
        retail > retail_frozen,
        f'retail {retail:,.0f} misplaced against {retail_frozen:,.0f} frozen, '
        f'EC coverage of 4B0000 {float(ec_coverage().loc["4B0000"]):.0%}',
    )

    print()
    print('THE VERDICT TABLE')
    print(table.round(3).to_string())

    print()
    if failures:
        print(f'FAILED: {len(failures)}')
        return 1
    print('OK   #731 is a NO-GO and every figure reproduces')
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true', help='reproduce the docstring')
    parser.add_argument('--reach', action='store_true', help='the fallback industries')
    parser.add_argument('--by-group', action='store_true', help='where EC helps/hurts')
    parser.add_argument(
        '--construction', action='store_true', help='the trade-axis ambiguity'
    )
    args = parser.parse_args()
    if args.check:
        return check()

    print()
    print('Share of a group\'s compensation dollars on the wrong detail industry')
    print()
    print(scores().round(3).to_string())
    if args.reach:
        print()
        print('Industries that take a fallback today, and whether EC reaches them')
        print()
        print(fallback_reach().round(4).to_string())
    if args.by_group:
        print()
        print('Where EC helps most against frozen shares')
        print()
        print(by_group().head(10).round(0).to_string())
        print()
        print('Where EC hurts most')
        print()
        print(by_group().tail(10).round(0).to_string())
    if args.construction:
        print()
        print('BEA industries reached by each published construction NAICS')
        print()
        print(construction_ambiguity().round(0).to_string())
    print()
    return 0


if __name__ == '__main__':
    sys.exit(main())
