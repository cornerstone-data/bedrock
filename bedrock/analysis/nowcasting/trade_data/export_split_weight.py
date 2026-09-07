"""A supply weight was splitting export mass, and the benchmark panel says so (#701).

Census publishes some of its trade NAICS as families rather than leaves — a
suppressed-detail residual such as ``33641X``, or a parent this build creates
itself when it consolidates the two vehicle codes (#702). Those rows reach the
crosswalk 1:m, and something has to decide how the mass splits across the BEA
commodities underneath.

#729 chose **same-year total supply, ``T007``**, on the reasoning that a weight
which moves with the year beats a frozen one. Graded on BEA's benchmark panel
that choice loses, and it loses for a reason that was visible from the start.

❌ **A supply weight answers "who produced it". The question is "who exported
it".** Those differ whenever export propensity differs inside a family, and in
aerospace it differs by a factor of eight:

========  =====================================  ========  ========  ========
code      commodity                              phi 2007  phi 2012  phi 2017
========  =====================================  ========  ========  ========
336411    aircraft                                  0.423     0.415     0.391
336412    aircraft engines and parts                0.611     0.631     0.830
336413    other aircraft parts                      0.703     0.658     0.544
336414    guided missiles and space vehicles        0.074     0.078     0.108
33641A    propulsion units for space vehicles       0.133     0.166     0.137
========  =====================================  ========  ========  ========

``phi`` is published exports over published total supply. Guided missiles export
a tenth of what they make and aircraft engines export five-sixths, so weighting
an *export* residual by *supply* hands missiles four times the mass they can
take and starves engines by the same amount. That is #701's ``336412``: 21,476
against a published 35,982, and 29.7% of its own intermediate row.

The measurement
---------------

For each 1:m family, the index of dissimilarity between a candidate weight's
shares and the published export shares — half the sum of absolute differences,
which reads as **the share of that family's export dollars sitting on the wrong
leaf**. Two spans, both genuine holdouts: a weight built from 2007 is scored on
2012, and one built from 2012 is scored on 2017.

Dollar-weighted across every 1:m export family — 163,490 million USD of Census
2017 export mass:

============================  ============  ============
weight                        2007 -> 2012  2012 -> 2017
============================  ============  ============
same-year ``T007`` (#729)           13.67%        20.94%
``T007``(t) x frozen ``phi``         4.98%        12.39%
**prior benchmark exports**          3.21%         4.69%
============================  ============  ============

The frozen export mix wins in 5 of 7 then 6 of 7 families, and on **99.7% then
100% of the dollars**; the only family it loses that carries any mass is cattle
``11211X`` at 414 million USD. The middle arm — keep the supply weight but correct it by
a frozen export propensity — is the informative loser: it wins the first span
outright, then gives it all back on the second. Supply *movement* inside these
families carries no export signal; all ``T007`` contributed was noise on top of
a level that was already wrong.

⚠️ **Two families are 99.7% of the stakes.** ``33641X`` aerospace carries
107,365 million USD and the consolidated vehicle parent ``336110`` carries
55,711; the other five carry 414 between them. This is not a broad reweighting,
it is two families that happen to be very large.

Splitting the residual is not enough — the family has to be consolidated
--------------------------------------------------------------------------

Changing the weight alone still left ``336413`` at 1.18x published, because the
Census leaf rows the residual is added *on top of* are not a representative
sample of the family:

- Census names a leaf for only **10.7%** of 2017 aerospace exports.
- The mix of that 10.7% is **39.6%** away from the published export mix.
  ``336413`` takes 41% of the assigned dollars against a 19% export share; the
  two space leaves take 21% against 2.6%.

So the leaves Census does publish are exactly the ones that must *not* receive
a family-proportional slice of the residual on top. Folding the whole family
onto ``33641X`` and letting the 1:m weight decide all of it — the treatment
#702 already applies to vehicles, for the same reason — is what removes the
bias.

⚠️ **Exports only.** There is no ``33641X`` on the import side; Census publishes
every aerospace leaf directly, so there is no residual to split and no reason
to overwrite an observation. What is wrong there is the c.i.f.-versus-``MCIF``
*level*, which belongs to #670.

What it buys, in dollars
------------------------

Aerospace at 2017, gross error across the five leaves, against a published
family total of 113,759 million USD:

=========================================  ========  ========
rule                                       gross $M    net $M
=========================================  ========  ========
``T007`` 2017, residual only (#729)          35,446    +6,434
``T007`` 2017, minus the space leaves        31,640    +6,434
``T007``(2017) x ``phi``(2012)               21,496    +6,434
frozen 2012 exports, residual only           16,062    +6,434
frozen 2012 exports, consolidated             13,993    +6,434
frozen 2017 exports, residual only             7,282    +6,434
**frozen 2017 exports, consolidated**         6,434    +6,434
=========================================  ========  ========

The shipped rule is the last row, and **its gross equals its net** — every leaf
lands at 1.06x published, the family-level gap and nothing else. The split
error is gone, not reduced.

⚠️ **The last row is anchored, not predicted**, like the frozen 2017 ``MCIF``
weight the import side already uses and like #771's service anchors: 2017 is
matched to published by construction and is not evidence. The honest
out-of-sample figure is the **13,993** two rows above, still 61% below the
shipped weight.

❌ **Restricting the residual to the three aircraft leaves does almost
nothing** (35,446 to 31,640). The tempting classification fix — "``33641X`` is
really aircraft, so drop missiles and space vehicles from its set" — addresses
11% of the error. The problem was never which leaves are in the set, it was the
weight across them.

⚠️ **The scorecard's MATCH count goes the other way, and should be ignored
here.** Splitting only the residual scores two more MATCH cells than
consolidating, because it happens to land ``336411`` at 0.99 and ``336412`` at
1.01. That is cancellation, not accuracy: those leaves' Census direct rows fall
*short* of their family share by almost exactly what the family-level excess
adds. The same cancellation compounds on the other three leaves and puts
``336414`` at 1.80 and ``33641A`` at 1.96.

⚠️ **Net error is +6,434 under every rule, and no split can move it.** Census
reports 120,193 million USD of 2017 aerospace exports against BEA's published
113,759 — a 5.7% level gap on a family whose economy-wide goods counterpart sits
at −0.5%, so it is family-specific, not the general f.a.s.-versus-purchasers
wedge. Not this issue's, and not yet anyone's.

Run::

    uv run python -m bedrock.analysis.nowcasting.trade_data.export_split_weight
    uv run python -m bedrock.analysis.nowcasting.trade_data.export_split_weight --check
    uv run python -m bedrock.analysis.nowcasting.trade_data.export_split_weight --families
"""

from __future__ import annotations

import argparse
import functools
import glob
import sys

import pandas as pd

from bedrock.extract.iot.io_2017 import _load_benchmark_detail_supply_use_usa
from bedrock.transform.trade.utilities import (
    AEROSPACE_CHILDREN,
    AEROSPACE_PARENT,
    consolidate_vehicle_activities,
)

#: BEA publishes 2007, 2012 and 2017 detail on one code basis in a single
#: archive, so the three can be differenced without a crosswalk.
BENCHMARKS: tuple[int, ...] = (2007, 2012, 2017)

#: The holdout spans. A weight built from ``base`` is scored on ``target``, so
#: neither pair can be fitted by choosing on the answer.
SPANS: tuple[tuple[int, int], ...] = ((2007, 2012), (2012, 2017))

#: The Census trade crosswalk whose 1:m rows this module grades.
CROSSWALK = (
    'bedrock/utils/mapping/activitytosectormapping/Sector_Crosswalk_Census_USATrade.csv'
)

#: Census's domestic-export flow (#762), the one the goods method reads.
EXPORT_FLOW = 'ALL_VAL_YR_DOM'

#: The BEA commodities the aerospace family fans out to.
AEROSPACE_LEAVES: tuple[str, ...] = (
    '336411',
    '336412',
    '336413',
    '336414',
    '33641A',
)

#: The two aerospace leaves a classification-only fix would drop from the set.
SPACE_LEAVES: tuple[str, ...] = ('336414', '33641A')

#: The Census codes that carry ``33641A``'s exports before consolidation.
SPACE_CENSUS_CODES: tuple[str, ...] = ('336415', '336419')

#: The names of the two rules the docstring tables are keyed on.
SHIPPED_RULE = 'frozen 2017 exports, consolidated (SHIPPED)'
HOLDOUT_RULE = 'frozen 2012 exports, consolidated (holdout)'
PRIOR_RULE = 'T007 2017, residual only (#729)'


def _cell(
    frame: pd.DataFrame, row: str | int | tuple[str, str], column: str | int
) -> float:
    """One cell of a frame as a float; mypy cannot narrow ``.loc[a, b]``."""
    return float(frame[column].loc[row])


@functools.cache
def _benchmark(column: str, table: str, year: int) -> pd.Series:
    """One published column of the benchmark detail panel, millions of dollars."""
    frame = _load_benchmark_detail_supply_use_usa(table, year)
    frame.columns = frame.columns.str.strip()
    return pd.to_numeric(frame[column], errors='coerce').astype(float)


def exports(year: int) -> pd.Series:
    """Published detail exports ``F04000`` for a benchmark year."""
    return _benchmark('F04000', 'Use_SUT_detail', year)


def total_supply(year: int) -> pd.Series:
    """Published detail total supply ``T007`` for a benchmark year."""
    return _benchmark('T007', 'Supply_detail', year)


def propensity(leaves: tuple[str, ...] = AEROSPACE_LEAVES) -> pd.DataFrame:
    """Export propensity ``F04000 / T007`` per leaf per benchmark.

    The mechanism the whole finding rests on: if propensity were flat inside a
    family, a supply weight and an export weight would agree and #729 would
    have been right.
    """
    codes = list(leaves)
    return pd.DataFrame(
        {
            year: exports(year).reindex(codes) / total_supply(year).reindex(codes)
            for year in BENCHMARKS
        }
    ).rename_axis(index='commodity', columns='benchmark')


@functools.cache
def census_exports() -> pd.Series:
    """Census 2017 domestic exports by activity, after the vehicle consolidation.

    ⚠️ The vehicle consolidation must be applied or ``336110`` reads zero:
    Census publishes the two children and the method relabels them onto the
    parent before attribution, so the parent is 1:m at attribution time even
    though it is absent from the raw extract (#702). The aerospace
    consolidation is deliberately **not** applied here — this function is the
    input the rules are graded on, and one of the rules is not consolidating.
    """
    paths = [
        path
        for path in glob.glob(
            'bedrock/extract/output_data/Census_USATrade_2017_*.parquet'
        )
        if 'metadata' not in path
    ]
    if not paths:
        raise FileNotFoundError('no Census_USATrade_2017 FBA on disk')
    frame = pd.DataFrame(pd.read_parquet(sorted(paths)[-1]))
    frame = consolidate_vehicle_activities(frame)
    frame = frame[frame['FlowName'].astype(str) == EXPORT_FLOW]
    grouped = frame.groupby(frame['ActivityProducedBy'].astype(str))['FlowAmount']
    return grouped.sum() / 1e6


@functools.cache
def one_to_many() -> pd.Series:
    """Census activities the export crosswalk fans out to more than one BEA code."""
    frame = pd.read_csv(CROSSWALK, dtype=str)
    frame = frame[frame['SectorSourceName'] == 'BEA_2017_Code']
    sets = frame.groupby('Activity')['Sector'].apply(lambda s: tuple(sorted(set(s))))
    published = exports(2017).index
    sets = sets.apply(lambda leaves: tuple(c for c in leaves if c in published))
    return sets[sets.apply(len) > 1]


def _shares(values: pd.Series) -> pd.Series:
    total = float(values.sum())
    return values / total if total else values


def _dissimilarity(estimate: pd.Series, actual: pd.Series) -> float:
    """Share of dollars on the wrong leaf, in percent."""
    return float((estimate - actual).abs().sum() / 2 * 100)


def _weights(leaves: list[str], base: int, target: int) -> dict[str, pd.Series]:
    """The candidate weight vectors for one family and one span."""
    base_exports = exports(base).reindex(leaves)
    base_supply = total_supply(base).reindex(leaves)
    target_supply = total_supply(target).reindex(leaves)
    return {
        'same-year T007': target_supply,
        'T007(t) x frozen phi': target_supply * (base_exports / base_supply),
        'prior benchmark exports': base_exports,
    }


def families() -> pd.DataFrame:
    """Every 1:m export family, its Census mass, and each weight's score."""
    rows = []
    census = census_exports()
    for activity, family in one_to_many().items():
        leaves = list(family)
        record: dict[str, object] = {
            'activity': activity,
            'leaves': ' '.join(leaves),
            'census_2017_$M': float(census.get(activity, 0.0)),
        }
        usable = True
        for base, target in SPANS:
            actual = _shares(exports(target).reindex(leaves))
            for name, weight in _weights(leaves, base, target).items():
                if weight.isna().any() or not float(weight.sum()):
                    usable = False
                    break
                record[f'{base}->{target} {name}'] = _dissimilarity(
                    _shares(weight), actual
                )
            if not usable:
                break
        if usable:
            rows.append(record)
    frame = pd.DataFrame(rows).set_index('activity')
    return frame.sort_values('census_2017_$M', ascending=False)


def grade() -> pd.DataFrame:
    """Dollar-weighted dissimilarity per weight per span, and who wins."""
    table = families()
    mass = table['census_2017_$M']
    rows = []
    for base, target in SPANS:
        shipped = table[f'{base}->{target} same-year T007']
        for name in (
            'same-year T007',
            'T007(t) x frozen phi',
            'prior benchmark exports',
        ):
            column = table[f'{base}->{target} {name}']
            rows.append(
                {
                    'span': f'{base}->{target}',
                    'weight': name,
                    'dissimilarity_%': float((column * mass).sum() / mass.sum()),
                    'families_beating_T007': int((column < shipped).sum()),
                    'share_of_dollars_beating_T007': float(
                        mass[column < shipped].sum() / mass.sum()
                    ),
                }
            )
    return pd.DataFrame(rows).set_index(['span', 'weight'])


def _aerospace_inputs() -> tuple[pd.Series, float, pd.Series]:
    """Census direct leaf rows, the residual, and published 2017 exports."""
    census = census_exports()
    direct = pd.Series(
        {
            '336411': float(census['336411']),
            '336412': float(census['336412']),
            '336413': float(census['336413']),
            '336414': float(census['336414']),
            # 336415 and 336419 both map to 33641A on the crosswalk.
            '33641A': float(sum(census[code] for code in SPACE_CENSUS_CODES)),
        }
    )
    published = exports(2017).reindex(list(AEROSPACE_LEAVES))
    return direct, float(census[AEROSPACE_PARENT]), published


def aerospace_arms() -> dict[str, tuple[pd.Series, bool]]:
    """Each candidate rule as ``(weight, consolidated)``.

    ``consolidated`` is the #702 treatment #701 extends to aerospace: fold the
    Census leaf rows into the residual so the 1:m weight decides the whole
    family. Without it only the residual is split and Census's own leaf rows
    stand, biases and all.
    """
    leaves = list(AEROSPACE_LEAVES)
    supply_2017 = total_supply(2017).reindex(leaves)
    aircraft_only = supply_2017.copy()
    aircraft_only[list(SPACE_LEAVES)] = 0.0
    corrected = supply_2017 * (
        exports(2012).reindex(leaves) / total_supply(2012).reindex(leaves)
    )
    frozen_2012 = exports(2012).reindex(leaves)
    frozen_2017 = exports(2017).reindex(leaves)
    return {
        PRIOR_RULE: (supply_2017, False),
        'T007 2017, minus the space leaves': (aircraft_only, False),
        'T007(2017) x phi(2012), residual only': (corrected, False),
        'frozen 2012 exports, residual only': (frozen_2012, False),
        HOLDOUT_RULE: (frozen_2012, True),
        'frozen 2017 exports, residual only': (frozen_2017, False),
        SHIPPED_RULE: (frozen_2017, True),
    }


def aerospace_dollars() -> pd.DataFrame:
    """Realized 2017 aerospace exports under each rule, error against published."""
    direct, residual, published = _aerospace_inputs()
    family_total = float(direct.sum()) + residual
    out = pd.DataFrame({'published': published, 'census_direct': direct})
    for name, (weight, consolidated) in aerospace_arms().items():
        shares = _shares(weight)
        realized = shares * family_total if consolidated else direct + shares * residual
        out[name] = realized - published
    return out


def aerospace_totals() -> pd.DataFrame:
    """Gross and net error per rule, which is how the rules are ranked."""
    errors = aerospace_dollars().drop(columns=['published', 'census_direct'])
    frame = pd.DataFrame({'gross_$M': errors.abs().sum(), 'net_$M': errors.sum()})
    return frame.sort_values('gross_$M', ascending=False)


def census_leaf_bias() -> pd.DataFrame:
    """How unrepresentative Census's own aerospace leaf rows are.

    The justification for consolidating: if the rows Census does assign carried
    the family's mix, leaving them alone and splitting only the residual would
    be right.
    """
    direct, _, published = _aerospace_inputs()
    return pd.DataFrame(
        {
            'census_direct_$M': direct,
            'census_direct_share': _shares(direct),
            'published_$M': published,
            'published_share': _shares(published),
        }
    )


def check() -> int:
    """Assert every figure the module docstring quotes."""
    failures: list[str] = []

    def expect(label: str, ok: bool, detail: str) -> None:
        print(f'  {"PASS" if ok else "FAIL"}  {label}  ({detail})')
        if not ok:
            failures.append(label)

    print('THE MECHANISM: EXPORT PROPENSITY IS NOT FLAT INSIDE A FAMILY')
    phi = propensity()
    spread = float(phi[2017].max() / phi[2017].min())
    expect(
        'aerospace propensity spans a factor of eight at 2017',
        7.0 < spread < 9.0,
        f'{_cell(phi, "336412", 2017):.0%} engines against '
        f'{_cell(phi, "336414", 2017):.0%} missiles, {spread:.1f}x',
    )
    expect(
        'and the ordering holds in all three benchmarks',
        all(
            _cell(phi, '336412', year) > _cell(phi, '336414', year)
            for year in BENCHMARKS
        ),
        'engines above missiles in 2007, 2012 and 2017',
    )

    print()
    print('THE STAKES ARE TWO FAMILIES')
    table = families()
    mass = table['census_2017_$M']
    top_two = float(mass.nlargest(2).sum() / mass.sum())
    expect(
        'aerospace and vehicles are 99.7% of the 1:m export mass',
        top_two > 0.996,
        f'{float(mass.sum()):,.0f} $M total, top two {top_two:.1%}',
    )
    expect(
        'aerospace carries 107,365 $M and vehicles 55,711 $M',
        abs(float(mass[AEROSPACE_PARENT]) - 107365) < 5
        and abs(float(mass['336110']) - 55711) < 5,
        f'{float(mass[AEROSPACE_PARENT]):,.0f} and {float(mass["336110"]):,.0f}',
    )

    print()
    print('THE FROZEN EXPORT MIX WINS BOTH HOLDOUT SPANS')
    scores = grade()
    for (base, target), want_shipped, want_frozen in (
        ((2007, 2012), 13.67, 3.21),
        ((2012, 2017), 20.94, 4.69),
    ):
        span = f'{base}->{target}'
        shipped = _cell(scores, (span, 'same-year T007'), 'dissimilarity_%')
        frozen = _cell(scores, (span, 'prior benchmark exports'), 'dissimilarity_%')
        expect(
            f'{span}: T007 {want_shipped}%, frozen exports {want_frozen}%',
            abs(shipped - want_shipped) < 0.05 and abs(frozen - want_frozen) < 0.05,
            f'{shipped:.2f}% against {frozen:.2f}%',
        )
        expect(
            f'{span}: frozen beats the shipped weight on all but 414 $M',
            _cell(
                scores,
                (span, 'prior benchmark exports'),
                'share_of_dollars_beating_T007',
            )
            > 0.997,
            f'{_cell(scores, (span, "prior benchmark exports"), "families_beating_T007"):.0f}'
            f' of {len(table)} families',
        )
    first = _cell(scores, ('2007->2012', 'T007(t) x frozen phi'), 'dissimilarity_%')
    second = _cell(scores, ('2012->2017', 'T007(t) x frozen phi'), 'dissimilarity_%')
    expect(
        'correcting T007 by propensity wins one span and loses the other',
        first < 5.0 < second,
        f'{first:.2f}% then {second:.2f}%',
    )

    print()
    print('WHY THE FAMILY IS CONSOLIDATED AND NOT JUST THE RESIDUAL')
    bias = census_leaf_bias()
    assigned = float(bias['census_direct_$M'].sum())
    family_total = assigned + _aerospace_inputs()[1]
    drift = float(
        (bias['census_direct_share'] - bias['published_share']).abs().sum() / 2 * 100
    )
    expect(
        'Census names a leaf for only 10.7% of aerospace exports',
        abs(assigned / family_total - 0.107) < 0.002,
        f'{assigned:,.0f} of {family_total:,.0f}',
    )
    expect(
        'and the mix of that 10.7% is 39.6% off the published export mix',
        abs(drift - 39.6) < 0.5,
        f'{drift:.1f}%, 336413 at {_cell(bias, "336413", "census_direct_share"):.0%} '
        f'of assigned dollars against a '
        f'{_cell(bias, "336413", "published_share"):.0%} export share',
    )

    print()
    print('WHAT IT BUYS IN AEROSPACE DOLLARS')
    totals = aerospace_totals()
    prior = _cell(totals, PRIOR_RULE, 'gross_$M')
    holdout = _cell(totals, HOLDOUT_RULE, 'gross_$M')
    ships = _cell(totals, SHIPPED_RULE, 'gross_$M')
    expect(
        'the weight #729 shipped leaves 35,446 $M gross',
        abs(prior - 35446) < 20,
        f'{prior:,.0f}',
    )
    expect(
        'the honest out-of-sample rule cuts that by 61%',
        holdout < 0.42 * prior,
        f'{holdout:,.0f}, {holdout / prior - 1:.0%}',
    )
    expect(
        'and what ships is left with the family level gap and nothing else',
        abs(ships - _cell(totals, SHIPPED_RULE, 'net_$M')) < 1.0,
        f'{ships:,.0f} gross against {_cell(totals, SHIPPED_RULE, "net_$M"):,.0f} net',
    )
    residual_only = _cell(totals, 'frozen 2017 exports, residual only', 'gross_$M')
    expect(
        'splitting only the residual costs 846 $M of avoidable mix error',
        abs((residual_only - ships) - 846) < 20,
        f'{residual_only:,.0f} against {ships:,.0f} consolidated',
    )
    dropped = _cell(totals, 'T007 2017, minus the space leaves', 'gross_$M')
    expect(
        'dropping missiles and space from the set fixes almost nothing',
        (prior - dropped) / prior < 0.15,
        f'{dropped:,.0f}, only {(prior - dropped) / prior:.0%} better',
    )
    nets = totals['net_$M'].round(0).unique()
    expect(
        'net error is identical under every rule, so no split can move it',
        len(nets) == 1 and abs(float(nets[0]) - 6434) < 5,
        f'{float(nets[0]):+,.0f} $M in all {len(totals)} arms',
    )

    print()
    print('THE CONSOLIDATION IS WIRED, AND ON EXPORTS ONLY')
    expect(
        'the crosswalk parent the leaves fold onto is Census own residual',
        AEROSPACE_PARENT == '33641X'
        and set(AEROSPACE_LEAVES) - {'33641A'} <= set(AEROSPACE_CHILDREN),
        f'{AEROSPACE_PARENT} <- {", ".join(AEROSPACE_CHILDREN)}',
    )
    exports_yaml = open(
        'bedrock/transform/trade/Trade_Exports_common.yaml', encoding='utf-8'
    ).read()
    imports_yaml = open(
        'bedrock/transform/trade/Trade_Imports_common.yaml', encoding='utf-8'
    ).read()
    expect(
        'exports use the hook that consolidates aerospace',
        'consolidate_export_activities_fba' in exports_yaml,
        'Trade_Exports_common.yaml',
    )
    expect(
        'imports keep the vehicles-only hook',
        'consolidate_export_activities_fba' not in imports_yaml
        and 'consolidate_vehicle_activities_fba' in imports_yaml,
        'Trade_Imports_common.yaml',
    )

    print()
    if failures:
        print(f'FAILED: {len(failures)}')
        return 1
    print(
        'OK   the export split weight belongs on exports, and every figure reproduces'
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true', help='reproduce the docstring')
    parser.add_argument(
        '--families', action='store_true', help='per-family scores, not just the total'
    )
    args = parser.parse_args()
    if args.check:
        return check()

    print()
    print('Export propensity F04000 / T007, the reason a supply weight cannot work')
    print()
    print(propensity().round(3).to_string())

    print()
    print("Share of a family's export dollars on the wrong leaf, dollar-weighted")
    print()
    print(grade().round(2).to_string())

    print()
    print("Census's own aerospace leaf rows against the published export mix")
    print()
    print(census_leaf_bias().round(4).to_string())

    print()
    print('Aerospace 2017: error per leaf under each rule, $M')
    print()
    print(aerospace_dollars().round(0).to_string())
    print()
    print(aerospace_totals().round(0).to_string())

    if args.families:
        print()
        print('Per family')
        print()
        print(families().round(1).to_string())
    print()
    return 0


if __name__ == '__main__':
    sys.exit(main())
