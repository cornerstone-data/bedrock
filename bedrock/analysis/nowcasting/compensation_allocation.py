"""What industry axis do ``V00100`` and ``V00300`` actually have in NIPA?

Step 2's other two rows (#538). ``other_taxes_allocation`` asked which
*allocator* ``T00OTOP`` wants; this asks a different question of the two big
rows, because they are not short of allocators -- they are short of a single
industry partition to allocate *within*. The answer differs sharply between
them, and in both cases it overturns something the plan had settled.

``V00100``: NIPA's axis is the BEA summary axis, verified numerically
--------------------------------------------------------------------

``T60200D`` states compensation for **69 industry groups**, and those groups are
not merely "about BEA summary granularity" -- they *are* the BEA summary
industries. Checked by value rather than by name:

- the 69 leaves **partition all 71 BEA summary industries exactly**, no gaps and
  no overlaps;
- **63 of them equal a summary industry's published compensation to the
  dollar**, and the remainder agree within BEA's own rounding.

So the NIPA axis and the BEA axis are the same axis, and each of the 69 groups
can be its own control. That is what lifts the method above a restatement of the
benchmark: the frozen 2017 shares only have to hold *within* a summary industry,
and in later years each group moves on its own published number.

Three places NIPA and BEA disagree on grain, and how each resolves:

- **Retail** -- NIPA lines 39-42 are taken rather than their parent 38, because
  BEA summary splits retail the same four ways. Likewise transport, information,
  finance, professional services, health care, arts and accommodation.
- **Wholesale** -- the parent (line 35) is taken, because NIPA's
  durable/nondurable wholesale split has no BEA summary counterpart.
- **Government** -- lines 89/90 (civilian/military) and 94/95 (education/other)
  look like finer versions of 88 and 93 and are **different cuts**. NIPA's state
  and local *Education* is 716,832 against the SUT's ``GSLGE`` 731,648, so they
  do not correspond; the parents are taken and the benchmark splits them.

⚠️ Splitting wages from supplements makes it worse, not better
---------------------------------------------------------------

``compensation_disaggregation_plan.md``'s headline decision was to disaggregate
wages (``T60300D``) and supplements (``T61000D`` + ``T61100D``) separately and
sum them, rather than allocating total compensation on wage shares. The
reasoning is sound: supplements-to-wages ratios vary systematically across
industries, so a wage-share split of the total overstates high-wage children.

**The execution is not available, and the plan's own evidence is what shows
it.** ``T60300D`` is line-for-line identical to ``T60200D`` at 69 groups, but
``T61000D`` and ``T61100D`` publish only **16**. Splitting therefore imposes a
16-group supplement rate on industries whose own rates differ -- which is the
same error the decision was written to avoid, one level up. Measured for 2017 by
:func:`supplement_split_cost`:

=================================================  ==========
misplaced by using the 16-group supplement rate     **99,025**
as a share of the row                                 **0.95%**
=================================================  ==========

against a ``T60200D``-only method that reproduces the benchmark exactly. The
worst cases are industries whose supplement rate is far from their group's:

===============================================  =========  =======  ==========
industry                                          own rate    group    shift $M
===============================================  =========  =======  ==========
Administrative and support services                  15.7%    14.0%      -6,898
Computer systems design and related services         11.8%    14.0%      +5,795
Computer and electronic products                     15.9%    19.5%      +5,414
Information and data processing services              8.8%    14.0%      +5,231
Miscellaneous manufacturing                          27.4%    19.5%      -4,285
===============================================  =========  =======  ==========

✅ **So "NIPA publishes both halves by industry" does not help, because it
publishes them at coarser industry grain than the total.** The decision should
be revisited only if the supplements tables ever reach ``T60200D``'s grain,
which they have not in any published year. The plan's underlying concern
survives untouched for the *within-summary* split, where QCEW wage growth is the
allocator -- but there NIPA offers no supplements data at all, so there is
nothing to trade off.

``V00300``: eight controls and no usable axis
----------------------------------------------

Gross operating surplus has no NIPA table of its own. It is assembled from eight
lines across five tables, and that assembly closes to **+13 on 7.87 trillion**.

⚠️ **Four of the eight have an industry table and it still does not add up**,
because the tables publish on **mutually incompatible partitions**:

=====================================  ==========  =====================
component                               $M, 2017   its own industry axis
=====================================  ==========  =====================
Consumption of fixed capital            3,148,953  by legal form; 63 groups for the corporate part only
Corporate profits, domestic             1,726,343  financial/nonfinancial plus 12 industries
Proprietors' income with IVA/CCAdj      1,428,634  21 groups, nonfarm only
Net interest and misc payments            720,494  20 groups
Rental income of persons                  642,028  **none**
Business current transfer payments        142,925  **none**
Statistical discrepancy                    67,902  **none, and not allocable in principle**
Current surplus of govt enterprises        -4,253  by named enterprise (``T30800``)
=====================================  ==========  =====================

Their common refinement is coarser than any one of them, and three components
have no axis at any scope. A component-wise build therefore imposes four
different coarse partitions at once -- the ``V00100`` mistake above, four times
over -- so this module's recommendation is the plain one: eight controls, one
industry distribution, and say so.

✅ **The extractor was built, and it settles the question the other way.**
:mod:`bedrock.extract.bea.BEA_GDPbyIndustry` reads BEA's GDP-by-Industry release
archive -- table ``TVA113``, *Components of Value Added by Industry*, annual,
no API key -- which does state gross operating surplus by industry directly.

⚠️ **But it is the summary Use SUT's ``V003`` row by another door, not an
independent estimate of it.** Measured: **all 71** BEA summary industries' ``V003``
match a ``TVA113`` gross-operating-surplus row *to the dollar*, and all 71
``V001`` match compensation the same way. BEA's industry accounts and the SUTs
are the same estimates published twice.

So the recommendation this section previously made -- buy the extractor and use
it as ``V00300``'s industry axis -- **is only half right**. The extractor is
worth having: it makes the series reachable as an FBA, keyed and versioned,
1997-2024, and it is the natural loader for the *test* set. Consuming it as a
Step 2 **input** is a different decision, because Step 5's Decision 3 holds the
summary SUT out of the target set precisely so it can grade the build. That
trade -- a near-exact ``V00300`` seed at summary grain, paid for with the ability
of summary ``V003`` to grade that seed -- is a judgement about the testing
strategy, not about data availability, and it is left open rather than taken
here.

⚠️ **Nothing may assume positivity.** ``S00201`` state and local passenger
transit carries a ``V00300`` of **-36,919**, and finance and insurance's net
interest is **-156,707**. A share method that clips at zero deletes them
silently.

Usage::

    uv run python -m bedrock.analysis.nowcasting.compensation_allocation
    uv run python -m bedrock.analysis.nowcasting.compensation_allocation --check
"""

from __future__ import annotations

import argparse
import functools
import sys

import pandas as pd

from bedrock.analysis.nowcasting.compare_NIPA_to_IOT import nipa_flat_table
from bedrock.extract.iot.io_2017 import (
    _load_2017_detail_supply_use_usa,
    _load_usa_summary_sut,
)
from bedrock.utils.mapping.write_value_added_crosswalk import (
    COMPENSATION_LINES,
    COMPENSATION_TABLE,
)
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES
from bedrock.utils.taxonomy.bea.v2017_industry_summary import (
    USA_2017_SUMMARY_INDUSTRY_CODES,
)

YEAR = 2017

#: ``T60300D`` is line-for-line identical to ``T60200D``, so one leaf map serves
#: both.  Asserted rather than assumed, in :func:`check`.
WAGES_TABLE = 'T60300D'

#: The two supplements tables, and the industry lines each publishes.  Lines
#: 4-20 in both; the rest of ``T61100D`` is its type-of-fund and benefits-paid
#: panels, which restate the same code and must not be read (#536).
SUPPLEMENT_TABLES = ('T61000D', 'T61100D')

#: Each supplements group against the ``T60200D`` leaf lines inside it.  This is
#: BEA's standard coarse aggregation, and it is what makes the split lossy: 16
#: groups covering the 69 that compensation itself is published on.
SUPPLEMENT_GROUPS: dict[int, tuple[int, ...]] = {
    4: (5, 6),
    5: (8, 9, 10),
    6: (11,),
    7: (12,),
    9: (15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25),
    10: (27, 28, 29, 30, 31, 32, 33, 34),
    11: (35,),
    12: (39, 40, 41, 42),
    13: (44, 45, 46, 47, 48, 49, 50, 51),
    14: (53, 54, 55, 56),
    15: (58, 59, 60, 61, 63, 64),
    16: (66, 67, 68, 69, 71, 72),
    17: (73, 75, 76, 77, 78),
    18: (80, 81, 83, 84),
    19: (85,),
    20: (88, 91, 93, 96),
}

#: The ``V00300`` assembly: label, table, code, and the finest industry axis that
#: component's own tables offer.  ``None`` means no industry axis at any scope.
SURPLUS_ASSEMBLY: tuple[tuple[str, str, str, str | None], ...] = (
    ('Consumption of fixed capital', 'T70500', 'A262RC', 'T62200D, corporate only'),
    ('Corporate profits, domestic', 'T61600D', 'A445RC', 'T61600D'),
    ("Proprietors' income with IVA/CCAdj", 'T11000', 'A041RC', 'T61200D, nonfarm only'),
    ('Net interest and misc payments', 'T11000', 'W272RC', 'T61500D'),
    ('Rental income of persons', 'T70900', 'A048RC', None),
    ('Business current transfer payments', 'T70700', 'B029RC', None),
    ('Statistical discrepancy', 'T11000', 'A030RC', None),
    ('Current surplus of govt enterprises', 'T11000', 'A108RC', 'T30800'),
)

#: The split is not worth its cost while it misplaces more than this share of the
#: row.  Measured 0.95%; the bar is set well above that so it fails only on a
#: real change in the supplements tables' grain, not on noise.
SPLIT_COST_BAR = 0.002

#: How many of the 69 leaves must equal a summary industry's published
#: compensation exactly.  Measured 63; the rest differ by BEA's own rounding.
EXACT_LEAF_MATCHES = 60


@functools.cache
def _use() -> pd.DataFrame:
    """The 2017 detail Use SUT."""
    return _load_2017_detail_supply_use_usa('Use_SUT_detail')


def industries() -> list[str]:
    """The 402 BEA 2017 detail industry codes."""
    return [str(code) for code in USA_2017_INDUSTRY_CODES]


def use_row(row: str) -> 'pd.Series[float]':
    """One Use SUT row across the 402 detail industries, $M."""
    series = _use().loc[row]
    assert isinstance(series, pd.Series)
    return series.reindex(industries()).astype(float).fillna(0.0)


def nipa_lines(table: str, year: int = YEAR) -> dict[int, float]:
    """A NIPA table as line number against value."""
    frame = nipa_flat_table(table, year).frame
    return dict(zip(frame['line'].astype(int), frame['value'].astype(float)))


def nipa_names(table: str, year: int = YEAR) -> dict[int, str]:
    """A NIPA table as line number against label."""
    frame = nipa_flat_table(table, year).frame
    return dict(zip(frame['line'].astype(int), frame['name'].astype(str)))


def summary_compensation(year: int = YEAR) -> 'pd.Series[float]':
    """``V001`` by BEA summary industry, from the summary Use SUT.

    Used only as *evidence* that the NIPA leaves are the summary industries --
    never as an input to a method, since Step 5's Decision 3 holds the summary
    SUT in the test set.
    """
    table = _load_usa_summary_sut('Use_SUT_summary', year)  # type: ignore[arg-type]
    codes = [code for code in USA_2017_SUMMARY_INDUSTRY_CODES if code in table.columns]
    series = table.loc['V001']
    assert isinstance(series, pd.Series)
    return pd.to_numeric(series.reindex(codes), errors='coerce').fillna(0.0)


def leaf_partition() -> dict[str, object]:
    """Do the 69 ``T60200D`` leaves partition the 71 summary industries?"""
    used = [code for codes in COMPENSATION_LINES.values() for code in codes]
    summary = list(USA_2017_SUMMARY_INDUSTRY_CODES)
    return {
        'leaves': len(COMPENSATION_LINES),
        'summary_codes': len(summary),
        'reached': len(set(used)),
        'duplicated': sorted({code for code in used if used.count(code) > 1}),
        'unreached': sorted(set(summary) - set(used)),
        'not_summary_codes': sorted(set(used) - set(summary)),
    }


def leaf_alignment(year: int = YEAR) -> pd.DataFrame:
    """Each single-summary leaf against that summary industry's published total.

    Only the leaves mapping to exactly one summary code are comparable; the two
    that span a pair (real estate, federal general government) are compared as a
    pair.
    """
    values = nipa_lines(COMPENSATION_TABLE, year)
    names = nipa_names(COMPENSATION_TABLE, year)
    published = summary_compensation(year)
    rows = []
    for line, summaries in COMPENSATION_LINES.items():
        available = [code for code in summaries if code in published.index]
        if not available:
            continue
        rows.append(
            {
                'line': line,
                'name': names[line],
                'summary': '+'.join(summaries),
                'nipa': values[line],
                'published': float(published[available].sum()),
            }
        )
    return pd.DataFrame(rows).assign(diff=lambda x: x['nipa'] - x['published'])


def supplement_split_cost(year: int = YEAR) -> pd.DataFrame:
    """What splitting wages from supplements misplaces, industry by industry.

    For a summary industry ``s`` inside supplements group ``g``, a split build
    gives it ``wages_s + supplements_g x (V_s / V_g)``, i.e. its own wage bill
    plus the *group's* supplement rate applied to its compensation. The published
    answer is ``V_s``. The shift is therefore ``V_s x (rate_g - rate_s)`` -- the
    gap between the group's supplement rate and the industry's own.
    """
    comp = nipa_lines(COMPENSATION_TABLE, year)
    wage = nipa_lines(WAGES_TABLE, year)
    names = nipa_names(COMPENSATION_TABLE, year)
    supplements = [nipa_lines(table, year) for table in SUPPLEMENT_TABLES]

    rows = []
    for group, leaves in SUPPLEMENT_GROUPS.items():
        group_compensation = sum(comp[line] for line in leaves)
        group_supplements = sum(table[group] for table in supplements)
        group_rate = group_supplements / group_compensation
        for line in leaves:
            own_rate = 1 - wage[line] / comp[line]
            rows.append(
                {
                    'line': line,
                    'name': names[line],
                    'compensation': comp[line],
                    'own_rate': own_rate,
                    'group_rate': group_rate,
                    'shift': comp[line] * (group_rate - own_rate),
                }
            )
    return pd.DataFrame(rows)


def split_cost_summary(year: int = YEAR) -> dict[str, float]:
    """The split's total misplacement, in dollars and as a share of the row."""
    frame = supplement_split_cost(year)
    total = float(frame['compensation'].sum())
    misplaced = float(frame['shift'].abs().sum())
    return {
        'row': total,
        'misplaced': misplaced,
        'share': misplaced / total,
        'groups': float(len(SUPPLEMENT_GROUPS)),
        'leaves': float(len(frame)),
    }


def surplus_assembly(year: int = YEAR) -> pd.DataFrame:
    """The eight ``V00300`` controls, their size, and their industry axis."""
    rows = []
    for label, table, code, axis in SURPLUS_ASSEMBLY:
        frame = nipa_flat_table(table, year).frame
        match = frame.loc[frame['code'] == code]
        if len(match) != 1:
            raise ValueError(
                f'{code} appears {len(match)} times in {table}@{year}; the '
                f'assembly selects by line for exactly this reason (#536)'
            )
        rows.append(
            {
                'component': label,
                'source': f'{table} {code}',
                'line': int(match['line'].iloc[0]),
                'value': float(match['value'].iloc[0]),
                'industry_axis': axis or '(none)',
            }
        )
    frame = pd.DataFrame(rows)
    return frame.assign(share=lambda x: x['value'] / x['value'].sum())


def report() -> None:
    """Print both rows' findings."""
    partition = leaf_partition()
    print(
        f'V00100 -- {COMPENSATION_TABLE} leaves: {partition["leaves"]}, reaching '
        f'{partition["reached"]} of {partition["summary_codes"]} BEA summary '
        f'industries'
    )
    print(
        f'  duplicated {partition["duplicated"] or "none"}   unreached '
        f'{partition["unreached"] or "none"}'
    )
    alignment = leaf_alignment()
    exact = int((alignment['diff'] == 0).sum())
    print(
        f'  leaves equal to the published summary compensation exactly: {exact} '
        f'of {len(alignment)};  largest disagreement '
        f'{alignment["diff"].abs().max():,.0f}'
    )

    cost = split_cost_summary()
    print(
        f'\nWages/supplements split: {int(cost["leaves"])} compensation groups '
        f'against {int(cost["groups"])} supplements groups'
    )
    print(
        f'  misplaced {cost["misplaced"]:,.0f} of {cost["row"]:,.0f} = '
        f'{cost["share"]:.2%} of the row -- so the split is not used'
    )
    frame = supplement_split_cost()
    worst = frame.reindex(frame['shift'].abs().sort_values(ascending=False).index)
    print(f'  {"industry":<48} {"own":>7} {"group":>7} {"shift $M":>10}')
    for _, row in worst.head(8).iterrows():
        print(
            f'  {row["name"][:46]:<48} {row["own_rate"]:>6.1%} '
            f'{row["group_rate"]:>7.1%} {row["shift"]:>+10,.0f}'
        )

    print('\nV00300 -- eight controls across five tables:')
    assembly = surplus_assembly()
    for _, row in assembly.iterrows():
        print(
            f'  {row["component"]:<36} {row["value"]:>11,.0f} {row["share"]:>6.1%}  '
            f'{row["source"]:<15} axis: {row["industry_axis"]}'
        )
    published = use_row('V00300')
    print(
        f'  assembly {assembly["value"].sum():,.0f} against a published '
        f'{published.sum():,.0f}   diff '
        f'{assembly["value"].sum() - published.sum():,.0f}'
    )
    negative = published[published < 0]
    print(
        f'  industries with a negative surplus: '
        f'{ {code: f"{value:,.0f}" for code, value in negative.items()} }'
    )


def check() -> int:
    """Assert the findings the two methods rest on."""
    failures = []

    partition = leaf_partition()
    if partition['duplicated']:
        failures.append(
            f'{COMPENSATION_TABLE} leaves now reach a summary industry twice: '
            f'{partition["duplicated"]}'
        )
    if partition['unreached']:
        failures.append(f'summary industries no leaf reaches: {partition["unreached"]}')
    if partition['not_summary_codes']:
        failures.append(
            f'COMPENSATION_LINES names codes that are not summary industries: '
            f'{partition["not_summary_codes"]}'
        )

    alignment = leaf_alignment()
    exact = int((alignment['diff'] == 0).sum())
    if exact < EXACT_LEAF_MATCHES:
        failures.append(
            f'only {exact} leaves still equal the published summary compensation '
            f'exactly, against {EXACT_LEAF_MATCHES} expected; the NIPA and BEA '
            f'industry axes may have diverged'
        )

    # T60300D has to stay line-for-line identical to T60200D, or the split
    # measurement below is comparing different partitions.
    comp_names = nipa_names(COMPENSATION_TABLE)
    wage_names = nipa_names(WAGES_TABLE)
    mismatched = [
        line
        for line in COMPENSATION_LINES
        if comp_names.get(line) != wage_names.get(line)
        and line not in (88, 93)  # both tables word the government lines differently
    ]
    if mismatched:
        failures.append(
            f'{WAGES_TABLE} no longer shares {COMPENSATION_TABLE}\'s line '
            f'structure at lines {mismatched}'
        )

    cost = split_cost_summary()
    if cost['share'] < SPLIT_COST_BAR:
        failures.append(
            f'the wages/supplements split now costs only {cost["share"]:.2%} of '
            f'the row; it may be worth adopting after all'
        )

    assembly = surplus_assembly()
    published = float(use_row('V00300').sum())
    diff = abs(float(assembly['value'].sum()) - published)
    if diff > 100:
        failures.append(
            f'the V00300 assembly is {diff:,.0f} from the published row; one of '
            f'the eight lines is no longer the right one'
        )

    if float(use_row('V00300').min()) >= 0:
        failures.append(
            'no industry carries a negative V00300 any more; the '
            'no-clipping requirement may have lapsed and should be rechecked '
            'rather than assumed'
        )

    if failures:
        print(f'{len(failures)} finding(s) no longer hold:')
        for failure in failures:
            print(f'  - {failure}')
        return 1
    print(
        f'V00100: the {partition["leaves"]} NIPA leaves partition the '
        f'{partition["summary_codes"]} summary industries, {exact} of them '
        f'exactly, and the wages/supplements split still costs '
        f'{cost["share"]:.2%} of the row. V00300: the eight-line assembly closes '
        f'to {diff:,.0f} and still carries a negative industry.'
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--check',
        action='store_true',
        help='assert the findings rather than printing the report',
    )
    args = parser.parse_args()
    if args.check:
        return check()
    report()
    return 0


if __name__ == '__main__':
    sys.exit(main())
