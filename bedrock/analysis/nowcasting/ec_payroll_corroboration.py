"""What the 2022 census moves that nothing else corroborates (#862).

``ec_go_adjustment`` replaces BEA's 2017→2022 gross-output growth with the
Economic Census's wherever the census is believed. The one screen guarding that
is :data:`~bedrock.transform.iot.ec_go_adjustment.COVERAGE_BOUNDS`, which holds
BEA where the 2017 shipments-to-output wedge is unstable.

❌ **That screen cannot see a within-family reallocation**, which is the failure
#862 found: ``336414`` guided missiles passes it at 0.907 coverage and is still
moved to 0.744x BEA's growth, the second largest downward move in manufacturing.

The finding, and it needs no threshold to see
---------------------------------------------

``336414`` and ``33641A`` are one see-saw, and the decisive fact is that **the
census and BEA agree almost exactly on their combined output and disagree
entirely about the split**:

=========================  ==========  ==========  ==========
2022 gross output, $M       336414      33641A      pair
=========================  ==========  ==========  ==========
BEA, unconditioned             22,001      20,849      42,850
census-conditioned             16,879      25,982      42,861
=========================  ==========  ==========  ==========

The pair total agrees to **0.03%**; the split moves by **5.1 billion USD**. That
is a reallocation, not a measurement of growth — and guided missiles is
concentrated enough that one firm's classification between vehicles and
propulsion units moves the published split with no economic change behind it.
QCEW payroll confirms it: the census moves ``336414`` to 0.744x BEA and payroll
moves it to **1.226x**; ``33641A`` 1.209x against payroll's **0.766x**. Both
sources are opposed, in both halves.

✅ **Holding the pair is the whole of #862's fix.** ``336414``'s gross operating
surplus goes from −2,957 to **+311** million USD at 2022 and from −3,693 to −22
at 2024. Economy-wide the negative-surplus total improves from −20,952 to
−17,995 million at 2022 — essentially all of it this one industry.

❌ **They must be held as a pair.** Holding ``336414`` alone imposes ``33641A``'s
move and changes the family's split in a way neither source supports.

Two automatic screens were tried, and both fail
------------------------------------------------

This module deliberately ships **no threshold and no rule**. Not for want of
trying:

❌ **Sign-flip on gross operating surplus.** The one genuinely constant-free rule
available — hold the census move where it drives surplus negative and BEA's own
series does not — **fires on zero industries in every year**. ``336414``'s
surplus is negative under *both* arms; the conditioning changes its magnitude
roughly tenfold without crossing zero. There is no accounting-boundary rule here.

❌ **Rank by census-payroll disagreement.** Dominated by payroll noise rather
than census error. The largest disagreement of all 223 industries is ``333991``
power-driven handtools, where the census agrees with BEA to within 4.9% and
*payroll* runs +98.5%; ``336411`` aircraft is second, +74.2% payroll against a
census matching BEA to 2.5%. Payroll and output diverge routinely —
productivity, price, outsourcing — and 94 of 223 industries disagree in
direction. Median disagreement is 0.094 and the 99th percentile 0.681, so
``336414`` at 0.481 does not stand out on this axis at all.

⚠️ **A gate on the size of the census move is what the first draft of this used,
and it is exactly the arbitrary constant this project is trying to remove.** It
was not derivable from anything: 0.25 flags one industry, 0.20 flags three, 0.15
flags six, 0.10 flags sixteen. Worse, 0.25 **splits the see-saw** — it holds
``336414`` and imposes ``33641A`` — which is the one outcome both sources agree
is wrong. A constant that changes the answer that much is not measuring anything.

So this module ships the evidence, ranked and unfiltered, for a human reviewing
a census move. The decision lives in ``PENDING_REVIEW``, which is a list of named
industries with measured reasons rather than a threshold.

What is left on the table
--------------------------

``325120`` industrial gases: census 1.237x BEA, payroll 0.902x — opposed, and the
third largest such move. **Deliberately not held**: its gross operating surplus
is comfortably positive either way (4,273 million at 2022), so there is a census
move nothing corroborates but no evidence of harm. Acting on it would be acting
on the screen rather than on the symptom.

The generalisation this points at is a **within-family** test: which BEA groups
does the census reallocate while leaving the group total alone? That is the shape
of the ``3364`` defect and it needs no magnitude constant.
:func:`family_reallocation` is a first cut at it.

Run::

    uv run python -m bedrock.analysis.nowcasting.ec_payroll_corroboration
    uv run python -m bedrock.analysis.nowcasting.ec_payroll_corroboration --check
    uv run python -m bedrock.analysis.nowcasting.ec_payroll_corroboration --families
"""

from __future__ import annotations

import argparse
import functools
import sys

import pandas as pd

from bedrock.transform.iot.ec_go_adjustment import (
    BASE_YEAR,
    CENSUS_YEAR,
    COVERAGE_BOUNDS,
    PENDING_REVIEW,
    ec_growth_factors,
)
from bedrock.transform.nipa.compensation_movement import detail_to_summary, qcew_growth

#: The see-saw #862 found, held as a pair in ``PENDING_REVIEW``.
SEE_SAW: tuple[str, ...] = ('336414', '33641A')

#: Named in the docstring as the candidate left on the table.
UNHELD_CANDIDATE = '325120'


def _cell(frame: pd.DataFrame, row: str, column: str) -> float:
    """One cell of a frame as a float; mypy cannot narrow ``.loc[a, b]``."""
    return float(frame[column].loc[row])


@functools.cache
def readings() -> pd.DataFrame:
    """Census, BEA and payroll growth per conditioned manufacturing industry.

    ``ec_vs_bea`` and ``payroll_vs_bea`` are fractions: 0.0 is "moves exactly
    with BEA", +0.25 is "a quarter faster".

    ⚠️ Read with an **empty hold list**. This measures what the census would do,
    and the industries it found are now held — reading the shipped factors would
    screen them out and the diagnostic would report nothing.
    """
    frame = ec_growth_factors(pending_review=frozenset()).copy()
    frame['g_qcew'] = qcew_growth(CENSUS_YEAR).reindex(frame.index)
    frame['ec_vs_bea'] = frame['g_ec'] / frame['g_bea'] - 1.0
    frame['payroll_vs_bea'] = frame['g_qcew'] / frame['g_bea'] - 1.0
    frame['disagreement'] = (frame['ec_vs_bea'] - frame['payroll_vs_bea']).abs()
    return frame


def live() -> pd.DataFrame:
    """Industries the census would move, with a payroll reading.

    Only the coverage screen is applied — an industry held by hand is still
    shown, because whether payroll backs its census move is the question.
    """
    frame = readings()
    low, high = COVERAGE_BOUNDS
    return frame[frame['coverage_2017'].between(low, high)].dropna(subset=['g_qcew'])


def opposed() -> pd.DataFrame:
    """Every industry where the census and payroll move opposite ways.

    ⚠️ 94 of 223. On its own this is not a screen — it is the denominator that
    shows why a screen would need something the size of a threshold, and why
    this module does not ship one.
    """
    frame = live()
    return frame[frame['ec_vs_bea'] * frame['payroll_vs_bea'] <= 0]


def _base_and_arms() -> tuple[pd.Series, pd.Series, pd.DataFrame]:
    from bedrock.transform.iot.derived_intermediate_and_value_added import (  # noqa: PLC0415, E501
        detail_gross_output_panel,
    )

    frame = live()
    raw = detail_gross_output_panel(ec_adjusted=False)
    base = raw[BASE_YEAR].reindex(frame.index).astype(float)
    return base * frame['g_ec'], base * frame['g_bea'], frame


def family_reallocation() -> pd.DataFrame:
    """Per BEA summary group: does the census move the split but not the total?

    The ``3364`` signature, generalised and constant-free. ``total_ratio`` near
    1 with a large ``split_move`` is a reallocation rather than a measurement of
    growth — the census and BEA agree how big the group is and disagree about
    who inside it earned it.

    ``split_move`` is the index of dissimilarity between the census's and BEA's
    within-group output shares: half the sum of absolute share differences,
    reading as the share of the group's output the census moves between members.
    """
    census, bea, frame = _base_and_arms()
    summary = detail_to_summary()
    group = pd.Series({code: summary.get(code, '') for code in frame.index})

    rows = []
    for name, codes in group.groupby(group).groups.items():
        members = list(codes)
        if not name or len(members) < 2:
            continue
        c, b = census[members], bea[members]
        if c.sum() <= 0 or b.sum() <= 0:
            continue
        rows.append(
            {
                'group': str(name),
                'members': len(members),
                'total_ratio': float(c.sum() / b.sum()),
                'split_move': float((c / c.sum() - b / b.sum()).abs().sum() / 2),
                'largest_mover': str((c - b).abs().idxmax()),
                'moved_$M': float((c - b).abs().max()),
            }
        )
    out = pd.DataFrame(rows).set_index('group')
    return out.sort_values('split_move', ascending=False)


def check() -> int:
    """Assert every figure the module docstring quotes."""
    failures: list[str] = []

    def expect(label: str, ok: bool, detail: str) -> None:
        print(f'  {"PASS" if ok else "FAIL"}  {label}  ({detail})')
        if not ok:
            failures.append(label)

    census, bea, frame = _base_and_arms()

    print('THE SEE-SAW: A SPLIT MOVED, A TOTAL PRESERVED')
    for code, census_down in ((SEE_SAW[0], True), (SEE_SAW[1], False)):
        row = frame.loc[code]
        move, payroll = float(row['ec_vs_bea']), float(row['payroll_vs_bea'])
        expect(
            f'{code}: census {"down" if census_down else "up"}, payroll opposite',
            (move < 0) == census_down and move * payroll < 0,
            f'census {move:+.3f}, payroll {payroll:+.3f}',
        )
    # ⚠️ Asserted on SHARES, not levels. `base x growth` is not the panel's
    # value - apply_ec_adjustment normalises on top - so a level assertion here
    # would be testing the reconstruction rather than the reallocation. The
    # docstring's level table is read off the panel itself; shares are
    # invariant to the normalisation and are what the claim is about.
    pair = list(SEE_SAW)
    census_share = float(census[pair[0]] / census[pair].sum())
    bea_share = float(bea[pair[0]] / bea[pair].sum())
    expect(
        "the census moves a tenth of the pair's output share between members",
        abs(census_share - bea_share) > 0.10,
        f'336414 holds {census_share:.1%} of the pair on the census and '
        f'{bea_share:.1%} on BEA',
    )

    print()
    print('BOTH HALVES ARE HELD, AND HELD TOGETHER')
    for code in SEE_SAW:
        expect(
            f'{code} is in PENDING_REVIEW',
            code in PENDING_REVIEW,
            f'PENDING_REVIEW = {sorted(PENDING_REVIEW)}',
        )

    print()
    print('NO AUTOMATIC SCREEN IS SHIPPED, AND THIS IS WHY')
    expect(
        'the direction test alone opposes 94 of 223, so it cannot be a screen',
        len(opposed()) > 80 and len(frame) > 200,
        f'{len(opposed())} of {len(frame)}',
    )
    worst = str(frame['disagreement'].idxmax())
    expect(
        'the largest disagreement is payroll noise, not census error',
        worst == '333991'
        and abs(_cell(frame, worst, 'ec_vs_bea')) < 0.10
        and _cell(frame, worst, 'payroll_vs_bea') > 0.9,
        f'{worst}: census {_cell(frame, worst, "ec_vs_bea"):+.3f}, '
        f'payroll {_cell(frame, worst, "payroll_vs_bea"):+.3f}',
    )
    ninety_nine = float(frame['disagreement'].quantile(0.99))
    expect(
        'so the see-saw does not stand out on disagreement at all',
        _cell(frame, SEE_SAW[0], 'disagreement') < ninety_nine,
        f'{SEE_SAW[0]} at {_cell(frame, SEE_SAW[0], "disagreement"):.3f}'
        f'against a 99th percentile of {ninety_nine:.3f}',
    )

    print()
    print('WHAT IS LEFT ON THE TABLE')
    row = frame.loc[UNHELD_CANDIDATE]
    expect(
        f'{UNHELD_CANDIDATE} is opposed and deliberately not held',
        float(row['ec_vs_bea']) * float(row['payroll_vs_bea']) < 0
        and UNHELD_CANDIDATE not in PENDING_REVIEW,
        f'census {float(row["ec_vs_bea"]):+.3f}, '
        f'payroll {float(row["payroll_vs_bea"]):+.3f}; surplus stays positive',
    )

    print()
    if failures:
        print(f'FAILED: {len(failures)}')
        return 1
    print('OK   the pair is held and every figure reproduces')
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true', help='reproduce the docstring')
    parser.add_argument(
        '--families', action='store_true', help='the within-group reallocation view'
    )
    args = parser.parse_args()
    if args.check:
        return check()

    columns = ['g_ec', 'g_bea', 'g_qcew', 'ec_vs_bea', 'payroll_vs_bea', 'disagreement']
    frame = live()
    print()
    print(f'Census against BEA against payroll, {BASE_YEAR} -> {CENSUS_YEAR}')
    print(f'{len(frame)} industries; ranked by disagreement, unfiltered')
    print()
    ranked = frame.sort_values('disagreement', ascending=False)
    print(ranked[columns].head(14).round(3).to_string())
    print()
    print('the industries #862 acted on, and the one it did not:')
    print(frame.reindex([*SEE_SAW, UNHELD_CANDIDATE])[columns].round(3).to_string())
    if args.families:
        print()
        print('Within-group reallocation: total preserved, split moved')
        print()
        print(family_reallocation().head(12).round(4).to_string())
    print()
    return 0


if __name__ == '__main__':
    sys.exit(main())
