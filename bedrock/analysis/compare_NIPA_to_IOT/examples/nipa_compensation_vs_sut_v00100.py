"""NIPA table 6.2D compensation of employees vs Use SUT detail row V00100, 2017.

A worked example of the whole package, and of how far a comparison gets before
you have to say anything by hand.

The reference throughout is the **detail** table -- ``V00100`` across 402 detail
industry codes.  What varies is the granularity the comparison is *made* at,
which is the subject of stage 0:

``stage 0``  the detail codes untouched.  Only 20 of 74 NIPA rows find a partner
             and 382 detail codes go unmatched, because NIPA 6.2D has no rows at
             detail granularity -- no "Oilseed farming", no "Offices of
             physicians".  This is the evidence for aggregating, and it also
             shows why the fuzzy pass is opt-in: turned on here it pairs
             "Support activities for mining" with detail code 323120, "Support
             activities for *printing*".
``stage 1``  the detail table rolled up to the 71 summary groups NIPA can
             actually address.  61 pair on name alone and agree to BEA's
             rounding.  Cells keep their detail composition, and 17 of them turn
             out to be 1:1 with a single detail code -- genuine detail-level
             comparisons that the rollup does not blur.
``stage 2``  the ten leftovers, which are not disagreements but places where
             NIPA and the IO accounts cut industries differently: NIPA splits
             wholesale trade into durable/nondurable where summary does not, BEA
             splits real estate into housing/other where NIPA does not, and the
             two carve federal government along different seams (civilian and
             military vs defense and nondefense).

Usage::

    uv run python -m bedrock.analysis.compare_NIPA_to_IOT.examples.nipa_compensation_vs_sut_v00100
"""

from __future__ import annotations

import os

from bedrock.analysis.compare_NIPA_to_IOT import bea_matrix_row, compare, nipa_sheet

SECTION6 = os.path.join(
    os.path.expanduser('~'),
    'Dropbox',
    'professional',
    'resources',
    'BEA',
    'NIPA Survey ALL download 2026-05-18',
    'Section6all_xls.xlsx',
)
SHEET = 'T60200D-A'
YEAR = 2017

OUT = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'output')

# --- stage 2 reconciliation ---------------------------------------------------

# NIPA splits these; BEA summary does not. Keep NIPA's parent, drop its children.
KEEP_PARENTS = ['N4037C']  # Wholesale trade = durable + nondurable
DROP_CHILDREN = [
    'N4038C',  # Wholesale trade / durable goods
    'N4039C',  # Wholesale trade / nondurable goods
    'B4079C',  # Federal general government / civilian
    'W4080C',  # Federal general government / military
    'B4084C',  # State and local general government / education
    'B4085C',  # State and local general government / other
    # The IO accounts cover domestic industries only, so the rest-of-world lines
    # have no counterpart at all. They also carry NIPA's "Less:" sign convention,
    # which this package does not interpret -- left in, they would add rather
    # than net against the total.
    'B4188C',  # Receipts from the rest of the world
    'B4189C',  # Less: Payments to the rest of the world
]
KEEP_FEDERAL_PARENTS = [
    'B568RC',  # Federal general government (= BEA defense + nondefense)
    'B251RC',  # State and local general government (= BEA GSLG)
]

# BEA splits these; NIPA does not. Sum BEA's parts back up.
MERGE_REFERENCE = {
    'RE': ['HS', 'ORE'],  # housing + other real estate -> NIPA "Real estate"
    'GFG': ['GFGD', 'GFGN'],  # federal defense + nondefense -> general government
}
MERGE_NAMES = {
    'RE': 'Real estate',
    'GFG': 'Federal general government',
}

# Pairs no code or name rule reaches: BEA and NIPA simply word these differently,
# and "Government enterprises" appears twice in NIPA so it cannot match on name.
OVERRIDES = {
    'N4055C': '511',  # Publishing industries (includes software)
    'N4058C': '514',  # Information and data processing services
    'A4081C': 'GFE',  # Government enterprises (federal)
    'B4086C': 'GSLE',  # Government enterprises (state and local)
    'B568RC': 'GFG',  # Federal general government
    'B251RC': 'GSLG',  # State and local general government
}


def main() -> None:
    reference = bea_matrix_row('V00100', label='SUT Detail Use 2017 V00100')
    print(f'reference: {reference!r}')

    print()
    print('=' * 78)
    print('STAGE 0 - against the 402 detail codes as published, no rollup')
    print('=' * 78)
    stage0 = compare(
        candidate=nipa_sheet(SECTION6, SHEET, YEAR).leaves(),
        reference=reference,
    )
    t0 = stage0.totals
    print(
        f'{int(t0["n_matched"])} of 74 NIPA rows matched; '
        f'{int(t0["n_reference_only"])} of 402 detail codes unmatched.\n'
        f'NIPA 6.2D simply has no rows at detail granularity, so a cell-by-cell\n'
        f'comparison here is not available -- hence the rollup in stage 1.'
    )
    print('\nthe matches it does find, all 1:1 detail industries:\n')
    print(
        stage0.cells[['candidate_name', 'reference_code', 'candidate', 'reference']]
        .sort_values('reference_code')
        .to_string(index=False)
    )

    print()
    print('=' * 78)
    print('STAGE 1 - detail table rolled up to summary, name matching only')
    print('=' * 78)
    stage1 = compare(
        candidate=nipa_sheet(SECTION6, SHEET, YEAR).leaves(),
        reference=reference,
        rollup='industry_to_summary',
    )
    print(stage1.report(n_worst=5))
    print('\ncells that are 1:1 with a single detail code (no aggregation):\n')
    print(
        stage1.one_to_one()[
            ['candidate_name', 'reference_code', 'detail_members', 'pct_diff']
        ].to_string(index=False)
    )

    print()
    print('=' * 78)
    print('STAGE 2 - partition mismatches reconciled')
    print('=' * 78)
    candidate = nipa_sheet(SECTION6, SHEET, YEAR).leaves(
        keep=KEEP_PARENTS + KEEP_FEDERAL_PARENTS,
        drop=DROP_CHILDREN,
    )
    stage2 = compare(
        candidate=candidate,
        reference=reference,
        rollup='industry_to_summary',
        merge_reference=MERGE_REFERENCE,
        merge_names=MERGE_NAMES,
        overrides=OVERRIDES,
    )
    print(stage2.report(n_worst=10))

    os.makedirs(OUT, exist_ok=True)
    path = stage1.to_csv(os.path.join(OUT, 'nipa_62D_vs_V00100_stage1.csv'))
    print(f'\nwrote {path}')
    path = stage2.to_csv(os.path.join(OUT, 'nipa_62D_vs_V00100_stage2.csv'))
    print(f'wrote {path}')


if __name__ == '__main__':
    main()
