"""NIPA table 6.2D compensation of employees vs Use SUT detail row V00100, 2017.

A worked example of the whole package, and of how far a comparison gets before
you have to say anything by hand.  Two stages:

``stage 1``  nothing but ``leaves()`` + ``rollup``.  61 of 71 summary industries
             pair on name alone and agree to BEA's rounding.
``stage 2``  the ten leftovers, which are not disagreements but places where
             NIPA and the IO accounts cut industries differently: NIPA splits
             wholesale trade into durable/nondurable where summary does not, BEA
             splits real estate into housing/other where NIPA does not, and the
             two carve federal government along different seams (civilian and
             military vs defense and nondefense).

Usage::

    uv run python -m bedrock.analysis.nimble_compare.examples.nipa_compensation_vs_sut_v00100
"""

from __future__ import annotations

import os

from bedrock.analysis.nimble_compare import bea_matrix_row, compare, nipa_sheet

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

    print('=' * 78)
    print('STAGE 1 - name matching only, no hand-written pairs')
    print('=' * 78)
    stage1 = compare(
        candidate=nipa_sheet(SECTION6, SHEET, YEAR).leaves(),
        reference=reference,
        rollup='industry_to_summary',
    )
    print(stage1.report(n_worst=5))

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
