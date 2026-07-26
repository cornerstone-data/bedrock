"""NIPA table 3.5 taxes on production vs Use SUT detail row T00OTOP, 2017.

A different *shape* of comparison from the compensation example, and the reason
this package reports totals separately from cells.

NIPA 3.5 is organized by level of government and kind of tax; ``T00OTOP`` is
organized by industry.  The two share no cell correspondence whatsoever, so
there is nothing to match and the whole answer lives in the totals.  Matching
them row-wise is not merely uninformative but actively misleading: BEA's detail
industry list contains a row genuinely named ``Customs duties`` (4200ID), so a
name match happily pairs NIPA's federal customs receipts with the other-taxes
row of the customs-duties *industry*, which is zero.

The other half of the job is the selection.  NIPA 3.5 covers all taxes on
production and imports, while ``T00OTOP`` is only the non-product part, so the
taxes-on-products branches have to come out first -- ``subtree()`` takes them by
following the sheet's own indentation.

Usage::

    uv run python -m bedrock.analysis.nimble_compare.examples.nipa_taxes_vs_sut_t00otop
"""

from __future__ import annotations

import os

from bedrock.analysis.nimble_compare import bea_matrix_row, compare, nipa_sheet

SECTION3 = os.path.join(
    os.path.expanduser('~'),
    'Dropbox',
    'professional',
    'resources',
    'BEA',
    'NIPA Survey ALL download 2026-05-18',
    'Section3all_xls.xlsx',
)
SHEET = 'T30500-A'
YEAR = 2017

OUT = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'output')

#: The two "Other taxes on production" subtotals -- one under Federal, one under
#: State and local. Everything else in table 3.5 is a tax on products, which
#: belongs to the Use table's T00TOP row instead.
OTHER_TAXES_SUBTOTALS = ['LA000237', 'LA000365']
PRODUCT_TAXES_SUBTOTALS = ['LA000236', 'LA000238']


def main() -> None:
    sheet = nipa_sheet(SECTION3, SHEET, YEAR, label='NIPA 3.5')
    print(f'{sheet!r}  ({sheet.n_missing} blank values)\n')

    for label, codes, row in (
        ('other taxes on production', OTHER_TAXES_SUBTOTALS, 'T00OTOP'),
        ('taxes on products', PRODUCT_TAXES_SUBTOTALS, 'T00TOP'),
    ):
        # take the subtotal rows themselves; their leaves are an equivalent
        # selection and are shown below as a check on the sheet's own arithmetic
        candidate = sheet.select(codes)
        reference = bea_matrix_row(row, label=f'SUT Detail Use 2017 {row}')
        result = compare(candidate, reference)
        totals = result.totals

        print('=' * 78)
        print(f'NIPA 3.5 {label}  vs  Use SUT detail {row}')
        print('=' * 78)
        print(f'  NIPA {label:<28} {totals["candidate_total"]:>14,.0f}')
        print(f'  Use SUT {row:<25} {totals["reference_total"]:>14,.0f}')
        print(
            f'  {"difference":<32} {totals["total_diff"]:>14,.0f}'
            f'   ({totals["total_pct_diff"]:+.4f}%)'
        )
        print(
            f'  {"matched cells":<32} {int(totals["n_matched"]):>14,d}'
            '   <- no cell correspondence exists; totals are the answer'
        )

        # include_parent matters: the federal subtotal has no children of its
        # own, and leaves() keeps a childless parent as the leaf it is
        leaves = sum(
            sheet.subtree(code, include_parent=True, leaves_only=True).total
            for code in codes
        )
        print(
            f'  {"sum of leaves under those rows":<32} {leaves:>14,.0f}'
            '   <- sheet subtotals reconcile'
        )
        print()

    print('=' * 78)
    print('WHY NOT MATCH ROW BY ROW')
    print('=' * 78)
    junk = compare(sheet.leaves(), bea_matrix_row('T00OTOP'))
    print(
        junk.cells[
            ['candidate_name', 'reference_code', 'candidate', 'reference']
        ].to_string(index=False)
    )
    print(
        '\n4200ID is a BEA detail industry genuinely named "Customs duties", so the\n'
        'name match is real and the comparison it produces is meaningless. This is\n'
        'what totals-only reconciliation looks like when forced into cells.'
    )

    os.makedirs(OUT, exist_ok=True)
    path = compare(
        sheet.select(OTHER_TAXES_SUBTOTALS), bea_matrix_row('T00OTOP')
    ).to_csv(os.path.join(OUT, 'nipa_35_vs_T00OTOP.csv'))
    print(f'\nwrote {path}')


if __name__ == '__main__':
    main()
