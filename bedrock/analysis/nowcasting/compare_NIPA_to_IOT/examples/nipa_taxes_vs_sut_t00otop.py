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
following the table's own hierarchy.

The table is read from BEA's ``FlatFiles.ZIP``, the archive the ``BEA_NIPA`` FBA
extracts from, rather than from a published ``SectionNall_xls.xlsx`` workbook.
Values, codes and hierarchy are identical; footnotes are the one thing the
archive does not carry, which the last section says out loud.

Usage::

    uv run python -m bedrock.analysis.nowcasting.compare_NIPA_to_IOT.examples.nipa_taxes_vs_sut_t00otop
"""

from __future__ import annotations

import os

import pandas as pd

from bedrock.analysis.nowcasting.compare_NIPA_to_IOT import (
    bea_matrix_column,
    bea_matrix_row,
    compare,
    frame_series,
    nipa_flat_table,
)

#: Table 3.5, read out of the FlatFiles.ZIP the BEA_NIPA FBA extracts from, so
#: this comparison and that FBA cannot drift onto different BEA vintages.
TABLE = 'T30500'
YEAR = 2017

OUT = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'output')

#: The two "Other taxes on production" subtotals -- one under Federal, one under
#: State and local. Everything else in table 3.5 is a tax on products, which
#: belongs to the Use table's T00TOP row instead.
OTHER_TAXES_SUBTOTALS = ['LA000237', 'LA000365']
PRODUCT_TAXES_SUBTOTALS = ['LA000236', 'LA000238']


def main() -> None:
    sheet = nipa_flat_table(TABLE, YEAR, label='NIPA 3.5')
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
    print('SPLITTING THE PRODUCT TAXES: the Supply table has two columns')
    print('=' * 78)
    print(
        'Use SUT carries taxes on products as one T00TOP row, but the Supply table\n'
        'splits the same money by commodity into MDTY (import duties) and TOP\n'
        '(everything else). NIPA 3.5 has customs duties as its own line, so the two\n'
        'sides do decompose -- two cells rather than one total.\n'
    )
    customs = float(sheet.frame.loc[sheet.frame['code'] == 'B235RC', 'value'].iloc[0])
    product_total = sheet.select(PRODUCT_TAXES_SUBTOTALS).total
    candidate = frame_series(
        pd.DataFrame(
            {
                'code': ['MDTY', 'TOP'],
                'name': ['Customs duties', 'Other taxes on products'],
                'value': [customs, product_total - customs],
            }
        ),
        code='code',
        name='name',
        value='value',
        label='NIPA 3.5 product taxes, split',
    )
    reference = frame_series(
        pd.DataFrame(
            {
                'code': ['MDTY', 'TOP'],
                'value': [
                    bea_matrix_column(
                        c, matrix='Supply_SUT_detail', across='commodity'
                    ).total
                    for c in ('MDTY', 'TOP')
                ],
            }
        ),
        code='code',
        value='value',
        label='Supply detail 2017, summed over commodities',
    )
    print(compare(candidate, reference, on='code').report(n_worst=2))

    print()
    print('=' * 78)
    print('WHAT THE RESIDUAL LINES ACTUALLY HOLD')
    print('=' * 78)
    print(
        'Every composition footnote in this table sits on a line named "Other" or\n'
        '"n.i.e" -- the same pattern the hierarchy pass exploits, one level down.\n'
        'The label cannot say what a residual contains, so BEA says it in prose,\n'
        'and that prose is the only basis for ever mapping these onto commodities.\n'
    )
    notes = sheet.annotated(composition_only=True)
    if notes.empty:
        # The price of reading the same archive the FBA reads: FlatFiles.ZIP holds
        # nipadata{A,Q,M}.txt plus the two registers, and the footnote block is in
        # none of them. The residual lines are all still here and still opaque --
        # what is gone is the only text that says what they hold.
        print(
            f'  ...and this table is read from {os.path.basename(str(sheet.meta["source"]))},'
            f' which does not carry that\n'
            f'  prose. The archive publishes values, labels and hierarchy; the footnote\n'
            f'  block belongs to the published workbooks alone. The residual lines are\n'
            f'  all still here and still opaque -- what is missing is the only text\n'
            f'  that describes them, which\n'
            f'  nipa_sheet(Section3all_xls.xlsx, "{TABLE}-A", {YEAR}) still has.'
        )
    else:
        live = notes.loc[notes['value'].notna()]
        for code, name, value, note in zip(
            live['code'], live['name'], live['value'], live['note']
        ):
            print(f'  {code:10} {name:28} {float(value):>10,.0f}')
            print(f'             {note}')
        live_total = float(live['value'].sum())
        table_total = sheet.select(['W056RC']).total
        print(
            f'\n  {len(live)} live residual lines, {live_total:,.0f} million '
            f'({live_total / table_total * 100:.1f}% of the table) '
            f'described only in prose.'
        )
        print(
            f'  ({len(notes) - len(live)} more carry composition notes but are blank '
            f'in {YEAR}.)'
        )

    print()
    print('=' * 78)
    print('WHY NOT MATCH ROW BY ROW')
    print('=' * 78)
    junk = compare(sheet.leaves(), bea_matrix_row('T00OTOP'))
    cells = junk.cells[['candidate_name', 'reference_code', 'candidate', 'reference']]
    print(cells.to_string(index=False) if len(cells) else '  no cells matched')
    print(
        '\nNothing pairs, which is the honest output -- but it is one label away from\n'
        "looking like a result. BEA's detail industry list has a row genuinely named\n"
        '"Customs duties" (4200ID), zero in T00OTOP. The flat file labels NIPA\'s line\n'
        '"Customs and other import duties (G1151)" and so misses it; the published\n'
        'workbook labels the same line "Customs duties", and there the match does fire,\n'
        'pairing 38,513 million of federal customs receipts with an industry row of\n'
        'zero. The pairing would be real and the comparison meaningless -- which is\n'
        'what totals-only reconciliation looks like when forced into cells.'
    )

    os.makedirs(OUT, exist_ok=True)
    path = compare(
        sheet.select(OTHER_TAXES_SUBTOTALS), bea_matrix_row('T00OTOP')
    ).to_csv(os.path.join(OUT, 'nipa_35_vs_T00OTOP.csv'))
    print(f'\nwrote {path}')


if __name__ == '__main__':
    main()
