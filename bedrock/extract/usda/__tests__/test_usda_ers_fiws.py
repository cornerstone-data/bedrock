"""The three filtering traps in ``USDA_ERS_FIWS``'s parse.

Each of these was a live bug found on 2026-08-25, and each is silent: the
output looks well-formed either way.  Built on a hand-made frame rather than
the 5.6MB source zip so the test needs no network.
"""

import pandas as pd

from bedrock.extract.usda.USDA_ERS_FIWS import fiws_parse

#: Columns ``fiws_parse`` reads, in the source's own spelling.
COLUMNS = [
    'Year',
    'State',
    'artificialKey',
    'VariableDescriptionTotal',
    'VariableDescriptionPart1',
    'VariableDescriptionPart2',
    'Amount',
    'unit_desc',
    'PublicationDate',
    'Source',
    'ChainType_GDP_Deflator',
]

EXPENSES = 'Intermediate product expenses'

#: ``(total, part1, part2, amount)`` -- the shapes that matter, at 2017 US.
ROWS = [
    # Cash receipts, in both of the source's spellings.
    ('Cash receipts value, soybeans, all', 'Soybeans', 'All', 1000.0),
    ('Cash receipt value, peppers, bell, all', 'Peppers, bell', 'All', 7.0),
    # The published total, and the three groups that must reconcile to it.
    (f'{EXPENSES}, all, excl. operator dwellings', EXPENSES, 'All', 600.0),
    (f'{EXPENSES}, all, incl. operator dwellings', EXPENSES, 'All', 650.0),
    (f'{EXPENSES}, farm origin', EXPENSES, 'Farm origin', 300.0),
    (f'{EXPENSES}, manufactured inputs', EXPENSES, 'Manufactured inputs', 200.0),
    (
        f'{EXPENSES}, other intermediate, excl. operator dwellings',
        EXPENSES,
        'Other intermediate',
        100.0,
    ),
    (
        f'{EXPENSES}, other intermediate, incl. operator dwellings',
        EXPENSES,
        'Other intermediate',
        150.0,
    ),
    # Four series sharing Part2 = 'Miscellaneous' -- the collision.
    (
        f'{EXPENSES}, miscellaneous , excl. operator dwellings',
        EXPENSES,
        'Miscellaneous',
        60.0,
    ),
    (f'{EXPENSES}, miscellaneous, irrigation', EXPENSES, 'Miscellaneous', 5.0),
    (
        f'{EXPENSES}, miscellaneous , insurance premiums',
        EXPENSES,
        'Miscellaneous',
        20.0,
    ),
    # A concept that must NOT survive the filter.
    ('Labor expenses, hired labor', 'Labor expenses', 'Hired labor', 999.0),
]


def _frame() -> pd.DataFrame:
    records = [
        {
            'Year': 2017,
            'State': 'US',
            'artificialKey': index,
            'VariableDescriptionTotal': total,
            'VariableDescriptionPart1': part1,
            'VariableDescriptionPart2': part2,
            'Amount': amount,
            'unit_desc': '$1,000',
            'PublicationDate': '2025-02-01',
            'Source': 'ERS',
            'ChainType_GDP_Deflator': 77.938,
        }
        for index, (total, part1, part2, amount) in enumerate(ROWS)
    ]
    return pd.DataFrame(records, columns=COLUMNS)


def _parsed() -> pd.DataFrame:
    return fiws_parse(df_list=[_frame()], year='2017')


def _expense_amounts() -> dict[str, float]:
    """Expense activity -> dollars, typed, so the assertions stay readable."""
    parsed = _parsed()
    expenses = parsed[parsed['FlowName'] == EXPENSES]
    return dict(
        zip(
            expenses['ActivityProducedBy'].astype(str),
            expenses['FlowAmount'].astype(float),
            strict=True,
        )
    )


def test_intermediate_expenses_survive_the_concept_filter() -> None:
    """The filter was ``Cash receipts`` alone, which dropped this entirely."""
    concepts = set(_parsed()['FlowName'])
    assert EXPENSES in concepts
    assert 'Labor expenses' not in concepts


def test_both_cash_receipt_spellings_survive() -> None:
    """ERS writes bell peppers as ``Cash receipt value``, singular."""
    parsed = _parsed()
    receipts = parsed[parsed['FlowName'].str.startswith('Cash receipt')]
    assert set(receipts['FlowName']) == {'Cash receipts value', 'Cash receipt value'}


def test_operator_dwelling_variants_are_dropped() -> None:
    """Keeping both doubles every paired series on any groupby."""
    parsed = _parsed()
    assert not parsed['Description'].str.contains('incl. operator dwellings').any()
    assert parsed['Description'].str.contains('excl. operator dwellings').any()


def test_groups_reconcile_to_the_published_total() -> None:
    """Farm origin + manufactured + other intermediate == the published all."""
    amounts = _expense_amounts()
    groups = [
        f'{EXPENSES}, farm origin',
        f'{EXPENSES}, manufactured inputs',
        f'{EXPENSES}, other intermediate, excl. operator dwellings',
    ]
    total = amounts[f'{EXPENSES}, all, excl. operator dwellings']
    assert sum(amounts[group] for group in groups) == total


def test_expense_activities_are_unique() -> None:
    """Four series share Part2 = 'Miscellaneous'; the Part1+Part2 name collides."""
    parsed = _parsed()
    expenses = parsed[parsed['FlowName'] == EXPENSES]
    assert expenses['ActivityProducedBy'].is_unique


def test_cash_receipt_activities_keep_the_crosswalk_names() -> None:
    """``Sector_Crosswalk_USDA_ERS_FIWS.csv`` is keyed on these."""
    parsed = _parsed()
    receipts = parsed[parsed['FlowName'].str.startswith('Cash receipt')]
    assert 'Soybeans' in set(receipts['ActivityProducedBy'])


def test_thousands_are_converted_to_dollars() -> None:
    """The source publishes $1,000 units; the FBA is USD."""
    assert set(_parsed()['Unit']) == {'USD'}
    assert _expense_amounts()[f'{EXPENSES}, farm origin'] == 300_000.0
