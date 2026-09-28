"""A_margin from each transaction's own margin commodities (#836)."""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.transform.eeio.derived_cornerstone import (
    _a_margin_from_sector_margins,
    _a_margin_from_type_margins,
)

COMMODITIES = ['g1', 'g2', 'w1', 'w2', 't1', 't2']
GROUPS = {'Wholesale': ['w1', 'w2'], 'Transportation': ['t1', 't2']}

#: Two purchased goods. g1 uses only w1 and t1; g2 splits its margins evenly.
SECTOR = pd.DataFrame(
    {'w1': [20.0, 5.0], 'w2': [0.0, 5.0], 't1': [10.0, 3.0], 't2': [0.0, 3.0]},
    index=['g1', 'g2'],
)
PRODUCERS = pd.Series({'g1': 100.0, 'g2': 50.0})
TYPES = pd.DataFrame(
    {
        "Producers' Value": PRODUCERS,
        'Wholesale': SECTOR[['w1', 'w2']].sum(axis=1),
        'Transportation': SECTOR[['t1', 't2']].sum(axis=1),
    }
).reindex(COMMODITIES, fill_value=0.0)
#: Output shares that differ from either good's own split.
Q = pd.Series({'g1': 1.0, 'g2': 1.0, 'w1': 30.0, 'w2': 70.0, 't1': 50.0, 't2': 50.0})


def test_a_margin_from_sector_margins_is_each_transactions_own_split() -> None:
    a = _a_margin_from_sector_margins(SECTOR, PRODUCERS, COMMODITIES)
    assert a.at['w1', 'g1'] == pytest.approx(0.20)
    assert a.at['w2', 'g1'] == 0.0
    assert a.at['t1', 'g2'] == pytest.approx(3.0 / 50.0)
    # Rows that are not margin commodities stay zero.
    assert float(a.loc[['g1', 'g2']].to_numpy().sum()) == 0.0


def test_type_totals_are_unchanged_only_the_split_moves() -> None:
    new = _a_margin_from_sector_margins(SECTOR, PRODUCERS, COMMODITIES)
    old = _a_margin_from_type_margins(TYPES, Q, GROUPS, COMMODITIES)
    for codes in GROUPS.values():
        pd.testing.assert_series_equal(
            new.loc[codes].sum(), old.loc[codes].sum(), check_names=False
        )
    # The table average gives g1 some of w2 by output share; g1 never paid w2.
    assert old.at['w2', 'g1'] == pytest.approx(0.20 * 0.70)
    assert new.at['w2', 'g1'] == 0.0


def test_zero_producers_value_gives_a_zero_column() -> None:
    a = _a_margin_from_sector_margins(
        SECTOR, pd.Series({'g1': 0.0, 'g2': 50.0}), COMMODITIES
    )
    assert float(a['g1'].sum()) == 0.0
