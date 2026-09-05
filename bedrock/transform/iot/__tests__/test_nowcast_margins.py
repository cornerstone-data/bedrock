"""Tests for the 2017 margin anchor (Step 4c phase 1, #610).

All synthetic: the rate structure is arithmetic over a published table, and the
published table is checked against the Supply columns by
``bedrock/analysis/nowcasting/margins_2017_baseline.py`` instead.
"""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.transform.iot.nowcast_margins import (
    BUYER_LEVEL,
    COMMODITY_LEVEL,
    INVENTORY_BUYER_CODE,
    LEVEL_BASIS,
    MARGIN_TYPE_LEVEL,
    RATE_BASIS,
    margin_bases,
    margin_levels,
    margin_rate_table,
    margin_receiving_sets,
    margins_by_commodity,
    rate_dispersion_by_commodity,
    receiving_set_summary,
)

MARGIN_COLUMNS = [
    "Producers' Value",
    'Transportation',
    'Wholesale',
    'Retail',
    "Purchasers' Value",
]


def margins_frame(
    rows: list[tuple[str, str, float, float, float, float]],
) -> pd.DataFrame:
    """``(buyer, commodity, producers, transport, wholesale, retail)`` rows."""
    frame = pd.DataFrame(
        [row[2:] for row in rows],
        columns=MARGIN_COLUMNS[:4],
        index=pd.MultiIndex.from_tuples(
            [row[:2] for row in rows], names=[BUYER_LEVEL, COMMODITY_LEVEL]
        ),
    )
    frame["Purchasers' Value"] = frame.sum(axis=1)
    return frame


def test_bases_cascade() -> None:
    margins = margins_frame([('111100', '325412', 100.0, 10.0, 20.0, 5.0)])
    bases = margin_bases(margins)
    assert bases['Transportation'].iloc[0] == 100.0
    assert bases['Wholesale'].iloc[0] == 110.0
    assert bases['Retail'].iloc[0] == 130.0


def test_rates_are_margin_over_the_cascading_base() -> None:
    margins = margins_frame([('111100', '325412', 100.0, 10.0, 22.0, 26.4)])
    table = margin_rate_table(margins)
    rates = table.xs(('111100', '325412'))['rate']
    assert rates['Transportation'] == pytest.approx(0.1)  # 10 / 100
    assert rates['Wholesale'] == pytest.approx(0.2)  # 22 / 110
    assert rates['Retail'] == pytest.approx(0.2)  # 26.4 / 132
    assert (table['basis'] == RATE_BASIS).all()


def test_receiving_sets_are_the_non_zero_transactions() -> None:
    margins = margins_frame(
        [
            ('111100', '325412', 100.0, 10.0, 20.0, 0.0),
            ('F01000', '325412', 100.0, 0.0, 20.0, 30.0),
            ('112100', '325412', 100.0, 0.0, 0.0, 0.0),
        ]
    )
    sets = margin_receiving_sets(margins)
    assert list(sets['Transportation']) == [('111100', '325412')]
    assert list(sets['Wholesale']) == [('111100', '325412'), ('F01000', '325412')]
    assert list(sets['Retail']) == [('F01000', '325412')]
    # a transaction with no margin of any kind is in no receiving set
    assert not any(('112100', '325412') in index for index in sets.values())


def test_inventories_are_carried_as_a_level_not_a_rate() -> None:
    margins = margins_frame(
        [
            ('111100', '325412', 100.0, 10.0, 20.0, 0.0),
            (INVENTORY_BUYER_CODE, '325412', 400.0, -8.0, 12.0, 0.0),
        ]
    )
    table = margin_rate_table(margins)
    inventory = table.xs(INVENTORY_BUYER_CODE, level=BUYER_LEVEL)
    assert (inventory['basis'] == LEVEL_BASIS).all()
    assert inventory['rate'].isna().all()
    # the negative margin survives untouched - it is a timing correction
    assert inventory.loc[('325412', 'Transportation'), 'margin'] == -8.0
    assert set(margin_levels(margins).index.get_level_values(BUYER_LEVEL)) == {
        INVENTORY_BUYER_CODE
    }


def test_non_positive_base_is_carried_as_a_level() -> None:
    margins = margins_frame(
        [
            ('F02E00', 'S00402', -1000.0, 40.0, 0.0, 0.0),
            ('111100', '335110', 0.0, 0.0, 0.0, 1.0),
            ('111100', '325412', 100.0, 10.0, 0.0, 0.0),
        ]
    )
    table = margin_rate_table(margins)
    assert table.loc[('F02E00', 'S00402', 'Transportation'), 'basis'] == LEVEL_BASIS
    assert table.loc[('111100', '335110', 'Retail'), 'basis'] == LEVEL_BASIS
    assert table.loc[('111100', '325412', 'Transportation'), 'basis'] == RATE_BASIS
    assert table['rate'].isna().sum() == 2


def test_shares_split_a_commodity_across_its_rate_rows() -> None:
    margins = margins_frame(
        [
            ('111100', '325412', 300.0, 0.0, 30.0, 0.0),
            ('F01000', '325412', 100.0, 0.0, 20.0, 0.0),
            (INVENTORY_BUYER_CODE, '325412', 400.0, 0.0, 40.0, 0.0),
        ]
    )
    wholesale = margin_rate_table(margins).xs('Wholesale', level=MARGIN_TYPE_LEVEL)
    rate_rows = wholesale['basis'] == RATE_BASIS
    assert wholesale.loc[rate_rows, 'base_share'].sum() == 1.0
    assert wholesale.loc[rate_rows, 'margin_share'].sum() == 1.0
    # the level row is excluded from both, so its 400 does not dilute the split
    assert wholesale.loc[('111100', '325412'), 'base_share'] == 0.75
    assert wholesale.loc[('F01000', '325412'), 'margin_share'] == 0.4
    assert (
        wholesale.xs(INVENTORY_BUYER_CODE, level=BUYER_LEVEL)['base_share'].isna().all()
    )


def test_margins_by_commodity_sums_trade_and_transport_separately() -> None:
    margins = margins_frame(
        [
            ('111100', '325412', 100.0, 10.0, 20.0, 0.0),
            ('F01000', '325412', 100.0, 5.0, 15.0, 40.0),
            ('111100', '311111', 100.0, 7.0, 0.0, 0.0),
        ]
    )
    by_commodity = margins_by_commodity(margins)
    assert by_commodity.loc['325412', 'trade_margins'] == 75.0
    assert by_commodity.loc['325412', 'transport_margins'] == 15.0
    assert by_commodity.loc['311111', 'trade_margins'] == 0.0
    # every commodity is present, so a margin-free one reads zero rather than
    # dropping out of a per-commodity check
    assert len(by_commodity) == 402
    assert by_commodity.loc['221100', 'trade_margins'] == 0.0


def test_receiving_set_summary_counts_both_treatments() -> None:
    margins = margins_frame(
        [
            ('111100', '325412', 100.0, 10.0, 20.0, 0.0),
            ('F01000', '325412', 100.0, 0.0, 20.0, 40.0),
            (INVENTORY_BUYER_CODE, '325412', 400.0, -8.0, 0.0, 0.0),
            ('112100', '311111', 50.0, 0.0, 0.0, 0.0),
        ]
    )
    summary = receiving_set_summary(margins)
    transportation = summary.loc['Transportation']
    assert transportation['transactions'] == 2
    assert transportation['transaction_share'] == 0.5
    assert transportation['rate_transactions'] == 1
    assert transportation['level_transactions'] == 1
    assert transportation['margin'] == 2.0
    assert summary.loc['Retail', 'transactions'] == 1
    assert summary.loc['Wholesale', 'buyers'] == 2


def test_dispersion_needs_more_than_one_transaction() -> None:
    margins = margins_frame(
        [
            ('111100', '325412', 100.0, 0.0, 10.0, 0.0),
            ('F01000', '325412', 100.0, 0.0, 30.0, 0.0),
            ('111100', '311111', 100.0, 0.0, 20.0, 0.0),
        ]
    )
    dispersion = rate_dispersion_by_commodity(margins)
    assert list(dispersion.index) == [('Wholesale', '325412')]
    assert dispersion['mean'].iloc[0] == pytest.approx(0.2)  # (0.1 + 0.3) / 2
    assert dispersion['cv'].iloc[0] == pytest.approx(0.5)  # std 0.1 over mean 0.2
