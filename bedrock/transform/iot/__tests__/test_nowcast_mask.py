"""Tests for nowcast Supply/Use mask sourcing.

Synthetic, following ``test_nowcast_margins.py``: every layer is a rule over a
panel, so the rules are checked against injected panels small enough to read.
The 2017 measurements they encode - 17 fixed cells at 5.1% of the Use panel's
mass, 897 sign-locked Supply cells - are asserted against the published tables
by ``mask_layer_feasibility.py --check`` instead.
"""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.transform.iot.nowcast_mask import (
    BLOCKS,
    EXCLUDED_COMMODITIES,
    ONE_TO_ONE_FD,
    SIGN_LOCKED_SUPPLY_COLUMNS,
    SIGN_LOCKED_USE_ROWS,
    SUPPLY_BRIDGE_COLUMNS,
    VA_ROWS,
    balance_commodities,
    balance_industries,
    build_sut_mask,
    fixed_value_mask,
    panel_labels,
    sign_lock_mask,
    structural_zero_mask,
)
from bedrock.utils.economic.balance.mask import assert_subsidies_negative
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES


def _use_panel() -> pd.DataFrame:
    """Two commodities and the VA rows, by two industries and three FD codes."""
    rows = ['111120', '111130', *VA_ROWS]
    columns = ['1111A0', '1111B0', 'F01000', 'F06C00', 'F06N00']
    panel = pd.DataFrame(0.0, index=rows, columns=columns)
    panel.loc['111120', ['1111A0', '1111B0', 'F01000']] = [10.0, 5.0, 20.0]
    panel.loc['111130', ['1111A0', 'F06C00']] = [4.0, 7.0]
    panel.loc['111120', 'F06N00'] = 3.0
    panel.loc['V00100', ['1111A0', '1111B0']] = [6.0, 2.0]
    panel.loc['V00300', '1111A0'] = -1.0  # legitimately negative, not locked
    panel.loc['T00SUB', '1111A0'] = -8.0  # already on the balance's convention
    return panel


def _supply_panel() -> pd.DataFrame:
    rows = ['111120', '111130']
    columns = ['1111A0', '1111B0', *SUPPLY_BRIDGE_COLUMNS]
    panel = pd.DataFrame(0.0, index=rows, columns=columns)
    panel.loc['111120', '1111A0'] = 30.0
    panel.loc['111130', '1111B0'] = 12.0
    panel.loc['111120', ['TRADE ', 'TRANS', 'TOP', 'SUB', 'MADJ']] = [
        5.0,
        -2.0,  # the margin give-up side
        1.0,
        -3.0,
        -1.0,
    ]
    return panel


# --------------------------------------------------------------------------
# Tier 4 - what the balance does not carry
# --------------------------------------------------------------------------


def test_4200id_is_excluded_as_a_commodity_but_kept_as_an_industry() -> None:
    """A code can mean different things on the two axes, and this one does.

    ``4200ID`` is customs duties. It produces no commodity, so its row is
    empty - but as an industry it carries the duty as ``T00TOP`` = ``VAPRO`` =
    38,513, which is its published detail gross output and the Supply ``MDTY``
    total to rounding. Excluding it from both axes drops a $38.5B hard
    constraint.
    """
    assert '4200ID' in EXCLUDED_COMMODITIES
    assert '4200ID' not in balance_commodities()
    assert '4200ID' in balance_industries()

    use_rows, use_columns = panel_labels('use')
    assert '4200ID' not in use_rows
    assert '4200ID' in use_columns


def test_s00900_is_dropped_and_was_never_an_industry() -> None:
    assert 'S00900' not in balance_commodities()
    assert 'S00900' not in balance_industries()  # commodity-only, so a no-op


def test_the_industry_axis_excludes_nothing() -> None:
    """Gross output is published for all 402, so all 402 carry a target."""
    assert len(balance_industries()) == 402


def test_the_two_axes_are_not_the_same_set() -> None:
    """The panel is not square, and nothing downstream may assume it is.

    Four codes are industry-only and four commodity-only. A balance that
    reindexed one axis onto the other would silently drop live industries
    (``331314`` alone buys 5,100 of intermediates) or invent commodity rows.
    """
    # The asymmetry BEA ships, before any exclusion of ours.
    published_commodities = set(USA_2017_COMMODITY_CODES)
    published_industries = set(USA_2017_INDUSTRY_CODES)
    assert published_industries - published_commodities == {
        '331314',
        'S00101',
        'S00201',
        'S00202',
    }
    assert published_commodities - published_industries == {
        'S00300',
        'S00401',
        'S00402',
        'S00900',
    }

    # And after Tier 4: 4200ID joins the industry-only side, because it is
    # excluded as a commodity and kept as an industry.
    commodities = set(balance_commodities())
    industries = set(balance_industries())
    assert industries - commodities == {
        '331314',
        'S00101',
        'S00201',
        'S00202',
        '4200ID',
    }
    assert commodities - industries == {'S00300', 'S00401', 'S00402'}

    rows, columns = panel_labels('supply')
    assert len(rows) != len(columns)


def test_panel_labels_carry_no_subtotals() -> None:
    """``T007``/``T013``/``T016`` and friends are derived, not solved for."""
    for block in BLOCKS:
        rows, columns = panel_labels(block)
        for subtotal in ('T001', 'T005', 'T007', 'T013', 'T014', 'T015', 'T016'):
            assert subtotal not in rows
            assert subtotal not in columns


# --------------------------------------------------------------------------
# Tier 0 - structural zeros
# --------------------------------------------------------------------------


def test_structural_zeros_are_the_zero_cells() -> None:
    panel = _use_panel()
    zeros = structural_zero_mask('use', panel)
    assert zeros.loc['111130', '1111B0']
    assert not zeros.loc['111120', '1111A0']
    # The value-added by final-demand corner is structurally empty.
    assert zeros.loc['V00100', 'F01000']


# --------------------------------------------------------------------------
# Tier 1 - fixed values
# --------------------------------------------------------------------------


def test_only_nonzero_cells_of_the_one_to_one_columns_are_fixed() -> None:
    panel = _use_panel()
    fixed = fixed_value_mask('use', 2017, panel)
    assert fixed.loc['111130', 'F06C00']
    assert fixed.loc['111120', 'F06N00']
    # zero cells of a 1:1 column are pattern, not measurement
    assert not fixed.loc['111120', 'F06C00']
    # and a Tier 2 column is never fixed, however well it reproduces its bridge
    assert not fixed['F01000'].any()
    assert int(fixed.to_numpy().sum()) == 2


def test_the_supply_block_has_no_fixed_values() -> None:
    """An empty layer is the honest default while the Supply fixed-value layer is open.

    A cell masked by accident cannot be corrected by the balance, so the
    Supply side stays unmasked until ``MCIF``/``MDTY`` are actually decided.
    """
    fixed = fixed_value_mask('supply', 2017, _supply_panel())
    assert not fixed.to_numpy().any()


def test_fixed_and_structural_layers_never_overlap() -> None:
    panel = _use_panel()
    mask = build_sut_mask('use', 2017, panel)
    overlap = mask.structural_zero.to_numpy() & mask.fixed_value.to_numpy()
    assert not overlap.any()


# --------------------------------------------------------------------------
# Tier 3 - sign locks
# --------------------------------------------------------------------------


def test_sign_locks_follow_the_published_sign_per_cell() -> None:
    """The give-up side needs no special case: the cell's own sign is the lock."""
    locks = sign_lock_mask('supply', _supply_panel())
    assert locks.loc['111120', 'TRADE '] == 1
    assert locks.loc['111120', 'TRANS'] == -1  # negative margin, locked negative
    assert locks.loc['111120', 'TOP'] == 1
    assert locks.loc['111120', 'SUB'] == -1
    assert locks.loc['111120', 'MADJ'] == -1
    # a zero cell is pattern, not a lock
    assert locks.loc['111130', 'TRADE '] == 0


def test_only_the_named_columns_and_rows_are_locked() -> None:
    supply_locks = sign_lock_mask('supply', _supply_panel())
    unlocked = [c for c in SUPPLY_BRIDGE_COLUMNS if c not in SIGN_LOCKED_SUPPLY_COLUMNS]
    assert not supply_locks[unlocked].to_numpy().any()

    use_locks = sign_lock_mask('use', _use_panel())
    assert use_locks.loc['T00SUB', '1111A0'] == -1
    assert list(SIGN_LOCKED_USE_ROWS) == ['T00SUB']


def test_gross_operating_surplus_is_not_locked() -> None:
    """``V00300`` is the residual the system lands on, and goes negative."""
    locks = sign_lock_mask('use', _use_panel())
    assert locks.loc['V00300', '1111A0'] == 0


# --------------------------------------------------------------------------
# assembly
# --------------------------------------------------------------------------


def test_build_sut_mask_validates_against_its_panel() -> None:
    panel = _use_panel()
    mask = build_sut_mask('use', 2017, panel)
    assert mask.shape == panel.shape
    mask.validate_against(panel)


def test_a_panel_contradicting_the_mask_raises() -> None:
    """Reusing a mask against a different panel is caught, not absorbed."""
    mask = build_sut_mask('use', 2017, _use_panel())
    moved = _use_panel()
    moved.loc['111130', '1111B0'] = 99.0  # was a structural zero
    with pytest.raises(ValueError, match='structural zero'):
        mask.validate_against(moved)


def test_an_injected_panel_locks_to_whatever_sign_it_carries() -> None:
    """Injecting a panel bypasses normalisation, so the lock follows the cell.

    ``build_sut_mask`` only reaches ``assert_subsidies_negative`` through
    ``published_2017_panel``; given a panel directly it takes it as given. A
    ``+1`` lock on ``T00SUB`` is therefore the signal that the panel was never
    normalised - not a rejection. The rejection is
    :func:`test_a_subsidy_stored_positive_is_rejected`.
    """
    panel = _use_panel()
    panel.loc['T00SUB', '1111A0'] = 8.0
    mask = build_sut_mask('use', 2017, panel)
    assert mask.sign_lock.loc['T00SUB', '1111A0'] == 1


def test_a_subsidy_stored_positive_is_rejected() -> None:
    """The convention check itself, on the path that actually runs it.

    BEA publishes the Use ``T00SUB`` row positive and the balance stores it
    negative; unnormalised, a producer-price column margin is wrong by
    ``2 x T00SUB``.
    """
    panel = _use_panel()
    panel.loc['T00SUB', '1111A0'] = 8.0
    with pytest.raises(ValueError, match='stores subsidies negative'):
        assert_subsidies_negative(panel, axis='row', label='T00SUB')

    # and the normalised panel passes
    assert_subsidies_negative(_use_panel(), axis='row', label='T00SUB')


def test_the_one_to_one_columns_are_the_six_the_plan_names() -> None:
    assert set(ONE_TO_ONE_FD) == {
        'F06C00',
        'F07C00',
        'F10C00',
        'F06N00',
        'F07N00',
        'F10N00',
    }


def test_unknown_block_raises() -> None:
    with pytest.raises(ValueError, match='use or supply'):
        panel_labels('intermediate')  # type: ignore[arg-type]
