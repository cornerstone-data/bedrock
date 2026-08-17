"""Tests for the Step 5 target set sourcing (#591).

Structural, following ``test_nowcast_margins.py``: these check that each target
is *shaped* the way the plan says, because that is what an engine is built
against. Whether the published tables actually satisfy the constraints is a
real-data question and lives in ``hard_target_residuals``, behind a check
rather than a unit test.

The highest-value test here is :func:`test_the_subsidy_identity_is_a_difference`
- writing T12 as a sum is wrong by exactly ``2 x 59,876`` and every other layer
would have accepted it silently.
"""

from __future__ import annotations

import pytest

from bedrock.transform.iot.nowcast_mask import ONE_TO_ONE_FD
from bedrock.transform.iot.nowcast_targets import (
    FD_TARGET_COLUMNS,
    WEIGHTS,
    identity_targets,
)
from bedrock.utils.economic.balance.targets import PLACEHOLDER_PREFIX

IDENTITY_NAMES = ('T11', 'T12', 'T13', 'T14', 'T15', 'T16')


def _by_name() -> dict[str, object]:
    return {t.name: t for t in identity_targets()}


def test_every_identity_is_hard_and_unsourced() -> None:
    """An identity costs no source, which is what makes it free to impose."""
    targets = identity_targets()
    assert tuple(t.name for t in targets) == IDENTITY_NAMES
    for target in targets:
        assert target.hard
        assert not target.is_placeholder
        assert target.source.startswith('identity')


def test_the_subsidy_identity_is_a_difference_not_a_sum() -> None:
    """T12 regression guard.

    BEA stores the Use ``T00SUB`` row positive and the Supply ``SUB`` column
    negative, so on the raw tables the identity is a sum. The balance
    normalises both negative, which makes it a difference - and writing it as a
    sum is wrong by exactly ``2 x 59,876`` while still looking plausible.
    """
    t12 = _by_name()['T12']
    coefficients = {term.block: term.coefficient for term in t12.terms}  # type: ignore[attr-defined]
    assert coefficients == {'use': 1.0, 'supply': -1.0}


def test_the_tax_identity_subtracts_both_top_and_mdty() -> None:
    """``T00TOP = TOP + MDTY``, not ``T00TOP = TOP``.

    Customs duties are a product tax the Supply table books in its own column
    while the Use table folds it into ``T00TOP``. Omitting ``MDTY`` is wrong by
    38,507 - the whole duty.
    """
    t13 = _by_name()['T13']
    supply = [term for term in t13.terms if term.block == 'supply'][0]  # type: ignore[attr-defined]
    assert supply.coefficient == -1.0
    assert set(supply.aggregator.detail) >= {'TOP', 'MDTY'}
    claimed = {
        label
        for row in supply.aggregator.matrix
        for label, on in zip(supply.aggregator.detail, row)
        if on
    }
    assert claimed == {'TOP', 'MDTY'}


def test_the_margin_identities_sum_a_single_column_to_zero() -> None:
    """Margins redistribute rather than create, so each column nets to zero."""
    for name, column in (('T15', 'TRADE '), ('T16', 'TRANS')):
        target = _by_name()[name]
        assert len(target.terms) == 1  # type: ignore[attr-defined]
        term = target.terms[0]  # type: ignore[attr-defined]
        assert term.block == 'supply'
        assert term.coefficient == 1.0
        claimed = {
            label
            for row in term.aggregator.matrix
            for label, on in zip(term.aggregator.detail, row)
            if on
        }
        assert claimed == {column}
        assert float(target.values.iloc[0]) == 0.0  # type: ignore[attr-defined]


def test_the_commodity_identity_spans_both_panels() -> None:
    """``T016 = T019`` is the constraint that lets a frozen Use row be
    absorbed by its Supply row, so it has to read both blocks."""
    t11 = _by_name()['T11']
    assert t11.is_cross_block  # type: ignore[attr-defined]
    assert set(t11.blocks) == {'use', 'supply'}  # type: ignore[attr-defined]
    coefficients = {term.block: term.coefficient for term in t11.terms}  # type: ignore[attr-defined]
    assert coefficients == {'supply': 1.0, 'use': -1.0}


def test_the_duty_hinge_reads_one_cell_against_one_column() -> None:
    """T14 is ``T00TOP[4200ID]`` against the whole ``MDTY`` column."""
    t14 = _by_name()['T14']
    use = [term for term in t14.terms if term.block == 'use'][0]  # type: ignore[attr-defined]
    assert use.restrict_to == ('T00TOP',)
    claimed = {
        label
        for row in use.aggregator.matrix
        for label, on in zip(use.aggregator.detail, row)
        if on
    }
    assert claimed == {'4200ID'}


def test_the_six_masked_columns_leave_the_target_set() -> None:
    """Masking those cells and targeting their total are the same constraint
    written twice, so a column cannot be in both."""
    assert len(FD_TARGET_COLUMNS) == 13
    assert not set(FD_TARGET_COLUMNS) & set(ONE_TO_ONE_FD)


def test_weights_order_identity_above_expenditure_above_income() -> None:
    """The ordering is the defensible part, not the values."""
    assert WEIGHTS['T2'] > WEIGHTS['T6'] > WEIGHTS['T4']
    assert all(0 < w <= 1 for w in WEIGHTS.values())


@pytest.mark.parametrize('name', IDENTITY_NAMES)
def test_no_identity_carries_a_placeholder(name: str) -> None:
    assert not _by_name()[name].source.startswith(PLACEHOLDER_PREFIX)  # type: ignore[attr-defined]
