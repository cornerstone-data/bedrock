"""Hand-checkable tests for the balance scaffolding.

Every matrix here is small enough to verify on paper, which is the point: the
offset method's failure modes are all *silent* - a lost value, a clamped sign,
a double-counted block - so a test that only checks "it ran" would pass on all
of them.

The seed used throughout::

            c1   c2   c3   | row
      r1    10    5    5   |  20
      r2     4    6    0   |  10
      r3     0    2    8   |  10
      ---------------------+
      col   14   13   13   |
"""

import numpy as np
import pandas as pd
import pytest

from bedrock.utils.economic.balance import (
    Aggregator,
    InfeasibleBalance,
    SutMask,
    Target,
    TargetSet,
    assert_free_seed,
    assert_subsidies_negative,
    margin,
    margin_report,
    offset_target,
    offset_targets,
    precheck,
    restore_fixed,
    split_fixed,
)

ROWS = ['r1', 'r2', 'r3']
COLS = ['c1', 'c2', 'c3']


def _seed() -> pd.DataFrame:
    return pd.DataFrame(
        [[10.0, 5.0, 5.0], [4.0, 6.0, 0.0], [0.0, 2.0, 8.0]],
        index=ROWS,
        columns=COLS,
    )


def _flags(cells: list[tuple[str, str]]) -> pd.DataFrame:
    flags = pd.DataFrame(False, index=ROWS, columns=COLS)
    for row, col in cells:
        flags.loc[row, col] = True
    return flags


def _mask(fixed: list[tuple[str, str]]) -> SutMask:
    return SutMask.from_pattern(_seed(), fixed_value=_flags(fixed))


def _scale_rows(free: pd.DataFrame, targets: pd.Series) -> pd.DataFrame:
    """Stand-in engine: one proportional row scaling, participation only.

    Deliberately the dumbest thing that respects a participation mask, so the
    tests exercise the offset rather than a solver.
    """
    current = free.sum(axis=1)
    factor = targets.reindex(current.index) / current.replace(0.0, np.nan)
    return free.mul(factor.fillna(0.0), axis=0)


# --------------------------------------------------------------------------
# margins
# --------------------------------------------------------------------------


def test_margin_row_and_column() -> None:
    seed = _seed()
    assert list(margin(seed, 'row')) == [20.0, 10.0, 10.0]
    assert list(margin(seed, 'column')) == [14.0, 13.0, 13.0]


def test_margin_restrict_to_selects_the_summed_axis() -> None:
    """``restrict_to`` narrows which rows feed a column margin.

    This is the shape the compensation target needs: one value-added row
    constrained by industry group, which is neither a plain row margin nor a
    plain column one.
    """
    seed = _seed()
    restricted = margin(seed, 'column', restrict_to=('r1', 'r2'))
    assert list(restricted) == [14.0, 11.0, 5.0]


# --------------------------------------------------------------------------
# the mask keeps its three layers apart
# --------------------------------------------------------------------------


def test_structural_zero_and_fixed_value_cannot_overlap() -> None:
    zeros = _flags([('r2', 'c3')])
    with pytest.raises(ValueError, match='different'):
        SutMask(
            structural_zero=zeros,
            fixed_value=zeros,
            sign_lock=pd.DataFrame(0, index=ROWS, columns=COLS),
        )


def test_from_pattern_takes_zeros_from_the_seed() -> None:
    mask = _mask([('r1', 'c1')])
    assert mask.structural_zero.loc['r2', 'c3']
    assert mask.structural_zero.loc['r3', 'c1']
    assert not mask.structural_zero.loc['r1', 'c1']
    assert mask.fixed_value.loc['r1', 'c1']
    # frozen is the participation mask an engine sees: zeros plus fixed values.
    assert int(mask.frozen.to_numpy().sum()) == 3


def test_seed_contradicting_its_structural_zeros_raises() -> None:
    mask = _mask([])
    contradicting = _seed()
    contradicting.loc['r2', 'c3'] = 1.0
    with pytest.raises(ValueError, match='structural zero'):
        mask.validate_against(contradicting)


def test_sign_lock_violation_in_the_seed_raises() -> None:
    locks = pd.DataFrame(0, index=ROWS, columns=COLS)
    locks.loc['r1', 'c1'] = -1  # must stay <= 0, but the seed has +10
    mask = SutMask(
        structural_zero=(_seed() == 0),
        fixed_value=_flags([]),
        sign_lock=locks,
    )
    with pytest.raises(ValueError, match='sign lock'):
        mask.validate_against(_seed())


# --------------------------------------------------------------------------
# a fixed cell is held at its value, not zeroed
# --------------------------------------------------------------------------


def test_split_fixed_holds_the_value_and_reconstructs_the_seed() -> None:
    seed = _seed()
    frozen, free = split_fixed(seed, _mask([('r1', 'c1')]))
    assert frozen.loc['r1', 'c1'] == 10.0
    assert free.loc['r1', 'c1'] == 0.0
    pd.testing.assert_frame_equal(frozen + free, seed)


def test_fixed_nonzero_cell_survives_the_balance_and_free_cells_absorb() -> None:
    """The property the whole offset exists to deliver.

    Row ``r1`` totals 20 with ``(r1, c1) = 10`` fixed. Target it at 25: the
    residual is 15, the two free cells hold 10 between them, so each scales
    1.5x to 7.5 and the fixed cell comes out at exactly 10.
    """
    seed = _seed()
    mask = _mask([('r1', 'c1')])
    frozen, free = split_fixed(seed, mask)

    target = Target(
        block='use',
        axis='row',
        values=pd.Series([25.0], index=['r1']),
        source='test',
    )
    residual = offset_target(target, frozen)
    assert residual.values.loc['r1'] == 15.0

    balanced = _scale_rows(free.loc[['r1']], residual.values)
    result = restore_fixed(balanced, frozen.loc[['r1']])

    assert result.loc['r1', 'c1'] == 10.0
    assert result.loc['r1', 'c2'] == 7.5
    assert result.loc['r1', 'c3'] == 7.5
    assert result.loc['r1'].sum() == 25.0


# --------------------------------------------------------------------------
# targets keep their sign
# --------------------------------------------------------------------------


def test_residual_target_may_change_sign() -> None:
    """A positive published target can go negative once frozen mass is out.

    ``r2`` has 6 frozen at ``(r2, c2)`` against a target of 3, so the free
    cells must sum to **-3**. An engine that clamps targets non-negative turns
    this into 0 silently.
    """
    frozen, _ = split_fixed(_seed(), _mask([('r2', 'c2')]))
    target = Target(
        block='use',
        axis='row',
        values=pd.Series([3.0], index=['r2']),
        source='test',
    )
    residual = offset_target(target, frozen)
    assert residual.values.loc['r2'] == -3.0
    assert residual.allow_negative
    assert 'residual' in residual.source


def test_negative_values_need_allow_negative() -> None:
    """The guard that would have caught a clamped ``F03000``."""
    with pytest.raises(ValueError, match='allow_negative'):
        Target(
            block='use',
            axis='column',
            values=pd.Series([-37568.0], index=['F03000']),
            source='NIPA T5.7.5B',
        )
    permitted = Target(
        block='use',
        axis='column',
        values=pd.Series([-37568.0], index=['F03000']),
        source='NIPA T5.7.5B',
        allow_negative=True,
    )
    assert permitted.values.loc['F03000'] == -37568.0


# --------------------------------------------------------------------------
# aggregate-level targets, with a mask inside the aggregate
# --------------------------------------------------------------------------


def test_aggregate_target_offsets_by_r_f_ct() -> None:
    """``A' = A - R @ F @ Cᵀ`` when the mask sits inside a group.

    Columns group as ``g1 = {c1, c2}`` and ``g2 = {c3}``. The fixed cell
    ``(r1, c1) = 10`` is inside ``g1``, so ``g1``'s residual drops by 10 while
    ``g2``'s is untouched.
    """
    aggregator = Aggregator.from_mapping({'g1': ['c1', 'c2'], 'g2': ['c3']}, COLS)
    frozen, _ = split_fixed(_seed(), _mask([('r1', 'c1')]))
    target = Target(
        block='use',
        axis='column',
        values=pd.Series([30.0, 15.0], index=['g1', 'g2']),
        source='test',
        aggregator=aggregator,
    )
    residual = offset_target(target, frozen)
    assert residual.values.loc['g1'] == 20.0
    assert residual.values.loc['g2'] == 15.0


def test_aggregator_rejects_unknown_detail_and_missing_margin() -> None:
    with pytest.raises(KeyError, match='not in the detail labels'):
        Aggregator.from_mapping({'g1': ['c9']}, COLS)
    aggregator = Aggregator.from_mapping({'g1': ['c1', 'c2'], 'g2': ['c3']}, COLS)
    with pytest.raises(KeyError, match='missing'):
        aggregator.apply(pd.Series([1.0], index=['c1']))


def test_aggregated_target_values_must_be_indexed_by_group() -> None:
    aggregator = Aggregator.from_mapping({'g1': ['c1', 'c2'], 'g2': ['c3']}, COLS)
    with pytest.raises(ValueError, match='aggregator groups'):
        Target(
            block='use',
            axis='column',
            values=pd.Series([1.0, 2.0], index=['c1', 'c2']),
            source='test',
            aggregator=aggregator,
        )


# --------------------------------------------------------------------------
# a margin that is entirely masked
# --------------------------------------------------------------------------


def test_fully_masked_margin_with_a_consistent_target_is_a_no_op() -> None:
    """Redundant, not infeasible: the frozen mass already satisfies it."""
    mask = _mask([('r3', 'c2'), ('r3', 'c3')])
    target = Target(
        block='use',
        axis='row',
        values=pd.Series([10.0], index=['r3']),
        source='test',
        hard=True,
    )
    findings = precheck(_seed(), mask, TargetSet.of(target))
    assert findings == []


def test_fully_masked_margin_with_an_inconsistent_target_raises() -> None:
    mask = _mask([('r3', 'c2'), ('r3', 'c3')])
    target = Target(
        block='use',
        axis='row',
        values=pd.Series([12.0], index=['r3']),
        source='test',
        hard=True,
    )
    with pytest.raises(InfeasibleBalance, match='no free mass'):
        precheck(_seed(), mask, TargetSet.of(target))


# --------------------------------------------------------------------------
# the zero control total
# --------------------------------------------------------------------------


def test_empty_margin_with_a_nonzero_target_is_fatal() -> None:
    """The classic silent failure: nothing to scale, and a target anyway.

    RAS divides by the margin, so an all-zero row against a nonzero target
    produces ``inf`` or ``nan`` and keeps going. Here it raises.
    """
    seed = pd.DataFrame(0.0, index=ROWS, columns=COLS)
    seed.loc['r1'] = [1.0, 2.0, 3.0]
    mask = SutMask.from_pattern(seed)
    target = Target(
        block='use',
        axis='row',
        values=pd.Series([5.0], index=['r2']),
        source='test',
    )
    with pytest.raises(InfeasibleBalance, match='no free mass'):
        precheck(seed, mask, TargetSet.of(target))


def test_zero_target_on_a_live_margin_is_feasible() -> None:
    """A zero *target* is fine - it is a zero *margin* that has no freedom."""
    target = Target(
        block='use',
        axis='row',
        values=pd.Series([0.0], index=['r2']),
        source='test',
    )
    findings = precheck(_seed(), _mask([]), TargetSet.of(target))
    assert findings == []


# --------------------------------------------------------------------------
# double-counting
# --------------------------------------------------------------------------


def test_passing_the_full_matrix_with_a_mask_does_not_silently_pass() -> None:
    """Handing the engine ``X`` *and* the residual targets counts ``F`` twice."""
    seed = _seed()
    mask = _mask([('r1', 'c1')])
    _, free = split_fixed(seed, mask)

    assert_free_seed(free, mask)  # Z is what the engine should get
    with pytest.raises(ValueError, match='double-counts'):
        assert_free_seed(seed, mask)


# --------------------------------------------------------------------------
# leverage and the report
# --------------------------------------------------------------------------


def test_high_leverage_warns_but_does_not_raise() -> None:
    """``r1`` keeps 0.5 of 20.5 free, so leverage is 41x - fragile, not stuck."""
    seed = _seed()
    seed.loc['r1'] = [10.0, 10.0, 0.5]
    mask = SutMask.from_pattern(seed, fixed_value=_flags([('r1', 'c1'), ('r1', 'c2')]))
    target = Target(
        block='use',
        axis='row',
        values=pd.Series([21.0], index=['r1']),
        source='test',
    )
    findings = precheck(seed, mask, TargetSet.of(target))
    assert len(findings) == 1
    assert findings[0].severity == 'warning'
    assert findings[0].kind == 'high_leverage'
    assert findings[0].leverage == pytest.approx(41.0)


def test_margin_report_carries_masses_and_provenance() -> None:
    mask = _mask([('r1', 'c1')])
    target = Target(
        block='use',
        axis='row',
        values=pd.Series([25.0], index=['r1']),
        source='UGO305-A',
    )
    report = margin_report(_seed(), mask, TargetSet.of(target))
    row = report.loc['r1']
    assert row['total_mass'] == 20.0
    assert row['frozen_mass'] == 10.0
    assert row['free_mass'] == 10.0
    assert row['residual_target'] == 15.0
    assert row['leverage'] == pytest.approx(2.0)
    assert row['source'] == 'UGO305-A'


def test_offset_targets_leaves_other_blocks_alone() -> None:
    frozen, _ = split_fixed(_seed(), _mask([('r1', 'c1')]))
    use = Target(
        block='use',
        axis='row',
        values=pd.Series([25.0], index=['r1']),
        source='test-use',
    )
    supply = Target(
        block='supply',
        axis='row',
        values=pd.Series([25.0], index=['r1']),
        source='test-supply',
    )
    offset = offset_targets(TargetSet.of(use, supply), frozen, block='use')
    by_block = {t.block: t for t in offset}
    assert by_block['use'].values.loc['r1'] == 15.0
    assert by_block['supply'].values.loc['r1'] == 25.0


# --------------------------------------------------------------------------
# provenance and the target set
# --------------------------------------------------------------------------


def test_a_target_needs_a_source() -> None:
    with pytest.raises(ValueError, match='provenance'):
        Target(
            block='use',
            axis='row',
            values=pd.Series([1.0], index=['r1']),
            source='',
        )


def test_the_same_source_cannot_constrain_the_same_margin_twice() -> None:
    target = Target(
        block='use',
        axis='row',
        values=pd.Series([1.0], index=['r1']),
        source='test',
    )
    with pytest.raises(ValueError, match='twice'):
        TargetSet.of(target, target)


# --------------------------------------------------------------------------
# the subsidy sign convention
# --------------------------------------------------------------------------


def test_subsidies_must_be_stored_negative() -> None:
    """BEA stores the Use row positive and the Supply column negative.

    Left unnormalised, a producer-price column margin is wrong by twice the
    subsidy - so the convention is asserted where the frame is assembled.
    """
    use = pd.DataFrame(
        [[1.0, 2.0], [59876.0, 0.0]], index=['V00100', 'T00SUB'], columns=['a', 'b']
    )
    with pytest.raises(ValueError, match='stores subsidies negative'):
        assert_subsidies_negative(use, axis='row', label='T00SUB')

    normalised = use.copy()
    normalised.loc['T00SUB'] *= -1
    assert_subsidies_negative(normalised, axis='row', label='T00SUB')
