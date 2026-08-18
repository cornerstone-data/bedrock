"""Hand-checkable tests for the balance scaffolding.

Every matrix here is small enough to verify on paper, which is the point: the
offset method's failure modes are all *silent* - a lost value, a clamped sign,
a double-counted block - so a test that only checks "it ran" would pass on all
of them.

The ``use`` block used throughout::

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
    TargetTerm,
    UnsourcedTargets,
    assert_free_seed,
    assert_subsidies_negative,
    margin,
    margin_report,
    offset_target,
    offset_targets,
    precheck,
    restore_fixed,
    split_fixed,
    split_fixed_blocks,
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


def _blocks(
    fixed: list[tuple[str, str]] | None = None,
) -> tuple[dict[str, pd.DataFrame], dict[str, SutMask]]:
    """One-block seed and mask mappings, the common case."""
    return {'use': _seed()}, {'use': _mask(fixed or [])}


def _row_target(label: str, value: float, *, hard: bool = False) -> Target:
    return Target.on_margin(
        'use', 'row', pd.Series([value], index=[label]), 'test', hard=hard
    )


def _scale_rows(free: pd.DataFrame, targets: pd.Series) -> pd.DataFrame:
    """Stand-in engine: one proportional row scaling, participation only."""
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


def test_restrict_to_narrows_the_summed_axis() -> None:
    """The shape the compensation target needs: one row, by column group."""
    term = TargetTerm('use', 'column', restrict_to=('r1', 'r2'))
    assert list(term.margin_of(_seed())) == [14.0, 11.0, 5.0]


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
    assert not mask.structural_zero.loc['r1', 'c1']
    assert mask.fixed_value.loc['r1', 'c1']
    assert int(mask.frozen.to_numpy().sum()) == 3


def test_seed_contradicting_its_structural_zeros_raises() -> None:
    contradicting = _seed()
    contradicting.loc['r2', 'c3'] = 1.0
    with pytest.raises(ValueError, match='structural zero'):
        _mask([]).validate_against(contradicting)


def test_sign_lock_violation_in_the_seed_raises() -> None:
    locks = pd.DataFrame(0, index=ROWS, columns=COLS)
    locks.loc['r1', 'c1'] = -1  # must stay <= 0, but the seed has +10
    mask = SutMask(
        structural_zero=(_seed() == 0), fixed_value=_flags([]), sign_lock=locks
    )
    with pytest.raises(ValueError, match='sign lock'):
        mask.validate_against(_seed())


# --------------------------------------------------------------------------
# a fixed cell is held at its value, not zeroed
# --------------------------------------------------------------------------


def test_split_fixed_holds_the_value_and_reconstructs_the_seed() -> None:
    frozen, free = split_fixed(_seed(), _mask([('r1', 'c1')]))
    assert frozen.loc['r1', 'c1'] == 10.0
    assert free.loc['r1', 'c1'] == 0.0
    pd.testing.assert_frame_equal(frozen + free, _seed())


def test_fixed_nonzero_cell_survives_the_balance_and_free_cells_absorb() -> None:
    """Row ``r1`` totals 20 with ``(r1, c1) = 10`` fixed; target it at 25.

    The residual is 15, the two free cells hold 10 between them, so each scales
    1.5x to 7.5 and the fixed cell comes out at exactly 10.
    """
    seeds, masks = _blocks([('r1', 'c1')])
    frozen, free = split_fixed_blocks(seeds, masks)

    residual = offset_target(_row_target('r1', 25.0), frozen)
    assert residual.values.loc['r1'] == 15.0

    balanced = _scale_rows(free['use'].loc[['r1']], residual.values)
    result = restore_fixed(balanced, frozen['use'].loc[['r1']])

    assert result.loc['r1', 'c1'] == 10.0
    assert result.loc['r1', 'c2'] == 7.5
    assert result.loc['r1', 'c3'] == 7.5
    assert result.loc['r1'].sum() == 25.0


# --------------------------------------------------------------------------
# targets keep their sign
# --------------------------------------------------------------------------


def test_residual_target_may_change_sign() -> None:
    """``r2`` has 6 frozen against a target of 3, so the free cells sum to -3."""
    frozen, _ = split_fixed_blocks(*_blocks([('r2', 'c2')]))
    residual = offset_target(_row_target('r2', 3.0), frozen)
    assert residual.values.loc['r2'] == -3.0
    assert residual.allow_negative
    assert 'residual' in residual.source


def test_negative_values_need_allow_negative() -> None:
    """The guard that would have caught a clamped ``F03000``."""
    values = pd.Series([-37568.0], index=['F03000'])
    with pytest.raises(ValueError, match='allow_negative'):
        Target.on_margin('use', 'column', values, 'NIPA T5.7.5B')
    permitted = Target.on_margin(
        'use', 'column', values, 'NIPA T5.7.5B', allow_negative=True
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
    frozen, _ = split_fixed_blocks(*_blocks([('r1', 'c1')]))
    target = Target.on_margin(
        'use',
        'column',
        pd.Series([30.0, 15.0], index=['g1', 'g2']),
        'test',
        aggregator=aggregator,
    )
    residual = offset_target(target, frozen)
    assert residual.values.loc['g1'] == 20.0
    assert residual.values.loc['g2'] == 15.0


def test_aggregator_total_collapses_a_margin_to_a_scalar() -> None:
    """The idiom the economy-wide identities use."""
    aggregator = Aggregator.total(['c1', 'c3'], COLS, 'both')
    assert list(aggregator.apply(margin(_seed(), 'column'))) == [27.0]


def test_aggregator_rejects_unknown_detail_and_missing_margin() -> None:
    with pytest.raises(KeyError, match='not in the detail labels'):
        Aggregator.from_mapping({'g1': ['c9']}, COLS)
    aggregator = Aggregator.from_mapping({'g1': ['c1', 'c2'], 'g2': ['c3']}, COLS)
    with pytest.raises(KeyError, match='missing'):
        aggregator.apply(pd.Series([1.0], index=['c1']))


# --------------------------------------------------------------------------
# cross-block targets - five of the seven hard constraints
# --------------------------------------------------------------------------


def _two_blocks() -> tuple[dict[str, pd.DataFrame], dict[str, SutMask]]:
    """A ``use`` block and a ``supply`` block on the same row labels."""
    supply = pd.DataFrame(
        [[12.0, 8.0, 0.0], [3.0, 7.0, 0.0], [1.0, 0.0, 9.0]],
        index=ROWS,
        columns=COLS,
    )
    seeds = {'use': _seed(), 'supply': supply}
    masks = {
        'use': _mask([]),
        'supply': SutMask.from_pattern(supply),
    }
    return seeds, masks


def test_cross_block_identity_evaluates_across_both_blocks() -> None:
    """``T016 = T019``: ``+1 · supply.row − 1 · use.row = 0``, per commodity.

    Supply rows total 20, 10, 10; Use rows total 20, 10, 10. The identity holds
    exactly here, so every residual is zero.
    """
    seeds, masks = _two_blocks()
    identity = Target(
        terms=(
            TargetTerm('supply', 'row', coefficient=1.0),
            TargetTerm('use', 'row', coefficient=-1.0),
        ),
        values=pd.Series(0.0, index=ROWS),
        source='identity',
        name='T11',
        hard=True,
    )
    assert identity.is_cross_block
    assert identity.blocks == ('supply', 'use')
    assert list(identity.evaluate(seeds)) == [0.0, 0.0, 0.0]

    frozen, _ = split_fixed_blocks(seeds, masks)
    assert list(offset_target(identity, frozen).values) == [0.0, 0.0, 0.0]


def test_cross_block_scalar_identity_with_three_terms() -> None:
    """The ``T00TOP = TOP + MDTY`` shape: one Use row against two Supply columns."""
    seeds, _ = _two_blocks()
    target = Target(
        terms=(
            TargetTerm('use', 'row', 1.0, Aggregator.total(['r1'], ROWS, 'taxes')),
            TargetTerm(
                'supply', 'column', -1.0, Aggregator.total(['c1', 'c2'], COLS, 'taxes')
            ),
        ),
        values=pd.Series([0.0], index=['taxes']),
        source='identity',
        name='T13',
        hard=True,
    )
    # Use r1 = 20; Supply c1 + c2 = 16 + 15 = 31; so 20 - 31 = -11.
    assert target.evaluate(seeds).loc['taxes'] == -11.0


def test_cross_block_free_mass_reads_both_tables() -> None:
    """``mask_layer_plan`` §3: a frozen Use row is only stuck if Supply is too.

    Freeze all of Use ``r3``. The identity still has the Supply row's mass to
    move, so it is feasible - and the arithmetic says so with no special case.
    """
    seeds, masks = _two_blocks()
    masks = dict(masks)
    masks['use'] = SutMask.from_pattern(
        _seed(), fixed_value=_flags([('r3', 'c2'), ('r3', 'c3')])
    )
    identity = Target(
        terms=(
            TargetTerm('supply', 'row', 1.0),
            TargetTerm('use', 'row', -1.0),
        ),
        values=pd.Series(0.0, index=ROWS),
        source='identity',
        name='T11',
        hard=True,
    )
    report = margin_report(seeds, masks, TargetSet.of(identity))
    # Use r3 contributes nothing free; Supply r3 contributes all 10 of its mass.
    assert report.loc['r3', 'frozen_mass'] == 10.0
    assert report.loc['r3', 'free_mass'] == 10.0
    assert precheck(seeds, masks, TargetSet.of(identity)) == []


def test_a_target_naming_a_missing_block_raises() -> None:
    target = Target(
        terms=(TargetTerm('supply', 'row'),),
        values=pd.Series(0.0, index=ROWS),
        source='test',
    )
    with pytest.raises(KeyError, match='not supplied'):
        target.evaluate({'use': _seed()})


# --------------------------------------------------------------------------
# a margin that is entirely masked
# --------------------------------------------------------------------------


def test_fully_masked_margin_with_a_consistent_target_is_a_no_op() -> None:
    """Redundant, not infeasible: the frozen mass already satisfies it."""
    seeds, masks = _blocks([('r3', 'c2'), ('r3', 'c3')])
    findings = precheck(seeds, masks, TargetSet.of(_row_target('r3', 10.0, hard=True)))
    assert findings == []


def test_fully_masked_margin_with_an_inconsistent_target_raises() -> None:
    seeds, masks = _blocks([('r3', 'c2'), ('r3', 'c3')])
    with pytest.raises(InfeasibleBalance, match='no free mass'):
        precheck(seeds, masks, TargetSet.of(_row_target('r3', 12.0, hard=True)))


# --------------------------------------------------------------------------
# the zero control total
# --------------------------------------------------------------------------


def _empty_margin_case() -> tuple[dict[str, pd.DataFrame], dict[str, SutMask]]:
    seed = pd.DataFrame(0.0, index=ROWS, columns=COLS)
    seed.loc['r1'] = [1.0, 2.0, 3.0]
    return {'use': seed}, {'use': SutMask.from_pattern(seed)}


def test_empty_margin_with_a_nonzero_hard_target_is_fatal() -> None:
    """RAS divides by the margin; an all-zero row against a nonzero target
    produces ``inf`` or ``nan`` and keeps going. Here it raises."""
    seeds, masks = _empty_margin_case()
    with pytest.raises(InfeasibleBalance, match='no free mass'):
        precheck(seeds, masks, TargetSet.of(_row_target('r2', 5.0, hard=True)))


def test_the_same_margin_with_a_soft_target_warns_instead() -> None:
    """Giving way is what soft means, so it reports rather than blocking.

    The situation is identical - no free mass, nonzero residual - and only the
    target's mode differs. Every placeholder target is soft, so this path is
    reachable today.
    """
    seeds, masks = _empty_margin_case()
    findings = precheck(seeds, masks, TargetSet.of(_row_target('r2', 5.0)))
    assert [(f.severity, f.kind) for f in findings] == [('warning', 'no_free_mass')]


def test_zero_target_on_a_live_margin_is_feasible() -> None:
    """A zero *target* is fine - it is a zero *margin* that has no freedom."""
    seeds, masks = _blocks()
    assert precheck(seeds, masks, TargetSet.of(_row_target('r2', 0.0))) == []


# --------------------------------------------------------------------------
# double-counting
# --------------------------------------------------------------------------


def test_passing_the_full_matrix_with_a_mask_does_not_silently_pass() -> None:
    """Handing the engine ``X`` *and* the residual targets counts ``F`` twice."""
    seeds, masks = _blocks([('r1', 'c1')])
    _, free = split_fixed_blocks(seeds, masks)
    assert_free_seed(free['use'], masks['use'])
    with pytest.raises(ValueError, match='double-counts'):
        assert_free_seed(seeds['use'], masks['use'])


# --------------------------------------------------------------------------
# leverage, provenance and the report
# --------------------------------------------------------------------------


def test_high_leverage_warns_but_does_not_raise() -> None:
    """``r1`` keeps 0.5 of 20.5 free, so leverage is 41x - fragile, not stuck."""
    seed = _seed()
    seed.loc['r1'] = [10.0, 10.0, 0.5]
    seeds = {'use': seed}
    masks = {
        'use': SutMask.from_pattern(
            seed, fixed_value=_flags([('r1', 'c1'), ('r1', 'c2')])
        )
    }
    findings = precheck(seeds, masks, TargetSet.of(_row_target('r1', 21.0)))
    assert len(findings) == 1
    assert findings[0].severity == 'warning'
    assert findings[0].kind == 'high_leverage'
    assert findings[0].leverage == pytest.approx(41.0)


def test_margin_report_carries_masses_and_provenance() -> None:
    seeds, masks = _blocks([('r1', 'c1')])
    target = Target.on_margin(
        'use', 'row', pd.Series([25.0], index=['r1']), 'UGO305-A', name='T1'
    )
    row = margin_report(seeds, masks, TargetSet.of(target)).loc['r1']
    assert row['total_mass'] == 20.0
    assert row['frozen_mass'] == 10.0
    assert row['free_mass'] == 10.0
    assert row['residual_target'] == 15.0
    assert row['leverage'] == pytest.approx(2.0)
    assert row['source'] == 'UGO305-A'
    assert row['target'] == 'T1'


def test_offset_targets_offsets_every_target() -> None:
    frozen, _ = split_fixed_blocks(*_blocks([('r1', 'c1')]))
    offset = offset_targets(
        TargetSet.of(_row_target('r1', 25.0), _row_target('r2', 10.0)), frozen
    )
    values = {t.values.index[0]: t.values.iloc[0] for t in offset}
    assert values == {'r1': 15.0, 'r2': 10.0}


# --------------------------------------------------------------------------
# placeholders must never be mistaken for estimates
# --------------------------------------------------------------------------


def test_a_placeholder_target_will_not_certify() -> None:
    seeds, masks = _blocks()
    placeholder = Target.on_margin(
        'use', 'row', pd.Series([20.0], index=['r1']), 'PLACEHOLDER: NIPA T2.4.5U'
    )
    targets = TargetSet.of(placeholder)
    assert len(targets.placeholders) == 1

    with pytest.raises(UnsourcedTargets, match='placeholder'):
        precheck(seeds, masks, targets)

    assert precheck(seeds, masks, targets, allow_placeholders=True) == []


def test_a_target_needs_a_source() -> None:
    with pytest.raises(ValueError, match='provenance'):
        Target.on_margin('use', 'row', pd.Series([1.0], index=['r1']), '')


def test_the_same_source_cannot_constrain_the_same_margin_twice() -> None:
    target = _row_target('r1', 1.0)
    with pytest.raises(ValueError, match='twice'):
        TargetSet.of(target, target)


def test_target_set_summary_reports_mode_and_provenance() -> None:
    targets = TargetSet.of(
        _row_target('r1', 25.0, hard=True),
        Target.on_margin(
            'use', 'row', pd.Series([10.0], index=['r2']), 'PLACEHOLDER: x', weight=0.7
        ),
    )
    summary = targets.summary()
    assert list(summary['mode']) == ['H', 'S0.7']
    assert list(summary['placeholder']) == [False, True]


# --------------------------------------------------------------------------
# the subsidy sign convention
# --------------------------------------------------------------------------


def test_subsidies_must_be_stored_negative() -> None:
    """BEA stores the Use row positive and the Supply column negative."""
    use = pd.DataFrame(
        [[1.0, 2.0], [59876.0, 0.0]], index=['V00100', 'T00SUB'], columns=['a', 'b']
    )
    with pytest.raises(ValueError, match='stores subsidies negative'):
        assert_subsidies_negative(use, axis='row', label='T00SUB')

    normalised = use.copy()
    normalised.loc['T00SUB'] *= -1
    assert_subsidies_negative(normalised, axis='row', label='T00SUB')
