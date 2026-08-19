"""Hand-checkable tests for SUT orchestration."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from bedrock.utils.economic.balance import (
    Aggregator,
    SutBalanceResult,
    SutMask,
    Target,
    TargetSet,
    TargetTerm,
    engine,
    offset_targets,
    restore_fixed_blocks,
    split_fixed_blocks,
)

USE_ROWS = ('c1', 'c2', 'T00TOP', 'T00SUB', 'V00100')
USE_COLS = ('i1', '4200ID', 'F01000')
SUPPLY_ROWS = ('c1', 'c2')
SUPPLY_COLS = ('i1', '4200ID', 'TRADE ', 'TRANS', 'SUB', 'TOP', 'MDTY')
INDUSTRIES = ('i1', '4200ID')
COMMODITIES = ('c1', 'c2')


def _use_seed() -> pd.DataFrame:
    return pd.DataFrame(
        [
            [8.0, 0.0, 4.0],
            [6.0, 0.0, 2.0],
            [2.0, 5.0, 0.0],
            [-1.0, 0.0, 0.0],
            [3.0, 0.0, 0.0],
        ],
        index=list(USE_ROWS),
        columns=list(USE_COLS),
    )


def _supply_seed() -> pd.DataFrame:
    return pd.DataFrame(
        [
            [6.0, 0.0, 1.0, 1.0, -1.0, 1.0, 4.0],
            [8.0, 0.0, -1.0, -1.0, 0.0, 1.0, 1.0],
        ],
        index=list(SUPPLY_ROWS),
        columns=list(SUPPLY_COLS),
    )


def _scalar(name: str, value: float = 0.0) -> pd.Series:
    return pd.Series([value], index=pd.Index([name], name='identity'))


def _t1(values: pd.Series, *, hard: bool = True) -> Target:
    return Target.on_margin(
        'use',
        'column',
        values,
        'test',
        name='T1',
        hard=hard,
        allow_negative=True,
    )


def _t11(values: pd.Series | None = None, *, hard: bool = True) -> Target:
    if values is None:
        values = pd.Series(0.0, index=pd.Index(COMMODITIES, name='commodity'))
    return Target(
        terms=(TargetTerm('supply', 'row', 1.0), TargetTerm('use', 'row', -1.0)),
        values=values,
        source='test',
        name='T11',
        hard=hard,
        allow_negative=True,
    )


def _t12(*, hard: bool = True) -> Target:
    return Target(
        terms=(
            TargetTerm(
                'use',
                'row',
                1.0,
                Aggregator.total(['T00SUB'], list(USE_ROWS), 'subsidies'),
            ),
            TargetTerm(
                'supply',
                'column',
                -1.0,
                Aggregator.total(['SUB'], list(SUPPLY_COLS), 'subsidies'),
            ),
        ),
        values=_scalar('subsidies'),
        source='test',
        name='T12',
        hard=hard,
        allow_negative=True,
    )


def _t13(*, hard: bool = True) -> Target:
    return Target(
        terms=(
            TargetTerm(
                'use',
                'row',
                1.0,
                Aggregator.total(['T00TOP'], list(USE_ROWS), 'product_taxes'),
            ),
            TargetTerm(
                'supply',
                'column',
                -1.0,
                Aggregator.total(['TOP', 'MDTY'], list(SUPPLY_COLS), 'product_taxes'),
            ),
        ),
        values=_scalar('product_taxes'),
        source='test',
        name='T13',
        hard=hard,
        allow_negative=True,
    )


def _t14(*, hard: bool = True) -> Target:
    return Target(
        terms=(
            TargetTerm(
                'use',
                'column',
                1.0,
                Aggregator.total(['4200ID'], list(USE_COLS), 'customs_duties'),
                restrict_to=('T00TOP',),
            ),
            TargetTerm(
                'supply',
                'column',
                -1.0,
                Aggregator.total(['MDTY'], list(SUPPLY_COLS), 'customs_duties'),
            ),
        ),
        values=_scalar('customs_duties'),
        source='test',
        name='T14',
        hard=hard,
        allow_negative=True,
    )


def _t15(*, hard: bool = True) -> Target:
    return Target(
        terms=(
            TargetTerm(
                'supply',
                'column',
                1.0,
                Aggregator.total(['TRADE '], list(SUPPLY_COLS), 'trade_margin'),
            ),
        ),
        values=_scalar('trade_margin'),
        source='test',
        name='T15',
        hard=hard,
        allow_negative=True,
    )


def _t16(*, hard: bool = True) -> Target:
    return Target(
        terms=(
            TargetTerm(
                'supply',
                'column',
                1.0,
                Aggregator.total(['TRANS'], list(SUPPLY_COLS), 'transport_margin'),
            ),
        ),
        values=_scalar('transport_margin'),
        source='test',
        name='T16',
        hard=hard,
        allow_negative=True,
    )


def _t17(use: pd.DataFrame, supply: pd.DataFrame, *, hard: bool = True) -> Target:
    industries = pd.Index(INDUSTRIES, name='industry')
    values = (
        supply.sum(axis=0).reindex(industries)
        - use.sum(axis=0).reindex(industries)
        + use.loc[['T00TOP', 'T00SUB']].sum(axis=0).reindex(industries)
    ).astype(float)
    return Target(
        terms=(
            TargetTerm('supply', 'column', 1.0),
            TargetTerm('use', 'column', -1.0),
            TargetTerm('use', 'column', 1.0, restrict_to=('T00TOP', 'T00SUB')),
        ),
        values=values,
        source='test',
        name='T17',
        hard=hard,
        allow_negative=True,
    )


def _hard_set(
    use: pd.DataFrame,
    supply: pd.DataFrame,
    extra: tuple[Target, ...] = (),
) -> TargetSet:
    t1 = _t1(use[list(INDUSTRIES)].sum(axis=0))
    return TargetSet.of(
        t1,
        _t11(),
        _t12(),
        _t13(),
        _t14(),
        _t15(),
        _t16(),
        _t17(use, supply),
        *extra,
    )


def _masks(
    use: pd.DataFrame,
    supply: pd.DataFrame,
    *,
    use_fixed: pd.DataFrame | None = None,
    supply_fixed: pd.DataFrame | None = None,
    supply_sign: pd.DataFrame | None = None,
) -> dict[str, SutMask]:
    return {
        'use': SutMask.from_pattern(use, fixed_value=use_fixed),
        'supply': SutMask.from_pattern(
            supply, fixed_value=supply_fixed, sign_lock=supply_sign
        ),
    }


def _run(
    use: pd.DataFrame,
    supply: pd.DataFrame,
    targets: TargetSet,
    masks: dict[str, SutMask],
    **engine_kw: object,
) -> tuple[
    dict[str, pd.DataFrame],
    dict[str, pd.DataFrame],
    SutBalanceResult,
    dict[str, pd.DataFrame],
]:
    seeds = {'use': use, 'supply': supply}
    frozen, free = split_fixed_blocks(seeds, masks)
    residual = offset_targets(targets, frozen)
    out = engine(free, residual, masks, **engine_kw)  # type: ignore[arg-type]
    restored = restore_fixed_blocks(out.blocks, frozen)
    return frozen, free, out, restored


def test_t11_frozen_fd_is_not_z_only_equality() -> None:
    use, supply = _use_seed(), _supply_seed()
    use_fixed = pd.DataFrame(False, index=use.index, columns=use.columns)
    use_fixed.loc['c1', 'F01000'] = True
    masks = _masks(use, supply, use_fixed=use_fixed)
    targets = _hard_set(use, supply)
    frozen, _free, out, restored = _run(use, supply, targets, masks)
    residual_t11 = offset_targets(targets, frozen)
    t11 = next(t for t in residual_t11 if t.name == 'T11')
    assert not (t11.values == 0.0).all()
    original = next(t for t in targets if t.name == 'T11')
    err = (original.evaluate(restored) - original.values).abs().max()
    assert float(err) < 1e-6
    z_use, z_supply = out.blocks['use'], out.blocks['supply']
    assert (
        not z_use.loc[list(COMMODITIES)]
        .sum(axis=1)
        .equals(z_supply.loc[list(COMMODITIES)].sum(axis=1))
    )


def test_t1_and_t15_use_residual_values() -> None:
    use, supply = _use_seed(), _supply_seed()
    supply_fixed = pd.DataFrame(False, index=supply.index, columns=supply.columns)
    supply_fixed.loc['c1', 'TRADE '] = True
    masks = _masks(use, supply, supply_fixed=supply_fixed)
    targets = _hard_set(use, supply)
    frozen, _free, out, restored = _run(
        use, supply, targets, masks, close_rows_on_last=False
    )
    t1 = next(t for t in targets if t.name == 'T1')
    pd.testing.assert_series_equal(
        restored['use'][list(INDUSTRIES)].sum(axis=0).astype(float),
        t1.values.astype(float),
        check_names=False,
        rtol=1e-6,
    )
    residual = offset_targets(targets, frozen)
    t15 = next(t for t in residual if t.name == 'T15')
    assert float(t15.values.item()) != 0.0
    assert out.blocks['supply']['TRADE '].sum() == pytest.approx(
        float(t15.values.item()), rel=1e-6
    )


def test_t11_iterate_hits_residual_values() -> None:
    use, supply = _use_seed(), _supply_seed()
    masks = _masks(use, supply)
    targets = _hard_set(use, supply)
    _frozen, _free, out, _restored = _run(use, supply, targets, masks, atol=1e-6)
    t11 = next(t for t in offset_targets(targets, _frozen) if t.name == 'T11')
    err = (t11.evaluate(out.blocks) - t11.values).abs().max()
    assert float(err) <= 1e-6


def test_t12_t14_t17_on_restored_blocks() -> None:
    use, supply = _use_seed(), _supply_seed()
    masks = _masks(use, supply)
    targets = _hard_set(use, supply)
    _frozen, _free, _out, restored = _run(
        use, supply, targets, masks, close_rows_on_last=False
    )
    by_name = {t.name: t for t in targets}
    for name in ('T12', 'T13', 'T14', 'T17'):
        err = (by_name[name].evaluate(restored) - by_name[name].values).abs().max()
        assert float(err) < 1e-5, name
    t17 = by_name['T17']
    wedge = restored['use'].loc[['T00TOP', 'T00SUB']].sum(axis=0)
    assert t17.terms[2].restrict_to == ('T00TOP', 'T00SUB')
    pd.testing.assert_series_equal(
        (restored['supply'].sum(axis=0) - restored['use'].sum(axis=0) + wedge).loc[
            t17.values.index
        ],
        t17.values.astype(float),
        check_names=False,
        rtol=1e-5,
    )


def test_skip_soft_does_not_read_values() -> None:
    use, supply = _use_seed(), _supply_seed()
    masks = _masks(use, supply)
    t2 = Target.on_margin(
        'use',
        'column',
        pd.Series([999.0], index=['F01000']),
        'PLACEHOLDER: T2',
        name='T2',
        hard=False,
    )
    targets = _hard_set(use, supply, extra=(t2,))
    _frozen, free, out, _restored = _run(
        use, supply, targets, masks, close_rows_on_last=False
    )
    assert 'T2' in out.skipped
    assert out.blocks['use']['F01000'].sum() == pytest.approx(
        free['use']['F01000'].sum(), rel=1e-6
    )


def test_unknown_hard_name_raises() -> None:
    use, supply = _use_seed(), _supply_seed()
    masks = _masks(use, supply)
    bogus = Target.on_margin(
        'use',
        'row',
        pd.Series([1.0], index=['c1']),
        'test',
        name='T99',
        hard=True,
    )
    targets = TargetSet.of(_t1(use[list(INDUSTRIES)].sum()), _t11(), bogus)
    frozen, free = split_fixed_blocks({'use': use, 'supply': supply}, masks)
    with pytest.raises(ValueError, match='unknown hard'):
        engine(free, offset_targets(targets, frozen), masks)


def test_missing_t1_or_t11_raises() -> None:
    use, supply = _use_seed(), _supply_seed()
    masks = _masks(use, supply)
    frozen, free = split_fixed_blocks({'use': use, 'supply': supply}, masks)
    only_t11 = TargetSet.of(_t11())
    with pytest.raises(ValueError, match='T1'):
        engine(free, offset_targets(only_t11, frozen), masks)
    only_t1 = TargetSet.of(_t1(use[list(INDUSTRIES)].sum()))
    with pytest.raises(ValueError, match='T11'):
        engine(free, offset_targets(only_t1, frozen), masks)


def test_t1_or_t11_soft_raises() -> None:
    use, supply = _use_seed(), _supply_seed()
    masks = _masks(use, supply)
    frozen, free = split_fixed_blocks({'use': use, 'supply': supply}, masks)
    soft_t1 = TargetSet.of(_t1(use[list(INDUSTRIES)].sum(), hard=False), _t11())
    with pytest.raises(ValueError, match='T1'):
        engine(free, offset_targets(soft_t1, frozen), masks)
    soft_t11 = TargetSet.of(_t1(use[list(INDUSTRIES)].sum()), _t11(hard=False))
    with pytest.raises(ValueError, match='T11'):
        engine(free, offset_targets(soft_t11, frozen), masks)


def test_hard_t13_without_t14_raises() -> None:
    use, supply = _use_seed(), _supply_seed()
    masks = _masks(use, supply)
    targets = TargetSet.of(_t1(use[list(INDUSTRIES)].sum()), _t11(), _t13(hard=True))
    frozen, free = split_fixed_blocks({'use': use, 'supply': supply}, masks)
    with pytest.raises(ValueError, match='T14'):
        engine(free, offset_targets(targets, frozen), masks)


def test_soft_t13_without_t14_and_t14_without_t13_do_not_raise() -> None:
    use, supply = _use_seed(), _supply_seed()
    masks = _masks(use, supply)
    frozen, free = split_fixed_blocks({'use': use, 'supply': supply}, masks)
    soft_t13 = TargetSet.of(_t1(use[list(INDUSTRIES)].sum()), _t11(), _t13(hard=False))
    engine(free, offset_targets(soft_t13, frozen), masks, max_outer=1)
    t14_only = TargetSet.of(_t1(use[list(INDUSTRIES)].sum()), _t11(), _t14(hard=True))
    engine(free, offset_targets(t14_only, frozen), masks, max_outer=1)


def test_duplicate_named_target_raises() -> None:
    use, supply = _use_seed(), _supply_seed()
    masks = _masks(use, supply)
    t1 = _t1(use[list(INDUSTRIES)].sum())
    targets = TargetSet.of(t1, t1.with_values(t1.values * 2, source_suffix='2'), _t11())
    frozen, free = split_fixed_blocks({'use': use, 'supply': supply}, masks)
    with pytest.raises(ValueError, match='duplicate'):
        engine(free, offset_targets(targets, frozen), masks)


def test_max_outer_less_than_one_raises() -> None:
    use, supply = _use_seed(), _supply_seed()
    masks = _masks(use, supply)
    targets = TargetSet.of(_t1(use[list(INDUSTRIES)].sum()), _t11())
    frozen, free = split_fixed_blocks({'use': use, 'supply': supply}, masks)
    with pytest.raises(ValueError, match='max_outer'):
        engine(free, offset_targets(targets, frozen), masks, max_outer=0)


def test_keyerror_t1_index_not_subset() -> None:
    use, supply = _use_seed(), _supply_seed()
    masks = _masks(use, supply)
    t1 = _t1(pd.Series({'i1': 1.0, 'missing': 2.0}))
    targets = TargetSet.of(t1, _t11())
    frozen, free = split_fixed_blocks({'use': use, 'supply': supply}, masks)
    with pytest.raises(KeyError):
        engine(free, offset_targets(targets, frozen), masks)


def test_keyerror_t11_index_not_subset() -> None:
    use, supply = _use_seed(), _supply_seed()
    masks = _masks(use, supply)
    t11 = _t11(pd.Series({'c1': 0.0, 'ghost': 0.0}))
    targets = TargetSet.of(_t1(use[list(INDUSTRIES)].sum()), t11)
    frozen, free = split_fixed_blocks({'use': use, 'supply': supply}, masks)
    with pytest.raises(KeyError):
        engine(free, offset_targets(targets, frozen), masks)


def test_keyerror_t17_index_not_subset_of_columns() -> None:
    use, supply = _use_seed(), _supply_seed()
    masks = _masks(use, supply)
    t17 = _t17(use, supply)
    t17 = t17.with_values(pd.concat([t17.values, pd.Series({'ghost': 0.0})]))
    targets = TargetSet.of(_t1(use[list(INDUSTRIES)].sum()), _t11(), t17)
    frozen, free = split_fixed_blocks({'use': use, 'supply': supply}, masks)
    with pytest.raises(KeyError):
        engine(free, offset_targets(targets, frozen), masks)


def test_keyerror_t17_missing_use_tax_rows() -> None:
    use = _use_seed().drop(index=['T00TOP', 'T00SUB'])
    supply = _supply_seed()
    masks = _masks(use, supply)
    t17 = Target(
        terms=(
            TargetTerm('supply', 'column', 1.0),
            TargetTerm('use', 'column', -1.0),
            TargetTerm('use', 'column', 1.0, restrict_to=('T00TOP', 'T00SUB')),
        ),
        values=pd.Series({'i1': 0.0, '4200ID': 0.0}),
        source='test',
        name='T17',
        hard=True,
        allow_negative=True,
    )
    targets = TargetSet.of(_t1(use[list(INDUSTRIES)].sum()), _t11(), t17)
    frozen, free = split_fixed_blocks({'use': use, 'supply': supply}, masks)
    with pytest.raises(KeyError):
        engine(free, offset_targets(targets, frozen), masks)


def test_pair_zero_always_runs_when_t11_already_holds() -> None:
    use, supply = _use_seed(), _supply_seed()
    masks = _masks(use, supply)
    scaled_t1 = _t1(use[list(INDUSTRIES)].sum() * 1.1)
    targets = TargetSet.of(scaled_t1, _t11())
    frozen, free = split_fixed_blocks({'use': use, 'supply': supply}, masks)
    residual = offset_targets(targets, frozen)
    t11 = next(t for t in residual if t.name == 'T11')
    assert float((t11.evaluate(free) - t11.values).abs().max()) <= 100.0
    out = engine(
        free, residual, masks, max_outer=2, close_rows_on_last=False, atol=100.0
    )
    assert out.outer_iterations == 1
    pd.testing.assert_series_equal(
        out.blocks['use'][list(INDUSTRIES)].sum(axis=0).astype(float),
        residual.targets[0].values.astype(float),
        check_names=False,
        rtol=1e-5,
    )


def test_finishing_pair_counts() -> None:
    use, supply = _use_seed(), _supply_seed()
    masks = _masks(use, supply)
    targets = TargetSet.of(_t1(use[list(INDUSTRIES)].sum()), _t11())
    frozen, free = split_fixed_blocks({'use': use, 'supply': supply}, masks)
    residual = offset_targets(targets, frozen)
    extra = engine(
        free, residual, masks, max_outer=2, close_rows_on_last=True, atol=100.0
    )
    assert extra.outer_iterations == 2
    one = engine(
        free, residual, masks, max_outer=1, close_rows_on_last=True, atol=100.0
    )
    assert one.outer_iterations == 1


def test_extra_keys_copy_through() -> None:
    use, supply = _use_seed(), _supply_seed()
    masks = _masks(use, supply)
    targets = TargetSet.of(_t1(use[list(INDUSTRIES)].sum()), _t11())
    frozen, free = split_fixed_blocks({'use': use, 'supply': supply}, masks)
    other = pd.DataFrame([[1.0]], index=['r'], columns=['c'])
    free = {**free, 'other': other}
    other_copy = other.copy()
    out = engine(free, offset_targets(targets, frozen), masks, max_outer=1)
    pd.testing.assert_frame_equal(out.blocks['other'], other_copy)
    assert 'other' not in out.last


def test_sign_lock_keeps_sub_nonpositive() -> None:
    use, supply = _use_seed(), _supply_seed()
    sign = pd.DataFrame(0, index=supply.index, columns=supply.columns, dtype=int)
    sign.loc['c1', 'SUB'] = -1
    masks = _masks(use, supply, supply_sign=sign)
    targets = _hard_set(use, supply)
    _frozen, _free, out, _restored = _run(
        use, supply, targets, masks, close_rows_on_last=False
    )
    assert float(np.asarray(out.blocks['supply'].loc['c1', 'SUB'])) <= 0.0


def test_restore_fixed_nonzero_f_unchanged() -> None:
    use, supply = _use_seed(), _supply_seed()
    use_fixed = pd.DataFrame(False, index=use.index, columns=use.columns)
    use_fixed.loc['c1', 'F01000'] = True
    masks = _masks(use, supply, use_fixed=use_fixed)
    targets = _hard_set(use, supply)
    frozen, _free, _out, restored = _run(use, supply, targets, masks)
    assert restored['use'].loc['c1', 'F01000'] == frozen['use'].loc['c1', 'F01000']
    assert restored['use'].loc['c1', 'F01000'] == 4.0


def test_assert_free_seed_rejects_full_x() -> None:
    use, supply = _use_seed(), _supply_seed()
    use_fixed = pd.DataFrame(False, index=use.index, columns=use.columns)
    use_fixed.loc['c1', 'F01000'] = True
    masks = _masks(use, supply, use_fixed=use_fixed)
    targets = _hard_set(use, supply)
    frozen, _free = split_fixed_blocks({'use': use, 'supply': supply}, masks)
    residual = offset_targets(targets, frozen)
    with pytest.raises(ValueError, match='fixed cells are nonzero'):
        engine({'use': use, 'supply': supply}, residual, masks)


def test_converged_false_still_returns() -> None:
    use, supply = _use_seed(), _supply_seed()
    use.loc['c1'] = 0.0
    none = pd.DataFrame(False, index=use.index, columns=use.columns)
    locks = pd.DataFrame(0, index=use.index, columns=use.columns, dtype=int)
    masks = {
        'use': SutMask(structural_zero=none, fixed_value=none.copy(), sign_lock=locks),
        'supply': SutMask.from_pattern(supply),
    }
    t1 = _t1(use[list(INDUSTRIES)].sum())
    t11 = _t11(pd.Series({'c1': 10.0, 'c2': 0.0}))
    targets = TargetSet.of(t1, t11)
    frozen, free = split_fixed_blocks({'use': use, 'supply': supply}, masks)
    out = engine(
        free,
        offset_targets(targets, frozen),
        masks,
        max_outer=1,
        close_rows_on_last=False,
        atol=1e-12,
    )
    assert isinstance(out.t11_max_abs_residual, float)
    assert out.last['use'].converged is False or out.last['supply'].converged is False


def test_empty_free_margin_valueerror_propagates() -> None:
    use, supply = _use_seed(), _supply_seed()
    use.loc['T00SUB'] = 0.0
    masks = _masks(use, supply)
    targets = TargetSet.of(_t1(use[list(INDUSTRIES)].sum()), _t11(), _t12())
    frozen, free = split_fixed_blocks({'use': use, 'supply': supply}, masks)
    residual = offset_targets(targets, frozen)
    t12 = next(t for t in residual if t.name == 'T12')
    t12 = t12.with_values(pd.Series([5.0], index=t12.values.index))
    residual = TargetSet.of(*[t12 if t.name == 'T12' else t for t in residual])
    with pytest.raises(ValueError, match='empty free margin'):
        engine(free, residual, masks, max_outer=1)


def test_t11_empty_use_row_closes_on_supply() -> None:
    """Empty-free Use T11 row holds; Supply absorbs; restored T11 holds.

    2017 1:1 FD commodities have no Use-side free cell and a frozen FD
    total. Do not zero the whole Use row: targeting Supply at 0 is a GRAS
    scale-by-zero (numpy warning) and does not exercise T11 on restore.
    """
    use, supply = _use_seed(), _supply_seed()
    use.loc['c1', list(INDUSTRIES)] = 0.0
    # Unconstrained Supply columns that sum to 0 make kernel atol 0.0 a
    # relative-inf GRAS stall (numpy warning in _fit_quality). The 2017
    # identity is T15/T16 = 0; this toy does not impose them.
    supply.loc['c2', 'TRADE '] = 2.0
    supply.loc['c2', 'TRANS'] = 0.5
    use_fixed = pd.DataFrame(False, index=use.index, columns=use.columns)
    use_fixed.loc['c1', 'F01000'] = True
    masks = _masks(use, supply, use_fixed=use_fixed)
    targets = TargetSet.of(_t1(use[list(INDUSTRIES)].sum()), _t11())
    frozen, _free, _out, restored = _run(
        use, supply, targets, masks, atol=1e-6
    )
    assert not bool(masks['use'].free.loc['c1'].any())
    residual = offset_targets(targets, frozen)
    t11_residual = next(t for t in residual if t.name == 'T11')
    assert float(t11_residual.values.loc['c1']) != 0.0
    original = next(t for t in targets if t.name == 'T11')
    err = (original.evaluate(restored) - original.values).abs().max()
    assert float(err) <= 1e-6
    pd.testing.assert_series_equal(
        restored['use'].loc['c1'].astype(float),
        use.loc['c1'].astype(float),
        check_names=False,
    )


def test_does_not_mutate_caller_free() -> None:
    use, supply = _use_seed(), _supply_seed()
    masks = _masks(use, supply)
    targets = TargetSet.of(_t1(use[list(INDUSTRIES)].sum()), _t11())
    frozen, free = split_fixed_blocks({'use': use, 'supply': supply}, masks)
    residual = offset_targets(targets, frozen)
    use_copy, supply_copy = free['use'].copy(), free['supply'].copy()
    engine(free, residual, masks, max_outer=1)
    pd.testing.assert_frame_equal(free['use'], use_copy)
    pd.testing.assert_frame_equal(free['supply'], supply_copy)
