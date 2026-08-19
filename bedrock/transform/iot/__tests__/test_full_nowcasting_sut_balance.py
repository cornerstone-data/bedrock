"""PR1 / PR2 / PR3 stages on one toy SUT. No 2017 / GCS."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from bedrock.transform.iot.__tests__.test_nowcast_sut_gras import (
    INDUSTRIES,
    _t1,
    _t2,
    _t11,
    _t12,
    _t13,
    _t14,
    _t15,
    _t16,
    _t17,
)
from bedrock.transform.iot.nowcast_sut_gras import engine
from bedrock.utils.economic.balance import (
    Aggregator,
    SutMask,
    Target,
    TargetSet,
    TargetTerm,
    gras_balance,
    offset_targets,
    restore_fixed_blocks,
    split_fixed_blocks,
)


def _full_use() -> pd.DataFrame:
    return pd.DataFrame(
        [
            [8.0, 0.0, 4.0],
            [6.0, 0.0, 2.0],
            [2.0, 5.0, 0.0],
            [-1.0, 0.0, 0.0],
            [3.0, 0.0, 0.0],
            [4.0, 0.0, 0.0],
        ],
        index=['c1', 'c2', 'T00TOP', 'T00SUB', 'V00100', 'V00300'],
        columns=['i1', '4200ID', 'F01000'],
    )


def _full_supply() -> pd.DataFrame:
    return pd.DataFrame(
        [
            [6.0, 0.0, 1.0, 1.0, -1.0, 1.0, 4.0],
            [8.0, 0.0, -1.0, -1.0, 0.0, 1.0, 1.0],
        ],
        index=['c1', 'c2'],
        columns=['i1', '4200ID', 'TRADE ', 'TRANS', 'SUB', 'TOP', 'MDTY'],
    )


def _full_masks(use: pd.DataFrame, supply: pd.DataFrame) -> dict[str, SutMask]:
    """Sign-lock every Use row except V00100/V00300 so T4 cannot raid T11."""
    use_sign = pd.DataFrame(0, index=use.index, columns=use.columns, dtype=int)
    for row in use.index:
        if row not in ('V00100', 'V00300'):
            use_sign.loc[row] = 1
    use_sign.loc['T00SUB'] = -1
    return {
        'use': SutMask.from_pattern(use, sign_lock=use_sign),
        'supply': SutMask.from_pattern(supply),
    }


def _full_hard_set(use: pd.DataFrame, supply: pd.DataFrame) -> TargetSet:
    return TargetSet.of(
        _t1(use[list(INDUSTRIES)].sum()),
        _t11(),
        _t12(),
        _t13(),
        _t14(),
        _t15(),
        _t16(),
        _t17(use, supply),
    )


def test_full_nowcasting_sut_balance() -> None:
    original_use = _full_use()
    original_supply = _full_supply()
    masks = _full_masks(original_use, original_supply)

    # Stage 1 — PR1 kernel on Use. Re-split before each later stage.
    _frozen, free = split_fixed_blocks(
        {'use': original_use.copy(), 'supply': original_supply.copy()}, masks
    )
    use_z = free['use']
    use_mask = masks['use']
    row_t = use_z.sum(axis=1).astype(float)
    col_t = use_z.sum(axis=0).astype(float)
    kernel = gras_balance(
        matrix=use_z.to_numpy(dtype=np.float64),
        row_targets=row_t.to_numpy(dtype=np.float64),
        col_targets=col_t.to_numpy(dtype=np.float64),
        free_mask=use_mask.free.to_numpy(),
        sign_flex=use_mask.sign_lock.to_numpy() == 0,
        project_infeasible=False,
        close_rows_exactly=False,
    )
    assert kernel.converged
    balanced = pd.DataFrame(kernel.matrix, index=use_z.index, columns=use_z.columns)
    assert not bool((~use_mask.free & (balanced != 0)).any().any())

    # Stage 2 — PR2 hard protocol; T2 skipped.
    frozen, free = split_fixed_blocks(
        {'use': original_use.copy(), 'supply': original_supply.copy()}, masks
    )
    t2 = _t2(pd.Series({'F01000': 8.0}), weight=0.5)
    hard = _full_hard_set(original_use, original_supply)
    hard_plus_t2 = TargetSet.of(*hard.targets, t2)
    residual = offset_targets(hard_plus_t2, frozen)
    out2 = engine(
        free,
        residual,
        masks,
        impose_soft=False,
        close_rows_on_last=False,
        atol=1e-6,
    )
    restored2 = restore_fixed_blocks(out2.blocks, frozen)
    assert 'T2' in out2.skipped
    by_name = {t.name: t for t in hard_plus_t2}
    for name in ('T1', 'T11', 'T12', 'T13', 'T14', 'T17'):
        err = (by_name[name].evaluate(restored2) - by_name[name].values).abs().max()
        assert float(err) < 1e-5, name
    assert out2.blocks['use']['F01000'].sum() == pytest.approx(
        float(free['use']['F01000'].sum()), rel=1e-6
    )

    # Stage 3 — PR3 on a fresh split of the same original seeds.
    frozen, free = split_fixed_blocks(
        {'use': original_use.copy(), 'supply': original_supply.copy()}, masks
    )
    aggregator = Aggregator.from_mapping(
        {'g': list(INDUSTRIES)}, list(original_use.columns)
    )
    t4 = Target(
        terms=(TargetTerm('use', 'column', 1.0, aggregator, restrict_to=('V00100',)),),
        values=pd.Series({'g': 6.0}),
        source='test',
        name='T4',
        hard=False,
        weight=0.6,
    )
    stage3 = TargetSet.of(*hard_plus_t2.targets, t4)
    residual = offset_targets(stage3, frozen)
    entry_t2 = next(t for t in residual if t.name == 'T2')
    t2_seed = float(free['use']['F01000'].sum())
    out3 = engine(
        free,
        residual,
        masks,
        impose_soft=True,
        close_rows_on_last=False,
        atol=1e-6,
    )
    restored3 = restore_fixed_blocks(out3.blocks, frozen)
    assert 'T2' not in out3.skipped
    assert 'T4' not in out3.skipped
    t1 = next(t for t in stage3 if t.name == 'T1')
    pd.testing.assert_series_equal(
        restored3['use'][list(INDUSTRIES)].sum(axis=0).astype(float),
        t1.values.astype(float),
        check_names=False,
        rtol=1e-6,
    )
    for name in ('T11', 'T12', 'T13', 'T14', 'T17'):
        err = (by_name[name].evaluate(restored3) - by_name[name].values).abs().max()
        assert float(err) < 1e-4, name
    t2_after = float(out3.blocks['use']['F01000'].sum())
    assert t2_after != pytest.approx(t2_seed, rel=1e-6)
    assert t2_after != pytest.approx(float(entry_t2.values.loc['F01000']), rel=1e-6)
    t4_res = next(t for t in residual if t.name == 'T4')
    initial = float((t4_res.evaluate(free) - t4_res.values.astype(float)).abs().max())
    final = float(
        (t4_res.evaluate(out3.blocks) - t4_res.values.astype(float)).abs().max()
    )
    if initial > 1e-9:
        assert final < (1.0 - t4.weight + 1e-6) * initial + 1e-9
