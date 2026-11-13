"""The Use tax rows follow the Supply tax columns through the balance.

``T00TOP``/``T00SUB`` are a conversion of ``TOP``/``MDTY``/``SUB``. The
engine's tax-row closer puts the rows on the operator's conversion after each
Use pass and takes the difference off ``V00300``. These tests use small frames
and need no data.

The group hold keeps ``TOP``/``SUB`` on BEA's summary groups while the detail
moves inside them.
"""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.transform.iot import nowcast_sut_assembly as asm
from bedrock.transform.iot import nowcast_va_taxes as vt
from bedrock.transform.iot.nowcast_sut_gras import (
    GroupHold,
    _apply_tax_row_closer,
    _balance_block,
    engine,
)
from bedrock.utils.economic.balance import (
    SutMask,
    Target,
    TargetSet,
    TargetTerm,
    offset_targets,
    restore_fixed_blocks,
    split_fixed_blocks,
)
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES

ROWS = ('c1', 'T00TOP', 'T00SUB', 'V00300')


def _use() -> pd.DataFrame:
    return pd.DataFrame(
        [[6.0, 3.0], [2.0, 0.0], [-1.0, 0.0], [5.0, 0.0]],
        index=list(ROWS),
        columns=['i1', 'F01000'],
    )


def _desired(top: float, sub: float) -> pd.DataFrame:
    return pd.DataFrame({'i1': [top, sub]}, index=['T00TOP', 'T00SUB'])


# --- the closer -------------------------------------------------------------


def test_the_closer_puts_the_rows_on_target_and_the_offset_on_v00300() -> None:
    use = _use()
    closed = _apply_tax_row_closer(use, SutMask.from_pattern(use), _desired(3.5, -1.5))
    assert closed.at['T00TOP', 'i1'] == pytest.approx(3.5)
    assert closed.at['T00SUB', 'i1'] == pytest.approx(-1.5)
    assert closed.at['V00300', 'i1'] == pytest.approx(5.0 - 1.5 + 0.5)
    pd.testing.assert_series_equal(closed.sum(axis=0), use.sum(axis=0))
    assert closed.at['c1', 'i1'] == use.at['c1', 'i1']


def test_the_closer_does_not_change_its_input() -> None:
    use = _use()
    _apply_tax_row_closer(use, SutMask.from_pattern(use), _desired(3.5, -1.5))
    pd.testing.assert_frame_equal(use, _use())


def test_a_frozen_v00300_leaves_the_column_alone() -> None:
    use = _use()
    fixed = pd.DataFrame(False, index=use.index, columns=use.columns)
    fixed.at['V00300', 'i1'] = True
    mask = SutMask.from_pattern(use, fixed_value=fixed)
    closed = _apply_tax_row_closer(use, mask, _desired(3.5, -1.5))
    pd.testing.assert_frame_equal(closed, use)


def test_a_move_across_a_sign_lock_is_skipped() -> None:
    use = _use()
    locks = pd.DataFrame(0, index=use.index, columns=use.columns)
    locks.at['T00SUB', 'i1'] = -1
    mask = SutMask.from_pattern(use, sign_lock=locks)
    closed = _apply_tax_row_closer(use, mask, _desired(3.5, 0.5))
    assert closed.at['T00SUB', 'i1'] == use.at['T00SUB', 'i1']
    assert closed.at['T00TOP', 'i1'] == pytest.approx(3.5)
    assert closed.at['V00300', 'i1'] == pytest.approx(5.0 - 1.5)


# --- the engine -------------------------------------------------------------


def _targets(use: pd.DataFrame) -> TargetSet:
    t1 = Target.on_margin(
        'use',
        'column',
        use[['i1']].sum(axis=0),
        'test',
        name='T1',
        hard=True,
        allow_negative=True,
    )
    t11 = Target(
        terms=(TargetTerm('supply', 'row', 1.0), TargetTerm('use', 'row', -1.0)),
        values=pd.Series([0.0], index=pd.Index(['c1'], name='commodity')),
        source='test',
        hard=True,
        name='T11',
        allow_negative=True,
    )
    return TargetSet.of(t1, t11)


def test_the_engine_runs_the_closer_after_every_use_pass_and_at_the_end() -> None:
    use = _use()
    supply = pd.DataFrame({'i1': [9.0]}, index=['c1'])
    masks = {'use': SutMask.from_pattern(use), 'supply': SutMask.from_pattern(supply)}
    seeds = {'use': use, 'supply': supply}
    frozen, free = split_fixed_blocks(seeds, masks)
    seen: list[pd.DataFrame] = []

    def tax_rows(z_supply: pd.DataFrame) -> pd.DataFrame:
        seen.append(z_supply.copy())
        return _desired(2.5, -0.5)

    out = engine(
        free,
        offset_targets(_targets(use), frozen),
        masks,
        tax_rows=tax_rows,
        atol=1e-9,
    )
    restored = restore_fixed_blocks(out.blocks, frozen)
    # Once per Use pass, and once more on the last Supply pass's columns.
    assert len(seen) == out.outer_iterations + 1
    pd.testing.assert_frame_equal(seen[-1], out.blocks['supply'])
    assert restored['use'].at['T00TOP', 'i1'] == pytest.approx(2.5)
    assert restored['use'].at['T00SUB', 'i1'] == pytest.approx(-0.5)
    assert float(restored['use']['i1'].sum()) == pytest.approx(
        float(use['i1'].sum()), abs=1e-6
    )


# --- the subsidy operator ---------------------------------------------------


@pytest.fixture
def seed_shares(monkeypatch: pytest.MonkeyPatch) -> pd.DataFrame:
    """A seed split: commodity 0's subsidy goes 75/25 to industries 0 and 1."""
    industries = list(USA_2017_INDUSTRY_CODES)
    commodities = list(USA_2017_COMMODITY_CODES)
    shares = pd.DataFrame(0.0, index=industries, columns=commodities)
    shares.at[industries[0], commodities[0]] = 0.75
    shares.at[industries[1], commodities[0]] = 0.25
    monkeypatch.setattr(vt, '_seed_sub_shares', lambda year: shares)
    return shares


def _block(cells: dict[tuple[int, int], float]) -> pd.DataFrame:
    commodities = list(USA_2017_COMMODITY_CODES)
    industries = list(USA_2017_INDUSTRY_CODES)
    block = pd.DataFrame(0.0, index=commodities, columns=industries)
    for (c, i), value in cells.items():
        block.iat[c, i] = value
    return block


def test_a_seeded_subsidy_keeps_its_split_at_the_new_level(
    seed_shares: pd.DataFrame,
) -> None:
    commodity = list(USA_2017_COMMODITY_CODES)[0]
    cells = vt.t00sub_cells_on(2022, pd.Series({commodity: -8.0}), _block({}))
    industries = list(USA_2017_INDUSTRY_CODES)
    assert cells.at[industries[0], commodity] == pytest.approx(-6.0)
    assert cells.at[industries[1], commodity] == pytest.approx(-2.0)
    assert float(cells.to_numpy().sum()) == pytest.approx(-8.0)


def test_an_unseeded_subsidy_takes_the_market_shares(
    seed_shares: pd.DataFrame,
) -> None:
    commodity = list(USA_2017_COMMODITY_CODES)[1]
    block = _block({(1, 2): 30.0, (1, 3): 10.0})
    cells = vt.t00sub_cells_on(2022, pd.Series({commodity: -4.0}), block)
    industries = list(USA_2017_INDUSTRY_CODES)
    assert cells.at[industries[2], commodity] == pytest.approx(-3.0)
    assert cells.at[industries[3], commodity] == pytest.approx(-1.0)


def test_an_unseeded_subsidy_with_no_producer_raises(
    seed_shares: pd.DataFrame,
) -> None:
    commodity = list(USA_2017_COMMODITY_CODES)[1]
    with pytest.raises(ValueError, match='no producer'):
        vt.t00sub_cells_on(2022, pd.Series({commodity: -4.0}), _block({}))


# --- the group hold ---------------------------------------------------------


GROUPS = pd.Series({'c1': 'g1', 'c2': 'g1', 'c3': 'g2'})


def _supply() -> pd.DataFrame:
    return pd.DataFrame(
        {'i1': [4.0, 4.0, 2.0], 'TOP': [1.0, 1.0, 2.0]}, index=['c1', 'c2', 'c3']
    )


def test_a_held_column_meets_each_group_and_keeps_the_rows() -> None:
    supply = _supply()
    hold = GroupHold('TOP', GROUPS, pd.Series({'g1': 3.0, 'g2': 1.0}))
    balanced, result = _balance_block(
        supply,
        SutMask.from_pattern(supply),
        supply.sum(axis=1),
        pd.Series({'i1': 10.0, 'TOP': 4.0}),
        rtol=1e-12,
        close_rows_exactly=True,
        holds=(hold,),
    )
    assert result.converged
    by_group = balanced['TOP'].groupby(GROUPS).sum()
    assert by_group['g1'] == pytest.approx(3.0, abs=1e-6)
    assert by_group['g2'] == pytest.approx(1.0, abs=1e-6)
    pd.testing.assert_series_equal(balanced.sum(axis=1), supply.sum(axis=1))
    assert list(balanced.columns) == ['i1', 'TOP']


def test_a_group_with_no_free_cell_holds_where_it_is() -> None:
    supply = _supply()
    fixed = pd.DataFrame(False, index=supply.index, columns=supply.columns)
    fixed.at['c3', 'TOP'] = True
    _frozen, free = split_fixed_blocks(
        {'supply': supply}, {'supply': SutMask.from_pattern(supply, fixed_value=fixed)}
    )
    hold = GroupHold('TOP', GROUPS, pd.Series({'g1': 2.0, 'g2': 5.0}))
    balanced, _ = _balance_block(
        free['supply'],
        SutMask.from_pattern(supply, fixed_value=fixed),
        free['supply'].sum(axis=1),
        pd.Series({'i1': 10.0, 'TOP': 0.0}),
        rtol=1e-12,
        close_rows_exactly=True,
        holds=(hold,),
    )
    assert balanced.at['c3', 'TOP'] == 0.0
    assert balanced['TOP'].sum() == pytest.approx(2.0, abs=1e-6)


@pytest.fixture
def published(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(asm, 'commodity_groups', lambda: GROUPS)
    monkeypatch.setattr(
        asm,
        'published_tax_groups',
        lambda year: pd.DataFrame(
            {'TOP': [3.0, 1.0], 'SUB': [0.0, 0.0]}, index=['g1', 'g2']
        ),
    )


def test_the_seed_is_scaled_onto_the_published_groups(published: None) -> None:
    supply = _supply().assign(SUB=0.0)
    held, report = asm.hold_seed_on_tax_groups(
        2022, supply, SutMask.from_pattern(supply)
    )
    assert held.loc[['c1', 'c2'], 'TOP'].tolist() == pytest.approx([1.5, 1.5])
    assert held.at['c3', 'TOP'] == pytest.approx(1.0)
    assert report.loc[('TOP', 'g1'), 'seed'] == pytest.approx(2.0)
    assert report.loc[('TOP', 'g1'), 'held'] == pytest.approx(3.0)
    pd.testing.assert_series_equal(held['i1'], supply['i1'])


def test_a_frozen_seed_cell_keeps_its_value(published: None) -> None:
    supply = _supply().assign(SUB=0.0)
    fixed = pd.DataFrame(False, index=supply.index, columns=supply.columns)
    fixed.at['c1', 'TOP'] = True
    held, _ = asm.hold_seed_on_tax_groups(
        2022, supply, SutMask.from_pattern(supply, fixed_value=fixed)
    )
    assert held.at['c1', 'TOP'] == 1.0
    assert held.at['c2', 'TOP'] == pytest.approx(2.0)


def test_a_group_whose_scale_would_flip_its_sign_keeps_its_seed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """g1's seed TOP is +2 but the group is published at -3: no flip, a warning."""
    monkeypatch.setattr(asm, 'commodity_groups', lambda: GROUPS)
    monkeypatch.setattr(
        asm,
        'published_tax_groups',
        lambda year: pd.DataFrame(
            {'TOP': [-3.0, 1.0], 'SUB': [0.0, 0.0]}, index=['g1', 'g2']
        ),
    )
    warned: list[str] = []
    monkeypatch.setattr(
        asm.log, 'warning', lambda message, *args: warned.append(message % args)
    )
    supply = _supply().assign(SUB=0.0)
    held, report = asm.hold_seed_on_tax_groups(
        2022, supply, SutMask.from_pattern(supply)
    )

    pd.testing.assert_series_equal(
        held.loc[['c1', 'c2'], 'TOP'], supply.loc[['c1', 'c2'], 'TOP']
    )
    assert report.loc[('TOP', 'g1'), 'held'] == pytest.approx(2.0)
    assert any('flip their sign' in w for w in warned)


# --- the import hold (#1288) ------------------------------------------------


def test_the_import_hold_targets_the_published_groups_less_frozen(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(asm, 'commodity_groups', lambda: GROUPS)
    monkeypatch.setattr(
        asm, 'published_import_groups', lambda year: pd.Series({'g1': 5.0, 'g2': 3.0})
    )
    frozen = pd.DataFrame({'i1': 0.0, 'MCIF': [0.0, 1.5, 0.0]}, index=GROUPS.index)
    (hold,) = asm.import_group_holds(2017, frozen)
    assert hold.column == 'MCIF'
    assert hold.targets.to_dict() == pytest.approx({'g1': 3.5, 'g2': 3.0})


def test_held_imports_keep_each_group_where_the_rows_would_move_them() -> None:
    """The rows want c1 and c2 to lose 2 of supply and c3 to gain 2; without
    the hold, imports move from g1 to g2; held, i1 absorbs it instead."""
    supply = pd.DataFrame(
        {'i1': [4.0, 4.0, 2.0], 'MCIF': [2.0, 2.0, 2.0]}, index=['c1', 'c2', 'c3']
    )
    rows = pd.Series({'c1': 5.0, 'c2': 5.0, 'c3': 6.0})
    cols = pd.Series({'i1': 10.0, 'MCIF': 6.0})
    mask = SutMask.from_pattern(supply)
    hold = GroupHold('MCIF', GROUPS, pd.Series({'g1': 4.0, 'g2': 2.0}))
    free, _ = _balance_block(
        supply, mask, rows, cols, rtol=1e-12, close_rows_exactly=True
    )
    held, _ = _balance_block(
        supply, mask, rows, cols, rtol=1e-12, close_rows_exactly=True, holds=(hold,)
    )
    assert free.loc[['c1', 'c2'], 'MCIF'].sum() < 4.0
    assert held.loc[['c1', 'c2'], 'MCIF'].sum() == pytest.approx(4.0, abs=1e-6)
    assert held.at['c3', 'MCIF'] == pytest.approx(2.0, abs=1e-6)
    pd.testing.assert_series_equal(held.sum(axis=1), rows, check_names=False)


# --- fixed service exports (#1250) ------------------------------------------


def test_service_export_cells_are_fixed_and_goods_stay_free(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        asm,
        'commodity_groups',
        lambda: pd.Series({'c1': '514', 'c2': '325', 'c3': '561'}),
    )
    use = pd.DataFrame(
        {'i1': [1.0, 1.0, 1.0], 'F04000': [2.0, 3.0, 0.0]}, index=['c1', 'c2', 'c3']
    )
    zero = pd.DataFrame(False, index=use.index, columns=use.columns)
    mask = SutMask(
        structural_zero=zero, fixed_value=zero.copy(), sign_lock=zero.astype(int)
    )
    fixed = asm.fix_service_exports(mask, use).fixed_value
    assert fixed['F04000'].to_dict() == {'c1': True, 'c2': False, 'c3': True}
    assert not fixed['i1'].any()
