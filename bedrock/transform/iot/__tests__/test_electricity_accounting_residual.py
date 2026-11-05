"""Unit tests for the electricity accounting residual, on synthetic tables."""

import pandas as pd
import pytest

from bedrock.transform.iot import eia_utility_go_adjustment as ug
from bedrock.transform.iot import electricity_accounting_residual as ear

CODE = ear.RESIDUAL_CODE


def _tables() -> tuple[pd.DataFrame, pd.DataFrame]:
    industries = ['221100', 'S00101', 'S00202', '111200', '531ORE']
    commodities = ['221100', '211000', '531ORE']
    use = pd.DataFrame(
        [
            [5.0, 1.0, 1.0, 40.0, 60.0, 200.0],
            [20.0, 2.0, 10.0, 15.0, 5.0, 3.0],
            [4.0, 0.0, 0.0, 5.0, 10.0, 50.0],
            [10.0, 2.0, 4.0, 20.0, 10.0, 0.0],
            [61.0, 5.0, 45.0, 40.0, 15.0, 0.0],
        ],
        index=[*commodities, 'V00100', 'V00300'],
        columns=[*industries, 'F01000'],
    )
    supply = pd.DataFrame(
        [
            [100.0, 10.0, 60.0, 0.0, 0.0, 2.0],
            [0.0, 0.0, 0.0, 120.0, 0.0, 1.0],
            [0.0, 0.0, 0.0, 0.0, 100.0, 0.0],
        ],
        index=commodities,
        columns=[*industries, 'MCIF'],
    )
    return use, supply


DELTA = pd.Series({'221100': 8.0, 'S00101': 1.0, 'S00202': 1.0})


def test_the_pair_is_added_and_only_the_named_cells_move() -> None:
    use, supply = _tables()
    new_use, new_supply = ear.split_residual(use, supply, DELTA)

    # Buyers lose 10 of 100 electricity, in proportion, and gain the residual.
    assert new_use.at['221100', '111200'] == pytest.approx(36.0)
    assert new_use.at['221100', '531ORE'] == pytest.approx(54.0)
    assert new_use.at[CODE, '111200'] == pytest.approx(4.0)
    assert new_use.at[CODE, '531ORE'] == pytest.approx(6.0)
    assert new_use[['111200', '531ORE']].sum().tolist() == pytest.approx(
        use[['111200', '531ORE']].sum().tolist()
    )
    # Electric inputs, households and other rows are untouched.
    assert new_use.at['211000', '221100'] == use.at['211000', '221100']
    assert new_use.at['221100', '221100'] == use.at['221100', '221100']
    assert new_use.at['221100', 'F01000'] == use.at['221100', 'F01000']
    assert new_use.at['211000', '111200'] == use.at['211000', '111200']
    # Value added moves between the electric industries and the residual.
    assert new_use.at['V00300', '221100'] == pytest.approx(53.0)
    assert new_use.at['V00300', CODE] == pytest.approx(10.0)
    assert new_use.loc['V00300'].sum() == pytest.approx(use.loc['V00300'].sum())
    # The residual buys nothing.
    assert new_use.loc[['221100', '211000', '531ORE', CODE], CODE].sum() == 0.0
    # Supply: electricity falls by each industry's amount; the pair is on the diagonal.
    assert new_supply.at['221100', '221100'] == pytest.approx(92.0)
    assert new_supply.at['221100', 'S00202'] == pytest.approx(59.0)
    assert new_supply.at[CODE, CODE] == pytest.approx(10.0)
    assert new_supply.at['211000', '111200'] == supply.at['211000', '111200']


def test_the_inputs_are_not_changed_in_place() -> None:
    use, supply = _tables()
    original = use.copy(), supply.copy()
    ear.split_residual(use, supply, DELTA)
    pd.testing.assert_frame_equal(use, original[0])
    pd.testing.assert_frame_equal(supply, original[1])


def test_row_and_column_order_is_kept() -> None:
    use, supply = _tables()
    new_use, new_supply = ear.split_residual(use, supply, DELTA)
    assert list(new_use.index) == [
        '221100',
        '211000',
        '531ORE',
        CODE,
        'V00100',
        'V00300',
    ]
    assert list(new_use.columns)[-2:] == [CODE, 'F01000']
    assert list(new_supply.columns)[-2:] == [CODE, 'MCIF']


def test_a_zero_residual_returns_the_same_values() -> None:
    use, supply = _tables()
    new_use, _ = ear.split_residual(use, supply, DELTA * 0.0)
    pd.testing.assert_frame_equal(new_use.loc[use.index, use.columns], use)


def test_a_negative_residual_raises() -> None:
    use, supply = _tables()
    with pytest.raises(ValueError, match='negative residual'):
        ear.split_residual(use, supply, pd.Series({'221100': -1.0}))


def test_a_residual_above_intermediate_electricity_raises() -> None:
    use, supply = _tables()
    with pytest.raises(ValueError, match='exceeds intermediate'):
        ear.split_residual(use, supply, pd.Series({'221100': 101.0}))


def test_amounts_are_old_output_less_retail_output_and_never_negative(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    panel = pd.DataFrame(
        {
            2017: [400.0, 50.0, 50.0],
            2022: [600.0, 70.0, 90.0],
        },
        index=['221100', 'S00101', 'S00202'],
    )
    monkeypatch.setattr(
        ear,
        'two_leg_gross_output',
        lambda _panel, _year: pd.Series(
            {'221100': 520.0, 'S00101': 999.0, 'S00202': 999.0}
        ),
    )
    monkeypatch.setattr(
        ear,
        'retail_gross_output',
        lambda _panel, _year: pd.Series(
            {'221100': 500.0, 'S00101': 75.0, 'S00202': 60.0}
        ),
    )
    delta = ear.residual_amounts(panel, 2022)
    assert delta['221100'] == pytest.approx(20.0)  # the two-leg output, as built
    assert delta['S00101'] == 0.0  # retail above BEA: no negative residual
    assert delta['S00202'] == pytest.approx(30.0)  # BEA's output, as built


def test_the_old_pathway_is_not_touched_by_importing_this() -> None:
    assert ug.CONTROLLED == ('221100',)
