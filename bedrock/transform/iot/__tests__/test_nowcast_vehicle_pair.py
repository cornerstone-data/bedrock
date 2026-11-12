"""The car row balances at the seed, and the pair's totals don't move (#1270)."""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.transform.iot.nowcast_vehicle_pair import split_vehicle_pair_imports

ROWS = ['336111', '336112']


def _seeds(
    car_use: float, truck_use: float, car_output: float = 25.0
) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Supply: output in one industry column, then imports and duties (2%).
    supply = pd.DataFrame(
        {'336111': [car_output, 0.0], '336112': [0.0, 330.0], 'MCIF': [68.0, 132.0]},
        index=ROWS,
    )
    supply['MDTY'] = 0.02 * supply['MCIF']
    use = pd.DataFrame(
        {'F01000': [car_use, truck_use], 'F04000': [26.0, 34.0]}, index=ROWS
    )
    return use, supply


def test_the_car_row_balances_and_the_pair_totals_hold() -> None:
    use, supply = _seeds(car_use=40.0, truck_use=420.0)
    out = split_vehicle_pair_imports(use, supply)

    assert out.loc['336111'].sum() == pytest.approx(use.loc['336111'].sum())
    assert out['MCIF'].sum() == pytest.approx(supply['MCIF'].sum())
    assert out['MDTY'].sum() == pytest.approx(supply['MDTY'].sum())
    assert out.loc['336111', 'MCIF'] < supply.loc['336111', 'MCIF']


def test_car_imports_stay_inside_the_pair_total() -> None:
    # Output alone exceeds the car row's use, so no imports can close it.
    use, supply = _seeds(car_use=0.0, truck_use=420.0, car_output=30.0)
    out = split_vehicle_pair_imports(use, supply)

    assert out.loc['336111', 'MCIF'] == 0.0
    assert out.loc['336112', 'MCIF'] == pytest.approx(supply['MCIF'].sum())


def test_other_rows_and_columns_are_untouched() -> None:
    use, supply = _seeds(car_use=40.0, truck_use=420.0)
    out = split_vehicle_pair_imports(use, supply)

    pd.testing.assert_frame_equal(
        out[['336111', '336112']], supply[['336111', '336112']]
    )
