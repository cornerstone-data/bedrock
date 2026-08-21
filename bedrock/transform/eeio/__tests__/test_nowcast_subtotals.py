"""Tests for the Supply-bridge and Use value-added subtotals in ``nowcast``.

Synthetic: both are arithmetic over a panel, so the arithmetic is checked
against panels small enough to read. The identities themselves were measured
against the published 2017 Detail Supply and Use_SUT tables, where every
commodity / industry reproduces to the workbook's own 1 million USD rounding.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from bedrock.transform.eeio.nowcast import (
    _SUPPLY_BRIDGE_CODES,
    USE_VALUE_ADDED_ROWS,
    fill_supply_bridge_subtotals,
    use_value_added_subtotals,
)


def _bridge(**columns: float) -> pd.DataFrame:
    frame = pd.DataFrame(
        np.nan,
        index=pd.Index(['111120', '111130'], name='commodity'),
        columns=list(_SUPPLY_BRIDGE_CODES),
        dtype=float,
    )
    for code, value in columns.items():
        frame[code] = value
    return frame


def test_supply_bridge_subtotals_add_up() -> None:
    filled = fill_supply_bridge_subtotals(
        _bridge(
            T007=100.0,
            MCIF=10.0,
            MADJ=1.0,
            TRADE=5.0,
            TRANS=3.0,
            MDTY=2.0,
            TOP=4.0,
            SUB=-1.0,
        )
    )
    assert (filled['T013'] == 111.0).all()
    assert (filled['T014'] == 8.0).all()
    # SUB is stored negative, so T015 adds it.
    assert (filled['T015'] == 5.0).all()
    assert (filled['T016'] == 124.0).all()


def test_supply_bridge_subtotal_stays_nan_when_a_component_is_unsourced() -> None:
    """An unsourced component must not be silently read as zero."""
    filled = fill_supply_bridge_subtotals(_bridge(T007=100.0, MCIF=10.0, MADJ=1.0))
    assert (filled['T013'] == 111.0).all()
    assert filled['T014'].isna().all()  # TRADE / TRANS not sourced yet
    assert filled['T016'].isna().all()


def test_fill_supply_bridge_subtotals_does_not_mutate_its_argument() -> None:
    bridge = _bridge(T007=100.0, MCIF=10.0, MADJ=1.0)
    fill_supply_bridge_subtotals(bridge)
    assert bridge['T013'].isna().all()


def _use_panel() -> pd.DataFrame:
    panel = pd.DataFrame(
        0.0,
        index=['111120', '111130', *USE_VALUE_ADDED_ROWS],
        columns=['1111A0', '111200', 'F01000'],
    )
    panel.loc['111120'] = [7.0, 2.0, 50.0]
    panel.loc['111130'] = [3.0, 1.0, 20.0]
    panel.loc['V00100', ['1111A0', '111200']] = [20.0, 5.0]
    panel.loc['T00OTOP', ['1111A0', '111200']] = [2.0, 1.0]
    panel.loc['V00300', ['1111A0', '111200']] = [8.0, 4.0]
    panel.loc['T00TOP', ['1111A0', '111200']] = [1.0, 0.0]
    # Balance sign convention: subsidies negative, as the Supply table's SUB.
    panel.loc['T00SUB', ['1111A0', '111200']] = [-3.0, 0.0]
    return panel


def test_use_value_added_subtotals_add_up() -> None:
    rows = use_value_added_subtotals(_use_panel(), ['1111A0', '111200']).T
    assert list(rows.columns) == ['T005', 'VABAS', 'T018', 'VAPRO']
    assert rows['T005'].tolist() == [10.0, 3.0]
    assert rows['VABAS'].tolist() == [30.0, 10.0]
    assert rows['T018'].tolist() == [40.0, 13.0]
    # VAPRO = VABAS + T00TOP + T00SUB, subsidies already negative.
    assert rows['VAPRO'].tolist() == [28.0, 10.0]


def test_use_value_added_subtotals_exclude_final_demand_columns() -> None:
    """BEA leaves the value-added subtotals blank in the final-demand columns,
    including ``T018`` - even though ``T005`` itself is not blank there."""
    rows = use_value_added_subtotals(_use_panel(), ['1111A0', '111200'])
    assert 'F01000' not in rows.columns


def test_use_value_added_subtotals_requires_the_value_added_rows() -> None:
    panel = _use_panel().drop(index=['V00300'])
    with pytest.raises(AssertionError, match='V00300'):
        use_value_added_subtotals(panel, ['1111A0'])
