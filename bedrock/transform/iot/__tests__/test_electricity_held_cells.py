"""Held electricity cells (#1035): data processing's cell on LBNL, and that the
interior fit holds a held cell while still meeting both margins.

Synthetic throughout; no extract.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from bedrock.transform.iot import eia_utility_go_adjustment as eia
from bedrock.transform.iot import nowcast_interior_fit as fi
from bedrock.transform.iot import nowcast_intermediate as ni
from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES

ELEC = ni.ELECTRICITY_COMMODITY


def _flag(monkeypatch: pytest.MonkeyPatch, data_centers: bool) -> None:
    config = get_usa_config().model_copy(
        update={'move_data_processing_electricity_on_lbnl': data_centers}
    )
    monkeypatch.setattr(ni, 'get_usa_config', lambda: config)


def test_the_fit_holds_pinned_cells_and_meets_both_margins(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rng = np.random.default_rng(5)
    shape = (len(USA_2017_COMMODITY_CODES), len(USA_2017_INDUSTRY_CODES))
    seed = pd.DataFrame(
        np.where(rng.uniform(size=shape) < 0.4, rng.uniform(1, 9, size=shape), 0.0)
        * 1e7,
        index=pd.Index(USA_2017_COMMODITY_CODES, name='commodity'),
        columns=pd.Index(USA_2017_INDUSTRY_CODES, name='industry'),
    )
    pinned = ['441000', '423A00']
    seed.loc[[ELEC], pinned] = 5e7
    row_t = seed.sum(axis=1) * 1.2
    col_t = seed.sum(axis=0) * 1.2
    monkeypatch.setattr(fi, 'interior_row_targets', lambda year: row_t)
    monkeypatch.setattr(fi, 'interior_column_targets', lambda year: col_t)
    monkeypatch.setattr(ni, 'pinned_electricity_buyers', lambda year: pinned)

    result = fi.fit_interior(2023, seed=seed)

    assert result.interior.loc[[ELEC], pinned].to_numpy() == pytest.approx(
        seed.loc[[ELEC], pinned].to_numpy(), rel=1e-12
    )
    active_cols = result.column_targets.index.difference(result.held_columns.index)
    col_miss = (result.interior.sum(axis=0) - result.column_targets)[active_cols]
    assert float(col_miss.abs().max()) < fi.TOLERANCE_USD
    # Unpinned cells in a pinned column absorb the whole column move.
    free = [c for c in USA_2017_COMMODITY_CODES if c != ELEC]
    grew = result.interior.loc[free, '441000'].sum() / seed.loc[free, '441000'].sum()
    assert float(grew) > 1.2


def test_the_data_center_flag_is_on_by_default() -> None:
    assert get_usa_config().move_data_processing_electricity_on_lbnl is True


def test_no_cell_is_held_with_the_flag_off(monkeypatch: pytest.MonkeyPatch) -> None:
    _flag(monkeypatch, False)
    assert ni.pinned_electricity_buyers(2023) == []


def test_the_data_center_cell_is_pinned_even_though_observed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _flag(monkeypatch, True)
    assert ni.pinned_electricity_buyers(2023) == [ni.DATA_CENTER_BUYER]


def test_the_lbnl_series_starts_at_one_and_rises_every_year() -> None:
    index = [ni.data_center_electricity_index(y) for y in range(2017, 2025)]
    assert index[0] == 1.0
    assert all(b > a for a, b in zip(index, index[1:]))


def test_the_data_center_cell_moves_on_kwh_times_price(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bench = pd.DataFrame(0.0, index=[ELEC], columns=[ni.DATA_CENTER_BUYER])
    bench.loc[ELEC, ni.DATA_CENTER_BUYER] = 100.0
    monkeypatch.setattr(ni, 'benchmark_intermediate', lambda: bench)
    monkeypatch.setattr(
        eia, 'commercial_price_index', lambda year: {2017: 1.0, 2024: 1.2}[year]
    )
    expected = 100.0 * ni.data_center_electricity_index(2024) * 1.2
    assert ni.data_center_electricity_cell(2024) == pytest.approx(expected)


def test_pin_writes_the_data_center_cell_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _flag(monkeypatch, True)
    monkeypatch.setattr(ni, 'data_center_electricity_cell', lambda year: 7.0)
    block = pd.DataFrame(
        1.0, index=[ELEC, 'other'], columns=[ni.DATA_CENTER_BUYER, '441000']
    )
    out = ni.pin_electricity_cells(block, 2024)
    assert out.at[ELEC, ni.DATA_CENTER_BUYER] == 7.0
    assert out.at[ELEC, '441000'] == 1.0
    assert out.at['other', ni.DATA_CENTER_BUYER] == 1.0


# --- electric power's gas purchases ------------------------------------------


def test_gas_hold_is_off_by_default() -> None:
    assert ni.pinned_gas_buyers() == []


def test_government_columns_take_their_share_up_to_their_room() -> None:
    base = pd.Series({'221100': 7.0, 'S00101': 2.0, 'S00202': 20.0})
    room = pd.Series({'221100': 500.0, 'S00101': 100.0, 'S00202': 30.0})
    cells = ni.allocate_electric_gas(58.0, base, room)
    assert cells['S00101'] == pytest.approx(4.0)
    assert cells['S00202'] == pytest.approx(30.0)  # 40 wanted, room for 30
    assert cells['221100'] == pytest.approx(24.0)
    assert float(cells.sum()) == pytest.approx(58.0)


def test_negative_room_gives_a_government_column_no_gas() -> None:
    base = pd.Series({'221100': 7.0, 'S00101': 2.0, 'S00202': 20.0})
    room = pd.Series({'221100': 500.0, 'S00101': -5.0, 'S00202': 30.0})
    cells = ni.allocate_electric_gas(29.0, base, room)
    assert cells['S00101'] == 0.0
    assert float(cells.sum()) == pytest.approx(29.0)


def test_gas_that_does_not_fit_raises() -> None:
    base = pd.Series({'221100': 7.0, 'S00101': 2.0, 'S00202': 20.0})
    room = pd.Series({'221100': 1.0, 'S00101': 1.0, 'S00202': 1.0})
    with pytest.raises(ValueError, match='does not fit'):
        ni.allocate_electric_gas(58.0, base, room)


def test_only_the_gas_cells_of_the_electric_columns_are_written(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _flag(monkeypatch, False)
    buyers = list(ni.ELECTRIC_GAS_BUYERS)
    monkeypatch.setattr(ni, 'pinned_gas_buyers', lambda: buyers)
    monkeypatch.setattr(
        ni,
        'electric_gas_cells',
        lambda year, control: pd.Series({'221100': 7.0, 'S00101': 8.0, 'S00202': 9.0}),
    )
    block = pd.DataFrame(
        1.0, index=[ni.GAS_COMMODITY, 'other'], columns=[*buyers, '441000']
    )
    out = ni.pin_electricity_cells(block, 2022)
    assert out.at[ni.GAS_COMMODITY, 'S00202'] == 9.0
    assert out.at[ni.GAS_COMMODITY, '441000'] == 1.0
    assert out.at['other', 'S00202'] == 1.0
