"""Hermetic tests for #755 RAS movement aggregates (no GRAS)."""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.analysis.nowcasting.ras_improvements.ras_movement import (
    absorption_share,
    build_commodity_ras_yoy,
    commodity_ras_delta,
    partition_labels,
    protocol_soft_vs_hard,
    top_movers,
)
from bedrock.transform.iot.nowcast_mask import VA_ROWS, balance_commodities
from bedrock.utils.taxonomy.bea.v2017_final_demand import SUT_FINAL_DEMAND_CODES


def test_partition_labels_match_mask_apis() -> None:
    commodities, industries, fd_codes = partition_labels()
    assert len(commodities) == 400
    assert len(industries) == 402
    assert fd_codes == tuple(SUT_FINAL_DEMAND_CODES)
    assert set(VA_ROWS).isdisjoint(commodities)
    assert set(commodities) == set(balance_commodities())


def test_absorption_share_both_zero_is_zero() -> None:
    assert absorption_share(0.0, 0.0) == 0.0
    series = absorption_share(
        pd.Series([0.0, 3.0, -2.0]),
        pd.Series([0.0, 1.0, 2.0]),
    )
    assert isinstance(series, pd.Series)
    assert series.iloc[0] == 0.0
    assert series.iloc[1] == pytest.approx(0.75)
    assert series.iloc[2] == pytest.approx(0.5)


def test_commodity_ras_delta_levels_and_aggregates() -> None:
    commodities = ['c1', 'c2']
    industries = ['i1', 'i2']
    fd = ['F01000', 'F04000']
    seed = pd.DataFrame(
        [[10.0, 0.0, 1.0, 0.0], [0.0, 5.0, 0.0, 2.0]],
        index=commodities,
        columns=industries + fd,
    )
    balanced = pd.DataFrame(
        [[12.0, 1.0, 0.5, 0.5], [1.0, 4.0, 1.0, 1.0]],
        index=commodities,
        columns=industries + fd,
    )
    # VA row present but must be ignored when commodities list excludes it.
    seed.loc['V00100'] = 0.0
    balanced.loc['V00100'] = 99.0

    frame = commodity_ras_delta(
        seed,
        balanced,
        year=2020,
        protocol='soft',
        commodities=commodities,
        industries=industries,
        fd_codes=fd,
    )
    assert list(frame.columns) == list(
        [
            'year',
            'protocol',
            'commodity',
            'seed_inter',
            'seed_fd',
            'bal_inter',
            'bal_fd',
            'delta_inter',
            'delta_fd',
            'absorption_share',
            'l1_inter',
            'l1_fd',
            'hygiene_ok',
            'hygiene_error',
        ]
    )
    assert set(frame['commodity']) == {'c1', 'c2'}
    c1 = frame.set_index('commodity').loc['c1']
    assert c1['seed_inter'] == pytest.approx(10.0)
    assert c1['seed_fd'] == pytest.approx(1.0)
    assert c1['bal_inter'] == pytest.approx(13.0)
    assert c1['bal_fd'] == pytest.approx(1.0)
    assert c1['delta_inter'] == pytest.approx(3.0)
    assert c1['delta_fd'] == pytest.approx(0.0)
    assert c1['l1_inter'] == pytest.approx(3.0)  # |2|+|1|
    assert c1['absorption_share'] == pytest.approx(1.0)


def test_top_movers_by_abs_delta_inter() -> None:
    frame = pd.DataFrame(
        {
            'commodity': ['a', 'b', 'c'],
            'delta_inter': [1.0, -5.0, 2.0],
            'delta_fd': [0.0, 0.0, 0.0],
        }
    )
    top = top_movers(frame, n=2)
    assert list(top['commodity']) == ['b', 'c']


def test_yoy_identity_and_omits_first_year() -> None:
    rows = []
    for year, seed_i, delta_i in (
        (2017, 100.0, 1.0),
        (2018, 110.0, 3.0),
        (2019, 105.0, 2.0),
    ):
        rows.append(
            {
                'year': year,
                'protocol': 'soft',
                'commodity': 'c1',
                'seed_inter': seed_i,
                'seed_fd': 0.0,
                'bal_inter': seed_i + delta_i,
                'bal_fd': 0.0,
                'delta_inter': delta_i,
                'delta_fd': 0.0,
                'absorption_share': 1.0,
                'l1_inter': abs(delta_i),
                'l1_fd': 0.0,
                'hygiene_ok': True,
                'hygiene_error': '',
            }
        )
    deltas = pd.DataFrame(rows)
    yoy = build_commodity_ras_yoy(deltas)
    assert set(yoy['year']) == {2018, 2019}
    r2018 = yoy.set_index('year').loc[2018]
    # bal_yoy == seed_yoy + ras_delta_yoy
    assert r2018['balanced_yoy_inter'] == pytest.approx(
        r2018['seed_yoy_inter'] + r2018['ras_delta_yoy_inter']
    )
    assert r2018['ras_delta_inter'] == pytest.approx(3.0)  # year-t passthrough
    assert r2018['seed_yoy_inter'] == pytest.approx(10.0)
    assert r2018['ras_delta_yoy_inter'] == pytest.approx(2.0)
    assert r2018['balanced_yoy_inter'] == pytest.approx(12.0)


def test_protocol_soft_vs_hard_on_delta() -> None:
    soft = pd.DataFrame(
        {
            'year': [2021, 2021],
            'commodity': ['c1', 'c2'],
            'delta_inter': [5.0, 1.0],
            'delta_fd': [0.0, 2.0],
            'hygiene_ok': [True, True],
            'hygiene_error': ['', ''],
        }
    )
    hard = pd.DataFrame(
        {
            'year': [2021, 2021],
            'commodity': ['c1', 'c2'],
            'delta_inter': [4.0, 1.0],
            'delta_fd': [1.0, 0.0],
            'hygiene_ok': [True, False],
            'hygiene_error': ['', 'hard fail'],
        }
    )
    cmp = protocol_soft_vs_hard(soft, hard)
    by_c = cmp.set_index('commodity')
    assert by_c.loc['c1', 'soft_minus_hard_inter'] == pytest.approx(1.0)
    assert by_c.loc['c1', 'soft_minus_hard_fd'] == pytest.approx(-1.0)
    assert bool(by_c.loc['c2', 'hygiene_ok_hard']) is False
