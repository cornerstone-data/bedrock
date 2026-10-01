"""Tests for the GHGRP sector floor (#1060)."""

from __future__ import annotations

import pandas as pd
import pytest

import bedrock.transform.ghg.ghgrp_sector_floor as gf

COAL = 'UMD_GHGIA_T_3_11.coal'
NG_MFG = 'UMD_GHGIA_T_3_11.ng_manufacturing'
NG_NON = 'UMD_GHGIA_T_3_11.natural_gas_nonmanufacturing'
STILL_GAS = 'UMD_GHGIA_T_3_11.petroleum_still_gas'


def test_match_to_sectors_uses_the_longest_prefix() -> None:
    by_naics = pd.Series({'324110': 5.0, '327310': 3.0, '331111': 2.0, '999999': 1.0})
    sectors = pd.Index(['32411', '3241', '327310', '3311'])
    out = gf.match_to_sectors(by_naics, sectors)
    assert out['32411'] == 5.0  # not the shorter 3241
    assert out['327310'] == 3.0
    assert out['3311'] == 2.0
    assert '999999' not in out.index


def test_fuel_mix_falls_back_to_parent_then_pool() -> None:
    pool = pd.DataFrame(
        {'Coal': [3.0, 0.0, 0.0], 'Natural Gas': [1.0, 0.0, 6.0]},
        index=['327310', '327400', '311221'],
    )
    assert gf.fuel_mix(pool, '327310').to_dict() == {'Coal': 0.75, 'Natural Gas': 0.25}
    # 327400 has no combustion of its own: NAICS-4 parent 3273 is all 327310's mix
    assert gf.fuel_mix(pool, '327999')['Coal'] == pytest.approx(0.75)
    assert gf.fuel_mix(pool, '336111')['Natural Gas'] == pytest.approx(0.7)


def test_floor_moves_match_the_recipients_fuel_and_respect_donor_floors() -> None:
    have = pd.Series({'327310': 40.0, '311221': 100.0, '325110': 50.0})
    floor = pd.Series({'327310': 64.0, '311221': 20.0, '325110': 48.0})
    pool = pd.DataFrame(
        {
            'Coal': [6.0, 30.0, 0.0],
            'Natural Gas': [2.0, 50.0, 30.0],
            'Petroleum': [0.0, 0.0, 0.0],
        },
        index=['327310', '311221', '325110'],
    )
    add, give = gf.floor_moves(have, floor, pool)
    # cement short 24, in its own 75/25 coal/gas mix
    assert add.loc['327310', 'Coal'] == pytest.approx(18.0)
    assert add.loc['327310', 'Natural Gas'] == pytest.approx(6.0)
    # each fuel taken only from that fuel
    assert give['Coal'].sum() == pytest.approx(18.0)
    assert give['Natural Gas'].sum() == pytest.approx(6.0)
    # 325110 has 2 of slack: its gas capacity is 2, so it gives at most 2
    assert give.loc['325110'].sum() <= 2.0 + 1e-12


def test_floor_moves_spill_a_short_fuel_to_fuels_with_spare_capacity() -> None:
    # 'a' burns only coal, but no donor has coal to give: its need moves to gas.
    have = pd.Series({'a': 0.0, 'b': 100.0})
    floor = pd.Series({'a': 50.0, 'b': 0.0})
    pool = pd.DataFrame(
        {'Coal': [1.0, 0.0], 'Natural Gas': [0.0, 100.0]}, index=['a', 'b']
    )
    add, give = gf.floor_moves(have, floor, pool)
    assert add.loc['a', 'Coal'] == pytest.approx(0.0)
    assert add.loc['a', 'Natural Gas'] == pytest.approx(50.0)
    assert give.loc['b', 'Natural Gas'] == pytest.approx(50.0)


def test_floor_moves_raise_when_all_fuels_cannot_cover() -> None:
    have = pd.Series({'a': 0.0, 'b': 10.0})
    floor = pd.Series({'a': 50.0, 'b': 0.0})
    pool = pd.DataFrame(
        {'Coal': [1.0, 0.0], 'Natural Gas': [0.0, 10.0]}, index=['a', 'b']
    )
    with pytest.raises(ValueError, match='across all fuels'):
        gf.floor_moves(have, floor, pool)


def test_only_eligible_sectors_donate() -> None:
    have = pd.Series({'a': 0.0, 'good': 100.0, 'poor': 100.0})
    floor = pd.Series({'a': 30.0})
    pool = pd.DataFrame(
        {'Natural Gas': [1.0, 100.0, 100.0]}, index=['a', 'good', 'poor']
    )
    add, give = gf.floor_moves(have, floor, pool, eligible=pd.Index(['good']))
    assert give.index.tolist() == ['good']
    assert give.loc['good', 'Natural Gas'] == pytest.approx(30.0)


def _row(
    sector: str, meta: str, amount: float, flowable: str = gf.CO2
) -> dict[str, object]:
    return {
        'Flowable': flowable,
        'SectorProducedBy': sector,
        'MetaSources': meta,
        'AttributionSources': 'Hybrid_Facility_MECS',
        'FlowAmount': amount,
    }


def test_apply_floor_conserves_each_activity_set_and_spares_still_gas(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fbs = pd.DataFrame(
        [
            _row('327310', 'UMD_GHGIA_T_2_S1.direct', 40.0),  # process
            _row('327310', COAL, 3.0),
            _row('327310', NG_MFG, 1.0),
            _row('311221', COAL, 30.0),
            _row('311221', NG_MFG, 40.0),
            _row('325110', NG_NON, 30.0),
            _row('32411', STILL_GAS, 90.0),
            _row('325110', NG_MFG, 5.0, flowable='Methane'),
        ]
    )
    monkeypatch.setattr(
        gf,
        'ghgrp_co2e_by_naics',
        lambda year: pd.Series(
            {'327310': 64.0, '311221': 10.0, '325110': 5.0, '324110': 90.0}
        ),
    )
    monkeypatch.setattr(gf, 'well_covered_sectors', lambda codes: codes)
    out = gf.apply_ghgrp_sector_floor(fbs, 2022)
    co2 = out[out['Flowable'] == gf.CO2]
    by_sector = co2.groupby('SectorProducedBy')['FlowAmount'].sum()
    assert by_sector['327310'] == pytest.approx(64.0)
    before = fbs[fbs['Flowable'] == gf.CO2].groupby('MetaSources')['FlowAmount'].sum()
    after = co2.groupby('MetaSources')['FlowAmount'].sum()
    pd.testing.assert_series_equal(before, after)
    added = out[out['AttributionSources'] == gf.FLOOR_ATTRIBUTION]
    by_fuel = added.groupby('MetaSources')['FlowAmount'].sum()
    assert by_fuel.sum() == pytest.approx(20.0)
    assert by_fuel[COAL] == pytest.approx(15.0)  # cement's own 3:1 coal:gas mix
    # still gas is not a donor
    still = out[out['MetaSources'] == STILL_GAS]
    assert still['FlowAmount'].tolist() == [90.0]
    assert out.loc[out['Flowable'] == 'Methane', 'FlowAmount'].tolist() == [5.0]
