"""Tests for the GHGRP sector floor (#1060)."""

from __future__ import annotations

import pandas as pd
import pytest

import bedrock.transform.ghg.ghgrp_sector_floor as gf


def test_match_to_sectors_uses_the_longest_prefix() -> None:
    by_naics = pd.Series({'324110': 5.0, '327310': 3.0, '331111': 2.0, '999999': 1.0})
    sectors = pd.Index(['32411', '3241', '327310', '3311'])
    out = gf.match_to_sectors(by_naics, sectors)
    assert out['32411'] == 5.0  # not the shorter 3241
    assert out['327310'] == 3.0
    assert out['3311'] == 2.0
    assert '999999' not in out.index


def test_floor_moves_fill_deficits_without_pushing_donors_below_their_floor() -> None:
    have = pd.Series({'cement': 40.0, 'boilers': 100.0, 'tight': 50.0})
    floor = pd.Series({'cement': 65.0, 'boilers': 20.0, 'tight': 48.0})
    pool = pd.Series({'cement': 0.0, 'boilers': 80.0, 'tight': 30.0})
    deficit, give = gf.floor_moves(have, floor, pool)
    assert deficit.to_dict() == {'cement': 25.0}
    # capacity = min(pool, slack): boilers min(80, 80) = 80, tight min(30, 2) = 2
    assert give.sum() == pytest.approx(25.0)
    assert give['boilers'] == pytest.approx(25.0 * 80 / 82)
    assert give['tight'] == pytest.approx(25.0 * 2 / 82)
    assert have['tight'] - give['tight'] >= floor['tight']


def test_floor_moves_raise_when_the_pool_cannot_cover() -> None:
    have = pd.Series({'a': 0.0, 'b': 10.0})
    floor = pd.Series({'a': 50.0, 'b': 0.0})
    pool = pd.Series({'b': 10.0})
    with pytest.raises(ValueError, match='exceed donor capacity'):
        gf.floor_moves(have, floor, pool)


def _row(sector: str, meta: str, amount: float, flowable: str = gf.CO2) -> dict:
    return {
        'Flowable': flowable,
        'SectorProducedBy': sector,
        'MetaSources': meta,
        'AttributionSources': 'Hybrid_Facility_MECS',
        'FlowAmount': amount,
    }


def test_apply_floor_conserves_each_activity_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ng, coal = f'{gf.POOL_PREFIX}.ng', f'{gf.POOL_PREFIX}.coal'
    fbs = pd.DataFrame(
        [
            _row('327310', 'UMD_GHGIA_T_2_S1.direct', 40.0),  # process only
            _row('311221', ng, 60.0),
            _row('311221', coal, 20.0),
            _row('325110', ng, 20.0),
            _row('325110', 'UMD_GHGIA_T_3_11.ng', 5.0, flowable='Methane'),
        ]
    )
    monkeypatch.setattr(
        gf,
        'ghgrp_co2_by_naics',
        lambda year: pd.Series({'327310': 64.0, '311221': 10.0, '325110': 5.0}),
    )
    out = gf.apply_ghgrp_sector_floor(fbs, 2022)
    co2 = out[out['Flowable'] == gf.CO2]
    by_sector = co2.groupby('SectorProducedBy')['FlowAmount'].sum()
    assert by_sector['327310'] == pytest.approx(64.0)
    by_meta_before = (
        fbs[fbs['Flowable'] == gf.CO2].groupby('MetaSources')['FlowAmount'].sum()
    )
    by_meta_after = co2.groupby('MetaSources')['FlowAmount'].sum()
    pd.testing.assert_series_equal(by_meta_before, by_meta_after)
    assert (
        by_sector
        >= pd.Series({'311221': 10.0, '325110': 5.0}).reindex(by_sector.index).fillna(0)
    ).all()
    added = out[out['AttributionSources'] == gf.FLOOR_ATTRIBUTION]
    assert added['FlowAmount'].sum() == pytest.approx(24.0)
    # non-CO2 rows are untouched
    assert out.loc[out['Flowable'] == 'Methane', 'FlowAmount'].tolist() == [5.0]
