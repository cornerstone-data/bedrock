"""Use-attributed waste emissions follow the waste disaggregation (#1053)."""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.transform.allocation import derived
from bedrock.transform.allocation.derived import (
    resplit_waste_use_emissions,
    waste_child_shares,
)
from bedrock.transform.eeio import cornerstone_disagg_pipeline as cdp
from bedrock.utils.taxonomy.cornerstone.industries import WASTE_DISAGG_INDUSTRIES

CHILDREN = [str(code) for code in WASTE_DISAGG_INDUSTRIES['562000']]


def _use() -> pd.DataFrame:
    """Gas bought mostly by the first child; other inputs spread evenly."""
    U = pd.DataFrame(1.0, index=['221200', '324110', '541100'], columns=CHILDREN)
    U.loc['221200'] = [0.0] * len(CHILDREN)
    U.loc['221200', CHILDREN[0]] = 9.0
    U.loc['221200', CHILDREN[1]] = 1.0
    return U


def _fbs() -> pd.DataFrame:
    rows = []
    for child in CHILDREN:
        rows.append(
            {
                'Flowable': 'Carbon dioxide',
                'MetaSources': 'UMD_GHGIA_T_3_4.non_manufacturing_natural_gas',
                'AttributionSources': derived.USE_ATTRIBUTION_SOURCE,
                'SectorProducedBy': child,
                'FlowAmount': 1.0,
            }
        )
    rows.append(
        {
            'Flowable': 'Methane',
            'MetaSources': 'UMD_GHGIA_T_2_S1.direct',
            'AttributionSources': 'Direct',
            'SectorProducedBy': '562212',
            'FlowAmount': 50.0,
        }
    )
    return pd.DataFrame(rows)


@pytest.fixture
def waste_on(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cdp, 'get_waste_disagg_weights', lambda: object())
    monkeypatch.setattr(
        cdp,
        'derive_cornerstone_U_after_waste',
        lambda: (_use(), pd.DataFrame(0.0, index=_use().index, columns=CHILDREN)),
    )


def test_fuel_sets_key_on_their_own_row() -> None:
    shares = waste_child_shares(_use(), CHILDREN, 'x.non_manufacturing_natural_gas')
    assert shares[CHILDREN[0]] == pytest.approx(0.9)
    assert shares[CHILDREN[1]] == pytest.approx(0.1)


def test_other_sets_key_on_the_whole_column() -> None:
    shares = waste_child_shares(_use(), CHILDREN, 'UMD_GHGIA_T_4_60.refrigerants')
    total = _use().sum(axis=0)
    assert shares.to_dict() == pytest.approx((total / total.sum()).to_dict())


@pytest.mark.usefixtures('waste_on')
def test_pools_keep_their_totals_and_direct_emissions_stay_put() -> None:
    before = _fbs()
    after = resplit_waste_use_emissions(before)
    assert after['FlowAmount'].sum() == pytest.approx(before['FlowAmount'].sum())
    use = after[after['AttributionSources'] == derived.USE_ATTRIBUTION_SOURCE]
    by_child = use.groupby('SectorProducedBy')['FlowAmount'].sum()
    assert by_child[CHILDREN[0]] == pytest.approx(0.9 * len(CHILDREN))
    direct = after[after['AttributionSources'] == 'Direct']
    assert direct['FlowAmount'].tolist() == [50.0]
    assert direct['SectorProducedBy'].tolist() == ['562212']


def test_nothing_changes_without_waste_disaggregation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cdp, 'get_waste_disagg_weights', lambda: None)
    before = _fbs()
    pd.testing.assert_frame_equal(resplit_waste_use_emissions(before), before)
