"""Tests for the per-mode transport margin allocation (Step 4c phase 2, #611).

Synthetic apart from the crosswalk, which is checked as real data: it is the
whole judgement layer of the pipeline allocation, so a typo in it would
otherwise only surface as a silently misplaced margin.
"""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.transform.iot import nowcast_transport_margins as tm
from bedrock.transform.iot.nowcast_transport_margins import (
    PIPELINE_ITEM_CODES,
    load_pipeline_crosswalk,
    mode_residual,
    pipeline_allocation,
    pipeline_bound_check,
    pipeline_margin_2017,
)
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES

MARGIN_COLUMNS = [
    "Producers' Value",
    'Transportation',
    'Wholesale',
    'Retail',
    "Purchasers' Value",
]


def margins_frame(rows: list[tuple[str, str, float, float, float, float, float]]):
    """(buyer, commodity, PV, transport, wholesale, retail, purchasers)."""
    frame = pd.DataFrame(
        rows,
        columns=['Industry Code', 'Commodity Code', *MARGIN_COLUMNS],
    )
    return frame.set_index(['Industry Code', 'Commodity Code'])


# --------------------------------------------------------------------------
# the crosswalk, as real data
# --------------------------------------------------------------------------


def test_crosswalk_covers_every_margin_item() -> None:
    """A missing item would drop its whole commodity set from the allocation."""
    crosswalk = load_pipeline_crosswalk()
    assert set(crosswalk['sas_naics']) == set(PIPELINE_ITEM_CODES)


def test_crosswalk_commodities_are_real_bea_2017_codes() -> None:
    crosswalk = load_pipeline_crosswalk()
    unknown = set(crosswalk['bea_2017_commodity']) - set(USA_2017_COMMODITY_CODES)
    assert not unknown, f'not BEA 2017 detail commodities: {sorted(unknown)}'


def test_crude_and_gas_share_one_bea_commodity() -> None:
    """
    BEA 2017 detail has no separate natural-gas commodity, so the two clearest
    margin items land together and their split never affects the allocation.
    """
    crosswalk = load_pipeline_crosswalk()
    crude = set(crosswalk.loc[crosswalk['sas_naics'] == '4861', 'bea_2017_commodity'])
    gas = set(crosswalk.loc[crosswalk['sas_naics'] == '4862', 'bea_2017_commodity'])
    assert crude == gas == {'211000'}


def test_every_crosswalk_row_records_its_basis() -> None:
    """The judgement calls are the point of the file; an unexplained row is a bug."""
    crosswalk = load_pipeline_crosswalk()
    assert crosswalk['basis'].str.len().gt(20).all()


# --------------------------------------------------------------------------
# the allocation
# --------------------------------------------------------------------------


@pytest.fixture
def synthetic(monkeypatch):
    """Two items over three commodities, with a 60/40 published transport split."""
    crosswalk = pd.DataFrame(
        {
            'sas_naics': ['4861', '4862', '48691', '48691'],
            'bea_2017_commodity': ['211000', '211000', '324110', '324190'],
            'basis': ['x' * 30] * 4,
        }
    )
    revenue = pd.Series(
        {'4861': 300.0, '4862': 300.0, '48691': 400.0},
        name='FlowAmount',
    )
    monkeypatch.setattr(tm, 'load_pipeline_crosswalk', lambda: crosswalk)
    monkeypatch.setattr(tm, 'load_pipeline_item_revenue', lambda year: revenue)
    return margins_frame(
        [
            # buyer, commodity, PV, transport, wholesale, retail, purchasers
            ('1111A0', '211000', 100.0, 1000.0, 0.0, 0.0, 1100.0),
            ('1111A0', '324110', 100.0, 600.0, 0.0, 0.0, 700.0),
            ('1111A0', '324190', 100.0, 400.0, 0.0, 0.0, 500.0),
        ]
    )


def test_allocation_is_an_identity_on_the_control_total(synthetic) -> None:
    allocation = pipeline_allocation(2017, control_total=1000.0, margins=synthetic)
    assert allocation.sum() == pytest.approx(1000.0)


def test_items_carry_their_revenue_share(synthetic) -> None:
    """4861+4862 are 60% of revenue and share one commodity; 48691 is 40%."""
    allocation = pipeline_allocation(2017, control_total=1000.0, margins=synthetic)
    assert allocation['211000'] == pytest.approx(600.0)
    assert allocation[['324110', '324190']].sum() == pytest.approx(400.0)


def test_within_a_set_the_split_is_proportional_to_published_transport(
    synthetic,
) -> None:
    """324110 carries 600 of the set's 1000 published transport, so 60% of 400."""
    allocation = pipeline_allocation(2017, control_total=1000.0, margins=synthetic)
    assert allocation['324110'] == pytest.approx(240.0)
    assert allocation['324190'] == pytest.approx(160.0)


def test_a_set_with_no_published_transport_raises(monkeypatch) -> None:
    """Splitting on a zero weight would divide by zero and silently emit NaN."""
    monkeypatch.setattr(
        tm,
        'load_pipeline_crosswalk',
        lambda: pd.DataFrame(
            {
                'sas_naics': ['4861'],
                'bea_2017_commodity': ['211000'],
                'basis': ['x' * 30],
            }
        ),
    )
    monkeypatch.setattr(
        tm, 'load_pipeline_item_revenue', lambda year: pd.Series({'4861': 1.0})
    )
    margins = margins_frame([('1111A0', '211000', 100.0, 0.0, 0.0, 0.0, 100.0)])
    with pytest.raises(ValueError, match='no basis on which to split'):
        pipeline_allocation(2017, control_total=100.0, margins=margins)


# --------------------------------------------------------------------------
# the bound test
# --------------------------------------------------------------------------


def test_bound_check_reports_headroom_for_the_other_modes(synthetic) -> None:
    check = pipeline_bound_check(2017, control_total=1000.0, margins=synthetic)
    assert (check['share'] <= 1.0).all()
    # 211000 takes 600 of its 1000 published, leaving 400 for the other modes
    assert check.loc['211000', 'headroom_other_modes'] == pytest.approx(400.0)
    assert check.loc['324110', 'headroom_other_modes'] == pytest.approx(360.0)


def test_bound_check_catches_an_over_allocation(synthetic) -> None:
    """
    A share above 1 needs the other four modes to contribute negative margin,
    which is the one way this construction can be visibly wrong.
    """
    check = pipeline_bound_check(2017, control_total=10_000.0, margins=synthetic)
    assert (check['share'] > 1.0).any()


# --------------------------------------------------------------------------
# the level being allocated
# --------------------------------------------------------------------------


def test_pipeline_give_up_is_read_off_its_own_rows() -> None:
    """
    Purchasers' Value on a transport commodity is not the sum of its components -
    the margin has been moved onto the goods - so the give-up is the difference.
    """
    margins = margins_frame(
        [
            ('F01000', '486000', 700.0, 0.0, 0.0, 0.0, 200.0),
            ('1111A0', '486000', 300.0, 0.0, 0.0, 0.0, 100.0),
            ('1111A0', '211000', 100.0, 50.0, 0.0, 0.0, 150.0),
        ]
    )
    assert pipeline_margin_2017(margins) == pytest.approx(700.0)


# --------------------------------------------------------------------------
# the five modes are one decomposition, not five independent answers
# --------------------------------------------------------------------------


def test_within_set_weight_can_be_supplied(synthetic) -> None:
    """
    The default weight assumes pipeline's within-set mix matches the all-mode
    mix. A caller with a mode-specific measure - FAF pipeline ton-miles - must
    be able to use it instead.
    """
    flat = pd.Series({'211000': 1.0, '324110': 1.0, '324190': 1.0})
    allocation = pipeline_allocation(
        2017, control_total=1000.0, margins=synthetic, within_set_weight=flat
    )
    # an even weight splits 48691's 400 in half rather than 240/160
    assert allocation['324110'] == pytest.approx(200.0)
    assert allocation['324190'] == pytest.approx(200.0)


def test_mode_residual_is_what_the_unbuilt_modes_must_supply(synthetic) -> None:
    allocation = pipeline_allocation(2017, control_total=1000.0, margins=synthetic)
    residual = mode_residual({'pipeline': allocation}, margins=synthetic)
    # 211000 publishes 1000 all-mode and pipeline takes 600
    assert residual.loc['211000', 'residual'] == pytest.approx(400.0)
    assert not residual['over_allocated'].any()


def test_mode_residual_flags_an_impossible_decomposition(synthetic) -> None:
    """
    Over-allocating one mode forces negative margin onto the others, which the
    published column never carries at commodity level.
    """
    allocation = pipeline_allocation(2017, control_total=10_000.0, margins=synthetic)
    residual = mode_residual({'pipeline': allocation}, margins=synthetic)
    assert residual['over_allocated'].any()
