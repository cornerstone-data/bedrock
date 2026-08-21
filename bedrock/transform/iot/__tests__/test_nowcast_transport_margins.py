"""Tests for the per-mode transport margin allocation (Step 4c phase 2, #611).

Synthetic apart from the crosswalk, which is checked as real data: it is the
whole judgement layer of the pipeline allocation, so a typo in it would
otherwise only surface as a silently misplaced margin.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from bedrock.transform.eeio.nowcast import (
    TRANSPORT_MARGIN_YEARS,
    derive_initial_supply_bridge,
)
from bedrock.transform.iot import nowcast_transport_margins as tm
from bedrock.transform.iot.nowcast_transport_margins import (
    MODE_COMMODITIES,
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


def margins_frame(
    rows: list[tuple[str, str, float, float, float, float, float]],
) -> pd.DataFrame:
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
def synthetic(monkeypatch: pytest.MonkeyPatch) -> pd.DataFrame:
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


def test_allocation_is_an_identity_on_the_control_total(
    synthetic: pd.DataFrame,
) -> None:
    allocation = pipeline_allocation(2017, control_total=1000.0, margins=synthetic)
    assert allocation.sum() == pytest.approx(1000.0)


def test_items_carry_their_revenue_share(synthetic: pd.DataFrame) -> None:
    """4861+4862 are 60% of revenue and share one commodity; 48691 is 40%."""
    allocation = pipeline_allocation(2017, control_total=1000.0, margins=synthetic)
    assert allocation['211000'] == pytest.approx(600.0)
    assert allocation[['324110', '324190']].sum() == pytest.approx(400.0)


def test_within_a_set_the_split_is_proportional_to_published_transport(
    synthetic: pd.DataFrame,
) -> None:
    """324110 carries 600 of the set's 1000 published transport, so 60% of 400."""
    allocation = pipeline_allocation(2017, control_total=1000.0, margins=synthetic)
    assert allocation['324110'] == pytest.approx(240.0)
    assert allocation['324190'] == pytest.approx(160.0)


def test_a_set_with_no_published_transport_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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


def test_bound_check_reports_headroom_for_the_other_modes(
    synthetic: pd.DataFrame,
) -> None:
    check = pipeline_bound_check(2017, control_total=1000.0, margins=synthetic)
    assert (check['share'] <= 1.0).all()
    # 211000 takes 600 of its 1000 published, leaving 400 for the other modes
    assert check.loc['211000', 'headroom_other_modes'] == pytest.approx(400.0)
    assert check.loc['324110', 'headroom_other_modes'] == pytest.approx(360.0)


def test_bound_check_catches_an_over_allocation(synthetic: pd.DataFrame) -> None:
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


def test_within_set_weight_can_be_supplied(synthetic: pd.DataFrame) -> None:
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


def test_mode_residual_is_what_the_unbuilt_modes_must_supply(
    synthetic: pd.DataFrame,
) -> None:
    allocation = pipeline_allocation(2017, control_total=1000.0, margins=synthetic)
    residual = mode_residual({'pipeline': allocation}, margins=synthetic)
    # 211000 publishes 1000 all-mode and pipeline takes 600
    assert residual.loc['211000', 'residual'] == pytest.approx(400.0)
    assert not residual['over_allocated'].any()


def test_mode_residual_flags_an_impossible_decomposition(
    synthetic: pd.DataFrame,
) -> None:
    """
    Over-allocating one mode forces negative margin onto the others, which the
    published column never carries at commodity level.
    """
    allocation = pipeline_allocation(2017, control_total=10_000.0, margins=synthetic)
    residual = mode_residual({'pipeline': allocation}, margins=synthetic)
    assert residual['over_allocated'].any()


# --------------------------------------------------------------------------
# rail
# --------------------------------------------------------------------------


def test_rail_crosswalk_targets_are_real_bea_2017_codes() -> None:
    crosswalk = tm.load_rail_crosswalk()
    targets = set(crosswalk['bea_2017_commodity']) - {''}
    assert targets <= set(USA_2017_COMMODITY_CODES)


def test_every_rail_crosswalk_row_records_its_basis() -> None:
    """Excluded rows must say why; mapped rows may inherit a shared note."""
    crosswalk = tm.load_rail_crosswalk()
    excluded = crosswalk[crosswalk['bea_2017_commodity'] == '']
    assert len(excluded) > 0
    assert excluded['basis'].str.startswith('EXCLUDED').all()


def test_rail_crosswalk_covers_every_published_stcc_code() -> None:
    """An unlisted code would be silently dropped from the allocation."""
    published = set(tm.load_rail_revenue_by_stcc(2017).index)
    listed = set(tm.load_rail_crosswalk()['stcc5'])
    assert published <= listed


def test_every_rail_target_receives_published_transport() -> None:
    """
    A target with no published ``TRANS`` has nowhere to put its margin.

    This is the check that caught ``562000`` waste management, which the
    hazardous-waste STCC codes originally mapped to and which BEA gives no
    transportation margin at all.
    """
    crosswalk = tm.load_rail_crosswalk()
    published = tm.published_transport_by_commodity()
    targets = sorted(set(crosswalk['bea_2017_commodity']) - {''})
    assert [c for c in targets if published.get(c, 0.0) <= 0] == []


def test_rail_allocation_is_an_identity_on_the_control_total() -> None:
    allocation = tm.rail_allocation(2017, control_total=1000.0)
    assert allocation.sum() == pytest.approx(1000.0)


def test_rail_commodity_shares_follow_stcc_revenue() -> None:
    """Coal is one STCC code, so its share is checkable straight off the source."""
    revenue = tm.load_rail_revenue_by_stcc(2017)
    by_commodity = tm.rail_revenue_by_commodity(2017)
    # 11212 prepared bituminous coal is the only code mapped to 212100
    assert by_commodity['212100'] == pytest.approx(revenue['11212'])


def test_excluded_codes_contribute_no_rail_margin() -> None:
    """
    The service-class and empty-move codes are dropped, not distributed.

    ``46111`` alone is 14% of released revenue, so a regression that let it
    through would move every share.
    """
    revenue = tm.load_rail_revenue_by_stcc(2017)
    mapped_total = tm.rail_revenue_by_commodity(2017).sum()
    assert mapped_total < revenue.sum()
    excluded = tm.load_rail_crosswalk().query("bea_2017_commodity == ''")['stcc5']
    dropped = revenue.reindex(excluded).dropna().sum()
    assert mapped_total + dropped == pytest.approx(revenue.sum())


def test_a_split_stcc_code_is_apportioned_not_duplicated() -> None:
    """
    ``37112`` spans two BEA truck commodities and must not be counted twice.

    Mapping it to heavy-duty trucks alone allocated 1,745 against a 581 ceiling,
    and emitting it to both without apportioning double-counts its revenue.
    """
    crosswalk = tm.load_rail_crosswalk()
    split = crosswalk[crosswalk['stcc5'] == '37112']['bea_2017_commodity'].tolist()
    assert sorted(split) == ['336112', '336120']

    revenue = tm.load_rail_revenue_by_stcc(2017)
    published = tm.published_transport_by_commodity()
    by_commodity = tm.rail_revenue_by_commodity(2017)
    weight = published['336112'] / (published['336112'] + published['336120'])
    # 336112 receives only this code, so its whole value is 37112's share
    assert by_commodity['336112'] == pytest.approx(revenue['37112'] * weight)


def test_rail_fits_under_the_published_ceiling_everywhere() -> None:
    """No commodity may take more rail margin than all five modes delivered."""
    check = tm.bound_check(tm.rail_allocation(2017))
    assert (check['share'] <= 1.0).all()


def test_pipeline_and_rail_fit_together() -> None:
    """
    The five modes are one decomposition, so built modes must not collide.

    Each fits alone; this is the check that they also fit jointly, which is a
    strictly stronger statement and the one that matters.
    """
    residual = tm.mode_residual(
        {'pipeline': tm.pipeline_allocation(2017), 'rail': tm.rail_allocation(2017)}
    )
    assert not residual['over_allocated'].any()


def test_built_modes_leave_room_for_the_unbuilt_ones() -> None:
    """
    What pipeline and rail leave behind should match truck, water and air.

    They are derived from opposite sides - the residual from the receiving
    column, the give-up from the transport commodities' own rows - so agreement
    is evidence the two built modes are sized right, not an identity.
    """
    published = tm.published_transport_by_commodity().sum()
    built = tm.pipeline_margin_2017() + tm.rail_margin_2017()
    unbuilt = sum(
        tm._mode_give_up_2017(code) for code in ('484000', '483000', '481000')
    )
    assert (published - built) == pytest.approx(unbuilt, rel=0.005)


def test_unknown_stcc_code_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """A code the crosswalk has never seen must fail loudly, not vanish."""
    real = tm.load_rail_revenue_by_stcc

    def with_a_new_code(year: int) -> pd.Series:
        series = real(year)
        series.loc['99999'] = 1234.0
        return series

    monkeypatch.setattr(tm, 'load_rail_revenue_by_stcc', with_a_new_code)
    with pytest.raises(ValueError, match='crosswalk does not list'):
        tm.rail_revenue_by_commodity(2017)


# --------------------------------------------------------------------------
# truck
# --------------------------------------------------------------------------


def test_truck_crosswalk_targets_are_real_bea_2017_codes() -> None:
    crosswalk = tm.load_truck_crosswalk()
    assert set(crosswalk['bea_2017_commodity']) <= set(USA_2017_COMMODITY_CODES)


def test_truck_crosswalk_covers_every_trans_receiving_commodity() -> None:
    """
    A commodity in no group gets no truck margin at all.

    For a mode carrying two thirds of the column that is a strong claim, so the
    crosswalk is required to be exhaustive over the receiving set.
    """
    published = tm.published_transport_by_commodity()
    receiving = set(published[published != 0].index)
    mapped = set(tm.load_truck_crosswalk()['bea_2017_commodity'])
    assert receiving <= mapped


def test_truck_groups_partition_motor_carrier_revenue() -> None:
    """
    The eleven groups sum to the published total, and hazmat is not one of them.

    The hazardous-materials row re-slices the same revenue, so sweeping it in
    would double-count roughly 6% of the column.
    """
    revenue = tm.load_truck_group_revenue(2017)
    assert len(revenue) == 11
    assert not any('Hazardous' in group for group in revenue.index)
    assert revenue.sum() == pytest.approx(270_154e6, rel=1e-6)


def test_other_goods_is_dropped_from_the_allocator() -> None:
    """
    BEA does not use it, and it is a third of motor carrier revenue.

    Letting it through would move every share, so this pins the exclusion.
    """
    revenue = tm.load_truck_group_revenue(2017)
    assert tm.TRUCK_OTHER_GOODS in revenue.index
    assert revenue[tm.TRUCK_OTHER_GOODS] / revenue.sum() == pytest.approx(
        0.324, abs=0.01
    )

    allocation = tm.truck_allocation(2017, control_total=1000.0)
    assert allocation.sum() == pytest.approx(1000.0)


def test_truck_group_shares_follow_table_8_revenue() -> None:
    """Used household goods maps to one commodity, so its share is checkable."""
    revenue = tm.load_truck_group_revenue(2017).drop(index=tm.TRUCK_OTHER_GOODS)
    share = revenue['Used household and office goods'] / revenue.sum()
    allocation = tm.truck_allocation(2017, control_total=1000.0)
    assert allocation['S00402'] == pytest.approx(share * 1000.0)


def test_truck_within_group_weight_can_be_supplied() -> None:
    """The weight is injectable, which is how the residual construction is run."""
    published = tm.published_transport_by_commodity()
    flat = pd.Series(1.0, index=published.index)
    default = tm.truck_allocation(2017, control_total=1000.0)
    weighted = tm.truck_allocation(2017, control_total=1000.0, within_group_weight=flat)
    assert weighted.sum() == pytest.approx(1000.0)
    assert not weighted.equals(default)


def test_independent_modes_collide_and_residual_weighting_shrinks_it() -> None:
    """
    ⚠️ The three built modes do **not** yet fit the published column jointly.

    Each is right on its own total and none of the first two exceeds the ceiling,
    but truck's commodity detail comes from a weight rather than from Table 8, so
    where rail is heavy the two overlap. This pins the size of the problem and
    the fact that residual weighting reduces but does not remove it - see the
    module docstring. It is a real finding about the construction, not a bug in
    any one mode, and the fix is a joint solve rather than a better weight.
    """
    published = tm.published_transport_by_commodity()
    pipeline = tm.pipeline_allocation(2017)
    rail = tm.rail_allocation(2017)

    naive = tm.mode_residual(
        {'pipeline': pipeline, 'rail': rail, 'truck': tm.truck_allocation(2017)}
    )
    naive_overshoot = -naive.loc[naive['over_allocated'], 'residual'].sum()

    claimed = pipeline.reindex(published.index).fillna(0.0) + rail.reindex(
        published.index
    ).fillna(0.0)
    room = (published - claimed).clip(lower=0)
    fitted = tm.mode_residual(
        {
            'pipeline': pipeline,
            'rail': rail,
            'truck': tm.truck_allocation(2017, within_group_weight=room),
        }
    )
    fitted_overshoot = -fitted.loc[fitted['over_allocated'], 'residual'].sum()

    assert naive_overshoot > 0
    assert fitted_overshoot < naive_overshoot / 2


def test_three_modes_leave_room_for_water_and_air_in_aggregate() -> None:
    """
    The per-commodity fit is unresolved; the totals are not.

    Pipeline, rail and truck together leave what water and air give up, to
    within the give-up-versus-receiving rounding, which says the four control
    totals are right even while their distribution collides.
    """
    published = tm.published_transport_by_commodity().sum()
    built = tm.pipeline_margin_2017() + tm.rail_margin_2017() + tm.truck_margin_2017()
    water_and_air = tm._mode_give_up_2017('483000') + tm._mode_give_up_2017('481000')
    assert (published - built) == pytest.approx(water_and_air, rel=0.07)


def test_unmapped_sas_group_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """A new published group must fail loudly rather than shrink the column."""
    real = tm.load_truck_group_revenue

    def with_a_new_group(year: int) -> pd.Series:
        series = real(year)
        series.loc['Antimatter'] = 5000.0
        return series

    monkeypatch.setattr(tm, 'load_truck_group_revenue', with_a_new_group)
    with pytest.raises(ValueError, match='crosswalk does not map'):
        tm.truck_allocation(2017)


# --------------------------------------------------------------------------
# water and air
# --------------------------------------------------------------------------


def test_multiplier_table_covers_every_faf_sctg() -> None:
    """An SCTG with no multiplier would silently default to bulk weighting."""
    crosswalk = tm.load_faf_sctg_crosswalk()
    multipliers = tm.load_difficulty_multipliers()
    assert set(crosswalk['sctg']) <= set(multipliers['sctg'])


def test_air_multipliers_are_exactly_what_bea_stated() -> None:
    """
    *"Air is simple, everything except animal is a 1 (animals is 3)."*

    Air needs no judgement at all, so this is a transcription check rather than
    a modelling one - any drift is a mistake.
    """
    multipliers = tm.load_difficulty_multipliers().set_index('sctg')['air_multiplier']
    assert multipliers['Live animals/fish'] == 3
    assert set(multipliers.drop('Live animals/fish')) == {1}


def test_water_multipliers_follow_the_stated_rule() -> None:
    """Vehicles and transport at the top, bulk at the bottom, the rest at 2."""
    multipliers = tm.load_difficulty_multipliers().set_index('sctg')['water_multiplier']
    for sctg in ('Motorized vehicles', 'Transport equip.', 'Machinery'):
        assert multipliers[sctg] == 3
    for sctg in ('Cereal grains', 'Crude petroleum', 'Coal', 'Gasoline'):
        assert multipliers[sctg] == 1
    assert set(multipliers) == {1, 2, 3}


def test_volume_allocations_are_identities_on_the_control_total() -> None:
    for mode in ('water', 'air'):
        allocation = tm.volume_mode_allocation(mode, 2017, control_total=1000.0)
        assert allocation.sum() == pytest.approx(1000.0)


def test_the_difficulty_multiplier_changes_the_answer() -> None:
    """
    Weighted ton-miles must differ from raw ton-miles, or the table is inert.

    BEA applies the multiplier *to* ton-miles rather than instead of them, so
    this pins that the weighting is actually reaching the allocation.
    """
    flat = pd.Series(1.0, index=tm.load_difficulty_multipliers()['sctg']).rename_axis(
        'sctg'
    )
    weighted = tm.water_allocation(2017)

    ton_miles = tm.load_faf_ton_miles('Water', 2017)
    shares = ton_miles / ton_miles.sum()
    assert (
        not shares.reindex(flat.index)
        .fillna(0.0)
        .equals(
            (ton_miles * 2).div((ton_miles * 2).sum()).reindex(flat.index).fillna(0.0)
            * 1.5
        )
    )
    # the real check: water's top commodity ordering is not the raw ton-mile one
    assert weighted.sum() == pytest.approx(tm.volume_mode_margin_2017('water'))


def test_five_modes_sum_to_the_published_column_in_aggregate() -> None:
    """
    The give-up side totals 415,548 against a receiving side of 414,559.

    That 0.24% gap is the known give-up-versus-receiving difference, and it is
    why a joint solve has to rescale one side rather than assume they agree.
    """
    published = tm.published_transport_by_commodity().sum()
    built = sum(
        tm._mode_give_up_2017(code)
        for code in ('486000', '482000', '484000', '483000', '481000')
    )
    assert built == pytest.approx(published, rel=0.0025)
    assert built > published


def test_adding_water_and_air_worsens_the_collision() -> None:
    """
    ⚠️ The five modes do not fit the published column per commodity.

    Water and air are only 3.8% of the column but concentrate in commodities the
    other modes already claim, so they add to the overshoot rather than filling
    gaps. This pins that fact, which is what rules out sequential weighting and
    forces a joint solve.
    """
    three = {
        'pipeline': tm.pipeline_allocation(2017),
        'rail': tm.rail_allocation(2017),
        'truck': tm.truck_allocation(2017),
    }
    five = {
        **three,
        'water': tm.water_allocation(2017),
        'air': tm.air_allocation(2017),
    }

    def overshoot(allocations: dict[str, pd.Series]) -> float:
        frame = tm.mode_residual(allocations)
        return float(-frame.loc[frame['over_allocated'], 'residual'].sum())

    assert overshoot(five) > overshoot(three) > 0


# --------------------------------------------------------------------------
# annual control totals
# --------------------------------------------------------------------------


def test_the_anchor_year_reproduces_the_published_give_up() -> None:
    """2017 must be an identity, or the anchor is not an anchor."""
    for mode in tm.FREIGHT_REVENUE_MODES:
        published = tm._mode_give_up_2017(tm.MODE_COMMODITIES[mode])
        assert tm.mode_control_total(mode, 2017) == pytest.approx(published)


def test_coverage_ratios_are_near_unity() -> None:
    """
    For a freight mode nearly all revenue is margin, because transport cost is
    unbundled and shifted forward onto the good it moved.

    Rail is nearest because the waybill sample covers essentially all Class I
    traffic. Truck and pipeline sit above 1 in the same direction, since SAS
    covers employer firms only while the margin includes owner-operators.
    """
    ratios = {m: tm.mode_coverage_ratio(m) for m in ('truck', 'rail', 'pipeline')}
    for mode, ratio in ratios.items():
        assert 0.9 < ratio < 1.1, f'{mode} coverage ratio {ratio}'
    assert ratios['rail'] == pytest.approx(0.995, abs=0.01)


def test_water_and_air_never_fall_back_to_the_parent_industry() -> None:
    """
    Their output is mostly passengers, so the parent must not stand in for it.

    A missing freight NAICS has to raise rather than quietly resolving to 481 or
    483, which would make air freight margin track air *passenger* demand - the
    exact failure the retired ton-mile chain hit.
    """
    real = tm.getFlowByActivity

    def without_scheduled_air_freight(
        source: str, year: int, **kwargs: Any
    ) -> pd.DataFrame:
        fba = real(source, year, **kwargs)
        if source != 'Census_SAS':
            return fba
        return fba[fba['ActivityProducedBy'].astype(str) != '481112']

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(tm, 'getFlowByActivity', without_scheduled_air_freight)
    try:
        with pytest.raises(ValueError, match='missing freight NAICS'):
            tm.mode_freight_revenue('air', 2017)
    finally:
        monkeypatch.undo()


def test_a_single_suppressed_truck_group_is_recovered() -> None:
    """
    2022 suppresses pharmaceutical and chemical products.

    The groups are a control total, so zeroing the cell would understate the
    total and inflate every other group once shares renormalise. The shortfall
    against the published total is exactly the suppressed cell.
    """
    revenue = tm.load_truck_group_revenue(2022)
    assert revenue.sum() == pytest.approx(414_693e6, rel=1e-6)
    assert revenue['Pharmaceutical and chemical products'] == pytest.approx(
        18_004e6, rel=1e-6
    )


def test_control_totals_move_with_observed_revenue() -> None:
    """The control is revenue-driven, so it must track the source, not a trend."""
    table = tm.control_total_table(range(2017, 2023))
    assert list(table.columns) == list(tm.FREIGHT_REVENUE_MODES)
    for mode in tm.FREIGHT_REVENUE_MODES:
        revenue = pd.Series(
            {y: tm.mode_freight_revenue(mode, y) for y in range(2017, 2023)}
        )
        ratio = table[mode] / revenue
        assert ratio.std() == pytest.approx(0.0, abs=1e-9)


def test_water_and_air_controls_come_from_freight_naics_only() -> None:
    """
    Their parent industry is mostly passengers, so the parent must never be used.

    Air's margin is 2.6% of its industry output, so falling back to 481 would
    overstate the control by more than an order of magnitude.
    """
    for mode in ('water', 'air'):
        assert tm.mode_control_total(mode, 2017) == pytest.approx(
            tm._mode_give_up_2017(tm.MODE_COMMODITIES[mode])
        )
    air = tm.mode_freight_revenue('air', 2017)
    assert air == pytest.approx(10_661e6, rel=1e-6)
    water = tm.mode_freight_revenue('water', 2017)
    assert water == pytest.approx(19_875e6, rel=1e-6)


def test_water_and_air_ratios_are_not_a_coverage_correction() -> None:
    """
    ⚠️ Unlike the land modes, these ratios are about halved, not near unity.

    Their freight revenue includes international legs while the margin is the
    domestic leg only. That is a larger thing to freeze, and this pins the
    difference so it cannot be mistaken for the coverage story.
    """
    land = {m: tm.mode_coverage_ratio(m) for m in ('truck', 'rail', 'pipeline')}
    sea_air = {m: tm.mode_coverage_ratio(m) for m in ('water', 'air')}
    assert all(0.9 < r < 1.1 for r in land.values())
    assert all(0.4 < r < 0.7 for r in sea_air.values())


def test_all_five_controls_reproduce_the_anchor_year() -> None:
    """2017 must be an identity for every mode, not just the land ones."""
    table = tm.control_total_table([2017])
    total = table.loc[2017].sum()
    published = sum(tm._mode_give_up_2017(c) for c in tm.MODE_COMMODITIES.values())
    assert total == pytest.approx(published)


# --------------------------------------------------------------------------
# the Supply TRANS column
# --------------------------------------------------------------------------


def test_the_column_sums_to_zero() -> None:
    """
    Margin is a redistribution, not value created - target T16's identity.

    This is the only constraint the balance places on Step 4c's own output, so
    it has to hold for every year, not just the anchor.
    """
    for year in (2017, 2019, 2022):
        assert tm.transport_margin_column(year).sum() == pytest.approx(0.0, abs=1.0)


def test_the_column_has_exactly_five_negatives() -> None:
    """The five transport commodities give up; everything else receives."""
    column = tm.transport_margin_column(2017)
    negative = column[column < 0]
    assert set(negative.index) == set(tm.MODE_COMMODITIES.values())


def test_no_mode_delivers_margin_to_a_transport_commodity() -> None:
    """
    BEA publishes zero transport margin *received* by all five modes in 2017.

    A crosswalk that routed margin to one would be a real error, not rounding,
    so the column builder rejects it rather than netting it off.
    """
    column = tm.transport_margin_column(2017)
    receiving = column[column > 0]
    assert not set(receiving.index) & set(tm.MODE_COMMODITIES.values())


def test_the_anchor_year_column_matches_the_published_total() -> None:
    """2017's give-up side is the published one, mode by mode."""
    column = tm.transport_margin_column(2017)
    for mode, commodity in tm.MODE_COMMODITIES.items():
        published = tm._mode_give_up_2017(commodity)
        assert column[commodity] == pytest.approx(-published, rel=1e-6)


def test_the_crosswalk_covers_every_year_the_column_runs() -> None:
    """
    STB publishes a different STCC code set each year.

    The crosswalk is authored on 2017 but must cover the union, or a later year
    silently drops revenue. 149 codes appear only after 2017.
    """
    listed = set(tm.load_rail_crosswalk()['stcc5'])
    for year in (2018, 2020, 2022, 2024):
        assert set(tm.load_rail_revenue_by_stcc(year).index) <= listed


# --------------------------------------------------------------------------
# the Supply bridge TRANS column
# --------------------------------------------------------------------------


def test_supply_bridge_trans_satisfies_target_t16() -> None:
    """
    The column must net to zero in every sourced year, which is target T16.

    Margin is a redistribution, not value created, so this is the only
    constraint the balance places on Step 4c's transport output.
    """
    for year in (min(TRANSPORT_MARGIN_YEARS), max(TRANSPORT_MARGIN_YEARS)):
        bridge = derive_initial_supply_bridge(year)
        assert bridge['TRANS'].sum() == pytest.approx(0.0, abs=1.0)
        assert not bridge['TRANS'].isna().any()


def test_supply_bridge_trans_is_zero_not_nan_off_the_receiving_set() -> None:
    """
    A commodity bearing no transport margin has zero, which is information.

    NaN would mean unsourced, and would also break the T16 sum. Only five
    commodities may be negative - the modes that give the margin up.
    """
    trans = derive_initial_supply_bridge(2017)['TRANS']
    assert (trans == 0.0).any()
    assert set(trans[trans < 0].index) == set(MODE_COMMODITIES.values())


def test_supply_bridge_leaves_unsourced_years_alone() -> None:
    """
    2023 has no truck or pipeline source, so TRANS must stay unfilled.

    Filling it from a partial set of modes would break the identity silently.
    """
    assert derive_initial_supply_bridge(2023)['TRANS'].isna().all()


def test_supply_bridge_trade_is_sourced_a_year_further_than_trans() -> None:
    """
    TRADE now lands too, and reaches 2023 where TRANS stops at 2022.

    ⚠️ **This test used to assert the opposite.** TRADE was expected to wait on
    4a (#570) and 4d (#580), because the plan reached it as a rate on producer
    value. The anchor-and-move construction in ``nowcast_trade_margins`` reaches
    the same column from the *give-up* side instead, and the give-up is observed
    - so like TRANS it never touches the nowcast base and carries no
    circularity with Step 6b. The extra year is the Census series running to
    2023 where SAS stops at 2022.
    """
    bridge_2023 = derive_initial_supply_bridge(2023)
    assert bridge_2023['TRADE'].notna().all()
    assert bridge_2023['TRANS'].isna().all()
    assert abs(bridge_2023['TRADE'].sum()) < 1


def test_supply_bridge_leaves_2024_trade_unsourced() -> None:
    """
    2024 is inside the bridge's year range but outside the Census series.

    ⚠️ NaN is the *only* correct answer here, and it has to stay NaN rather
    than becoming zeros. In a sourced year the non-receiving commodities are
    filled with zeros deliberately - a commodity that bears no trade margin has
    none, and that is information. A 2024 column of zeros would be
    indistinguishable from that, so an unsourced year filled the same way would
    read as "no commodity bears a trade margin in 2024" instead of "we have not
    measured 2024". Extrapolating 2023 forward would be worse still.
    """
    bridge_2024 = derive_initial_supply_bridge(2024)
    assert bridge_2024['T007'].notna().any(), '2024 should reach the bridge at all'
    assert bridge_2024['TRADE'].isna().all()
    assert bridge_2024['TRANS'].isna().all()
