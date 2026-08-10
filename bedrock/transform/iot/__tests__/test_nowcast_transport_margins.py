"""Tests for the annual transport margin (Step 4c phase 2, #611).

All synthetic. The real inputs - the FBS ton-miles by SCTG and by mode, the
gross output and price index tables, the NIPA final-use index and the published
2017 anchor - are substituted, because what is worth testing here is the
arithmetic that joins them: that the anchor year comes back untouched under
*every* control method, that each method moves the level its own way and
:data:`MODE_CONTROL` dispatches between them, that the level is the control
total while the shape is ton-miles, and that a commodity which loses its
ton-miles raises rather than silently carrying its 2017 margin forward.

The two ``pce_final_use_index`` tests are the exception: they run the real
function over a synthetic bridge and NIPA table, since the 2017-share-freezing
join is the one piece of new arithmetic worth exercising directly.
"""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.transform.iot import nowcast_transport_margins as ntm
from bedrock.transform.iot.nowcast_transport_margins import (
    ANCHOR_YEAR,
    TRANSPORT_MODE_COMMODITIES,
    _as_years,
    commodity_ton_miles,
    control_total_components,
    movement_summary,
    ton_mile_growth,
    transport_margin_control_total,
    transport_margins,
)

YEARS = (2017, 2018)

#: Captured before the autouse fixture replaces it, for the two tests that
#: exercise the real join rather than stubbing it.
_REAL_PCE_INDEX = ntm.pce_final_use_index

#: The mixed control total in 2018, given the stubs below: air and water on
#: freight volume at 1200 each, rail, truck and pipeline at 800.
MIXED_2018 = 4800.0

#: Two commodities in one SCTG, one commodity spanning two.
CROSSWALK = pd.DataFrame(
    {
        'sctg': ['Coal', 'Coal', 'Gasoline', 'Fuel oils'],
        'Commodity Code': ['212100', '221100', '324110', '324110'],
    }
)

TON_MILES = pd.DataFrame(
    {2017: [100.0, 40.0, 60.0], 2018: [50.0, 60.0, 60.0]},
    index=pd.Index(['Coal', 'Gasoline', 'Fuel oils'], name='Flowable'),
)

ANCHOR = pd.Series(
    {'212100': 800.0, '221100': 200.0, '324110': 1000.0},
    name='transport_margins',
)

#: Mode output doubles. Under output_ratio that doubles the give-up; under
#: residual it does not, since direct uses move with PCE instead.
MODE_OUTPUT = pd.DataFrame(
    {2017: [1000.0] * 5, 2018: [2000.0] * 5},
    index=list(TRANSPORT_MODE_COMMODITIES),
)
MODE_GIVEN_UP = pd.Series(
    dict.fromkeys(TRANSPORT_MODE_COMMODITIES, 400.0), name='margin_given_up'
)

#: Freight volume doubles and freight prices rise by half, so the
#: freight_volume treatment reaches 400 x 2 x 1.5 = 1200 in 2018. Distinct from
#: both other treatments' 800, which is what lets the dispatch be tested.
MODE_TON_MILES = pd.DataFrame(
    {2017: [100.0] * 5, 2018: [200.0] * 5}, index=list(TRANSPORT_MODE_COMMODITIES)
)
MODE_PRICES = pd.DataFrame(
    {2017: [1.0] * 5, 2018: [1.5] * 5}, index=list(TRANSPORT_MODE_COMMODITIES)
)
#: PCE doubles, so residual direct uses go 600 -> 1200 and the residual is
#: 2000 - 1200 = 800.
MODE_PCE_INDEX = pd.DataFrame(
    {2017: [1.0] * 5, 2018: [2.0] * 5}, index=list(TRANSPORT_MODE_COMMODITIES)
)


@pytest.fixture(autouse=True)
def stub_sources(monkeypatch: pytest.MonkeyPatch) -> None:
    """Substitute the three pipeline inputs, and the 402-commodity index."""
    monkeypatch.setattr(ntm, 'sctg_ton_miles', lambda years: TON_MILES)
    monkeypatch.setattr(ntm, 'sctg_to_commodity', lambda: CROSSWALK)
    monkeypatch.setattr(ntm, '_gross_output', lambda years: MODE_OUTPUT)
    monkeypatch.setattr(ntm, 'mode_ton_miles', lambda years: MODE_TON_MILES)
    monkeypatch.setattr(ntm, 'mode_price_index', lambda years: MODE_PRICES)
    monkeypatch.setattr(ntm, 'pce_final_use_index', lambda years: MODE_PCE_INDEX)
    monkeypatch.setattr(ntm, '_anchor_margins', lambda: ANCHOR)
    monkeypatch.setattr(
        ntm,
        'margins_by_commodity',
        lambda: pd.DataFrame({'margin_given_up': MODE_GIVEN_UP}),
    )
    monkeypatch.setattr(
        ntm,
        'USA_2017_COMMODITY_CODES',
        ('212100', '221100', '324110', '484000'),
    )


def test_as_years_always_carries_the_anchor() -> None:
    assert _as_years([2020, 2018, 2018]) == (2017, 2018, 2020)
    assert _as_years([2017]) == (2017,)


def test_commodity_ton_miles_sums_the_groups_a_commodity_belongs_to() -> None:
    ton_miles = commodity_ton_miles(YEARS)
    # both coal commodities carry the whole SCTG, not a share of it
    assert ton_miles.loc['212100', 2017] == 100.0
    assert ton_miles.loc['221100', 2017] == 100.0
    # the two-SCTG commodity takes the total of both
    assert ton_miles.loc['324110', 2017] == 100.0
    assert ton_miles.loc['324110', 2018] == 120.0


def test_commodity_ton_miles_raises_on_an_sctg_the_fbs_lacks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        ntm,
        'sctg_to_commodity',
        lambda: pd.DataFrame({'sctg': ['Renamed'], 'Commodity Code': ['212100']}),
    )
    with pytest.raises(ValueError, match='SCTGs the FBS does not carry'):
        commodity_ton_miles(YEARS)


def test_growth_is_one_in_the_anchor_year() -> None:
    growth = ton_mile_growth(YEARS)
    assert (growth[ANCHOR_YEAR] == 1.0).all()
    assert growth.loc['212100', 2018] == pytest.approx(0.5)
    assert growth.loc['324110', 2018] == pytest.approx(1.2)


def test_control_components_reproduce_the_anchor_give_up() -> None:
    components = control_total_components(YEARS)
    assert components[ANCHOR_YEAR].equals(MODE_GIVEN_UP.rename(None))
    # the movement is per mode now, not one ratio - see MODE_CONTROL
    assert components[2018].sum() == pytest.approx(MIXED_2018)


def test_control_total_is_rescaled_onto_the_published_anchor() -> None:
    control = transport_margin_control_total(YEARS)
    assert control[ANCHOR_YEAR] == pytest.approx(ANCHOR.sum())
    # the composite is 2000 in the anchor year, so the rescaling is 1:1 here
    assert control[2018] == pytest.approx(MIXED_2018)


def test_anchor_year_reproduces_the_published_column_exactly() -> None:
    margins = transport_margins(YEARS)
    for commodity, published in ANCHOR.items():
        assert margins.loc[commodity, ANCHOR_YEAR] == pytest.approx(published)


def test_total_equals_the_control_total_and_shape_follows_ton_miles() -> None:
    margins = transport_margins(YEARS)
    control = transport_margin_control_total(YEARS)
    assert margins[2018].sum() == pytest.approx(control[2018])
    # shape: 800*0.5, 200*0.5, 1000*1.2 = 400, 100, 1200, scaled to the control
    assert margins.loc['212100', 2018] == pytest.approx(MIXED_2018 * 400 / 1700)
    assert margins.loc['324110', 2018] == pytest.approx(MIXED_2018 * 1200 / 1700)


def test_reindexed_to_every_commodity_with_zero_for_non_receivers() -> None:
    margins = transport_margins(YEARS)
    assert list(margins.index) == ['212100', '221100', '324110', '484000']
    # a transport mode gives margin up rather than receiving it
    assert (margins.loc['484000'] == 0.0).all()


def test_a_receiving_commodity_without_ton_miles_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        ntm,
        'sctg_to_commodity',
        lambda: CROSSWALK[CROSSWALK['Commodity Code'] != '221100'],
    )
    with pytest.raises(ValueError, match='no ton-miles to move by'):
        transport_margins(YEARS)


def test_movement_summary_separates_repricing_from_volume() -> None:
    summary = movement_summary(YEARS)
    assert (summary.loc[ANCHOR_YEAR] == 1.0).all()
    # ton-miles carry the total from 2000 down to 1700 while the level rises
    level = MIXED_2018 / 2000
    assert summary.loc[2018, 'volume'] == pytest.approx(1700 / 2000)
    assert summary.loc[2018, 'level'] == pytest.approx(level)
    assert summary.loc[2018, 'repricing'] == pytest.approx(level * 2000 / 1700)


def test_every_treatment_is_an_identity_in_the_anchor_year() -> None:
    """The point of anchoring: the choice of driver changes movement, not 2017."""
    for method in (ntm.MIXED, ntm.RESIDUAL, ntm.FREIGHT_VOLUME, ntm.OUTPUT_RATIO):
        components = control_total_components(YEARS, method=method)
        assert components[ANCHOR_YEAR].tolist() == [400.0] * 5, method


def test_each_treatment_moves_the_level_its_own_way() -> None:
    """Residual 2000-600x2, freight 400x2x1.5, ratio 2000x0.4 - all distinct."""
    by_method = {
        method: control_total_components(YEARS, method=method)[2018]
        for method in (ntm.RESIDUAL, ntm.FREIGHT_VOLUME, ntm.OUTPUT_RATIO)
    }
    assert by_method[ntm.RESIDUAL].tolist() == [800.0] * 5
    assert by_method[ntm.FREIGHT_VOLUME].tolist() == [1200.0] * 5
    assert by_method[ntm.OUTPUT_RATIO].tolist() == [800.0] * 5


def test_mixed_gives_each_mode_the_treatment_mode_control_assigns() -> None:
    """Air and water on freight volume, rail and truck residual, pipeline ratio."""
    components = control_total_components(YEARS, method=ntm.MIXED)[2018]
    expected = {
        '481000': 1200.0,  # air, freight_volume
        '482000': 800.0,  # rail, residual
        '483000': 1200.0,  # water, freight_volume
        '484000': 800.0,  # truck, residual
        '486000': 800.0,  # pipeline, output_ratio
    }
    assert components.to_dict() == expected
    assert ntm.MODE_CONTROL.keys() == set(TRANSPORT_MODE_COMMODITIES)


def test_comparison_carries_every_method_and_agrees_with_each() -> None:
    comparison = ntm.control_total_comparison(YEARS)
    assert set(comparison.columns) == {
        ntm.RESIDUAL,
        ntm.FREIGHT_VOLUME,
        ntm.OUTPUT_RATIO,
        ntm.MIXED,
    }
    for method in comparison.columns:
        expected = control_total_components(YEARS, method=method).sum()
        pd.testing.assert_series_equal(comparison[method], expected, check_names=False)
    # every method is the same total in the anchor year, and they diverge after
    assert comparison.loc[ANCHOR_YEAR].nunique() == 1
    assert comparison.loc[2018].nunique() > 1


def test_an_unknown_control_method_raises_rather_than_silently_defaulting() -> None:
    with pytest.raises(ValueError, match='unknown control method'):
        control_total_components(YEARS, method='ton_miles_only')


def test_pce_index_holds_a_split_lines_2017_share_and_is_one_at_the_anchor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """NIPA line 7 splits 75/25 across air and water; that split is frozen."""
    bridge = pd.DataFrame(
        {
            'NIPA Line': [3, 7, 7],
            'Commodity Code': ['484000', '481000', '483000'],
            "Purchasers' Value": [100.0, 75.0, 25.0],
        }
    )
    pce = {2017: pd.Series({3: 100.0, 7: 100.0}), 2018: pd.Series({3: 50.0, 7: 400.0})}
    monkeypatch.setattr(ntm, 'load_2017_pce_bridge_detail_usa', lambda: bridge)
    monkeypatch.setattr(ntm, '_pce_by_nipa_line', lambda year: pce[year])
    monkeypatch.setattr(ntm, 'pce_final_use_index', _REAL_PCE_INDEX)
    _REAL_PCE_INDEX.cache_clear()

    index = ntm.pce_final_use_index(YEARS)
    assert index[ANCHOR_YEAR].tolist() == [1.0] * 5
    # line 7 quadruples, and both its commodities move with it, share intact
    assert index.loc['481000', 2018] == 4.0
    assert index.loc['483000', 2018] == 4.0
    assert index.loc['484000', 2018] == 0.5
    # rail and pipeline have no PCE at all: index 1, never NaN
    assert index.loc['482000', 2018] == 1.0
    assert index.loc['486000', 2018] == 1.0
    _REAL_PCE_INDEX.cache_clear()


def test_pce_index_raises_when_the_bridge_names_a_line_nipa_lacks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bridge = pd.DataFrame(
        {
            'NIPA Line': [999],
            'Commodity Code': ['484000'],
            "Purchasers' Value": [100.0],
        }
    )
    monkeypatch.setattr(ntm, 'load_2017_pce_bridge_detail_usa', lambda: bridge)
    monkeypatch.setattr(ntm, '_pce_by_nipa_line', lambda year: pd.Series({3: 1.0}))
    monkeypatch.setattr(ntm, 'pce_final_use_index', _REAL_PCE_INDEX)
    _REAL_PCE_INDEX.cache_clear()
    with pytest.raises(ValueError, match='different vintages'):
        ntm.pce_final_use_index(YEARS)
    _REAL_PCE_INDEX.cache_clear()
