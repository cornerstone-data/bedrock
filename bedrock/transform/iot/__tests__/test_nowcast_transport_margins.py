"""Tests for the annual transport margin (Step 4c phase 2, #611).

All synthetic. The three real inputs - the FBS ton-miles, the gross output FBA
and the published 2017 anchor - are substituted, because what is worth testing
here is the arithmetic that joins them: that the anchor year comes back
untouched, that the level is the control total and the shape is ton-miles, and
that a commodity which loses its ton-miles raises rather than silently carrying
its 2017 margin forward.
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

#: Mode output doubles, so the control total doubles too.
MODE_OUTPUT = pd.DataFrame(
    {2017: [1000.0] * 5, 2018: [2000.0] * 5},
    index=list(TRANSPORT_MODE_COMMODITIES),
)
MODE_GIVEN_UP = pd.Series(
    dict.fromkeys(TRANSPORT_MODE_COMMODITIES, 400.0), name='margin_given_up'
)


@pytest.fixture(autouse=True)
def stub_sources(monkeypatch: pytest.MonkeyPatch) -> None:
    """Substitute the three pipeline inputs, and the 402-commodity index."""
    monkeypatch.setattr(ntm, 'sctg_ton_miles', lambda years: TON_MILES)
    monkeypatch.setattr(ntm, 'sctg_to_commodity', lambda: CROSSWALK)
    monkeypatch.setattr(ntm, '_gross_output', lambda years: MODE_OUTPUT)
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
    # output doubles at a fixed ratio, so the margin given up doubles
    assert components[2018].sum() == pytest.approx(2 * components[2017].sum())


def test_control_total_is_rescaled_onto_the_published_anchor() -> None:
    control = transport_margin_control_total(YEARS)
    assert control[ANCHOR_YEAR] == pytest.approx(ANCHOR.sum())
    assert control[2018] == pytest.approx(2 * ANCHOR.sum())


def test_anchor_year_reproduces_the_published_column_exactly() -> None:
    margins = transport_margins(YEARS)
    for commodity, published in ANCHOR.items():
        assert margins.loc[commodity, ANCHOR_YEAR] == pytest.approx(published)


def test_total_equals_the_control_total_and_shape_follows_ton_miles() -> None:
    margins = transport_margins(YEARS)
    control = transport_margin_control_total(YEARS)
    assert margins[2018].sum() == pytest.approx(control[2018])
    # shape: 800*0.5, 200*0.5, 1000*1.2 = 400, 100, 1200, scaled to 4000
    assert margins.loc['212100', 2018] == pytest.approx(4000 * 400 / 1700)
    assert margins.loc['324110', 2018] == pytest.approx(4000 * 1200 / 1700)


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
    # ton-miles carry the total from 2000 to 1700, while the level doubles
    assert summary.loc[2018, 'volume'] == pytest.approx(1700 / 2000)
    assert summary.loc[2018, 'level'] == pytest.approx(2.0)
    assert summary.loc[2018, 'repricing'] == pytest.approx(2.0 * 2000 / 1700)
