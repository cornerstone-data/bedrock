"""Tests for the subsidies column (Step 4d, #580).

Two things dominate what can go wrong here, and neither shows up in a total.

**The sign.** ``SUB`` is stored negative in the Supply table and positive in NIPA
and in the Use table's ``T00SUB`` row. A single missed flip fails ``T015`` by
twice the subsidy total rather than by the subsidy total, so the convention is
asserted at every boundary rather than once at the end.

**2020-2021.** The column is 11.7x its 2017 level and its composition inverts.
Every aggregate check passes on a 2020 column that puts 420bn on housing or
377bn on insurance carriers, so those two years are pinned on their commodity
distribution, not on their total.
"""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.transform.iot import nowcast_subsidies as sb

MILLION = 1e6

#: 2017 totals, $M. NIPA reads it positive, the Supply table negative.
NIPA_SUB_2017 = 59_875
PUBLISHED_SUB_2017 = -59_876

#: BEA's published PPP totals, $bn - and the T31300 "other" line they sit inside.
PPP_2020_BN = 447.5
PPP_2021_BN = 235.8
OTHER_2020_BN = 587.3
OTHER_2021_BN = 527.3


# --- the sign convention, checked at every boundary -------------------------


def test_nipa_is_read_positive() -> None:
    """NIPA publishes subsidies positive; this module flips the sign exactly once."""
    assert sb.sub_control_total(sb.ANCHOR_YEAR) > 0
    assert (sb.subsidy_type_totals(sb.ANCHOR_YEAR) > 0).all()
    assert (sb.ppp_by_sector()[2020] >= 0).all()


def test_the_published_column_is_negative() -> None:
    published = sb.published_sub_by_commodity()

    assert (published <= 0).all()
    assert published.sum() / MILLION == pytest.approx(PUBLISHED_SUB_2017, abs=1)


@pytest.mark.parametrize('year', list(sb.SUB_YEARS))
def test_the_built_column_is_negative_and_ties_to_nipa(year: int) -> None:
    """Both halves matter: the sign, and the total it sums to with that sign."""
    column = sb.sub_column(year)

    assert (column <= 0).all()
    assert column.sum() == pytest.approx(-sb.sub_control_total(year), abs=MILLION)
    assert len(column) == 402


def test_a_sign_flipped_source_raises_rather_than_doubling_the_wedge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A positive NIPA total would be flipped twice and pass every total check."""
    monkeypatch.setattr(sb, 'nipa_line', lambda code, year: -1.0)

    with pytest.raises(ValueError, match='flipped'):
        sb.sub_control_total(2017)


# --- the anchor -------------------------------------------------------------


def test_every_commodity_with_2017_subsidy_is_typed() -> None:
    """An untyped commodity is dropped from every later year, silently.

    The column total would still tie to NIPA, because the types are scaled to it -
    so this is invisible to every aggregate check.
    """
    published = sb.published_sub_by_commodity()
    typed = {c for _, commodities in sb.SUBSIDY_TYPES.values() for c in commodities}

    assert set(published.index[published < 0]) <= typed
    assert len(published[published < 0]) == 15


def test_types_do_not_share_a_commodity_or_a_series() -> None:
    commodities = [c for _, cs in sb.SUBSIDY_TYPES.values() for c in cs]
    series = [s for s, _ in sb.SUBSIDY_TYPES.values()]

    assert len(commodities) == len(set(commodities))
    assert len(series) == len(set(series))
    assert sb.STATE_LOCAL_SERIES not in series


def test_anchor_shares_partition_each_type() -> None:
    shares = sb.anchor_shares()

    for subsidy_type, (_, commodities) in sb.SUBSIDY_TYPES.items():
        column = shares[subsidy_type]
        assert column.sum() == pytest.approx(1.0)
        assert set(column[column > 0].index) <= set(commodities)


def test_2017_replays_the_published_column_per_commodity() -> None:
    """Anchoring on the published column rather than NIPA's type totals is what buys this.

    NIPA housing is 35,771 in 2017 where the three housing commodities carry
    39,636; anchoring the other way round would push ``531HST`` out by ~3,500 $M
    in the one year BEA actually publishes a split.
    """
    built = sb.sub_column(sb.ANCHOR_YEAR)
    published = sb.published_sub_by_commodity()

    assert (built - published).abs().max() < MILLION


def test_type_growth_is_one_in_the_anchor_year() -> None:
    growth = sb.type_growth(sb.ANCHOR_YEAR)

    assert growth.eq(1.0).all()


def test_the_type_lines_carry_the_pandemic_signal() -> None:
    """Most of 2020 is visible in NIPA's own type lines, before any modelling."""
    growth = sb.type_growth(2020)

    assert growth['air carriers'] > 80  # payroll support
    assert growth['agricultural'] > 3.5
    assert growth['housing'] == pytest.approx(1.23, abs=0.05)


# --- BEA's PPP allocation ---------------------------------------------------


def test_ppp_rows_exclude_the_subtotals_and_sum_to_the_published_total() -> None:
    """Manufacturing carries durable and nondurable beneath it; summing all three double counts."""
    sectors = sb.ppp_by_sector()

    assert sectors[2020].sum() / 1e9 == pytest.approx(PPP_2020_BN, abs=0.5)
    assert sectors[2021].sum() / 1e9 == pytest.approx(PPP_2021_BN, abs=0.5)
    assert sectors[2022].sum() == 0.0  # the programme ended
    assert 'Durable goods' not in sectors.index


def test_every_private_commodity_is_in_the_ppp_base() -> None:
    """A commodity matching no prefix takes no PPP while the shares still sum to 1."""
    base = sb.ppp_base_commodities()

    assert len(base) == 402 - len(sb._PPP_EXCLUDED_COMMODITIES)
    assert not set(base.index) & sb._PPP_EXCLUDED_COMMODITIES


def test_customs_duties_is_not_filed_under_wholesale_trade() -> None:
    """``4200ID`` starts ``42``; a bare prefix rule makes it a wholesaler."""
    assert '4200ID' in sb._PPP_EXCLUDED_COMMODITIES
    assert '4200ID' not in sb.ppp_base_commodities().index


def test_government_and_specials_take_no_ppp() -> None:
    """BEA's table is subsidies to *private* industries."""
    base = sb.ppp_base_commodities().index

    for code in ('S00500', 'S00600', 'GSLGE', 'GSLGO', 'S00402', 'S00300', '491000'):
        assert code not in base


def test_imputed_housing_is_out_of_the_ppp_base() -> None:
    """Otherwise real-estate PPP lands on imputed rent and doubles the housing line."""
    base = sb.ppp_base_commodities().index

    assert '531HST' not in base
    assert '531HSO' not in base
    assert '531ORE' in base


def test_ppp_shares_are_shares() -> None:
    shares = sb.ppp_commodity_shares(2020)

    assert shares.sum() == pytest.approx(1.0)
    assert (shares >= 0).all()
    assert len(shares) == 402


def test_ppp_is_unavailable_outside_the_years_bea_publishes() -> None:
    with pytest.raises(ValueError, match='outside that'):
        sb.ppp_commodity_shares(2024)


# --- 2020-2021, where the frozen vector fails -------------------------------


def test_2020_does_not_put_the_pandemic_on_housing_or_insurance() -> None:
    """The two failure modes, both of which pass every total check.

    Freezing the 2017 commodity shares puts ~420bn on housing; moving the 2017
    *other* vector by its own line puts ~377bn on ``5241XX``, which is 64% of
    that vector. The build does neither.
    """
    column = sb.sub_column(2020).abs()
    total = column.sum()

    housing = column[list(sb.SUBSIDY_TYPES['housing'][1])].sum()
    assert housing / total < 0.10  # NIPA says 6.3%
    assert column['5241XX'] / total < 0.05


def test_2020_and_2021_reach_the_sectors_ppp_actually_went_to() -> None:
    """Restaurants, health care and accommodation, none of which carry 2017 subsidy."""
    published = sb.published_sub_by_commodity()
    column = sb.sub_column(2021).abs()

    for code in ('722110', '722211', '721000', '233411'):
        assert published[code] == 0.0
        assert column[code] > 1e9

    assert (column > 0).sum() > 300


def test_non_pandemic_years_stay_on_the_fifteen_anchored_commodities() -> None:
    """The PPP override is 2020-2021 only; PPP is zero from 2022."""
    for year in (2017, 2018, 2019, 2022, 2023, 2024):
        assert (sb.sub_column(year) != 0).sum() == 15


def test_a_year_outside_the_nipa_window_raises() -> None:
    with pytest.raises(ValueError, match='outside the years'):
        sb.sub_column(2025)


# --- what is still assumed, pinned so it cannot drift unnoticed -------------


def test_ppp_is_only_part_of_the_other_line() -> None:
    """The standing assumption, and #689's target.

    Applying the PPP vector to the whole *other* line assumes the non-PPP
    remainder distributes like PPP. It covers 76% of 2020 and only 45% of 2021,
    so that assumption is doing real work in 2021 and this pins how much.
    """
    other = sb.control_total_table()['other'].astype(float)

    assert PPP_2020_BN * 1000 / float(other[2020]) == pytest.approx(0.76, abs=0.02)
    assert PPP_2021_BN * 1000 / float(other[2021]) == pytest.approx(0.45, abs=0.02)
    assert float(other[2020]) / 1000 == pytest.approx(OTHER_2020_BN, abs=0.5)
    assert float(other[2021]) / 1000 == pytest.approx(OTHER_2021_BN, abs=0.5)


def test_decomposition_row_margin_is_the_column() -> None:
    decomposition = sb.sub_decomposition(2020)

    assert (
        decomposition[list(sb.SUBSIDY_TYPES)]
        .sum(axis='columns')
        .equals(decomposition['SUB'])
    )
    pd.testing.assert_series_equal(
        decomposition['SUB'].rename('SUB'), sb.sub_column(2020)
    )
