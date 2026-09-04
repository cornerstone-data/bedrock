"""Tests for the subsidies column (Step 4d, #580; products-only control #784).

Two things dominate what can go wrong here, and neither shows up in a total.

**The sign.** ``SUB`` is stored negative in the Supply table and positive in NIPA
and in the Use table's ``T00SUB`` row. A single missed flip fails ``T015`` by
twice the subsidy total rather than by the subsidy total, so the convention is
asserted at every boundary rather than once at the end.

**The concept.** NIPA T31300 is products *plus production* subsidies; the
column holds products only. The two are identical 2017-2019 and 5.9x apart in
2020. Leveling to NIPA - the construction before #784 - put ~580bn of
pandemic production subsidies onto commodities; the column control is the
published summary Supply total now, and 2020-21 are pinned on it.
"""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.transform.iot import nowcast_subsidies as sb

MILLION = 1e6

#: 2017 totals, $M. NIPA reads it positive, the Supply table negative.
NIPA_SUB_2017 = 59_875
PUBLISHED_SUB_2017 = -59_876

#: The production-subsidy wedge, $M - NIPA total minus published products-only.
WEDGE_2020 = 580_140
WEDGE_2021 = 522_383


# --- the sign convention, checked at every boundary -------------------------


def test_nipa_is_read_positive() -> None:
    """NIPA publishes subsidies positive; this module flips the sign exactly once."""
    assert sb.sub_control_total(sb.ANCHOR_YEAR) > 0
    assert (sb.subsidy_type_totals(sb.ANCHOR_YEAR) > 0).all()


def test_the_published_column_is_negative() -> None:
    published = sb.published_sub_by_commodity()

    assert (published <= 0).all()
    assert published.sum() / MILLION == pytest.approx(PUBLISHED_SUB_2017, abs=1)


@pytest.mark.parametrize('year', list(sb.SUB_YEARS))
def test_the_built_column_is_negative_and_ties_to_the_published_summary(
    year: int,
) -> None:
    """Both halves matter: the sign, and the products-only total it sums to."""
    column = sb.sub_column(year)

    assert (column <= 0).all()
    assert column.sum() == pytest.approx(-sb.published_sub_total(year), abs=MILLION)
    assert len(column) == 402


def test_the_two_controls_agree_before_the_pandemic_and_not_after() -> None:
    """The concept split #784 is built on, pinned from both sources.

    NIPA total subsidies and the published products-only column are the same
    money 2017-2019; from 2020 NIPA carries production subsidies (PPP, ERC,
    Provider Relief) that belong on industries, not on any commodity row.
    """
    for year in (2017, 2018, 2019):
        assert sb.sub_control_total(year) == pytest.approx(
            sb.published_sub_total(year), abs=2 * MILLION
        )
    wedge_2020 = sb.sub_control_total(2020) - sb.published_sub_total(2020)
    wedge_2021 = sb.sub_control_total(2021) - sb.published_sub_total(2021)
    assert wedge_2020 / MILLION == pytest.approx(WEDGE_2020, abs=10)
    assert wedge_2021 / MILLION == pytest.approx(WEDGE_2021, abs=10)


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

    The column total would still tie to the control, because every group is
    scaled to it - so this is invisible to every aggregate check.
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
    """The conditioning is an identity at the anchor.

    The summary groups aggregate the published detail column, so scaling the
    anchored shape to them reproduces the detail cells - to the workbook's own
    whole-million rounding, which stacks once per group.
    """
    built = sb.sub_column(sb.ANCHOR_YEAR)
    published = sb.published_sub_by_commodity()

    assert (built - published).abs().max() < 2 * MILLION


def test_type_growth_is_one_in_the_anchor_year() -> None:
    growth = sb.type_growth(sb.ANCHOR_YEAR)

    assert growth.eq(1.0).all()


def test_the_type_lines_carry_the_pandemic_signal() -> None:
    """The NIPA lines still shape within-group splits; their reading is pinned."""
    growth = sb.type_growth(2020)

    assert growth['air carriers'] > 80  # payroll support
    assert growth['agricultural'] > 3.5
    assert growth['housing'] == pytest.approx(1.23, abs=0.05)


# --- 2020-2021, where the two subsidy concepts diverge ----------------------


def test_2020_does_not_carry_the_pandemic_production_subsidies() -> None:
    """The old construction put ~580bn of PPP-family money on commodities."""
    column = sb.sub_column(2020).abs()

    assert column.sum() / MILLION == pytest.approx(118_367, abs=25)
    # products-only, housing is a third of the column again - not the 6.3%
    # NIPA's combined total implied when production money swamped it
    housing = column[list(sb.SUBSIDY_TYPES['housing'][1])].sum()
    assert 0.30 < housing / column.sum() < 0.45


def test_transit_is_injected_from_the_published_group() -> None:
    """``485000`` carries no 2017 subsidy and cannot be scaled into existence."""
    assert sb.sub_column(2017)['485000'] == 0.0
    assert sb.sub_column(2020)['485000'] / MILLION == pytest.approx(-15_617, abs=2)
    assert sb.sub_column(2022)['485000'] / MILLION == pytest.approx(-21_948, abs=2)


def test_support_is_the_anchored_commodities_plus_the_injections() -> None:
    """The conditioning can move mass only where the shape or an injection is."""
    allowed = {
        c for _, commodities in sb.SUBSIDY_TYPES.values() for c in commodities
    } | set(sb.INJECTED_COMMODITIES.values())

    for year in sb.SUB_YEARS:
        column = sb.sub_column(year)
        support = set(column.index[column != 0])
        assert support <= allowed
        assert len(support) >= 14


def test_an_unreachable_published_group_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A new programme year deserves a decision, not a silent hole."""
    real = sb.published_sub_by_group.__wrapped__

    def with_utilities(year: int) -> pd.Series:
        groups = real(year).copy()
        groups.loc['22'] = -5_000 * MILLION
        return groups

    monkeypatch.setattr(sb, 'published_sub_by_group', with_utilities)
    with pytest.raises(ValueError, match='needs a home'):
        sb.sub_column(2018)


def test_a_year_outside_the_window_raises() -> None:
    with pytest.raises(ValueError, match='outside the years'):
        sb.sub_column(2025)


def test_decomposition_row_margin_is_the_column() -> None:
    decomposition = sb.sub_decomposition(2020)
    types = [*sb.SUBSIDY_TYPES, sb.INJECTED_TYPE]

    assert decomposition[types].sum(axis='columns').equals(decomposition['SUB'])
    pd.testing.assert_series_equal(
        decomposition['SUB'].rename('SUB'), sb.sub_column(2020)
    )
