"""Tests for the taxes-on-products column (Step 4d, #580).

Real data rather than synthetic, for the same reason as the margin modules: every
failure worth guarding against here is a property of the *sources*. A NIPA series
that is renamed still parses, a customs line left in still sums, and a commodity
set too narrow for its tax line still produces a well-formed column - the whole
defect is that the wrong answer looks fine.

The named-line assignment is checked hardest because it is the judgement layer:
it decides which commodity a tax lands on, and a wrong set moves billions
silently while every total still ties.
"""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.transform.iot import nowcast_product_taxes as pt

MILLION = 1e6

#: The published 2017 Supply ``TOP`` column total, $M, and NIPA's own reading of
#: the same quantity. The 1 is BEA's publication rounding, not a residual.
PUBLISHED_TOP_2017 = 716_926
NIPA_TOP_2017 = 716_925

#: NIPA table 3.5, 2017, $M: taxes on products gross of duties, and the customs
#: line that has to come out of it.
NIPA_PRODUCT_TAXES_2017 = 755_438
NIPA_CUSTOMS_DUTIES_2017 = 38_513

#: What the named lines account for in 2017, $M, and the residual left over.
NAMED_TOTAL_2017 = 213_347
RESIDUAL_TOTAL_2017 = 503_578


# --- the annual level, which is observed ------------------------------------


def test_top_control_total_is_nipa_less_customs_duties() -> None:
    """The 2017 total is NIPA's, and the gap to it is exactly the customs line.

    This is the identity the whole column rests on: ``TOP`` is not estimated at
    the aggregate at all. Leaving duties in would put 38,513 $M of import tax
    into the domestic output block, where ``MDTY`` already carries it.
    """
    gross = pt.nipa_line_total(pt.PRODUCT_TAX_SUBTOTALS, pt.ANCHOR_YEAR)
    customs = pt.nipa_line_total([pt.CUSTOMS_DUTIES_SERIES], pt.ANCHOR_YEAR)

    assert gross / MILLION == pytest.approx(NIPA_PRODUCT_TAXES_2017, abs=1)
    assert customs / MILLION == pytest.approx(NIPA_CUSTOMS_DUTIES_2017, abs=1)
    assert pt.top_control_total(pt.ANCHOR_YEAR) == pytest.approx(gross - customs)


def test_control_total_ties_to_the_published_column() -> None:
    """716,925 against a published 716,926 - a $1M agreement on a $717bn column."""
    published = pt.published_top_by_commodity().sum() / MILLION
    control = pt.top_control_total(pt.ANCHOR_YEAR) / MILLION

    assert published == pytest.approx(PUBLISHED_TOP_2017, abs=1)
    assert control == pytest.approx(published, abs=1)


@pytest.mark.parametrize('year', list(pt.TOP_YEARS))
def test_every_year_has_a_control_total(year: int) -> None:
    """NIPA publishes the whole window, so no year of the column is unsourced."""
    assert pt.top_control_total(year) > 0


def test_a_retired_nipa_series_raises_rather_than_summing_to_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A named line that disappears must stop the build.

    Defaulting it to zero would move that tax into the residual and spread it
    over 339 commodities, while the column total still tied to NIPA exactly -
    the failure would be invisible in every aggregate check.
    """
    lines = pt.nipa_product_tax_lines(pt.ANCHOR_YEAR).drop('B2002C')
    monkeypatch.setattr(pt, 'nipa_product_tax_lines', lambda year: lines)

    with pytest.raises(ValueError, match='B2002C'):
        pt.nipa_line_total(['B2002C'], pt.ANCHOR_YEAR)


# --- the named lines, which are the judgement layer -------------------------


def test_named_groups_do_not_share_a_nipa_series() -> None:
    """A series in two groups would be levied twice on two different commodities."""
    seen: list[str] = []
    for series, _ in pt.NAMED_TAX_LINES.values():
        seen.extend(series)

    assert len(seen) == len(set(seen)), sorted(
        code for code in set(seen) if seen.count(code) > 1
    )


def test_named_series_are_all_published_in_the_anchor_year() -> None:
    """Every mapped series exists in NIPA 3.5, so no group is silently empty."""
    published = set(pt.nipa_product_tax_lines(pt.ANCHOR_YEAR).index)
    mapped = {code for series, _ in pt.NAMED_TAX_LINES.values() for code in series}

    assert mapped <= published


def test_named_series_do_not_double_count_a_subtotal() -> None:
    """No group maps a subtotal row, whose children are mapped separately.

    NIPA 3.5 is a hierarchy, and taking a parent alongside its children is the
    classic way to read a table 50% high. The check is arithmetic: the named
    lines together cannot exceed the taxes-on-products total they are drawn from.
    """
    named = sum(
        pt.nipa_line_total(series, pt.ANCHOR_YEAR)
        for series, _ in pt.NAMED_TAX_LINES.values()
    )

    assert named / MILLION == pytest.approx(NAMED_TOTAL_2017, abs=1)
    assert named < pt.top_control_total(pt.ANCHOR_YEAR)


def test_every_named_group_fits_the_tax_its_commodities_carry() -> None:
    """A group larger than its set's 2017 ``TOP`` is not merely odd, it is impossible.

    It would drive the residual negative on those commodities and, because the
    residual is renormalised to the control, invent sales tax everywhere else.
    """
    feasibility = pt.named_line_feasibility()

    assert feasibility['fits'].all(), feasibility[~feasibility['fits']]
    assert feasibility['ratio'].max() <= 1.0


def test_named_line_weights_partition_each_group() -> None:
    """Each group's weights sum to 1 over its own commodities and nowhere else."""
    weights = pt.named_line_weights()

    for group, (_, commodities) in pt.NAMED_TAX_LINES.items():
        column = weights[group]
        assert column.sum() == pytest.approx(1.0)
        assert set(column[column > 0].index) <= set(commodities)


def test_residual_is_non_negative_per_commodity() -> None:
    """The residual carries general sales tax, so it has no negative branch."""
    residual = pt.residual_2017()

    assert (residual >= 0).all()
    assert residual.sum() / MILLION == pytest.approx(RESIDUAL_TOTAL_2017, abs=1)


def test_named_lines_are_about_thirty_percent_of_the_column() -> None:
    """The 30/70 split is the headline claim; a large move in it changes the method."""
    named = pt.named_line_allocation(pt.ANCHOR_YEAR).sum()
    share = named / pt.top_control_total(pt.ANCHOR_YEAR)

    assert share == pytest.approx(0.298, abs=0.005)


# --- the column -------------------------------------------------------------


def test_2017_replays_the_published_column_per_commodity() -> None:
    """Per commodity, not in aggregate.

    The aggregate ties by construction - the residual is scaled to it - so a
    totals check here passes on any commodity split whatsoever. What is being
    checked is that the named lines plus the residual reconstruct the published
    column cell by cell, which is only true because the residual is defined by
    subtraction from that same cell.
    """
    built = pt.top_column(pt.ANCHOR_YEAR)
    published = pt.published_top_by_commodity()
    difference = (built - published).abs()

    assert difference.max() < MILLION, difference.sort_values().tail()


@pytest.mark.parametrize('year', list(pt.TOP_YEARS))
def test_column_is_non_negative_and_ties_to_nipa(year: int) -> None:
    column = pt.top_column(year)

    assert (column >= 0).all()
    assert column.sum() == pytest.approx(pt.top_control_total(year), abs=MILLION)
    assert len(column) == 402


def test_commodities_with_no_product_tax_stay_at_zero() -> None:
    """Zero is sourced information here, not an unfilled cell.

    63 commodities bear no tax on products in 2017; they take no named line and
    no share of the residual, so they stay zero in every year rather than picking
    up a sliver of the growth.
    """
    untaxed = pt.published_top_by_commodity().pipe(lambda top: top[top == 0]).index
    assert len(untaxed) == 63

    for year in (2017, 2020, 2024):
        assert (pt.top_column(year)[untaxed] == 0).all()


def test_decomposition_row_margin_is_the_column() -> None:
    decomposition = pt.top_decomposition(2024)
    parts = [*pt.NAMED_TAX_LINES, 'residual']

    assert decomposition[parts].sum(axis='columns').equals(decomposition['TOP'])
    pd.testing.assert_series_equal(
        decomposition['TOP'].rename('TOP'), pt.top_column(2024)
    )


def test_a_year_outside_the_nipa_window_raises() -> None:
    with pytest.raises(ValueError, match='outside the years'):
        pt.top_column(2025)


# --- the named lines are what beats the default proposal --------------------


def test_named_lines_move_against_the_column_rather_than_with_it() -> None:
    """The whole gain over "2017 shares held constant" is here.

    The default grows every commodity at the column's rate. NIPA measures tobacco
    tax **falling 40%** over a window in which the column rises 42%, air
    transport tax collapsing to a third in 2020, and severance tripling in 2022.
    A change that let these move with the column would pass every total check in
    this file, so the divergence is pinned directly.
    """
    lines = pt.control_total_table()

    def growth(line: str, year: int) -> float:
        series = lines[line].astype(float)
        return float(series[year]) / float(series[pt.ANCHOR_YEAR])

    assert growth('TOP', 2024) == pytest.approx(1.42, abs=0.02)
    assert growth('tobacco', 2024) < 0.65
    assert growth('air transport', 2020) < 0.40
    assert growth('severance', 2022) > 2.5


def test_purchaser_base_reaches_what_domestic_output_cannot() -> None:
    """``S00402`` is why the base is T013+T014 and not ``T007``.

    Used and secondhand goods carry the eighth-largest 2017 ``TOP`` position but
    have **zero** domestic output by definition, so a ``T007`` mover could not
    move them at all. Their purchaser base is large because secondhand goods are
    almost entirely trade margin - which is exactly the layer ``T007`` misses.
    """
    base = pt.purchaser_base(pt.ANCHOR_YEAR)
    assert base['S00402'] > 100_000 * MILLION
    assert (base >= 0).all(), 'a tax base cannot be negative'


def test_residual_does_not_drift_against_its_control() -> None:
    """The rejected one-sided mover reached 1.86x; this base must not drift.

    The residual is renormalised to its control every year, so drift shows up as
    the *base* growing at a different rate from taxes on products. Purchaser
    value is what sales tax is levied on, so the two should track.
    """
    base_2017 = pt.purchaser_base(pt.ANCHOR_YEAR).sum()
    control_2017 = pt.top_control_total(pt.ANCHOR_YEAR)
    for year in range(2018, 2024):
        base_growth = pt.purchaser_base(year).sum() / base_2017
        control_growth = pt.top_control_total(year) / control_2017
        assert 0.90 < base_growth / control_growth < 1.10, (
            f'{year}: base grows {base_growth:.3f} against a control of '
            f'{control_growth:.3f}'
        )


def test_2024_holds_2023_shares_rather_than_reverting_to_2017() -> None:
    """The margin columns stop at 2023, so 2024 is a hold, not a measurement.

    Reverting to the frozen 2017 vector would undo six years of movement in one
    step, which would read as a real 2024 reallocation.
    """
    assert pt.residual_share_for_year(2024).equals(pt.residual_share_for_year(2023))


def test_the_two_constructions_diverge_where_the_named_lines_do() -> None:
    """Sized on commodities, not just lines, against the default proposal itself.

    Tobacco is 26,278 $M lower than the default in 2024 and insurance 17,791 $M
    higher in 2020 - the ACA provider fee, which the default cannot see at all.
    Half the summed absolute difference is 6.3% of the 2020 column and 7.7% of
    the 2024 one, so this is a re-allocation of tens of billions rather than a
    refinement.

    ⚠️ **The divergence grew when the residual started moving.** It was 3.7% and
    5.8% while the residual sat on frozen 2017 shares; sourcing TRADE and TRANS
    (#611) gave the residual a purchaser-price base, so the other 70% of the
    column moves too - see ``residual_share_for_year``. A *fall* here would mean
    the residual had stopped moving.

    ⚠️ **Tobacco was re-fit from 26,153 to 26,278 for #734.** #734 remapped the
    Census NAICS 2022 activities onto the goods Crosswalk, which moves the Trade
    FBS, which moves ``purchaser_base`` and so the residual's annual shares. It
    was not caught before the merge because #734 and #733 were each green on
    their own branch and neither was ever built with the other. The 2017 anchors
    above are unmoved, so this is the shares re-fitting, not the level.
    """
    published = pt.published_top_by_commodity()

    def default(year: int) -> pd.Series:
        return published * (pt.top_control_total(year) / published.sum())

    gap_2024 = pt.top_column(2024) - default(2024)
    gap_2020 = pt.top_column(2020) - default(2020)

    # Grouped rather than asserted one at a time: these are fitted bands, so when
    # a source under ``purchaser_base`` moves them they tend to move together,
    # and a chain of bare asserts reports only the first and hides the rest.
    assert {
        '312200 2024': gap_2024['312200'] / MILLION,
        '5241XX 2020': gap_2020['5241XX'] / MILLION,
    } == pytest.approx({'312200 2024': -26_278, '5241XX 2020': 17_791}, abs=100)
    assert {
        2020: gap_2020.abs().sum() / 2 / pt.top_column(2020).sum(),
        2024: gap_2024.abs().sum() / 2 / pt.top_column(2024).sum(),
    } == pytest.approx({2020: 0.063, 2024: 0.077}, abs=0.003)


# --- the producer-level / trade-level split Step 4c consumes ----------------


def test_trade_level_share_is_a_share() -> None:
    share = pt.trade_level_share()

    assert ((share >= 0) & (share <= 1)).all()
    assert share['315000'] == pytest.approx(1.0, abs=0.01)
    assert share['324110'] == pytest.approx(0.998, abs=0.005)
    assert share['722110'] == 0.0
    assert share['481000'] == 0.0


def test_the_two_levels_add_back_to_the_column() -> None:
    levels = pt.top_by_level(2024)

    # Not ``.equals``: ``producer_level`` is *defined* as ``TOP - trade_level``,
    # so this asks whether ``(a - b) + b == a`` in binary floating point, which
    # is not guaranteed and is luck rather than a property of the split. It held
    # until the shares moved under #734. What the test is actually for is that
    # nothing is lost or double-counted between the two levels.
    rebuilt = levels['producer_level'].add(levels['trade_level'])
    assert rebuilt.to_numpy() == pytest.approx(levels['TOP'].to_numpy(), rel=1e-12)
    assert (levels['producer_level'] >= 0).all()


def test_the_2017_split_reproduces_the_observed_trade_level_tax() -> None:
    """391,162 $M, straight from ``Wholesale + Retail - TRADE`` with nothing modelled."""
    levels = pt.top_by_level(pt.ANCHOR_YEAR)

    assert levels['trade_level'].sum() / MILLION == pytest.approx(391_162, abs=200)
