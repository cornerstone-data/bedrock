"""Tests for the wholesale and retail trade margin column (Step 4c, #612/#613).

Mostly real data rather than synthetic, and deliberately so. Every failure this
module is guarding against is a property of the *sources* - a suppressed cell
that parses to zero, an AIES type-of-operation code that returns a well-formed
zero at the wrong sector, a published total row that diverges from the sum of
its own parts. None of those reproduce against a synthetic frame, because the
whole defect is that the real data looks fine.

The crosswalk is checked as real data for the same reason it is on the transport
side: it is a judgement layer, and a typo in it moves a margin silently.
"""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.transform.iot import nowcast_trade_margins as tr

#: The 2017 give-up per kind, $M, off the published Supply table.
GIVE_UP_2017 = {'wholesale': 1_718_990, 'retail': 1_545_941}

#: The 2017 Census gross margin per kind, $M, off the published total row.
CENSUS_2017 = {'wholesale': 1_100_925, 'retail': 1_458_243}

MILLION = 1e6


# --- the crosswalk ---------------------------------------------------------


def test_crosswalk_partitions_every_giver_but_the_unsourced_one() -> None:
    """
    Every giver except ``425000`` is reachable from a Census code, and no code
    reaches two givers.

    A giver missing here would take no share of the annual movement and quietly
    hold its 2017 level; a duplicated Census code would count a kind of business
    into two commodities.
    """
    crosswalk = tr.load_trade_crosswalk()
    mapped = set(crosswalk['bea_2017_commodity'])
    expected = (
        set(tr.GIVER_COMMODITIES['wholesale']) | set(tr.GIVER_COMMODITIES['retail'])
    ) - {tr.UNSOURCED_GIVER}

    assert mapped == expected
    assert not crosswalk['census_code'].duplicated().any()


def test_crosswalk_kinds_do_not_cross() -> None:
    """A wholesale Census code never maps to a retail commodity, or the reverse."""
    crosswalk = tr.load_trade_crosswalk()
    for kind in tr.TRADE_KINDS:
        rows = crosswalk[crosswalk['kind'] == kind]
        assert set(rows['bea_2017_commodity']) <= set(tr.GIVER_COMMODITIES[kind])


# --- the anchor ------------------------------------------------------------


@pytest.mark.eeio_integration
@pytest.mark.parametrize('kind', tr.TRADE_KINDS)
def test_census_gross_margin_reads_the_published_total_row(kind: str) -> None:
    """
    ⚠️ The published row, not the sum of sub-industries.

    This is the single most consequential reading choice in the trade index: on
    the four-digit sum wholesale 2021->2022 reads -6.4% where the published row
    moves +11.4%, a 17.8pp error in one year of the growth factor.
    """
    assert tr.census_gross_margin(kind, 2017) == pytest.approx(
        CENSUS_2017[kind] * MILLION, rel=1e-6
    )


@pytest.mark.eeio_integration
@pytest.mark.parametrize('kind', tr.TRADE_KINDS)
def test_coverage_ratio_is_the_2017_give_up_over_the_census_margin(kind: str) -> None:
    """
    1.561 wholesale, 1.061 retail - and the gap between them is the point.

    Retail's is near 1 because ARTS covers the sector. Wholesale's is not a
    rounding: AWTS is merchant wholesalers only, so 36% of BEA's wholesale margin
    - manufacturers' sales branches, agents and brokers - sits outside the series
    that moves it and this ratio is what carries it.
    """
    assert tr.trade_coverage_ratio(kind) == pytest.approx(
        GIVE_UP_2017[kind] / CENSUS_2017[kind], rel=1e-6
    )


@pytest.mark.eeio_integration
@pytest.mark.parametrize('kind', tr.TRADE_KINDS)
def test_2017_control_total_reproduces_the_published_give_up(kind: str) -> None:
    """The anchor year is an identity, which is what anchor-and-move buys."""
    assert tr.trade_control_total(kind, 2017) == pytest.approx(
        GIVE_UP_2017[kind] * MILLION, rel=1e-6
    )


@pytest.mark.eeio_integration
def test_2017_column_reproduces_the_published_trade_column_per_commodity() -> None:
    """
    ⚠️ The whole construction rests on this, on **both** sides.

    Two earlier versions passed a totals check and failed here: taking the giver
    split from Census put drugs and druggists' sundries 40% low, and weighting
    the receiving side on the gross Wholesale and Retail columns - which are
    gross of the trade-level tax - put a 59% error on the worst commodity. A
    column that nets to zero proves nothing on its own.
    """
    built = tr.trade_margin_column(2017)
    published = tr.published_trade_by_commodity()

    aligned = pd.concat([built, published], axis=1, join='inner')
    aligned.columns = ['built', 'published']
    scale = aligned['published'].abs()
    relative = (aligned['built'] - aligned['published']).abs() / scale.where(scale > 0)

    assert relative.max() < 1e-6


# --- the column identity ---------------------------------------------------


@pytest.mark.eeio_integration
@pytest.mark.parametrize('year', tr.TRADE_MARGIN_YEARS)
def test_column_sums_to_zero_every_year(year: int) -> None:
    """
    Margin is a redistribution, not value created - target T16.

    The receiving side and the give-up side are the same dollars counted twice,
    so this holds in a nowcast year exactly as in the anchor year.
    """
    column = tr.trade_margin_column(year)
    assert abs(column.sum()) / column.abs().sum() < 1e-9


@pytest.mark.eeio_integration
@pytest.mark.parametrize('year', tr.TRADE_MARGIN_YEARS)
def test_all_nineteen_givers_are_present_every_year(year: int) -> None:
    """
    ⚠️ This is the suppression regression.

    Gasoline stations are suppressed in ARTS 2022 and parse to zero; before the
    recovery-by-subtraction they dropped out of the column entirely and their
    5.1% of retail was spread over every other kind of business. Professional
    equipment and drugs do the same on the wholesale side in AWTS 2022.
    """
    column = tr.trade_margin_column(year)
    givers = [*tr.GIVER_COMMODITIES['wholesale'], *tr.GIVER_COMMODITIES['retail']]

    present = column.reindex(givers)
    assert present.notna().all()
    assert (present < 0).all()


@pytest.mark.eeio_integration
@pytest.mark.parametrize('year', tr.TRADE_MARGIN_YEARS)
def test_receivers_are_never_negative(year: int) -> None:
    """
    A commodity that receives trade margin receives a positive amount.

    Unlike the transport side there is no inventory-timing negative here: those
    live on ``F03000`` in the transaction table, which the Supply column
    aggregates away.
    """
    column = tr.trade_margin_column(year)
    givers = {*tr.GIVER_COMMODITIES['wholesale'], *tr.GIVER_COMMODITIES['retail']}
    receivers = column[~column.index.isin(givers)]

    assert (receivers >= 0).all()


# --- suppression -----------------------------------------------------------


@pytest.mark.eeio_integration
def test_suppressed_retail_2022_is_recovered_at_the_measured_gap() -> None:
    """
    ARTS 2022 retail sums 130,671 $M short of its published total.

    That figure was measured independently while writing the plan, off the raw
    workbook, so agreeing with it here is a check of the recovery arithmetic
    rather than a restatement of it.
    """
    detail, suppressed, _ = tr._census_detail('retail', 2022)
    assert suppressed == {'447'}

    published = tr.census_gross_margin('retail', 2022)
    residual = published - detail.drop(index=list(suppressed)).sum()

    assert residual / MILLION == pytest.approx(130_671, rel=1e-4)


@pytest.mark.eeio_integration
def test_suppressed_codes_get_a_plausible_share_not_zero() -> None:
    """
    ⚠️ The failure this guards is silent, not loud.

    Zeroing a suppressed cell leaves a well-formed column that nets to zero and
    has the right total. Only the affected commodity is wrong - and it is wrong
    by all of itself.
    """
    by_giver = tr.census_margin_by_giver('retail', 2022)
    gasoline = by_giver['447000'] / by_giver.sum()

    # 5.1% in 2017; higher in 2022 on fuel prices, but nowhere near zero
    assert 0.03 < gasoline < 0.10


def test_the_unsourced_giver_still_gets_a_share() -> None:
    """
    ``425000`` has no annual Census series and must not vanish because of it.

    Agents and brokers never take title, so AWTS - the merchant-wholesaler table
    - excludes them by construction. They hold the wholesale aggregate's growth.
    """
    for year in (2017, 2022, 2023):
        allocation = tr.giver_allocation('wholesale', year)
        assert allocation[tr.UNSOURCED_GIVER] < 0


# --- the two controls are not interchangeable ------------------------------


@pytest.mark.eeio_integration
def test_gross_margin_control_exceeds_the_trade_control_by_the_trade_tax() -> None:
    """
    ``sum(W + R) = TRADE + TOP``, so the two controls differ by 391,163 $M.

    Mixing them up is a real hazard rather than a pedantic one: applying the
    gross control to the Supply column double-counts the trade-level tax, and
    applying the ``TRADE`` control to the Margins table deletes it.
    """
    gross = sum(tr.gross_margin_control_total(k, 2017) for k in tr.TRADE_KINDS)
    net = sum(tr.trade_control_total(k, 2017) for k in tr.TRADE_KINDS)

    assert (gross - net) / MILLION == pytest.approx(391_163, rel=1e-3)


# --- the guards ------------------------------------------------------------


def test_2024_raises_rather_than_quietly_modelling_a_level() -> None:
    """
    No survey publishes 2024. A modelled level must be asked for explicitly.
    """
    with pytest.raises(ValueError, match='not published'):
        tr.census_gross_margin('wholesale', 2024)

    assert tr.census_gross_margin('wholesale', 2024, allow_extrapolation=True) > 0


def test_an_unknown_kind_raises() -> None:
    """The kind selects source, TYPOP code, total row and giver set at once."""
    with pytest.raises(ValueError, match='kind must be one of'):
        tr.census_gross_margin('wholesale trade', 2017)


# --- the three-way decomposition -------------------------------------------


def test_trade_is_wholesale_plus_retail_less_the_tax() -> None:
    """
    ⚠️ The identity the whole decomposition rests on, per commodity.

    ``TRADE[c] = Wholesale[c] + Retail[c] - trade_level_tax[c]``, straight from
    published data with nothing modelled in it. The tax is the reason the Margins
    table's margin columns (3,656,094 $M) exceed the Supply column (3,264,932).
    """
    gross = tr.published_gross_margin_by_kind()
    tax = tr.trade_level_tax_2017()
    trade = tr.published_trade_by_commodity().reindex(gross.index)

    derived = gross.sum(axis=1) - tax
    assert (derived - trade).abs().max() < 1


def test_the_tax_split_hits_both_observed_column_totals_exactly() -> None:
    """
    Both totals are observed off the give-up side, not assumed.

    A trade commodity's published give-up is its margin net of the tax it
    collected, so wholesale's tax is 1,894,329 - 1,718,990 and retail's is
    1,761,765 - 1,545,941. The tilt is solved by bisection, so this is exact
    rather than converged.

    ⚠️ **Retail is allowed 1 $M where wholesale is allowed none**, and that is
    BEA's rounding rather than ours. Bisection pins the wholesale column, and
    retail is the residual - so retail absorbs the fact that the published
    ``TRADE`` column itself nets to 1 $M rather than to 0. Tightening this would
    be asserting that BEA's table balances more exactly than it does.
    """
    split = tr.trade_level_tax_by_kind_2017()
    gross = tr.published_gross_margin_by_kind()
    published_rounding = {'wholesale': 1.0, 'retail': MILLION}

    for kind in tr.TRADE_KINDS:
        expected = gross[kind].sum() - tr._kind_give_up_2017(kind)
        assert split[kind].sum() == pytest.approx(
            expected, abs=published_rounding[kind]
        )


def test_the_tax_does_not_split_pro_rata() -> None:
    """
    ⚠️ This is the error the first version of the module made.

    Retail carries 55.2% of the trade-level tax on a 48.2% share of the margin -
    sales tax is levied at the counter. Spreading the tax pro rata is what made
    wholesale's receiving side miss its give-up by 17,900 $M, which was then
    misread as proof that the kinds could not be separated at all.
    """
    split = tr.trade_level_tax_by_kind_2017()
    gross = tr.published_gross_margin_by_kind()

    retail_tax_share = split['retail'].sum() / split.sum().sum()
    retail_margin_share = gross['retail'].sum() / gross.sum().sum()

    assert retail_tax_share == pytest.approx(0.552, abs=0.005)
    assert retail_margin_share == pytest.approx(0.482, abs=0.005)
    assert retail_tax_share > retail_margin_share + 0.05


def test_no_commodity_pays_more_tax_than_the_margin_it_is_levied_on() -> None:
    """A margin negative net of its own tax would be nonsense."""
    split = tr.trade_level_tax_by_kind_2017()
    gross = tr.published_gross_margin_by_kind()

    assert (split >= -1).all().all()
    assert (split <= gross + MILLION).all().all()


@pytest.mark.eeio_integration
@pytest.mark.parametrize('year', tr.TRADE_MARGIN_YEARS)
def test_each_kind_sums_to_zero_on_its_own(year: int) -> None:
    """
    A stronger statement than the combined column netting to zero.

    Wholesale's givers and wholesale's receivers are the same dollars, and so are
    retail's. It only holds because the tax is carried as its own term - netting
    it pro rata inside the kinds breaks it.
    """
    components = tr.trade_margin_components(year)

    for kind in tr.TRADE_KINDS:
        column = components[kind]
        assert abs(column.sum()) / column.abs().sum() < 1e-9


@pytest.mark.eeio_integration
def test_components_add_back_to_the_trade_column() -> None:
    """``trade_margin_column`` is exactly the two margin components summed."""
    components = tr.trade_margin_components(2022)
    column = tr.trade_margin_column(2022)

    rebuilt = components[list(tr.TRADE_KINDS)].sum(axis=1)
    assert (rebuilt - column).abs().max() < 1


@pytest.mark.eeio_integration
def test_the_tax_is_positive_everywhere_and_only_on_receivers() -> None:
    """
    Tax is collected on the transaction, so it lands on the good being sold.

    The 19 trade commodities give margin up; they do not receive tax.
    """
    components = tr.trade_margin_components(2017)
    givers = [*tr.GIVER_COMMODITIES['wholesale'], *tr.GIVER_COMMODITIES['retail']]

    assert (components['trade_tax'] >= -1).all()
    assert components.loc[givers, 'trade_tax'].abs().max() < 1


@pytest.mark.eeio_integration
def test_receiving_shares_are_frozen_at_2017() -> None:
    """
    Every year's receiving split is the 2017 mix exactly - and must stay so.

    ⚠️ **This pins an open assumption, not a result.** The level moves annually
    and so does the kind-of-business split, but *which commodities* receive the
    margin is the published 2017 column, because nothing annual observes it -
    BEA's own answer is the product-line method, deferred at #615. That makes
    the frozen mix 100% of the trade commodity detail, so #613 cannot be read as
    "the column is sourced per commodity" until #614 validates it.

    The test exists so the assumption cannot change quietly: swapping the weight
    for ``T013``, or letting an annual source in, breaks it and forces the
    change to be argued rather than absorbed.
    """
    anchor = tr.receiving_allocation(tr.ANCHOR_YEAR)
    anchor_shares = anchor / anchor.sum()

    levels = {}
    for year in tr.TRADE_MARGIN_YEARS:
        received = tr.receiving_allocation(year)
        levels[year] = received.sum()
        assert list(received.index) == list(anchor.index)
        assert (received / received.sum() - anchor_shares).abs().max() < 1e-12

    assert min(levels.values()) < max(
        levels.values()
    ), 'the levels are identical too, so the shares matching proves nothing'
