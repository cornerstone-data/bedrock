"""The per-cell price bridge from basic to purchaser value.

One row per ``(buyer, commodity)``, and one column per step of the bridge::

    Basic | producer taxes | duties | subsidies | Producers' Value
          | <one column per margin commodity> | sales tax | Purchasers' Value

with two row identities that close cell by cell (subsidies stored negative)::

    Basic + producer taxes + duties + subsidies       = Producers' Value
    Producers' Value + margin columns + sales tax     = Purchasers' Value

⚠️ **The margin columns are tax-free.** BEA embeds the sales tax its trade
margins collect inside ``Wholesale``/``Retail``; here that money is the
``sales tax`` column instead, and the Supply bridge is the authority on the
partition. BEA's five-column layout is therefore a *derived view* -
:func:`collapse_to_bea` re-embeds the tax and reproduces the published table.

The margins detail comes from ``margin_rates.build_rate_panel`` and the fiscal
columns from ``tax_subsidy_layer.build_fiscal_layer``; this module places and
assembles, and owns no rate structure of its own.
"""

from __future__ import annotations

import argparse
import sys
import typing as ta

import pandas as pd

from bedrock.extract.iot.io_2017 import load_benchmark_margins_before_redef_usa
from bedrock.transform.iot.margin_rates import (
    BUYER_CODES,
    MARGIN_COMMODITIES,
    RatePanel,
    build_rate_panel,
    goods_commodities,
)
from bedrock.transform.iot.sut_use_to_mut_use import ReplayReport, score_replay
from bedrock.transform.iot.tax_subsidy_layer import FiscalLayer, build_fiscal_layer
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.taxonomy.bea.matrix_mappings import USA_BENCHMARK_DETAIL_SUT_YEARS

# --- the deliverable -------------------------------------------------------


def price_bridge(
    year: USA_BENCHMARK_DETAIL_SUT_YEARS,
    use_purchaser: pd.DataFrame,
    panel: RatePanel | None = None,
    fiscal: FiscalLayer | None = None,
) -> pd.DataFrame:
    """The layered bridge for *year*, one row per transaction. USD.

    Index ``(Industry Code, Commodity Code)``, columns :data:`BRIDGE_COLUMNS`.
    Rows carrying nothing on any column are left out.

    :param use_purchaser: purchaser-price Use, commodity x buyer, USD - the
        transactions' ``Purchasers' Value``.
    """
    rates = panel if panel is not None else build_rate_panel(year)
    layer = fiscal if fiscal is not None else build_fiscal_layer(year)
    purchaser = _goods_grid(use_purchaser)

    layer = _restrict_fiscal(layer, purchaser)
    producer = rates.rates['pro'] * purchaser
    margins = margin_columns(purchaser, rates, layer)
    sales_tax = layer.sales_tax

    columns: dict[str, 'pd.Series[float]'] = {}
    for name, frame in (
        (PRODUCER_TAXES, layer.top_rest),
        (DUTIES, layer.duties),
        (SUBSIDIES, layer.subsidies),
    ):
        columns[name] = _flatten(frame)
    columns[PRODUCERS_VALUE] = _flatten(producer)
    columns[BASIC] = columns[PRODUCERS_VALUE] - sum(
        columns[name] for name in FISCAL_COLUMNS
    )
    for commodity in MARGIN_COMMODITIES:
        columns[commodity] = _flatten(margins[commodity])
    columns[SALES_TAX] = _flatten(sales_tax)
    columns[PURCHASERS_VALUE] = _flatten(purchaser)

    frame = pd.DataFrame(columns)[list(BRIDGE_COLUMNS)]
    return frame.loc[frame.abs().sum(axis=1) > 0].sort_index()


def margin_columns(
    purchaser: pd.DataFrame, panel: RatePanel, fiscal: FiscalLayer | None = None
) -> dict[str, pd.DataFrame]:
    """Margin per transaction, one goods x buyer frame per margin commodity. USD.

    Each cell's transport, wholesale and retail margin comes from the rate
    panel, then each family's amount is spread over *its own* codes by
    :func:`family_placement`.

    Pass *fiscal* for the contract's **tax-free** columns - the sales tax is
    lifted out of the trade pair before placing. Leave it off for BEA's own
    tax-inclusive definition, which is what the published margin rows carry and
    therefore what :func:`margin_row_recovery` has to grade against.

    ⚠️ **The families do not mix, and that is measured rather than assumed.**
    Per buyer the anchor already partitions three ways - wholesale margin paid
    equals what the ten wholesale codes earn, retail the nine, transport the
    five - to 862 / 274 / 1,113 $M gross over 422 buyers at 2017 and the same
    at 2012, worst buyer 12 $M. ``placed_margin_rows`` pools all 24 into one
    vector, which is right for a row total and cannot give per-family columns
    at all: it would book transport margin onto wholesale codes. Splitting
    also grades better out of anchor - 735,154 $M against 769,463 $M replaying
    2012 on the 2017 panel - but correctness is the reason, not the 4%.
    """
    shares = family_placement(panel)
    amounts = {
        family: panel.rates[component] * purchaser
        for family, component in RATE_COMPONENT_BY_FAMILY.items()
    }
    if fiscal is not None:
        amounts.update(_strip_sales_tax(amounts, fiscal, purchaser))

    out: dict[str, pd.DataFrame] = {}
    for family, commodities in MARGIN_FAMILIES.items():
        amount = amounts[family]
        for commodity in commodities:
            out[commodity] = amount.mul(shares[family].loc[commodity], axis=1)
    return out


def family_placement(panel: RatePanel) -> dict[str, pd.DataFrame]:
    """The panel's placement shares renormalised within each margin family.

    One frame per :data:`MARGIN_FAMILIES` entry, its own codes x buyer, each
    buyer's column summing to 1. A buyer with no anchor mass in a family takes
    that family's economy-wide mix, so no stripped margin is silently dropped.
    """
    out: dict[str, pd.DataFrame] = {}
    for family, commodities in MARGIN_FAMILIES.items():
        block = panel.placement.reindex(list(commodities)).fillna(0.0)
        total = block.sum(axis=0)
        economy = block.sum(axis=1)
        economy = economy / economy.sum()
        share = block.div(total.where(total != 0.0), axis=1)
        out[family] = share.apply(lambda column: column.fillna(economy))
    return out


def collapse_to_bea(bridge: pd.DataFrame) -> pd.DataFrame:
    """BEA's five-column Margins layout, derived from *bridge*. USD.

    The published table embeds the sales tax in its trade columns, so the
    collapse re-embeds it - split between ``Wholesale`` and ``Retail`` in
    proportion to each cell's own tax-free trade margin. This is the view that
    replays against the published table, and the only place the tax goes back
    inside a margin column.
    """
    wholesale = bridge[list(MARGIN_FAMILIES['wholesale'])].sum(axis=1)
    retail = bridge[list(MARGIN_FAMILIES['retail'])].sum(axis=1)
    trade = wholesale + retail
    share = (wholesale / trade.where(trade != 0.0)).fillna(1.0)
    tax = bridge[SALES_TAX]

    return pd.DataFrame(
        {
            "Producers' Value": bridge[PRODUCERS_VALUE],
            'Transportation': bridge[list(MARGIN_FAMILIES['transport'])].sum(axis=1),
            'Wholesale': wholesale + tax * share,
            'Retail': retail + tax * (1.0 - share),
            "Purchasers' Value": bridge[PURCHASERS_VALUE],
        }
    )


#: The bridge's own columns, in the order they step from basic to purchaser.
BASIC = 'Basic Value'
PRODUCER_TAXES = 'Taxes on products'
DUTIES = 'Import duties'
SUBSIDIES = 'Subsidies'
PRODUCERS_VALUE = "Producers' Value"
SALES_TAX = 'Sales tax'
PURCHASERS_VALUE = "Purchasers' Value"

#: The three wedges between basic and producers' value. Subsidies are stored
#: negative, the balance convention.
FISCAL_COLUMNS: tuple[str, ...] = (PRODUCER_TAXES, DUTIES, SUBSIDIES)

BRIDGE_COLUMNS: tuple[str, ...] = (
    BASIC,
    *FISCAL_COLUMNS,
    PRODUCERS_VALUE,
    *MARGIN_COMMODITIES,
    SALES_TAX,
    PURCHASERS_VALUE,
)

#: Which margin commodities earn which margin. ``MARGIN_COMMODITIES`` is
#: written in this order - ten wholesale codes, nine retail, five transport
#: modes - and the split is checked against that rather than assumed.
MARGIN_FAMILIES: dict[str, tuple[str, ...]] = {
    'wholesale': MARGIN_COMMODITIES[:10],
    'retail': MARGIN_COMMODITIES[10:19],
    'transport': MARGIN_COMMODITIES[19:],
}

#: The rate component each family is paid out of.
RATE_COMPONENT_BY_FAMILY: dict[str, str] = {
    'wholesale': 'whl',
    'retail': 'ret',
    'transport': 'trans',
}


# --- validation ------------------------------------------------------------


def row_identities(bridge: pd.DataFrame) -> pd.DataFrame:
    """The two row identities' residuals per transaction, USD.

    Exact arithmetic on both, so anything past rounding is a construction bug
    rather than a modelling choice.
    """
    margins = bridge[list(MARGIN_COMMODITIES)].sum(axis=1)
    return pd.DataFrame(
        {
            'basic_to_producer': bridge[BASIC]
            + bridge[list(FISCAL_COLUMNS)].sum(axis=1)
            - bridge[PRODUCERS_VALUE],
            'producer_to_purchaser': bridge[PRODUCERS_VALUE]
            + margins
            + bridge[SALES_TAX]
            - bridge[PURCHASERS_VALUE],
        }
    )


def column_identities(
    bridge: pd.DataFrame, year: USA_BENCHMARK_DETAIL_SUT_YEARS
) -> pd.DataFrame:
    """Per commodity, each layer's column sum against the published Supply bridge.

    ⚠️ **Per commodity, never in aggregate** - the margin columns net to about
    nothing economy-wide, so a total passes on broken data.

    The fiscal layer has no per-cell answer key at all; this column check and
    the 2012 collapse are the only things grading it.
    """
    from bedrock.transform.iot.tax_subsidy_layer import _bridge  # noqa: PLC0415

    published = _bridge(year)
    by_commodity = bridge.groupby(level='Commodity Code').sum()
    ours = pd.DataFrame(
        {
            'TRADE': by_commodity[
                [*MARGIN_FAMILIES['wholesale'], *MARGIN_FAMILIES['retail']]
            ].sum(axis=1),
            'TRANS': by_commodity[list(MARGIN_FAMILIES['transport'])].sum(axis=1),
            'TOP': by_commodity[SALES_TAX] + by_commodity[PRODUCER_TAXES],
            'MDTY': by_commodity[DUTIES],
            'SUB': by_commodity[SUBSIDIES],
        }
    )
    return ours - published.reindex(ours.index).fillna(0.0)


def score_collapse(
    bridge: pd.DataFrame, year: USA_BENCHMARK_DETAIL_SUT_YEARS
) -> dict[str, ReplayReport]:
    """The five-column view against the published Margins table, per column.

    The answer-keyed half of the contract: ``Producers' Value`` and the margin
    columns are published per cell at both benchmark years.

    ⚠️ **Goods rows only.** In this layout the margin commodities are the
    *columns*; BEA carries them as rows too, and what those rows hold - margin
    routed onto them from the whole of their buyer's column - is graded by
    :func:`margin_row_recovery` instead. Scoring against the whole published
    table would compare goods rows to a frame carrying both and report tens of
    trillions of nothing.
    """
    answer = load_benchmark_margins_before_redef_usa(year)
    goods = goods_commodities()
    answer = answer.loc[answer.index.get_level_values('Commodity Code').isin(goods)]
    collapsed = collapse_to_bea(bridge)
    return {
        column: score_replay(
            collapsed[column].unstack('Industry Code'),
            answer[column].unstack('Industry Code'),
        )
        for column in collapsed.columns
    }


def margin_row_recovery(
    year: USA_BENCHMARK_DETAIL_SUT_YEARS,
    use_purchaser: pd.DataFrame | None = None,
    panel: RatePanel | None = None,
) -> ReplayReport:
    """Each margin commodity's column rebuilt as its row in the producer Use table.

    The contract's tie between this table and the 6b conversion: what a margin
    commodity earns from a buyer is that buyer's cells in its column, and its
    published ``Producers' Value`` row is that plus the buyer's direct purchase
    of it. Grading this grades the placement, which is the only part of the
    split with a published per-cell answer at both years.
    """
    answer = load_benchmark_margins_before_redef_usa(year)
    purchaser = _goods_grid(
        answer[PURCHASERS_VALUE].unstack('Industry Code')
        if use_purchaser is None
        else use_purchaser
    )
    # ⚠️ Tax-inclusive placement, not the bridge's own columns. BEA's margin
    # rows carry the sales tax those commodities collected; grading the
    # tax-free columns against them misses by the whole wedge - 391,474 $M at
    # 2017 against a published wedge of 391,163 $M, which is the wedge and not
    # a placement error.
    inclusive = margin_columns(
        purchaser, panel if panel is not None else build_rate_panel(year)
    )
    earned = (
        pd.DataFrame(
            {commodity: frame.sum(axis=0) for commodity, frame in inclusive.items()}
        )
        .T.reindex(index=list(MARGIN_COMMODITIES), columns=list(BUYER_CODES))
        .fillna(0.0)
    )
    direct = (
        answer[PURCHASERS_VALUE]
        .unstack('Industry Code')
        .reindex(index=list(MARGIN_COMMODITIES), columns=list(BUYER_CODES))
        .fillna(0.0)
    )
    published = (
        answer[PRODUCERS_VALUE]
        .unstack('Industry Code')
        .reindex(index=list(MARGIN_COMMODITIES), columns=list(BUYER_CODES))
        .fillna(0.0)
    )
    return score_replay(earned + direct, published)


# --- report / check --------------------------------------------------------


def report() -> None:
    """Build the bridge at both benchmark years and grade it."""
    million = MILLION_CURRENCY_TO_CURRENCY
    for year, anchor in REPLAYS:
        bridge = benchmark_bridge(year, anchor)
        label = f'{year}' if anchor is None else f'{year} on the {anchor} anchor'
        print(f'\nprice bridge, {label}: {len(bridge):,} rows')

        residual = row_identities(bridge)
        for name in residual.columns:
            worst = float(residual[name].abs().max())
            print(f'  identity {name:<22} worst cell {worst:>12,.2f} USD')

        print('  collapse to the published five columns:')
        for column, score in score_collapse(bridge, year).items():
            print(
                f'    {column:<18} {score.n_outside:>7,} cells outside, '
                f'{score.gross / million:>10,.0f} $M gross'
            )

        recovery = margin_row_recovery(
            year, panel=build_rate_panel(anchor) if anchor else None
        )
        print(
            f'  margin rows recovered from their columns: '
            f'{recovery.n_outside:,} cells outside, '
            f'{recovery.gross / million:,.0f} $M gross'
        )

        gaps = column_identities(bridge, year)
        print('  column sums against the Supply bridge, per commodity:')
        for name in gaps.columns:
            print(
                f'    {name:<6} {gaps[name].abs().sum() / million:>10,.0f} $M gross, '
                f'worst commodity {gaps[name].abs().max() / million:>8,.0f} $M'
            )


def check() -> int:
    """Assert the contract's identities and the collapse grade. Exit code."""
    million = MILLION_CURRENCY_TO_CURRENCY
    failures: list[str] = []

    for year, anchor in REPLAYS:
        bridge = benchmark_bridge(year, anchor)

        worst = float(row_identities(bridge).abs().to_numpy().max())
        if worst > ROW_IDENTITY_TOLERANCE:
            failures.append(
                f'{year} misses a row identity by {worst:,.2f} USD on its worst '
                f'cell. Both are exact arithmetic, so this is a construction '
                f'bug rather than a modelling choice.'
            )

        recovery = margin_row_recovery(
            year, panel=build_rate_panel(anchor) if anchor else None
        )
        recovery_ceiling = 6_000 if anchor is None else 800_000
        if recovery.gross / million > recovery_ceiling:
            failures.append(
                f'{year} recovers the margin rows to '
                f'{recovery.gross / million:,.0f} $M, past its '
                f'{recovery_ceiling:,} $M ceiling. That is the placement, and '
                f'it is the only part of the split with a per-cell key.'
            )

        ceilings = COLLAPSE_CEILING if anchor is None else FROZEN_COLLAPSE_CEILING
        scores = score_collapse(bridge, year)
        for column, ceiling in ceilings.items():
            gross = scores[column].gross / million
            if gross > ceiling:
                failures.append(
                    f'{year} {column} collapses {gross:,.0f} $M from the '
                    f'published table, past its {ceiling:,} $M ceiling.'
                )

    if failures:
        print('FAILED:')
        for failure in failures:
            print(f'  - {failure}')
        return 1
    print('OK: both row identities close and the collapse holds at 2012 and 2017')
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--check',
        action='store_true',
        help='assert the identities and the collapse grade instead of reporting',
    )
    args = parser.parse_args()
    if args.check:
        return check()
    report()
    return 0


#: The replays, as ``(year, anchor)``. A year on its own structure cannot
#: fail; ``(2012, 2017)`` is the only one that grades the freeze, and it is the
#: number to move.
REPLAYS: tuple[
    tuple[USA_BENCHMARK_DETAIL_SUT_YEARS, USA_BENCHMARK_DETAIL_SUT_YEARS | None], ...
] = (
    (2012, None),
    (2017, None),
    (2012, 2017),
)

#: Both row identities are exact arithmetic; this is float noise, not slack.
ROW_IDENTITY_TOLERANCE = 1.0

#: How much of a fiscal wedge may land on a commodity the purchaser table has
#: no live cell for before it counts as lost rather than rounded.
UNREACHABLE_FISCAL_TOLERANCE: float = 100 * MILLION_CURRENCY_TO_CURRENCY

#: How far each collapsed column may sit from the published table on a
#: same-year replay, $M. Measured; the residual is the rate panel's cell-floor
#: fallback, not this module's placement.
COLLAPSE_CEILING: dict[str, int] = {
    "Producers' Value": 12_000,
    'Transportation': 1_500,
    'Wholesale': 4_500,
    'Retail': 3_500,
    "Purchasers' Value": 1,
}

#: The same, replaying 2012 on the 2017 anchor - the cost of freezing the
#: structure, and the only grade here that can actually move.
FROZEN_COLLAPSE_CEILING: dict[str, int] = {
    "Producers' Value": 700_000,
    'Transportation': 130_000,
    'Wholesale': 420_000,
    'Retail': 300_000,
    "Purchasers' Value": 1,
}


def benchmark_bridge(
    year: USA_BENCHMARK_DETAIL_SUT_YEARS,
    anchor: USA_BENCHMARK_DETAIL_SUT_YEARS | None = None,
) -> pd.DataFrame:
    """The bridge at a benchmark year, from its own published purchaser cells.

    The purchaser side comes from the published Margins table rather than the
    SUT workbook - ``margin_rates`` measures the two as the same object to
    1 $M - so the replay grades the split rather than a loader.

    Pass *anchor* to freeze the rate panel on a different year. ``year=2012,
    anchor=2017`` is the **out-of-anchor** grade: everything else in the
    module replays a year against its own structure, which cannot fail. Only
    the rate panel freezes; the fiscal layer stays on *year*, since it has no
    per-cell answer key at either year and ``top_sales_share`` is the frozen
    handle for a nowcast year.
    """
    margins = load_benchmark_margins_before_redef_usa(year)
    purchaser = margins[PURCHASERS_VALUE].unstack('Industry Code')
    panel = build_rate_panel(anchor) if anchor is not None else None
    return price_bridge(year, purchaser, panel=panel)


def _goods_grid(use_purchaser: pd.DataFrame) -> pd.DataFrame:
    """*use_purchaser* on the goods rows and every buyer column, zero-filled."""
    grid = use_purchaser.reindex(
        index=goods_commodities(), columns=list(BUYER_CODES)
    ).fillna(0.0)
    grid.index.name = 'Commodity Code'
    grid.columns.name = 'Industry Code'
    return grid.astype(float)


def _restrict_fiscal(layer: FiscalLayer, purchaser: pd.DataFrame) -> FiscalLayer:
    """*layer* re-spread onto the cells *purchaser* actually has. USD.

    ⚠️ **``build_fiscal_layer`` allocates on the anchor's cell pattern**, so a
    Use table with a different one - any nowcast year, or a subset - would book
    tax on cells that do not exist and miss the ones that do. Rescaling within
    each commodity keeps the column identity against the Supply bridge, which
    is the only thing grading this layer.
    """
    frames = {}
    live = purchaser != 0.0
    for name in ('sales_tax', 'top_rest', 'duties', 'subsidies'):
        frame = getattr(layer, name).reindex_like(purchaser).fillna(0.0)
        kept = frame.where(live, 0.0)
        before, after = frame.sum(axis=1), kept.sum(axis=1)
        stranded = float((before - after)[after == 0.0].abs().sum())
        assert stranded < UNREACHABLE_FISCAL_TOLERANCE, (
            f'{stranded / MILLION_CURRENCY_TO_CURRENCY:,.0f} $M of {name} sits '
            f'on commodities the purchaser table has no cell for, so it cannot '
            f'be re-spread and would vanish.'
        )
        frames[name] = kept.mul(
            (before / after.where(after != 0.0)).fillna(0.0), axis=0
        )
    return layer._replace(**frames)


def _strip_sales_tax(
    amounts: dict[str, pd.DataFrame], fiscal: FiscalLayer, purchaser: pd.DataFrame
) -> dict[str, pd.DataFrame]:
    """Wholesale and retail net of the sales tax they collect.

    The tax comes out of the pair in proportion to each cell's own wholesale
    and retail margin, so a cell with only retail margin gives it all up there.
    """
    tax = fiscal.sales_tax.reindex_like(purchaser).fillna(0.0)
    trade = amounts['wholesale'] + amounts['retail']
    share = (amounts['wholesale'] / trade.where(trade != 0.0)).fillna(1.0)
    return {
        'wholesale': amounts['wholesale'] - tax * share,
        'retail': amounts['retail'] - tax * (1.0 - share),
    }


def _flatten(frame: pd.DataFrame) -> 'pd.Series[float]':
    """A commodity x buyer frame as BEA's ``(Industry Code, Commodity Code)`` series."""
    return ta.cast('pd.Series[float]', frame.stack().swaplevel())


if __name__ == '__main__':
    sys.exit(main())
