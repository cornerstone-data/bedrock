"""Margin rates and placement shares for the purchaser -> producer conversion.

The published Margins table (one row per buyer x commodity: Producers' Value,
Transportation, Wholesale, Retail, Purchasers' Value) is two different objects
stacked in one frame, and this module splits them:

* **Goods rows** obey ``PRO + Transportation + Wholesale + Retail = PUR`` per
  line. Normalizing by ``PUR`` gives the *rates* - how each purchaser dollar
  decomposes - which is the year-portable structure (#815).
* **The margin commodities' own rows do not obey that identity, on purpose.**
  A margin commodity's Producers' Value carries the margin routed onto it from
  every other row of that buyer's column; its purchaser cell is the direct
  purchase only. So the block ``PRO - PUR`` over :data:`MARGIN_COMMODITIES` is
  BEA's own **per-buyer seller/mode placement**, published - the object #798's
  sales-composition matrix approximates. Normalized per buyer it is
  :func:`placement_shares`.

Both identities that make this the whole conversion hold at 2012 **and** 2017
(measured, cell for cell): the ``PUR`` column *is* the purchaser SUT Use
interior (<= 1 $M), and the ``PRO`` column - with SUT values where no margins
line exists - *is* the producer-price MUT Use interior (exact). Converting a
year is therefore: apply goods rates, pool each buyer's stripped margin, place
it with the shares.

⚠️ **A raw cell rate is noise where the anchor cell is small.** ``PRO/PUR`` on
a sub-floor denominator produced a $102bn single-cell error in the first 2012
replay. :func:`goods_rates` therefore uses the cell rate only where the anchor
``|PUR|`` clears :data:`CELL_RATE_FLOOR`, falls back to the commodity-level
rate, then to a pass-through (``pro = 1``).

Frozen-2017 structure replayed on the published 2012 SUT (the out-of-anchor
answer key; ``--check`` reproduces these): goods rows land within 2.1% gross of
the 2012 producer interior (worst cell: crude petroleum into refineries,
price-driven); placement shares drift only 0.009 mean absolute, but
mass-weighted placement misses total ~25.0% of margin-row mass (the shares
barely move; the miss is the stripped-mass estimate carrying the rate drift).
Both numbers are the measured cost of freezing, not targets.
"""

from __future__ import annotations

import argparse
import operator
import sys
import typing as ta
from functools import reduce

import numpy as np
import pandas as pd

from bedrock.extract.iot.io_2017 import load_benchmark_margins_before_redef_usa
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.taxonomy.bea.matrix_mappings import (
    USA_BENCHMARK_DETAIL_SUT_YEARS,
)
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_final_demand import USA_2017_FINAL_DEMAND_CODES
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES

#: The commodities whose rows carry routed margin instead of the goods
#: identity: ten wholesale codes, nine retail codes, five transport modes.
#: Derived from the anchor as every commodity with more than $500M routed onto
#: its rows economy-wide; 2012 and 2017 select the SAME set, which is what
#: makes pinning it as a constant safe. ``derive_margin_commodities`` stays the
#: check that this stays true.
MARGIN_COMMODITIES: tuple[str, ...] = (
    # wholesale
    '423100',
    '423400',
    '423600',
    '423800',
    '423A00',
    '424200',
    '424400',
    '424700',
    '424A00',
    '425000',
    # retail
    '441000',
    '444000',
    '445000',
    '446000',
    '447000',
    '448000',
    '452000',
    '454000',
    '4B0000',
    # transport modes
    '481000',
    '482000',
    '483000',
    '484000',
    '486000',
)

#: Routed-mass threshold for :func:`derive_margin_commodities`, USD.
ROUTED_MASS_FLOOR = 500.0 * MILLION_CURRENCY_TO_CURRENCY

#: Anchor purchaser value below which a per-cell rate is publication-rounding
#: noise rather than structure, USD. See the module docstring.
CELL_RATE_FLOOR = 10.0 * MILLION_CURRENCY_TO_CURRENCY

#: Rate components, in the order they decompose a purchaser dollar.
RATE_COMPONENTS: tuple[str, ...] = ('pro', 'trans', 'whl', 'ret')

_VALUE_BY_COMPONENT = {
    'pro': "Producers' Value",
    'trans': 'Transportation',
    'whl': 'Wholesale',
    'ret': 'Retail',
}
_PUR = "Purchasers' Value"

#: Every buyer column of the margins table: 402 industries plus the 20 MUT
#: final-demand codes.
BUYER_CODES: tuple[str, ...] = (
    *USA_2017_INDUSTRY_CODES,
    *USA_2017_FINAL_DEMAND_CODES,
)


class RatePanel(ta.NamedTuple):
    """The year-portable structure of one anchor year's Margins table.

    ``rates[component]`` is goods-commodity x buyer, the four components
    summing to 1 everywhere; ``placement`` is margin-commodity x buyer, each
    buyer's column summing to 1 (or 0 where the buyer strips no margin).
    """

    year: int
    rates: dict[str, pd.DataFrame]
    placement: pd.DataFrame


def goods_commodities() -> list[str]:
    """The commodity rows that obey the goods identity, in taxonomy order."""
    return [c for c in USA_2017_COMMODITY_CODES if c not in MARGIN_COMMODITIES]


def _wide(margins: pd.DataFrame, column: str) -> pd.DataFrame:
    block = margins[column].unstack('Industry Code')
    return block.reindex(
        index=list(USA_2017_COMMODITY_CODES), columns=list(BUYER_CODES)
    )


def derive_margin_commodities(
    year: USA_BENCHMARK_DETAIL_SUT_YEARS = 2017,
) -> tuple[str, ...]:
    """The margin-commodity set as the data selects it. See the constant."""
    margins = load_benchmark_margins_before_redef_usa(year)
    routed = (
        (margins[_VALUE_BY_COMPONENT['pro']] - margins[_PUR])
        .groupby(level='Commodity Code')
        .sum()
    )
    chosen = routed[routed > ROUTED_MASS_FLOOR].index
    return tuple(sorted(c for c in chosen if c in USA_2017_COMMODITY_CODES))


def build_rate_panel(year: USA_BENCHMARK_DETAIL_SUT_YEARS = 2017) -> RatePanel:
    """Rates and placement shares from one published benchmark year. Unitless.

    Goods rates: per (commodity, buyer), cell rate where the anchor cell's
    component sum clears :data:`CELL_RATE_FLOOR`; the commodity-level rate
    (economy-wide sums, same floor) where it does not; pass-through
    (``pro=1``, margins 0) where the commodity has no margins presence at all.
    Every fallback level sums to exactly 1 across components by construction.

    Placement shares: each margin commodity's routed mass ``PRO - PUR`` as a
    share of its buyer's total routed mass; buyers with no routed mass in the
    anchor take the economy-wide shares, so money stripped through fallback
    rates always has somewhere to land. Negative routed cells (margin
    give-backs; a handful exist) are kept - shares still sum to 1.
    """
    margins = load_benchmark_margins_before_redef_usa(year)
    pur = _wide(margins, _PUR)
    goods = goods_commodities()

    # Normalize by the component sum, not by PUR: BEA rounds each column to
    # $1M independently, so PRO+T+W+R misses PUR by up to a few $M on small
    # cells, and dividing by PUR would make the rates sum to != 1.
    components = {
        component: _wide(margins, _VALUE_BY_COMPONENT[component])
        for component in RATE_COMPONENTS
    }
    cell_total = reduce(operator.add, components.values())
    by_commodity = margins.groupby(level='Commodity Code').sum()
    commodity_total = reduce(
        operator.add, (by_commodity[_VALUE_BY_COMPONENT[c]] for c in RATE_COMPONENTS)
    )
    cell_ok = cell_total.abs() >= CELL_RATE_FLOOR
    commodity_ok = commodity_total.abs() >= CELL_RATE_FLOOR

    rates: dict[str, pd.DataFrame] = {}
    for component in RATE_COMPONENTS:
        cell_rate = (components[component] / cell_total).where(cell_ok)
        commodity_rate = (
            by_commodity[_VALUE_BY_COMPONENT[component]] / commodity_total
        ).where(commodity_ok)
        commodity_rate = commodity_rate.reindex(list(USA_2017_COMMODITY_CODES))
        fill = 1.0 if component == 'pro' else 0.0
        rate = (
            cell_rate.apply(lambda col: col.fillna(commodity_rate))
            .fillna(fill)
            .loc[goods]
        )
        rates[component] = rate

    pro = components['pro']
    routed = (pro - pur).loc[list(MARGIN_COMMODITIES)].fillna(0.0)
    mass = routed.sum(axis=0)
    placement = routed.div(mass.where(mass != 0.0), axis=1)
    # A buyer with no anchor margins rows still strips margin through the
    # commodity-fallback rates; zero shares would silently drop that money,
    # so such buyers place with the economy-wide shares.
    economy = routed.sum(axis=1)
    economy = economy / economy.sum()
    placement = placement.apply(lambda col: col.fillna(economy))
    return RatePanel(year=int(year), rates=rates, placement=placement)


def producer_goods_interior(
    use_purchaser: pd.DataFrame, panel: RatePanel
) -> pd.DataFrame:
    """Goods rows of the producer-price Use block: ``pro`` rates applied.

    *use_purchaser* is commodity x buyer in any consistent unit; the result is
    in the same unit, on the goods rows only. Margin rows are placement
    (:func:`placed_margin_rows`), not rates - see the module docstring.
    """
    goods = goods_commodities()
    aligned = use_purchaser.reindex(
        index=goods, columns=panel.rates['pro'].columns
    ).fillna(0.0)
    return panel.rates['pro'] * aligned


def placed_margin_rows(use_purchaser: pd.DataFrame, panel: RatePanel) -> pd.DataFrame:
    """Margin-commodity rows of the producer-price Use block.

    Each buyer's stripped margin - the mass the goods rates removed - is
    pooled and placed across :data:`MARGIN_COMMODITIES` by the anchor's
    placement shares, on top of the buyer's direct purchases of those
    services (which cross at purchaser value; margins on margins are ~0).
    """
    goods = goods_commodities()
    aligned = use_purchaser.reindex(
        index=list(USA_2017_COMMODITY_CODES), columns=panel.placement.columns
    ).fillna(0.0)
    stripped = ((1.0 - panel.rates['pro']) * aligned.loc[goods]).sum(axis=0)
    return panel.placement.mul(stripped, axis=1) + aligned.loc[list(MARGIN_COMMODITIES)]


# --- the replay ------------------------------------------------------------


def check(argv: list[str] | None = None) -> int:
    """Replay the panel against the published benchmarks and print the cost.

    ``--year 2017`` is the plumbing check (the anchor replays itself);
    ``--year 2012`` with the default 2017 anchor is the real one: five years
    of structure drift, measured against a published answer key.
    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--anchor', type=int, default=2017)
    parser.add_argument('--year', type=int, default=2012)
    args = parser.parse_args(argv)

    from bedrock.extract.iot.io_2017 import (  # noqa: PLC0415 - heavy loaders
        _load_benchmark_detail_supply_use_usa,
    )
    from bedrock.transform.iot.sut_use_to_mut_use import (  # noqa: PLC0415
        published_mut_use,
    )

    panel = build_rate_panel(args.anchor)
    industries = list(USA_2017_INDUSTRY_CODES)
    sut = (
        _load_benchmark_detail_supply_use_usa('Use_SUT_detail', args.year)
        .loc[list(USA_2017_COMMODITY_CODES), industries]
        .apply(pd.to_numeric, errors='coerce')
        .fillna(0.0)
    )
    mut = (
        published_mut_use(args.year).loc[list(USA_2017_COMMODITY_CODES), industries]
        / MILLION_CURRENCY_TO_CURRENCY
    )

    goods = goods_commodities()
    built = producer_goods_interior(sut, panel)[industries]
    reference = mut.loc[goods]
    gap = (built - reference).abs()
    outside = ~np.isclose(built.to_numpy(), reference.to_numpy(), rtol=0.01, atol=0.5)
    print(
        f'goods rows, {args.anchor} rates on the {args.year} SUT: '
        f'{outside.sum()} of {outside.size} cells outside 1%/0.5$M, '
        f'gross {gap.sum().sum():,.0f} $M '
        f'({100 * gap.sum().sum() / reference.abs().sum().sum():.2f}% of the '
        f'goods interior), worst cell {gap.max().max():,.0f} $M'
    )

    placed = placed_margin_rows(sut, panel)[industries]
    reference_margin = mut.loc[list(MARGIN_COMMODITIES)]
    gap_margin = (placed - reference_margin).abs()
    print(
        f'margin rows, {args.anchor} placement on the {args.year} SUT: '
        f'gross {gap_margin.sum().sum():,.0f} $M '
        f'({100 * gap_margin.sum().sum() / reference_margin.abs().sum().sum():.2f}% '
        f'of margin-row mass), worst cell {gap_margin.max().max():,.0f} $M'
    )
    return 0


if __name__ == '__main__':
    sys.exit(check())
