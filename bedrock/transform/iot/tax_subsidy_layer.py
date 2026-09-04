"""The per-cell tax and subsidy layer of the Use table (#815 part 2, for #584).

BEA publishes the fiscal wedges only as commodity totals - the Supply bridge's
``TOP``/``MDTY``/``SUB`` columns - while the layered Margins product #584
specifies needs them per (buyer, commodity). This module allocates them, on a
partition the two published tables pin down jointly:

* **Transportation carries no tax**: the Margins table's goods-row
  Transportation sums match the bridge's ``TRANS`` to rounding (-$1.0bn on
  $415bn, both benchmark years).
* **The Wholesale/Retail margin columns carry a tax wedge**: their goods-row
  sums exceed the bridge's ``TRADE`` by $391.2bn (2017) / $322.6bn (2012) -
  the sales and excise taxes BEA books inside margins but inside ``TOP`` on
  the bridge (refined petroleum's motor-fuel taxes are the largest single
  piece). The same wedge reappears as the gap between the margin commodities'
  routed mass and the bridge's negative offsets (agreement to 0.15%), and it
  is bounded by ``0 <= wedge <= TOP`` for every commodity in both years.

So each goods commodity's ``TOP`` splits into a **margin-collected part** (the
wedge, allocated across buyers by their retail-margin dollars - retail first,
wholesale where a commodity has a wedge but no retail base) and a
**producer-level part** (the rest, allocated by producers'-value use, like
``MDTY`` and ``SUB``). The retail base gives the export exception for free:
``F04000`` buys no retail margin, so it gets exactly $0 of sales tax without a
hardcoded rule.

⚠️ **Where the layers sit in the price build-up.** The producer-level taxes
come *before* producers' value; the margin-collected taxes come *after* it,
inside the published Wholesale/Retail margins:

    basic + top_rest + duties + subsidies = producers' value
    producers' value + tax-free margins + transport + sales_tax = purchasers' value

Column sums reproduce the bridge by construction: ``sales_tax + top_rest``
sums to ``TOP`` per commodity, ``duties`` to ``MDTY``, ``subsidies`` to
``SUB``.

⚠️ **No published per-cell answer key exists for this layer.** The wedge and
its bases are observed; the per-cell split of the producer-level wedges is
proportional allocation. It is graded through column sums and through #584's
collapse check, never cell by cell.
"""

from __future__ import annotations

import argparse
import sys
import typing as ta

import pandas as pd

from bedrock.extract.iot.io_2017 import (
    _load_benchmark_detail_supply_use_usa,
    load_benchmark_margins_before_redef_usa,
)
from bedrock.transform.iot.margin_rates import BUYER_CODES, goods_commodities
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.taxonomy.bea.matrix_mappings import (
    USA_BENCHMARK_DETAIL_SUT_YEARS,
)

_PRO = "Producers' Value"
_TRANS = 'Transportation'
_WHL = 'Wholesale'
_RET = 'Retail'
_PUR = "Purchasers' Value"


class FiscalLayer(ta.NamedTuple):
    """Per-cell allocations of one benchmark year's fiscal wedges, USD.

    Each frame is goods-commodity x buyer. ``sales_tax`` is the
    margin-collected part of ``TOP`` (post-producers'-value); ``top_rest``,
    ``duties`` and ``subsidies`` are producer-level (``subsidies`` is
    negative, balance convention).
    """

    year: int
    sales_tax: pd.DataFrame
    top_rest: pd.DataFrame
    duties: pd.DataFrame
    subsidies: pd.DataFrame


def _goods_wide(margins: pd.DataFrame, column: str) -> pd.DataFrame:
    block = margins[column].unstack('Industry Code')
    return block.reindex(index=goods_commodities(), columns=list(BUYER_CODES)).fillna(
        0.0
    )


def _bridge(year: USA_BENCHMARK_DETAIL_SUT_YEARS) -> pd.DataFrame:
    supply = _load_benchmark_detail_supply_use_usa('Supply_detail', year)
    supply = supply.rename(columns=str.strip)
    return (
        supply[['TRADE', 'TRANS', 'TOP', 'MDTY', 'SUB']]
        .apply(pd.to_numeric, errors='coerce')
        .reindex(goods_commodities())
        .fillna(0.0)
        * MILLION_CURRENCY_TO_CURRENCY
    )


def sales_tax_wedge(year: USA_BENCHMARK_DETAIL_SUT_YEARS) -> pd.Series:
    """The margin-collected part of ``TOP`` per goods commodity, USD.

    ``sum(Wholesale + Retail) - TRADE``, clipped to ``[0, TOP]`` - the clip
    never binds beyond rounding in 2012 or 2017 (measured), it is a guard.
    """
    margins = load_benchmark_margins_before_redef_usa(year)
    whl_ret = _goods_wide(margins, _WHL) + _goods_wide(margins, _RET)
    bridge = _bridge(year)
    wedge = whl_ret.sum(axis=1) - bridge['TRADE']
    return wedge.clip(lower=0.0, upper=bridge['TOP'].clip(lower=0.0))


def top_sales_share(year: USA_BENCHMARK_DETAIL_SUT_YEARS = 2017) -> pd.Series:
    """``sales_tax_wedge / TOP`` per commodity - the anchor ratio a nowcast
    year freezes, since its own margins table does not exist. 0 where TOP is."""
    bridge = _bridge(year)
    top = bridge['TOP']
    return (sales_tax_wedge(year) / top.where(top != 0.0)).fillna(0.0)


def _shares(base: pd.DataFrame) -> pd.DataFrame:
    total = base.sum(axis=1)
    return base.div(total.where(total != 0.0), axis=0).fillna(0.0)


def build_fiscal_layer(
    year: USA_BENCHMARK_DETAIL_SUT_YEARS = 2017,
) -> FiscalLayer:
    """Allocate the year's published wedges per cell. See the module docstring.

    Bases: the sales-tax wedge follows retail-margin dollars (wholesale
    dollars where a commodity has a wedge but no retail base - those are
    wholesale-collected excises); ``top_rest``/``duties``/``subsidies``
    follow producers'-value use.
    """
    margins = load_benchmark_margins_before_redef_usa(year)
    bridge = _bridge(year)

    retail = _goods_wide(margins, _RET)
    wholesale = _goods_wide(margins, _WHL)
    has_retail = retail.sum(axis=1) != 0.0
    tax_base = retail.where(has_retail, wholesale)
    # The named exception (#815): exports carry none of the margin-collected
    # taxes. The retail base gives that for free; the wholesale fallback
    # would leak ~$2bn onto F04000, so exports leave the base explicitly.
    tax_base['F04000'] = 0.0
    sales_tax = _shares(tax_base).mul(sales_tax_wedge(year), axis=0)

    pro_share = _shares(_goods_wide(margins, _PRO))
    top_rest = pro_share.mul(bridge['TOP'] - sales_tax.sum(axis=1), axis=0)
    duties = pro_share.mul(bridge['MDTY'], axis=0)
    subsidies = pro_share.mul(bridge['SUB'], axis=0)
    return FiscalLayer(
        year=int(year),
        sales_tax=sales_tax,
        top_rest=top_rest,
        duties=duties,
        subsidies=subsidies,
    )


# --- the reconciliation ----------------------------------------------------


def check(argv: list[str] | None = None) -> int:
    """Print the partition facts and the layer's column-sum identities."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--year', type=int, default=2017)
    args = parser.parse_args(argv)
    year = ta.cast(USA_BENCHMARK_DETAIL_SUT_YEARS, args.year)

    million = MILLION_CURRENCY_TO_CURRENCY
    margins = load_benchmark_margins_before_redef_usa(year)
    bridge = _bridge(year)
    trans_gap = (_goods_wide(margins, _TRANS).sum(axis=1) - bridge['TRANS']).sum()
    print(f'transport vs TRANS, goods rows: {trans_gap / million:,.0f} $M')
    wedge = sales_tax_wedge(year)
    print(
        f'sales-tax wedge: {wedge.sum() / million:,.0f} $M '
        f'({100 * wedge.sum() / bridge["TOP"].sum():.1f}% of TOP)'
    )

    layer = build_fiscal_layer(year)
    top_gap = layer.sales_tax.sum(axis=1) + layer.top_rest.sum(axis=1) - bridge['TOP']
    duty_gap = layer.duties.sum(axis=1) - bridge['MDTY']
    sub_gap = layer.subsidies.sum(axis=1) - bridge['SUB']
    for name, gap in (('TOP', top_gap), ('MDTY', duty_gap), ('SUB', sub_gap)):
        unallocated = gap.abs().sum() / million
        print(f'{name} column-sum residual: {unallocated:,.1f} $M')
    exports = layer.sales_tax.get('F04000')
    if exports is not None:
        print(f'sales tax allocated to exports: {exports.sum() / million:,.1f} $M')
    return 0


if __name__ == '__main__':
    sys.exit(check())
