"""Electricity accounting residual: a second pathway for electric power, 2017-2024.

The BEA-based pathway stays as it is. With ``electricity_accounting_residual`` on,
electric output follows EIA retail revenue, electric non-fuel inputs follow a
target, and what is left of BEA's electric value added moves to commercial and
industrial buyers' operating surplus (``V00300``). 2017 moves only with
``electric_output_on_eia_level`` on.

The residual
------------

For all electric industries together::

    non-fuel target = 2017 non-fuel inputs x input prices x EIA kWh
    electric value added = retail output - EIA fuel - non-fuel target
    residual = BEA value added - electric value added, never negative

The non-fuel target holds the real input mix per kWh fixed. Input prices are each
commodity's own price, weighted by the 2017 cells. The fall in electric value
added equals the sum of the rises in buyers' value added, so total value added
still equals final demand.

Only intermediate buyers take the residual, never households or exports, whose
electricity follows EIA revenue.

With ``electric_output_on_eia_level`` on, retail output starts below BEA's 2017
level by what commercial and industrial buyers paid in 2017 above EIA revenue
(:func:`level_excess`). Inputs don't change, so the difference comes out of
electric value added and into the residual, in 2017 as well.

Who buys it
-----------

Commercial and industrial buyers split it by each class's bills above EIA revenue
that year, and within a class by each buyer's electricity purchases that year. The
bills are Step 3's electricity cells, on column totals before the transfer, because
balanced bills come out of the balance this feeds, which would be circular. On the
BEA-based build they put commercial's share within 7 points of balanced bills, and
run above them in 2023-24, before the interior fit.

How it reaches the balance
--------------------------

The residual has no row or column in any table. The balance sees a transfer of
value added (:func:`value_added_transfers`): electric value added falls and each
buyer's rises by its share, so the total is unchanged, and every target built on
``GO - VAPRO`` follows.

Where it ends up
----------------

Before the balance, the transfer goes on each industry's ``V00300``
(``nowcast._reconcile_to_published_vapro``). The balance can then move that row,
which is unlocked, so the released surplus includes the transfer but does not
isolate it. Buyers' output is fixed, so the transfer moves real inputs from their
columns to electric power's.

A residual commodity with a row in each buyer's column would give the same total
factors (``N``), because it has no inputs and no emissions. It would change ``A``
column sums and the outputs built on them, so it is not added (#1271).
"""

from __future__ import annotations

import functools

import pandas as pd

from bedrock.transform.iot.eia_utility_go_adjustment import (
    BASE_YEAR,
    CONTROLLED_YEARS,
    ELECTRIC_INDUSTRIES,
    retail_index,
    retail_sales_mwh,
)

ELECTRICITY = '221100'

#: The rows EIA's fuel bill stands for, left out of the non-fuel target's weights.
FUEL_ROWS = ('212100', '211000', '324110')

#: Two-digit prefixes of EIA's industrial class: farms, mining, construction,
#: manufacturing. Every other intermediate buyer is in its commercial class.
INDUSTRIAL_PREFIXES = ('11', '21', '23', '31', '32', '33')


def residual_total(
    output_2017: float,
    value_added_2017: float,
    value_added: float,
    fuel_2017: float,
    fuel: float,
    retail_relative: float,
    input_price_relative: float,
    kwh_relative: float,
    excess_2017: float = 0.0,
) -> float:
    """The residual in one year, $M, from all-electric totals. Never negative.

    *excess_2017* is 2017 output taken out of retail output, which leaves the
    non-fuel target where it is.
    """
    non_fuel_2017 = output_2017 - value_added_2017 - fuel_2017
    non_fuel = non_fuel_2017 * input_price_relative * kwh_relative
    retail_output = (output_2017 - excess_2017) * retail_relative
    electric_value_added = retail_output - fuel - non_fuel
    return max(value_added - electric_value_added, 0.0)


def split_across_columns(total: float, value_added: pd.Series) -> pd.Series:
    """The residual by electric industry, in proportion to its value added."""
    return value_added.astype(float) * (total / float(value_added.sum()))


def buyer_shares(
    bills: pd.Series, revenue_commercial: float, revenue_industrial: float
) -> pd.Series:
    """Each intermediate buyer's share of the residual, from one year's bills.

    Classes split by their bills above EIA revenue, and buyers within a class by
    bill. A class at or below its revenue takes none.
    """
    positive = bills[bills > 0].astype(float)
    industrial = [c for c in positive.index if str(c).startswith(INDUSTRIAL_PREFIXES)]
    commercial = [c for c in positive.index if c not in industrial]
    excess = pd.Series(
        {
            'commercial': float(positive[commercial].sum()) - revenue_commercial,
            'industrial': float(positive[industrial].sum()) - revenue_industrial,
        }
    ).clip(lower=0.0)
    if float(excess.sum()) <= 0:
        raise ValueError('no class has bills above its EIA revenue')
    weights = excess / float(excess.sum())
    return pd.concat(
        [
            positive[commercial]
            / float(positive[commercial].sum())
            * float(weights['commercial']),
            positive[industrial]
            / float(positive[industrial].sum())
            * float(weights['industrial']),
        ]
    )


def _use_2017() -> pd.DataFrame:
    from bedrock.extract.iot.io_2017 import (  # noqa: PLC0415
        _load_2017_detail_supply_use_usa,
    )

    use = _load_2017_detail_supply_use_usa('Use_SUT_detail').copy()
    use.index = pd.Index([str(i).strip() for i in use.index])
    use.columns = pd.Index([str(c).strip() for c in use.columns])
    return use


def base_year_excess(
    bills: pd.Series, revenue_commercial: float, revenue_industrial: float
) -> float:
    """What commercial and industrial buyers paid above EIA revenue, $M."""
    return max(float(bills.sum()) - revenue_commercial - revenue_industrial, 0.0)


@functools.cache
def _excess_2017() -> float:
    from bedrock.extract.disaggregation.egrid_generation import (  # noqa: PLC0415
        eia_retail_revenue_usd,
    )
    from bedrock.utils.taxonomy.bea.v2017_industry import (  # noqa: PLC0415
        USA_2017_INDUSTRY_CODES,
    )

    buyers = [c for c in USA_2017_INDUSTRY_CODES if c not in ELECTRIC_INDUSTRIES]
    row = pd.to_numeric(pd.Series(_use_2017().loc[ELECTRICITY]), errors='coerce')
    return base_year_excess(
        row.reindex(buyers).fillna(0.0),
        eia_retail_revenue_usd(BASE_YEAR, 'Commercial') / 1e6,
        eia_retail_revenue_usd(BASE_YEAR, 'Industrial') / 1e6,
    )


def level_excess() -> float:
    """2017 output above EIA's level, $M. Zero unless ``electric_output_on_eia_level``."""
    from bedrock.utils.config.usa_config import get_usa_config  # noqa: PLC0415

    if not get_usa_config().electric_output_on_eia_level:
        return 0.0
    return _excess_2017()


def _step_3_bills(year: int, value_added: pd.Series) -> 'pd.Series[float]':
    """Step 3's electricity cells in *year*, other than electric buyers, $M.

    On column totals before the transfer, passed explicitly so this does not read
    the value-added panel it helps build.
    """
    from bedrock.transform.iot.derived_intermediate_and_value_added import (  # noqa: PLC0415
        detail_gross_output_panel,
    )
    from bedrock.transform.iot.nowcast_intermediate import (  # noqa: PLC0415
        derive_intermediate_use,
    )
    from bedrock.utils.economic.units import (  # noqa: PLC0415
        MILLION_CURRENCY_TO_CURRENCY,
    )

    output = detail_gross_output_panel()[int(year)].reindex(value_added.index)
    control = (output - value_added).astype(float) * MILLION_CURRENCY_TO_CURRENCY
    block = derive_intermediate_use(int(year), column_control=control)
    row = pd.Series(block.loc[ELECTRICITY], dtype=float) / MILLION_CURRENCY_TO_CURRENCY
    return row.drop(index=list(ELECTRIC_INDUSTRIES), errors='ignore')


@functools.cache
def _non_fuel_weights() -> 'pd.Series[float]':
    """The electric columns' 2017 non-fuel input cells, by commodity, $M."""
    from bedrock.utils.taxonomy.bea.v2017_commodity import (  # noqa: PLC0415
        USA_2017_COMMODITY_CODES,
    )

    cells = (
        _use_2017()[list(ELECTRIC_INDUSTRIES)]
        .apply(pd.to_numeric, errors='coerce')
        .fillna(0.0)
        .sum(axis=1)
    )
    keep = [c for c in USA_2017_COMMODITY_CODES if c not in FUEL_ROWS]
    return pd.Series(cells.reindex(keep).fillna(0.0), dtype=float)


def input_price_relative(year: int) -> float:
    """The price of the electric columns' non-fuel inputs relative to 2017."""
    from bedrock.transform.iot.nowcast_intermediate import (  # noqa: PLC0415
        commodity_price_factor,
    )

    weights = _non_fuel_weights()
    factor = commodity_price_factor(int(year)).reindex(weights.index).fillna(1.0)
    return float((weights * factor).sum() / weights.sum())


def value_added_transfers(
    year: int, value_added: pd.Series, value_added_2017: float, output_2017: float
) -> pd.Series:
    """The change in each industry's value added for the balance, $M. Sums to zero.

    *value_added* is the year's value added by detail industry before the
    transfer. *value_added_2017* and *output_2017* are BEA's all-electric totals.
    """
    from bedrock.analysis.nowcasting.utilities_expense_seed import (  # noqa: PLC0415
        fuel_costs,
    )
    from bedrock.extract.disaggregation.egrid_generation import (  # noqa: PLC0415
        eia_retail_revenue_usd,
    )

    codes = [code for code in ELECTRIC_INDUSTRIES if code in value_added.index]
    transfers = pd.Series(0.0, index=value_added.index)
    excess = level_excess()
    if int(year) not in CONTROLLED_YEARS or (int(year) == BASE_YEAR and excess == 0.0):
        return transfers
    fuel = fuel_costs()[['coal', 'gas', 'oil']].sum(axis=1)
    total = residual_total(
        output_2017=output_2017,
        value_added_2017=value_added_2017,
        value_added=float(value_added[codes].sum()),
        fuel_2017=float(fuel[BASE_YEAR]),
        fuel=float(fuel[int(year)]),
        retail_relative=retail_index(int(year)),
        input_price_relative=input_price_relative(int(year)),
        kwh_relative=retail_sales_mwh(int(year)) / retail_sales_mwh(BASE_YEAR),
        excess_2017=excess,
    )
    transfers[codes] = -split_across_columns(total, value_added[codes])
    shares = buyer_shares(
        _step_3_bills(int(year), value_added),
        eia_retail_revenue_usd(int(year), 'Commercial') / 1e6,
        eia_retail_revenue_usd(int(year), 'Industrial') / 1e6,
    )
    transfers[shares.index] = shares * total
    return transfers
