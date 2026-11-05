"""Electricity accounting residual: a second pathway for electric power, 2018-2024.

The BEA-based pathway stays as it is. With ``electricity_accounting_residual`` on,
electric output follows EIA retail revenue, electric non-fuel inputs follow a
target, and what is left of BEA's electric value added moves to a residual
commodity and industry.

The residual
------------

For all electric industries together::

    non-fuel target = 2017 non-fuel inputs x input prices x EIA kWh
    electric value added = retail output - EIA fuel - non-fuel target
    residual = BEA value added - electric value added, never negative

The non-fuel target holds the real input mix per kWh fixed. Input prices are each
commodity's own price, weighted by the 2017 cells. Electric value added plus
residual value added equals BEA's, so total value added still equals final demand.

The residual has no inputs and no emissions. It is sold to intermediate buyers,
never to households or exports, whose electricity follows EIA revenue.

Who buys it
-----------

Commercial and industrial buyers split it by each class's 2017 bills above EIA
revenue, and within a class by each buyer's 2017 electricity purchases. Both are
measured at the benchmark because a year's own bills come out of the balance this
feeds, which would be circular.

How it reaches the balance
--------------------------

The balance has no row or column for the pair. It sees a transfer of value added
(:func:`value_added_transfers`): electric value added falls and each buyer's rises
by what it pays for the residual, so the total is unchanged, and every target
built on ``GO - VAPRO`` follows. :func:`split_residual` then moves those buyer
amounts out of surplus into the residual row, on the balanced tables.

Not yet wired past Step 5: Steps 6 and 7, the taxonomy and ``A`` do not read the
pair, so the flag must stay off in any build that publishes tables.
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

#: The commodity and the industry share one code.
RESIDUAL_CODE = 'S00700'
RESIDUAL_NAME = 'Electricity accounting residual'

ELECTRICITY = '221100'

#: The value-added row the change goes through.
SURPLUS_ROW = 'V00300'

#: The rows EIA's fuel bill stands for, left out of the non-fuel target's weights.
FUEL_ROWS = ('212100', '211000', '324110')

#: Two-digit prefixes of EIA's industrial class: farms, mining, construction,
#: manufacturing. Every other intermediate buyer is in its commercial class.
INDUSTRIAL_PREFIXES = ('11', '21', '23', '31', '32', '33')

#: Identities hold to this, $M.
TOLERANCE_USD_M = 1e-6


def _row(frame: pd.DataFrame, label: str) -> 'pd.Series[float]':
    """One row of a table as floats."""
    return pd.Series(frame.loc[label], dtype=float)


def residual_total(
    output_2017: float,
    value_added_2017: float,
    value_added: float,
    fuel_2017: float,
    fuel: float,
    retail_relative: float,
    input_price_relative: float,
    kwh_relative: float,
) -> float:
    """The residual in one year, $M, from all-electric totals. Never negative."""
    non_fuel_2017 = output_2017 - value_added_2017 - fuel_2017
    non_fuel = non_fuel_2017 * input_price_relative * kwh_relative
    electric_value_added = output_2017 * retail_relative - fuel - non_fuel
    return max(value_added - electric_value_added, 0.0)


def split_across_columns(total: float, value_added: pd.Series) -> pd.Series:
    """The residual by electric industry, in proportion to its value added."""
    return value_added.astype(float) * (total / float(value_added.sum()))


def buyer_shares(
    bills: pd.Series, revenue_commercial: float, revenue_industrial: float
) -> pd.Series:
    """Each intermediate buyer's share of the residual, from 2017 bills.

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
        raise ValueError('no class has 2017 bills above its EIA revenue')
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


@functools.cache
def _bills_2017() -> 'pd.Series[float]':
    """Intermediate electricity purchases at the benchmark, other than electric, $M."""
    from bedrock.utils.taxonomy.bea.v2017_industry import (  # noqa: PLC0415
        USA_2017_INDUSTRY_CODES,
    )

    row = pd.Series(
        pd.to_numeric(pd.Series(_use_2017().loc[ELECTRICITY]), errors='coerce')
    )
    keep = [c for c in USA_2017_INDUSTRY_CODES if c not in ELECTRIC_INDUSTRIES]
    return pd.Series(row.reindex(keep).fillna(0.0), dtype=float)


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
    if int(year) == BASE_YEAR or int(year) not in CONTROLLED_YEARS:
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
    )
    transfers[codes] = -split_across_columns(total, value_added[codes])
    shares = buyer_shares(
        _bills_2017(),
        eia_retail_revenue_usd(BASE_YEAR, 'Commercial') / 1e6,
        eia_retail_revenue_usd(BASE_YEAR, 'Industrial') / 1e6,
    )
    transfers[shares.index] = shares * total
    return transfers


def split_residual(
    use: pd.DataFrame, supply: pd.DataFrame, purchases: pd.Series
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """The balanced tables with the residual pair added, $M.

    *purchases* is what each buyer pays for the residual, the positive part of
    :func:`value_added_transfers`. The balance holds that amount in each buyer's
    surplus. Here it moves to the residual row, and the same total becomes the
    residual industry's output and value added. Raises rather than returning
    tables that break an identity.
    """
    if bool((purchases < 0).any()):
        raise ValueError('a negative purchase of the residual')
    total = float(purchases.sum())
    industries = [c for c in use.columns if c in supply.columns]
    commodities = [r for r in use.index if r in supply.index]

    new_use = _add_pair(use, commodities, industries)
    surplus = _row(use, SURPLUS_ROW)
    for code in purchases.index:
        new_use.loc[RESIDUAL_CODE, code] = float(purchases[code])
        new_use.loc[SURPLUS_ROW, code] = float(surplus[code]) - float(purchases[code])
    new_use.loc[SURPLUS_ROW, RESIDUAL_CODE] = total

    new_supply = _add_pair(supply, commodities, industries)
    new_supply.loc[RESIDUAL_CODE, RESIDUAL_CODE] = total

    _check_identities(use, new_use, new_supply, purchases)
    return new_use, new_supply


def _add_pair(
    frame: pd.DataFrame, commodities: list[str], industries: list[str]
) -> pd.DataFrame:
    """*frame* with a zero row after the commodities and column after the industries."""
    rows = [*commodities, RESIDUAL_CODE, *frame.index.drop(commodities)]
    columns = [*industries, RESIDUAL_CODE, *frame.columns.drop(industries)]
    return frame.reindex(index=rows, columns=columns).fillna(0.0).astype(float)


def _check_identities(
    use: pd.DataFrame,
    new_use: pd.DataFrame,
    new_supply: pd.DataFrame,
    purchases: pd.Series,
) -> None:
    """Column totals, the pair's balance, and that nothing else moved."""
    failures: list[str] = []
    total = float(purchases.sum())

    kept = new_use.drop(index=RESIDUAL_CODE, columns=RESIDUAL_CODE)
    change = kept - use.reindex(index=kept.index, columns=kept.columns).astype(float)
    allowed = pd.DataFrame(False, index=change.index, columns=change.columns)
    allowed.loc[[SURPLUS_ROW], list(purchases.index)] = True
    outside = float(change.where(~allowed, 0.0).abs().to_numpy().max())
    if outside > TOLERANCE_USD_M:
        failures.append(f'a Use cell outside the change moved by {outside:,.6f}')

    buyers = list(purchases.index)
    moved = float((new_use[buyers].sum() - use[buyers].astype(float).sum()).abs().max())
    if moved > TOLERANCE_USD_M:
        failures.append(f"a buyer's column total moved by {moved:,.6f}")

    for name, value in {
        'the residual row': float(_row(new_use, RESIDUAL_CODE).sum()),
        'the residual column': float(new_use[RESIDUAL_CODE].sum()),
        'residual supply': float(new_supply[RESIDUAL_CODE].sum()),
    }.items():
        if abs(value - total) > TOLERANCE_USD_M:
            failures.append(f'{name} sums to {value:,.3f}, not {total:,.3f}')

    surplus_change = float(_row(new_use, SURPLUS_ROW).sum()) - float(
        _row(use, SURPLUS_ROW).sum()
    )
    if abs(surplus_change) > TOLERANCE_USD_M:
        failures.append(f'total value added moved by {surplus_change:,.6f}')

    if failures:
        raise ValueError('; '.join(failures))
