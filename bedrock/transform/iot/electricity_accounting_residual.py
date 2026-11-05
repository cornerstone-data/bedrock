"""Electricity accounting residual: a layer on the balanced tables, 2018-2024.

A separate pathway from the BEA-based electric output, which is left unchanged.
This module takes that pathway's balanced Supply and Use tables and returns them
with one more commodity and one more industry.

What it does
------------

BEA's electric gross output counts the wholesale price cycle twice (see
``bedrock/analysis/electricity/About_electricity_modeling_record.md``). Lowering
output inside the balance has a cost: BEA's value added stays pinned, so the cut
comes out of the industry's inputs, and the buyers' lost electricity is replaced
by other commodities. In 2022 refineries bought $4.5bn more oil and gas.

Here the difference between the two outputs, ``delta``, moves to a residual pair:

==================  ==============================================================
electric industry   output falls by ``delta``, value added falls by ``delta``,
                    inputs unchanged
buyers              purchases of electricity fall by ``delta`` in total, purchases
                    of the residual rise by the same amount, column totals
                    unchanged
residual industry   output ``delta``, value added ``delta``, no inputs
==================  ==============================================================

Every other cell is unchanged, so nothing passes to other industries. Electric
value added plus residual value added equals the old pathway's electric value
added, so total value added still equals final demand.

The residual has no inputs and no emissions. Households and exports keep their
electricity, because the excess over EIA revenue is on intermediate buyers
(household electricity follows EIA residential revenue).

Emissions shift between buyers
------------------------------

Electric output falls, so its emissions per dollar rise, and intermediate buyers
buy fewer dollars of electricity. Their cut is larger than the fall in output as a
share, because households are excluded, so emissions from electricity move from
intermediate buyers to households. Total emissions are unchanged.

Not yet wired into the build: Steps 6 and 7, the taxonomy and ``A`` do not read
these tables. Run::

    uv run python -m bedrock.transform.iot.electricity_accounting_residual --check
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from bedrock.transform.iot.derived_intermediate_and_value_added import (
    detail_gross_output_panel,
)
from bedrock.transform.iot.eia_utility_go_adjustment import (
    BASE_YEAR,
    CONTROLLED,
    CONTROLLED_YEARS,
    ELECTRIC_INDUSTRIES,
    retail_gross_output,
    retail_price_index,
    retail_sales_mwh,
    two_leg_gross_output,
)

#: The commodity and the industry share one code.
RESIDUAL_CODE = 'S00700'
RESIDUAL_NAME = 'Electricity accounting residual'

ELECTRICITY = '221100'

#: The value-added row the change goes through.
SURPLUS_ROW = 'V00300'

OUTPUT_DATA = Path(__file__).resolve().parents[1] / 'output_data'

#: Identities hold to this, $M.
TOLERANCE_USD_M = 1e-6


def _row(frame: pd.DataFrame, label: str) -> 'pd.Series[float]':
    """One row of a table as floats."""
    return pd.Series(frame.loc[label], dtype=float)


def residual_amounts(panel: pd.DataFrame, year: int) -> pd.Series:
    """``delta`` by electric industry in *year*, $M, never negative.

    The old pathway's output less retail-based output, from BEA's published
    *panel*. Where retail-based output is the higher, the industry keeps the old
    pathway's output: a negative residual has no meaning. So real output follows
    kWh only where ``delta`` is positive.
    """
    two_leg = two_leg_gross_output(panel, year)
    old = panel.loc[two_leg.index, int(year)].astype(float)
    for code in CONTROLLED:
        old[code] = two_leg[code]
    return (old - retail_gross_output(panel, year)).clip(lower=0.0)


def _intermediate_buyers(use: pd.DataFrame, supply: pd.DataFrame) -> list[str]:
    """Industries that buy electricity, other than the electric industries."""
    row = _row(use, ELECTRICITY)
    industries = [c for c in use.columns if c in supply.columns]
    keep = [c for c in industries if c not in ELECTRIC_INDUSTRIES]
    return [c for c in keep if float(row[c]) > 0]


def split_residual(
    use: pd.DataFrame, supply: pd.DataFrame, delta: pd.Series
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """The balanced Use and Supply tables with the residual pair added, $M.

    The new tables keep the row and column order and add the pair at the end of
    the commodity rows and industry columns. Raises rather than returning a table
    that breaks an identity.
    """
    if bool((delta < 0).any()):
        raise ValueError(f'a negative residual: {delta[delta < 0].to_dict()}')
    total = float(delta.sum())
    buyers = _intermediate_buyers(use, supply)
    bills = _row(use, ELECTRICITY)[buyers]
    if total > float(bills.sum()):
        raise ValueError(
            f'residual of {total:,.0f} exceeds intermediate electricity purchases '
            f'of {float(bills.sum()):,.0f}'
        )
    moved = bills * (total / float(bills.sum()))

    industries = [c for c in use.columns if c in supply.columns]
    commodities = [r for r in use.index if r in supply.index]

    new_use = _add_pair(use, commodities, industries)
    new_use.loc[ELECTRICITY, buyers] = bills - moved
    new_use.loc[RESIDUAL_CODE, buyers] = moved
    surplus = _row(use, SURPLUS_ROW)
    for code in delta.index:
        new_use.loc[SURPLUS_ROW, code] = float(surplus[code]) - float(delta[code])
    new_use.loc[SURPLUS_ROW, RESIDUAL_CODE] = total

    new_supply = _add_pair(supply, commodities, industries)
    electricity = _row(supply, ELECTRICITY)
    for code in delta.index:
        new_supply.loc[ELECTRICITY, code] = float(electricity[code]) - float(
            delta[code]
        )
    new_supply.loc[RESIDUAL_CODE, RESIDUAL_CODE] = total

    _check_identities(use, supply, new_use, new_supply, delta, buyers)
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
    supply: pd.DataFrame,
    new_use: pd.DataFrame,
    new_supply: pd.DataFrame,
    delta: pd.Series,
    buyers: list[str],
) -> None:
    """Every column and row total, and that nothing else moved."""
    failures: list[str] = []
    total = float(delta.sum())

    change = new_use.drop(index=RESIDUAL_CODE, columns=RESIDUAL_CODE) - use.reindex(
        index=new_use.index.drop(RESIDUAL_CODE),
        columns=new_use.columns.drop(RESIDUAL_CODE),
    ).astype(float)
    allowed = pd.DataFrame(False, index=change.index, columns=change.columns)
    allowed.loc[[ELECTRICITY], buyers] = True
    allowed.loc[[SURPLUS_ROW], list(delta.index)] = True
    outside = change.where(~allowed, 0.0).abs().to_numpy().max()
    if outside > TOLERANCE_USD_M:
        failures.append(f'a Use cell outside the change moved by {outside:,.6f}')

    buyer_change = (new_use[buyers].sum() - use[buyers].astype(float).sum()).abs().max()
    if buyer_change > TOLERANCE_USD_M:
        failures.append(f"a buyer's column total moved by {buyer_change:,.6f}")

    row_change = float(new_use.loc[RESIDUAL_CODE].sum())
    if abs(row_change - total) > TOLERANCE_USD_M:
        failures.append(f'the residual row sums to {row_change:,.3f}, not {total:,.3f}')

    value_added = float(new_use.loc[SURPLUS_ROW].sum())
    if abs(value_added - float(use.loc[SURPLUS_ROW].astype(float).sum())) > (
        TOLERANCE_USD_M
    ):
        failures.append('total value added moved')

    for code in delta.index:
        amount = float(delta[code])
        fall = float(use[code].astype(float).sum()) - float(new_use[code].sum())
        if abs(fall - amount) > TOLERANCE_USD_M:
            failures.append(
                f'{code} column total fell by {fall:,.3f}, not {amount:,.3f}'
            )
        supplied = float(supply[code].astype(float).sum())
        if abs(supplied - float(new_supply[code].sum()) - amount) > TOLERANCE_USD_M:
            failures.append(f'{code} supply did not fall by {amount:,.3f}')

    if failures:
        raise ValueError('; '.join(failures))


def _load(name: str, year: int, vintage: str) -> pd.DataFrame:
    frame = pd.read_parquet(OUTPUT_DATA / f'{name}_{year}_{vintage}.parquet')
    frame = frame.astype(float)
    frame.index = pd.Index([str(i).strip() for i in frame.index])
    frame.columns = pd.Index([str(c).strip() for c in frame.columns])
    return frame


def report(vintage: str) -> pd.DataFrame:
    """Per year: the residual, what moved, and real output against kWh, on *vintage*."""
    panel = detail_gross_output_panel(ec_adjusted=False)
    rows: dict[int, dict[str, float]] = {}
    for year in CONTROLLED_YEARS:
        if year == BASE_YEAR:
            continue
        delta = residual_amounts(panel, year)
        use = _load('Balanced_Detail_Use_SUT', year, vintage)
        supply = _load('Balanced_Detail_Supply', year, vintage)
        new_use, new_supply = split_residual(use, supply, delta)

        codes = list(ELECTRIC_INDUSTRIES)
        old = float(_row(use, SURPLUS_ROW)[codes].sum())
        new_output = float(new_supply[codes].sum().sum())
        base = _load('Balanced_Detail_Supply', BASE_YEAR, vintage)
        base_output = float(base[codes].sum().sum())
        price = retail_price_index(year) / retail_price_index(BASE_YEAR)
        kwh = retail_sales_mwh(year) / retail_sales_mwh(BASE_YEAR)

        output = float(supply[codes].sum().sum())
        intermediate = _intermediate_buyers(use, supply)
        bills = float(_row(use, ELECTRICITY)[intermediate].sum())
        rows[year] = {
            **{f'delta {code}, $bn': float(delta[code]) / 1e3 for code in codes},
            'residual, $bn': float(delta.sum()) / 1e3,
            'share of intermediate electricity, %': 100 * float(delta.sum()) / bills,
            'electric surplus before, $bn': old / 1e3,
            'electric surplus after, $bn': float(
                _row(new_use, SURPLUS_ROW)[codes].sum()
            )
            / 1e3,
            **{
                f'surplus after {code}, $bn': float(_row(new_use, SURPLUS_ROW)[code])
                / 1e3
                for code in codes
            },
            'real output on EIA price, 2017 = 1': new_output / base_output / price,
            'kWh, 2017 = 1': kwh,
            'intermediate buyers, electricity emissions': (
                (bills - float(delta.sum())) / bills / (new_output / output)
            ),
            'other electricity buyers, emissions': output / new_output,
        }
    return pd.DataFrame(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split('\n', maxsplit=1)[0])
    parser.add_argument('--check', action='store_true', help='run every year')
    parser.add_argument('--vintage', default='v1.0.0_5514c3b')
    args = parser.parse_args()
    pd.set_option('display.width', 200)
    print(report(args.vintage).round(3).to_string())
    print('\nOK: every identity held in every year')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
