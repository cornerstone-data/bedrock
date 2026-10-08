"""Convert a before-redefinition Supply block into a producer-price Make table.

The core conversion is deliberately data-source agnostic: callers provide the
commodity x industry domestic-output block and the already-allocated industry
tax rows.  ``--check`` supplies the 2017 nowcast inputs and compares the result
with BEA's published before-redefinition Make table.
"""

from __future__ import annotations

import argparse
import sys
from typing import cast

import numpy as np
import pandas as pd

from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY

TOP_ROW = 'T00TOP'
SUB_ROW = 'T00SUB'
CUSTOMS = '4200ID'
_REQUIRED_TAX_ROWS = (TOP_ROW, SUB_ROW)


def make_from_sut(
    supply_block: pd.DataFrame,
    tax_rows: pd.DataFrame,
    year: int,
) -> pd.DataFrame:
    """Return an industry x commodity Make table at producer prices.

    ``supply_block`` is domestic production at basic prices, with commodities
    on rows and industries on columns. ``tax_rows`` carries ``T00TOP`` as
    positive and ``T00SUB`` as negative, both indexed over industries.  The
    function preserves each industry's commodity mix while adding that
    industry's net product-tax wedge.

    The commodity-to-industry allocation has already happened in ``tax_rows``.
    Because that operation retains only industry totals, this function places
    each total over the commodities produced by that industry in proportion to
    its domestic output. This is the deliberately isolated assumption a future
    cell-level allocator can replace without changing the function's inputs.

    Customs duties are the one zero-output exception: BEA books the whole
    amount on the synthetic ``4200ID`` industry/commodity cell.
    """
    if not isinstance(supply_block, pd.DataFrame) or not isinstance(
        tax_rows, pd.DataFrame
    ):
        raise TypeError('supply_block and tax_rows must be pandas DataFrames')
    if not supply_block.index.is_unique or not supply_block.columns.is_unique:
        raise ValueError(f'{year} supply_block labels must be unique')

    missing_rows = [row for row in _REQUIRED_TAX_ROWS if row not in tax_rows.index]
    if missing_rows:
        raise ValueError(f'{year} tax_rows is missing {missing_rows}')
    missing_industries = supply_block.columns.difference(tax_rows.columns)
    if not missing_industries.empty:
        raise ValueError(
            f'{year} tax_rows is missing industries {missing_industries.tolist()}'
        )

    basic = supply_block.astype(float).T.copy()
    basic.index.name = 'industry'
    basic.columns.name = 'commodity'
    top = tax_rows.reindex(index=[TOP_ROW], columns=basic.index).iloc[0].astype(float)
    sub = tax_rows.reindex(index=[SUB_ROW], columns=basic.index).iloc[0].astype(float)
    if (sub > 0).any():
        raise ValueError(
            f'{year} {SUB_ROW} must be non-positive; positive on '
            f'{sub.index[sub > 0].tolist()}'
        )

    wedge = top + sub
    output = basic.sum(axis=1)
    shares = basic.div(output.replace(0.0, np.nan), axis=0).fillna(0.0)
    make = basic + shares.mul(wedge, axis=0)

    zero_output_wedge = wedge[(output == 0.0) & (wedge != 0.0)]
    for industry, amount in zero_output_wedge.items():
        if industry == CUSTOMS and CUSTOMS in make.columns:
            current = cast(float, make.at[CUSTOMS, CUSTOMS])
            make.at[CUSTOMS, CUSTOMS] = current + amount
            continue
        raise ValueError(
            f'{year} industry {industry!r} has a {amount:,.0f} wedge but no '
            'domestic output on which to allocate it'
        )

    expected = output + wedge
    gap = (make.sum(axis=1) - expected).abs()
    if float(gap.max()) > 1e-3:
        raise AssertionError(
            f'{year} Make conversion failed to conserve the industry wedge; '
            f'max gap {gap.max():,.6f}'
        )
    return make


def _difference_report(
    actual: pd.DataFrame,
    expected: pd.DataFrame,
    trade_industry_codes: list[str],
) -> None:
    """Print the 2017 cell residual distribution; it is diagnostic, not a gate."""
    difference_m = (actual - expected) / MILLION_CURRENCY_TO_CURRENCY
    absolute_m = difference_m.abs()
    matches = np.isclose(
        actual.to_numpy(),
        expected.to_numpy(),
        rtol=0.01,
        atol=0.5 * MILLION_CURRENCY_TO_CURRENCY,
    )
    flat = absolute_m.to_numpy().ravel()
    print('\n2017 Make replay')
    print(f'  matching cells (rtol 1%, atol $0.5M): {matches.sum():,}/{matches.size:,}')
    print(
        '  absolute difference ($M): '
        f'median {np.median(flat):,.3f}, p90 {np.quantile(flat, 0.90):,.3f}, '
        f'p99 {np.quantile(flat, 0.99):,.3f}, max {flat.max():,.3f}'
    )
    industry_residual = absolute_m.sum(axis=1)
    total_residual = float(industry_residual.sum())
    trade_residual = float(
        industry_residual.reindex(trade_industry_codes).fillna(0.0).sum()
    )
    trade_share = trade_residual / total_residual if total_residual else 0.0
    print(f'  trade-industry share of gross |difference|: {trade_share:.1%}')
    print('  largest industry residuals, gross |difference| ($M):')
    print(industry_residual.nlargest(20).to_string())
    print('  largest commodity residuals, gross |difference| ($M):')
    print(absolute_m.sum(axis=0).nlargest(10).to_string())
    print('  expected ceiling: residuals concentrated in trade industries')


def check() -> int:
    """Replay 2017 and fail only the invariants that should be exact."""
    from bedrock.extract.iot.io_2017 import (  # noqa: PLC0415
        _load_2017_detail_supply_use_usa,
        load_2017_V_before_redef_usa,
    )
    from bedrock.transform.iot.nowcast_va_taxes import (  # noqa: PLC0415
        government_industries,
        make_block,
        trade_industries,
        va_tax_rows,
    )
    from bedrock.utils.taxonomy.bea.v2017_commodity import (  # noqa: PLC0415
        USA_2017_COMMODITY_CODES,
    )
    from bedrock.utils.taxonomy.bea.v2017_industry import (  # noqa: PLC0415
        USA_2017_INDUSTRY_CODES,
    )

    year = 2017
    supply = make_block(year).reindex(
        index=list(USA_2017_COMMODITY_CODES),
        columns=list(USA_2017_INDUSTRY_CODES),
    )
    taxes = va_tax_rows(year, block=supply)
    actual = make_from_sut(supply, taxes, year)
    expected = load_2017_V_before_redef_usa().reindex_like(actual)
    _difference_report(actual, expected, trade_industries())

    failures: list[str] = []
    published_supply = _load_2017_detail_supply_use_usa('Supply_detail')
    published_basic = (
        published_supply.loc[
            list(USA_2017_COMMODITY_CODES), list(USA_2017_INDUSTRY_CODES)
        ].astype(float)
        * MILLION_CURRENCY_TO_CURRENCY
    )
    transpose_gap_m = float(
        ((supply - published_basic).abs() / MILLION_CURRENCY_TO_CURRENCY).max().max()
    )
    print(f'\n  domestic-block replay max difference: ${transpose_gap_m:,.3f}M')
    if transpose_gap_m > 2.0:
        failures.append(
            f'domestic block differs from the published Supply table by up to '
            f'${transpose_gap_m:,.3f}M; expected no more than $2M'
        )

    actual_customs = cast(float, actual.at[CUSTOMS, CUSTOMS])
    expected_customs = cast(float, expected.at[CUSTOMS, CUSTOMS])
    customs_gap_m = (
        abs(actual_customs - expected_customs) / MILLION_CURRENCY_TO_CURRENCY
    )
    print(f'  {CUSTOMS} cell difference: ${customs_gap_m:,.3f}M')
    if not np.isclose(actual_customs, expected_customs, rtol=0.0, atol=1.0):
        failures.append(
            f'{CUSTOMS} is not exact; it differs from the published Make cell by '
            f'${customs_gap_m:,.6f}M'
        )

    government_tax = (
        taxes.reindex(index=[TOP_ROW], columns=government_industries())
        .iloc[0]
        .astype(float)
    )
    government_tax_m = float(government_tax.abs().sum() / MILLION_CURRENCY_TO_CURRENCY)
    print(f'  product tax on government industries: ${government_tax_m:,.3f}M')
    if government_tax_m > 0.5:
        failures.append(
            f'government industries carry ${government_tax_m:,.3f}M of T00TOP'
        )

    if failures:
        print('\nFAILED:')
        for failure in failures:
            print(f'  - {failure}')
        return 1
    print(
        '\nOK: exact invariants hold; the known trade-industry residual is reported above'
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true', help='run the 2017 replay')
    args = parser.parse_args()
    if args.check:
        return check()
    parser.print_help()
    return 0


if __name__ == '__main__':
    sys.exit(main())
