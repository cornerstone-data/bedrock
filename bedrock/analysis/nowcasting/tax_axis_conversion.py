"""Can the product-tax rows be converted from commodity to industry, 2017?

`T00TOP` and `T00SUB` exist on both axes.  The Supply table carries them by
commodity (``TOP``, ``SUB``, ``MDTY``, built in Step 4d, #690); the Use table
carries them by industry as value-added rows.  It is the same money, so Step 2
should **transform** rather than re-estimate -- the question is only whether an
available operator reproduces the published industry row well enough to be worth
seeding the balance with.

This measures that for 2017, where the published answer exists.  The operator
tested is the obvious one: the benchmark **market-share matrix** from the Supply
table, ``D[c, i] = V[c, i] / T007[c]``, which distributes each commodity's tax to
the industries that *produce* that commodity.

The answer is no, and the reason is structural
----------------------------------------------

=========================  ===========  ===============================
row                        correlation  ``sum |est - published|``
=========================  ===========  ===============================
``T00TOP``  (TOP + MDTY)         0.202  865,515  = **114.6%** of the row
``T00TOP``  (TOP only)           0.211  827,322  = 109.5% of the row
``T00SUB``                       0.676   47,776  = 79.8% of the row
=========================  ===========  ===============================

The column totals are right by construction, and per industry the estimate is
worth nothing.  **55.7% of published ``T00TOP`` sits in wholesale and retail
industries**, and market shares put almost none of it there, because a tax on a
product is remitted by whoever *sells* it, not whoever makes it:

===========================  ==========  ==========  =========================
industry                       estimate   published   what it is
===========================  ==========  ==========  =========================
``424700``                           13      88,362   petroleum wholesalers
``324110``                       92,893         397   petroleum refineries
``441000``                        2,301      45,947   motor vehicle dealers
``336111`` + ``336112``          24,141          26   automobile manufacturing
``452000``                          635      37,990   general merchandise
``312200``                       35,559      13,345   tobacco manufacturing
===========================  ==========  ==========  =========================

Read the pairs: the entire petroleum tax moves from wholesalers to refineries,
and the entire motor-vehicle tax from dealers to assemblers.  The operator is
not noisy, it is pointed at the wrong stage of the chain.

⚠️ **This is not a "the benchmark year drifts" finding.**  It is measured *in*
the benchmark year, against the table the mix comes from, with nothing assumed
about later years at all.  A conversion that fails in 2017 cannot be rescued by
being applied closer to 2017.

``T00SUB`` is better but fails differently
------------------------------------------

Correlation 0.676, and the residual is two named structures rather than a
smear.  Government enterprises are under-attributed -- ``S00203`` other state and
local enterprises is 19,471 published against 1,964 estimated (public transit
operating subsidies), ``S00102`` 6,339 against 102 -- while tenant-occupied
housing ``531HST`` is over-attributed at 33,848 against 16,307.  Both are
subsidies paid to an *operator* rather than attaching to a product, so no
product-side operator can place them.  ``T30800`` and the housing tables already
carry those sectors directly (see `compensation_disaggregation_plan.md`).

The one piece that converts exactly
-----------------------------------

✅ **Import duties are a lookup, not an allocation.**  Published ``T00TOP`` on
``4200ID`` is **38,513** against a Supply ``MDTY`` total of 38,510 -- BEA books
the whole of customs duties to that one synthetic industry code.  That is 5.1%
of the row, free and exact, in every year.

What this means for Step 2 and Step 5
-------------------------------------

The plan's decision of 2026-08-17 -- leave the industry distribution of
``T00TOP``/``T00SUB`` to Step 5's balance under economy-wide soft targets --
**stands, and for a stronger reason than it was given.**  It was argued from
stability ("a fixed 2017 conversion ratio is exactly what we cannot assume").
The measured objection is sharper: the conversion is wrong in the benchmark year
itself.

But "leave it free" is not the same as "seed it with nothing", and two pieces of
structure are available now:

- ``4200ID`` takes ``MDTY`` exactly.
- The remaining split should ride the **margin** structure, not the Make matrix.
  A commodity's wholesale and retail margins say which trade industries handle
  it, which is the point-of-sale signal the tax actually follows.  Those margins
  are built (`nowcast_trade_margins`), so the better operator is testable the
  same way this one was -- that is the open follow-up, and this module is where
  it should be measured.

Usage::

    uv run python -m bedrock.analysis.nowcasting.tax_axis_conversion
    uv run python -m bedrock.analysis.nowcasting.tax_axis_conversion --check
"""

from __future__ import annotations

import argparse
import sys

import numpy as np
import pandas as pd

from bedrock.extract.iot.io_2017 import _load_2017_detail_supply_use_usa
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES

YEAR = 2017

#: Wholesale and retail industry code prefixes.  ``4200ID`` is BEA's customs
#: duties code, which sits inside the trade block and is counted with it.
TRADE_PREFIXES = ('42', '44', '45', '4B')

#: The naive operator reproduces so little of ``T00TOP`` that any bar worth
#: stating is one it fails.  Correlation above this would mean the conversion is
#: worth seeding with; it is 0.20.
USABLE_CORRELATION = 0.80


def _frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    return (
        _load_2017_detail_supply_use_usa('Supply_detail'),
        _load_2017_detail_supply_use_usa('Use_SUT_detail'),
    )


def market_share_matrix() -> pd.DataFrame:
    """``D[c, i]``: industry ``i``'s share of commodity ``c``'s domestic output.

    Normalised by ``T007`` (commodity output, basic, domestic) rather than by
    industry output -- that is the *market share* matrix, not the commodity mix.
    A commodity-indexed quantity has to be spread over the industries that make
    that commodity, which is this one; the commodity mix answers the transposed
    question and would give an unrelated number.
    """
    supply, _ = _frames()
    commodities, industries = (
        list(USA_2017_COMMODITY_CODES),
        list(USA_2017_INDUSTRY_CODES),
    )
    make = supply.loc[commodities, industries].astype(float)
    output = supply.loc[commodities, 'T007'].astype(float)
    return make.div(output.replace(0, np.nan), axis=0).fillna(0.0)


def convert_to_industry(by_commodity: 'pd.Series[float]') -> 'pd.Series[float]':
    """Spread a commodity-indexed series over industries by market share."""
    return market_share_matrix().mul(by_commodity, axis=0).sum(axis=0)


def published_row(use: pd.DataFrame, row: str) -> 'pd.Series[float]':
    """One Use SUT value-added row, over the 402 industries, as floats."""
    industries = list(USA_2017_INDUSTRY_CODES)
    series = use.loc[row]
    assert isinstance(series, pd.Series)
    return series.reindex(industries).astype(float).fillna(0.0)


def _score(
    estimate: 'pd.Series[float]', published: 'pd.Series[float]'
) -> dict[str, float]:
    industries = list(USA_2017_INDUSTRY_CODES)
    estimate = estimate.reindex(industries).astype(float).fillna(0.0)
    published = published.reindex(industries).astype(float).fillna(0.0)
    nonzero = (estimate.abs() + published.abs()) > 0
    absolute_error = float((estimate - published).abs().sum())
    return {
        'estimate_total': float(estimate.sum()),
        'published_total': float(published.sum()),
        'correlation': float(np.corrcoef(estimate[nonzero], published[nonzero])[0, 1]),
        'absolute_error': absolute_error,
        'error_share': absolute_error / float(published.abs().sum()),
    }


def conversion_scores() -> dict[str, dict[str, float]]:
    """Score the market-share conversion against the published Use rows."""
    supply, use = _frames()
    commodities = list(USA_2017_COMMODITY_CODES)
    taxes = supply.loc[commodities, 'TOP'].astype(float)
    duties = supply.loc[commodities, 'MDTY'].astype(float)
    # SUB is stored negative on the Supply table and positive on the Use table
    subsidies = -supply.loc[commodities, 'SUB'].astype(float)

    return {
        'T00TOP (TOP + MDTY)': _score(
            convert_to_industry(taxes + duties), published_row(use, 'T00TOP')
        ),
        'T00TOP (TOP only)': _score(
            convert_to_industry(taxes), published_row(use, 'T00TOP')
        ),
        'T00SUB': _score(convert_to_industry(subsidies), published_row(use, 'T00SUB')),
    }


def trade_concentration() -> dict[str, float]:
    """How much of the published ``T00TOP`` row sits in trade industries."""
    _, use = _frames()
    published = published_row(use, 'T00TOP')
    trade = [c for c in published.index if str(c).startswith(TRADE_PREFIXES)]
    return {
        'total': float(published.sum()),
        'trade': float(published[trade].sum()),
        'trade_share': float(published[trade].sum() / published.sum()),
    }


def duties_lookup() -> dict[str, float]:
    """``4200ID`` carries the whole of ``MDTY``, exactly."""
    supply, use = _frames()
    commodities = list(USA_2017_COMMODITY_CODES)
    return {
        'supply_mdty': float(supply.loc[commodities, 'MDTY'].astype(float).sum()),
        'use_4200ID': float(published_row(use, 'T00TOP')['4200ID']),
    }


def report() -> None:
    print(f'Commodity -> industry conversion of the product-tax rows, {YEAR}\n')
    print(f"{'row':<22}{'corr':>7}{'|error|':>14}{'of row':>9}")
    for name, s in conversion_scores().items():
        print(
            f"{name:<22}{s['correlation']:>7.3f}"
            f"{s['absolute_error']:>14,.0f}{s['error_share']:>9.1%}"
        )
    concentration = trade_concentration()
    print(
        f"\npublished T00TOP in trade industries: "
        f"{concentration['trade']:,.0f} of {concentration['total']:,.0f} "
        f"= {concentration['trade_share']:.1%}"
    )
    duties = duties_lookup()
    print(
        f"import duties: Supply MDTY {duties['supply_mdty']:,.0f} vs "
        f"Use T00TOP on 4200ID {duties['use_4200ID']:,.0f}"
    )


def check() -> int:
    """Assert the findings this module's docstring rests on.

    Analysis modules here carry their checks as a CLI flag rather than as unit
    tests.  The point is not to protect the conversion -- it is unusable -- but
    to fail if a later Supply or Use vintage makes the *argument* untrue, since
    Step 2 and Step 5 both lean on it.
    """
    failures = []
    scores = conversion_scores()
    for name, s in scores.items():
        if s['correlation'] >= USABLE_CORRELATION:
            failures.append(
                f'{name}: correlation {s["correlation"]:.3f} is now usable '
                f'(>= {USABLE_CORRELATION}); revisit the decision to leave the '
                f'industry split to the balance'
            )
    if scores['T00TOP (TOP + MDTY)']['error_share'] < 0.5:
        failures.append('T00TOP conversion error has fallen below 50% of the row')

    concentration = trade_concentration()
    if concentration['trade_share'] < 0.4:
        failures.append(
            f'trade industries now hold only '
            f'{concentration["trade_share"]:.1%} of T00TOP; the point-of-sale '
            f'argument no longer holds'
        )

    duties = duties_lookup()
    if abs(duties['supply_mdty'] - duties['use_4200ID']) > 25:
        failures.append(
            f'4200ID {duties["use_4200ID"]:,.0f} no longer equals Supply MDTY '
            f'{duties["supply_mdty"]:,.0f}; the duties lookup is not exact'
        )

    for failure in failures:
        print(f'FAIL: {failure}')
    if not failures:
        print('OK: all findings hold')
    return 1 if failures else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--check',
        action='store_true',
        help='assert the findings rather than printing the report',
    )
    args = parser.parse_args()
    if args.check:
        return check()
    report()
    return 0


if __name__ == '__main__':
    sys.exit(main())
