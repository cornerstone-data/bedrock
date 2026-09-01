"""The seller × commodity matrix — which trade industry sells which products
(#745).

Step 6b's margin redistribution needs the wholesale/retail margin distributed
back to the **specific detail trade codes** that earned it (Wes, 2026-08-31:
all detail trade rows, never a wholesale-vs-retail two-way split). Step 4c's
give-up supplies each trade code's margin *total*; what it cannot say is the
commodity-specific *mix* — whether a buyer's food margin belongs to grocery
wholesale or machinery wholesale. That mix is what the Economic Census
product-line data observes, and the inventories attribution already computes
it: ``Census_EC_PxI`` product-line sales per selling industry, prepared by
:func:`~bedrock.extract.census.Census_EC.prepare_pxi_for_attribution`.

**This module is retention, not new data** (#745's framing): it runs the very
same preparation chain the ``Inventories_<year>`` FBS runs — suppression
recovery, product-to-activity reshape, synthetic-line addition, vintage
interpolation — and reads off the axis that chain *parks* rather than drops:
after the reshape the selling industry's NAICS sits in ``FlowName``, the
product in ``ActivityConsumedBy``, and the mapped BEA commodity one crosswalk
join away. The FBS then collapses the seller at aggregation because an FBS
has only two sector axes; here the pair survives as its own matrix.

The matrix is **annual**: ``pxi_weights_for_year`` interpolates the product
mix between the 2017 and 2022 census vintages and holds it after, so Step 6b
gets a seller mix that moves with the census rather than a frozen 2017 table.

⚠️ **The synthetic lines are excluded.**
:data:`~bedrock.extract.census.Census_EC.SYNTHETIC_PXI_LINES` adds "General
merchandise stores" and "Nonmerchant wholesale, nondurable goods" as
attribution *weights* that duplicate value already present under their
component lines — summing them into a matrix double-counts, the exact trap
the issue records. Their sellers' true product mixes are already in the
matrix under the component industries.

⚠️ **Values are weights, not dollars to add.** Product-line sales are the
observation; use each seller's row as *shares* (which products this industry
sells) or each commodity's column as shares (which sellers sell this
product). Step 6b wants the column reading: margin per commodity split
across sellers.

Run::

    uv run python -m bedrock.transform.inventories.seller_matrix --year 2017
"""

from __future__ import annotations

import argparse
import functools
import sys

import pandas as pd

from bedrock.extract.census.Census_EC import (
    SYNTHETIC_PXI_LINES,
    estimate_suppressed_ec_pxi,
    move_pxi_product_to_activity,
    pxi_weights_for_year,
    synthesize_missing_trade_lines,
)
from bedrock.extract.flowbyactivity import getFlowByActivity
from bedrock.utils.config.settings import crosswalkpath

#: The census PxI vintage to load; the interpolation reaches every nowcast
#: year from it. Must stay 2017 — see ``prepare_pxi_for_attribution``.
PXI_VINTAGE = 2017

#: BEA 2017 detail trade commodity codes and the NAICS prefixes each spans.
#: Longest prefix wins; retail families without their own detail code
#: (furniture 442, electronics 443, sporting 451, miscellaneous 453) belong
#: to ``4B0000`` "all other retail". ``454000`` nonstore keeps its own code.
BEA_TRADE_PREFIXES: dict[str, tuple[str, ...]] = {
    '423100': ('4231',),
    '423400': ('4234',),
    '423600': ('4236',),
    '423800': ('4238',),
    '423A00': ('4232', '4233', '4235', '4237', '4239'),
    '424200': ('4242',),
    '424400': ('4244',),
    '424700': ('4247',),
    '424A00': ('4241', '4243', '4245', '4246', '4248', '4249'),
    '425000': ('4251',),
    '441000': ('441',),
    '444000': ('444',),
    '445000': ('445',),
    '446000': ('446',),
    '447000': ('447',),
    '448000': ('448',),
    '452000': ('452',),
    '454000': ('454',),
    '4B0000': ('442', '443', '451', '453'),
}


@functools.cache
def _prepared_pxi(target_year: int) -> pd.DataFrame:
    """The PxI frame exactly as the Inventories FBS attribution sees it.

    The same chain ``prepare_pxi_for_attribution`` runs, called function by
    function so the target year is explicit rather than read off a config.
    """
    fba = pd.DataFrame(getFlowByActivity('Census_EC_PxI', PXI_VINTAGE))
    prepared = synthesize_missing_trade_lines(
        move_pxi_product_to_activity(estimate_suppressed_ec_pxi(fba))
    )
    return pd.DataFrame(pxi_weights_for_year(prepared, int(target_year)))


@functools.cache
def _product_to_commodity() -> pd.DataFrame:
    """Normalized product description -> BEA 2017 commodity, from the PxI
    crosswalk the FBS attribution maps through.

    The crosswalk is 1:m for some products; a product's value splits equally
    across its mapped commodities (``split`` column) — the unweighted rule,
    stated rather than hidden.
    """
    crosswalk = pd.read_csv(
        crosswalkpath / 'Sector_Crosswalk_Census_EC_PxI.csv', dtype=str
    )
    out = pd.DataFrame(
        {
            'product': crosswalk['Activity'].str.strip(),
            'commodity': crosswalk['Sector'].str.strip(),
        }
    ).dropna()
    out['split'] = 1.0 / out.groupby('product')['commodity'].transform('count')
    return out


def seller_commodity_matrix(target_year: int) -> pd.DataFrame:
    """Selling industry NAICS × BEA commodity weights for *target_year*.

    Rows are the census selling industries (the finest observed grain, ~115
    six-digit trade NAICS); columns are BEA 2017 commodities; values are
    product-line sales weights on the interpolated vintage mix. Synthetic
    lines excluded (module docstring).
    """
    prepared = _prepared_pxi(int(target_year))
    synthetic = prepared['ActivityProducedBy'].astype(str).isin(SYNTHETIC_PXI_LINES)
    real = prepared.loc[~synthetic].copy()

    frame = pd.DataFrame(
        {
            'seller': real['FlowName'].astype(str).str.strip(),
            'product': real['ActivityConsumedBy'].astype(str).str.strip(),
            'value': pd.to_numeric(real['FlowAmount'], errors='coerce').fillna(0.0),
        }
    )
    frame = frame[(frame['value'] > 0) & frame['seller'].str.fullmatch(r'\d{6}')]
    joined = frame.merge(_product_to_commodity(), on='product', how='inner')
    joined['value'] = joined['value'] * joined['split']
    matrix = (
        joined.groupby(['seller', 'commodity'])['value'].sum().unstack().fillna(0.0)
    )
    matrix.index.name = 'seller_naics'
    matrix.columns.name = 'commodity'
    return matrix


def bea_trade_code_for_seller(naics: str) -> str | None:
    """The BEA 2017 detail trade commodity a selling NAICS belongs to.

    Longest prefix wins; ``None`` for a NAICS outside the trade families
    (PxI also covers some non-trade industries the matrix keeps at their own
    grain).
    """
    code = str(naics)
    best: tuple[int, str] | None = None
    for bea, prefixes in BEA_TRADE_PREFIXES.items():
        for prefix in prefixes:
            if code.startswith(prefix) and (best is None or len(prefix) > best[0]):
                best = (len(prefix), bea)
    return best[1] if best else None


def bea_trade_matrix(target_year: int) -> pd.DataFrame:
    """The matrix rolled to BEA detail trade commodity codes on the seller
    axis — the grain Step 6b's redistribution consumes."""
    fine = seller_commodity_matrix(int(target_year))
    rolled = pd.Series(
        {naics: bea_trade_code_for_seller(naics) for naics in fine.index}
    )
    keep = rolled.notna()
    out = fine.loc[keep].groupby(rolled[keep]).sum()
    out.index.name = 'bea_trade_commodity'
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--year', type=int, default=2017)
    args = parser.parse_args(argv)

    fine = seller_commodity_matrix(args.year)
    rolled = bea_trade_matrix(args.year)
    pairs = int((fine > 0).to_numpy().sum())
    print(
        f'{args.year}: {fine.shape[0]} sellers x {fine.shape[1]} commodities, '
        f'{pairs} observed pairs'
    )
    print(
        f'rolled to {rolled.shape[0]} BEA trade codes; weight totals per code '
        f'($M-equivalent of the census weights):'
    )
    print((rolled.sum(axis=1) / 1e3).round(0).to_string())
    return 0


if __name__ == '__main__':
    sys.exit(main())
