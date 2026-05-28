from __future__ import annotations

import functools

import numpy as np
import pandas as pd

from bedrock.extract.iot.io_2017 import load_2017_margins_usa
from bedrock.transform.eeio.derived_2017_helpers import EXPANDED_SECTORS_2012_TO_2017
from bedrock.utils.economic.inflation_helpers_cornerstone import (
    get_sector_commodity_price_ratio,
    get_vnorm_adjusted_commodity_price_ratio,
)
from bedrock.utils.taxonomy.usa_taxonomy_correspondence_helpers import (
    USA_2017_COMMODITY_INDEX,
    load_usa_2017_commodity__ceda_v7_correspondence,
    load_usa_2017_commodity__cornerstone_commodity_correspondence,
)


def derive_2017_producer_to_purchaser_price_ratio_ceda_usa() -> pd.Series[float]:
    """
    Derive the ratio to convert EF from producer to purchaser price for each CEDA v7 sector.
    Formula: purchaser price = producer price + margin
    Since original EF is in kgCO2e/USD_producer, ratio here is calculated as
    (output_producer / (output_producer + margin)).
    """
    corresp = load_usa_2017_commodity__ceda_v7_correspondence()
    corresp.columns.names = ["commodity"]

    margin = corresp @ (
        load_2017_margins_usa()
        .groupby(level="Commodity Code")
        .sum()
        .reindex(USA_2017_COMMODITY_INDEX)
        .fillna(0.0)
    )
    # assume expanded_sectors will receive equal portion of value from aggregated sector
    margin.loc[EXPANDED_SECTORS_2012_TO_2017, :] *= 1 / len(
        EXPANDED_SECTORS_2012_TO_2017
    )

    return (margin["Producers' Value"] / margin["Purchasers' Value"]).replace(
        [np.inf, -np.inf, np.nan], 1.0
    )


@functools.cache
def derive_2017_margins_cornerstone_usa() -> pd.DataFrame:
    """
    Margins aggregated to Cornerstone commodity taxonomy, summed over all industries.
    Routes before/after BEA redefinitions via ``USAConfig.iot_before_or_after_redefinition``.

    Returns a DataFrame indexed by Cornerstone ``COMMODITIES`` with columns:
    ``Producers' Value``, ``Transportation``, ``Wholesale``, ``Retail``,
    ``Purchasers' Value``. Unit is USD.
    """
    corresp = load_usa_2017_commodity__cornerstone_commodity_correspondence()

    margins_by_commodity = (
        load_2017_margins_usa()
        .groupby(level="Commodity Code")
        .sum()
        .reindex(USA_2017_COMMODITY_INDEX)
        .fillna(0.0)
    )

    return corresp @ margins_by_commodity


@functools.cache
def derive_2017_margins_cornerstone_inflated_usa(
    original_year: int, target_year: int
) -> pd.DataFrame:
    """
    Margins aggregated to Cornerstone commodity taxonomy, inflated from
    ``original_year`` to ``target_year``.

    ``Producers' Value`` is inflated using the V-norm-weighted commodity price
    index (same basis as ``inflate_cornerstone_q_or_y_with_commodity_pi``).

    ``Transportation``, ``Wholesale``, and ``Retail`` are inflated using the
    ITA-based sector commodity price ratio for BEA sector codes 48TW, 42,
    and 44RT respectively (see ``get_sector_commodity_price_ratio``).

    ``Purchasers' Value`` is recomputed as the sum of the four inflated
    components to preserve internal consistency.
    """
    df = derive_2017_margins_cornerstone_usa().copy()

    commodity_pi = get_vnorm_adjusted_commodity_price_ratio(original_year, target_year)
    df["Producers' Value"] *= commodity_pi.reindex(df.index, fill_value=1.0)

    sector_pi = get_sector_commodity_price_ratio(original_year, target_year)
    df["Transportation"] *= sector_pi["48TW"]
    df["Wholesale"] *= sector_pi["42"]
    df["Retail"] *= sector_pi["44RT"]

    df["Purchasers' Value"] = (
        df["Producers' Value"] + df["Transportation"] + df["Wholesale"] + df["Retail"]
    )
    return df


@functools.cache
def derive_2017_pur_price_ratio_cornerstone_usa() -> pd.Series:
    """
    Producer-to-purchaser price ratio per Cornerstone commodity.

    Computed as ``Producers' Value / Purchasers' Value`` from the margins table.
    Rows where the purchaser price is zero (``inf`` / ``nan``) are set to ``1.0``
    (no margin adjustment). Routes before/after BEA redefinitions via
    ``USAConfig.iot_before_or_after_redefinition``.
    """
    margins = derive_2017_margins_cornerstone_usa()
    return (margins["Producers' Value"] / margins["Purchasers' Value"]).replace(
        [np.inf, -np.inf, np.nan], 1.0
    )
