from __future__ import annotations

import dataclasses
import functools

import numpy as np
import pandas as pd

from bedrock.extract.iot.io_2017 import load_2017_margins_usa
from bedrock.transform.eeio.derived_2017_helpers import EXPANDED_SECTORS_2012_TO_2017
from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.economic.inflation_helpers_cornerstone import (
    get_sector_commodity_price_ratio,
    get_vnorm_adjusted_commodity_price_ratio,
)
from bedrock.utils.taxonomy.bea.v2017_final_demand import USA_2017_FINAL_DEMAND_CODES
from bedrock.utils.taxonomy.usa_taxonomy_correspondence_helpers import (
    USA_2017_COMMODITY_INDEX,
    load_usa_2017_commodity__ceda_v7_correspondence,
    load_usa_2017_commodity__cornerstone_commodity_correspondence,
)


@dataclasses.dataclass(frozen=True)
class MarginsFilters:
    """Codes to exclude from the margins table before commodity aggregation.

    Both sets filter on the raw ``(Industry Code, Commodity Code)`` MultiIndex,
    so any row where *either* code appears in the corresponding exclude set is
    dropped before the groupby-sum step.
    """

    exclude_commodity_codes: frozenset[str] = dataclasses.field(
        default_factory=frozenset
    )
    exclude_industry_codes: frozenset[str] = dataclasses.field(
        default_factory=frozenset
    )


# Exclude all final demand destinations — the CEDA pipeline only wants
# industry-to-industry margin flows.
_ceda_margins_filters: MarginsFilters = MarginsFilters(
    exclude_industry_codes=frozenset(USA_2017_FINAL_DEMAND_CODES)
)

# Exclude BEA bookkeeping commodities that are not real product flows:
#   S00401 Scrap, S00402 Used and secondhand goods,
#   S00300 Noncomparable imports, S00900 Rest of the world adjustment
# Exclude final demand destinations whose margins distort commodity-level ratios:
#   F04000 Exports, F05000 Imports, F03000 Change in private inventories
_useeio_margins_filters: MarginsFilters = MarginsFilters(
    exclude_commodity_codes=frozenset({"S00401", "S00402", "S00300", "S00900"}),
    exclude_industry_codes=frozenset({"F04000", "F05000", "F03000"}),
)


def set_ceda_margins_filters(filters: MarginsFilters) -> None:
    """Set the margins filters applied in the CEDA pipeline."""
    global _ceda_margins_filters
    _ceda_margins_filters = filters


def set_useeio_margins_filters(filters: MarginsFilters) -> None:
    """Set the margins filters applied in the USEEIO pipeline."""
    global _useeio_margins_filters
    _useeio_margins_filters = filters


def _get_active_margins_filters() -> MarginsFilters:
    """Return the active filter set based on config flags.

    ``apply_useeio_margins_filters`` takes precedence when
    ``use_useeio_schema`` is True; otherwise ``apply_ceda_margins_filters``
    controls the CEDA/Cornerstone path. Returns an empty ``MarginsFilters``
    (no-op) when the relevant flag is off.
    """
    cfg = get_usa_config()
    if cfg.use_useeio_schema and cfg.apply_useeio_margins_filters:
        return _useeio_margins_filters
    if cfg.apply_ceda_margins_filters:
        return _ceda_margins_filters
    return MarginsFilters()


def _apply_margins_filter(df: pd.DataFrame, filters: MarginsFilters) -> pd.DataFrame:
    """Drop rows from a margins MultiIndex DataFrame based on ``filters``."""
    if not filters.exclude_commodity_codes and not filters.exclude_industry_codes:
        return df
    mask = pd.Series(True, index=df.index)
    if filters.exclude_commodity_codes:
        mask &= ~df.index.get_level_values("Commodity Code").isin(
            filters.exclude_commodity_codes
        )
    if filters.exclude_industry_codes:
        mask &= ~df.index.get_level_values("Industry Code").isin(
            filters.exclude_industry_codes
        )
    return df.loc[mask]


def _margins_by_commodity(
    filters: MarginsFilters,
    abs_negative_producers_value: bool = False,
) -> pd.DataFrame:
    """Load raw margins, apply ``filters``, and sum to per-commodity totals.

    When ``abs_negative_producers_value`` is ``True``, any row with a negative
    ``Producers' Value`` has its sign flipped before the groupby-sum.
    """
    df = _apply_margins_filter(load_2017_margins_usa(), filters)
    if abs_negative_producers_value:
        mask = df["Producers' Value"] < 0
        df.loc[mask, "Producers' Value"] = df.loc[mask, "Producers' Value"].abs()
    return (
        df.groupby(level="Commodity Code")
        .sum()
        .reindex(USA_2017_COMMODITY_INDEX)
        .fillna(0.0)
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

    filters = (
        _ceda_margins_filters
        if get_usa_config().apply_ceda_margins_filters
        else MarginsFilters()
    )
    margin = corresp @ _margins_by_commodity(filters)
    # assume expanded_sectors will receive equal portion of value from aggregated sector
    margin.loc[EXPANDED_SECTORS_2012_TO_2017, :] *= 1 / len(
        EXPANDED_SECTORS_2012_TO_2017
    )

    return (margin["Producers' Value"] / margin["Purchasers' Value"]).replace(
        [np.inf, -np.inf, np.nan], 1.0
    )


def derive_2017_margins_cornerstone_usa() -> pd.DataFrame:
    """
    Margins aggregated to Cornerstone commodity taxonomy, summed over all industries.
    Routes before/after BEA redefinitions via ``USAConfig.iot_before_or_after_redefinition``.
    Applies the active pipeline's ``MarginsFilters`` before aggregation.

    Returns a DataFrame indexed by Cornerstone ``COMMODITIES`` with columns:
    ``Producers' Value``, ``Transportation``, ``Wholesale``, ``Retail``,
    ``Purchasers' Value``. Unit is USD.
    """
    cfg = get_usa_config()
    corresp = load_usa_2017_commodity__cornerstone_commodity_correspondence()
    return corresp @ _margins_by_commodity(
        _get_active_margins_filters(),
        abs_negative_producers_value=cfg.use_useeio_schema
        and cfg.apply_useeio_margins_filters,
    )


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
