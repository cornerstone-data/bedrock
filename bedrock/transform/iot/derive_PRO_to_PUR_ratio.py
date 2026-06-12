"""
Functions to process and filter Margins data
for various models, triggered by *_margins config vars

- Prepares Margins tabels using filters and other
margins treatment and recalculates PUR price from
PRO price and margins
- Generates phi (PRO:PUR) ratios, generally in model_base_year
unless otherwise specified

"""

from __future__ import annotations

import dataclasses
import functools

import numpy as np
import pandas as pd

from bedrock.extract.iot.io_2017 import load_2017_margins_usa
from bedrock.transform.eeio.derived_2017_helpers import EXPANDED_SECTORS_2012_TO_2017
from bedrock.utils.config.usa_config import USAConfig, get_usa_config
from bedrock.utils.economic.inflation_helpers_cornerstone import (
    get_rho_inflation_ratio,
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

# All BEA 2017 detailed commodity codes whose code begins with "4"
# (wholesale trade, retail trade, transportation, and warehousing sectors).
_COMMODITY_CODES_STARTING_WITH_4: frozenset[str] = frozenset(
    {
        '4200ID',
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
        '441000',
        '444000',
        '445000',
        '446000',
        '447000',
        '448000',
        '452000',
        '454000',
        '481000',
        '482000',
        '483000',
        '484000',
        '485000',
        '486000',
        '48A000',
        '491000',
        '492000',
        '493000',
        '4B0000',
    }
)

# Exclude BEA bookkeeping commodities that are not real product flows or special commodities:
#   S00401 Scrap, S00402 Used and secondhand goods,
#   S00300 Noncomparable imports, S00900 Rest of the world adjustment
# Exclude final demand destinations whose margins distort commodity-level ratios:
#   F04000 Exports, F03000 Change in private inventories
# # Matches exported ``useeior`` ``model$Margins``: keep Import ``F05000`` rows because
# R ``purchaser_removal`` uses ``%in%`` on a length-3 vector; drop only Export and
# change-in-inventories industries; scrap/RoW commodities only (no ``4*``).
_useeio_margins_filters: MarginsFilters = MarginsFilters(
    exclude_commodity_codes=frozenset({'S00401', 'S00402', 'S00300', 'S00900'}),
    exclude_industry_codes=frozenset({'F04000', 'F03000'}),
)

# Cornerstone filters
# Exclude BEA bookkeeping commodities that are removed from model as well as scrap
#   S00300 Noncomparable imports, S00900 Rest of the world adjustment, S00401 Scrap
# Exclude wholesale, retail, transportation, and warehousing commodity flows.
# Exclude final demand destinations whose margins do not reflect industry consumers
# or consumption within the US. Exclude import and export and all final uses
# except nonresidential investment (F02E00, F02N00, F02S00) and change in
# private inventories (F03000)
# Exclude state and local government industries (GSLGE, GSLGH, GSLGO).
# See justification here:
# https://github.com/cornerstone-data/methods/discussions/25
_cornerstone_industry_avg_margins_filters: MarginsFilters = MarginsFilters(
    exclude_commodity_codes=frozenset({'S00401', 'S00300', 'S00900'})
    | _COMMODITY_CODES_STARTING_WITH_4,
    exclude_industry_codes=frozenset(USA_2017_FINAL_DEMAND_CODES)
    - frozenset({'F03000', 'F02E00', 'F02N00', 'F02S00'})
    | frozenset({'GSLGE', 'GSLGH', 'GSLGO'}),
)


def _get_active_margins_filters() -> MarginsFilters:
    """Return the active filter set based on config flags.

    ``useeio_margins`` takes precedence; otherwise ``cornerstone_industry_avg_margins``
    controls the Cornerstone path, then ``ceda_margins`` the
    CEDA path. Returns an empty ``MarginsFilters`` (no-op) when no flag is set.
    """
    cfg = get_usa_config()
    if cfg.useeio_margins:
        return _useeio_margins_filters
    if cfg.cornerstone_industry_avg_margins:
        return _cornerstone_industry_avg_margins_filters
    if cfg.ceda_margins:
        return _ceda_margins_filters
    return MarginsFilters()


def _apply_margins_filter(df: pd.DataFrame, filters: MarginsFilters) -> pd.DataFrame:
    """Drop rows from a margins MultiIndex DataFrame based on ``filters``."""
    if not filters.exclude_commodity_codes and not filters.exclude_industry_codes:
        return df
    mask = pd.Series(True, index=df.index)
    if filters.exclude_commodity_codes:
        mask &= ~df.index.get_level_values('Commodity Code').isin(
            filters.exclude_commodity_codes
        )
    if filters.exclude_industry_codes:
        mask &= ~df.index.get_level_values('Industry Code').isin(
            filters.exclude_industry_codes
        )
    return df.loc[mask]


_MARGIN_VALUE_COLUMNS = ("Producers' Value", 'Transportation', 'Wholesale', 'Retail')


def _margin_negatives_treatment(
    df: pd.DataFrame,
    abs_negative_producers_value: bool = False,
    abs_negative_margin_columns: bool = False,
) -> pd.DataFrame:
    """Flip negative margin values to positive in-place.

    ``abs_negative_margin_columns`` (triggered by ``cornerstone_industry_avg_margins`` config
    flag) flips negatives across all four margin columns and takes precedence.
    ``abs_negative_producers_value`` flips only ``Producers' Value``.
    """
    if abs_negative_margin_columns:
        for col in _MARGIN_VALUE_COLUMNS:
            mask = df[col] < 0
            df.loc[mask, col] = df.loc[mask, col].abs()
    elif abs_negative_producers_value:
        mask = df["Producers' Value"] < 0
        df.loc[mask, "Producers' Value"] = df.loc[mask, "Producers' Value"].abs()
    return df


def _margins_by_commodity(
    filters: MarginsFilters,
    abs_negative_producers_value: bool = False,
    abs_negative_margin_columns: bool = False,
) -> pd.DataFrame:
    """Load raw margins, apply ``filters``, and sum to per-commodity totals."""
    df = _apply_margins_filter(load_2017_margins_usa(), filters)
    df = _margin_negatives_treatment(
        df,
        abs_negative_producers_value=abs_negative_producers_value,
        abs_negative_margin_columns=abs_negative_margin_columns,
    )
    result = (
        df.groupby(level='Commodity Code')
        .sum()
        .reindex(USA_2017_COMMODITY_INDEX)
        .fillna(0.0)
    )
    # Recompute Purchasers' Value from its components after aggregation so it
    # stays consistent regardless of which negatives treatment was applied.
    result["Purchasers' Value"] = (
        result["Producers' Value"]
        + result['Transportation']
        + result['Wholesale']
        + result['Retail']
    )
    return result


def derive_2017_margins_ceda_usa() -> pd.DataFrame:
    """
    Margins aggregated to CEDA v7 sector taxonomy, summed over all industries.
    Applies ``_ceda_margins_filters`` when ``USAConfig.ceda_margins`` is set.

    Returns a DataFrame indexed by CEDA v7 sectors with columns:
    ``Producers' Value``, ``Transportation``, ``Wholesale``, ``Retail``,
    ``Purchasers' Value``. Unit is USD.
    """
    corresp = load_usa_2017_commodity__ceda_v7_correspondence()
    corresp.columns.names = ['commodity']
    filters = (
        _ceda_margins_filters if get_usa_config().ceda_margins else MarginsFilters()
    )
    margin = corresp @ _margins_by_commodity(filters)
    # Expanded sectors share value equally from the aggregated 2012 sector.
    margin.loc[EXPANDED_SECTORS_2012_TO_2017, :] *= 1 / len(
        EXPANDED_SECTORS_2012_TO_2017
    )
    return margin


def derive_phi_ceda_usa() -> pd.Series[float]:
    """
    Derive the Phi ratio to convert EF from producer to purchaser price for each CEDA v7 sector.
    Formula: purchaser price = producer price + margin
    Since original EF is in kgCO2e/USD_producer, Phi here is calculated as
    (output_producer / (output_producer + margin)).
    """
    margin = derive_2017_margins_ceda_usa()
    phi = margin["Producers' Value"] / margin["Purchasers' Value"]
    avg_mask = (phi > 0) & (phi <= 1)
    avg = phi[avg_mask].mean()
    in_range_mask = (phi > 0) & (phi < 1)
    phi[~in_range_mask] = avg
    return phi


def _inflate_margin_trade_components(
    df: pd.DataFrame, *, original_year: int, target_year: int
) -> pd.DataFrame:
    """Scale transportation, wholesale, and retail margin columns to *target_year*."""
    sector_pi = get_sector_commodity_price_ratio(original_year, target_year)
    out = df.copy()
    out['Transportation'] *= sector_pi['48TW']
    out['Wholesale'] *= sector_pi['42']
    out['Retail'] *= sector_pi['44RT']
    return out


def _inflate_margins_bedrock_style(
    df: pd.DataFrame, *, original_year: int, target_year: int
) -> pd.DataFrame:
    """Inflate producer margin with V-norm commodity PI (Cornerstone convention)."""
    out = df.copy()
    commodity_pi = get_vnorm_adjusted_commodity_price_ratio(original_year, target_year)
    out["Producers' Value"] *= commodity_pi.reindex(out.index, fill_value=1.0)
    return _inflate_margin_trade_components(
        out, original_year=original_year, target_year=target_year
    )


def _inflate_margins_useeior_style(
    df: pd.DataFrame, *, original_year: int, target_year: int
) -> pd.DataFrame:
    """Inflate producer margin with per-sector Rho (useeior convention)."""
    out = df.copy()
    rho = get_rho_inflation_ratio(original_year, target_year)
    out["Producers' Value"] *= rho.reindex(out.index, fill_value=1.0)
    return _inflate_margin_trade_components(
        out, original_year=original_year, target_year=target_year
    )


def _inflate_margins_to_year(df: pd.DataFrame, *, target_year: int) -> pd.DataFrame:
    """Inflate margin components from ``usa_base_io_data_year`` to *target_year*."""
    cfg = get_usa_config()
    if not (cfg.useeio_margins or cfg.cornerstone_industry_avg_margins):
        return df
    original_year = cfg.usa_base_io_data_year
    if original_year == target_year:
        return df
    if cfg.useeio_margins:
        return _inflate_margins_useeior_style(
            df, original_year=original_year, target_year=target_year
        )
    return _inflate_margins_bedrock_style(
        df, original_year=original_year, target_year=target_year
    )


@functools.cache
def derive_margins_cornerstone_usa_at_year(target_year: int) -> pd.DataFrame:
    """
    Margins aggregated to Cornerstone commodity taxonomy, summed over all industries.

    Margin components inflate from ``usa_base_io_data_year`` to *target_year* when
    a margins methodology flag is active. PRO inflation follows the useeior ``Rho``
    path when ``useeio_margins`` is set; otherwise the V-norm commodity PI path
    (``cornerstone_industry_avg_margins``).

    Returns a DataFrame indexed by Cornerstone ``COMMODITIES`` with columns:
    ``Producers' Value``, ``Transportation``, ``Wholesale``, ``Retail``,
    ``Purchasers' Value``. Unit is USD.
    """
    cfg = get_usa_config()
    corresp = load_usa_2017_commodity__cornerstone_commodity_correspondence()
    df = corresp @ _margins_by_commodity(
        _get_active_margins_filters(),
        abs_negative_producers_value=cfg.useeio_margins,
        abs_negative_margin_columns=cfg.cornerstone_industry_avg_margins,
    )
    df = _inflate_margins_to_year(df, target_year=target_year)
    df["Purchasers' Value"] = (
        df["Producers' Value"] + df['Transportation'] + df['Wholesale'] + df['Retail']
    )
    return df


def derive_margins_cornerstone_usa() -> pd.DataFrame:
    """Margins at ``model_base_year`` (alias for :func:`derive_margins_cornerstone_usa_at_year`)."""
    return derive_margins_cornerstone_usa_at_year(get_usa_config().model_base_year)


def _phi_from_margins(margins: pd.DataFrame) -> pd.Series[float]:
    return (margins["Producers' Value"] / margins["Purchasers' Value"]).replace(
        [np.inf, -np.inf, np.nan], 1.0
    )


@functools.cache
def derive_phi_cornerstone_usa_at_year(target_year: int) -> pd.Series[float]:
    """
    Phi (PRO:PUR) per Cornerstone commodity for *target_year* USD margins.

    Rows where purchaser price is zero (``inf`` / ``nan``) are set to ``1.0``.
    """
    return _phi_from_margins(derive_margins_cornerstone_usa_at_year(target_year))


def derive_phi_cornerstone_usa() -> pd.Series[float]:
    """Phi at ``model_base_year``."""
    return derive_phi_cornerstone_usa_at_year(get_usa_config().model_base_year)


def margins_phi_active(cfg: USAConfig | None = None) -> bool:
    """Return whether margins-based Phi should be applied for *cfg*."""
    c = cfg or get_usa_config()
    return bool(c.useeio_margins or c.cornerstone_industry_avg_margins)


def phi_for_sectors(sector_index: pd.Index) -> pd.Series[float]:
    """Phi aligned to *sector_index*; identity when margins methodology is inactive."""
    if not margins_phi_active():
        return pd.Series(1.0, index=sector_index, dtype=float)
    return derive_phi_cornerstone_usa().reindex(sector_index, fill_value=1.0)


def apply_phi_to_ef_vector(ef: pd.Series[float]) -> pd.Series[float]:
    """Convert producer-price EFs to purchaser price via sector Phi."""
    return ef * phi_for_sectors(ef.index)
