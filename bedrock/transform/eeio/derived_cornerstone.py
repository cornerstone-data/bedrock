"""Cornerstone IO data processing pipeline.

Derives detail IO matrices (V, U, Y, A, B, g, q) using the Cornerstone 2026
taxonomy (405 sectors), from the published BEA 2017 tables or a nowcast year's
MUT via ``bedrock.extract.iot.detail_io``.

**Core approach** — A is computed in the original BEA 2017 ~400-sector
space and then *expanded* to 405 Cornerstone sectors by duplicating
rows/columns for disaggregated codes. V, U, and Y are mapped via
correspondence-matrix multiplication. B is computed directly in
Cornerstone space from runtime `derive_E_usa()`. Waste subsectors receive
special intragroup treatment to prevent Leontief-inverse inflation.

Year-scaling logic (summary → detail disaggregation) uses the cornerstone
summary correspondence instead of the CEDA v7 version.

This module is self-contained: it does NOT modify or gate any existing CEDA v7
code paths. The caller decides which pipeline to invoke based on config.

Internal helpers live in sibling modules:
- ``cornerstone_disagg_pipeline`` — waste/electricity sector-disaggregation orchestration
- ``cornerstone_expansion`` — BEA ↔ Cornerstone correspondence & expansion
- ``cornerstone_bea_intermediates`` — BEA-space intermediate matrices
- ``cornerstone_year_scaling`` — summary-ratio year-scaling for A, q, B
"""

from __future__ import annotations

import functools
import typing as ta
from typing import cast

import numpy as np
import pandas as pd
import pandera.typing as pt

from bedrock.extract.iot.detail_io import (
    load_detail_Uimp_usa,
    load_detail_Utot_usa,
    load_detail_V_usa,
    load_detail_value_added_usa,
    load_detail_Ytot_usa,
)
from bedrock.transform.allocation.derived import derive_E_usa
from bedrock.transform.eeio.cornerstone_bea_intermediates import (
    bea_Aq,
)
from bedrock.transform.eeio.cornerstone_disagg_pipeline import (
    build_electricity_mixed_units_aq,
    build_electricity_mixed_units_b,
    cornerstone_sector_disagg_active,
    derive_disagg_io_bundle,
    derive_disagg_Ytot_with_trade,
    distribute_waste_parent_x_using_v_row_shares,
    electricity_conversion_factors,
    electricity_disaggregation_enabled,
    electricity_mixed_units_enabled,
)
from bedrock.transform.eeio.cornerstone_expansion import (
    CS_COMMODITY_LIST,
    commodity_corresp,
    cs_commodity_to_bea_map,
    expand_industry_output_vector,
    expand_square_matrix,
    expand_vector,
    industry_corresp,
)
from bedrock.transform.eeio.cornerstone_year_scaling import (
    scale_cornerstone_A,
    scale_cornerstone_B,
    scale_cornerstone_q,
)
from bedrock.transform.eeio.derived_2017 import (
    derive_summary_Yimp_usa,
    derive_summary_Ytot_usa_matrix_set,
)
from bedrock.transform.iot.derive_PRO_to_PUR_ratio import (
    derive_margins_cornerstone_usa,
)
from bedrock.transform.iot.derived_gross_industry_output import (
    derive_gross_output,
)
from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.economic.inflation_helpers_cornerstone import (
    get_cornerstone_industry_price_ratio,
    inflate_cornerstone_A_matrix_with_commodity_pi,
    inflate_cornerstone_A_matrix_with_industry_pi,
    inflate_cornerstone_B_matrix_with_industry_pi,
    inflate_cornerstone_q_or_y_with_commodity_pi,
    inflate_cornerstone_q_or_y_with_industry_pi,
    inflate_cornerstone_V_with_industry_pi,
)
from bedrock.utils.math.disaggregation import disaggregate_vector
from bedrock.utils.math.formulas import (
    backcompute_y_from_A_and_q,
    compute_q,
    compute_Unorm_matrix,
    compute_Vnorm_matrix,
    compute_x,
)
from bedrock.utils.math.handle_negatives import (
    handle_negative_matrix_values,
    handle_negative_vector_values,
)
from bedrock.utils.math.split_using_aggregated_weights import (
    split_vector_using_agg_ratio,
)
from bedrock.utils.schemas.cornerstone_schemas import (
    ELECTRICITY_AGGREGATE_SECTOR,
    ELECTRICITY_DISAGG_SECTORS,
    validate_cornerstone,
)
from bedrock.utils.schemas.single_region_schemas import AMatrix, UMatrix
from bedrock.utils.schemas.single_region_types import (
    SingleRegionAqMatrixSet,
    SingleRegionUMatrixSet,
    SingleRegionYtotAndTradeVectorSet,
    SingleRegionYVectorSet,
)
from bedrock.utils.taxonomy.bea.matrix_mappings import USA_GROSS_INDUSTRY_OUTPUT_YEARS
from bedrock.utils.taxonomy.bea.v2017_final_demand import (
    USA_2017_FINAL_DEMAND_EXPORT_CODE,
    USA_2017_FINAL_DEMAND_IMPORT_CODE,
    USA_2017_FINAL_DEMAND_PERSONAL_CONSUMPTION_EXPENDITURE_CODE,
)
from bedrock.utils.taxonomy.bea_v2017_to_cornerstone_helpers import (
    get_bea_v2017_summary_to_cornerstone_corresp_df,
)
from bedrock.utils.taxonomy.mappings.bea_v2017_sector__cornerstone_commodity import (
    load_margin_type_to_cornerstone_commodity,
)


def _cornerstone_aq_matrix_set(
    Adom: pd.DataFrame,
    Aimp: pd.DataFrame,
    scaled_q: pd.Series[float],
) -> SingleRegionAqMatrixSet:
    validate_cornerstone(Adom, 'A')
    validate_cornerstone(Aimp, 'A')
    validate_cornerstone(scaled_q, 'Q')
    # Cornerstone A uses 405/407-sector taxonomy; cast only for mypy — do not
    # use pt.DataFrame[AMatrix](...) which runs CEDA v7 Pandera validation.
    return SingleRegionAqMatrixSet(
        Adom=cast(pt.DataFrame[AMatrix], Adom),
        Aimp=cast(pt.DataFrame[AMatrix], Aimp),
        scaled_q=scaled_q,
    )


# ---------------------------------------------------------------------------
# Baseline IO (correspondence only — no waste, no electricity)
# ---------------------------------------------------------------------------


def _derive_cornerstone_V_baseline() -> pd.DataFrame:
    V_detail = load_detail_V_usa()  # published 2017 or nowcast year, per the router
    V = industry_corresp() @ V_detail @ commodity_corresp().T
    V.index.name = 'sector'
    V.columns.name = 'sector'
    return V


def _derive_cornerstone_U_baseline() -> tuple[pd.DataFrame, pd.DataFrame]:
    Utot = load_detail_Utot_usa()
    Uimp = load_detail_Uimp_usa()
    Udom = Utot - Uimp

    com_c = commodity_corresp()
    ind_c = industry_corresp()

    Udom_cs = com_c @ Udom @ ind_c.T
    Uimp_cs = com_c @ Uimp @ ind_c.T

    for df in (Udom_cs, Uimp_cs):
        df.index.name = 'sector'
        df.columns.name = 'sector'

    return Udom_cs, Uimp_cs


def _derive_cornerstone_VA_baseline() -> pd.DataFrame:
    VA = load_detail_value_added_usa() @ industry_corresp().T
    VA.columns.name = 'sector'
    return VA


def _derive_cornerstone_Ytot_baseline() -> pd.DataFrame:
    Ytot_orig = load_detail_Ytot_usa()
    Ytot = commodity_corresp() @ Ytot_orig
    Ytot.index.name = 'sector'
    return Ytot


def _ytot_for_public_routers() -> pd.DataFrame:
    if cornerstone_sector_disagg_active():
        return derive_disagg_Ytot_with_trade().copy()
    return _derive_cornerstone_Ytot_baseline()


# ---------------------------------------------------------------------------
# Base detail IO matrices (published 2017 or nowcast year) — V, g, q
# ---------------------------------------------------------------------------


@functools.cache
def derive_cornerstone_V(
    apply_inflation: bool = False, target_year: int = 0
) -> pd.DataFrame:
    """V matrix (industry × commodity) via correspondence multiplication."""
    if cornerstone_sector_disagg_active():
        V = derive_disagg_io_bundle().V.copy()
    else:
        V = _derive_cornerstone_V_baseline()

    if apply_inflation:
        V = inflate_cornerstone_V_with_industry_pi(V, target_year=target_year)
    validate_cornerstone(V, 'V')
    return V


@functools.cache
def derive_cornerstone_x() -> pd.Series[float]:
    x = compute_x(V=derive_cornerstone_V())
    validate_cornerstone(x, 'X')
    return x


def _distribute_waste_parent_x_using_v_row_shares(
    x_cs: pd.Series[float],
) -> pd.Series[float]:
    """Split duplicated BEA parent gross output across waste children using ``V`` row-sum shares.

    After industry-output expand, one-to-many BEA→Cornerstone splits
    (e.g. 562000) assign the **full** parent total to **each** child. When
    waste disaggregation is enabled, replace those rows with
    ``parent_go * (x_v[i] / sum_j x_v[j])`` where ``x_v`` is row sums of
    uninflated disaggregated ``V`` (2017-detail Make structure as mapped to
    Cornerstone) and ``parent_go`` is the duplicated scalar (GHG-year \$ scale).
    """
    return distribute_waste_parent_x_using_v_row_shares(x_cs)


@functools.cache
def derive_cornerstone_x_after_redefinition(year: int = 0) -> pd.Series[float]:
    """Gross industry output in Cornerstone schema, after BEA redefinitions.

    ``usa_detail_io_source == 'nowcast'``: the detail Make the router loads is
    already the after-redefinition nowcast table for ``usa_base_io_data_year``
    (Step 7), so industry output is its row sum, ``derive_cornerstone_x()``.
    Waste and electricity splits are already inside that ``V``; nothing is
    expanded from a BEA gross-output series. *year* must be 0 or
    ``usa_base_io_data_year`` - a nowcast Make exists for one calendar year
    only.

    ``bea_published``: uses gross-output time series for *year* (defaults to
    ``usa_ghg_data_year`` when *year* is 0), selecting before/after-redefinition
    source from config, then expands it to Cornerstone industries via the
    BEA→Cornerstone industry correspondence.

    Industry gross output is expanded with
    ``expand_industry_output_vector`` so many-to-one aggregates (government
    electric / transit enterprises into ``221100`` / ``485000``) sum BEA
    parents. For one-to-many splits (e.g. waste 562000), that expand first
    duplicates the parent scalar to each child. When waste disaggregation is
    on, those waste rows are then replaced so each child gets a share of the
    parent total consistent with row sums of disaggregated ``V`` (same nominal
    level as the BEA gross output for *year*, split from 2017 Make structure).

    This is the industry ``x`` in ``derive_cornerstone_B_via_vnorm`` when
    ``use_ghg_year_x_in_B`` (apply_io_year_adjustments) is set; otherwise that path uses
    ``derive_cornerstone_x()``.
    """
    cfg = get_usa_config()
    if cfg.usa_detail_io_source == 'nowcast':
        if year not in (0, cfg.usa_base_io_data_year):
            raise ValueError(
                'nowcast detail IO carries one calendar year; x is available at '
                f'usa_base_io_data_year={cfg.usa_base_io_data_year}, got year={year}'
            )
        return derive_cornerstone_x()
    effective_year = (
        cfg.usa_ghg_data_year
        if year == 0
        else cast('USA_GROSS_INDUSTRY_OUTPUT_YEARS', year)
    )
    x_bea = derive_gross_output(
        target_year=effective_year,
        iot_before_or_after_redefinition=cfg.iot_before_or_after_redefinition,
    )
    x_cs = expand_industry_output_vector(x_bea)
    x_cs.index.name = 'sector'
    x_out = _distribute_waste_parent_x_using_v_row_shares(x_cs)
    validate_cornerstone(x_out, 'X')
    return x_out


@functools.cache
def derive_cornerstone_q() -> pd.Series[float]:
    q = compute_q(V=derive_cornerstone_V())
    validate_cornerstone(q, 'Q')
    return q


@functools.cache
def derive_cornerstone_Vnorm_scrap_corrected(
    apply_inflation: bool | None = None,
    target_year: int = 0,
) -> pd.DataFrame:
    """Scrap-corrected V norm. Inflation is applied via ``derive_cornerstone_V``.

    When ``target_year`` is not positive and inflation is on, uses
    ``USAConfig.model_base_year``.
    """
    use_inflation = bool(apply_inflation)
    effective_year = (
        target_year
        if target_year > 0
        else (get_usa_config().model_base_year if use_inflation else 0)
    )

    V = derive_cornerstone_V(use_inflation, effective_year)
    q = compute_q(V=V)
    x = compute_x(V=V)

    Vnorm = compute_Vnorm_matrix(V=V, q=q)

    scrap_detail = load_detail_V_usa().loc[:, 'S00401']
    scrap_fraction = industry_corresp() @ scrap_detail
    if get_usa_config().implement_electricity_disaggregation:
        parent_scrap = float(scrap_fraction.get(ELECTRICITY_AGGREGATE_SECTOR, 0.0))
        scrap_fraction = scrap_fraction.drop(
            ELECTRICITY_AGGREGATE_SECTOR, errors='ignore'
        )
        for code in ELECTRICITY_DISAGG_SECTORS:
            scrap_fraction.loc[code] = parent_scrap
    scrap_fraction = scrap_fraction.reindex(V.index, fill_value=0.0)
    x_aligned = x.reindex(V.index, fill_value=0.0)

    V_scrap_corrected = Vnorm.divide(
        (1.0 - (scrap_fraction / x_aligned).fillna(0.0)), axis=0
    )
    V_scrap_corrected = V_scrap_corrected.reindex(
        index=V.index, columns=V.columns, fill_value=0.0
    )
    validate_cornerstone(V_scrap_corrected, 'V')
    return V_scrap_corrected


# ---------------------------------------------------------------------------
# Base detail IO matrices (published 2017 or nowcast year) — U
# ---------------------------------------------------------------------------


@functools.cache
def derive_cornerstone_U_with_negatives() -> SingleRegionUMatrixSet:
    if cornerstone_sector_disagg_active():
        bundle = derive_disagg_io_bundle()
        Udom_cs, Uimp_cs = bundle.Udom.copy(), bundle.Uimp.copy()
    else:
        Udom_cs, Uimp_cs = _derive_cornerstone_U_baseline()
    validate_cornerstone(Udom_cs, 'U')
    validate_cornerstone(Uimp_cs, 'U')
    return SingleRegionUMatrixSet(
        Udom=cast(pt.DataFrame[UMatrix], Udom_cs),
        Uimp=cast(pt.DataFrame[UMatrix], Uimp_cs),
    )


@functools.cache
def derive_cornerstone_U_set() -> SingleRegionUMatrixSet:
    uset = derive_cornerstone_U_with_negatives()
    Udom = handle_negative_matrix_values(uset.Udom)
    Uimp = handle_negative_matrix_values(uset.Uimp)
    assert not (Udom < 0).any().any(), 'Udom has negative values.'
    assert not (Uimp < 0).any().any(), 'Uimp has negative values.'
    validate_cornerstone(Udom, 'U')
    validate_cornerstone(Uimp, 'U')
    return SingleRegionUMatrixSet(
        Udom=cast(pt.DataFrame[UMatrix], Udom),
        Uimp=cast(pt.DataFrame[UMatrix], Uimp),
    )


# ---------------------------------------------------------------------------
# Base detail IO matrices (published 2017 or nowcast year) — Y
# ---------------------------------------------------------------------------


def derive_cornerstone_Ytot_full_cs_matrix() -> pd.DataFrame:
    """Full commodity-by-final-demand ``Y`` in Cornerstone space (incl. trade FD columns).

    Returns a **copy** of the gated public-router Y pipeline so callers cannot
    mutate cached state.
    """
    return _ytot_for_public_routers()


@functools.cache
def derive_cornerstone_Ytot_matrix_set() -> SingleRegionYtotAndTradeVectorSet:
    Ytot_with_trade = _ytot_for_public_routers()
    return SingleRegionYtotAndTradeVectorSet(
        ytot=handle_negative_vector_values(
            Ytot_with_trade.drop(
                columns=[
                    USA_2017_FINAL_DEMAND_EXPORT_CODE,
                    USA_2017_FINAL_DEMAND_IMPORT_CODE,
                ]
            ).sum(axis=1)
        ),
        exports=Ytot_with_trade[USA_2017_FINAL_DEMAND_EXPORT_CODE],
        imports=(
            -1
            * Ytot_with_trade[USA_2017_FINAL_DEMAND_IMPORT_CODE].apply(
                lambda x: np.min(x, 0)
            )
        ),
    )


def derive_cornerstone_Y_personal_consumption_expenditure() -> pd.Series[float]:
    return _ytot_for_public_routers()[
        USA_2017_FINAL_DEMAND_PERSONAL_CONSUMPTION_EXPENDITURE_CODE
    ]


# ---------------------------------------------------------------------------
# Base detail IO matrices (published 2017 or nowcast year) — VA
# ---------------------------------------------------------------------------


@functools.cache
def derive_cornerstone_VA() -> pd.DataFrame:
    """Value Added (VA rows × 405 Cornerstone industries) via correspondence.

    Callers needing Cornerstone-space VA should use this function rather than
    assembling VA manually.
    """
    if cornerstone_sector_disagg_active():
        return derive_disagg_io_bundle().VA.copy()
    return _derive_cornerstone_VA_baseline()


# ---------------------------------------------------------------------------
# A matrices and q — expanded from BEA space
# ---------------------------------------------------------------------------


@functools.cache
def derive_cornerstone_Aq() -> SingleRegionAqMatrixSet:
    """Base 2017 A matrices and q.

    When waste disaggregation is **off**: A is computed in BEA ~400-sector
    space and expanded to 405 Cornerstone commodities by duplicating
    rows/columns. Intragroup treatment is applied to prevent Leontief-inverse
    inflation.

    When waste disaggregation is **on**: A and q are derived directly in
    Cornerstone space from disaggregated V and U. No intragroup treatment
    is applied — the waste block already reflects real CSV weights.
    """
    if cornerstone_sector_disagg_active():
        return _derive_cornerstone_Aq_from_disaggregated()

    Adom_bea, Aimp_bea, q_bea = bea_Aq()
    com_map = cs_commodity_to_bea_map()

    Adom = expand_square_matrix(
        Adom_bea, CS_COMMODITY_LIST, com_map, zero_intragroup_cross_terms=True
    )
    Aimp = expand_square_matrix(
        Aimp_bea, CS_COMMODITY_LIST, com_map, zero_intragroup_cross_terms=True
    )
    q = expand_vector(q_bea, CS_COMMODITY_LIST, com_map)
    q.index.name = 'sector'

    assert (Adom >= 0).all().all(), 'Adom has negative values.'
    assert (Aimp >= 0).all().all(), 'Aimp has negative values.'
    assert (q >= 0).all(), 'q has negative values.'

    return _cornerstone_aq_matrix_set(Adom=Adom, Aimp=Aimp, scaled_q=q)


def _derive_cornerstone_Aq_from_disaggregated() -> SingleRegionAqMatrixSet:
    """A and q from disaggregated Cornerstone V and U (no intragroup treatment)."""
    V = derive_cornerstone_V()
    uset = derive_cornerstone_U_set()
    Udom: pd.DataFrame = uset.Udom
    Uimp: pd.DataFrame = uset.Uimp

    q = compute_q(V=V)
    x = compute_x(V=V)
    Vnorm = derive_cornerstone_Vnorm_scrap_corrected()

    Adom = compute_Unorm_matrix(U=Udom, x=x) @ Vnorm
    Aimp = compute_Unorm_matrix(U=Uimp, x=x) @ Vnorm

    Adom.index.name = 'sector'
    Adom.columns.name = 'sector'
    Aimp.index.name = 'sector'
    Aimp.columns.name = 'sector'

    return _cornerstone_aq_matrix_set(Adom=Adom, Aimp=Aimp, scaled_q=q)


# ---------------------------------------------------------------------------
# Scaled / inflated A, q
# ---------------------------------------------------------------------------


def _reanchor_electricity_aq_if_disaggregation_enabled(
    aq: SingleRegionAqMatrixSet,
    *,
    original_year: int,
    target_year: int,
    model_year: int,
    use_commodity_pi: bool,
) -> SingleRegionAqMatrixSet:
    """Re-apply EIA end-use class shares at model year onto A/q when enabled.

    No-op when electricity disaggregation is off. Otherwise delegates to
    ``reanchor_electricity_aq_after_year_scaling``, which re-runs the allocator
    so class MWh targets come from EIA Table 2.2 / 2.14 at ``model_year``
    (not the scaled 2017 structure) and rewrites electricity rows/columns of
    A and q after year scaling and price-index inflation.
    """
    if not electricity_disaggregation_enabled():
        return aq
    from bedrock.transform.eeio.electricity_gtd_allocation import (  # noqa: PLC0415
        reanchor_electricity_aq_after_year_scaling,
    )

    out = reanchor_electricity_aq_after_year_scaling(
        aq,
        original_year=original_year,
        target_year=target_year,
        model_year=model_year,
        use_commodity_pi=use_commodity_pi,
    )

    return _cornerstone_aq_matrix_set(
        Adom=out.Adom, Aimp=out.Aimp, scaled_q=out.scaled_q
    )


@functools.cache
def derive_cornerstone_Aq_scaled() -> SingleRegionAqMatrixSet:
    """Return model-ready A matrices and ``q``.

    On the published-BEA path, scales detail A and ``q`` from
    ``usa_detail_original_year`` to ``usa_io_data_year``, then inflates to
    ``model_base_year``. When electricity disaggregation is enabled, re-anchors
    electricity rows/columns after inflation.

    No-op shortcuts (return ``derive_cornerstone_Aq()`` unchanged):

    - ``usa_detail_io_source == 'nowcast'`` — detail IO is already at the IO
      calendar year; summary-ratio scaling and PI inflation do not apply.
    - ``scale_a_matrix_with_useeio_method`` — USEEIO-parity A path.
    """
    base = derive_cornerstone_Aq()
    cfg = get_usa_config()
    if cfg.usa_detail_io_source == 'nowcast':
        return base
    io_year = cfg.usa_io_data_year
    detail_year = cfg.usa_detail_original_year
    model_year = cfg.model_base_year

    # USEEIO method: return 2017 base A unchanged — no scaling, no inflation.
    if cfg.scale_a_matrix_with_useeio_method:
        return base

    # 1. Scale detail A and q to usa_io_data_year.
    #    With apply_io_year_adjustments, scale_cornerstone_A/_q rebase the
    #    target-year summary tables into 2017 USD before the ratio is taken,
    #    so the structural cross-year ratio is formed entirely in 2017 USD.
    Adom = scale_cornerstone_A(
        base.Adom,
        target_year=io_year,
        original_year=detail_year,
        dom_or_imp_or_total='dom',
    )
    Aimp = scale_cornerstone_A(
        base.Aimp,
        target_year=io_year,
        original_year=detail_year,
        dom_or_imp_or_total='imp',
    )
    q = scale_cornerstone_q(
        base.scaled_q,
        target_year=io_year,
        original_year=detail_year,
    )

    # 2. Inflate to model_base_year (commodity pi for v0.3 IO-year path;
    #    industry pi otherwise).
    if cfg.apply_io_year_adjustments:
        Adom = inflate_cornerstone_A_matrix_with_commodity_pi(
            Adom, original_year=detail_year, target_year=model_year
        )
        Aimp = inflate_cornerstone_A_matrix_with_commodity_pi(
            Aimp, original_year=detail_year, target_year=model_year
        )
        q = inflate_cornerstone_q_or_y_with_commodity_pi(
            q, original_year=detail_year, target_year=model_year
        )
        use_commodity_pi = True
    else:
        Adom = inflate_cornerstone_A_matrix_with_industry_pi(
            Adom, original_year=io_year, target_year=model_year
        )
        Aimp = inflate_cornerstone_A_matrix_with_industry_pi(
            Aimp, original_year=io_year, target_year=model_year
        )
        q = inflate_cornerstone_q_or_y_with_industry_pi(
            q, original_year=io_year, target_year=model_year
        )
        use_commodity_pi = False

    # 3. Re-apply electricity end-use class shares at model year when enabled.
    return _reanchor_electricity_aq_if_disaggregation_enabled(
        _cornerstone_aq_matrix_set(Adom=Adom, Aimp=Aimp, scaled_q=q),
        original_year=int(detail_year),
        target_year=int(io_year),
        model_year=int(model_year),
        use_commodity_pi=use_commodity_pi,
    )


# ---------------------------------------------------------------------------
# Margin A matrix (A_margin)
# ---------------------------------------------------------------------------


def _margin_sector_commodity_output_ratio(
    q: pd.Series[float], margin_sector_commodities: ta.Mapping[str, ta.Sequence[str]]
) -> pd.Series[float]:
    """Each margin-sector commodity's share of its group's total ``q``.

    Analogous to useeior's ``calculateOutputRatio(model, output_type="Commodity")``
    ``toSectorRatio`` column, restricted to the Transportation/Wholesale/Retail
    BEA-Sector groups used to allocate margins.
    """
    ratios: dict[str, float] = {}
    for codes in margin_sector_commodities.values():
        group_q = q.loc[list(codes)]
        group_total = group_q.sum()
        ratios.update((group_q / group_total).to_dict() if group_total else {})
    return pd.Series(ratios, dtype=float)


@functools.cache
def derive_cornerstone_A_margin() -> pd.DataFrame:
    """Margin-provider A matrix (commodity supplying margin x commodity purchased).

    Python port of useeior's ``calculateMarginSectorImpacts`` A_margin
    construction: for each Cornerstone commodity, its Transportation/Wholesale/
    Retail margin fraction of producer price (from ``derive_margins_cornerstone_usa()``,
    the equivalent of ``model$Margins``) is allocated across the Cornerstone
    commodities in the corresponding BEA-Sector margin group in proportion to
    each commodity's share of that group's total ``derive_cornerstone_q()``
    output (the equivalent of ``model$q``).

    Row ``i`` (a margin-providing commodity) gives, for every column ``j``
    (purchasing commodity), the fraction of ``j``'s producer-price purchase
    that flows to commodity ``i`` to cover ``j``'s trade/transportation margin.
    Non-margin-providing rows are all zero.
    """
    margins = derive_margins_cornerstone_usa()
    margin_sector_commodities = load_margin_type_to_cornerstone_commodity()
    margin_types = list(margin_sector_commodities)
    margin_coefficients = (
        margins[margin_types]
        .div(margins["Producers' Value"], axis=0)
        .replace([np.inf, -np.inf, np.nan], 0.0)
    )

    output_ratio = _margin_sector_commodity_output_ratio(
        derive_cornerstone_q(), margin_sector_commodities
    )

    margin_allocation = pd.DataFrame(0.0, index=margin_types, columns=CS_COMMODITY_LIST)
    for margin_type, codes in margin_sector_commodities.items():
        code_list = list(codes)
        margin_allocation.loc[margin_type, code_list] = output_ratio.loc[code_list]

    margins_by_sector = margin_coefficients @ margin_allocation

    A_margin = pd.DataFrame(0.0, index=CS_COMMODITY_LIST, columns=CS_COMMODITY_LIST)
    A_margin.index.name = 'sector'
    A_margin.columns.name = 'sector'
    for codes in margin_sector_commodities.values():
        code_list = list(codes)
        A_margin.loc[code_list, :] = margins_by_sector[code_list].T
    return A_margin


# ---------------------------------------------------------------------------
# B matrix (runtime E path)
# ---------------------------------------------------------------------------


def derive_cornerstone_B_via_vnorm() -> pd.DataFrame:
    """B (ghg × Cornerstone commodity).

    Always computed in Cornerstone space: E = derive_E_usa(), then B = (E / x) @ Vnorm.
    Industry ``x`` is:
    - ``usa_detail_io_source == 'nowcast'``: row sums of the nowcast Make,
      ``derive_cornerstone_x()``. E and the Make share one calendar year
      (``usa_ghg_data_year == usa_base_io_data_year``, validator-enforced), so
      no gross-output series and no deflation enter.
    - ``deflate_x_to_detail_io_year_for_B=True``: gross output from the BEA
      gross-output time series at ``usa_ghg_data_year`` (nominal), divided by
      ``PI(usa_ghg_data_year)/PI(usa_detail_original_year)`` so ``E/x`` uses
      ``usa_detail_original_year`` chain dollars. ``USAConfig`` requires
      ``use_ghg_year_x_in_B`` to be true whenever deflation is on; the
      deflate branch always builds nominal ``x`` via
      ``derive_cornerstone_x_after_redefinition()`` before the PI ratio, so
      ``use_ghg_year_x_in_B`` does not further branch choice here.
    - otherwise: ``derive_cornerstone_x_after_redefinition()`` when
      ``use_ghg_year_x_in_B`` is True, else ``derive_cornerstone_x()``.
    No BEA intermediate or expand_ghg_matrix_from_bea_to_cornerstone.
    """
    cfg = get_usa_config()
    E = derive_E_usa()
    if cfg.usa_detail_io_source == 'nowcast':
        x = derive_cornerstone_x()
    elif cfg.deflate_x_to_detail_io_year_for_B:
        # Deflate GHG-year nominal gross output to detail IO year ($) for E/x:
        #   1) nominal industry output at usa_ghg_data_year
        #   2) divide by PI(ghg)/PI(detail) so x matches usa_detail_original_year $
        #   3) divide E by adjusted industry output; map to commodities via Vnorm
        x_nominal = derive_cornerstone_x_after_redefinition()
        ratio = get_cornerstone_industry_price_ratio(
            original_year=cfg.usa_detail_original_year,
            target_year=cfg.usa_ghg_data_year,
        )
        # ratio is PI_target / PI_original; divide nominal GHG-year dollars
        # so x is expressed in usa_detail_original_year chain dollars for E/x.
        ratio_aligned = ratio.reindex(x_nominal.index)
        ratio_aligned = ratio_aligned.where(ratio_aligned.notna(), 1.0)
        x = x_nominal / ratio_aligned
    else:
        x = (
            derive_cornerstone_x_after_redefinition()
            if cfg.use_ghg_year_x_in_B
            else derive_cornerstone_x()
        )
    Vnorm = derive_cornerstone_Vnorm_scrap_corrected()
    Bi = E.divide(x, axis=1).fillna(0.0)
    B = Bi @ Vnorm
    validate_cornerstone(B, 'B')
    return B


@functools.cache
def derive_cornerstone_B_non_finetuned() -> pd.DataFrame:
    """Year-scaled + inflated B, derived self-contained from CEDA v7 → cornerstone.

    When ``use_ghg_year_x_in_B`` is true, B is already on the GHG-year footing
    and stays on vnorm only. On the legacy footing, B is scaled 2017 →
    ``usa_io_data_year`` with summary q ratios and then inflated to
    ``model_base_year`` with the industry PI.

    No-op shortcut, mirroring ``derive_cornerstone_Aq_scaled``: when
    ``usa_detail_io_source == 'nowcast'``, E, x and A already share the IO
    calendar year, so summary-ratio scaling to ``usa_io_data_year`` and PI
    inflation to ``model_base_year`` do not apply.
    """
    cfg = get_usa_config()
    if cfg.use_ghg_year_x_in_B or cfg.usa_detail_io_source == 'nowcast':
        return derive_cornerstone_B_via_vnorm()
    return inflate_cornerstone_B_matrix_with_industry_pi(
        scale_cornerstone_B(
            B=derive_cornerstone_B_via_vnorm(),
            original_year=cfg.usa_detail_original_year,
            target_year=cfg.usa_io_data_year,
        ),
        original_year=cfg.usa_io_data_year,
        target_year=cfg.model_base_year,
    )


@functools.cache
def derive_cornerstone_Aq_mixed_units() -> SingleRegionAqMatrixSet:
    """Mixed-unit A/q (221110 in MWh) when gate is on; else monetary scaled A/q."""
    return build_electricity_mixed_units_aq(derive_cornerstone_Aq_scaled())


@functools.cache
def derive_cornerstone_B_mixed_units() -> pd.DataFrame:
    """Mixed-unit B (221110 column CO2/MWh) when gate is on; else monetary B."""
    aq = derive_cornerstone_Aq_scaled()
    c_col, _ = electricity_conversion_factors(aq)
    return build_electricity_mixed_units_b(derive_cornerstone_B_non_finetuned(), c_col)


# ---------------------------------------------------------------------------
# Y vectors — disaggregation + inflation
# ---------------------------------------------------------------------------


def _disaggregate_and_inflate_vector(
    base: pd.Series[float],
    weight: pd.Series[float],
    corresp_df: pd.DataFrame,
    *,
    original_year: int,
    target_year: int,
    clip_negatives: bool = False,
) -> pd.Series[float]:
    """Disaggregate a summary vector to detail and inflate to target year."""
    v = disaggregate_vector(
        base_series=base,
        weight_series=weight,
        corresp_df=corresp_df,
    )
    if clip_negatives:
        v = handle_negative_vector_values(v)
    return inflate_cornerstone_q_or_y_with_industry_pi(
        v,
        original_year=original_year,
        target_year=target_year,
    )


def derive_cornerstone_Y_and_trade_scaled() -> SingleRegionYtotAndTradeVectorSet:
    """Year-scaled Y, exports, imports."""
    detail_2017 = derive_cornerstone_Ytot_matrix_set()
    summary_Y = derive_summary_Ytot_usa_matrix_set(get_usa_config().usa_io_data_year)
    cfg = get_usa_config()

    common = dict(
        corresp_df=get_bea_v2017_summary_to_cornerstone_corresp_df(),
        original_year=cfg.usa_io_data_year,
        target_year=cfg.model_base_year,
    )
    ytot = _disaggregate_and_inflate_vector(
        summary_Y.ytot,
        detail_2017.ytot,
        **common,  # type: ignore[arg-type]
    )
    exports = _disaggregate_and_inflate_vector(
        summary_Y.exports,
        detail_2017.exports,
        **common,  # type: ignore[arg-type]
    )
    imports = _disaggregate_and_inflate_vector(
        summary_Y.imports,
        detail_2017.imports,
        clip_negatives=True,
        **common,  # type: ignore[arg-type]
    )
    if electricity_disaggregation_enabled():
        from bedrock.transform.eeio.electricity_gtd_allocation import (  # noqa: PLC0415
            collapse_electricity_imports_onto_generation,
        )

        imports = collapse_electricity_imports_onto_generation(imports)

    return SingleRegionYtotAndTradeVectorSet(
        ytot=ytot, exports=exports, imports=imports
    )


@functools.cache
def derive_cornerstone_y_nab() -> pd.Series[float]:
    """National-accounting final demand consistent with scaled ``Adom`` and ``q``.

    Enforces row balance ``q = Adom @ diag(q) + y_nab`` using the same
    ``derive_cornerstone_Aq_scaled`` object whose ``scaled_q`` is snapshotted.
    Any future change to ``scaled_q`` in that path propagates to ``y_nab``.

    Negative values are retained so ``q ≈ L_dom @ y_nab`` holds numerically;
    clipping would break the domestic Leontief identity.
    """
    aq = derive_cornerstone_Aq_scaled()
    return backcompute_y_from_A_and_q(A=aq.Adom, q=aq.scaled_q)


@functools.cache
def derive_cornerstone_y_nab_mixed_units() -> pd.Series[float]:
    """Hybrid final demand for mixed-unit BLy diagnostics (221110 in MWh).

    When the mixed-units gate is off, delegates to monetary
    ``derive_cornerstone_y_nab``. Does not alter monetary ``y_nab`` consumers.
    """
    if not electricity_mixed_units_enabled():
        return derive_cornerstone_y_nab()
    aq = derive_cornerstone_Aq_mixed_units()
    return backcompute_y_from_A_and_q(A=aq.Adom, q=aq.scaled_q)


def derive_cornerstone_ydom_and_yimp() -> SingleRegionYVectorSet:
    """Split ytot into ydom and yimp using summary ratios."""
    summary_2022_ytot = derive_summary_Ytot_usa_matrix_set(2022).ytot
    summary_2022_yimp = derive_summary_Yimp_usa(2022).sum(axis=1)

    summary_2022_ydom_over_ytot_ratio = handle_negative_vector_values(
        1 - (summary_2022_yimp / summary_2022_ytot).fillna(0.0)
    )

    summary_corresp = get_bea_v2017_summary_to_cornerstone_corresp_df()
    detail_2022_ytot = disaggregate_vector(
        corresp_df=summary_corresp,
        base_series=summary_2022_ytot,
        weight_series=derive_cornerstone_Ytot_matrix_set().ytot,
    )
    ydom, yimp = split_vector_using_agg_ratio(
        base_series=detail_2022_ytot,
        agg_ratio_series=summary_2022_ydom_over_ytot_ratio,
        corresp_df=summary_corresp,
    )
    return SingleRegionYVectorSet(ydom=ydom, yimp=yimp)


def _disaggregate_ytot_matrix_set(
    summary_Y: SingleRegionYtotAndTradeVectorSet,
    detail_weights: SingleRegionYtotAndTradeVectorSet,
    corresp_df: pd.DataFrame,
) -> SingleRegionYtotAndTradeVectorSet:
    """Disaggregate summary ytot/exports/imports to detail using weights."""
    return SingleRegionYtotAndTradeVectorSet(
        ytot=disaggregate_vector(
            base_series=summary_Y.ytot,
            weight_series=detail_weights.ytot,
            corresp_df=corresp_df,
        ),
        exports=disaggregate_vector(
            base_series=summary_Y.exports,
            weight_series=detail_weights.exports,
            corresp_df=corresp_df,
        ),
        imports=handle_negative_vector_values(
            disaggregate_vector(
                base_series=summary_Y.imports,
                weight_series=detail_weights.imports,
                corresp_df=corresp_df,
            )
        ),
    )


def derive_cornerstone_detail_Ytot_matrix_set() -> SingleRegionYtotAndTradeVectorSet:
    """Year-scaled detail Ytot (equivalent of derive_v7_detail_Ytot_usa_matrix_set)."""
    return _disaggregate_ytot_matrix_set(
        summary_Y=derive_summary_Ytot_usa_matrix_set(year=2022),
        detail_weights=derive_cornerstone_Ytot_matrix_set(),
        corresp_df=get_bea_v2017_summary_to_cornerstone_corresp_df(),
    )
