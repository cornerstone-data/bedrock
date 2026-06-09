"""Cornerstone IO data processing pipeline.

Derives 2017 detail IO matrices (V, U, Y, A, B, g, q) using the
Cornerstone 2026 taxonomy (405 sectors).

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
from typing import cast

import numpy as np
import pandas as pd
import pandera.typing as pt

from bedrock.extract.iot.io_2017 import (
    load_2017_Uimp_usa,
    load_2017_Utot_usa,
    load_2017_V_usa,
    load_2017_value_added_usa,
    load_2017_Ytot_usa,
)
from bedrock.transform.allocation.derived import derive_E_usa
from bedrock.transform.eeio.cornerstone_bea_intermediates import (
    bea_Aq,
)
from bedrock.transform.eeio.cornerstone_disagg_pipeline import (
    cornerstone_sector_disagg_active,
    derive_disagg_io_bundle,
    derive_disagg_Ytot_with_trade,
    distribute_waste_parent_x_using_v_row_shares,
)
from bedrock.transform.eeio.cornerstone_expansion import (
    CS_COMMODITY_LIST,
    CS_INDUSTRY_LIST,
    commodity_corresp,
    cs_commodity_to_bea_map,
    cs_industry_to_bea_map,
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
from bedrock.transform.eeio.electricity_disaggregation import (
    split_electricity_e_for_disaggregated_b,
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
from bedrock.utils.taxonomy.bea_v2017_to_ceda_v7_helpers import (
    get_bea_v2017_summary_to_cornerstone_corresp_df,
)


def _cornerstone_aq_matrix_set(
    Adom: pd.DataFrame,
    Aimp: pd.DataFrame,
    scaled_q: pd.Series[float],
) -> SingleRegionAqMatrixSet:
    validate_cornerstone(Adom, "A")
    validate_cornerstone(Aimp, "A")
    validate_cornerstone(scaled_q, "Q")
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
    V_2017 = load_2017_V_usa()
    V = industry_corresp() @ V_2017 @ commodity_corresp().T
    V.index.name = 'sector'
    V.columns.name = 'sector'
    return V


def _derive_cornerstone_U_baseline() -> tuple[pd.DataFrame, pd.DataFrame]:
    Utot = load_2017_Utot_usa()
    Uimp = load_2017_Uimp_usa()
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
    VA = load_2017_value_added_usa() @ industry_corresp().T
    VA.columns.name = 'sector'
    return VA


def _derive_cornerstone_Ytot_baseline() -> pd.DataFrame:
    Ytot_orig = load_2017_Ytot_usa()
    Ytot = commodity_corresp() @ Ytot_orig
    Ytot.index.name = 'sector'
    return Ytot


def _ytot_for_public_routers() -> pd.DataFrame:
    if cornerstone_sector_disagg_active():
        return derive_disagg_Ytot_with_trade().copy()
    return _derive_cornerstone_Ytot_baseline()


# ---------------------------------------------------------------------------
# Base 2017 IO matrices — V, g, q
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
    validate_cornerstone(V, "V")
    return V


@functools.cache
def derive_cornerstone_x() -> pd.Series[float]:
    x = compute_x(V=derive_cornerstone_V())
    validate_cornerstone(x, "X")
    return x


def _distribute_waste_parent_x_using_v_row_shares(
    x_cs: pd.Series[float],
) -> pd.Series[float]:
    """Split duplicated BEA parent gross output across waste children using ``V`` row-sum shares.

    After ``expand_vector``, one-to-many BEA→Cornerstone splits (e.g. 562000)
    assign the **full** parent total to **each** child. When waste
    disaggregation is enabled, replace those rows with
    ``parent_go * (x_v[i] / sum_j x_v[j])`` where ``x_v`` is row sums of
    uninflated disaggregated ``V`` (2017-detail Make structure as mapped to
    Cornerstone) and ``parent_go`` is the duplicated scalar (GHG-year \$ scale).
    """
    return distribute_waste_parent_x_using_v_row_shares(x_cs)


@functools.cache
def derive_cornerstone_x_after_redefinition(year: int = 0) -> pd.Series[float]:
    """Gross industry output in Cornerstone schema, after BEA redefinitions.

    Uses gross-output time series for *year* (defaults to
    ``usa_ghg_data_year`` when *year* is 0), selecting before/after-redefinition
    source from config, then expands it to Cornerstone industries via the
    BEA→Cornerstone industry correspondence.

    For one-to-many splits (e.g. waste 562000), ``expand_vector`` first
    duplicates the parent scalar to each child. When waste disaggregation is
    on, those waste rows are then replaced so each child gets a share of the
    parent total consistent with row sums of disaggregated ``V`` (same nominal
    level as the BEA gross output for *year*, split from 2017 Make structure).

    This is the industry ``x`` in ``derive_cornerstone_B_via_vnorm`` when
    ``use_E_data_year_for_x_in_B`` is True; otherwise that path uses
    ``derive_cornerstone_x()``.
    """
    cfg = get_usa_config()
    effective_year = (
        cfg.usa_ghg_data_year
        if year == 0
        else cast('USA_GROSS_INDUSTRY_OUTPUT_YEARS', year)
    )
    x_bea = derive_gross_output(
        target_year=effective_year,
        iot_before_or_after_redefinition=cfg.iot_before_or_after_redefinition,
    )
    x_cs = expand_vector(x_bea, CS_INDUSTRY_LIST, cs_industry_to_bea_map())
    x_cs.index.name = "sector"
    x_out = _distribute_waste_parent_x_using_v_row_shares(x_cs)
    validate_cornerstone(x_out, "X")
    return x_out


@functools.cache
def derive_cornerstone_q() -> pd.Series[float]:
    cfg = get_usa_config()
    q = compute_q(
        V=derive_cornerstone_V(
            apply_inflation=cfg.apply_inflation_to_V, target_year=cfg.model_base_year
        )
    )
    validate_cornerstone(q, "Q")
    return q


@functools.cache
def derive_cornerstone_Vnorm_scrap_corrected(
    apply_inflation: bool | None = None,
    target_year: int = 0,
) -> pd.DataFrame:
    """Scrap-corrected V norm. Inflation is applied via ``derive_cornerstone_V``.

    When ``apply_inflation`` is omitted, uses ``USAConfig.apply_inflation_to_V``.
    When ``target_year`` is not positive and inflation is on, uses
    ``USAConfig.model_base_year``.
    """
    cfg = get_usa_config()
    use_inflation = (
        cfg.apply_inflation_to_V if apply_inflation is None else apply_inflation
    )
    effective_year = (
        target_year
        if target_year > 0
        else (cfg.model_base_year if use_inflation else 0)
    )
    V = derive_cornerstone_V(use_inflation, effective_year)

    q = compute_q(V=V)
    x = compute_x(V=V)
    Vnorm = compute_Vnorm_matrix(V=V, q=q)

    scrap_2017 = load_2017_V_usa().loc[:, 'S00401']
    scrap_fraction = industry_corresp() @ scrap_2017
    if cfg.implement_electricity_disaggregation:
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
    validate_cornerstone(V_scrap_corrected, "V")
    return V_scrap_corrected


@functools.cache
def scale_cornerstone_V_with_authoritative_x() -> pd.DataFrame:
    """Estimate V rescaled to match model-year gross industry output.

    Derives x_new from the BEA gross-output time series at ``model_base_year``
    (after redefinitions) via ``derive_cornerstone_x_after_redefinition``.
    Inflates the base 2017 V to model-year dollars, computes its row sums
    (x_model_year), then scales each row i proportionally by
    ``x_new[i] / x_model_year[i]``.  Industries with zero model-year output
    receive a zero row.

    Returns
    -------
    pd.DataFrame
        New V with ``V_new.sum(axis=1) ≈ x_new`` for all industries that have
        non-zero model-year output.
    """
    cfg = get_usa_config()
    x_new = derive_cornerstone_x_after_redefinition(year=cfg.model_base_year)

    V_model_year = derive_cornerstone_V(
        apply_inflation=True, target_year=cfg.model_base_year
    )
    x_model_year = V_model_year.sum(axis=1)
    x_new_aligned = x_new.reindex(x_model_year.index).fillna(0.0)

    x_model_year_np = x_model_year.to_numpy(dtype=float)
    x_new_aligned_np = x_new_aligned.to_numpy(dtype=float)
    scale = pd.Series(
        np.where(
            x_model_year_np != 0,
            x_new_aligned_np / x_model_year_np,
            0.0,
        ),
        index=x_model_year.index,
    )
    V_new = V_model_year.multiply(scale, axis=0)

    mask = x_model_year_np != 0
    assert np.allclose(
        V_new.sum(axis=1).to_numpy(dtype=float)[mask], x_new_aligned_np[mask], rtol=1e-6
    ), 'Row sums of V_new do not match x_new'

    return V_new


def derive_q_from_scaled_cornerstone_V_from_authoritative_x() -> pd.Series[float]:
    V = scale_cornerstone_V_with_authoritative_x()
    return compute_q(V=V)


# ---------------------------------------------------------------------------
# Base 2017 IO matrices — U
# ---------------------------------------------------------------------------


@functools.cache
def derive_cornerstone_U_with_negatives() -> SingleRegionUMatrixSet:
    if cornerstone_sector_disagg_active():
        bundle = derive_disagg_io_bundle()
        Udom_cs, Uimp_cs = bundle.Udom.copy(), bundle.Uimp.copy()
    else:
        Udom_cs, Uimp_cs = _derive_cornerstone_U_baseline()
    validate_cornerstone(Udom_cs, "U")
    validate_cornerstone(Uimp_cs, "U")
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
    validate_cornerstone(Udom, "U")
    validate_cornerstone(Uimp, "U")
    return SingleRegionUMatrixSet(
        Udom=cast(pt.DataFrame[UMatrix], Udom),
        Uimp=cast(pt.DataFrame[UMatrix], Uimp),
    )


# ---------------------------------------------------------------------------
# Base 2017 IO matrices — Y
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
# Base 2017 IO matrices — VA
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
    # When apply_inflation_to_V is True: q and x use uninflated derive_cornerstone_V()
    # (2017 $), while Vnorm uses derive_cornerstone_Vnorm_scrap_corrected() (model-year $).
    # derive_cornerstone_q() applies the flag but is not used here. Intentional for now;
    # see inflation/A dollar-year design notes.
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


@functools.cache
def derive_cornerstone_Aq_scaled() -> SingleRegionAqMatrixSet:
    """Year-scaled and inflated A matrices and q."""
    base = derive_cornerstone_Aq()
    cfg = get_usa_config()
    io_year = cfg.usa_io_data_year
    detail_year = cfg.usa_detail_original_year
    model_year = cfg.model_base_year

    # USEEIO method: return 2017 base A unchanged — no scaling, no inflation.
    if cfg.scale_a_matrix_with_useeio_method:
        return base

    # USEEIO nowcast: load externally-balanced detail SUTs from GCS and
    # derive Cornerstone A directly. Bypasses all internal scaling/inflation;
    # treats the upstream USEEIO team's GRAS-balanced 2018–2023 SUTs as the
    # source of structural change. Loaders: bedrock.extract.iot.useeio_nowcast;
    # derivation: bedrock.transform.eeio.derived_useeio_nowcast.
    if cfg.load_useeio_nowcast_A_matrix:
        from bedrock.transform.eeio.derived_useeio_nowcast import (  # noqa: PLC0415
            derive_useeio_nowcast_Aq_cornerstone,
        )

        return derive_useeio_nowcast_Aq_cornerstone(year=model_year)

    # Summary tables: scale 2017 → model_year using summary A ratios.
    #
    # When `cfg.adjust_summary_A_and_q_dollar_year` is set, `scale_cornerstone_A`
    # rebases the target-year summary A into 2017 USD before the ratio is taken,
    # so the structural cross-year ratio is formed entirely in 2017 USD; the
    # scaled detail A is then inflated 2017 → model_year. When the flag is off,
    # the ratio carries the raw target-year-vs-2017 price drift and no final
    # inflation is applied (pre-realignment behavior).
    if cfg.scale_a_matrix_with_summary_tables:
        Adom = scale_cornerstone_A(
            base.Adom,
            target_year=model_year,
            original_year=detail_year,
            dom_or_imp_or_total='dom',
        )
        Aimp = scale_cornerstone_A(
            base.Aimp,
            target_year=model_year,
            original_year=detail_year,
            dom_or_imp_or_total='imp',
        )
        q = scale_cornerstone_q(
            base.scaled_q,
            target_year=model_year,
            original_year=detail_year,
        )
        if cfg.adjust_summary_A_and_q_dollar_year:
            Adom = inflate_cornerstone_A_matrix_with_commodity_pi(
                Adom, original_year=detail_year, target_year=model_year
            )
            Aimp = inflate_cornerstone_A_matrix_with_commodity_pi(
                Aimp, original_year=detail_year, target_year=model_year
            )
            q = inflate_cornerstone_q_or_y_with_commodity_pi(
                q, original_year=detail_year, target_year=model_year
            )
        return _cornerstone_aq_matrix_set(Adom=Adom, Aimp=Aimp, scaled_q=q)

    # Commodity price index (V-norm-derived): like the industry-price branch,
    # but uses V_norm to weight industry price ratios into commodity space
    # before applying diag(p) @ A @ diag(1/p).
    if cfg.scale_a_matrix_with_commodity_price_index:
        Adom = inflate_cornerstone_A_matrix_with_commodity_pi(
            base.Adom, original_year=detail_year, target_year=model_year
        )
        Aimp = inflate_cornerstone_A_matrix_with_commodity_pi(
            base.Aimp, original_year=detail_year, target_year=model_year
        )
        q = inflate_cornerstone_q_or_y_with_commodity_pi(
            base.scaled_q, original_year=detail_year, target_year=model_year
        )
        return _cornerstone_aq_matrix_set(Adom=Adom, Aimp=Aimp, scaled_q=q)

    # CEDA method: our fallback option as of CY26Q2.
    # Scale to 2022 (io_year), then inflate to model_base_year.
    # However, we are applying some subtle changes to this method:
    # 1. scale detail A and q with dollar year adjusted summary numbers
    # 2. inflate with commodity pi instead of industry pi
    #
    # Codepath of this approach is very similar to the scale_a_matrix_with_summary_tables approach,
    # the only difference is which year to scale to.
    #
    # When `cfg.adjust_summary_A_and_q_dollar_year` is set, `scale_cornerstone_A`
    # rebases the target-year summary A into 2017 USD before the ratio is taken,
    # so the structural cross-year ratio is formed entirely in 2017 USD; the
    # scaled detail A is then inflated 2017 → io_year. When the flag is off,
    # the ratio carries the raw target-year-vs-2017 price drift and no final
    # inflation is applied (pre-realignment behavior).
    if cfg.scale_a_matrix_with_ceda_method_as_fallback:
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
        if cfg.adjust_summary_A_and_q_dollar_year:
            Adom = inflate_cornerstone_A_matrix_with_commodity_pi(
                Adom, original_year=detail_year, target_year=model_year
            )
            Aimp = inflate_cornerstone_A_matrix_with_commodity_pi(
                Aimp, original_year=detail_year, target_year=model_year
            )
            q = inflate_cornerstone_q_or_y_with_commodity_pi(
                q, original_year=detail_year, target_year=model_year
            )
        return _cornerstone_aq_matrix_set(Adom=Adom, Aimp=Aimp, scaled_q=q)

    Adom = inflate_cornerstone_A_matrix_with_industry_pi(
        scale_cornerstone_A(
            base.Adom,
            target_year=io_year,
            original_year=detail_year,
            dom_or_imp_or_total='dom',
        ),
        original_year=io_year,
        target_year=model_year,
    )
    Aimp = inflate_cornerstone_A_matrix_with_industry_pi(
        scale_cornerstone_A(
            base.Aimp,
            target_year=io_year,
            original_year=detail_year,
            dom_or_imp_or_total='imp',
        ),
        original_year=io_year,
        target_year=model_year,
    )
    q = inflate_cornerstone_q_or_y_with_industry_pi(
        scale_cornerstone_q(
            base.scaled_q, target_year=io_year, original_year=detail_year
        ),
        original_year=io_year,
        target_year=model_year,
    )

    return _cornerstone_aq_matrix_set(Adom=Adom, Aimp=Aimp, scaled_q=q)


# ---------------------------------------------------------------------------
# B matrix (runtime E path)
# ---------------------------------------------------------------------------


def derive_cornerstone_B_via_vnorm() -> pd.DataFrame:
    """B (ghg × Cornerstone commodity).

    Always computed in Cornerstone space: E = derive_E_usa(), then B = (E / x) @ Vnorm.
    Industry ``x`` is:
    - ``deflate_x_to_detail_io_year_for_B=True``: gross output from the BEA
      gross-output time series at ``usa_ghg_data_year`` (nominal), divided by
      ``PI(usa_ghg_data_year)/PI(usa_detail_original_year)`` so ``E/x`` uses
      ``usa_detail_original_year`` chain dollars. ``USAConfig`` requires
      ``use_E_data_year_for_x_in_B`` to be true whenever deflation is on; the
      deflate branch always builds nominal ``x`` via
      ``derive_cornerstone_x_after_redefinition()`` before the PI ratio, so
      ``use_E_data_year_for_x_in_B`` does not further branch choice here.
    - otherwise: ``derive_cornerstone_x_after_redefinition()`` when
      ``use_E_data_year_for_x_in_B`` is True, else ``derive_cornerstone_x()``.
    No BEA intermediate or expand_ghg_matrix_from_bea_to_cornerstone.
    """
    cfg = get_usa_config()
    E = derive_E_usa()
    if cfg.implement_electricity_disaggregation:
        E = split_electricity_e_for_disaggregated_b(E)
    if cfg.deflate_x_to_detail_io_year_for_B:
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
            if cfg.use_E_data_year_for_x_in_B
            else derive_cornerstone_x()
        )
    Vnorm = derive_cornerstone_Vnorm_scrap_corrected()
    Bi = E.divide(x, axis=1).fillna(0.0)
    B = Bi @ Vnorm
    validate_cornerstone(B, "B")
    return B


@functools.cache
def derive_cornerstone_B_non_finetuned() -> pd.DataFrame:
    """Year-scaled + inflated B, derived self-contained from CEDA v7 → cornerstone."""
    cfg = get_usa_config()
    # ``deflate_x_to_detail_io_year_for_B`` implies ``use_E_data_year_for_x_in_B``
    # (``USAConfig``). When either path is active, keep B on vnorm only (no
    # summary scaling / industry PI inflation of B here); vnorm applies the
    # deflate branch when that flag is set.
    if cfg.use_E_data_year_for_x_in_B:
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
