"""Cornerstone IO data processing pipeline.

Derives 2017 detail IO matrices (V, U, Y, A, B, E, g, q) using the
Cornerstone 2026 taxonomy (405 sectors).

**Core approach** — A and B are computed in the original BEA 2017 ~400-sector
space and then *expanded* to 405 Cornerstone sectors by duplicating
rows/columns for disaggregated codes.  V, U, and Y are mapped via
correspondence-matrix multiplication.  Waste subsectors receive special
intragroup treatment to prevent Leontief-inverse inflation.

Year-scaling logic (summary → detail disaggregation) uses the cornerstone
summary correspondence instead of the CEDA v7 version.

This module is self-contained: it does NOT modify or gate any existing CEDA v7
code paths. The caller decides which pipeline to invoke based on config.

Internal helpers live in sibling modules:
- ``cornerstone_expansion`` — BEA ↔ Cornerstone correspondence & expansion
- ``cornerstone_bea_intermediates`` — BEA-space intermediate matrices
- ``cornerstone_year_scaling`` — summary-ratio year-scaling for A, q, B
"""

from __future__ import annotations

import functools

import numpy as np
import pandas as pd
import pandera.pandas as pa
import pandera.typing as pt

from bedrock.extract.iot.io_2017 import (
    load_2017_Uimp_usa,
    load_2017_Utot_usa,
    load_2017_V_usa,
    load_2017_Ytot_usa,
    load_summary_Uimp_usa,
)
from bedrock.transform.eeio.cornerstone_bea_intermediates import bea_Aq, bea_B, bea_E
from bedrock.transform.eeio.cornerstone_expansion import (
    CS_COMMODITY_LIST,
    CS_INDUSTRY_LIST,
    commodity_corresp,
    cs_commodity_to_bea_map,
    cs_industry_to_bea_map,
    expand_ghg_matrix,
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
from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.economic.inflate_cornerstone_to_target_year import (
    inflate_cornerstone_A_matrix,
    inflate_cornerstone_B_matrix,
    inflate_cornerstone_q_or_y,
)
from bedrock.utils.math.disaggregation import disaggregate_vector
from bedrock.utils.math.formulas import (
    compute_g,
    compute_q,
    compute_Vnorm_matrix,
    compute_y_for_national_accounting_balance,
    compute_y_imp,
)
from bedrock.utils.math.handle_negatives import (
    handle_negative_matrix_values,
    handle_negative_vector_values,
)
from bedrock.utils.math.split_using_aggregated_weights import (
    split_vector_using_agg_ratio,
)
from bedrock.utils.schemas.cornerstone_schemas import (
    CornerstoneAMatrix,
    CornerstoneBMatrix,
    CornerstoneEMatrix,
    CornerstoneGVectorSchema,
    CornerstoneQVectorSchema,
    CornerstoneUMatrix,
    CornerstoneVMatrix,
)
from bedrock.utils.schemas.single_region_types import (
    SingleRegionAqMatrixSet,
    SingleRegionUMatrixSet,
    SingleRegionYtotAndTradeVectorSet,
    SingleRegionYVectorSet,
)
from bedrock.utils.taxonomy.bea.v2017_final_demand import (
    USA_2017_FINAL_DEMAND_EXPORT_CODE,
    USA_2017_FINAL_DEMAND_IMPORT_CODE,
    USA_2017_FINAL_DEMAND_PERSONAL_CONSUMPTION_EXPENDITURE_CODE,
)
from bedrock.utils.taxonomy.bea.v2017_industry_summary import (
    USA_2017_SUMMARY_INDUSTRY_CODES,
)
from bedrock.utils.taxonomy.bea_v2017_to_ceda_v7_helpers import (
    get_bea_v2017_summary_to_cornerstone_corresp_df,
)

# ---------------------------------------------------------------------------
# Base 2017 IO matrices — V, g, q
# ---------------------------------------------------------------------------


@functools.cache
@pa.check_output(CornerstoneVMatrix.to_schema())
def derive_cornerstone_V() -> pd.DataFrame:
    """V matrix (industry × commodity) via correspondence multiplication."""
    V_2017 = load_2017_V_usa()
    V = industry_corresp() @ V_2017 @ commodity_corresp().T
    V.index.name = 'sector'
    V.columns.name = 'sector'
    return V


@functools.cache
@pa.check_output(CornerstoneGVectorSchema)
def derive_cornerstone_g() -> pd.Series[float]:
    return compute_g(V=derive_cornerstone_V())


@functools.cache
@pa.check_output(CornerstoneQVectorSchema)
def derive_cornerstone_q() -> pd.Series[float]:
    return compute_q(V=derive_cornerstone_V())


@functools.cache
@pa.check_output(CornerstoneVMatrix.to_schema())
def derive_cornerstone_Vnorm_scrap_corrected(
    apply_inflation: bool = False, target_year: int = 0
) -> pd.DataFrame:
    V = derive_cornerstone_V()

    if apply_inflation:
        from bedrock.utils.economic.inflate_cornerstone_to_target_year import (  # noqa: PLC0415
            get_cornerstone_price_ratio,
        )

        price_ratio = get_cornerstone_price_ratio(2017, target_year)
        V = pd.DataFrame(
            V.multiply(price_ratio, axis=1).values,
            index=V.index,
            columns=V.columns,
        )

    q = compute_q(V=V)
    g = compute_g(V=V)
    Vnorm = compute_Vnorm_matrix(V=V, q=q)

    scrap_2017 = load_2017_V_usa().loc[:, 'S00401']
    scrap_fraction = industry_corresp() @ scrap_2017

    V_scrap_corrected = Vnorm.divide((1.0 - (scrap_fraction / g).fillna(0.0)), axis=0)
    return V_scrap_corrected


# ---------------------------------------------------------------------------
# Base 2017 IO matrices — U
# ---------------------------------------------------------------------------


@functools.cache
def derive_cornerstone_U_with_negatives() -> SingleRegionUMatrixSet:
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

    return SingleRegionUMatrixSet(
        Udom=pt.DataFrame[CornerstoneUMatrix](Udom_cs),  # type: ignore[arg-type]
        Uimp=pt.DataFrame[CornerstoneUMatrix](Uimp_cs),  # type: ignore[arg-type]
    )


@functools.cache
def derive_cornerstone_U_set() -> SingleRegionUMatrixSet:
    uset = derive_cornerstone_U_with_negatives()
    Udom = handle_negative_matrix_values(uset.Udom)
    Uimp = handle_negative_matrix_values(uset.Uimp)
    assert not (Udom < 0).any().any(), 'Udom has negative values.'
    assert not (Uimp < 0).any().any(), 'Uimp has negative values.'
    return SingleRegionUMatrixSet(
        Udom=pt.DataFrame[CornerstoneUMatrix](Udom),  # type: ignore[arg-type]
        Uimp=pt.DataFrame[CornerstoneUMatrix](Uimp),  # type: ignore[arg-type]
    )


# ---------------------------------------------------------------------------
# Base 2017 IO matrices — Y
# ---------------------------------------------------------------------------


@functools.cache
def _derive_cornerstone_Ytot_with_trade() -> pd.DataFrame:
    Ytot_orig = load_2017_Ytot_usa()
    Ytot = commodity_corresp() @ Ytot_orig
    Ytot.index.name = 'sector'
    return Ytot


@functools.cache
def derive_cornerstone_Ytot_matrix_set() -> SingleRegionYtotAndTradeVectorSet:
    Ytot_with_trade = _derive_cornerstone_Ytot_with_trade()
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
    return _derive_cornerstone_Ytot_with_trade()[
        USA_2017_FINAL_DEMAND_PERSONAL_CONSUMPTION_EXPENDITURE_CODE
    ]


# ---------------------------------------------------------------------------
# A matrices and q — expanded from BEA space
# ---------------------------------------------------------------------------


@functools.cache
def derive_cornerstone_Aq() -> SingleRegionAqMatrixSet:
    """Base 2017 A matrices and q — computed in BEA space, then expanded.

    A is computed in BEA ~400-sector space (without structural reflection)
    and expanded to 405 Cornerstone commodities by duplicating rows/columns
    for disaggregated sectors (waste, aluminum).  Per-unit intensities are
    preserved: A[562111, j] == A_bea[562000, j].

    Within-group cross-terms are **zeroed out** so that disaggregated
    subsectors (e.g. the 7 waste codes) do not create artificial circular
    supply chains that would inflate the Leontief inverse and N values.
    """
    Adom_bea, Aimp_bea, q_bea = bea_Aq()
    com_map = cs_commodity_to_bea_map()

    Adom = expand_square_matrix(
        Adom_bea, CS_COMMODITY_LIST, com_map, zero_intragroup_cross_terms=True
    )
    Aimp = expand_square_matrix(
        Aimp_bea, CS_COMMODITY_LIST, com_map, zero_intragroup_cross_terms=True
    )
    q = expand_vector(q_bea, CS_COMMODITY_LIST, com_map)

    assert (Adom >= 0).all().all(), 'Adom has negative values.'
    assert (Aimp >= 0).all().all(), 'Aimp has negative values.'
    assert (q >= 0).all(), 'q has negative values.'

    return SingleRegionAqMatrixSet(
        Adom=pt.DataFrame[CornerstoneAMatrix](Adom),  # type: ignore[arg-type]
        Aimp=pt.DataFrame[CornerstoneAMatrix](Aimp),  # type: ignore[arg-type]
        scaled_q=q,
    )


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

    # Price index only: inflate 2017 → model_year directly using price index,
    # skipping the summary table scaling step entirely.
    if cfg.scale_a_matrix_with_price_index:
        Adom = inflate_cornerstone_A_matrix(
            base.Adom, original_year=detail_year, target_year=model_year
        )
        Aimp = inflate_cornerstone_A_matrix(
            base.Aimp, original_year=detail_year, target_year=model_year
        )
        q = inflate_cornerstone_q_or_y(
            base.scaled_q, original_year=detail_year, target_year=model_year
        )
        return SingleRegionAqMatrixSet(
            Adom=pt.DataFrame[CornerstoneAMatrix](Adom),  # type: ignore[arg-type]
            Aimp=pt.DataFrame[CornerstoneAMatrix](Aimp),  # type: ignore[arg-type]
            scaled_q=q,
        )

    Adom = inflate_cornerstone_A_matrix(
        scale_cornerstone_A(
            base.Adom,
            target_year=io_year,
            original_year=detail_year,
            dom_or_imp_or_total='dom',
        ),
        original_year=io_year,
        target_year=model_year,
    )
    Aimp = inflate_cornerstone_A_matrix(
        scale_cornerstone_A(
            base.Aimp,
            target_year=io_year,
            original_year=detail_year,
            dom_or_imp_or_total='imp',
        ),
        original_year=io_year,
        target_year=model_year,
    )
    q = inflate_cornerstone_q_or_y(
        scale_cornerstone_q(
            base.scaled_q, target_year=io_year, original_year=detail_year
        ),
        original_year=io_year,
        target_year=model_year,
    )

    return SingleRegionAqMatrixSet(
        Adom=pt.DataFrame[CornerstoneAMatrix](Adom),  # type: ignore[arg-type]
        Aimp=pt.DataFrame[CornerstoneAMatrix](Aimp),  # type: ignore[arg-type]
        scaled_q=q,
    )


# ---------------------------------------------------------------------------
# E and B — expanded from BEA space
# ---------------------------------------------------------------------------


@functools.cache
@pa.check_output(CornerstoneEMatrix.to_schema())
def derive_cornerstone_E() -> pd.DataFrame:
    """E (ghg × Cornerstone industry) — expanded from BEA space."""
    return expand_ghg_matrix(bea_E(), CS_INDUSTRY_LIST, cs_industry_to_bea_map())


@pa.check_output(CornerstoneBMatrix.to_schema())
def derive_cornerstone_B_via_vnorm() -> pd.DataFrame:
    """B (ghg × Cornerstone commodity) — computed in BEA space, then expanded.

    B = (E / g) @ V_norm is computed entirely in BEA ~400-sector space,
    then expanded to 405 Cornerstone commodities.  This guarantees
    D[562111] == D_bea[562000] exactly.
    """
    return expand_ghg_matrix(bea_B(), CS_COMMODITY_LIST, cs_commodity_to_bea_map())


@functools.cache
@pa.check_output(CornerstoneBMatrix.to_schema())
def derive_cornerstone_B_non_finetuned() -> pd.DataFrame:
    """Year-scaled + inflated B, derived self-contained from CEDA v7 → cornerstone."""
    cfg = get_usa_config()
    if cfg.transform_b_matrix_with_useeio_method:
        return derive_cornerstone_B_via_vnorm()
    else:
        return inflate_cornerstone_B_matrix(
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
    return inflate_cornerstone_q_or_y(
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
    """Y for national accounting balance, year-scaled."""
    cfg = get_usa_config()
    detail_2017 = derive_cornerstone_Ytot_matrix_set()

    y_nab_2017 = compute_y_for_national_accounting_balance(
        y_tot=detail_2017.ytot,
        y_imp=compute_y_imp(
            imports=detail_2017.imports,
            Uimp=derive_cornerstone_U_set().Uimp,
        ),
        exports=detail_2017.exports,
    )

    summary_Y = derive_summary_Ytot_usa_matrix_set(cfg.usa_io_data_year)
    y_nab_summary = compute_y_for_national_accounting_balance(
        y_tot=summary_Y.ytot,
        y_imp=compute_y_imp(
            imports=summary_Y.imports,
            Uimp=load_summary_Uimp_usa(cfg.usa_io_data_year).loc[
                USA_2017_SUMMARY_INDUSTRY_CODES, USA_2017_SUMMARY_INDUSTRY_CODES
            ],
        ),
        exports=summary_Y.exports,
    )

    y_nab_scaled = _disaggregate_and_inflate_vector(
        base=y_nab_summary,
        weight=y_nab_2017,
        corresp_df=get_bea_v2017_summary_to_cornerstone_corresp_df(),
        original_year=cfg.usa_io_data_year,
        target_year=cfg.model_base_year,
    )

    return handle_negative_vector_values(y_nab_scaled)


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
