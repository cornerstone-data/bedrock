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
"""

from __future__ import annotations

import functools
import typing as ta

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
from bedrock.transform.allocation.derived import derive_E_usa
from bedrock.transform.eeio.derived_2017 import (
    derive_summary_Adom_usa,
    derive_summary_Aimp_usa,
    derive_summary_q_usa,
    derive_summary_Yimp_usa,
    derive_summary_Ytot_usa_matrix_set,
)
from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.economic.inflate_cornerstone_to_target_year import (
    get_cornerstone_price_ratio,
    inflate_cornerstone_A_matrix,
    inflate_cornerstone_B_matrix,
    inflate_cornerstone_q_or_y,
)
from bedrock.utils.math.disaggregation import disaggregate_vector
from bedrock.utils.math.formulas import (
    compute_A_matrix,
    compute_g,
    compute_q,
    compute_total_industry_inputs,
    compute_Unorm_matrix,
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
from bedrock.utils.taxonomy.bea.matrix_mappings import USA_SUMMARY_MUT_YEARS
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
    load_bea_v2017_summary_to_cornerstone,
)
from bedrock.utils.taxonomy.cornerstone.commodities import (
    COMMODITIES as _CS_COMMODITIES,
)
from bedrock.utils.taxonomy.cornerstone.industries import INDUSTRIES as _CS_INDUSTRIES
from bedrock.utils.taxonomy.usa_taxonomy_correspondence_helpers import (
    load_usa_2017_commodity__ceda_v7_correspondence,
    load_usa_2017_commodity__cornerstone_commodity_correspondence,
    load_usa_2017_industry__cornerstone_industry_correspondence,
)

# ---------------------------------------------------------------------------
# Correspondence helpers (cached)
# ---------------------------------------------------------------------------


@functools.cache
def _commodity_corresp_raw() -> pd.DataFrame:
    """Raw binary (Cornerstone_commodity × BEA_2017_commodity) correspondence."""
    return load_usa_2017_commodity__cornerstone_commodity_correspondence()


@functools.cache
def _industry_corresp_raw() -> pd.DataFrame:
    """Raw binary (Cornerstone_industry × BEA_2017_industry) correspondence."""
    return load_usa_2017_industry__cornerstone_industry_correspondence()


def _col_normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Column-normalize so each BEA code's total is distributed (not duplicated)."""
    col_sums = df.sum(axis=0)
    return df.div(col_sums.replace(0, 1), axis=1)


@functools.cache
def _commodity_corresp() -> pd.DataFrame:
    """Column-normalized commodity correspondence.

    Ensures one-to-many BEA→cornerstone splits (e.g. waste 562000 → 7 subsectors)
    distribute values proportionally rather than duplicating.
    """
    return _col_normalize(_commodity_corresp_raw())


@functools.cache
def _industry_corresp() -> pd.DataFrame:
    """Column-normalized industry correspondence.

    Same principle as commodity: one-to-many splits distribute proportionally.
    """
    return _col_normalize(_industry_corresp_raw())


# ---------------------------------------------------------------------------
# BEA → Cornerstone expansion helpers
# ---------------------------------------------------------------------------
#
# Instead of mapping V/U via correspondence multiplication (which distorts
# per-unit intensities for disaggregated sectors), we compute A and B in the
# original BEA 2017 ~400-sector space and then *expand* to 405 Cornerstone
# sectors by duplicating rows/columns for disaggregated codes (waste, aluminum).
# This guarantees D = column_sum(B) is exactly preserved for those sectors.

_CS_COMMODITY_LIST: list[str] = list(_CS_COMMODITIES)
_CS_INDUSTRY_LIST: list[str] = list(_CS_INDUSTRIES)


def _build_reverse_map(corresp: pd.DataFrame) -> dict[str, str]:
    """Build {new_code: bea_parent_code} from a binary correspondence matrix."""
    mapping: dict[str, str] = {}
    for code in corresp.index:
        bea_hits = corresp.columns[corresp.loc[code] > 0].tolist()
        if bea_hits:
            mapping[code] = bea_hits[0]
    return mapping


@functools.cache
def _cs_commodity_to_bea_map() -> dict[str, str]:
    """Map each Cornerstone commodity to its BEA 2017 parent commodity code."""
    return _build_reverse_map(_commodity_corresp_raw())


@functools.cache
def _cs_industry_to_bea_map() -> dict[str, str]:
    """Map each Cornerstone industry to its BEA 2017 parent industry code."""
    return _build_reverse_map(_industry_corresp_raw())


def _valid_pairs(
    target_codes: list[str],
    code_map: dict[str, str],
    valid_labels: pd.Index,
) -> tuple[list[str], list[str]]:
    """Return (cs_codes, bea_codes) for Cornerstone codes that have a BEA parent in *valid_labels*."""
    pairs = [
        (cs, code_map[cs])
        for cs in target_codes
        if cs in code_map and code_map[cs] in valid_labels
    ]
    return [c for c, _ in pairs], [b for _, b in pairs]


def _apply_waste_intragroup_treatment(expanded: pd.DataFrame) -> None:
    """Zero cross-terms and divide non-group entries for waste subsectors in-place.

    Only applies to waste — these have no real disaggregated data, so the BEA
    parent value is duplicated.  Other disaggregations (e.g. aluminum) have
    real correspondence-informed splits and are left as-is.
    """
    from bedrock.utils.taxonomy.cornerstone.commodities import (  # noqa: PLC0415
        WASTE_DISAGG_COMMODITIES,
    )

    for _old_code, new_codes in WASTE_DISAGG_COMMODITIES.items():
        siblings = [c for c in new_codes if c in expanded.index]
        n = len(siblings)
        if n <= 1:
            continue
        # Zero within-group cross-terms (no artificial circular flow)
        for i in siblings:
            for j in siblings:
                if i != j:
                    expanded.loc[i, j] = 0.0
        # Divide non-group entries by n in both directions so that
        # total flow between the group and outside sectors is preserved.
        # The diagonal keeps the original per-unit self-supply coefficient.
        sibling_set = set(siblings)
        non_siblings = [c for c in expanded.index if c not in sibling_set]
        for s in siblings:
            expanded.loc[non_siblings, s] /= n
            expanded.loc[s, non_siblings] /= n  # type: ignore[index]


def _expand_square_matrix(
    M: pd.DataFrame,
    target_codes: list[str],
    code_map: dict[str, str],
    *,
    zero_intragroup_cross_terms: bool = False,
) -> pd.DataFrame:
    """Expand a BEA square matrix to Cornerstone space.

    Rows and columns for disaggregated sectors are **duplicated** (not split).
    When *zero_intragroup_cross_terms* is True, waste subsector cross-terms
    are zeroed and non-group entries divided by n to prevent L inflation.
    """
    cs_valid, bea_valid = _valid_pairs(target_codes, code_map, M.index)

    expanded = M.loc[bea_valid, bea_valid].copy()
    expanded.index = cs_valid
    expanded.columns = cs_valid
    expanded = expanded.reindex(
        index=target_codes, columns=target_codes, fill_value=0.0
    )

    if zero_intragroup_cross_terms:
        _apply_waste_intragroup_treatment(expanded)

    expanded.index.name = 'sector'
    expanded.columns.name = 'sector'
    return expanded


def _expand_vector(
    v: pd.Series[float],
    target_codes: list[str],
    code_map: dict[str, str],
) -> pd.Series[float]:
    """Expand a BEA vector to Cornerstone by duplicating entries."""
    cs_valid, bea_valid = _valid_pairs(target_codes, code_map, v.index)

    expanded = v.loc[bea_valid].copy()
    expanded.index = cs_valid
    return expanded.reindex(target_codes, fill_value=0.0)


def _expand_ghg_matrix(
    M: pd.DataFrame,
    target_col_codes: list[str],
    col_map: dict[str, str],
) -> pd.DataFrame:
    """Expand a (ghg × BEA_sector) matrix to Cornerstone columns."""
    cs_valid, bea_valid = _valid_pairs(target_col_codes, col_map, M.columns)

    expanded = M.loc[:, bea_valid].copy()
    expanded.columns = cs_valid
    expanded = expanded.reindex(columns=target_col_codes, fill_value=0.0)
    expanded.index.name = 'ghg'
    expanded.columns.name = 'sector'
    return expanded


# ---------------------------------------------------------------------------
# BEA-space computations (internal — no taxonomy expansion)
# ---------------------------------------------------------------------------


@functools.cache
def _bea_g() -> pd.Series[float]:
    """Industry total output in BEA 2017 space."""
    return compute_g(V=load_2017_V_usa())


@functools.cache
def _bea_q() -> pd.Series[float]:
    """Commodity total output in BEA 2017 space."""
    return compute_q(V=load_2017_V_usa())


@functools.cache
def _bea_Vnorm_scrap_corrected() -> pd.DataFrame:
    """Scrap-corrected V_norm in BEA 2017 space."""
    V = load_2017_V_usa()
    q = _bea_q()
    g = _bea_g()
    Vnorm = compute_Vnorm_matrix(V=V, q=q)
    scrap = V.loc[:, 'S00401']
    return Vnorm.divide((1.0 - (scrap / g).fillna(0.0)), axis=0)


@functools.cache
def _bea_Aq() -> tuple[pd.DataFrame, pd.DataFrame, pd.Series[float]]:
    """(Adom, Aimp, q) in BEA 2017 space, with scrap correction."""
    g = _bea_g()
    Vnorm = _bea_Vnorm_scrap_corrected()

    Utot = load_2017_Utot_usa()
    Uimp = load_2017_Uimp_usa()
    Udom = handle_negative_matrix_values(Utot - Uimp)
    Uimp_clean = handle_negative_matrix_values(Uimp)

    Adom = compute_A_matrix(
        U_norm=compute_Unorm_matrix(U=Udom, g=g),
        V_norm=Vnorm,
    )
    Aimp = compute_A_matrix(
        U_norm=compute_Unorm_matrix(U=Uimp_clean, g=g),
        V_norm=Vnorm,
    )
    return Adom, Aimp, _bea_q()


@functools.cache
def _ceda_v7_commodity_corresp() -> pd.DataFrame:
    """BEA 2017 commodity → CEDA v7 correspondence (rows=CEDA v7, cols=BEA 2017 commodity)."""
    return load_usa_2017_commodity__ceda_v7_correspondence()


@functools.cache
def _g_weighted_ceda_corresp() -> pd.DataFrame:
    """CEDA v7 → BEA commodity correspondence, row-normalized by industry output.

    The raw correspondence has shape (CEDA_v7 × BEA_commodity) with binary 1s.
    When one CEDA v7 sector maps to multiple BEA codes (e.g. CEDA 331313 →
    BEA {331313, 33131B}), a plain matmul would duplicate the emission into
    both BEA columns.  Instead, we weight each column by its BEA industry
    output (g) and then row-normalize, so emissions are **split** proportionally
    rather than duplicated.

    For 1:1 mappings the result is identical (weight / weight = 1.0).
    """
    corresp = _ceda_v7_commodity_corresp()  # (CEDA_v7 × BEA_commodity)
    g = _bea_g()
    g_aligned = g.reindex(corresp.columns, fill_value=0.0)
    weighted = corresp.multiply(g_aligned, axis=1)
    row_sums = weighted.sum(axis=1)
    return weighted.div(row_sums.replace(0, 1), axis=0)


@functools.cache
def _bea_E() -> pd.DataFrame:
    """E (ghg × BEA_industry) in BEA space.

    Maps CEDA v7 E to BEA commodity codes using a g-weighted correspondence
    so that when a CEDA v7 sector covers multiple BEA codes (e.g. 331313 →
    {331313, 33131B}), emissions are split proportionally by industry output
    rather than duplicated.  Then reindexes to BEA industry codes.
    """
    E_ceda = derive_E_usa()
    corresp = _g_weighted_ceda_corresp()
    E_bea_commodity = E_ceda @ corresp
    return E_bea_commodity.reindex(columns=_bea_g().index, fill_value=0.0)


@functools.cache
def _bea_B() -> pd.DataFrame:
    """B (ghg × BEA_commodity) in BEA space.  B = (E / g) @ V_norm."""
    E = _bea_E()
    g = _bea_g()
    Vnorm = _bea_Vnorm_scrap_corrected()
    Bi = E.divide(g, axis=1).fillna(0.0)
    return Bi @ Vnorm


# ---------------------------------------------------------------------------
# Base 2017 IO matrices (Cornerstone space — still used for V, U, Y)
# ---------------------------------------------------------------------------


@functools.cache
@pa.check_output(CornerstoneVMatrix.to_schema())
def derive_cornerstone_V() -> pd.DataFrame:
    """V matrix (industry × commodity) via correspondence multiplication."""
    V_2017 = load_2017_V_usa()
    V = _industry_corresp() @ V_2017 @ _commodity_corresp().T
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
    scrap_fraction = _industry_corresp() @ scrap_2017

    # Scrap correction is per-industry (row): each industry's output includes
    # some scrap, so divide each row by (1 - scrap[i] / g[i]).
    # In CEDA v7 industry=commodity so axis didn't matter; here we must use
    # g (industry output) and divide along axis=0 (rows).
    V_scrap_corrected = Vnorm.divide((1.0 - (scrap_fraction / g).fillna(0.0)), axis=0)
    return V_scrap_corrected


@functools.cache
def derive_cornerstone_U_with_negatives() -> SingleRegionUMatrixSet:
    Utot = load_2017_Utot_usa()
    Uimp = load_2017_Uimp_usa()
    Udom = Utot - Uimp

    commodity_c = _commodity_corresp()
    industry_c = _industry_corresp()

    Udom_cs = commodity_c @ Udom @ industry_c.T
    Uimp_cs = commodity_c @ Uimp @ industry_c.T

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


def _derive_cornerstone_Ytot_with_trade() -> pd.DataFrame:
    Ytot_orig = load_2017_Ytot_usa()
    Ytot = _commodity_corresp() @ Ytot_orig
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
    Adom_bea, Aimp_bea, q_bea = _bea_Aq()
    com_map = _cs_commodity_to_bea_map()

    Adom = _expand_square_matrix(
        Adom_bea, _CS_COMMODITY_LIST, com_map, zero_intragroup_cross_terms=True
    )
    Aimp = _expand_square_matrix(
        Aimp_bea, _CS_COMMODITY_LIST, com_map, zero_intragroup_cross_terms=True
    )
    q = _expand_vector(q_bea, _CS_COMMODITY_LIST, com_map)

    assert (Adom >= 0).all().all(), 'Adom has negative values.'
    assert (Aimp >= 0).all().all(), 'Aimp has negative values.'
    assert (q >= 0).all(), 'q has negative values.'

    return SingleRegionAqMatrixSet(
        Adom=pt.DataFrame[CornerstoneAMatrix](Adom),  # type: ignore[arg-type]
        Aimp=pt.DataFrame[CornerstoneAMatrix](Aimp),  # type: ignore[arg-type]
        scaled_q=q,
    )


# ---------------------------------------------------------------------------
# Year-scaling (summary → cornerstone detail)
# ---------------------------------------------------------------------------


def _scale_cornerstone_A(
    A: pd.DataFrame,
    target_year: USA_SUMMARY_MUT_YEARS,
    original_year: USA_SUMMARY_MUT_YEARS,
    dom_or_imp_or_total: ta.Literal['dom', 'imp', 'total'],
) -> pd.DataFrame:
    """Scale detail A element-wise using summary A ratios."""
    match dom_or_imp_or_total:
        case 'dom':
            A_summary_base = derive_summary_Adom_usa(original_year)
            A_summary_target = derive_summary_Adom_usa(target_year)
        case 'imp':
            A_summary_base = derive_summary_Aimp_usa(original_year)
            A_summary_target = derive_summary_Aimp_usa(target_year)
        case 'total':
            A_summary_base = derive_summary_Adom_usa(
                original_year
            ) + derive_summary_Aimp_usa(original_year)
            A_summary_target = derive_summary_Adom_usa(
                target_year
            ) + derive_summary_Aimp_usa(target_year)

    summary_to_cornerstone = load_bea_v2017_summary_to_cornerstone()
    detail_sectors = list(A.index)
    summary_ratios = (A_summary_target / A_summary_base).fillna(1.0)
    summary_ratios[np.isinf(summary_ratios)] = 1.0

    A_scaled = A.copy()

    block_rows = []
    for i, row in summary_ratios.iterrows():
        if i not in summary_to_cornerstone:
            continue
        block_mat = pd.DataFrame(
            index=summary_to_cornerstone[i],  # type: ignore
            columns=detail_sectors,
            data=0,
            dtype=float,
        )
        for col_summary_sector, val in row.items():
            if val == 0:
                continue
            if col_summary_sector in ['Used', 'Other']:
                continue
            col_sectors = summary_to_cornerstone[col_summary_sector]  # type: ignore
            block_mat.loc[:, col_sectors] = val
        block_rows.append(block_mat)

    # reindex with fill_value=1.0 so cornerstone-only sectors (e.g. S00402)
    # that have no BEA summary parent get a neutral scaling ratio.
    ratio_multiplier = pd.concat(block_rows, axis=0).reindex(
        index=A_scaled.index, columns=A_scaled.columns, fill_value=1.0
    )
    A_scaled = A_scaled * ratio_multiplier

    # Cap column sums at 0.98
    total_industry_inputs = compute_total_industry_inputs(A=A_scaled)
    oob_idx = total_industry_inputs[total_industry_inputs > 1].index
    for col in oob_idx:
        A_scaled[col] *= 0.98 / total_industry_inputs[col]

    assert (compute_total_industry_inputs(A=A_scaled) <= 1).all(), (
        'A column sums exceed 1 after scaling.'
    )

    return A_scaled


def _apply_summary_ratio_to_sectors(
    ratio: pd.Series[float],
    target: pd.DataFrame | pd.Series[float],
    *,
    axis: ta.Literal['rows', 'columns'],
) -> pd.DataFrame | pd.Series[float]:
    """Multiply *target* entries by sector-mapped summary *ratio*.

    Parameters
    ----------
    axis : 'rows' or 'columns'
        Whether to apply the ratio to rows (vector / A-matrix rows) or
        columns (B-matrix columns).
    """
    result = target.copy()
    summary_to_cornerstone = load_bea_v2017_summary_to_cornerstone()
    for summary_sector, val in ratio.items():
        if summary_sector not in summary_to_cornerstone:
            continue
        sectors = summary_to_cornerstone[summary_sector]  # type: ignore
        if axis == 'rows':
            result.loc[sectors] *= val  # type: ignore[index,assignment]
        else:
            result.loc[:, sectors] *= val  # type: ignore[assignment]
    return result


def _scale_cornerstone_q(
    q: pd.Series[float],
    target_year: USA_SUMMARY_MUT_YEARS,
    original_year: USA_SUMMARY_MUT_YEARS,
) -> pd.Series[float]:
    ratio = (
        derive_summary_q_usa(target_year) / derive_summary_q_usa(original_year)
    ).fillna(1.0)
    return ta.cast(
        pd.Series,
        _apply_summary_ratio_to_sectors(ratio, q, axis='rows'),
    )


def _scale_cornerstone_B(
    B: pd.DataFrame,
    target_year: USA_SUMMARY_MUT_YEARS,
    original_year: USA_SUMMARY_MUT_YEARS,
) -> pd.DataFrame:
    ratio = (
        derive_summary_q_usa(original_year) / derive_summary_q_usa(target_year)
    ).fillna(1.0)
    return ta.cast(
        pd.DataFrame,
        _apply_summary_ratio_to_sectors(ratio, B, axis='columns'),
    )


# ---------------------------------------------------------------------------
# Scaled / inflated outputs (equivalents of derived.py public API)
# ---------------------------------------------------------------------------


@functools.cache
def derive_cornerstone_Aq_scaled() -> SingleRegionAqMatrixSet:
    """Year-scaled and inflated A matrices and q."""
    base = derive_cornerstone_Aq()
    cfg = get_usa_config()
    io_year = cfg.usa_io_data_year
    detail_year = cfg.usa_detail_original_year
    model_year = cfg.model_base_year

    # 1. Year-scale using summary ratios
    Adom = _scale_cornerstone_A(
        A=base.Adom,
        target_year=io_year,
        original_year=detail_year,
        dom_or_imp_or_total='dom',
    )
    Aimp = _scale_cornerstone_A(
        A=base.Aimp,
        target_year=io_year,
        original_year=detail_year,
        dom_or_imp_or_total='imp',
    )
    q = _scale_cornerstone_q(
        q=base.scaled_q, target_year=io_year, original_year=detail_year
    )
    assert q is not None, 'q is None'

    # 2. Inflate to model base year
    Adom = inflate_cornerstone_A_matrix(
        Adom, original_year=io_year, target_year=model_year
    )
    Aimp = inflate_cornerstone_A_matrix(
        Aimp, original_year=io_year, target_year=model_year
    )
    q = inflate_cornerstone_q_or_y(q, original_year=io_year, target_year=model_year)

    return SingleRegionAqMatrixSet(
        Adom=pt.DataFrame[CornerstoneAMatrix](Adom),  # type: ignore[arg-type]
        Aimp=pt.DataFrame[CornerstoneAMatrix](Aimp),  # type: ignore[arg-type]
        scaled_q=q,
    )


# ---------------------------------------------------------------------------
# Expanded E and B (public API)
# ---------------------------------------------------------------------------


@functools.cache
@pa.check_output(CornerstoneEMatrix.to_schema())
def derive_cornerstone_E() -> pd.DataFrame:
    """E (ghg × Cornerstone industry) — expanded from BEA space.

    E is first computed in BEA ~400-sector space (via CEDA v7 → BEA commodity),
    then expanded to 405 Cornerstone industries by **duplicating** columns for
    disaggregated sectors.  Each waste subsector gets the full parent E value
    (not 1/7), matching the expansion approach used for A and B.
    """
    E_bea = _bea_E()
    ind_map = _cs_industry_to_bea_map()
    return _expand_ghg_matrix(E_bea, _CS_INDUSTRY_LIST, ind_map)


@pa.check_output(CornerstoneBMatrix.to_schema())
def derive_cornerstone_B_via_vnorm() -> pd.DataFrame:
    """B (ghg × Cornerstone commodity) — computed in BEA space, then expanded.

    B = (E / g) @ V_norm is computed entirely in BEA ~400-sector space,
    then expanded to 405 Cornerstone commodities by duplicating columns
    for disaggregated sectors (waste, aluminum).

    This guarantees D[562111] == D_bea[562000] exactly — the direct
    emission intensity is preserved for every disaggregated sector.
    """
    B_bea = _bea_B()
    com_map = _cs_commodity_to_bea_map()
    return _expand_ghg_matrix(B_bea, _CS_COMMODITY_LIST, com_map)


@functools.cache
@pa.check_output(CornerstoneBMatrix.to_schema())
def derive_cornerstone_B_non_finetuned() -> pd.DataFrame:
    """Self-contained: derives E from CEDA v7 → cornerstone internally."""
    cfg = get_usa_config()
    B = inflate_cornerstone_B_matrix(
        _scale_cornerstone_B(
            B=derive_cornerstone_B_via_vnorm(),
            original_year=cfg.usa_detail_original_year,
            target_year=cfg.usa_io_data_year,
        ),
        original_year=cfg.usa_io_data_year,
        target_year=cfg.model_base_year,
    )
    return B


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
    summary_corresp = get_bea_v2017_summary_to_cornerstone_corresp_df()
    summary_Y = derive_summary_Ytot_usa_matrix_set(get_usa_config().usa_io_data_year)
    cfg = get_usa_config()

    common = dict(
        corresp_df=summary_corresp,
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


# ---------------------------------------------------------------------------
# Summary-level functions (reused from derived_2017 — they don't depend on taxonomy)
# ---------------------------------------------------------------------------

# derive_summary_Adom_usa, derive_summary_Aimp_usa, derive_summary_q_usa,
# derive_summary_Ytot_usa_matrix_set, derive_summary_Yimp_usa
# are imported from derived_2017 and used directly. They operate at BEA summary
# level and are independent of the detail taxonomy choice.
