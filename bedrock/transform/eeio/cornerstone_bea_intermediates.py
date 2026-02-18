"""BEA 2017 ~400-sector intermediate matrices used by the Cornerstone pipeline.

These computations live entirely in the original BEA detail space.  Results are
consumed by ``derived_cornerstone`` which expands them to the 405-sector
Cornerstone taxonomy.
"""

from __future__ import annotations

import functools

import pandas as pd

from bedrock.extract.iot.io_2017 import (
    load_2017_Uimp_usa,
    load_2017_Utot_usa,
    load_2017_V_usa,
)
from bedrock.transform.allocation.derived import derive_E_usa
from bedrock.utils.math.formulas import (
    compute_A_matrix,
    compute_g,
    compute_q,
    compute_Unorm_matrix,
    compute_Vnorm_matrix,
)
from bedrock.utils.math.handle_negatives import handle_negative_matrix_values
from bedrock.utils.taxonomy.usa_taxonomy_correspondence_helpers import (
    load_usa_2017_industry__ceda_v7_correspondence,
)

# ---------------------------------------------------------------------------
# Core output vectors
# ---------------------------------------------------------------------------


@functools.cache
def bea_g() -> pd.Series[float]:
    """Industry total output in BEA 2017 space."""
    return compute_g(V=load_2017_V_usa())


@functools.cache
def bea_q() -> pd.Series[float]:
    """Commodity total output in BEA 2017 space."""
    return compute_q(V=load_2017_V_usa())


# ---------------------------------------------------------------------------
# Normalised make matrix (scrap-corrected)
# ---------------------------------------------------------------------------


@functools.cache
def bea_Vnorm_scrap_corrected() -> pd.DataFrame:
    """Scrap-corrected V_norm in BEA 2017 space."""
    V = load_2017_V_usa()
    q = bea_q()
    g = bea_g()
    Vnorm = compute_Vnorm_matrix(V=V, q=q)
    scrap = V.loc[:, 'S00401']
    return Vnorm.divide((1.0 - (scrap / g).fillna(0.0)), axis=0)


# ---------------------------------------------------------------------------
# A matrices and q
# ---------------------------------------------------------------------------


@functools.cache
def bea_Aq() -> tuple[pd.DataFrame, pd.DataFrame, pd.Series[float]]:
    """(Adom, Aimp, q) in BEA 2017 space, with scrap correction."""
    g = bea_g()
    Vnorm = bea_Vnorm_scrap_corrected()

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
    return Adom, Aimp, bea_q()


# ---------------------------------------------------------------------------
# Emissions: CEDA v7 → BEA industry → B
# ---------------------------------------------------------------------------


@functools.cache
def _ceda_v7_industry_corresp() -> pd.DataFrame:
    """CEDA v7 → BEA 2017 industry correspondence (rows=CEDA v7, cols=BEA industry)."""
    return load_usa_2017_industry__ceda_v7_correspondence()


@functools.cache
def _g_weighted_ceda_industry_corresp() -> pd.DataFrame:
    """CEDA v7 → BEA industry correspondence, row-normalized by industry output (g).

    Handles both disaggregation (one CEDA v7 → multiple BEA codes) and
    government aggregation by weighting proportionally by g.
    """
    corresp = _ceda_v7_industry_corresp()
    g = bea_g()
    g_aligned = g.reindex(corresp.columns, fill_value=0.0)
    weighted = corresp.multiply(g_aligned, axis=1)
    row_sums = weighted.sum(axis=1)
    return weighted.div(row_sums.replace(0, 1), axis=0)


@functools.cache
def bea_E() -> pd.DataFrame:
    """E (ghg × BEA_industry) — CEDA v7 emissions mapped to BEA industry space."""
    return derive_E_usa() @ _g_weighted_ceda_industry_corresp()


@functools.cache
def bea_B() -> pd.DataFrame:
    """B (ghg × BEA_commodity).  B = (E / g) @ V_norm."""
    E = bea_E()
    g = bea_g()
    Vnorm = bea_Vnorm_scrap_corrected()
    Bi = E.divide(g, axis=1).fillna(0.0)
    return Bi @ Vnorm
