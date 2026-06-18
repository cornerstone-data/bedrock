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
from bedrock.utils.math.formulas import (
    compute_A_matrix,
    compute_q,
    compute_Unorm_matrix,
    compute_Vnorm_matrix,
    compute_x,
)
from bedrock.utils.math.handle_negatives import handle_negative_matrix_values

# ---------------------------------------------------------------------------
# Core output vectors
# ---------------------------------------------------------------------------


@functools.cache
def bea_x() -> pd.Series[float]:
    """Industry total output in BEA 2017 space."""
    return compute_x(V=load_2017_V_usa())


@functools.cache
def bea_q() -> pd.Series[float]:
    """Commodity total output in BEA 2017 space."""
    return compute_q(V=load_2017_V_usa())


# ---------------------------------------------------------------------------
# Normalised make matrix (scrap-corrected)
# ---------------------------------------------------------------------------


@functools.cache
def bea_Vnorm_scrap_corrected() -> pd.DataFrame:
    """Scrap-corrected V_norm in BEA 2017 space.

    Matches CEDA v7's column-wise correction: each commodity column j is
    divided by (1 − scrap_j / q_j), where scrap_j is the scrap output of the
    industry sharing code j.
    """
    V = load_2017_V_usa()
    q = bea_q()
    Vnorm = compute_Vnorm_matrix(V=V, q=q)
    scrap = V.loc[:, 'S00401']
    return Vnorm.divide((1.0 - (scrap / q).fillna(0.0)))


# ---------------------------------------------------------------------------
# A matrices and q
# ---------------------------------------------------------------------------


@functools.cache
def bea_Aq() -> tuple[pd.DataFrame, pd.DataFrame, pd.Series[float]]:
    """(Adom, Aimp, q) in BEA 2017 space, with scrap correction."""
    x = bea_x()
    Vnorm = bea_Vnorm_scrap_corrected()

    Utot = load_2017_Utot_usa()
    Uimp = load_2017_Uimp_usa()
    Udom = handle_negative_matrix_values(Utot - Uimp)
    Uimp_clean = handle_negative_matrix_values(Uimp)

    Adom = compute_A_matrix(
        U_norm=compute_Unorm_matrix(U=Udom, x=x),
        V_norm=Vnorm,
    )
    Aimp = compute_A_matrix(
        U_norm=compute_Unorm_matrix(U=Uimp_clean, x=x),
        V_norm=Vnorm,
    )
    return Adom, Aimp, bea_q()
