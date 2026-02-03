"""
Indices
-------
  - i: industry (+country for MRIO models)
  - c: commodity (+country for MRIO models)
  - g: greenhouse gas (GHG)
  - s: emissions source

Matrices
--------
  - V (i,c) : make matrix [USD]
  - U (c,i) : use matrix [USD]
  - q (c,)  : commodity output [USD]
  - g (i,)  : industry output [USD]
  - A (c,c) : direct requirements [USD/USD]
  - L (c,c) : total requirements [USD/USD]
  - B (g,c) : direct emissions factor by GHG [kgco2e/USD]
  - M (g,c) : total (direct+upstream) emissions factor by GHG [kgco2e/USD]
  - E (s,i) : total emissions by GHG [kgco2e]

"""

from __future__ import annotations

import logging
from typing import Annotated

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ------------------------------#
# core computations
# ------------------------------#


def compute_g(*, V: pd.DataFrame) -> pd.Series[float]:
    """
    V is the Make matrix (industry x commodity), axis = 1 produces industry output
    """
    return V.sum(axis=1)


def compute_q(*, V: pd.DataFrame) -> pd.Series[float]:
    """
    V is the Make matrix (industry x commodity), axis = 0 produces commodity output
    """
    return V.sum(axis=0)


def compute_Unorm_matrix(*, U: pd.DataFrame, g: pd.Series[float]) -> pd.DataFrame:
    """
    U is the intermediate transaction part of comprehensive Use matrix (commodity x industry) representing "industries use commodities",
    i.e. the part of Final Demand consuming commodities are excluded.

    g is the industry output vector (industry x 1).

    This function generates direct requirements matrix (commodity x industry) that shows
    how much of each commodity is required to produce one unit of output of each industry.
    """
    return U.divide(g, axis=1).fillna(0)


def compute_Vnorm_matrix(*, V: pd.DataFrame, q: pd.Series[float]) -> pd.DataFrame:
    """
    V is the Make matrix (industry x commodity)

    q is the commodity output vector (commodity x 1).

    This function generates market shares matrix (industry x commodity) that shows
    how much of each industry contribute to the production of each commodity.
    """
    return V.divide(q, axis=1).fillna(0)


def compute_A_matrix(*, U_norm: pd.DataFrame, V_norm: pd.DataFrame) -> pd.DataFrame:
    """
    U_norm is the normalized Use matrix (commodity x industry) representing "industries use commodities".
    V_norm is the normalized Make matrix (industry x commodity) representing "industries supply commodities".

    This function generates direct requirements matrix in commodity x commodity format that shows
    how much of each commodity is required to produce one unit of output of each commodity.
    """
    return U_norm @ V_norm


def compute_L_matrix(*, A: pd.DataFrame) -> pd.DataFrame:
    # (I - A)^-1 ... invert the matrix (I - A)
    if A.shape[0] > 1000:
        logger.info(
            f"computing L for a matrix of shape {A.shape}... this may take a while"
        )
    return pd.DataFrame(
        np.linalg.inv(np.identity(A.shape[0]) - A), index=A.index, columns=A.columns
    )


def compute_B_ind_matrix(*, E: pd.DataFrame, g: pd.Series[float]) -> pd.DataFrame:
    return E.divide(g, axis=1).fillna(0)


def compute_B_matrix(*, B_ind: pd.DataFrame, V_norm: pd.DataFrame) -> pd.DataFrame:
    """
    B_ind is the direct emissions matrix (greenhouse gas x industry) representing direct emissions of each industry.
    V_norm is the normalized Make matrix (industry x commodity) representing "commodities supplied by industries".

    This function generates direct emissions matrix in greenhouse gas x commodity format that shows
    direct emissions of each greenhouse gas associated with the production of each commodity.
    """
    return B_ind @ V_norm


def compute_M_matrix(*, B: pd.DataFrame, L: pd.DataFrame) -> pd.DataFrame:
    return B @ L


def compute_n(*, M: pd.DataFrame) -> pd.Series[float]:
    # 1 @ M ... sum the rows of M
    return M.sum(axis=0)


def compute_d(*, B: pd.DataFrame) -> pd.Series[float]:
    # 1 @ B ... sum the rows of B
    return B.sum(axis=0)


# ------------------------------#
# derivations
# ------------------------------#


def compute_input_contribution(*, A: pd.DataFrame, N: pd.Series[float]) -> pd.DataFrame:
    # diag(N) @ A  ... scale the rows of A by N
    return A.multiply(N, axis=0)


def compute_output_contribution(
    *, L: pd.DataFrame, D: pd.Series[float]
) -> pd.DataFrame:
    # diag(D) @ L ... scale the rows of L by D
    return L.multiply(D, axis=0)


def compute_total_industry_inputs(*, A: pd.DataFrame) -> pd.Series[float]:
    return A.sum(axis=0)


def compute_y_imp(*, imports: pd.Series[float], Uimp: pd.DataFrame) -> pd.Series[float]:
    return imports - Uimp.sum(axis=1)


def compute_y_for_national_accounting_balance(
    *,
    y_tot: pd.Series[float],
    y_imp: pd.Series[float],
    exports: pd.Series[float],
) -> pd.Series[float]:
    return y_tot - y_imp + exports


def compute_E_from_BLy(
    *, B: pd.DataFrame, L: pd.DataFrame, y: pd.Series[float]
) -> pd.DataFrame:
    return B.multiply(L.multiply(y, axis=1).sum(axis=1), axis=1)


# ------------------------------#
# backward computation
# ------------------------------#


def backcompute_E_matrix_via_commodity_shortcut(
    *, B: pd.DataFrame, q: pd.Series[float]
) -> pd.DataFrame:
    # E = B @ diag(q) ... scale the rows of B by q
    return B.multiply(q, axis=1)


def backcompute_U_matrix_via_commodity_shortcut(
    *, A: pd.DataFrame, q: pd.Series[float]
) -> pd.DataFrame:
    return A.multiply(q, axis=1)


def backcompute_q_from_L_and_y(
    *,
    L: Annotated[pd.DataFrame, "Domestic Leontief inverse (Ldom)"],
    y: Annotated[pd.Series[float], "National accounting balance final demand (y_nab)"],
) -> pd.Series[float]:
    """
    Backcompute commodity output q from Leontief inverse L and final demand y.

    .. warning::
        **CRITICAL**: This function requires a matching pair of L and y:
        - L must be the **domestic** Leontief inverse (Ldom)
        - y must be the **national accounting balance** final demand (y_nab),
          i.e., domestic final consumption + exports

        Using mismatched pairs (e.g., total L with domestic y, or domestic L
        with total y) will produce incorrect results.

    This way we capture all possible uses of a commodity, including:
    1. intermediate consumption by domestic industries
    2. domestic final consumption
    3. exports to foreign industries and final consumption

    Parameters
    ----------
    L : pd.DataFrame
        The domestic Leontief inverse matrix (Ldom)
    y : pd.Series[float]
        Final demand for national accounting balance (y_nab)

    Returns
    -------
    pd.Series[float]
        Commodity output vector q

    Raises
    ------
    ValueError
        If L is not square, or if L and y have mismatched indices
    """
    # Validation with helpful error messages
    if not L.index.equals(L.columns):
        raise ValueError(
            "L must be a square matrix (Leontief inverse). "
            f"Expected L.index == L.columns, but got shape {L.shape}"
        )
    if not L.index.equals(y.index):
        raise ValueError(
            "L and y must have matching indices. "
            f"L.index length: {len(L.index)}, y.index length: {len(y.index)}. "
            "This function requires L to be the domestic Leontief inverse (Ldom) "
            "and y to be the national accounting balance final demand (y_nab)."
        )

    return (L @ np.diag(y)).sum(axis=1)


def backcompute_y_from_q_and_Aq(
    *, A: pd.DataFrame, q: pd.Series[float]
) -> pd.Series[float]:
    return q - A.multiply(q, axis=1).sum(axis=1)


# ------------------------------#
# approximations
# ------------------------------#


def approximate_q_from_U(*, U: pd.DataFrame) -> pd.Series[float]:
    # U will not contain ALL users of a commodity, so the sum is approximate
    return U.sum(axis=1)
