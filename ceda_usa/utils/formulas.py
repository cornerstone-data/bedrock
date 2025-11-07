"""
Indices
-------
  - i: industry (+country for MRIO models)
  - c: commodity (+country for MRIO models)
  - g: greenhouse gas
  - s: emissions source

Matrices
--------
  - V (i,c) : make matrix [USD]
  - U (c,i) : use matrix [USD]
  - q (c,)  : commodity output [USD]
  - g (i,)  : industry output [USD]
  - A (c,c) : direct requirements [USD/USD]
  - L (c,c) : total requirements [USD/USD]
  - B (g,c) : direct emissions factor [kgco2e/USD]
  - M (g,c) : total (direct+upstream) emissions factor [kgco2e/USD]
  - E (s,i) : total emissions [kgco2e]

See also
--------
https://www.notion.so/watershedclimate/EEIO-Analysis-c47e33641c5943fda3ff97a7d3fe7568?pvs=4
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# golden path


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


# derivations


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
