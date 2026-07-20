"""3×3 monetary toy IO tables with a 221110 generation analogue."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from bedrock.transform.eeio.electricity_disaggregation import GENERATION_SECTOR
from bedrock.utils.math.formulas import (
    backcompute_y_from_A_and_q,
    compute_A_matrix,
    compute_B_ind_matrix,
    compute_B_matrix,
    compute_q,
    compute_Unorm_matrix,
    compute_Vnorm_matrix,
    compute_x,
)
from bedrock.utils.taxonomy.cornerstone.value_added import VALUE_ADDEDS

TOY_SECTORS: tuple[str, ...] = (GENERATION_SECTOR, 'C1', 'C2')
FD_COLUMN = 'F01000'


@dataclass(frozen=True)
class ToyMonetaryTables:
    """Balanced 3×3 monetary cornerstone-style tables."""

    V: pd.DataFrame
    U: pd.DataFrame
    VA: pd.DataFrame
    Y: pd.DataFrame
    B: pd.DataFrame
    A: pd.DataFrame
    q: pd.Series[float]
    x: pd.Series[float]
    y_d: pd.Series[float]
    y_nab: pd.Series[float]


def _coefficient_matrix() -> pd.DataFrame:
    codes = list(TOY_SECTORS)
    a = pd.DataFrame(0.0, index=codes, columns=codes)
    a.loc[GENERATION_SECTOR, GENERATION_SECTOR] = 0.05
    a.loc[GENERATION_SECTOR, 'C1'] = 0.02
    a.loc[GENERATION_SECTOR, 'C2'] = 0.01
    a.loc['C1', GENERATION_SECTOR] = 0.01
    a.loc['C1', 'C1'] = 0.10
    a.loc['C1', 'C2'] = 0.02
    a.loc['C2', GENERATION_SECTOR] = 0.01
    a.loc['C2', 'C1'] = 0.01
    a.loc['C2', 'C2'] = 0.08
    return a


def build_toy_monetary_tables() -> ToyMonetaryTables:
    """
    Build row-balanced 3×3 monetary tables from a diagonal Make matrix.

    Industries make only their own commodity on the diagonal, so
    ``U[c, j] = A[c, j] * q[j]`` and ``Adom = Unorm @ Vnorm`` reproduces the
    chosen coefficient matrix.
    """
    codes = list(TOY_SECTORS)
    q_values = {GENERATION_SECTOR: 500.0, 'C1': 100.0, 'C2': 100.0}
    q = pd.Series(q_values, dtype=float)
    a = _coefficient_matrix()

    v = pd.DataFrame(0.0, index=codes, columns=codes)
    for code in codes:
        v.loc[code, code] = float(q[code])

    u = pd.DataFrame(0.0, index=codes, columns=codes)
    for col in codes:
        u.loc[:, col] = a.loc[:, col] * float(q[col])

    y_nab = backcompute_y_from_A_and_q(A=a, q=q)
    y = pd.DataFrame(0.0, index=codes, columns=[FD_COLUMN])
    y.loc[GENERATION_SECTOR, FD_COLUMN] = float(y_nab[GENERATION_SECTOR])
    y.loc['C1', FD_COLUMN] = float(y_nab['C1'])
    y.loc['C2', FD_COLUMN] = float(y_nab['C2'])

    va_rows = list(VALUE_ADDEDS[:3])
    va = pd.DataFrame(
        {
            GENERATION_SECTOR: [200.0, 80.0, 120.0],
            'C1': [30.0, 10.0, 20.0],
            'C2': [25.0, 8.0, 15.0],
        },
        index=va_rows,
    )

    x = compute_x(V=v)
    e_ind = pd.DataFrame(
        [[400.0, 50.0, 45.0]],
        index=['GHG1'],
        columns=codes,
    )
    vnorm = compute_Vnorm_matrix(V=v, q=q)
    b = compute_B_matrix(B_ind=compute_B_ind_matrix(E=e_ind, x=x), V_norm=vnorm)

    a_derived, q_derived, _ = derive_aq_from_flows(v, u)
    pd.testing.assert_frame_equal(a, a_derived, atol=1e-9, rtol=0.0)
    pd.testing.assert_series_equal(q, q_derived, atol=1e-9, rtol=0.0)

    y_d = y.sum(axis=1).astype(float)
    pd.testing.assert_series_equal(y_d, y_nab, atol=1e-9, rtol=0.0)

    return ToyMonetaryTables(
        V=v,
        U=u,
        VA=va,
        Y=y,
        B=b,
        A=a,
        q=q,
        x=x,
        y_d=y_d,
        y_nab=y_nab,
    )


def derive_aq_from_flows(
    v: pd.DataFrame,
    u: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.Series[float], pd.Series[float]]:
    """Rederive domestic ``A`` and ``q`` from Make/Use flow tables."""
    q = compute_q(V=v)
    x = compute_x(V=v)
    vnorm = compute_Vnorm_matrix(V=v, q=q)
    unorm = compute_Unorm_matrix(U=u, x=x)
    a = compute_A_matrix(U_norm=unorm, V_norm=vnorm)
    return a, q, x


def toy_end_use_map() -> dict[str, str]:
    """Class-price mapping for the 3×3 toy (distinct end uses per column)."""
    return {
        GENERATION_SECTOR: 'Industrial',
        'C1': 'Residential',
        'C2': 'Commercial',
        FD_COLUMN: 'Industrial',
    }


def toy_prices_cents_kwh() -> dict[str, float]:
    return {
        'Residential': 12.0,
        'Commercial': 10.0,
        'Industrial': 7.0,
        'Transportation': 9.0,
        'Total': 10.0,
    }
