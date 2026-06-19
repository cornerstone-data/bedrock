"""Weight builders for d_85 scenarios (UGO305-A vs EPA Table 8.3)."""

from __future__ import annotations

import pandas as pd

from bedrock.analysis.electricity.d_85.eia_inputs import table_8_3_gtd_expenses_musd
from bedrock.transform.eeio.electricity_disaggregation import (
    build_electricity_disagg_go_weights,
)
from bedrock.utils.schemas.cornerstone_schemas import ELECTRICITY_DISAGG_SECTORS

ELEC_CODES: list[str] = list(ELECTRICITY_DISAGG_SECTORS)


def ugo305_go_weights(year: int = 2017) -> pd.Series[float]:
    """Return normalized GO shares for 221110/221121/221122.

    Currently wraps production ``build_electricity_disagg_go_weights()`` (2017 UGO305-A).
    """
    if year != 2017:
        raise NotImplementedError(
            f'ugo305_go_weights for year {year} not implemented; use 2017 or extend loader'
        )
    w = build_electricity_disagg_go_weights()
    return w.reindex(ELEC_CODES).astype(float)


def table83_go_weights(
    year: int = 2017, *, fba: pd.DataFrame | None = None
) -> pd.Series[float]:
    """Map Table 8.3 G/T/D expense shares to 221110/221121/221122."""
    expenses = table_8_3_gtd_expenses_musd(year, fba=fba)
    total = sum(expenses.values())
    return pd.Series(
        {
            '221110': expenses['Production'] / total,
            '221121': expenses['Transmission'] / total,
            '221122': expenses['Distribution'] / total,
        },
        dtype=float,
    )


def build_ugo_col_table83_row_intersection_matrix(
    w_ugo: pd.Series[float],
    w_83: pd.Series[float],
    total: float,
) -> pd.DataFrame:
    """Build 3×3 Use-intersection matrix for ``d8_offdiag`` (Rules D1/D2).

    Rows/columns indexed ``[221110, 221121, 221122]`` (commodity × industry).
    Cell values are absolute dollars summing to ``total`` (aggregate intersection T).
    """
    i, j, k = ELEC_CODES
    wu = w_ugo.reindex(ELEC_CODES).astype(float)
    w8 = w_83.reindex(ELEC_CODES).astype(float)
    t = float(total)

    # Rule D2 — column i (generation): 100% diagonal
    m_ii = float(wu[i]) * t
    # Rule D2 — columns j, k: row split by Table 8.3 shares within UGO column totals
    cells = {
        (i, i): m_ii,
        (j, i): 0.0,
        (k, i): 0.0,
        (i, j): float(wu[j]) * float(w8[i]) * t,
        (j, j): float(wu[j]) * float(w8[j]) * t,
        (k, j): float(wu[j]) * float(w8[k]) * t,
        (i, k): float(wu[k]) * float(w8[i]) * t,
        (j, k): float(wu[k]) * float(w8[j]) * t,
        (k, k): float(wu[k]) * float(w8[k]) * t,
    }
    matrix = pd.DataFrame(0.0, index=ELEC_CODES, columns=ELEC_CODES, dtype=float)
    for (row, col), val in cells.items():
        matrix.at[row, col] = val

    # Verification (Rule D1 + total preservation)
    for code in ELEC_CODES:
        col_sum = float(matrix[code].sum())
        expected = float(wu[code]) * t
        if abs(col_sum - expected) > 1e-6 * max(abs(expected), 1.0):
            raise AssertionError(
                f'D1 failed for column {code}: sum={col_sum}, expected={expected}'
            )
    if abs(float(matrix.sum().sum()) - t) > 1e-6 * max(abs(t), 1.0):
        raise AssertionError(f'Total preservation failed: {matrix.sum().sum()} != {t}')
    return matrix
