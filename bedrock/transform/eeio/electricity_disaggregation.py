"""221100 electricity co-production reallocation for the Cornerstone IO pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

import numpy as np
import numpy.typing as npt
import pandas as pd

ELECTRICITY_AGGREGATE = "221100"
BALANCE_TOLERANCE = 1e6


def _float_ndarray(values: npt.ArrayLike) -> npt.NDArray[np.float64]:
    return np.asarray(values, dtype=np.float64)


def _frame_cell_float(frame: pd.DataFrame, row: str, col: str) -> float:
    return cast(float, frame.at[row, col])


@dataclass(frozen=True)
class CoprodTransfer:
    source: str
    target: str
    amount: float


def build_coproduction_transfer_schedule(V: pd.DataFrame) -> list[CoprodTransfer]:
    """
    This function is creating an ordered list of transfers of the electricity
    re-allocations for the make table 221100 row/column off-diagonals which are
    carried out in reallocate_electricity_coproduction() function one at a time.

    Inbound transfers (other industries -> 221100 diagonal) run first, then
    outbound transfers (221100 row -> other commodity diagonals).

    This order matters in two ways:
    1) The movements for all tables (Make, Use, VA) have to be done for each step before
    the next movement for any of these tables can be done, or else the totals will not match.
    2) Applying inbound transfers first results in smaller transfers out of the Use and VA table's
    221100 industry column in absolute value.

    """
    agg = ELECTRICITY_AGGREGATE
    inbound_to_221100_diagonal: list[tuple[float, CoprodTransfer]] = []
    outbound_from_221100_diagonal: list[tuple[float, CoprodTransfer]] = []

    for s in V.index:
        if s == agg:
            continue
        t = _frame_cell_float(V, str(s), agg)
        if t > 0:
            inbound_to_221100_diagonal.append(
                (t, CoprodTransfer(source=str(s), target=agg, amount=t))
            )

    for d in V.columns:
        if d == agg:
            continue
        t = _frame_cell_float(V, agg, str(d))
        if t > 0:
            outbound_from_221100_diagonal.append(
                (t, CoprodTransfer(source=agg, target=str(d), amount=t))
            )

    inbound_to_221100_diagonal.sort(key=lambda x: x[0], reverse=True)
    outbound_from_221100_diagonal.sort(key=lambda x: x[0], reverse=True)
    return [tr for _, tr in inbound_to_221100_diagonal] + [
        tr for _, tr in outbound_from_221100_diagonal
    ]


def _assert_row_totals_unchanged(
    before: pd.DataFrame,
    after: pd.DataFrame,
    *,
    label: str,
) -> None:
    row_before = before.sum(axis=1)
    row_after = after.sum(axis=1)
    np.testing.assert_allclose(
        _float_ndarray(row_after.to_numpy()),
        _float_ndarray(row_before.to_numpy()),
        rtol=1e-9,
        atol=1.0,
        err_msg=f"{label} row totals changed",
    )


def _make_diagonal(V: pd.DataFrame, industry: str) -> float:
    if industry in V.index and industry in V.columns:
        return _frame_cell_float(V, industry, industry)
    return 0.0


def apply_single_coproduction_transfer(
    V: pd.DataFrame,
    Udom: pd.DataFrame,
    Uimp: pd.DataFrame,
    VA: pd.DataFrame,
    transfer: CoprodTransfer,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Apply one co-production transfer and run post-transfer assertions."""
    s, d = transfer.source, transfer.target
    T = transfer.amount

    row_sum_s = cast(float, V.loc[s, :].sum())
    if row_sum_s == 0:
        raise ValueError(f"Cannot transfer from industry {s!r}: Make row sum is zero")
    R = T / row_sum_s

    V = V.copy()
    Udom = Udom.copy()
    Uimp = Uimp.copy()
    VA = VA.copy()

    udom_before = Udom.copy()
    uimp_before = Uimp.copy()
    va_before = VA.copy()

    V.loc[d, d] = _make_diagonal(V, d) + T
    V.loc[s, d] = 0.0

    for frame in (Udom, Uimp, VA):
        for r in frame.index:
            shift = R * _frame_cell_float(frame, str(r), s)
            frame.loc[r, s] -= shift
            frame.loc[r, d] += shift

    _assert_row_totals_unchanged(udom_before, Udom, label="Udom")
    _assert_row_totals_unchanged(uimp_before, Uimp, label="Uimp")
    _assert_row_totals_unchanged(va_before, VA, label="VA")

    if (V < -1e-6).any().any():
        raise AssertionError("Make has negative values after transfer")

    return V, Udom, Uimp, VA


def reallocate_electricity_coproduction(
    V: pd.DataFrame,
    Udom: pd.DataFrame,
    Uimp: pd.DataFrame,
    VA: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Run the full 221100 co-production reallocation schedule on Make/Use/VA.

    Final demand (Y) is not modified.
    """
    V = V.copy()
    Udom = Udom.copy()
    Uimp = Uimp.copy()
    VA = VA.copy()

    schedule = build_coproduction_transfer_schedule(V)
    for transfer in schedule:
        V, Udom, Uimp, VA = apply_single_coproduction_transfer(
            V, Udom, Uimp, VA, transfer
        )

    assert_221100_make_sparsity(V)
    return V, Udom, Uimp, VA


def assert_221100_make_sparsity(V: pd.DataFrame, *, atol: float = 1.0) -> None:
    """Raise AssertionError if 221100 row/col off-diagonals exceed atol."""
    agg = ELECTRICITY_AGGREGATE
    non_agg_cols = V.columns.drop(agg)
    non_agg_rows = V.index.drop(agg)
    row_off = cast(pd.Series, V.loc[agg]).reindex(non_agg_cols)
    col_off = V[agg].reindex(non_agg_rows)
    if (row_off.abs() > atol).any() or (col_off.abs() > atol).any():
        raise AssertionError(
            f"221100 co-production off-diagonals remain above {atol}: "
            f"row_max={float(row_off.abs().max())}, "
            f"col_max={float(col_off.abs().max())}"
        )
