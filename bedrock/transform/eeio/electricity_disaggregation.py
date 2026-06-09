"""221100 electricity co-production reallocation and sector disaggregation."""

from __future__ import annotations

import functools
import logging
import pathlib
import warnings
from dataclasses import dataclass
from typing import cast

import numpy as np
import numpy.typing as npt
import pandas as pd

from bedrock.extract.disaggregation.disagg_weights import DisaggWeights, weights_to_csv
from bedrock.extract.iot.gdp import SECTOR_NAME_COL, load_go_detail
from bedrock.transform.eeio.waste_disaggregation import (
    apply_waste_disagg_to_V,
)
from bedrock.utils.math.formulas import compute_x
from bedrock.utils.schemas.cornerstone_schemas import (
    CORNERSTONE_COMMODITIES_ELEC,
    CORNERSTONE_INDUSTRIES_ELEC,
    ELECTRICITY_DISAGG_SECTORS,
)
from bedrock.utils.taxonomy.cornerstone.value_added import VALUE_ADDEDS

logger = logging.getLogger(__name__)

ELECTRICITY_AGGREGATE = "221100"
BALANCE_TOLERANCE = 1e6
DISAGG_BALANCE_ATOL = 1.0
IO_ACCOUNT_YEAR = 2017

GENERATION_GO_SECTOR_NAMES: tuple[str, ...] = (
    "Hydroelectric   power generation",
    "Fossil fuel electric power generation",
    "Nuclear electric power generation",
    "Solar electric power generation",
    "Wind electric power generation",
    "Geothermal electric power generation",
    "Biomass electric power generation",
    "Other electric power generation",
)
TRANSMISSION_GO_SECTOR_NAME = "Electric bulk power transmission and control"
DISTRIBUTION_GO_SECTOR_NAME = "Electric power distribution"
ALL_ELECTRICITY_GO_SECTOR_NAMES: tuple[str, ...] = (
    *GENERATION_GO_SECTOR_NAMES,
    TRANSMISSION_GO_SECTOR_NAME,
    DISTRIBUTION_GO_SECTOR_NAME,
)

GENERATION_FUEL_COMMODITIES: frozenset[str] = frozenset(
    {"212100", "211000", "324110", "424700", "221200"}
)

_WEIGHTS_EXPORT_DIR = (
    pathlib.Path(__file__).resolve().parents[2]
    / "extract"
    / "disaggregation"
    / "electricity_disagg_inputs"
)


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


# ---------------------------------------------------------------------------
# PR3 — 221100 → 221110 / 221121 / 221122 monetary disaggregation
# ---------------------------------------------------------------------------


def _normalize_sector_name(name: str) -> str:
    return " ".join(str(name).split())


def _assert_go_sector_names_present(go: pd.DataFrame) -> None:
    available = {_normalize_sector_name(n) for n in go[SECTOR_NAME_COL]}
    expected = {_normalize_sector_name(n) for n in ALL_ELECTRICITY_GO_SECTOR_NAMES}
    missing = expected - available
    if missing:
        raise ValueError(
            "UGO305-A missing expected electricity gross-output sector names: "
            f"{sorted(missing)}"
        )


def _resolve_go_year_column(go: pd.DataFrame, year: int) -> str | int:
    if year in go.columns:
        return year
    year_str = str(year)
    if year_str in go.columns:
        return year_str
    available = sorted(c for c in go.columns if c not in (SECTOR_NAME_COL, "Line"))
    raise ValueError(
        f"UGO305-A missing IO account year column {year}. "
        f"Available year columns: {available}"
    )


@functools.cache
def build_electricity_disagg_go_weights() -> pd.Series[float]:
    """Return GO shares w_221110, w_221121, w_221122 (sum to 1)."""
    go = load_go_detail()
    _assert_go_sector_names_present(go)
    year_col = _resolve_go_year_column(go, IO_ACCOUNT_YEAR)
    name_to_value: dict[str, float] = {}
    for raw_name in go[SECTOR_NAME_COL]:
        norm = _normalize_sector_name(raw_name)
        if norm in {_normalize_sector_name(n) for n in ALL_ELECTRICITY_GO_SECTOR_NAMES}:
            row = go.loc[go[SECTOR_NAME_COL] == raw_name].iloc[0]
            name_to_value[norm] = float(row[year_col])

    gen_total = sum(
        name_to_value[_normalize_sector_name(n)] for n in GENERATION_GO_SECTOR_NAMES
    )
    trans = name_to_value[_normalize_sector_name(TRANSMISSION_GO_SECTOR_NAME)]
    dist = name_to_value[_normalize_sector_name(DISTRIBUTION_GO_SECTOR_NAME)]
    total = gen_total + trans + dist
    if total <= 0:
        raise ValueError("Electricity gross-output total is non-positive")

    return pd.Series(
        {
            "221110": gen_total / total,
            "221121": trans / total,
            "221122": dist / total,
        },
        dtype=float,
    )


def _diagonal_intersection_weights(w: pd.Series[float]) -> pd.DataFrame:
    """3×3 diagonal-only intersection weight tables."""
    data = np.diag([float(w[s]) for s in ELECTRICITY_DISAGG_SECTORS])
    return pd.DataFrame(
        data,
        index=ELECTRICITY_DISAGG_SECTORS,
        columns=ELECTRICITY_DISAGG_SECTORS,
        dtype=float,
    )


def build_electricity_disagg_weights(w: pd.Series[float]) -> DisaggWeights:
    """Build programmatic DisaggWeights for steps 1–2 (intersection only)."""
    intersection = _diagonal_intersection_weights(w)
    empty = pd.DataFrame(dtype=float)
    default_row = pd.DataFrame(
        [w.reindex(ELECTRICITY_DISAGG_SECTORS).values],
        index=["__default__"],
        columns=ELECTRICITY_DISAGG_SECTORS,
        dtype=float,
    )
    return DisaggWeights(
        use_intersection=intersection,
        use_disagg_industry_columns_all_rows=default_row.copy(),
        use_disagg_commodity_rows_all_columns=default_row.copy(),
        use_disagg_rows_specific_columns=empty,
        use_va_rows_for_disagg_industry_columns=empty,
        use_fd_columns_for_disagg_commodity_rows=empty,
        make_intersection=intersection.copy(),
        make_disagg_commodity_columns_all_rows=default_row.copy(),
        make_disagg_commodity_columns_specific_rows=empty,
        make_disagg_industry_rows_specific_columns=empty,
        year=IO_ACCOUNT_YEAR,
        source_name="BEA_UGO305_A_electricity_go",
    )


def reindex_v_to_elec_schema(V: pd.DataFrame) -> pd.DataFrame:
    return V.reindex(
        index=CORNERSTONE_INDUSTRIES_ELEC,
        columns=CORNERSTONE_COMMODITIES_ELEC,
        fill_value=0.0,
    )


def reindex_u_to_elec_schema(U: pd.DataFrame) -> pd.DataFrame:
    return U.reindex(
        index=CORNERSTONE_COMMODITIES_ELEC,
        columns=CORNERSTONE_INDUSTRIES_ELEC,
        fill_value=0.0,
    )


def reindex_va_to_elec_schema(VA: pd.DataFrame) -> pd.DataFrame:
    return VA.reindex(columns=CORNERSTONE_INDUSTRIES_ELEC, fill_value=0.0)


def reindex_y_commodities_to_elec_schema(Y: pd.DataFrame) -> pd.DataFrame:
    return Y.reindex(index=CORNERSTONE_COMMODITIES_ELEC, fill_value=0.0)


def _column_total_use_plus_va(
    Udom: pd.DataFrame,
    Uimp: pd.DataFrame,
    VA: pd.DataFrame,
    industry: str,
) -> float:
    return (
        float(Udom[industry].sum())
        + float(Uimp[industry].sum())
        + float(VA[industry].sum())
    )


def _enforce_go_identity_precondition(
    V: pd.DataFrame,
    Udom: pd.DataFrame,
    Uimp: pd.DataFrame,
    VA: pd.DataFrame,
) -> None:
    """Ensure Make row-sum GO matches Use+VA column total before step 3.

    Small upstream residuals (post waste disagg / reallocation) are absorbed
    into the aggregate 221100 VA column so VA-row preservation remains feasible.
    """
    agg = ELECTRICITY_AGGREGATE
    x_make = float(compute_x(V=V)[agg])
    c_total = _column_total_use_plus_va(Udom, Uimp, VA, agg)
    residual = x_make - c_total
    if abs(residual) <= DISAGG_BALANCE_ATOL:
        return
    rel = abs(residual) / abs(x_make) if x_make else 0.0
    if rel > 0.01:
        raise AssertionError(
            f"221100 gross-output identity failed before step 3: "
            f"compute_x(V)[221100]={x_make}, "
            f"Udom+Uimp+VA column total={c_total}, "
            f"residual={residual} (relative={rel:.4%})"
        )
    warnings.warn(
        f"221100 GO identity residual {residual:,.0f} ({rel:.4%} of Make GO); "
        "absorbing into aggregate VA column before disaggregation",
        stacklevel=2,
    )
    va_col = VA[agg].astype(float)
    va_total = float(va_col.sum())
    if va_total != 0.0:
        VA.loc[list(VALUE_ADDEDS), agg] = va_col + residual * (va_col / va_total)
    else:
        VA.loc["V00300", agg] = float(VA.at["V00300", agg]) + residual
    c_after = _column_total_use_plus_va(Udom, Uimp, VA, agg)
    np.testing.assert_allclose(
        x_make,
        c_after,
        rtol=1e-9,
        atol=DISAGG_BALANCE_ATOL,
        err_msg="221100 GO identity still fails after VA absorption",
    )


def disaggregate_make_intersection(
    V: pd.DataFrame,
    weights: DisaggWeights,
) -> pd.DataFrame:
    """Step 1 — split 221100 Make diagonal into 3×3 diagonal block."""
    agg = ELECTRICITY_AGGREGATE
    orig_ind_total = float(V[agg].sum())
    orig_com_total = float(V.loc[agg].sum())
    V_out = apply_waste_disagg_to_V(V, weights, original_code=agg)
    V_out = reindex_v_to_elec_schema(V_out)
    new_codes = ELECTRICITY_DISAGG_SECTORS
    np.testing.assert_allclose(
        float(V_out[new_codes].sum().sum()),
        orig_ind_total,
        rtol=1e-9,
        atol=DISAGG_BALANCE_ATOL,
        err_msg="Make industry total not preserved",
    )
    np.testing.assert_allclose(
        float(V_out.loc[new_codes].sum().sum()),
        orig_com_total,
        rtol=1e-9,
        atol=DISAGG_BALANCE_ATOL,
        err_msg="Make commodity total not preserved",
    )
    return V_out


def disaggregate_use_intersection(
    Udom: pd.DataFrame,
    Uimp: pd.DataFrame,
    w: pd.Series[float],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Step 2 — diagonal-only Use intersection split."""
    agg = ELECTRICITY_AGGREGATE
    results: list[pd.DataFrame] = []
    for U in (Udom, Uimp):
        U = U.copy()
        orig = float(U.at[agg, agg])
        for code in ELECTRICITY_DISAGG_SECTORS:
            if code not in U.index:
                U.loc[code] = 0.0
            if code not in U.columns:
                U[code] = 0.0
            U.at[code, code] = orig * float(w[code])
        U.at[agg, agg] = 0.0
        results.append(U)
    return results[0], results[1]


def _split_aggregate_column_by_rule(
    U: pd.DataFrame,
    *,
    w: pd.Series[float],
    va_rows: list[str],
) -> pd.DataFrame:
    """Split the 221100 industry column across disagg columns (step 3 inputs)."""
    agg = ELECTRICITY_AGGREGATE
    elec_set = set(ELECTRICITY_DISAGG_SECTORS)
    U = U.copy()
    for row in U.index:
        if row in elec_set or row == agg:
            continue
        val = float(U.at[row, agg])
        if val == 0.0:
            for code in ELECTRICITY_DISAGG_SECTORS:
                U.at[row, code] = 0.0
            continue
        if row in GENERATION_FUEL_COMMODITIES:
            U.at[row, "221110"] = val
            U.at[row, "221121"] = 0.0
            U.at[row, "221122"] = 0.0
        elif row in va_rows:
            continue
        else:
            for code in ELECTRICITY_DISAGG_SECTORS:
                U.at[row, code] = val * float(w[code])
        U.at[row, agg] = 0.0
    return U


def disaggregate_use_industry_columns(
    x_agg: float,
    Udom: pd.DataFrame,
    Uimp: pd.DataFrame,
    VA: pd.DataFrame,
    w: pd.Series[float],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Step 3 — split 221100 industry column + VA balancing."""
    agg = ELECTRICITY_AGGREGATE
    va_rows = list(VALUE_ADDEDS)
    orig_va = VA[agg].copy()
    orig_row_totals: dict[str, float] = {}
    for com in Udom.index:
        if com in set(ELECTRICITY_DISAGG_SECTORS) | {agg} | set(va_rows):
            continue
        orig_row_totals[com] = float(Udom.at[com, agg]) + float(Uimp.at[com, agg])

    Udom = _split_aggregate_column_by_rule(Udom, w=w, va_rows=va_rows)
    Uimp = _split_aggregate_column_by_rule(Uimp, w=w, va_rows=va_rows)

    VA = VA.copy()
    for code in ELECTRICITY_DISAGG_SECTORS:
        if code not in VA.columns:
            VA[code] = 0.0

    va_share = orig_va / float(orig_va.sum()) if float(orig_va.sum()) != 0 else (
        pd.Series(1.0 / len(va_rows), index=va_rows)
    )

    for code in ELECTRICITY_DISAGG_SECTORS:
        x_s = float(w[code]) * x_agg
        inputs_s = float(Udom[code].sum()) + float(Uimp[code].sum())
        va_total_s = x_s - inputs_s
        if va_total_s < 0:
            warnings.warn(
                f"Negative VA total for electricity sub-industry {code}: "
                f"{va_total_s}",
                stacklevel=2,
            )
        for va_row in va_rows:
            VA.at[va_row, code] = float(va_share[va_row]) * va_total_s
        col_total = inputs_s + float(VA[code].sum())
        np.testing.assert_allclose(
            col_total,
            x_s,
            rtol=1e-9,
            atol=DISAGG_BALANCE_ATOL,
            err_msg=f"Column {code} does not balance to gross output",
        )

    for va_row in va_rows:
        orig_row_total = float(orig_va[va_row])
        new_total = float(VA.loc[va_row, ELECTRICITY_DISAGG_SECTORS].sum())
        np.testing.assert_allclose(
            new_total,
            orig_row_total,
            rtol=1e-9,
            atol=DISAGG_BALANCE_ATOL,
            err_msg=f"VA row {va_row} total not preserved",
        )

    for com, orig_val in orig_row_totals.items():
        new_val = (
            float(Udom.loc[com, ELECTRICITY_DISAGG_SECTORS].sum())
            + float(Uimp.loc[com, ELECTRICITY_DISAGG_SECTORS].sum())
        )
        np.testing.assert_allclose(
            new_val,
            orig_val,
            rtol=1e-9,
            atol=DISAGG_BALANCE_ATOL,
            err_msg=f"Use row {com} industry-column total not preserved",
        )

    Udom = Udom.drop(columns=[agg], errors="ignore")
    Uimp = Uimp.drop(columns=[agg], errors="ignore")
    VA = VA.drop(columns=[agg], errors="ignore")
    return Udom, Uimp, VA


def disaggregate_use_commodity_rows(
    Udom: pd.DataFrame,
    Uimp: pd.DataFrame,
    w: pd.Series[float],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Step 4 — split 221100 commodity row across consumers (equal-price w_k)."""
    agg = ELECTRICITY_AGGREGATE
    elec_set = set(ELECTRICITY_DISAGG_SECTORS)
    orig_col_totals: dict[str, float] = {}
    for col in Udom.columns:
        if col in elec_set:
            continue
        orig_col_totals[col] = float(Udom.at[agg, col]) + float(Uimp.at[agg, col])
    results: list[pd.DataFrame] = []
    for U in (Udom, Uimp):
        U = U.copy()
        for col in U.columns:
            if col in elec_set:
                continue
            orig = float(U.at[agg, col])
            for code in ELECTRICITY_DISAGG_SECTORS:
                if code not in U.index:
                    U.loc[code] = 0.0
                U.at[code, col] = orig * float(w[code])
            U.at[agg, col] = 0.0
        results.append(U)
    Udom_out, Uimp_out = results
    for col, orig in orig_col_totals.items():
        new = (
            float(Udom_out.loc[ELECTRICITY_DISAGG_SECTORS, col].sum())
            + float(Uimp_out.loc[ELECTRICITY_DISAGG_SECTORS, col].sum())
        )
        np.testing.assert_allclose(
            new,
            orig,
            rtol=1e-9,
            atol=DISAGG_BALANCE_ATOL,
            err_msg=f"Commodity row split failed for column {col}",
        )
    Udom_out = Udom_out.drop(index=[agg], errors="ignore")
    Uimp_out = Uimp_out.drop(index=[agg], errors="ignore")
    return Udom_out, Uimp_out


def disaggregate_electricity_commodity_row_in_y(
    Y: pd.DataFrame,
    w: pd.Series[float],
) -> pd.DataFrame:
    """Step 4 (Y) — split 221100 commodity row across FD columns."""
    agg = ELECTRICITY_AGGREGATE
    Y = Y.copy()
    for col in Y.columns:
        orig = float(Y.at[agg, col])
        for code in ELECTRICITY_DISAGG_SECTORS:
            if code not in Y.index:
                Y.loc[code] = 0.0
            Y.at[code, col] = orig * float(w[code])
        Y.at[agg, col] = 0.0
    Y = Y.drop(index=[agg], errors="ignore")
    return reindex_y_commodities_to_elec_schema(Y)


def export_electricity_disagg_weights_to_csv(
    weights: DisaggWeights,
    output_dir: pathlib.Path | None = None,
) -> pathlib.Path:
    """Write disaggregation weights CSV for inspection."""
    out_dir = output_dir or _WEIGHTS_EXPORT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "electricity_disagg_weights.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        weights_to_csv(weights, handle)
    w = build_electricity_disagg_go_weights()
    w.to_csv(out_dir / "electricity_disagg_go_weights.csv", header=["weight"])
    return path


def disaggregate_electricity_make_use_va(
    V: pd.DataFrame,
    Udom: pd.DataFrame,
    Uimp: pd.DataFrame,
    VA: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Run PR3 steps 1–4 on Make/Use/VA (post-reallocation inputs)."""
    w = build_electricity_disagg_go_weights()
    weights = build_electricity_disagg_weights(w)
    export_electricity_disagg_weights_to_csv(weights)

    _enforce_go_identity_precondition(V, Udom, Uimp, VA)
    x_agg = float(compute_x(V=V)[ELECTRICITY_AGGREGATE])

    V = disaggregate_make_intersection(V, weights)
    Udom, Uimp = disaggregate_use_intersection(Udom, Uimp, w)
    Udom, Uimp, VA = disaggregate_use_industry_columns(x_agg, Udom, Uimp, VA, w)
    Udom, Uimp = disaggregate_use_commodity_rows(Udom, Uimp, w)

    V = reindex_v_to_elec_schema(V)
    Udom = reindex_u_to_elec_schema(Udom)
    Uimp = reindex_u_to_elec_schema(Uimp)
    VA = reindex_va_to_elec_schema(VA)

    if ELECTRICITY_AGGREGATE in V.index or ELECTRICITY_AGGREGATE in V.columns:
        raise AssertionError("221100 remains in V after electricity disaggregation")
    for frame, label in ((Udom, "Udom"), (Uimp, "Uimp")):
        if ELECTRICITY_AGGREGATE in frame.index or ELECTRICITY_AGGREGATE in frame.columns:
            raise AssertionError(f"221100 remains in {label} after disaggregation")

    return V, Udom, Uimp, VA


def split_electricity_e_for_disaggregated_b(E: pd.DataFrame) -> pd.DataFrame:
    """Fallback: route aggregate 221100 emissions to 221110/221121 by gas row."""
    if ELECTRICITY_AGGREGATE not in E.columns:
        return E.reindex(columns=CORNERSTONE_INDUSTRIES_ELEC, fill_value=0.0)
    col = E[ELECTRICITY_AGGREGATE]
    out = E.drop(columns=[ELECTRICITY_AGGREGATE])
    out["221110"] = 0.0
    out["221121"] = 0.0
    out["221122"] = 0.0
    for gas in out.index:
        if gas == "SF6":
            out.loc[gas, "221121"] = float(col[gas])
        else:
            out.loc[gas, "221110"] = float(col[gas])
    return out.reindex(columns=CORNERSTONE_INDUSTRIES_ELEC, fill_value=0.0)


def distribute_electricity_aggregate_x_using_v_row_shares(
    x_cs: pd.Series[float],
    V: pd.DataFrame,
) -> pd.Series[float]:
    """Split aggregate 221100 x across 221110/221121/221122 using V row shares."""
    agg = ELECTRICITY_AGGREGATE
    if agg not in x_cs.index:
        return x_cs.reindex(CORNERSTONE_INDUSTRIES_ELEC)
    x = x_cs.copy()
    parent_go = float(x.loc[agg])
    x_v = compute_x(V=V)
    present = [c for c in ELECTRICITY_DISAGG_SECTORS if c in x_v.index]
    xv_w = x_v.reindex(present).astype(float)
    total_v = float(xv_w.sum())
    if total_v <= 0:
        return x.reindex(CORNERSTONE_INDUSTRIES_ELEC)
    shares = xv_w / total_v
    for code in present:
        x.loc[code] = parent_go * float(shares.loc[code])
    x = x.drop(agg)
    return x.reindex(CORNERSTONE_INDUSTRIES_ELEC)
