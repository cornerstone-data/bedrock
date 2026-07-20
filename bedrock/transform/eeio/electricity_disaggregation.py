"""221100 electricity co-production reallocation and sector disaggregation."""

from __future__ import annotations

import functools
import logging
import pathlib
import warnings
from dataclasses import dataclass
from typing import Mapping, cast

import numpy as np
import numpy.typing as npt
import pandas as pd

from bedrock.extract.disaggregation.disagg_weights import DisaggWeights, weights_to_csv
from bedrock.extract.iot.gdp import SECTOR_NAME_COL, load_go_detail
from bedrock.transform.eeio.derived_2017 import derive_summary_q_usa
from bedrock.transform.eeio.waste_disaggregation import (
    apply_waste_disagg_to_V,
)
from bedrock.utils.math.formulas import compute_q, compute_x
from bedrock.utils.schemas.cornerstone_schemas import (
    CORNERSTONE_COMMODITIES_ELEC,
    CORNERSTONE_INDUSTRIES_ELEC,
    ELECTRICITY_DISAGG_SECTORS,
)
from bedrock.utils.taxonomy.bea.matrix_mappings import USA_SUMMARY_MUT_YEARS
from bedrock.utils.taxonomy.cornerstone.value_added import VALUE_ADDEDS

logger = logging.getLogger(__name__)

ELECTRICITY_AGGREGATE = '221100'
BALANCE_TOLERANCE = 1e6
DISAGG_BALANCE_ATOL = 1.0
IO_ACCOUNT_YEAR = 2017
EGRID_FBS_METHOD_NAME = 'GHG_national_Cornerstone_2023_egrid'

TABLE_8_3_DESCRIPTION = (
    'Table 8.3 Revenue and expense statistics for major U.S. '
    'investor-owned electric utilities'
)
TABLE_8_3_PRODUCER = 'Investor-owned electric utilities'
TABLE_8_3_PURCHASED_POWER_FLOW = 'expenses: Purchased Power'
TABLE_8_3_TRANSMISSION_FLOW = 'expenses: Transmission'
TABLE_8_3_DISTRIBUTION_FLOW = 'expenses: Distribution'

GENERATION_GO_SECTOR_NAMES: tuple[str, ...] = (
    'Hydroelectric   power generation',
    'Fossil fuel electric power generation',
    'Nuclear electric power generation',
    'Solar electric power generation',
    'Wind electric power generation',
    'Geothermal electric power generation',
    'Biomass electric power generation',
    'Other electric power generation',
)
TRANSMISSION_GO_SECTOR_NAME = 'Electric bulk power transmission and control'
DISTRIBUTION_GO_SECTOR_NAME = 'Electric power distribution'
ALL_ELECTRICITY_GO_SECTOR_NAMES: tuple[str, ...] = (
    *GENERATION_GO_SECTOR_NAMES,
    TRANSMISSION_GO_SECTOR_NAME,
    DISTRIBUTION_GO_SECTOR_NAME,
)

GENERATION_FUEL_COMMODITIES: frozenset[str] = frozenset(
    {'212100', '211000', '324110', '424700', '221200'}
)

_WEIGHTS_EXPORT_DIR = (
    pathlib.Path(__file__).resolve().parents[2]
    / 'extract'
    / 'disaggregation'
    / 'electricity_disagg_inputs'
)


def _float_ndarray(values: npt.ArrayLike) -> npt.NDArray[np.float64]:
    return np.asarray(values, dtype=np.float64)


def _frame_cell_float(frame: pd.DataFrame, row: str, col: str) -> float:
    return cast(float, frame.at[row, col])


def _loc_cols_sum(frame: pd.DataFrame, row: str, cols: list[str]) -> float:
    return float(frame[cols].loc[row].sum())


def _loc_rows_col_sum(frame: pd.DataFrame, rows: list[str], col: str) -> float:
    return float(frame[col].loc[rows].sum())


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
        err_msg=f'{label} row totals changed',
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
        raise ValueError(f'Cannot transfer from industry {s!r}: Make row sum is zero')
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

    _assert_row_totals_unchanged(udom_before, Udom, label='Udom')
    _assert_row_totals_unchanged(uimp_before, Uimp, label='Uimp')
    _assert_row_totals_unchanged(va_before, VA, label='VA')

    if (V < -1e-6).any().any():
        raise AssertionError('Make has negative values after transfer')

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
            f'221100 co-production off-diagonals remain above {atol}: '
            f'row_max={float(row_off.abs().max())}, '
            f'col_max={float(col_off.abs().max())}'
        )


# ---------------------------------------------------------------------------
# PR3 — 221100 → 221110 / 221121 / 221122 monetary disaggregation
# ---------------------------------------------------------------------------


def _normalize_sector_name(name: str) -> str:
    return ' '.join(str(name).split())


def _assert_go_sector_names_present(go: pd.DataFrame) -> None:
    available = {_normalize_sector_name(n) for n in go[SECTOR_NAME_COL]}
    expected = {_normalize_sector_name(n) for n in ALL_ELECTRICITY_GO_SECTOR_NAMES}
    missing = expected - available
    if missing:
        raise ValueError(
            'UGO305-A missing expected electricity gross-output sector names: '
            f'{sorted(missing)}'
        )


def _resolve_go_year_column(go: pd.DataFrame, year: int) -> str | int:
    if year in go.columns:
        return year
    year_str = str(year)
    if year_str in go.columns:
        return year_str
    available = sorted(c for c in go.columns if c not in (SECTOR_NAME_COL, 'Line'))
    raise ValueError(
        f'UGO305-A missing IO account year column {year}. '
        f'Available year columns: {available}'
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
        raise ValueError('Electricity gross-output total is non-positive')

    return pd.Series(
        {
            '221110': gen_total / total,
            '221121': trans / total,
            '221122': dist / total,
        },
        dtype=float,
    )


def _table83_purchased_power_expenses(
    year: int,
    *,
    fba: pd.DataFrame | None = None,
) -> dict[str, float]:
    """Return Purchased Power + T/D operating expenses from EIA Table 8.3."""
    if fba is None:
        from bedrock.extract.flowbyactivity import getFlowByActivity  # noqa: PLC0415

        fba = getFlowByActivity('EIA_ElectricPowerAnnual', year)
    mask = (
        (fba['Year'] == year)
        & (fba['Description'].str.startswith(TABLE_8_3_DESCRIPTION, na=False))
        & (fba['ActivityProducedBy'] == TABLE_8_3_PRODUCER)
    )
    subset = fba.loc[mask]
    flownames = {
        'PurchasedPower': TABLE_8_3_PURCHASED_POWER_FLOW,
        'Transmission': TABLE_8_3_TRANSMISSION_FLOW,
        'Distribution': TABLE_8_3_DISTRIBUTION_FLOW,
    }
    out: dict[str, float] = {}
    for label, flow_name in flownames.items():
        rows = subset.loc[subset['FlowName'] == flow_name, 'FlowAmount']
        if rows.empty:
            raise ValueError(
                f'Table 8.3 missing FlowName {flow_name!r} for year {year}'
            )
        out[label] = float(rows.iloc[0])
    return out


def _weights_from_table83_expenses(expenses: dict[str, float]) -> pd.Series[float]:
    keys = list(expenses.keys())
    if len(keys) != 3:
        raise ValueError(f'expected 3 expense buckets, got {keys!r}')
    gen_key, trans_key, dist_key = keys
    total = sum(expenses.values())
    if total <= 0:
        raise ValueError('Table 8.3 expense total is non-positive')
    return pd.Series(
        {
            '221110': expenses[gen_key] / total,
            '221121': expenses[trans_key] / total,
            '221122': expenses[dist_key] / total,
        },
        dtype=float,
    )


@functools.cache
def build_electricity_disagg_use_intersection_weights() -> pd.Series[float]:
    """Return Table 8.3 Purchased Power + T/D shares for step 2 intersection."""
    expenses = _table83_purchased_power_expenses(IO_ACCOUNT_YEAR)
    return _weights_from_table83_expenses(expenses)


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
        index=['__default__'],
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
        source_name='BEA_UGO305_A_electricity_go',
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
            f'221100 gross-output identity failed before step 3: '
            f'compute_x(V)[221100]={x_make}, '
            f'Udom+Uimp+VA column total={c_total}, '
            f'residual={residual} (relative={rel:.4%})'
        )
    warnings.warn(
        f'221100 GO identity residual {residual:,.0f} ({rel:.4%} of Make GO); '
        'absorbing into aggregate VA column before disaggregation',
        stacklevel=2,
    )
    va_col = VA[agg].astype(float)
    va_total = float(va_col.sum())
    if va_total != 0.0:
        VA.loc[list(VALUE_ADDEDS), agg] = va_col + residual * (va_col / va_total)
    else:
        VA.loc['V00300', agg] = _frame_cell_float(VA, 'V00300', agg) + residual
    c_after = _column_total_use_plus_va(Udom, Uimp, VA, agg)
    np.testing.assert_allclose(
        x_make,
        c_after,
        rtol=1e-9,
        atol=DISAGG_BALANCE_ATOL,
        err_msg='221100 GO identity still fails after VA absorption',
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
        err_msg='Make industry total not preserved',
    )
    np.testing.assert_allclose(
        float(V_out.loc[new_codes].sum().sum()),
        orig_com_total,
        rtol=1e-9,
        atol=DISAGG_BALANCE_ATOL,
        err_msg='Make commodity total not preserved',
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
        orig = _frame_cell_float(U, agg, agg)
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
        val = _frame_cell_float(U, str(row), agg)
        if val == 0.0:
            for code in ELECTRICITY_DISAGG_SECTORS:
                U.at[row, code] = 0.0
            continue
        if row in GENERATION_FUEL_COMMODITIES:
            U.at[row, '221110'] = val
            U.at[row, '221121'] = 0.0
            U.at[row, '221122'] = 0.0
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
    va_rows = [str(va) for va in VALUE_ADDEDS]
    orig_va = VA[agg].copy()
    orig_row_totals: dict[str, float] = {}
    for com in Udom.index:
        if com in set(ELECTRICITY_DISAGG_SECTORS) | {agg} | set(va_rows):
            continue
        orig_row_totals[str(com)] = _frame_cell_float(
            Udom, str(com), agg
        ) + _frame_cell_float(Uimp, str(com), agg)

    Udom = _split_aggregate_column_by_rule(Udom, w=w, va_rows=va_rows)
    Uimp = _split_aggregate_column_by_rule(Uimp, w=w, va_rows=va_rows)

    VA = VA.copy()
    for code in ELECTRICITY_DISAGG_SECTORS:
        if code not in VA.columns:
            VA[code] = 0.0

    va_share = (
        orig_va / float(orig_va.sum())
        if float(orig_va.sum()) != 0
        else (pd.Series(1.0 / len(va_rows), index=va_rows))
    )

    for code in ELECTRICITY_DISAGG_SECTORS:
        x_s = float(w[code]) * x_agg
        inputs_s = float(Udom[code].sum()) + float(Uimp[code].sum())
        va_total_s = x_s - inputs_s
        if va_total_s < 0:
            warnings.warn(
                f'Negative VA total for electricity sub-industry {code}: {va_total_s}',
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
            err_msg=f'Column {code} does not balance to gross output',
        )

    for va_row in va_rows:
        orig_row_total = float(orig_va[va_row])
        new_total = _loc_cols_sum(VA, va_row, list(ELECTRICITY_DISAGG_SECTORS))
        np.testing.assert_allclose(
            new_total,
            orig_row_total,
            rtol=1e-9,
            atol=DISAGG_BALANCE_ATOL,
            err_msg=f'VA row {va_row} total not preserved',
        )

    for com, orig_val in orig_row_totals.items():
        new_val = _loc_cols_sum(
            Udom, com, list(ELECTRICITY_DISAGG_SECTORS)
        ) + _loc_cols_sum(Uimp, com, list(ELECTRICITY_DISAGG_SECTORS))
        np.testing.assert_allclose(
            new_val,
            orig_val,
            rtol=1e-9,
            atol=DISAGG_BALANCE_ATOL,
            err_msg=f'Use row {com} industry-column total not preserved',
        )

    Udom = Udom.drop(columns=[agg], errors='ignore')
    Uimp = Uimp.drop(columns=[agg], errors='ignore')
    VA = VA.drop(columns=[agg], errors='ignore')
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
        orig_col_totals[str(col)] = _frame_cell_float(
            Udom, agg, str(col)
        ) + _frame_cell_float(Uimp, agg, str(col))
    results: list[pd.DataFrame] = []
    for U in (Udom, Uimp):
        U = U.copy()
        for col in U.columns:
            if col in elec_set:
                continue
            orig = _frame_cell_float(U, agg, str(col))
            for code in ELECTRICITY_DISAGG_SECTORS:
                if code not in U.index:
                    U.loc[code] = 0.0
                U.at[code, col] = orig * float(w[code])
            U.at[agg, col] = 0.0
        results.append(U)
    Udom_out, Uimp_out = results
    for col, orig in orig_col_totals.items():
        new = _loc_rows_col_sum(
            Udom_out, list(ELECTRICITY_DISAGG_SECTORS), col
        ) + _loc_rows_col_sum(Uimp_out, list(ELECTRICITY_DISAGG_SECTORS), col)
        np.testing.assert_allclose(
            new,
            orig,
            rtol=1e-9,
            atol=DISAGG_BALANCE_ATOL,
            err_msg=f'Commodity row split failed for column {col}',
        )
    Udom_out = Udom_out.drop(index=[agg], errors='ignore')
    Uimp_out = Uimp_out.drop(index=[agg], errors='ignore')
    return Udom_out, Uimp_out


def _capture_intersection_total(Udom: pd.DataFrame, Uimp: pd.DataFrame) -> float:
    agg = ELECTRICITY_AGGREGATE
    return _frame_cell_float(Udom, agg, agg) + _frame_cell_float(Uimp, agg, agg)


def _capture_purchases_total(
    Udom: pd.DataFrame,
    Uimp: pd.DataFrame,
    Y: pd.DataFrame,
) -> float:
    """Non-intersection electricity purchases P on post-reallocation checkpoint."""
    agg = ELECTRICITY_AGGREGATE
    t = _capture_intersection_total(Udom, Uimp)
    row_sum = float(Udom.loc[agg].sum()) + float(Uimp.loc[agg].sum())
    y_row = float(Y.loc[agg].sum()) if agg in Y.index else 0.0
    return (row_sum - t) + y_row


def _compute_w_row(
    w_go: pd.Series[float],
    w_int: pd.Series[float],
    t: float,
    p: float,
) -> pd.Series[float]:
    """Compensating uniform row weights preserving UGO305 total allocation."""
    if p <= 0:
        raise ValueError(f'Non-intersection purchases P must be positive, got P={p}')
    w_row: dict[str, float] = {}
    for code in ELECTRICITY_DISAGG_SECTORS:
        i_k = float(w_int[code]) * t
        w_row[str(code)] = (float(w_go[code]) * (t + p) - i_k) / p
    result = pd.Series(w_row, dtype=float)
    if (result < 0).any():
        negative = result[result < 0]
        raise ValueError(
            f'Negative compensating w_row for sectors {negative.index.tolist()}: '
            f'{negative.to_dict()}'
        )
    return result


@functools.cache
def _derive_post_reallocation_checkpoint_for_disagg() -> (
    tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]
):
    """Post-reallocation V/U/VA before PR3 steps 1–4 (production path only)."""
    from bedrock.transform.eeio.cornerstone_disagg_pipeline import (  # noqa: PLC0415
        derive_cornerstone_U_after_waste,
        derive_cornerstone_V_after_waste,
        derive_cornerstone_VA_after_waste,
    )

    v = derive_cornerstone_V_after_waste()
    udom, uimp = derive_cornerstone_U_after_waste()
    va = derive_cornerstone_VA_after_waste()
    return reallocate_electricity_coproduction(v, udom, uimp, va)


def _derive_y_before_electricity_disagg_lazy() -> pd.DataFrame:
    """Lazy wrapper — Y after waste disagg, before electricity row split."""
    from bedrock.transform.eeio.cornerstone_disagg_pipeline import (  # noqa: PLC0415
        _derive_y_before_electricity_disagg,
    )

    return _derive_y_before_electricity_disagg()


@functools.cache
def get_electricity_commodity_row_weights() -> pd.Series[float]:
    """Cached compensating w_row for Y split and analysis merge gate."""
    v, udom, uimp, _va = _derive_post_reallocation_checkpoint_for_disagg()
    y = _derive_y_before_electricity_disagg_lazy()
    w_go = build_electricity_disagg_go_weights()
    w_int = build_electricity_disagg_use_intersection_weights()
    t = _capture_intersection_total(udom, uimp)
    p = _capture_purchases_total(udom, uimp, y)
    q_total = float(compute_q(V=v)[ELECTRICITY_AGGREGATE])
    atol = max(DISAGG_BALANCE_ATOL, abs(q_total) * 0.01)
    if abs((t + p) - q_total) > atol:
        raise ValueError(
            f'T+P={t + p} does not match compute_q(V)[221100]={q_total} (atol={atol})'
        )
    w_row = _compute_w_row(w_go, w_int, t, p)
    if abs(float(w_row.sum()) - 1.0) > 1e-9:
        raise ValueError(f'w_row must sum to 1, got {float(w_row.sum())}')
    return w_row


def disaggregate_electricity_commodity_row_in_y(
    Y: pd.DataFrame,
    w: pd.Series[float],
) -> pd.DataFrame:
    """Step 4 (Y) — split 221100 commodity row across FD columns."""
    agg = ELECTRICITY_AGGREGATE
    Y = Y.copy()
    for col in Y.columns:
        orig = _frame_cell_float(Y, agg, str(col))
        for code in ELECTRICITY_DISAGG_SECTORS:
            if code not in Y.index:
                Y.loc[code] = 0.0
            Y.at[code, col] = orig * float(w[code])
        Y.at[agg, col] = 0.0
    Y = Y.drop(index=[agg], errors='ignore')
    return reindex_y_commodities_to_elec_schema(Y)


def export_electricity_disagg_weights_to_csv(
    weights: DisaggWeights,
    output_dir: pathlib.Path | None = None,
    *,
    w_int: pd.Series[float] | None = None,
    w_row: pd.Series[float] | None = None,
) -> pathlib.Path:
    """Write disaggregation weights CSV for inspection."""
    out_dir = output_dir or _WEIGHTS_EXPORT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / 'electricity_disagg_weights.csv'
    with path.open('w', encoding='utf-8', newline='') as handle:
        weights_to_csv(weights, handle)
    w_go = build_electricity_disagg_go_weights()
    w_go.to_csv(out_dir / 'electricity_disagg_go_weights.csv', header=['weight'])
    w_int_series = (
        w_int
        if w_int is not None
        else build_electricity_disagg_use_intersection_weights()
    )
    w_int_series.to_csv(
        out_dir / 'electricity_disagg_use_intersection_weights.csv',
        header=['weight'],
    )
    if w_row is not None:
        w_row.to_csv(out_dir / 'electricity_disagg_row_weights.csv', header=['weight'])
    return path


def disaggregate_electricity_make_use_va(
    V: pd.DataFrame,
    Udom: pd.DataFrame,
    Uimp: pd.DataFrame,
    VA: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Run PR3 steps 1–4 on Make/Use/VA (post-reallocation inputs)."""
    w_go = build_electricity_disagg_go_weights()
    w_int = build_electricity_disagg_use_intersection_weights()
    t = _capture_intersection_total(Udom, Uimp)
    y = _derive_y_before_electricity_disagg_lazy()
    p = _capture_purchases_total(Udom, Uimp, y)
    weights = build_electricity_disagg_weights(w_go)
    export_electricity_disagg_weights_to_csv(weights, w_int=w_int)

    _enforce_go_identity_precondition(V, Udom, Uimp, VA)
    x_agg = float(compute_x(V=V)[ELECTRICITY_AGGREGATE])

    V = disaggregate_make_intersection(V, weights)
    Udom, Uimp = disaggregate_use_intersection(Udom, Uimp, w_int)
    Udom, Uimp, VA = disaggregate_use_industry_columns(x_agg, Udom, Uimp, VA, w_go)
    w_row = _compute_w_row(w_go, w_int, t, p)
    export_electricity_disagg_weights_to_csv(weights, w_int=w_int, w_row=w_row)
    Udom, Uimp = disaggregate_use_commodity_rows(Udom, Uimp, w_row)

    V = reindex_v_to_elec_schema(V)
    Udom = reindex_u_to_elec_schema(Udom)
    Uimp = reindex_u_to_elec_schema(Uimp)
    VA = reindex_va_to_elec_schema(VA)

    if ELECTRICITY_AGGREGATE in V.index or ELECTRICITY_AGGREGATE in V.columns:
        raise AssertionError('221100 remains in V after electricity disaggregation')
    for frame, label in ((Udom, 'Udom'), (Uimp, 'Uimp')):
        if (
            ELECTRICITY_AGGREGATE in frame.index
            or ELECTRICITY_AGGREGATE in frame.columns
        ):
            raise AssertionError(f'221100 remains in {label} after disaggregation')

    return V, Udom, Uimp, VA


# ---------------------------------------------------------------------------
# PR 3.1 Decision 7 — pure UGO305 per-child scaling correction
# ---------------------------------------------------------------------------


def _go_levels_for_year(year: int) -> pd.Series[float]:
    go = load_go_detail()
    _assert_go_sector_names_present(go)
    year_col = _resolve_go_year_column(go, year)
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
    return pd.Series(
        {'221110': gen_total, '221121': trans, '221122': dist},
        dtype=float,
    )


@functools.cache
def build_electricity_ugo305_scaling_ratios(
    original_year: int,
    target_year: int,
) -> pd.Series[float]:
    """UGO305 detail GO ratios original_year → target_year for G/T/D children."""
    go_base = _go_levels_for_year(original_year)
    go_tgt = _go_levels_for_year(target_year)
    ratios = go_tgt / go_base.replace(0, float('nan'))
    return ratios.fillna(1.0).reindex(ELECTRICITY_DISAGG_SECTORS)


def utilities_summary_ratio_22(original_year: int, target_year: int) -> float:
    """Summary Utilities sector-22 q ratio used as D7 base scaling factor."""
    orig = cast(USA_SUMMARY_MUT_YEARS, original_year)
    tgt = cast(USA_SUMMARY_MUT_YEARS, target_year)
    ratio = (derive_summary_q_usa(tgt) / derive_summary_q_usa(orig)).fillna(1.0)
    val = float(ratio.get('22', 1.0))
    return val if np.isfinite(val) else 1.0


def apply_electricity_d7_scaling_correction_to_A(
    a: pd.DataFrame,
    original_year: int,
    target_year: int,
) -> pd.DataFrame:
    """Rescale electricity child rows after summary-ratio A scaling (D7 pure)."""
    ratios = build_electricity_ugo305_scaling_ratios(original_year, target_year)
    base = utilities_summary_ratio_22(original_year, target_year)
    out = a.copy()
    for code in ELECTRICITY_DISAGG_SECTORS:
        if code not in out.index:
            continue
        factor = float(ratios[code]) / base if base else 1.0
        out.loc[code] = out.loc[code] * factor
    return out


def apply_electricity_d7_scaling_correction_to_q(
    q: pd.Series[float],
    original_year: int,
    target_year: int,
) -> pd.Series[float]:
    """Rescale electricity child q rows after summary-ratio q scaling (D7 pure)."""
    ratios = build_electricity_ugo305_scaling_ratios(original_year, target_year)
    base = utilities_summary_ratio_22(original_year, target_year)
    out = q.copy()
    for code in ELECTRICITY_DISAGG_SECTORS:
        if code not in out.index:
            continue
        factor = float(ratios[code]) / base if base else 1.0
        out.loc[code] = float(out.loc[code]) * factor
    return out


def split_electricity_e_for_disaggregated_b(E: pd.DataFrame) -> pd.DataFrame:
    """Fallback: route aggregate 221100 emissions to 221110/221121 by gas row."""
    if ELECTRICITY_AGGREGATE not in E.columns:
        return E.reindex(columns=CORNERSTONE_INDUSTRIES_ELEC, fill_value=0.0)
    col = E[ELECTRICITY_AGGREGATE]
    out = E.drop(columns=[ELECTRICITY_AGGREGATE])
    out['221110'] = 0.0
    out['221121'] = 0.0
    out['221122'] = 0.0
    for gas in out.index:
        if gas == 'SF6':
            out.loc[gas, '221121'] = float(col[gas])
        else:
            out.loc[gas, '221110'] = float(col[gas])
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


GENERATION_SECTOR = '221110'


def electricity_output_factor(q_usd_221110: float, mwh_221110: float) -> float:
    """Scalar c_col = MWh / $ for generation output/column conversion."""
    if not np.isfinite(q_usd_221110) or q_usd_221110 <= 0:
        raise ValueError(
            f'electricity_output_factor: non-positive q$_221110={q_usd_221110!r}'
        )
    if not np.isfinite(mwh_221110) or mwh_221110 <= 0:
        raise ValueError(
            f'electricity_output_factor: non-positive mwh_221110={mwh_221110!r}'
        )
    return float(mwh_221110 / q_usd_221110)


def _class_price(
    col: str,
    prices_by_class: Mapping[str, float],
    end_use_map: dict[str, str],
) -> float:
    if col not in end_use_map:
        raise ValueError(
            f'electricity_class_row_factors: column {col!r} absent from end_use_map'
        )
    eu = end_use_map[col]
    if eu not in prices_by_class:
        raise ValueError(
            f'electricity_class_row_factors: missing Table 2.4 price for class {eu!r}'
        )
    p = float(prices_by_class[eu])
    if p <= 0 or not np.isfinite(p):
        raise ValueError(
            f'electricity_class_row_factors: non-positive price for class {eu!r} (col={col!r})'
        )
    return p


def electricity_class_row_factors(
    adom_row_221110: pd.Series,
    scaled_q: pd.Series[float],
    y_row_221110: pd.Series[float],
    prices_by_class: Mapping[str, float],
    end_use_map: dict[str, str],
    mwh_221110: float,
) -> pd.Series[float]:
    """Per-column c_j = λ / p_j preserving domestic row MWh = mwh_221110."""
    if not np.isfinite(mwh_221110) or mwh_221110 <= 0:
        raise ValueError(
            f'electricity_class_row_factors: non-positive mwh_221110={mwh_221110!r}'
        )

    denom = 0.0
    for col in adom_row_221110.index:
        coef = float(adom_row_221110[col])
        if coef == 0.0:
            continue
        if col not in scaled_q.index:
            raise ValueError(
                f'electricity_class_row_factors: scaled_q missing column {col!r}'
            )
        p_j = _class_price(str(col), prices_by_class, end_use_map)
        flow_usd = coef * float(scaled_q[col])
        denom += flow_usd / p_j

    for col in y_row_221110.index:
        y_val = float(y_row_221110[col])
        if y_val == 0.0:
            continue
        p_f = _class_price(str(col), prices_by_class, end_use_map)
        denom += y_val / p_f

    if denom <= 0 or not np.isfinite(denom):
        raise ValueError(
            'electricity_class_row_factors: λ denominator non-positive or non-finite'
        )

    lam = float(mwh_221110 / denom)
    if lam <= 0 or not np.isfinite(lam):
        raise ValueError(
            f'electricity_class_row_factors: non-positive λ={lam!r} for sector 221110'
        )

    cols = adom_row_221110.index.union(y_row_221110.index)
    c_row = pd.Series(index=cols, dtype=float)
    for col in cols:
        p_j = _class_price(str(col), prices_by_class, end_use_map)
        c_row[col] = lam / p_j
    return c_row


def apply_electricity_unit_conversion_to_A(
    A: pd.DataFrame,
    *,
    c_col: float,
    c_row: pd.Series[float],
    generation_sector: str = GENERATION_SECTOR,
) -> pd.DataFrame:
    """Convert generation sales row/column in a single A block."""
    out = A.copy()
    gen = generation_sector
    for col in out.columns:
        if col == gen:
            c_j = float(c_row.get(col, c_col))
            out.loc[gen, col] = cast(float, out.loc[gen, col]) * c_j / c_col
        else:
            c_j = float(c_row[col])
            out.loc[gen, col] = cast(float, out.loc[gen, col]) * c_j
    for idx in out.index:
        if idx != gen:
            out.loc[idx, gen] = cast(float, out.loc[idx, gen]) / c_col
    return out


def apply_electricity_unit_conversion_to_q(
    q: pd.Series[float],
    c_col: float,
    generation_sector: str = GENERATION_SECTOR,
) -> pd.Series[float]:
    out = q.copy()
    out.loc[generation_sector] = float(out.loc[generation_sector]) * c_col
    return out


def apply_electricity_unit_conversion_to_B(
    B: pd.DataFrame,
    c_col: float,
    generation_sector: str = GENERATION_SECTOR,
) -> pd.DataFrame:
    out = B.copy()
    if generation_sector in out.columns:
        out[generation_sector] = out[generation_sector] / c_col
    return out
