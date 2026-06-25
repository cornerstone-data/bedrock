"""Custom-weight PR3 disaggregation scenarios for methods #85 analysis."""

from __future__ import annotations

import typing as ta

import pandas as pd

from bedrock.analysis.electricity.d_85.disagg_weights import (
    build_ugo_col_table83_row_intersection_matrix,
    table83_go_weights,
    table83_purchased_power_weights,
    ugo305_go_weights,
)
from bedrock.analysis.electricity.d_85.eia_inputs import table_2_4_prices_cents_kwh
from bedrock.analysis.electricity.d_85.end_use_mapping import (
    build_end_use_map,
    build_price_tilt_weights_by_column,
)
from bedrock.analysis.electricity.d_85.scenario_types import (
    DisaggScenarioResult,
    ScenarioWeights,
)
from bedrock.analysis.electricity.disaggregation_matrices import (
    derive_post_reallocation_checkpoint,
)
from bedrock.transform.eeio.electricity_disaggregation import (
    ELECTRICITY_AGGREGATE,
    _enforce_go_identity_precondition,
    _frame_cell_float,
    build_electricity_disagg_weights,
    disaggregate_electricity_commodity_row_in_y,
    disaggregate_electricity_make_use_va,
    disaggregate_make_intersection,
    disaggregate_use_industry_columns,
    disaggregate_use_intersection,
    reindex_u_to_elec_schema,
    reindex_v_to_elec_schema,
    reindex_va_to_elec_schema,
    reindex_y_commodities_to_elec_schema,
)
from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.math.formulas import compute_q, compute_x
from bedrock.utils.schemas.cornerstone_schemas import (
    ELECTRICITY_DISAGG_SECTORS,
    validate_cornerstone,
)
from bedrock.utils.taxonomy.cornerstone.final_demand import FINAL_DEMANDS

ELEC = list(ELECTRICITY_DISAGG_SECTORS)
AGG = ELECTRICITY_AGGREGATE

ScenarioId = ta.Literal[
    'baseline',
    't8.3_production_diag',
    't8.3_production_offdiag',
    't8.3_purchased_power_diag',
    't8.3_purchased_power_offdiag',
    'p24_2017',
    'p24_target',
]


def apply_use_intersection_custom(
    Udom: pd.DataFrame,
    Uimp: pd.DataFrame,
    matrix_3x3: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply absolute-dollar 3×3 Use intersection (row=commodity, col=industry)."""
    matrix = matrix_3x3.reindex(index=ELEC, columns=ELEC, fill_value=0.0)
    results: list[pd.DataFrame] = []
    for U in (Udom, Uimp):
        U = U.copy()
        for row in ELEC:
            if row not in U.index:
                U.loc[row] = 0.0
        for col in ELEC:
            if col not in U.columns:
                U[col] = 0.0
            for row in ELEC:
                U.at[row, col] = _frame_cell_float(matrix, row, col)
        if AGG in U.index and AGG in U.columns:
            U.at[AGG, AGG] = 0.0
        results.append(U)
    return results[0], results[1]


def _normalize_row_weights(w_by_col: pd.DataFrame, col: str) -> pd.Series[float]:
    s = w_by_col[col].reindex(ELEC).astype(float)
    total = float(s.sum())
    return s / total if total else s


def split_commodity_row_by_column(
    Udom: pd.DataFrame,
    Uimp: pd.DataFrame,
    w_by_col: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split 221100 commodity row with per-purchaser column weights."""
    elec_set = set(ELEC)
    results: list[pd.DataFrame] = []
    for U in (Udom, Uimp):
        U = U.copy()
        for col in U.columns:
            if col in elec_set:
                continue
            orig = _frame_cell_float(U, AGG, str(col))
            w_col = _normalize_row_weights(w_by_col, str(col))
            for code in ELEC:
                if code not in U.index:
                    U.loc[code] = 0.0
                U.at[code, col] = orig * float(w_col[code])
            U.at[AGG, col] = 0.0
        U = U.drop(index=[AGG], errors='ignore')
        results.append(U)
    return results[0], results[1]


def split_y_commodity_row_by_column(
    Y: pd.DataFrame,
    w_by_col: pd.DataFrame,
) -> pd.DataFrame:
    """Split 221100 commodity row on Y using per-FD-column weights."""
    Y = Y.copy()
    for col in Y.columns:
        orig = _frame_cell_float(Y, AGG, str(col))
        w_col = _normalize_row_weights(w_by_col, str(col))
        for code in ELEC:
            if code not in Y.index:
                Y.loc[code] = 0.0
            Y.at[code, col] = orig * float(w_col[code])
        Y.at[AGG, col] = 0.0
    Y = Y.drop(index=[AGG], errors='ignore')
    return reindex_y_commodities_to_elec_schema(Y)


def _assert_no_aggregate(scenario: DisaggScenarioResult) -> None:
    for frame, label in (
        (scenario.V, 'V'),
        (scenario.Udom, 'Udom'),
        (scenario.Uimp, 'Uimp'),
        (scenario.Y, 'Y'),
    ):
        if AGG in frame.index or AGG in frame.columns:
            raise AssertionError(f'{AGG} remains in {label} after disaggregation')


def _run_baseline() -> DisaggScenarioResult:
    V, Udom, Uimp, VA, Y = derive_post_reallocation_checkpoint()
    V, Udom, Uimp, VA = disaggregate_electricity_make_use_va(V, Udom, Uimp, VA)
    w = ugo305_go_weights()
    Y = disaggregate_electricity_commodity_row_in_y(Y, w)
    q = compute_q(V=V)
    x = compute_x(V=V)
    weights = ScenarioWeights(
        w_make_intersection=w,
        w_use_intersection=None,
        w_column_steps=w,
        w_row_uniform=w,
        w_row_by_column=None,
        intersection_3x3=None,
    )
    result = DisaggScenarioResult(
        name='baseline',
        weights=weights,
        V=V,
        Udom=Udom,
        Uimp=Uimp,
        VA=VA,
        Y=Y,
        q=q,
        x=x,
    )
    _assert_no_aggregate(result)
    return result


def _run_stepwise(
    scenario_id: ScenarioId,
    *,
    weights: ScenarioWeights,
) -> DisaggScenarioResult:
    V, Udom, Uimp, VA, Y = derive_post_reallocation_checkpoint()
    metrics_only = False

    try:
        _enforce_go_identity_precondition(V, Udom, Uimp, VA)
    except AssertionError:
        metrics_only = True

    x_agg = float(compute_x(V=V)[AGG])
    w_make = weights.w_make_intersection
    w_col = weights.w_column_steps
    w_use = (
        weights.w_use_intersection if weights.w_use_intersection is not None else w_make
    )

    w_obj = build_electricity_disagg_weights(w_make)
    V = disaggregate_make_intersection(V, w_obj)

    if weights.intersection_3x3 is not None:
        Udom, Uimp = apply_use_intersection_custom(Udom, Uimp, weights.intersection_3x3)
    else:
        Udom, Uimp = disaggregate_use_intersection(Udom, Uimp, w_use)

    try:
        Udom, Uimp, VA = disaggregate_use_industry_columns(x_agg, Udom, Uimp, VA, w_col)
    except AssertionError:
        metrics_only = True

    if not metrics_only:
        for code in ELEC:
            if code in VA.columns and (VA[code] < 0).any():
                metrics_only = True
                break

    if weights.w_row_by_column is not None:
        Udom, Uimp = split_commodity_row_by_column(Udom, Uimp, weights.w_row_by_column)
        Y = split_y_commodity_row_by_column(Y, weights.w_row_by_column)
    else:
        w_row = weights.w_row_uniform if weights.w_row_uniform is not None else w_col
        Udom, Uimp = _split_uniform_row(Udom, Uimp, w_row)
        Y = disaggregate_electricity_commodity_row_in_y(Y, w_row)

    V = reindex_v_to_elec_schema(V)
    Udom = reindex_u_to_elec_schema(Udom)
    Uimp = reindex_u_to_elec_schema(Uimp)
    VA = reindex_va_to_elec_schema(VA)
    Y = reindex_y_commodities_to_elec_schema(Y)

    validate_cornerstone(V, 'V')
    validate_cornerstone(Udom, 'U')
    validate_cornerstone(Uimp, 'U')

    if AGG in V.index or AGG in V.columns:
        metrics_only = True

    q = compute_q(V=V)
    x = compute_x(V=V)
    return DisaggScenarioResult(
        name=scenario_id,
        weights=weights,
        V=V,
        Udom=Udom,
        Uimp=Uimp,
        VA=VA,
        Y=Y,
        q=q,
        x=x,
        metrics_only=metrics_only,
    )


def _split_uniform_row(
    Udom: pd.DataFrame,
    Uimp: pd.DataFrame,
    w: pd.Series[float],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    from bedrock.transform.eeio.electricity_disaggregation import (  # noqa: PLC0415
        disaggregate_use_commodity_rows,
    )

    return disaggregate_use_commodity_rows(Udom, Uimp, w)


def _weights_t83_diag(w_83: pd.Series[float]) -> ScenarioWeights:
    w_ugo = ugo305_go_weights()
    return ScenarioWeights(
        w_make_intersection=w_ugo,
        w_use_intersection=w_83,
        w_column_steps=w_ugo,
        w_row_uniform=w_ugo,
        w_row_by_column=None,
        intersection_3x3=None,
    )


def _weights_t83_offdiag(w_83: pd.Series[float]) -> ScenarioWeights:
    w_ugo = ugo305_go_weights()
    _, Udom, _, _, _ = derive_post_reallocation_checkpoint()
    T = _frame_cell_float(Udom, AGG, AGG)
    matrix = build_ugo_col_table83_row_intersection_matrix(w_ugo, w_83, T)
    return ScenarioWeights(
        w_make_intersection=w_ugo,
        w_use_intersection=None,
        w_column_steps=w_ugo,
        w_row_uniform=w_ugo,
        w_row_by_column=None,
        intersection_3x3=matrix,
    )


def _weights_t83_production_diag() -> ScenarioWeights:
    return _weights_t83_diag(table83_go_weights())


def _weights_t83_production_offdiag() -> ScenarioWeights:
    return _weights_t83_offdiag(table83_go_weights())


def _weights_t83_purchased_power_diag() -> ScenarioWeights:
    return _weights_t83_diag(table83_purchased_power_weights())


def _weights_t83_purchased_power_offdiag() -> ScenarioWeights:
    return _weights_t83_offdiag(table83_purchased_power_weights())


def _weights_p24(price_year: int) -> ScenarioWeights:
    w_ugo = ugo305_go_weights()
    raw_prices = table_2_4_prices_cents_kwh(price_year)
    prices: dict[str, float] = {str(k): float(v) for k, v in raw_prices.items()}
    end_use_map = build_end_use_map()
    _, Udom, Uimp, _, Y = derive_post_reallocation_checkpoint()
    cols = [c for c in Udom.columns if c not in ELEC and c != AGG]
    fd_cols = [c for c in Y.columns if c in FINAL_DEMANDS]
    all_cols = list(dict.fromkeys(cols + fd_cols))
    w_by_col = build_price_tilt_weights_by_column(w_ugo, prices, end_use_map, all_cols)
    return ScenarioWeights(
        w_make_intersection=w_ugo,
        w_use_intersection=None,
        w_column_steps=w_ugo,
        w_row_uniform=None,
        w_row_by_column=w_by_col,
        intersection_3x3=None,
    )


def run_scenario(scenario_id: ScenarioId) -> DisaggScenarioResult:
    """Run one registered disaggregation scenario."""
    if scenario_id == 'baseline':
        return _run_baseline()
    if scenario_id == 't8.3_production_diag':
        return _run_stepwise(
            't8.3_production_diag', weights=_weights_t83_production_diag()
        )
    if scenario_id == 't8.3_production_offdiag':
        return _run_stepwise(
            't8.3_production_offdiag', weights=_weights_t83_production_offdiag()
        )
    if scenario_id == 't8.3_purchased_power_diag':
        return _run_stepwise(
            't8.3_purchased_power_diag', weights=_weights_t83_purchased_power_diag()
        )
    if scenario_id == 't8.3_purchased_power_offdiag':
        return _run_stepwise(
            't8.3_purchased_power_offdiag',
            weights=_weights_t83_purchased_power_offdiag(),
        )
    if scenario_id == 'p24_2017':
        return _run_stepwise('p24_2017', weights=_weights_p24(2017))
    if scenario_id == 'p24_target':
        cfg = get_usa_config()
        return _run_stepwise('p24_target', weights=_weights_p24(cfg.usa_ghg_data_year))
    raise ValueError(f'Unknown scenario: {scenario_id}')


def run_decision3_scenarios() -> dict[str, DisaggScenarioResult]:
    return {
        'baseline': run_scenario('baseline'),
        't8.3_production_diag': run_scenario('t8.3_production_diag'),
        't8.3_production_offdiag': run_scenario('t8.3_production_offdiag'),
        't8.3_purchased_power_diag': run_scenario('t8.3_purchased_power_diag'),
        't8.3_purchased_power_offdiag': run_scenario('t8.3_purchased_power_offdiag'),
    }


def run_decision5_scenarios() -> dict[str, DisaggScenarioResult]:
    return {
        'baseline': run_scenario('baseline'),
        'p24_2017': run_scenario('p24_2017'),
        'p24_target': run_scenario('p24_target'),
    }
