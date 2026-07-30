"""Data helpers for methods #85 scenario figures (analysis-only)."""

from __future__ import annotations

import pandas as pd

from bedrock.analysis.electricity.d_85.balance_metrics import compute_balance_metrics
from bedrock.analysis.electricity.d_85.disagg_weights import (
    build_ugo_col_table83_row_intersection_matrix,
    table83_go_weights,
    table83_purchased_power_weights,
    ugo305_go_weights,
)
from bedrock.analysis.electricity.d_85.scenario_types import DisaggScenarioResult
from bedrock.analysis.electricity.disaggregation_matrices import (
    derive_post_reallocation_checkpoint,
)
from bedrock.transform.eeio.electricity_disaggregation import (
    ELECTRICITY_AGGREGATE,
    _frame_cell_float,
)
from bedrock.utils.schemas.cornerstone_schemas import ELECTRICITY_DISAGG_SECTORS

ELEC_CODES: list[str] = list(ELECTRICITY_DISAGG_SECTORS)

SECTOR_SHORT: dict[str, str] = {
    '221110': 'Generation',
    '221121': 'Transmission',
    '221122': 'Distribution',
}

FIGURE_C_PANELS: tuple[tuple[str, str], ...] = (
    ('baseline', 'Baseline — UGO305 diagonal'),
    ('t8.3_production_diag', 'Production + T/D diagonal'),
    ('t8.3_production_offdiag', 'Production hybrid off-diagonal'),
    ('t8.3_purchased_power_diag', 'Purchased Power + T/D diagonal'),
)

FIGURE_D_SCENARIOS: tuple[tuple[str, str, str], ...] = (
    ('baseline', 'baseline', 'Reference'),
    ('t8.3_production_diag', 'decision3', 't8.3 prod diag'),
    ('t8.3_production_offdiag', 'decision3', 't8.3 prod offdiag'),
    ('t8.3_purchased_power_diag', 'decision3', 't8.3 PP diag'),
    ('t8.3_purchased_power_offdiag', 'decision3', 't8.3 PP offdiag'),
    ('p24_2017', 'decision5', 'p24 2017'),
    ('p24_target', 'decision5', 'p24 target'),
)


def intersection_total_usd() -> float:
    """Aggregate U[221100, 221100] before step-2 split (domestic Use)."""
    _, udom, _, _, _ = derive_post_reallocation_checkpoint()
    return _frame_cell_float(udom, ELECTRICITY_AGGREGATE, ELECTRICITY_AGGREGATE)


def build_diagonal_intersection_matrix(
    w: pd.Series[float],
    total: float,
) -> pd.DataFrame:
    """3×3 diagonal Use-intersection block from normalized weight shares."""
    matrix = pd.DataFrame(0.0, index=ELEC_CODES, columns=ELEC_CODES, dtype=float)
    t = float(total)
    for code in ELEC_CODES:
        matrix.at[code, code] = float(w[code]) * t
    return matrix


def step2_intersection_matrix(
    scenario_id: str, *, total: float | None = None
) -> pd.DataFrame:
    """Reconstruct the step-2 Use-intersection 3×3 block for figure C."""
    t = float(total) if total is not None else intersection_total_usd()
    w_ugo = ugo305_go_weights()

    if scenario_id == 'baseline':
        return build_diagonal_intersection_matrix(w_ugo, t)

    if scenario_id == 't8.3_production_diag':
        return build_diagonal_intersection_matrix(table83_go_weights(), t)

    if scenario_id == 't8.3_purchased_power_diag':
        return build_diagonal_intersection_matrix(table83_purchased_power_weights(), t)

    if scenario_id == 't8.3_production_offdiag':
        return build_ugo_col_table83_row_intersection_matrix(
            w_ugo, table83_go_weights(), t
        )

    if scenario_id == 't8.3_purchased_power_offdiag':
        return build_ugo_col_table83_row_intersection_matrix(
            w_ugo, table83_purchased_power_weights(), t
        )

    raise ValueError(f'No step-2 intersection matrix for scenario {scenario_id!r}')


def market_clearing_gaps_table(
    d3: dict[str, DisaggScenarioResult],
    d5: dict[str, DisaggScenarioResult],
) -> pd.DataFrame:
    """Long-form market-clearing gaps ($) for figure D."""
    rows: list[dict[str, object]] = []
    seen: set[str] = set()
    for scenario_id, decision, label in FIGURE_D_SCENARIOS:
        if scenario_id in seen:
            continue
        seen.add(scenario_id)
        pool = d3 if decision == 'decision3' else d5
        scenario = pool[scenario_id]
        metrics = compute_balance_metrics(scenario)
        for _, row in metrics.iterrows():
            rows.append(
                {
                    'scenario_id': scenario_id,
                    'decision': decision,
                    'label': label,
                    'sector': str(row['sector']),
                    'sector_short': SECTOR_SHORT[str(row['sector'])],
                    'market_clearing_gap': float(row['market_clearing_gap']),
                    'market_clearing_gap_b': float(row['market_clearing_gap']) / 1e9,
                    'metrics_only': scenario.metrics_only,
                }
            )
    return pd.DataFrame(rows)
