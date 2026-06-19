"""Balance and VA metrics for d_85 scenario comparison."""

from __future__ import annotations

import pandas as pd

from bedrock.analysis.electricity.d_85.scenario_types import DisaggScenarioResult
from bedrock.transform.eeio.electricity_disaggregation import (
    ELECTRICITY_AGGREGATE,
    _frame_cell_float,
)
from bedrock.utils.schemas.cornerstone_schemas import ELECTRICITY_DISAGG_SECTORS
from bedrock.utils.taxonomy.cornerstone.final_demand import FINAL_DEMANDS

ELEC = list(ELECTRICITY_DISAGG_SECTORS)


def compute_balance_metrics(scenario: DisaggScenarioResult) -> pd.DataFrame:
    """Return per-child balance metrics as a long-form DataFrame."""
    rows: list[dict[str, object]] = []
    for code in ELEC:
        q_k = float(scenario.q.get(code, 0.0))
        x_k = float(scenario.x.get(code, 0.0))
        u_sum = float(scenario.Udom.loc[code].sum()) + float(
            scenario.Uimp.loc[code].sum()
        )
        y_sum = float(scenario.Y.loc[code].sum()) if code in scenario.Y.index else 0.0
        market_clear = u_sum + y_sum - q_k
        qx_rel = abs(q_k - x_k) / abs(x_k) if x_k else 0.0
        va_total = (
            float(scenario.VA[code].sum()) if code in scenario.VA.columns else 0.0
        )
        neg_va = (
            bool((scenario.VA[code] < 0).any())
            if code in scenario.VA.columns
            else False
        )
        rows.append(
            {
                'scenario': scenario.name,
                'sector': code,
                'q': q_k,
                'x': x_k,
                'q_minus_x': q_k - x_k,
                'qx_relative_error': qx_rel,
                'use_row_sum': u_sum,
                'y_row_sum': y_sum,
                'market_clearing_gap': market_clear,
                'va_total': va_total,
                'has_negative_va': neg_va,
            }
        )
    return pd.DataFrame(rows)


def va_summary_by_scenario(scenario: DisaggScenarioResult) -> pd.DataFrame:
    """Per-child VA totals and negative-VA row counts."""
    records: list[dict[str, object]] = []
    for code in ELEC:
        if code not in scenario.VA.columns:
            continue
        col = scenario.VA[code].astype(float)
        neg_rows = col[col < 0]
        records.append(
            {
                'scenario': scenario.name,
                'sector': code,
                'va_total': float(col.sum()),
                'negative_va_count': len(neg_rows),
                'negative_va_min': float(neg_rows.min()) if len(neg_rows) else 0.0,
            }
        )
    return pd.DataFrame(records)


def aggregate_221100_row_by_end_use(
    udom: pd.DataFrame,
    uimp: pd.DataFrame,
    y: pd.DataFrame,
    end_use_map: dict[str, str],
) -> pd.Series[float]:
    """Sum aggregate 221100 electricity purchases by EPA end-use class."""
    agg = ELECTRICITY_AGGREGATE
    totals: dict[str, float] = {}
    for col in udom.columns:
        if col in ELEC or col == agg:
            continue
        eu = end_use_map.get(str(col), 'Commercial')
        val = _frame_cell_float(udom, agg, str(col)) + _frame_cell_float(
            uimp, agg, str(col)
        )
        totals[eu] = totals.get(eu, 0.0) + val
    for fd in FINAL_DEMANDS:
        if fd in y.columns and agg in y.index:
            eu = end_use_map.get(str(fd), 'Commercial')
            val = _frame_cell_float(y, agg, str(fd))
            totals[eu] = totals.get(eu, 0.0) + val
    return pd.Series(totals, dtype=float)


def electricity_va_breakdown(scenario: DisaggScenarioResult) -> pd.DataFrame:
    """VA row breakdown for electricity child columns."""
    frames: list[pd.DataFrame] = []
    for code in ELEC:
        if code not in scenario.VA.columns:
            continue
        col = scenario.VA[code].astype(float)
        df = col.rename('value').reset_index()
        df.columns = ['va_row', 'value']
        df['sector'] = code
        df['scenario'] = scenario.name
        frames.append(df)
    if not frames:
        return pd.DataFrame(columns=['scenario', 'sector', 'va_row', 'value'])
    return pd.concat(frames, ignore_index=True)
