"""Decision 3 report: Table 8.3 intersection exploration."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from bedrock.analysis.electricity.d_85.balance_metrics import (
    compute_balance_metrics,
    va_summary_by_scenario,
)
from bedrock.analysis.electricity.d_85.diagnostics_comparison import (
    diff_ef_vectors,
    report_metadata,
)
from bedrock.analysis.electricity.d_85.disagg_scenarios import run_decision3_scenarios
from bedrock.analysis.electricity.d_85.disagg_weights import (
    build_ugo_col_table83_row_intersection_matrix,
    table83_go_weights,
    table83_purchased_power_weights,
    ugo305_go_weights,
)
from bedrock.analysis.electricity.d_85.scenario_types import DisaggScenarioResult

_OUTPUT = Path(__file__).resolve().parent / 'output'
_REPORT = 'decision3_table83_report.xlsx'


def _weight_comparison_table() -> pd.DataFrame:
    w_ugo = ugo305_go_weights()
    w_prod = table83_go_weights()
    w_pp = table83_purchased_power_weights()
    rows = [
        {
            'source': 'UGO305-A 2017',
            'w_221110': float(w_ugo['221110']),
            'w_221121': float(w_ugo['221121']),
            'w_221122': float(w_ugo['221122']),
            'denominator': '10 BEA GO sectors',
        },
        {
            'source': 'EPA Table 8.3 2017 — Production + T/D',
            'w_221110': float(w_prod['221110']),
            'w_221121': float(w_prod['221121']),
            'w_221122': float(w_prod['221122']),
            'denominator': 'Production+Trans+Dist IOU expenses',
        },
        {
            'source': 'EPA Table 8.3 2017 — Purchased Power + T/D',
            'w_221110': float(w_pp['221110']),
            'w_221121': float(w_pp['221121']),
            'w_221122': float(w_pp['221122']),
            'denominator': 'PurchasedPower+Trans+Dist IOU expenses',
        },
    ]
    df = pd.DataFrame(rows)
    df['delta_vs_ugo_221110'] = df['w_221110'] - float(w_ugo['221110'])
    df['iou_coverage_note'] = (
        'Table 8.3 IOU-only expense shares; not national IOU+coop+public mix'
    )
    return df


def _worked_example_matrix() -> tuple[pd.DataFrame, pd.DataFrame]:
    w_ugo = ugo305_go_weights()
    w_83 = table83_go_weights()
    T = 1000.0
    matrix = build_ugo_col_table83_row_intersection_matrix(w_ugo, w_83, T)
    matrix.index.name = 'commodity'
    matrix.columns.name = 'industry'
    meta = pd.DataFrame(
        [
            {'parameter': 'T_sample', 'value': T},
            {'parameter': 'index_order', 'value': '221110,221121,221122'},
        ]
    )
    return matrix.reset_index(), meta


def build_report(scenarios: dict[str, DisaggScenarioResult] | None = None) -> Path:
    scenarios = scenarios or run_decision3_scenarios()
    _OUTPUT.mkdir(parents=True, exist_ok=True)
    dest = _OUTPUT / _REPORT

    weights = _weight_comparison_table()
    matrix, meta = _worked_example_matrix()
    balance_frames = [compute_balance_metrics(s) for s in scenarios.values()]
    balance = pd.concat(balance_frames, ignore_index=True)
    va_frames = [va_summary_by_scenario(s) for s in scenarios.values()]
    va = pd.concat(va_frames, ignore_index=True)

    baseline = scenarios['baseline']
    ef_frames = []
    for name, scen in scenarios.items():
        if name == 'baseline':
            continue
        if scen.metrics_only:
            ef_frames.append(
                pd.DataFrame(
                    [
                        {
                            'scenario': name,
                            'sector': '',
                            'D_baseline': float('nan'),
                            'D_scenario': float('nan'),
                            'delta': float('nan'),
                            'pct_change': float('nan'),
                            'significant': False,
                            'ef_skipped_reason': 'metrics_only',
                        }
                    ]
                )
            )
            continue
        ef_frames.append(diff_ef_vectors(baseline, scen).assign(scenario=name))
    ef = pd.concat(ef_frames, ignore_index=True) if ef_frames else pd.DataFrame()

    metadata = pd.DataFrame([report_metadata()])

    with pd.ExcelWriter(dest, engine='openpyxl') as writer:
        weights.to_excel(writer, sheet_name='A_weights', index=False)
        matrix.to_excel(writer, sheet_name='A_worked_matrix', index=False)
        meta.to_excel(writer, sheet_name='A_worked_meta', index=False)
        va.to_excel(writer, sheet_name='B_va_summary', index=False)
        balance.to_excel(writer, sheet_name='B_balance', index=False)
        ef.to_excel(writer, sheet_name='C_ef_diff', index=False)
        metadata.to_excel(writer, sheet_name='metadata', index=False)

    return dest
