"""Decision 7 report: UGO305 differentiated year scaling simulation."""

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
from bedrock.analysis.electricity.d_85.disagg_scenarios import run_scenario
from bedrock.analysis.electricity.d_85.scaling_scenarios import (
    compare_q_trajectories,
    ratio_table,
    run_d7_scenario,
)
from bedrock.utils.config.usa_config import get_usa_config

_OUTPUT = Path(__file__).resolve().parent / 'output'
_REPORT = 'decision7_ugo305_scaling_report.xlsx'


def build_report() -> Path:
    _OUTPUT.mkdir(parents=True, exist_ok=True)
    dest = _OUTPUT / _REPORT
    cfg = get_usa_config()

    ratios = ratio_table()
    q_traj = compare_q_trajectories()
    baseline = run_scenario('baseline')
    balance = compute_balance_metrics(baseline)
    va = va_summary_by_scenario(baseline)

    ef_rows = []
    for variant in ('d7_pure', 'd7_anchored'):
        _, overrides = run_d7_scenario(variant)
        diff = diff_ef_vectors(
            baseline, baseline, electricity_ratio_overrides=overrides
        ).assign(variant=variant, ef_method='summary_ratio_override')
        ef_rows.append(diff)
    ef = pd.concat(ef_rows, ignore_index=True) if ef_rows else pd.DataFrame()

    metadata = pd.DataFrame(
        [
            {
                **report_metadata(),
                'usa_io_data_year': cfg.usa_io_data_year,
                'model_base_year': cfg.model_base_year,
                'ratio_target_year': cfg.usa_io_data_year,
            }
        ]
    )

    with pd.ExcelWriter(dest, engine='openpyxl') as writer:
        ratios.to_excel(writer, sheet_name='ratio_k', index=False)
        q_traj.to_excel(writer, sheet_name='q_trajectories', index=False)
        balance.to_excel(writer, sheet_name='baseline_balance', index=False)
        va.to_excel(writer, sheet_name='baseline_va', index=False)
        ef.to_excel(writer, sheet_name='ef_diff', index=False)
        metadata.to_excel(writer, sheet_name='metadata', index=False)

    return dest
