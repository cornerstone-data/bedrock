"""Build Excel tables and figures for monetary_disagg_report.md."""

from __future__ import annotations

import warnings
from pathlib import Path

import pandas as pd

from bedrock.analysis.electricity.d_85.balance_metrics import compute_balance_metrics
from bedrock.analysis.electricity.d_85.disagg_scenarios import ScenarioId, run_scenario
from bedrock.analysis.electricity.disaggregation_matrices import (
    assert_disaggregation_export_config,
    write_electricity_disaggregation_intermediate_outputs,
)
from bedrock.analysis.electricity.monetary_disagg.figure_data import (
    balance_summary,
    weight_summary,
)
from bedrock.analysis.electricity.monetary_disagg.figures import build_report_figures
from bedrock.utils.config.usa_config import reset_usa_config, set_global_usa_config

OUT = Path(__file__).resolve().parent / 'output'


def main() -> None:
    warnings.filterwarnings('ignore')
    reset_usa_config(should_reset_env_var=True)
    set_global_usa_config(
        'test_usa_config_waste_disagg_electricity_disaggregation.yaml'
    )
    assert_disaggregation_export_config()

    OUT.mkdir(parents=True, exist_ok=True)

    pipeline_xlsx = write_electricity_disaggregation_intermediate_outputs(
        output_dir=OUT.parent
    )
    figure_paths = build_report_figures(output_dir=OUT)

    scenarios: tuple[ScenarioId, ...] = (
        'baseline',
        't8.3_purchased_power_diag',
        't8.3_purchased_power_diag_compensated',
    )
    clearing = []
    for sid in scenarios:
        r = run_scenario(sid)
        bm = compute_balance_metrics(r)
        bm.insert(0, 'scenario_id', sid)
        clearing.append(bm)

    report_xlsx = OUT / 'monetary_disagg_balance_tables.xlsx'
    with pd.ExcelWriter(report_xlsx, engine='openpyxl') as writer:
        pd.read_excel(pipeline_xlsx, sheet_name='electricity_balance').to_excel(
            writer, sheet_name='pipeline_stage_balance', index=False
        )
        balance_summary().to_excel(writer, sheet_name='aggregate_balance', index=False)
        weight_summary().to_excel(writer, sheet_name='production_weights')
        pd.concat(clearing, ignore_index=True).to_excel(
            writer, sheet_name='scenario_market_clearing', index=False
        )

    print(f'Wrote {report_xlsx}')
    for name, path in figure_paths.items():
        print(f'Wrote {name}: {path}')
    print(f'Pipeline workbook: {pipeline_xlsx}')


if __name__ == '__main__':
    main()
