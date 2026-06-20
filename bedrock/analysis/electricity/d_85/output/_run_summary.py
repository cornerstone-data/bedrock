"""Run all d_85 decision reports and write analysis_summary.json."""

from __future__ import annotations

import json
import warnings
from pathlib import Path

import pandas as pd

from bedrock.analysis.electricity.d_85.balance_metrics import (
    aggregate_221100_row_by_end_use,
    compute_balance_metrics,
    va_summary_by_scenario,
)
from bedrock.analysis.electricity.d_85.decision3_table83_analysis import (
    build_report as build_d3_report,
)
from bedrock.analysis.electricity.d_85.decision5_table24_analysis import (
    build_report as build_d5_report,
)
from bedrock.analysis.electricity.d_85.decision7_ugo305_scaling_analysis import (
    build_report as build_d7_report,
)
from bedrock.analysis.electricity.d_85.diagnostics_comparison import (
    diff_ef_vectors,
    report_metadata,
)
from bedrock.analysis.electricity.d_85.disagg_scenarios import (
    run_decision3_scenarios,
    run_decision5_scenarios,
    run_scenario,
)
from bedrock.analysis.electricity.d_85.disagg_weights import (
    table83_go_weights,
    ugo305_go_weights,
)
from bedrock.analysis.electricity.d_85.end_use_mapping import build_end_use_map
from bedrock.analysis.electricity.d_85.scaling_scenarios import (
    compare_q_trajectories,
    ratio_table,
    run_d7_scenario,
)
from bedrock.analysis.electricity.d_85.scenario_types import DisaggScenarioResult
from bedrock.analysis.electricity.disaggregation_matrices import (
    assert_disaggregation_export_config,
    derive_post_reallocation_checkpoint,
)
from bedrock.utils.config.usa_config import get_usa_config, set_global_usa_config

OUT_DIR = Path(__file__).resolve().parent
SUMMARY_PATH = OUT_DIR / 'analysis_summary.json'


def _records(df: pd.DataFrame) -> list[dict[str, object]]:
    return json.loads(df.to_json(orient='records'))


def _ef_summary(
    label: str,
    base: DisaggScenarioResult,
    scen: DisaggScenarioResult | None,
    *,
    overrides: dict[str, float] | None = None,
) -> list[dict[str, object]]:
    if scen is not None and scen.metrics_only:
        return [{'scenario': label, 'note': 'metrics_only — EF skipped'}]
    diff = diff_ef_vectors(base, scen or base, electricity_ratio_overrides=overrides)
    sig = diff.loc[diff['significant']].copy()
    if sig.empty:
        return [{'scenario': label, 'note': 'no significant-sector EF change'}]
    sig['pct_change'] = sig['pct_change'].round(4)
    rows = _records(sig.head(15))
    for row in rows:
        row['scenario'] = label
    return rows


def main() -> None:
    warnings.filterwarnings('ignore')
    set_global_usa_config(
        '2025_usa_cornerstone_full_model_electricity_disaggregation.yaml'
    )
    assert_disaggregation_export_config()
    cfg = get_usa_config()

    print('Running Decision 3 report...')
    d3_path = build_d3_report()
    d3 = run_decision3_scenarios()

    print('Running Decision 5 report...')
    d5_path = build_d5_report()
    d5 = run_decision5_scenarios()

    print('Running Decision 7 report...')
    d7_path = build_d7_report()
    baseline = run_scenario('baseline')

    _, udom, uimp, _, y = derive_post_reallocation_checkpoint()
    eu = aggregate_221100_row_by_end_use(udom, uimp, y, build_end_use_map())
    eu_total = float(eu.sum())

    w_ugo = ugo305_go_weights()
    w_83 = table83_go_weights()

    ef_rows: list[dict[str, object]] = []
    for label, base, scen in (
        ('d8_mixed', d3['baseline'], d3['d8_mixed']),
        ('d8_offdiag', d3['baseline'], d3['d8_offdiag']),
        ('p24_2017', d5['baseline'], d5['p24_2017']),
        ('p24_target', d5['baseline'], d5['p24_target']),
    ):
        ef_rows.extend(_ef_summary(label, base, scen))
    for variant in ('d7_pure', 'd7_anchored'):
        _, overrides = run_d7_scenario(variant)
        ef_rows.extend(_ef_summary(variant, baseline, baseline, overrides=overrides))

    summary: dict[str, object] = {
        'config': {
            'model_base_year': cfg.model_base_year,
            'usa_io_data_year': cfg.usa_io_data_year,
            'usa_ghg_data_year': cfg.usa_ghg_data_year,
        },
        'reports': {
            'decision3': str(d3_path),
            'decision5': str(d5_path),
            'decision7': str(d7_path),
        },
        'metadata': report_metadata(),
        'decision3_weights': [
            {
                'source': 'UGO305-A 2017',
                'w_221110': float(w_ugo['221110']),
                'w_221121': float(w_ugo['221121']),
                'w_221122': float(w_ugo['221122']),
            },
            {
                'source': 'EPA Table 8.3 2017',
                'w_221110': float(w_83['221110']),
                'w_221121': float(w_83['221121']),
                'w_221122': float(w_83['221122']),
            },
        ],
        'decision3': {
            'scenario_flags': {k: v.metrics_only for k, v in d3.items()},
            'balance': _records(
                pd.concat([compute_balance_metrics(s) for s in d3.values()])
            ),
            'va': _records(pd.concat([va_summary_by_scenario(s) for s in d3.values()])),
        },
        'decision5': {
            'scenario_flags': {k: v.metrics_only for k, v in d5.items()},
            'balance': _records(
                pd.concat([compute_balance_metrics(s) for s in d5.values()])
            ),
            'va': _records(pd.concat([va_summary_by_scenario(s) for s in d5.values()])),
            'end_use_pct': {
                k: round(float(v) / eu_total * 100, 2) for k, v in eu.items()
            },
        },
        'decision7': {
            'ratios': _records(ratio_table()),
            'q_trajectories': _records(compare_q_trajectories()),
        },
        'ef_diagnostics': ef_rows,
    }

    SUMMARY_PATH.write_text(json.dumps(summary, indent=2), encoding='utf-8')
    print(f'Wrote {SUMMARY_PATH}')


if __name__ == '__main__':
    main()
