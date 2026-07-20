"""Decision 5 report: Table 2.4 price-differentiated row/Y splits."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from bedrock.analysis.electricity.d_85.balance_metrics import (
    aggregate_221100_row_by_end_use,
    compute_balance_metrics,
)
from bedrock.analysis.electricity.d_85.diagnostics_comparison import (
    diff_ef_vectors,
    report_metadata,
)
from bedrock.analysis.electricity.d_85.disagg_scenarios import run_decision5_scenarios
from bedrock.analysis.electricity.d_85.end_use_mapping import build_end_use_map
from bedrock.analysis.electricity.d_85.scenario_types import DisaggScenarioResult
from bedrock.analysis.electricity.disaggregation_matrices import (
    derive_post_reallocation_checkpoint,
)

_OUTPUT = Path(__file__).resolve().parent / 'output'
_REPORT = 'decision5_table24_report.xlsx'


def _end_use_coverage() -> pd.DataFrame:
    V, Udom, Uimp, VA, Y = derive_post_reallocation_checkpoint()
    _ = (V, VA)
    end_use_map = build_end_use_map()
    shares = aggregate_221100_row_by_end_use(Udom, Uimp, Y, end_use_map)
    total = float(shares.sum())
    return (
        shares.rename('dollars')
        .reset_index()
        .rename(columns={'index': 'epa_end_use'})
        .assign(pct=lambda df: df['dollars'] / total if total else 0.0)
    )


def build_report(scenarios: dict[str, DisaggScenarioResult] | None = None) -> Path:
    scenarios = scenarios or run_decision5_scenarios()
    _OUTPUT.mkdir(parents=True, exist_ok=True)
    dest = _OUTPUT / _REPORT

    end_use = _end_use_coverage()
    balance = pd.concat(
        [compute_balance_metrics(s) for s in scenarios.values()],
        ignore_index=True,
    )
    baseline = scenarios['baseline']
    ef_frames = []
    for name, scen in scenarios.items():
        if name != 'baseline':
            ef_frames.append(diff_ef_vectors(baseline, scen).assign(scenario=name))
    ef = pd.concat(ef_frames, ignore_index=True) if ef_frames else pd.DataFrame()
    metadata = pd.DataFrame([report_metadata()])

    with pd.ExcelWriter(dest, engine='openpyxl') as writer:
        end_use.to_excel(writer, sheet_name='end_use_map', index=False)
        balance.to_excel(writer, sheet_name='qx_balance', index=False)
        ef.to_excel(writer, sheet_name='ef_diff', index=False)
        metadata.to_excel(writer, sheet_name='metadata', index=False)

    return dest
