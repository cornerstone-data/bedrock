"""EF diagnostics comparison for d_85 scenarios vs baseline."""

from __future__ import annotations

import pandas as pd

from bedrock.analysis.electricity.d_85.scenario_ef_pipeline import (
    compute_ef_vectors,
    probe_e_source,
)
from bedrock.analysis.electricity.d_85.scenario_types import DisaggScenarioResult
from bedrock.utils.schemas.cornerstone_schemas import ELECTRICITY_DISAGG_SECTORS

ELEC = list(ELECTRICITY_DISAGG_SECTORS)
SIGNIFICANT = ELEC + ['212100', '331110', 'F01000']


def diff_ef_vectors(
    baseline: DisaggScenarioResult,
    scenario: DisaggScenarioResult,
    *,
    electricity_ratio_overrides: dict[str, float] | None = None,
) -> pd.DataFrame:
    """Compare D vectors between baseline and scenario."""
    if scenario.metrics_only:
        return pd.DataFrame(
            columns=['sector', 'D_baseline', 'D_scenario', 'pct_change', 'skipped']
        )
    base_ef = compute_ef_vectors(baseline)
    scen_ef = compute_ef_vectors(
        scenario, electricity_ratio_overrides=electricity_ratio_overrides
    )
    d_base = base_ef['D']
    d_scen = scen_ef['D']
    union = d_base.index.union(d_scen.index)
    rows: list[dict[str, object]] = []
    for sector in union:
        b = float(d_base.get(sector, float('nan')))
        s = float(d_scen.get(sector, float('nan')))
        pct = (s - b) / b * 100 if b and pd.notna(b) else float('nan')
        rows.append(
            {
                'sector': sector,
                'D_baseline': b,
                'D_scenario': s,
                'delta': s - b if pd.notna(s) and pd.notna(b) else float('nan'),
                'pct_change': pct,
                'significant': sector in SIGNIFICANT,
            }
        )
    return pd.DataFrame(rows)


def report_metadata() -> dict[str, str]:
    e_src = probe_e_source()
    return {
        'e_source': e_src['source'],
        'e_source_note': e_src['note'],
        'x_basis_note': (
            'Non-baseline B uses scenario compute_x(V) at 2017; production baseline '
            'uses derive_cornerstone_x_after_redefinition at usa_ghg_data_year'
        ),
    }
