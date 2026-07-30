"""Tests for EF diagnostics comparison."""

from __future__ import annotations

import pandas as pd

from bedrock.analysis.electricity.d_85.diagnostics_comparison import (
    diff_ef_vectors,
    report_metadata,
)
from bedrock.analysis.electricity.d_85.scenario_types import (
    DisaggScenarioResult,
    ScenarioWeights,
)


def _result(*, metrics_only: bool) -> DisaggScenarioResult:
    idx = pd.Index(['221110', '221121', '221122'], name='sector')
    z = pd.DataFrame(0.0, index=idx, columns=idx)
    q = pd.Series({'221110': 1.0, '221121': 1.0, '221122': 1.0})
    w = pd.Series({'221110': 0.34, '221121': 0.04, '221122': 0.62})
    weights = ScenarioWeights(w, None, w, w, None, None)
    return DisaggScenarioResult(
        's', weights, z, z, z, z, z, q, q, metrics_only=metrics_only
    )


def test_diff_ef_vectors_skips_metrics_only() -> None:
    base = _result(metrics_only=False)
    broken = _result(metrics_only=True)
    out = diff_ef_vectors(base, broken)
    assert out.empty


def test_report_metadata_keys() -> None:
    meta = report_metadata()
    assert 'e_source' in meta
    assert 'x_basis_note' in meta
