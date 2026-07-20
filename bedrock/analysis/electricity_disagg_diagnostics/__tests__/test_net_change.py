"""Tests for chained net BLy change metrics and chart bars."""

from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use('Agg')

import pandas as pd

from bedrock.analysis.electricity_disagg_diagnostics.dispersion import DISPERSION_TOL
from bedrock.analysis.electricity_disagg_diagnostics.net_change import (
    compute_chained_net_change,
)
from bedrock.analysis.electricity_disagg_diagnostics.waterfall import (
    render_net_change_waterfall,
)


def test_build_bars_only_delta_when_total_changes() -> None:
    footing = pd.Series({'1111A0': 100.0, '221100': 0.0})
    realloc = pd.Series({'1111A0': 100.0, '221100': 0.0})
    disagg = pd.Series({'1111A0': 100.0, '221100': 0.0, '221110': 0.0})
    mixed = pd.Series({'1111A0': 100.0, '221100': 0.0, '221110': 5.0})

    result = compute_chained_net_change(
        footing,
        [realloc, disagg, mixed],
        [
            'Co-production\nreallocation',
            '3-way\nsplit',
            'Conversion to\nphysical units',
        ],
        footing_label='Cornerstone v0.2',
    )
    assert result.step_deltas_mmt[0] == 0.0
    assert result.step_deltas_mmt[1] == 0.0
    assert result.step_deltas_mmt[2] == 5.0

    bars = result.build_bars()
    labels = [b.label for b in bars]
    assert labels[0] == 'Cornerstone v0.2'
    assert labels[1] == 'Co-production\nreallocation'
    assert labels[2] == '3-way\nsplit'
    assert labels[3] == 'Δ from\nConversion to\nphysical units'
    assert labels[4] == 'Conversion to\nphysical units'
    assert len([b for b in bars if b.kind == 'delta']) == 1


def test_net_change_waterfall_writes_png(tmp_path: Path) -> None:
    footing = pd.Series({'1111A0': 100.0})
    mixed = pd.Series({'1111A0': 103.0})
    result = compute_chained_net_change(
        footing,
        [mixed],
        ['Conversion to\nphysical units'],
        footing_label='Cornerstone v0.2',
    )
    out = tmp_path / 'net_change_mmt.png'
    render_net_change_waterfall(result, use_pct=False, out_path=out)
    assert out.exists()
    assert out.stat().st_size > 0


def test_step_deltas_sum_to_combined_delta() -> None:
    footing = pd.Series({'1111A0': 10.0, '221100': 20.0})
    step1 = pd.Series({'1111A0': 11.0, '221100': 0.0, '221110': 20.0})
    step2 = pd.Series({'1111A0': 12.0, '221100': 0.0, '221110': 21.0})
    result = compute_chained_net_change(
        footing,
        [step1, step2],
        ['A', 'B'],
        footing_label='footing',
    )
    assert abs(sum(result.step_deltas_mmt) - result.combined_delta_mmt) < DISPERSION_TOL
