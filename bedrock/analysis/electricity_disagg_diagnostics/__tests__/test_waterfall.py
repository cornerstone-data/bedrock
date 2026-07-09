"""Smoke tests for waterfall renderer."""

from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use('Agg')

from bedrock.analysis.electricity_disagg_diagnostics.dispersion import ChainedDispersion
from bedrock.analysis.electricity_disagg_diagnostics.waterfall import (
    render_dispersion_waterfall,
)


def test_waterfall_writes_png(tmp_path: Path) -> None:
    no_offset = ChainedDispersion(
        step_labels=['A', 'B', 'C'],
        step_values_mmt=[1.0, 2.0, 3.0],
        combined_mmt=6.0,
        offset_mmt=0.0,
        footing_total_mmt=100.0,
    )
    out = tmp_path / 'waterfall_mmt.png'
    render_dispersion_waterfall(no_offset, use_pct=False, out_path=out)
    assert out.exists()
    assert out.stat().st_size > 0


def test_waterfall_with_offsetting_bar(tmp_path: Path) -> None:
    with_offset = ChainedDispersion(
        step_labels=['A', 'B'],
        step_values_mmt=[5.0, 5.0],
        combined_mmt=3.0,
        offset_mmt=7.0,
        footing_total_mmt=50.0,
    )
    assert with_offset.show_offsetting_bar
    out = tmp_path / 'waterfall_pct.png'
    render_dispersion_waterfall(with_offset, use_pct=True, out_path=out)
    assert out.exists()
