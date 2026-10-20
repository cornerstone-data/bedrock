"""Nowcast-2024 reaggregation ladder (v0.4, no mixed-units rung)."""

from __future__ import annotations

from bedrock.analysis.electricity.current.diagnostics.ladders.registry import (
    LadderSpec,
    register,
)
from bedrock.utils.snapshots import releases

LADDER = register(
    LadderSpec(
        id='nowcast_2024_reaggregation',
        steps=(),
        terminal='reaggregation',
        plain_baseline='2025_usa_cornerstone_v0_5_2024',
        production_baseline='2025_usa_cornerstone_v0_4',
        nowcast_mut_vintage='v0.3.0_4276083',
        snapshot_sha=releases.v0_4_0,
    )
)
