"""BEA v0.3.1 reaggregation ladder (collapse G/T/D → monetary 221100)."""

from __future__ import annotations

from bedrock.analysis.electricity.current.diagnostics.ladders.registry import (
    LadderSpec,
    register,
)
from bedrock.utils.snapshots import releases

LADDER = register(
    LadderSpec(
        id='bea_v03_reaggregation',
        steps=(
            ('footing', '2025_usa_cornerstone_v0_3_electricity_footing'),
            ('reallocation', '2025_usa_cornerstone_v0_3_electricity_reallocation'),
            (
                'three_way',
                '2025_usa_cornerstone_v0_3_electricity_disaggregation',
            ),
            (
                'reaggregation',
                '2025_usa_cornerstone_v0_3_electricity_reaggregation',
            ),
        ),
        terminal='reaggregation',
        plain_baseline='2025_usa_cornerstone_v0_3',
        production_baseline='2025_usa_cornerstone_v0_3',
        nowcast_mut_vintage=None,
        snapshot_sha=releases.v0_3_1,
    )
)
