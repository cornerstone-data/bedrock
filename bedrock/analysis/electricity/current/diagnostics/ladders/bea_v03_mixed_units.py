"""BEA v0.3.1 mixed-units research ladder (default diagnostics chain)."""

from __future__ import annotations

from bedrock.analysis.electricity.current.diagnostics.ladders.registry import (
    LadderSpec,
    register,
)
from bedrock.utils.snapshots import releases

LADDER = register(
    LadderSpec(
        id='bea_v03_mixed_units',
        steps=(
            ('footing', '2025_usa_cornerstone_v0_3_electricity_footing'),
            ('reallocation', '2025_usa_cornerstone_v0_3_electricity_reallocation'),
            (
                'three_way',
                '2025_usa_cornerstone_v0_3_electricity_disaggregation',
            ),
            ('mixed_units', '2025_usa_cornerstone_v0_3_electricity_mixed_units'),
        ),
        terminal='mixed_units',
        plain_baseline=None,
        production_baseline='2025_usa_cornerstone_v0_3',
        nowcast_mut_vintage=None,
        snapshot_sha=releases.v0_3_1,
    )
)
