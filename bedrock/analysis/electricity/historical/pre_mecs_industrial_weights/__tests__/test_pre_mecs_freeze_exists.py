"""The pre-MECS Industrial-weight freeze must stay on disk and tracked.

Records EIA-anchored G/T/D mixed units with dollar manufacturing weights
(the production path immediately before Table 7.7 shares). Compare live MECS
output with
``bedrock.analysis.electricity.current.vs_pre_mecs_industrial_weights``.
Not a CI gate for numeric drift.
"""

from __future__ import annotations

from bedrock.analysis.electricity.historical.pre_mecs_industrial_weights.paths import (
    MIXED_CONFIG,
    SNAPSHOT_FILES,
    config_dir,
)


def test_pre_mecs_industrial_weights_freeze_exists() -> None:
    folder = config_dir(MIXED_CONFIG)
    assert folder.is_dir(), f'pre-MECS freeze missing at {folder}'
    for name in SNAPSHOT_FILES:
        path = folder / name
        assert path.is_file(), f'pre-MECS freeze file missing: {path}'
