"""Production 3-way is no longer the Table 8.3 + w_row compensated scenario.

That identity died with the EIA-anchored G/T/D replacement. The P0 freeze
under ``bedrock/analysis/electricity_disagg_eia/output/`` is the historical
record of the old production path.
"""

from __future__ import annotations

from bedrock.analysis.electricity_disagg_eia.paths import (
    DISAGG_CONFIG,
    MIXED_CONFIG,
    SNAPSHOT_FILES,
    config_dir,
)


def test_p0_freeze_exists_for_old_3way() -> None:
    for stem in (DISAGG_CONFIG, MIXED_CONFIG):
        folder = config_dir(stem)
        assert folder.is_dir(), f'P0 freeze missing at {folder}'
        for name in SNAPSHOT_FILES:
            path = folder / name
            assert path.is_file(), f'P0 freeze file missing: {path}'
