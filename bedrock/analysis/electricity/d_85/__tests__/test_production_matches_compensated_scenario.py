"""The Table 8.3 + w_row compensated-scenario identity is no longer production.

Production now uses EIA-anchored G/T/D allocation. The freeze under
``bedrock/analysis/electricity_disagg_eia/output/`` records the previous
production path.
"""

from __future__ import annotations

from bedrock.analysis.electricity_disagg_eia.paths import (
    DISAGG_CONFIG,
    MIXED_CONFIG,
    SNAPSHOT_FILES,
    config_dir,
)


def test_prior_production_freeze_exists() -> None:
    for stem in (DISAGG_CONFIG, MIXED_CONFIG):
        folder = config_dir(stem)
        assert folder.is_dir(), f'prior-production freeze missing at {folder}'
        for name in SNAPSHOT_FILES:
            path = folder / name
            assert path.is_file(), f'prior-production freeze file missing: {path}'
