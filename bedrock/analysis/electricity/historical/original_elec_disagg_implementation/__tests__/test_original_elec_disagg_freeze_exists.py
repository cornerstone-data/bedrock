"""The original electricity-disagg freeze must stay on disk and tracked.

Production now uses EIA-anchored G/T/D allocation. The freeze under
``bedrock/analysis/electricity/historical/original_elec_disagg_implementation/output/``
records the previous UGO / Table 8.3 / Table 2.4 path.
"""

from __future__ import annotations

from bedrock.analysis.electricity.historical.original_elec_disagg_implementation.paths import (
    DISAGG_CONFIG,
    MIXED_CONFIG,
    SNAPSHOT_FILES,
    config_dir,
)


def test_original_elec_disagg_freeze_exists() -> None:
    for stem in (DISAGG_CONFIG, MIXED_CONFIG):
        folder = config_dir(stem)
        assert folder.is_dir(), f'original-elec-disagg freeze missing at {folder}'
        for name in SNAPSHOT_FILES:
            path = folder / name
            assert path.is_file(), f'original-elec-disagg freeze file missing: {path}'
        if stem == MIXED_CONFIG:
            c_row = folder / 'c_row.parquet'
            assert c_row.is_file(), f'mixed-units freeze missing c_row.parquet: {c_row}'
