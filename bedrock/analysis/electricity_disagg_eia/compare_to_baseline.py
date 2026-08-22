"""Compare live production to the P0 freeze.

Stub until P6 rewrites this against the EIA-anchored G/T/D path.
The historical CF report used 2018 eGRID and absolute EIA sales; that
comparison is not a P6 gate.
"""

from __future__ import annotations

from bedrock.analysis.electricity_disagg_eia.paths import (
    BASELINE_DIR,
    DISAGG_CONFIG,
    MIXED_CONFIG,
    config_dir,
)


def baseline_dir_for(config_stem: str) -> str:
    """Return the freeze directory for a waterfall config stem."""
    if config_stem not in (DISAGG_CONFIG, MIXED_CONFIG):
        raise ValueError(
            f'P0 freeze only covers {DISAGG_CONFIG!r} and {MIXED_CONFIG!r}; '
            f'got {config_stem!r}'
        )
    path = config_dir(config_stem)
    if not path.is_dir():
        raise FileNotFoundError(
            f'P0 freeze missing at {path}. Run snapshot_current_production first.'
        )
    return str(path)


def freeze_root() -> str:
    return str(BASELINE_DIR)
