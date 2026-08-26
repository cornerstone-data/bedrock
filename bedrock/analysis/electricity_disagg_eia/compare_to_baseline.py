"""Compare live EIA-anchored G/T/D production to the prior-production freeze.

The historical CF report used 2018 eGRID and absolute EIA sales; that
comparison is not a production gate.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

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
            f'prior-production freeze only covers {DISAGG_CONFIG!r} and {MIXED_CONFIG!r}; '
            f'got {config_stem!r}'
        )
    path = config_dir(config_stem)
    if not path.is_dir():
        raise FileNotFoundError(
            f'prior-production freeze missing at {path}. '
            f'Run snapshot_current_production first.'
        )
    return str(path)


def freeze_root() -> str:
    return str(BASELINE_DIR)


def load_freeze_q(config_stem: str) -> pd.Series:
    path = Path(baseline_dir_for(config_stem)) / 'q.parquet'
    frame = pd.read_parquet(path)
    if 'q' in frame.columns:
        return frame['q'].astype(float)
    if frame.shape[1] == 1:
        return frame.iloc[:, 0].astype(float)
    squeezed = frame.squeeze()
    if not isinstance(squeezed, pd.Series):
        raise TypeError(f'expected a Series from {path}, got {type(squeezed)}')
    return squeezed.astype(float)


def compare_q_to_freeze(
    live_q: pd.Series,
    config_stem: str,
) -> pd.DataFrame:
    """Percent-diff live vs prior-production freeze ``q`` (not a CI gate)."""
    frozen = load_freeze_q(config_stem)
    idx = live_q.index.union(frozen.index)
    live = live_q.reindex(idx).astype(float)
    base = frozen.reindex(idx).astype(float)
    out = pd.DataFrame({'live': live, 'freeze': base})
    out['abs_diff'] = out['live'] - out['freeze']
    out['pct_diff'] = out['abs_diff'] / out['freeze'].replace(0.0, pd.NA)
    return out
