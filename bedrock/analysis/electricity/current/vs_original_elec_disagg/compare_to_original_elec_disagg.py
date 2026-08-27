"""Compare live EIA-anchored G/T/D production to the original-implementation freeze.

The historical CF report used 2018 eGRID and absolute EIA sales; that
comparison is not a production gate.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from bedrock.analysis.electricity.historical.original_elec_disagg_implementation.paths import (
    DISAGG_CONFIG,
    MIXED_CONFIG,
    OUT_DIR,
    config_dir,
)


def original_elec_disagg_dir_for(config_stem: str) -> str:
    """Return the original-implementation freeze directory for a waterfall config stem."""
    if config_stem not in (DISAGG_CONFIG, MIXED_CONFIG):
        raise ValueError(
            f'original-elec-disagg freeze only covers {DISAGG_CONFIG!r} and {MIXED_CONFIG!r}; '
            f'got {config_stem!r}'
        )
    path = config_dir(config_stem)
    if not path.is_dir():
        raise FileNotFoundError(
            f'original-elec-disagg freeze missing at {path}. '
            'The freeze is committed under '
            'bedrock/analysis/electricity/historical/original_elec_disagg_implementation/output/; '
            'if files are absent, they are missing or not tracked.'
        )
    return str(path)


def original_elec_disagg_output_dir() -> str:
    return str(OUT_DIR)


def load_freeze_q(config_stem: str) -> pd.Series:
    path = Path(original_elec_disagg_dir_for(config_stem)) / 'q.parquet'
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
    """Percent-diff live vs original-elec-disagg freeze ``q`` (not a CI gate)."""
    frozen = load_freeze_q(config_stem)
    idx = live_q.index.union(frozen.index)
    live = live_q.reindex(idx).astype(float)
    base = frozen.reindex(idx).astype(float)
    out = pd.DataFrame({'live': live, 'freeze': base})
    out['abs_diff'] = out['live'] - out['freeze']
    out['pct_diff'] = out['abs_diff'] / out['freeze'].replace(0.0, pd.NA)
    return out
