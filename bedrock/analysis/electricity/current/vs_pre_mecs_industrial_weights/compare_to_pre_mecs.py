"""Compare live mixed-units production to the pre-MECS Industrial-weight freeze.

Not a CI gate. The freeze is EIA-anchored G/T/D with dollar manufacturing
weights. Live production uses Table 7.7 purchased kWh inside manufacturing.
v0.3.1 footing stays a separate comparison.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from bedrock.analysis.electricity.historical.pre_mecs_industrial_weights.paths import (
    MIXED_CONFIG,
    config_dir,
)


def pre_mecs_dir_for(config_stem: str = MIXED_CONFIG) -> str:
    if config_stem != MIXED_CONFIG:
        raise ValueError(
            f'pre-MECS freeze only covers {MIXED_CONFIG!r}; got {config_stem!r}'
        )
    path = config_dir(config_stem)
    if not path.is_dir():
        raise FileNotFoundError(
            f'pre-MECS freeze missing at {path}. '
            'The freeze is committed under '
            'bedrock/analysis/electricity/historical/pre_mecs_industrial_weights/output/; '
            'if files are absent, they are missing or not tracked.'
        )
    return str(path)


def _load_series(path: Path, value_col: str) -> pd.Series:
    frame = pd.read_parquet(path)
    if value_col in frame.columns:
        return frame[value_col].astype(float)
    if frame.shape[1] == 1:
        return frame.iloc[:, 0].astype(float)
    squeezed = frame.squeeze()
    if not isinstance(squeezed, pd.Series):
        raise TypeError(f'expected a Series from {path}, got {type(squeezed)}')
    return squeezed.astype(float)


def load_freeze_q(config_stem: str = MIXED_CONFIG) -> pd.Series:
    return _load_series(Path(pre_mecs_dir_for(config_stem)) / 'q.parquet', 'q')


def load_freeze_n(config_stem: str = MIXED_CONFIG) -> pd.DataFrame:
    path = Path(pre_mecs_dir_for(config_stem)) / 'N.parquet'
    return pd.read_parquet(path)


def compare_q_to_freeze(
    live_q: pd.Series,
    config_stem: str = MIXED_CONFIG,
) -> pd.DataFrame:
    """Percent-diff live vs pre-MECS freeze ``q`` (not a CI gate)."""
    frozen = load_freeze_q(config_stem)
    idx = live_q.index.union(frozen.index)
    live = live_q.reindex(idx).astype(float)
    base = frozen.reindex(idx).astype(float)
    out = pd.DataFrame({'live': live, 'freeze': base})
    out['abs_diff'] = out['live'] - out['freeze']
    out['pct_diff'] = out['abs_diff'] / out['freeze'].replace(0.0, pd.NA)
    return out


def compare_n_to_freeze(
    live_n: pd.DataFrame,
    config_stem: str = MIXED_CONFIG,
) -> pd.DataFrame:
    """Percent-diff live vs pre-MECS freeze ``N`` (not a CI gate)."""
    frozen = load_freeze_n(config_stem)
    live = live_n.astype(float)
    base = frozen.astype(float).reindex(index=live.index, columns=live.columns)
    diff = live - base
    pct = diff / base.replace(0.0, pd.NA)
    return pd.DataFrame(
        {
            'abs_diff': diff.stack(),
            'pct_diff': pct.stack(),
        }
    )
