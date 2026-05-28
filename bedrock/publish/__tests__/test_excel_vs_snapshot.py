"""eeio_integration: bedrock-published XLSX `B` sheet matches the parquet snapshot.

Reproducibility guard: when this test passes we know the bedrock workbook
written at HEAD reproduces the values of the pinned
`B_USA_non_finetuned` parquet snapshot from `.SNAPSHOT_KEY`. Divergence
implies either (a) bedrock derivation drift, or (b) the snapshot is stale
and needs regenerating via `bedrock.utils.snapshots.generate_snapshots`.

TODO: `_DEFAULT_CONFIG` is hard-coded to `2025_usa_cornerstone_full_model`,
so this guard only covers that config. Parameterize via env var or pytest
parameter so arbitrary configs published via `bedrock.publish.excel.cli` can be
validated against their corresponding snapshots before upload.
"""

from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from bedrock.publish.__tests__._helpers import setup_config, teardown
from bedrock.publish.excel.writer import _LOCATION, write_model_to_xlsx
from bedrock.utils.snapshots.loader import load_current_snapshot

_DEFAULT_CONFIG = '2025_usa_cornerstone_full_model'


def _strip_loc_suffix(values: pd.Index) -> pd.Index:
    suffix = f'/{_LOCATION}'
    return pd.Index(
        [str(v)[: -len(suffix)] if str(v).endswith(suffix) else str(v) for v in values],
        name=values.name,
    )


@pytest.mark.eeio_integration
def test_published_B_matches_snapshot(tmp_path: Path) -> None:
    """Compare bedrock XLSX `B` sheet vs `B_USA_non_finetuned` parquet snapshot."""
    setup_config(_DEFAULT_CONFIG)
    try:
        out = os.fspath(tmp_path / 'model.xlsx')
        write_model_to_xlsx(out, config_name=_DEFAULT_CONFIG)

        B_xlsx = pd.read_excel(out, sheet_name='B', index_col=0, engine='openpyxl')
        B_xlsx.columns = _strip_loc_suffix(B_xlsx.columns)

        B_snapshot = load_current_snapshot('B_USA_non_finetuned')
    finally:
        teardown()

    assert list(B_xlsx.index) == list(B_snapshot.index), (
        f'ghg index mismatch: xlsx={list(B_xlsx.index)} '
        f'snapshot={list(B_snapshot.index)}'
    )
    assert list(B_xlsx.columns) == list(
        B_snapshot.columns
    ), 'sector column mismatch (after stripping /US suffix)'
    np.testing.assert_allclose(
        B_xlsx.values.astype(float),
        B_snapshot.values.astype(float),
        rtol=1e-9,
        atol=1e-12,
    )
