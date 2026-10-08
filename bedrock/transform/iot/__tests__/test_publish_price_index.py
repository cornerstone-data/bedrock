"""Tests for the published BEA price index artifact. Hermetic - no BEA / GCS."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from bedrock.transform.iot import publish_price_index as module
from bedrock.utils.config.settings import GIT_HASH, PKG_VERSION_NUMBER

_WIDE = pd.DataFrame(
    {2024: [110.0, 95.5], 2025: [112.0, 97.0]},
    index=pd.Index(['111110', '562111'], name='sector_code'),
)


@pytest.fixture(autouse=True)
def _toy_index() -> Iterator[None]:
    with patch.object(module, 'derive_industry_price_index', return_value=_WIDE):
        yield


def test_long_format_round_trips_the_wide_index() -> None:
    long = module.price_index_long('2026Q2')
    assert list(long.columns) == ['sector_code', 'year', 'price_index']
    back = long.pivot(index='sector_code', columns='year', values='price_index')
    pd.testing.assert_frame_equal(back, _WIDE, check_names=False)


def test_stem_names_the_bea_vintage() -> None:
    stem = module.artifact_stem('2026Q2')
    assert stem.startswith(f'BEA_PriceIndex_2026Q2_v{PKG_VERSION_NUMBER}')
    if GIT_HASH is not None:
        assert stem.endswith(GIT_HASH)


def test_save_writes_parquet_and_sidecar(tmp_path: Path) -> None:
    parquet, meta = module.save_price_index('2026Q2', tmp_path)
    assert parquet.name == f'{module.artifact_stem("2026Q2")}.parquet'
    assert len(pd.read_parquet(parquet)) == _WIDE.size
    tool_meta = json.loads(meta.read_text())['tool_meta']
    assert tool_meta['bea_data_vintage'] == '2026Q2'
    assert tool_meta['years'] == [2024, 2025]


def test_upload_refuses_to_overwrite_a_published_stem(tmp_path: Path) -> None:
    with (
        patch('bedrock.utils.io.gcp.gcs_path_exists', return_value=True),
        patch('bedrock.utils.io.gcp.upload_file_to_gcs') as upload,
        pytest.raises(FileExistsError),
    ):
        module.save_price_index('2026Q2', tmp_path, upload=True)
    upload.assert_not_called()
