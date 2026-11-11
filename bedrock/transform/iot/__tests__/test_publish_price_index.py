"""Tests for the published BEA / Cornerstone price index artifacts. Hermetic."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from bedrock.transform.iot import publish_price_index as module
from bedrock.transform.iot.eia_utility_go_adjustment import CONTROLLED_YEARS
from bedrock.utils.config.settings import GIT_HASH, PKG_VERSION_NUMBER
from bedrock.utils.economic import inflation_helpers_usa as helpers

_YEARS = [2016, *CONTROLLED_YEARS, 2025]
_WIDE = pd.DataFrame(
    {year: [100.0 + (year - 2017), 80.0, 50.0] for year in _YEARS},
    index=pd.Index(['221100', 'S00101', '111200'], name='sector_code'),
    dtype=float,
)
_WIDE.loc['221100', 2024] = 200.0
_WIDE.loc['221100', 2025] = 220.0
_WIDE.loc['221100', 2016] = 90.0


@pytest.fixture(autouse=True)
def _toy_index() -> Iterator[None]:
    eia_span = {year: 1.0 + 0.1 * (year - 2017) for year in CONTROLLED_YEARS}
    with (
        patch.object(module, 'derive_industry_price_index', return_value=_WIDE.copy()),
        patch(
            'bedrock.transform.iot.eia_utility_go_adjustment.retail_price_index',
            side_effect=lambda year: eia_span[int(year)],
        ),
    ):
        yield


def test_bea_flavor_is_pure_derive(tmp_path: Path) -> None:
    long = module.price_index_long('2026Q2', flavor='bea')
    back = long.pivot(index='sector_code', columns='year', values='price_index')
    pd.testing.assert_frame_equal(back.reindex_like(_WIDE), _WIDE, check_names=False)
    _, meta = module.save_price_index('2026Q2', tmp_path, flavor='bea')
    tool_meta = json.loads(meta.read_text())['tool_meta']
    assert tool_meta['flavor'] == 'bea'
    assert 'electric_price_override' not in tool_meta


def test_cornerstone_electric_matches_eia_helper_non_electric_unchanged(
    tmp_path: Path,
) -> None:
    expected = helpers._electric_lines_on_eia_retail_price(_WIDE.copy())
    long = module.price_index_long('2026Q2', flavor='cornerstone')
    back = long.pivot(index='sector_code', columns='year', values='price_index')
    pd.testing.assert_frame_equal(
        back.reindex_like(expected), expected, check_names=False
    )
    non_electric = back.loc['111200']
    expected_non_electric = _WIDE.loc['111200']
    assert isinstance(non_electric, pd.Series)
    assert isinstance(expected_non_electric, pd.Series)
    pd.testing.assert_series_equal(
        non_electric, expected_non_electric, check_names=False
    )

    parquet, meta = module.save_price_index('2026Q2', tmp_path, flavor='cornerstone')
    assert parquet.name.startswith('Cornerstone_PriceIndex_2026Q2_')
    tool_meta = json.loads(meta.read_text())['tool_meta']
    assert tool_meta['flavor'] == 'cornerstone'
    assert 'electric_price_override' in tool_meta


def test_stem_names_the_flavor_and_bea_vintage() -> None:
    bea = module.artifact_stem('2026Q2', flavor='bea')
    cs = module.artifact_stem('2026Q2', flavor='cornerstone')
    assert bea.startswith(f'BEA_PriceIndex_2026Q2_v{PKG_VERSION_NUMBER}')
    assert cs.startswith(f'Cornerstone_PriceIndex_2026Q2_v{PKG_VERSION_NUMBER}')
    if GIT_HASH is not None:
        assert bea.endswith(GIT_HASH)
        assert cs.endswith(GIT_HASH)


def test_upload_refuses_to_overwrite_a_published_stem(tmp_path: Path) -> None:
    with (
        patch('bedrock.utils.io.gcp.gcs_path_exists', return_value=True),
        patch('bedrock.utils.io.gcp.upload_file_to_gcs') as upload,
        pytest.raises(FileExistsError),
    ):
        module.save_price_index('2026Q2', tmp_path, upload=True, flavor='cornerstone')
    upload.assert_not_called()
