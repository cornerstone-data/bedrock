"""Which price index ``_industry_price_index_levels`` loads per vintage. Hermetic."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

import bedrock.utils.economic.inflation_helpers_bea as bea
import bedrock.utils.economic.inflation_helpers_cornerstone as cornerstone
from bedrock.extract.iot.gdp import BeaDataVersion
from bedrock.transform.iot import publish_price_index
from bedrock.utils.config.usa_config import get_usa_config, reset_usa_config

STEM = 'BEA_PriceIndex_2026Q2_v0.5.0_abc1234'
_WIDE = pd.DataFrame(
    {2024: [110.0, 95.5], 2025: [112.0, 97.0]},
    index=pd.Index(['111110', '562111'], name='sector_code'),
)


@pytest.fixture(autouse=True)
def _fresh() -> Iterator[None]:
    reset_usa_config(should_reset_env_var=True)
    cornerstone.clear_cornerstone_inflation_caches()
    yield
    cornerstone.clear_cornerstone_inflation_caches()
    reset_usa_config(should_reset_env_var=True)


@pytest.mark.parametrize(
    ('vintage', 'loader'),
    [
        ('2025Q2', '_load_reference_parquet'),
        ('2026Q2', 'load_published_price_index'),
    ],
)
def test_vintage_picks_the_price_index(vintage: BeaDataVersion, loader: str) -> None:
    with patch.object(bea, loader, return_value=_WIDE) as load:
        assert bea.obtain_inflation_factors_from_reference_data(vintage) is _WIDE
    load.assert_called_once()


def test_vintage_defaults_to_the_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(get_usa_config(), 'bea_price_index_vintage', '2026Q2')
    with patch.object(bea, 'load_published_price_index', return_value=_WIDE) as load:
        bea.obtain_inflation_factors_from_reference_data()
    load.assert_called_once_with('2026Q2')


@pytest.mark.parametrize('apply_io', [False, True])
def test_margin_levels_follow_the_vintage(
    monkeypatch: pytest.MonkeyPatch, apply_io: bool
) -> None:
    cfg = get_usa_config()
    monkeypatch.setattr(cfg, 'useeio_margins', False)
    monkeypatch.setattr(cfg, 'apply_io_year_adjustments', apply_io)
    monkeypatch.setattr(cfg, 'bea_price_index_vintage', '2026Q2')
    loader = (
        'derive_industry_price_index'
        if apply_io
        else 'obtain_inflation_factors_from_reference_data'
    )
    with patch.object(cornerstone, loader, return_value=_WIDE) as load:
        assert cornerstone._industry_price_index_levels() is _WIDE
    load.assert_called_once()


def test_unpinned_vintage_names_the_publish_step() -> None:
    with (
        patch.dict(bea.PUBLISHED_PRICE_INDEX_STEMS, {}, clear=True),
        pytest.raises(KeyError, match='publish_price_index'),
    ):
        bea.load_published_price_index('2026Q2')


def test_stem_pinned_under_the_wrong_vintage_is_refused() -> None:
    wrong = {'2026Q2': 'BEA_PriceIndex_2025Q2_v0.5.0_abc1234'}
    with (
        patch.dict(bea.PUBLISHED_PRICE_INDEX_STEMS, wrong),
        pytest.raises(ValueError, match='names another'),
    ):
        bea.load_published_price_index('2026Q2')


def test_published_file_reads_back_as_the_derived_index(tmp_path: Path) -> None:
    with (
        patch.object(
            publish_price_index, 'derive_industry_price_index', return_value=_WIDE
        ),
        patch.object(publish_price_index, 'artifact_stem', return_value=STEM),
    ):
        publish_price_index.save_price_index('2026Q2', tmp_path)
    with (
        patch.dict(bea.PUBLISHED_PRICE_INDEX_STEMS, {'2026Q2': STEM}),
        patch.object(bea, 'FBA_DIR', tmp_path),
        patch.object(bea, 'download_gcs_file') as download,
    ):
        loaded = bea.load_published_price_index('2026Q2')
    download.assert_not_called()
    pd.testing.assert_frame_equal(loaded, _WIDE, check_names=False)
