from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from bedrock.extract.iot.gdp import BeaDataVersion
from bedrock.utils.config.usa_config import USAConfig
from bedrock.utils.validation.diagnostics_baseline import (
    DEFAULT_USEEIO_BASELINE_PIN_JSON,
)
from bedrock.utils.validation.useeio_excel_baseline import (
    load_useeio_baseline_pin_overrides,
    split_cornerstone_default_gs_uri,
    useeio_baseline_price_index_levels,
)


def test_split_cornerstone_default_gs_uri() -> None:
    uri = (
        'gs://cornerstone-default/snapshots/USEEIOv2.6.0-phoebe-23/'
        'USEEIOv2.6.0-phoebe-23.xlsx'
    )
    name, sub = split_cornerstone_default_gs_uri(uri)
    assert name == 'USEEIOv2.6.0-phoebe-23.xlsx'
    assert sub == 'snapshots/USEEIOv2.6.0-phoebe-23'


def test_split_cornerstone_default_gs_uri_rejects_wrong_bucket() -> None:
    with pytest.raises(ValueError, match='cornerstone-default'):
        split_cornerstone_default_gs_uri('gs://other-bucket/foo/bar.xlsx')


def test_load_useeio_baseline_pin_overrides(tmp_path: Path) -> None:
    pin = tmp_path / 'pin.json'
    pin.write_text(
        json.dumps(
            {
                'gs_uri': 'gs://cornerstone-default/snapshots/a/b.xlsx',
                'sha256': 'a' * 64,
                'model_version_label': 'test-label',
            }
        ),
        encoding='utf-8',
    )
    got = load_useeio_baseline_pin_overrides(str(pin))
    assert got == {
        'useeio_baseline_xlsx_gs_uri': 'gs://cornerstone-default/snapshots/a/b.xlsx',
        'useeio_baseline_xlsx_sha256': 'a' * 64,
        'useeio_model_version_label': 'test-label',
    }


def test_load_useeio_baseline_pin_overrides_price_index(tmp_path: Path) -> None:
    pin = tmp_path / 'pin.json'
    pin.write_text(
        json.dumps(
            {
                'gs_uri': 'gs://cornerstone-default/snapshots/a/b.xlsx',
                'sha256': 'a' * 64,
                'model_version_label': 'test-label',
                'price_index': 'useeior_v1.8.0',
            }
        ),
        encoding='utf-8',
    )
    got = load_useeio_baseline_pin_overrides(str(pin))
    assert got['useeio_baseline_price_index'] == 'useeior_v1.8.0'


def test_committed_pin_names_the_baseline_price_index() -> None:
    got = load_useeio_baseline_pin_overrides(DEFAULT_USEEIO_BASELINE_PIN_JSON)
    assert got['useeio_baseline_price_index'] == 'useeior_v1.8.0'


_USEEIOR_LEVELS = pd.DataFrame(
    {2017: [100.0, 100.0], 2024: [125.0, 140.0]},
    index=pd.Index(['1111A0', '562000']),
)


@pytest.mark.parametrize('vintage', ['2025Q2', '2026Q2'])
def test_baseline_price_index_ignores_bea_vintage(vintage: BeaDataVersion) -> None:
    """The pinned index is the same whatever BEA vintage the run uses (#1102)."""
    cfg = USAConfig(bea_price_index_vintage=vintage)
    with patch(
        'bedrock.utils.economic.inflation_helpers_useeio.obtain_useeior_detail_industry_cpi_levels',
        return_value=_USEEIOR_LEVELS,
    ):
        got = useeio_baseline_price_index_levels(cfg)
    assert got is not None
    assert got.loc['1111A0', 2024] == 125.0


def test_baseline_price_index_fills_waste_children_from_parent() -> None:
    with patch(
        'bedrock.utils.economic.inflation_helpers_useeio.obtain_useeior_detail_industry_cpi_levels',
        return_value=_USEEIOR_LEVELS,
    ):
        got = useeio_baseline_price_index_levels(USAConfig())
    assert got is not None
    for child in ['562111', '562HAZ', '562212', '562213', '562910', '562920', '562OTH']:
        assert got.loc[child, 2024] == 140.0


def test_baseline_price_index_can_follow_bea_vintage() -> None:
    cfg = USAConfig(useeio_baseline_price_index='bea_price_index_vintage')
    assert useeio_baseline_price_index_levels(cfg) is None


def test_load_useeio_baseline_pin_overrides_missing_key(tmp_path: Path) -> None:
    pin = tmp_path / 'pin.json'
    pin.write_text(
        json.dumps({'gs_uri': 'gs://cornerstone-default/x/y.xlsx'}), encoding='utf-8'
    )
    with pytest.raises(ValueError, match='sha256'):
        load_useeio_baseline_pin_overrides(str(pin))
