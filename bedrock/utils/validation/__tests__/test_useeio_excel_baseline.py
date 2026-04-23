from __future__ import annotations

import json
from pathlib import Path

import pytest

from bedrock.utils.validation.useeio_excel_baseline import (
    load_useeio_baseline_pin_overrides,
    split_cornerstone_default_gs_uri,
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


def test_load_useeio_baseline_pin_overrides_missing_key(tmp_path: Path) -> None:
    pin = tmp_path / 'pin.json'
    pin.write_text(
        json.dumps({'gs_uri': 'gs://cornerstone-default/x/y.xlsx'}), encoding='utf-8'
    )
    with pytest.raises(ValueError, match='sha256'):
        load_useeio_baseline_pin_overrides(str(pin))
