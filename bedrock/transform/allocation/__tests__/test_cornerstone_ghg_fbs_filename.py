"""Pinned Cornerstone GHG FBS filename versus newest-upload selection."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from bedrock.transform.allocation import derived
from bedrock.utils.config.usa_config import USAConfig


def _frame(marker: str) -> pd.DataFrame:
    return pd.DataFrame({'FlowAmount': [1.0], 'marker': [marker]})


def _config(filename: str | None) -> USAConfig:
    return USAConfig.model_validate({'cornerstone_ghg_fbs_filename': filename})


def test_pin_reads_the_local_file_and_skips_gcs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    name = 'GHG_national_Cornerstone_nowcast_facilities_2024_v0.3.0_3a1dddc.parquet'
    _frame('local').to_parquet(tmp_path / name)
    monkeypatch.setattr(derived, 'get_usa_config', lambda: _config(name))

    def _no_download(*_a: object, **_k: object) -> None:
        raise AssertionError('pinned file is already local')

    monkeypatch.setattr(
        'bedrock.utils.config.settings.FBS_DIR',
        tmp_path,
    )
    monkeypatch.setattr('bedrock.utils.io.gcp.download_gcs_file', _no_download)

    loaded = derived._load_cornerstone_ghg_fbs_from_gcs(
        base_name='GHG_national_Cornerstone_nowcast_facilities_2024'
    )
    assert loaded['marker'].tolist() == ['local']


def test_pin_downloads_that_object_when_the_local_file_is_absent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pinned = 'GHG_national_Cornerstone_nowcast_facilities_2024_v0.3.0_3a1dddc.parquet'
    newer = 'GHG_national_Cornerstone_nowcast_facilities_2024_v0.5.0_a166975.parquet'
    monkeypatch.setattr(derived, 'get_usa_config', lambda: _config(pinned))
    monkeypatch.setattr('bedrock.utils.config.settings.FBS_DIR', tmp_path)
    calls: list[str] = []

    def _download(name: str, _sub: str, pth: str) -> None:
        calls.append(name)
        _frame('downloaded').to_parquet(pth)

    monkeypatch.setattr('bedrock.utils.io.gcp.download_gcs_file', _download)
    monkeypatch.setattr(
        'bedrock.utils.io.gcp.list_bucket_files',
        lambda _sub: pd.DataFrame(
            {
                'base_name': ['GHG_national_Cornerstone_nowcast_facilities_2024'],
                'extension': ['.parquet'],
                'created': ['2026-10-10'],
                'full_path': [
                    f'gs://cornerstone-default/transform/output_data/{newer}'
                ],
            }
        ),
    )

    loaded = derived._load_cornerstone_ghg_fbs_from_gcs(
        base_name='GHG_national_Cornerstone_nowcast_facilities_2024'
    )
    assert calls == [pinned]
    assert loaded['marker'].tolist() == ['downloaded']


def test_omitted_pin_uses_the_newest_upload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    older = 'GHG_national_Cornerstone_nowcast_facilities_2024_v0.3.0_3a1dddc.parquet'
    newer = 'GHG_national_Cornerstone_nowcast_facilities_2024_v0.5.0_a166975.parquet'
    monkeypatch.setattr(derived, 'get_usa_config', lambda: _config(None))
    monkeypatch.setattr('bedrock.utils.config.settings.FBS_DIR', tmp_path)

    def _download(name: str, _sub: str, pth: str) -> None:
        _frame(name).to_parquet(pth)

    monkeypatch.setattr('bedrock.utils.io.gcp.download_gcs_file', _download)
    monkeypatch.setattr(
        'bedrock.utils.io.gcp.list_bucket_files',
        lambda _sub: pd.DataFrame(
            {
                'base_name': [
                    'GHG_national_Cornerstone_nowcast_facilities_2024',
                    'GHG_national_Cornerstone_nowcast_facilities_2024',
                ],
                'extension': ['.parquet', '.parquet'],
                'created': ['2026-01-01', '2026-10-10'],
                'full_path': [
                    f'gs://cornerstone-default/transform/output_data/{older}',
                    f'gs://cornerstone-default/transform/output_data/{newer}',
                ],
            }
        ),
    )

    loaded = derived._load_cornerstone_ghg_fbs_from_gcs(
        base_name='GHG_national_Cornerstone_nowcast_facilities_2024'
    )
    assert loaded['marker'].tolist() == [newer]


def test_pin_for_a_different_stem_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    name = 'GHG_national_Cornerstone_nowcast_facilities_2024_v0.3.0_3a1dddc.parquet'
    monkeypatch.setattr(derived, 'get_usa_config', lambda: _config(name))
    with pytest.raises(ValueError, match='does not match'):
        derived._load_cornerstone_ghg_fbs_from_gcs(
            base_name='GHG_national_Cornerstone_nowcast_facilities_2023'
        )


def test_pin_without_a_git_hash_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        derived,
        'get_usa_config',
        lambda: _config('GHG_national_Cornerstone_nowcast_facilities_2024.parquet'),
    )
    with pytest.raises(ValueError, match='version and git hash'):
        derived._load_cornerstone_ghg_fbs_from_gcs(
            base_name='GHG_national_Cornerstone_nowcast_facilities_2024'
        )
