from __future__ import annotations

from pathlib import Path
from typing import Generator
from unittest.mock import patch

import pandas as pd
import pytest

from bedrock.utils.config.usa_config import reset_usa_config, set_global_usa_config
from bedrock.utils.snapshots import loader as loader_mod
from bedrock.utils.snapshots.loader import load_snapshot, resolve_snapshot_key


@pytest.fixture(autouse=True, scope="function")
def reset_global_usa_config_before_test() -> Generator[None, None, None]:
    reset_usa_config(should_reset_env_var=True)
    yield


def test_resolve_snapshot_key_uses_v0_when_config_defaults() -> None:
    set_global_usa_config("test_usa_config.yaml")
    assert resolve_snapshot_key() == "v0"


def test_resolve_snapshot_key_uses_configured_git_sha() -> None:
    set_global_usa_config("test_usa_config_git_sha.yaml")
    assert resolve_snapshot_key() == "2ebb51f7190c3a62b5d8b2420bff9b20f57282fc"


def _write_parquet(path: Path, value: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"v": [value]}).to_parquet(path)


def test_load_snapshot_b_usa_prefers_new_local_name(tmp_path: Path) -> None:
    key = "alias_key"
    snap_dir = tmp_path / key
    _write_parquet(snap_dir / "B_USA.parquet", 1.0)
    _write_parquet(snap_dir / "B_USA_non_finetuned.parquet", 2.0)

    with (
        patch.object(loader_mod, "snapshot_local_dir", return_value=str(snap_dir)),
        patch.object(loader_mod, "download_gcs_file_if_not_exists") as download_mock,
    ):
        df = load_snapshot("B_USA", key)

    assert df["v"].iloc[0] == 1.0
    download_mock.assert_not_called()


def test_load_snapshot_b_usa_falls_back_to_legacy_local_name(tmp_path: Path) -> None:
    key = "alias_key"
    snap_dir = tmp_path / key
    _write_parquet(snap_dir / "B_USA_non_finetuned.parquet", 3.0)

    with (
        patch.object(loader_mod, "snapshot_local_dir", return_value=str(snap_dir)),
        patch.object(loader_mod, "download_gcs_file_if_not_exists") as download_mock,
    ):
        df = load_snapshot("B_USA", key)

    assert df["v"].iloc[0] == 3.0
    download_mock.assert_not_called()


def test_load_snapshot_b_usa_falls_back_to_legacy_gcs_download(tmp_path: Path) -> None:
    key = "alias_key"
    snap_dir = tmp_path / key

    def _fake_download(name: str, sub_bucket: str, pth: str) -> None:
        if name == "B_USA_non_finetuned.parquet":
            _write_parquet(Path(pth), 4.0)

    with (
        patch.object(loader_mod, "snapshot_local_dir", return_value=str(snap_dir)),
        patch.object(
            loader_mod, "download_gcs_file_if_not_exists", side_effect=_fake_download
        ),
    ):
        df = load_snapshot("B_USA", key)

    assert df["v"].iloc[0] == 4.0
