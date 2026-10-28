import os
import posixpath

import pandas as pd

from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.io.gcp import download_gcs_file_if_not_exists
from bedrock.utils.snapshots.names import SnapshotName

SNAPSHOT_BASE = os.path.dirname(__file__)
GCS_SNAPSHOT_DIR = "snapshots"
USEEIO_BASELINE_CACHE_SUBDIR = "useeio_baseline"

# Historical GCS / local keys written before the B_USA rename (#1150).
_B_USA_LEGACY_BASENAME = "B_USA_non_finetuned"


def load_current_snapshot(name: SnapshotName) -> pd.DataFrame:
    return load_snapshot(name, key=current_snapshot_key())


def resolve_snapshot_key() -> str:
    return get_usa_config().snapshot_version_or_git_sha


def load_configured_snapshot(name: SnapshotName) -> pd.DataFrame:
    return load_snapshot(name, key=resolve_snapshot_key())


def load_snapshot(name: SnapshotName, key: str) -> pd.DataFrame:
    path = download_snapshot(name, key=key)
    return pd.read_parquet(path)


def download_snapshot(name: SnapshotName, key: str) -> str:
    """Ensure ``name`` is local for ``key``; return the path to read.

    For ``B_USA``, prefer ``B_USA.parquet``, else fall back to the legacy
    ``B_USA_non_finetuned.parquet`` (local, then GCS).
    """
    local_dir = snapshot_local_dir(key=key)
    gcs_dir = snapshot_gcs_dir(key=key)

    if name == "B_USA":
        return _download_b_usa(local_dir, gcs_dir)

    local_pth = os.path.join(local_dir, f"{name}.parquet")
    download_gcs_file_if_not_exists(f"{name}.parquet", gcs_dir, local_pth)
    return local_pth


def _download_b_usa(local_dir: str, gcs_dir: str) -> str:
    preferred = os.path.join(local_dir, "B_USA.parquet")
    legacy = os.path.join(local_dir, f"{_B_USA_LEGACY_BASENAME}.parquet")

    if os.path.exists(preferred):
        return preferred
    if os.path.exists(legacy):
        return legacy

    download_gcs_file_if_not_exists("B_USA.parquet", gcs_dir, preferred)
    if os.path.exists(preferred):
        return preferred

    download_gcs_file_if_not_exists(
        f"{_B_USA_LEGACY_BASENAME}.parquet", gcs_dir, legacy
    )
    if os.path.exists(legacy):
        return legacy

    raise FileNotFoundError(
        f"Neither B_USA.parquet nor {_B_USA_LEGACY_BASENAME}.parquet "
        f"found under {local_dir} or gs://cornerstone-default/{gcs_dir}/"
    )


def current_snapshot_key() -> str:
    with open(os.path.join(SNAPSHOT_BASE, ".SNAPSHOT_KEY")) as f:
        return f.read().strip()


def snapshot_local_dir(key: str) -> str:
    snapshot_dir = os.path.join(SNAPSHOT_BASE, "data", key)
    os.makedirs(snapshot_dir, exist_ok=True)
    return snapshot_dir


def useeio_baseline_local_dir() -> str:
    """Local cache dir for USEEIO Excel baselines (under ``snapshots/data``, like parquet snapshots)."""
    d = os.path.join(SNAPSHOT_BASE, "data", USEEIO_BASELINE_CACHE_SUBDIR)
    os.makedirs(d, exist_ok=True)
    return d


def snapshot_gcs_dir(key: str) -> str:
    return posixpath.join(GCS_SNAPSHOT_DIR, key)
