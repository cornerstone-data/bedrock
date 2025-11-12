import os
import posixpath

import pandas as pd

from bedrock.ceda_usa.utils.gcp import GCS_CEDA_USA_DIR, download_gcs_file_if_not_exists
from bedrock.ceda_usa.utils.snapshots.names import SnapshotName

SNAPSHOT_BASE = os.path.dirname(__file__)
GCS_SNAPSHOT_DIR = posixpath.join(GCS_CEDA_USA_DIR, "snapshots")


def load_current_snapshot(name: SnapshotName) -> pd.DataFrame:
    return load_snapshot(name, key=current_snapshot_key())


def load_snapshot(name: SnapshotName, key: str) -> pd.DataFrame:
    download_snapshot(name, key=key)
    return pd.read_parquet(os.path.join(snapshot_local_dir(key=key), f"{name}.parquet"))


def download_snapshot(name: SnapshotName, key: str) -> None:
    local_pth = os.path.join(snapshot_local_dir(key=key), f"{name}.parquet")
    gs_url = posixpath.join(snapshot_gcs_dir(key=key), f"{name}.parquet")
    download_gcs_file_if_not_exists(gs_url, local_pth)


def current_snapshot_key() -> str:
    with open(os.path.join(SNAPSHOT_BASE, ".SNAPSHOT_KEY")) as f:
        return f.read()


def snapshot_local_dir(key: str) -> str:
    snapshot_dir = os.path.join(SNAPSHOT_BASE, "data", key)
    os.makedirs(snapshot_dir, exist_ok=True)
    return snapshot_dir


def snapshot_gcs_dir(key: str) -> str:
    return posixpath.join(GCS_SNAPSHOT_DIR, key)
