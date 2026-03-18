import os
from unittest.mock import patch

import pandas as pd

from bedrock.utils.io.gcp import download_gcs_file, get_most_recent_from_bucket


def test_download_gcs_file() -> None:
    tmp_path = './test_file.csv'

    try:
        download_gcs_file("example.csv", "examples", tmp_path)
        assert os.path.exists(tmp_path)
        assert os.path.getsize(tmp_path) > 0
    finally:
        os.remove(tmp_path)


def test_get_most_recent_from_bucket_ignores_snapshot_folder_sha() -> None:
    df = pd.DataFrame(
        [
            {
                "full_path": "snapshots/a0e514e361bb0a87cd40b68b1eccc8bb911fa35c/B_USA_non_finetuned.parquet",
                "created": pd.Timestamp("2026-03-10T13:19:19Z"),
                "extension": ".parquet",
                "version": None,
                "hash": None,
                "base_name": "B_USA_non_finetuned",
                "filename": "B_USA_non_finetuned.parquet",
                "year": None,
            }
        ]
    )

    with patch("bedrock.utils.io.gcp.list_bucket_files", return_value=df):
        result = get_most_recent_from_bucket(
            "B_USA_non_finetuned.parquet",
            "snapshots/a0e514e361bb0a87cd40b68b1eccc8bb911fa35c",
        )

    assert result == ["B_USA_non_finetuned.parquet"]
