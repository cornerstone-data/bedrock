import os
from datetime import datetime, timezone
from unittest.mock import patch

import pandas as pd

from bedrock.utils.io.gcp import (
    download_gcs_file,
    get_most_recent_from_bucket,
    list_bucket_files,
)


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
                "full_path": "snapshots/2ebb51f7190c3a62b5d8b2420bff9b20f57282fc/B_USA_non_finetuned.parquet",
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
            "snapshots/2ebb51f7190c3a62b5d8b2420bff9b20f57282fc",
        )

    assert result == ["B_USA_non_finetuned.parquet"]


def test_list_bucket_files_extracts_base_name_for_v0_1_versions() -> None:
    # Ensure artifact filenames like:
    #   GHG_national_Cornerstone_2023_v0.1_4a1e550.parquet
    # produce base_name == GHG_national_Cornerstone_2023 so downloads can match.
    method = "GHG_national_Cornerstone_2023"
    parquet_name = f"flowsa/FlowBySector/{method}_v0.1_4a1e550.parquet"
    metadata_name = f"flowsa/FlowBySector/{method}_v0.1_4a1e550_metadata.json"

    class FakeBlob:
        def __init__(self, name: str) -> None:
            self.name = name
            self.updated = datetime(2026, 3, 23, tzinfo=timezone.utc)
            self.time_created = datetime(2026, 3, 23, tzinfo=timezone.utc)

    class FakeBucket:
        def list_blobs(self) -> list[FakeBlob]:
            return [FakeBlob(parquet_name), FakeBlob(metadata_name)]

    class FakeClient:
        def bucket(self, _: str) -> FakeBucket:
            return FakeBucket()

    with patch("bedrock.utils.io.gcp.__storage_client", return_value=FakeClient()):
        df = list_bucket_files("flowsa/FlowBySector")

    parquet_rows = df[df["extension"] == ".parquet"]
    assert len(parquet_rows) == 1

    row = parquet_rows.iloc[0]
    assert row["base_name"] == method
    assert row["version"] == "v0.1"
    assert row["hash"] == "4a1e550"
