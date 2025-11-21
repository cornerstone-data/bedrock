import os

from bedrock.utils.gcp import download_gcs_file


def test_download_gcs_file() -> None:
    tmp_path = './test_file.csv'

    try:
        download_gcs_file("example.csv", "examples", tmp_path)
        assert os.path.exists(tmp_path)
        assert os.path.getsize(tmp_path) > 0
    finally:
        os.remove(tmp_path)
