import os
import tempfile

from ceda_usa.utils.gcp import download_gcs_file


def test_download_gcs_file() -> None:
    fd, tmp_path = tempfile.mkstemp()

    # Close immediately so Windows can reopen it to write
    os.close(fd)
    try:
        download_gcs_file("gs://cornerstone-default/examples/example.csv", tmp_path)
        assert os.path.exists(tmp_path)
        assert os.path.getsize(tmp_path) > 0
    finally:
        os.unlink(tmp_path)
