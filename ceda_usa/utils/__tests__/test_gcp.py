import os
import tempfile

from ceda_usa.utils.gcp import download_gcs_file


def test_download_gcs_file() -> None:
    # TODO set up cornerstone bucket
    tmpfile = './testfile.csv'
    try:
        download_gcs_file("gs://cornerstone-default/examples/example.csv", tmpfile)
        assert os.path.exists(tmpfile)
        assert os.path.getsize(tmpfile) > 0
    finally:
        os.unlink(tmpfile)
