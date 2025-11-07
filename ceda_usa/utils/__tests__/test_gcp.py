import os
import tempfile

from ceda_usa.utils.gcp import download_gcs_file


def test_download_gcs_file() -> None:
    # TODO set up cornerstone bucket
    tmpfile = tempfile.NamedTemporaryFile(delete=False)
    try:
        download_gcs_file("gs://cornerstone-default/examples/example.csv", tmpfile.name)
        assert os.path.exists(tmpfile.name)
        assert os.path.getsize(tmpfile.name) > 0
    finally:
        os.unlink(tmpfile.name)
