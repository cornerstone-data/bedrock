import functools
import logging
import os
import ssl
import typing as ta
import uuid

import google.auth
import google.auth.credentials
import google.cloud.storage
import googleapiclient
import googleapiclient.discovery
import pandas as pd
import tenacity
from google.cloud.storage.blob import Blob

logger = logging.getLogger(__name__)

GCS_CEDA_USA_DIR = "gs://cornerstone-default/ceda-usa"
GCS_CEDA_INPUT_DIR = os.path.join(GCS_CEDA_USA_DIR, "input")
GCS_CEDA_V5_INPUT_DIR = os.path.join(GCS_CEDA_INPUT_DIR, "v5")


def download_gcs_file_if_not_exists(gs_url: str, pth: str) -> None:
    os.makedirs(os.path.dirname(pth), exist_ok=True)
    if os.path.exists(pth):
        return
    download_gcs_file(gs_url, pth)


def load_from_gcs(
    gs_url: str,
    local_dir: str,
    loader: ta.Callable[[str], pd.DataFrame],
    overwrite: bool = False,
) -> pd.DataFrame:
    """simple loader that downloads with the same name as the gcs file"""
    pth = os.path.join(local_dir, gs_url.split("/")[-1])
    if overwrite:
        download_gcs_file(gs_url, pth)
    else:
        download_gcs_file_if_not_exists(gs_url, pth)
    return loader(pth)


@tenacity.retry(
    stop=tenacity.stop_after_attempt(3),
    wait=tenacity.wait_fixed(2),
    # during CI we are intermittently getting...
    # ssl.SSLEOFError: EOF occurred in violation of protocol (_ssl.c:2427)
    # unclear why, but perhaps retries will help
    retry=tenacity.retry_if_exception_type(ssl.SSLEOFError),
)
def download_gcs_file(gs_url: str, pth: str) -> None:
    client = __storage_client()
    logger.debug(f"Downloading `{gs_url}` to `{pth}`.")

    tmp_pth = f"{pth}.{uuid.uuid4().hex}.tmp"
    blob = Blob.from_string(gs_url, client=client)
    blob.download_to_filename(tmp_pth)

    os.rename(tmp_pth, pth)
    logger.info(f"Downloaded `{gs_url}` to `{pth}`.")


def upload_file_to_gcs(pth: str, gs_url: str) -> None:
    client = __storage_client()
    logger.debug(f"Uploading `{pth}` to `{gs_url}`.")

    blob = Blob.from_string(gs_url, client=client)
    blob.upload_from_filename(pth)
    logger.info(f"Uploaded `{pth}` to `{gs_url}`.")


def gcs_path_exists(gs_url: str) -> bool:
    client = __storage_client()
    splat = gs_url.split("gs://")[1].split("/")
    bucket = client.bucket(splat[0])
    return Blob(bucket=bucket, name="/".join(splat[1:])).exists(client)


@functools.cache
def __storage_client() -> googleapiclient.discovery.Resource:
    credentials, _ = __credentials()

    return google.cloud.storage.Client(credentials=credentials)


@functools.cache
def __credentials() -> ta.Tuple[google.auth.credentials.Credentials, ta.Any]:
    credentials, project_id = google.auth.default(
        scopes=[
            "https://www.googleapis.com/auth/cloud-platform",
        ]
    )
    return credentials, project_id
