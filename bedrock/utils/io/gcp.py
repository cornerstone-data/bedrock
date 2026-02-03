import functools
import logging
import os
import posixpath
import re
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
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)

GCS_CORNERSTONE = "gs://cornerstone-default"


def download_gcs_file_if_not_exists(name: str, sub_bucket: str, pth: str) -> None:
    """
    Download a file from Google Cloud Storage (GCS) if it does not already exist
    locally. This will also download any associated metadata files (e.g.,
    _metadta.json) or log files.

    Parameters
    ----------
    name : str
        Target file name with extension, but without version or hash
    sub_bucket : str
        Subdirectory within the GCS bucket.
    pth : str
        Local file path where the file should be saved, including extension.
    """

    os.makedirs(os.path.dirname(pth), exist_ok=True)
    if os.path.exists(pth):
        return
    [
        download_gcs_file(n, sub_bucket, pth)
        for n in get_most_recent_from_bucket(name, sub_bucket)
    ]


def load_from_gcs(
    name: str,
    sub_bucket: str,
    local_dir: str,
    loader: ta.Callable[[str], pd.DataFrame],
    overwrite: bool = False,
) -> pd.DataFrame:
    """
    Download a file from GCS and load it into a DataFrame using a custom loader.

    Parameters
    ----------
    name : str
        Target file name with extension, but without version or hash
    sub_bucket : str
        Subdirectory within the GCS bucket.
    local_dir : str
        Local directory to store the downloaded file.
    loader : Callable[[str], pandas.DataFrame]
        Function that takes a file path and returns a DataFrame.
    overwrite : bool, optional
        If True, forces re-download even if the file exists locally. Default is False.

    Returns
    -------
    pandas.DataFrame
        DataFrame loaded from the downloaded file.

    """
    pth = os.path.join(local_dir, name)
    if overwrite:
        [
            download_gcs_file(n, sub_bucket, pth)
            for n in get_most_recent_from_bucket(name, sub_bucket)
        ]
    else:
        download_gcs_file_if_not_exists(name, sub_bucket, pth)
    return loader(pth)


@tenacity.retry(
    stop=tenacity.stop_after_attempt(3),
    wait=tenacity.wait_fixed(2),
    # during CI we are intermittently getting...
    # ssl.SSLEOFError: EOF occurred in violation of protocol (_ssl.c:2427)
    # unclear why, but perhaps retries will help
    retry=tenacity.retry_if_exception_type(ssl.SSLEOFError),
)
def download_gcs_file(name: str, sub_bucket: str, pth: str) -> None:
    """
    Download a file from GCS to a local path with retry logic.

    Parameters
    ----------
    name : str
        Name of the file in GCS.
    sub_bucket : str
        Subdirectory within the GCS bucket.
    pth : str
        Local file path where the file should be saved.

    Notes
    -----
    Retries up to 3 times in case of SSL errors.
    """
    client = __storage_client()
    gs_url = posixpath.join(GCS_CORNERSTONE, sub_bucket, name)

    logger.debug(f"Downloading `{gs_url}` to `{pth}`.")

    os.makedirs(os.path.dirname(pth), exist_ok=True)
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

    return google.cloud.storage.Client(
        project='cornerstone-data',
        credentials=credentials,
    )


@functools.cache
def __credentials() -> ta.Tuple[google.auth.credentials.Credentials, ta.Any]:
    credentials, project_id = google.auth.default(
        scopes=[
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/spreadsheets",
        ]
    )
    return credentials, project_id


@functools.cache
def __sheets_client() -> googleapiclient.discovery.Resource:
    credentials, _ = __credentials()
    return googleapiclient.discovery.build('sheets', 'v4', credentials=credentials)


def update_sheet_tab(
    sheet_id: str,
    tab: str,
    data: pd.DataFrame,
    clean_nans: bool = False,
) -> None:
    """
    Write a DataFrame to a Google Sheets tab, creating the tab if needed.

    Args:
        sheet_id: The Google Sheets document ID
        tab: The name of the tab to write to
        data: The DataFrame to write
        clean_nans: If True, replace NaN values with None
    """
    logger.info(f'updating data "{sheet_id}:{tab}"')
    client = __sheets_client()

    values = data.values.tolist()
    if clean_nans:
        clean_values = []
        for row in values:
            clean_row = [None if pd.isna(val) else val for val in row]
            clean_values.append(clean_row)
        values = clean_values

    try:
        sheet_metadata = client.spreadsheets().get(spreadsheetId=sheet_id).execute()
        sheet_exists = any(
            sheet["properties"]["title"] == tab
            for sheet in sheet_metadata.get("sheets", [])
        )
    except HttpError:
        sheet_exists = False

    # If the sheet doesn't exist, create it
    if not sheet_exists:
        request_body = {"requests": [{"addSheet": {"properties": {"title": tab}}}]}
        client.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id, body=request_body
        ).execute()

    data_range = f"'{tab}'"  # the whole tab

    client.spreadsheets().values().clear(
        spreadsheetId=sheet_id, range=data_range
    ).execute()
    client.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=data_range,
        valueInputOption="RAW",
        body={"values": [data.columns.tolist()] + values},
    ).execute()


def list_bucket_files(sub_bucket: str = "") -> pd.DataFrame:
    """
    List all files in the GCS bucket and return a DataFrame
    with columns: full_path, last_modified, created, extension,
    version, hash, base_name, filename, year.

    Parameters
    ----------
    sub_bucket : str
        Subdirectory within the GCS bucket to filter files. Default is "" (all files).

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns:
        - full_path : str
        - last_modified : datetime
        - created : datetime
        - extension : str
        - version : str or None
        - hash : str or None
        - base_name : str
        - filename : str
        - year : str or None

    Examples
    ----------
    >>> list_bucket_files("flowsa/FlowByActivity")
    """
    client = __storage_client()
    bucket = client.bucket("cornerstone-default")
    blobs = bucket.list_blobs()

    file_data = []
    for blob in blobs:
        file_name = blob.name
        last_modified = blob.updated  # datetime object
        created = blob.time_created
        extension = os.path.splitext(file_name)[1]  # includes dot, e.g. '.csv'
        file_data.append(
            {
                "full_path": file_name,
                "last_modified": last_modified,
                "created": created,
                "extension": extension if extension else "",
            }
        )

    # Convert to DataFrame
    df = pd.DataFrame(file_data).query('extension != ""')
    df = (
        df.query('full_path.str.startswith(@sub_bucket)').reset_index(drop=True)
        if sub_bucket
        else df
    )

    version_pattern = r"v\d+\.\d+\.\d+"  # Matches v#.#.#
    hash_pattern = r"([a-fA-F0-9]{7})"  # Matches 7-character alphanumeric hash
    year_pattern = r"(\d{4})$"  # Matches 4 digits at the end of base_name

    def extract_base_name(full_path: str) -> str:
        if not isinstance(full_path, str):
            return None
        # Get last part of path
        name_part = re.split(r"/", full_path)[-1]
        # Check for version and hash
        version_match = re.search(version_pattern, name_part)
        hash_match = re.search(hash_pattern, name_part)
        if version_match and hash_match:
            # Everything before version
            return name_part[: version_match.start()].rstrip("_")
        else:
            # Remove extension
            return ".".join(name_part.split('.')[:-1])

    df['version'] = df['full_path'].apply(
        lambda x: (
            re.search(version_pattern, x).group(0)  # type: ignore
            if isinstance(x, str) and re.search(version_pattern, x)
            else None
        )
    )
    df['hash'] = df['full_path'].apply(
        lambda x: (
            re.search(hash_pattern, x).group(0)  # type: ignore
            if isinstance(x, str) and re.search(hash_pattern, x)
            else None
        )
    )
    df['base_name'] = df['full_path'].apply(extract_base_name)
    df['filename'] = df['full_path'].apply(
        lambda x: re.split(r"/", x)[-1] if isinstance(x, str) else None
    )
    # Extract year if base_name ends with 4 digits
    df['year'] = df['base_name'].apply(
        lambda x: (
            re.search(year_pattern, x).group(0)  # type: ignore
            if isinstance(x, str) and re.search(year_pattern, x)
            else None
        )
    )
    return df


def get_most_recent_from_bucket(name: str, sub_bucket: str) -> list[str]:
    """
    Sorts the bucket by most recent date for the required extension
    and identifies the matching files of that name that share the same version
    and hash

    Parameters
    ----------
    sub_bucket : str
        Subdirectory within the GCS bucket.
    name : str
        Target file name with extension, but without version or hash

    Returns
    ----------
    list of str
        List of filenames corresponding to the most recent version and hash.
        Includes related files such as metadata and logs.

    Examples
    ----------
    >>> len(get_most_recent_from_bucket("BEA_Detail_GrossOutput_IO_2021.parquet",
                                        "flowsa/FlowByActivity"))
    2
    """
    # sub_bucket='flowsa/FlowByActivity'
    # name='BEA_Detail_GrossOutput_IO_2021.parquet'
    n, extension = os.path.splitext(name)
    df = list_bucket_files(sub_bucket)
    if df is None:
        return []

    # subset using "file_name" instead of "name" to work when a user
    # includes a GitHub version and hash
    df = df[df['base_name'] == n]
    df_ext = df[df['extension'] == extension]
    if len(df_ext) == 0:
        return []
    else:
        df_ext = df_ext.sort_values(
            by=["version", "created"], ascending=False
        ).reset_index(drop=True)
        # select first file name in list, extract the file version and git
        # hash, return list of files that include version/hash (to include
        # metadata and log files)
        first = df_ext.iloc[0]
        recent_file = first['filename']
        vh = "_".join(
            [val for val in [first['version'], first['hash']] if pd.notna(val)]
        )
        if vh != '':
            selected_files = [string for string in df['filename'] if vh in string]
        else:
            selected_files = [recent_file]
        return selected_files
