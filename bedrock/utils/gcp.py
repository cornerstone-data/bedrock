import os
import re

import pandas as pd

from bedrock.ceda_usa.utils.gcp import __storage_client


def list_bucket_files(sub_bucket: str = "") -> pd.DataFrame:
    """
    List all files in the GCS bucket and return a DataFrame
    with columns: full_path, last_modified, created, extension,
    version, hash, base_name, filename, year.

    Parameters
    ----------
    sub_bucket : str
        folder path to limit query

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


def get_most_recent_from_bucket(sub_bucket, name, extension):
    """
    Sorts the bucket by most recent date for the required extension
    and returns the matching files of that name that share the same version
    and hash

    Parameters
    ----------
    sub_bucket : str
        folder path to limit
    name : str
        target file name without version or hash
    extension : str
        extension of file, e.g., ".parquet"

    Returns
    ----------
    list
        Most recently created datafiles, metadata, log files
    """
    # sub_bucket='flowsa/FlowByActivity'
    # name='BEA_Detail_GrossOutput_IO_2021'
    # extension='.parquet'
    df = list_bucket_files(sub_bucket)
    if df is None:
        return None

    # subset using "file_name" instead of "name" to work when a user
    # includes a GitHub version and hash
    df = df[df['base_name'] == name]
    df_ext = df[df['extension'] == extension]
    if len(df_ext) == 0:
        return None
    else:
        df_ext = (df_ext.sort_values(by=["version", "created"], ascending=False)
                  .reset_index(drop=True))
        # select first file name in list, extract the file version and git
        # hash, return list of files that include version/hash (to include
        # metadata and log files)
        recent_file = df_ext['filename'][0]
        vh = str(df_ext.iloc[0]['version']) + '_' + str(df_ext.iloc[0]['hash'])
        if vh != '':
            selected_files = [string for string in df['filename'] if vh in string]
        else:
            selected_files = [recent_file]
        return selected_files
