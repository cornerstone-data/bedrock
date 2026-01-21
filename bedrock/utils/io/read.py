import logging as log
import os

import pandas as pd
from esupy.processed_data_mgmt import FileMeta


def load_preprocessed_output(file_meta: FileMeta, pth: str) -> pd.DataFrame:
    """
    Loads a preprocessed file
    :param file_meta: populated instance of class FileMeta
    :param pth: str, directory of file
    :return: a pandas dataframe of the datafile or FileNotFoundError if the directory or file does not exist
    """
    try:
        f = find_file(file_meta, pth)
    except FileNotFoundError as e:
        raise FileNotFoundError(
            f"Failed to load preprocessed output '{file_meta.name_data}': {e}"
        ) from e
    log.info(f'Returning {f}')
    df = pd.read_parquet(f)
    return df


def find_file(meta: FileMeta, pth: str) -> str:
    """
    Searches for file within pth based on file metadata; if
    metadata matches, returns most recently created file path
    :param meta: populated instance of class FileMeta
    :param pth: str, directory of file
    :return: str with the file path if found or FileNotFoundError if the directory or file does not exist
    """
    if not os.path.exists(pth):
        raise FileNotFoundError(f"Directory not found: '{pth}'")

    with os.scandir(pth) as files:
        # List all file satisfying the criteria in the passed metadata
        matches = [
            f
            for f in files
            if f.name.startswith(meta.name_data) and meta.ext.lower() in f.name.lower()
        ]
        # Sort files in reverse order by ctime (creation time on Windows,
        # last metadata modification time on Unix)
        sorted_matches = sorted(matches, key=lambda f: f.stat().st_ctime, reverse=True)

    # Return the path to the most recent matching file
    if sorted_matches:
        return f"{pth}/{sorted_matches[0].name}"

    raise FileNotFoundError(
        f"No file found matching name '{meta.name_data}' "
        f"with extension '{meta.ext}' in directory '{pth}'"
    )
