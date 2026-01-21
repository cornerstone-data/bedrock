import logging as log
import os

import pandas as pd
from esupy.processed_data_mgmt import FileMeta


def load_preprocessed_output(file_meta: FileMeta, pth: str) -> pd.DataFrame | None:
    """
    Loads a preprocessed file
    :param file_meta: populated instance of class FileMeta
    :param paths: instance of class Paths
    :return: a pandas dataframe of the datafile if exists or None if it
        doesn't exist
    """
    f = find_file(file_meta, pth)
    if isinstance(f, str):
        log.info(f'Returning {f}')
        df = pd.read_parquet(f)
        return df
    else:
        return None


def find_file(meta: FileMeta, pth: str) -> str | None:
    """
    Searches for file within path.local_path based on file metadata; if
    metadata matches, returns most recently created file path object
    :param meta: populated instance of class FileMeta
    :param paths: populated instance of class Paths
    :return: str with the file path if found, otherwise an empty string
    """
    if os.path.exists(pth):
        with os.scandir(pth) as files:
            # List all file satisfying the criteria in the passed metadata
            matches = [
                f
                for f in files
                if f.name.startswith(meta.name_data)
                and meta.ext.lower() in f.name.lower()
            ]
            # Sort files in reverse order by ctime (creation time on Windows,
            # last metadata modification time on Unix)
            sorted_matches = sorted(
                matches, key=lambda f: f.stat().st_ctime, reverse=True
            )
        # Return the path to the most recent matching file, or '' if no
        # match exists.
        if sorted_matches:
            return f"{pth}/{sorted_matches[0].name}"
    return None
