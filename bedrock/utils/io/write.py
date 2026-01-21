import os

import pandas as pd
from esupy.processed_data_mgmt import FileMeta


def write_fb_to_file(df: pd.DataFrame, meta: FileMeta, pth: str) -> None:
    """
    Stores FBA as parquet within repository directory

    Parameters
    ----------
    df : pd.DataFrame
        FBA to save to parquet.
    meta: FileMeta
        metadata object for FBA
    pth: str
        path to directory
    """
    fname = f'{meta.name_data}_v{meta.tool_version}'
    if meta.git_hash is not None:
        fname = f'{fname}_{meta.git_hash}'
    os.makedirs(pth, exist_ok=True)
    df.to_parquet(f'{pth}/{fname}.parquet')
