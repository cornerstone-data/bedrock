from __future__ import annotations

import logging
import os

import pandas as pd

logger = logging.getLogger(__name__)


IN_DIR = os.path.join(os.path.dirname(__file__), "input_data")


def handle_negative_matrix_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    There are negative values in U or A matrix, set them to 0
    TODO(mo.li) some reason why this might be

    Returns a copy. Inputs are often cached frames (``derive_U_with_negatives``),
    so clipping in place would change what later callers read (#1270).
    """
    out = df.copy()
    out[out < 0] = 0
    return out


def handle_negative_vector_values(ser: pd.Series[float]) -> pd.Series[float]:
    """
    There are negative values in vector, set them to 0

    Returns a copy, as ``handle_negative_matrix_values`` does.
    """
    out = ser.copy()
    out[out < 0] = 0
    return out
