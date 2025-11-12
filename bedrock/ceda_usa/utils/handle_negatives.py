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
    """
    df[df < 0] = 0
    return df


def handle_negative_vector_values(ser: pd.Series[float]) -> pd.Series[float]:
    """
    There are negative values in vector, set them to 0
    """
    ser[ser < 0] = 0
    return ser
