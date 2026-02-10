"""Unit tests for the scrap correction in derived_2017.py."""

import numpy as np
import pandas as pd


def test_divide_axis0_equals_diag_premultiply() -> None:
    """
    Validate that Vnorm.divide(d, axis=0) is mathematically equivalent to
    np.diag(1/d) @ Vnorm (pre-multiplying by the inverse diagonal matrix).

    This confirms the scrap correction formula:
        V_scrap_corrected = Vnorm.divide(1.0 - (scrap_faction / q))
    is equivalent to:
        V_scrap_corrected = diag((1 - scrap_faction/q)^-1) @ Vnorm
    """
    # Create a 3x3 V matrix
    Vnorm = pd.DataFrame(
        [
            [1.0, 2.0, 3.0],
            [4.0, 5.0, 6.0],
            [7.0, 8.0, 9.0],
        ],
        index=[
            "ind1",
            "ind2",
            "ind3",
        ],
        columns=[
            "com1",
            "com2",
            "com3",
        ],
    )

    # Divisor vector (simulating 1.0 - scrap_faction/q, values between 0 and 1)
    d = pd.Series(
        [
            0.9,
            0.8,
            0.95,
        ],
        index=[
            "ind1",
            "ind2",
            "ind3",
        ],
    )

    # Method 1: pandas divide along axis=0 (current implementation)
    result_divide = Vnorm.divide(d, axis=0)

    # Method 2: pre-multiply by diagonal inverse
    result_diag = pd.DataFrame(
        np.diag(1 / d) @ Vnorm.values,
        index=Vnorm.index,
        columns=Vnorm.columns,
    )

    pd.testing.assert_frame_equal(result_divide, result_diag)
