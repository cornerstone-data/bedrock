"""The negative-value handlers clip to zero and leave their input unchanged (#1270)."""

from __future__ import annotations

import pandas as pd

from bedrock.utils.math.handle_negatives import (
    handle_negative_matrix_values,
    handle_negative_vector_values,
)


def test_matrix_negatives_are_zero_and_the_input_keeps_them() -> None:
    df = pd.DataFrame([[1.0, -2.0], [-3.0, 4.0]], index=['a', 'b'], columns=['a', 'b'])
    out = handle_negative_matrix_values(df)
    pd.testing.assert_frame_equal(
        out,
        pd.DataFrame([[1.0, 0.0], [0.0, 4.0]], index=['a', 'b'], columns=['a', 'b']),
    )
    assert df.loc['a', 'b'] == -2.0
    assert df.loc['b', 'a'] == -3.0


def test_vector_negatives_are_zero_and_the_input_keeps_them() -> None:
    ser = pd.Series([1.0, -2.0, float('nan')], index=['a', 'b', 'c'])
    out = handle_negative_vector_values(ser)
    pd.testing.assert_series_equal(
        out, pd.Series([1.0, 0.0, float('nan')], index=['a', 'b', 'c'])
    )
    assert ser['b'] == -2.0
