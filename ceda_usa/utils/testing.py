from __future__ import annotations

import typing as ta

import pandas as pd

ATOL = 1e-6
RTOL = 0.01


def assert_snapshot_frame_equal(
    *,
    actual: pd.DataFrame,
    expected: pd.DataFrame,
    msg: ta.Optional[str] = None,
) -> None:
    assert_frame_equal(
        actual=actual,
        expected=expected,
        msg=msg,
        rtol=1e-04,
        atol=1e-08,
        check_names=False,  # TODO fixme
    )


def assert_snapshot_series_equal(
    *,
    actual: pd.Series[float],
    expected: pd.Series[float],
    msg: ta.Optional[str] = None,
) -> None:
    assert_series_equal(
        actual=actual,
        expected=expected,
        msg=msg,
        rtol=1e-04,
        atol=1e-08,
        # TODO add check_names=True param to function call
    )


def assert_frame_equal(
    *,
    actual: pd.DataFrame,
    expected: pd.DataFrame,
    msg: ta.Optional[str] = None,
    rtol: float = RTOL,
    atol: float = ATOL,
    **kwargs: ta.Any,
) -> None:
    try:
        pd.testing.assert_frame_equal(
            actual.sort_index(axis=0).sort_index(axis=1),
            expected.sort_index(axis=0).sort_index(axis=1),
            atol=atol,
            rtol=rtol,
            **kwargs,
        )
    except AssertionError as e:
        if "values are different" in str(e):
            diagnostics = _produce_frame_diagnostics(
                actual, expected, msg=msg, atol=atol, rtol=rtol
            )
            raise AssertionError(diagnostics) from e
        raise e


def _produce_frame_diagnostics(
    actual: pd.DataFrame,
    expected: pd.DataFrame,
    msg: ta.Optional[str],
    atol: float,
    rtol: float,
) -> str:
    def _stack_to_series(df: pd.DataFrame) -> pd.Series[float]:
        while isinstance(df, pd.DataFrame):
            df = df.stack()  # type: ignore
        return df  # now a series

    return _produce_series_diagnostics(
        actual=_stack_to_series(actual),
        expected=_stack_to_series(expected),
        msg=msg,
        atol=atol,
        rtol=rtol,
    )


def assert_series_equal(
    *,
    actual: pd.Series[float],
    expected: pd.Series[float],
    msg: ta.Optional[str],
    rtol: float = RTOL,
    atol: float = ATOL,
    **kwargs: ta.Any,
) -> None:
    if "check_names" in kwargs:
        raise RuntimeError("check_names is not allowed in assert_series_equal")
    assert isinstance(actual, pd.Series)
    assert isinstance(expected, pd.Series)

    assert not actual.isna().any(), "found NAs in actual series"
    assert not expected.isna().any(), "found NAs in expected series"

    actual = actual.sort_index()
    expected = expected.sort_index()

    try:
        pd.testing.assert_series_equal(
            actual,
            expected,
            atol=atol,
            rtol=rtol,
            check_names=False,
            **kwargs,
        )
    except AssertionError as e:
        if "Series values are different" in str(e):
            diagnostics = _produce_series_diagnostics(
                actual, expected, msg=msg, atol=atol, rtol=rtol
            )
            raise AssertionError(diagnostics) from e
        raise e


def _produce_series_diagnostics(
    actual: pd.Series[float],
    expected: pd.Series[float],
    msg: ta.Optional[str],
    atol: float,
    rtol: float,
) -> str:
    abs = (actual - expected).abs()
    exp = expected.abs()
    pct = abs / exp
    ub = atol + rtol * exp

    diff = abs - ub
    oob = (diff > 0).mean()
    worst_idx = diff.idxmax()
    return (
        f"{msg} actual {actual.loc[worst_idx]:0.10f} expected {expected.loc[worst_idx]:0.10f} @ {worst_idx} | "
        f"abs {abs[worst_idx]:0.4f} pct {pct[worst_idx]:0.4f} ub {ub[worst_idx]:0.4f} — oob {oob:0.4f}"
    )
