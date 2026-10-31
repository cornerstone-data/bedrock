import numpy as np
import pandas as pd

from bedrock.extract.bea.BEA_IEA import interpolate_suppressed

NAN = np.nan


def _filled(values: list[float]) -> np.ndarray:
    panel = pd.DataFrame(
        [values], index=['leaf'], columns=range(2019, 2019 + len(values))
    )
    return interpolate_suppressed(panel).to_numpy()[0]


def test_interior_gap_is_geometric_between_positive_endpoints() -> None:
    np.testing.assert_allclose(
        _filled([100.0, NAN, NAN, 800.0]), [100.0, 200.0, 400.0, 800.0]
    )


def test_interior_gap_with_a_zero_endpoint_is_linear() -> None:
    np.testing.assert_allclose(_filled([30.0, NAN, 0.0]), [30.0, 15.0, 0.0])


def test_edge_gaps_and_unobserved_rows_stay_suppressed() -> None:
    np.testing.assert_allclose(_filled([NAN, 5.0, 6.0, NAN]), [NAN, 5.0, 6.0, NAN])
    assert np.isnan(_filled([NAN, NAN, NAN])).all()


def test_observed_cells_are_unchanged() -> None:
    out = _filled([3.0, NAN, 24.0, 20.0, 8.0])
    assert out[[0, 2, 3, 4]].tolist() == [3.0, 24.0, 20.0, 8.0]
    np.testing.assert_allclose(out[1], (3.0 * 24.0) ** 0.5)
