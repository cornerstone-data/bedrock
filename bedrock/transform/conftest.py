from __future__ import annotations

import pandas as pd
import pytest

from bedrock.utils.snapshots.loader import load_current_snapshot


@pytest.fixture(autouse=True)
def download_fba_on_api_error_for_eeio_tests(request: pytest.FixtureRequest) -> None:
    """When running eeio_integration tests, download FBA from GCS if API key is missing."""
    if request.node.get_closest_marker("eeio_integration"):
        import bedrock.utils.config.common as common

        common.download_fba_on_api_error = True


# snapshots
@pytest.fixture(scope="session")
def E_usa_es_snapshot() -> pd.DataFrame:
    return load_current_snapshot("E_USA_ES")


@pytest.fixture(scope="session")
def b_usa_non_finetuned_snapshot() -> pd.DataFrame:
    return load_current_snapshot("B_USA_non_finetuned")


@pytest.fixture(scope="session")
def adom_usa_snapshot() -> pd.DataFrame:
    return load_current_snapshot("Adom_USA")


@pytest.fixture(scope="session")
def aimp_usa_snapshot() -> pd.DataFrame:
    return load_current_snapshot("Aimp_USA")


@pytest.fixture(scope="session")
def scaled_q_usa_snapshot() -> pd.Series[float]:
    df = load_current_snapshot("scaled_q_USA")
    squeezed = df.squeeze()
    assert isinstance(
        squeezed, pd.Series
    ), f"Expected Series after squeeze, got {type(squeezed)}"
    return squeezed


@pytest.fixture(scope="session")
def y_nab_usa_snapshot() -> pd.Series[float]:
    df = load_current_snapshot("y_nab_USA")
    squeezed = df.squeeze()
    assert isinstance(
        squeezed, pd.Series
    ), f"Expected Series after squeeze, got {type(squeezed)}"
    return squeezed


@pytest.fixture(scope="session")
def ytot_usa_snapshot() -> pd.Series[float]:
    df = load_current_snapshot("ytot_USA")
    squeezed = df.squeeze()
    assert isinstance(
        squeezed, pd.Series
    ), f"Expected Series after squeeze, got {type(squeezed)}"
    return squeezed


@pytest.fixture(scope="session")
def exports_usa_snapshot() -> pd.Series[float]:
    df = load_current_snapshot("exports_USA")
    squeezed = df.squeeze()
    assert isinstance(
        squeezed, pd.Series
    ), f"Expected Series after squeeze, got {type(squeezed)}"
    return squeezed


@pytest.fixture(scope="session")
def ydom_usa_snapshot() -> pd.Series[float]:
    df = load_current_snapshot("ydom_USA")
    squeezed = df.squeeze()
    assert isinstance(
        squeezed, pd.Series
    ), f"Expected Series after squeeze, got {type(squeezed)}"
    return squeezed


@pytest.fixture(scope="session")
def yimp_usa_snapshot() -> pd.Series[float]:
    df = load_current_snapshot("yimp_USA")
    squeezed = df.squeeze()
    assert isinstance(
        squeezed, pd.Series
    ), f"Expected Series after squeeze, got {type(squeezed)}"
    return squeezed
