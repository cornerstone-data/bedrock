from __future__ import annotations

import pandas as pd
import pytest

from bedrock.ceda_usa.utils.snapshots.loader import load_current_snapshot


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
