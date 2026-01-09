from __future__ import annotations

import pandas as pd
import pytest

from bedrock.ceda_usa.utils.snapshots.loader import load_current_snapshot


@pytest.fixture(scope="session")
def E_usa_es_snapshot() -> pd.DataFrame:
    return load_current_snapshot("E_USA_ES")
