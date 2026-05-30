"""Shared parquet loaders for analysis scripts in this package."""

from __future__ import annotations

import pandas as pd

from bedrock.analysis.a_matrix_time_series.constants import RESULTS_DIR


def load_a_pair(approach: str, year: int) -> dict[str, pd.DataFrame]:
    """Load (Adom, Aimp) from ``A_{approach}_{year}.parquet``.

    Returns ``{"dom": Adom, "imp": Aimp}`` so callers can iterate over the
    two kinds with a single key.
    """
    combined = pd.read_parquet(RESULTS_DIR / f"A_{approach}_{year}.parquet")
    return {
        "dom": pd.DataFrame(combined.loc["dom"]),
        "imp": pd.DataFrame(combined.loc["imp"]),
    }
