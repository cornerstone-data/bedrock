"""Cache useeio_nowcast A parquets for the time-series analysis (Step N1).

Loops over ``USEEIO_NOWCAST_YEARS`` (2017–2023), calls the transform-layer
function for each year, and writes parquet caches in the same layout that
``_loaders.load_a_pair`` expects.

Loaders live in `bedrock.extract.iot.useeio_nowcast`; derivation lives in
`bedrock.transform.eeio.derived_useeio_nowcast`. This file is the
analysis-side driver only.

CLI:
    python -m bedrock.analysis.a_matrix_time_series.derive_useeio_nowcast_A
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

from bedrock.analysis.a_matrix_time_series.constants import RESULTS_DIR
from bedrock.extract.iot.useeio_nowcast import USEEIO_NOWCAST_YEARS
from bedrock.transform.eeio.derived_useeio_nowcast import (
    derive_useeio_nowcast_Aq_cornerstone,
)
from bedrock.utils.schemas.single_region_types import SingleRegionAqMatrixSet

logger = logging.getLogger(__name__)


def _write_parquets(year: int, aq: SingleRegionAqMatrixSet) -> tuple[Path, Path]:
    """Stack (Adom, Aimp) by ``dom_or_imp`` and write to parquet."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    a_path = RESULTS_DIR / f"A_useeio_nowcast_{year}.parquet"
    q_path = RESULTS_DIR / f"q_useeio_nowcast_{year}.parquet"

    combined = pd.concat(
        {"dom": pd.DataFrame(aq.Adom), "imp": pd.DataFrame(aq.Aimp)},
        axis=0,
        names=["dom_or_imp"],
    )
    combined.to_parquet(a_path)
    aq.scaled_q.to_frame("q").to_parquet(q_path)
    return a_path, q_path


def _summary_row(
    year: int, kind: str, df: pd.DataFrame, file_path: Path
) -> dict[str, object]:
    arr = df.to_numpy()
    return {
        "approach": "useeio_nowcast",
        "year": year,
        "matrix_kind": kind,
        "n_rows": int(df.shape[0]),
        "n_cols": int(df.shape[1]),
        "nan_count": int(np.isnan(arr).sum()),
        "neg_count": int((arr < 0).sum()),
        "max_col_sum": float(np.nansum(arr, axis=0).max()) if arr.size else 0.0,
        "file_size_bytes": int(file_path.stat().st_size),
    }


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    summary_rows: list[dict[str, object]] = []
    for year in USEEIO_NOWCAST_YEARS:
        logger.info("Deriving useeio_nowcast A for %d ...", year)
        derive_useeio_nowcast_Aq_cornerstone.cache_clear()
        aq = derive_useeio_nowcast_Aq_cornerstone(year)
        a_path, _ = _write_parquets(year, aq)
        summary_rows.append(_summary_row(year, "dom", pd.DataFrame(aq.Adom), a_path))
        summary_rows.append(_summary_row(year, "imp", pd.DataFrame(aq.Aimp), a_path))

    summary_df = pd.DataFrame(summary_rows)
    summary_path = RESULTS_DIR / "cache_summary_useeio_nowcast.csv"
    summary_df.to_csv(summary_path, index=False)
    logger.info("Wrote %d parquet pairs + %s", len(USEEIO_NOWCAST_YEARS), summary_path)


if __name__ == "__main__":
    main()
