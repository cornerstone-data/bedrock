"""USEEIO nowcasted detail MUTs (V, U total, U imports) for 2017–2023.

Files were produced by the upstream USEEIO/useeior nowcasting team
(``cornerstone-data/USEEIO`` @ ``nowcasting`` branch, commit 2025-09-30) and
staged to GCS by the bedrock team. See:

- [USEEIO_nowcasting.md](../../../../USEEIO_nowcasting.md) — methodology, what the nowcast does and doesn't capture
- [bedrock/analysis/a_matrix_time_series/docs/implement_useeio_nowcast_plan.md](../../analysis/a_matrix_time_series/docs/implement_useeio_nowcast_plan.md) — integration plan

Source CSVs at ``gs://cornerstone-default/extract/input-data/USEEIO_nowcasted_MUTs/``:

- ``V_out_{yr}.csv`` — Make table, **commodity × industry** (BEA 2017 Detail
  schema, bare codes; mUSD)
- ``U_out_{yr}.csv`` — full Use table, **commodity+VA × {industry + FD + VA}**
  (last 3 rows are VA: V00100/V00200/V00300; FD columns vary by year)
- ``U_imports_out_{yr}.csv`` — imports portion of Use, **commodity × {industry + FD}**

All values in millions of USD. Codes are bare (no ``/US`` suffix).
"""

from __future__ import annotations

import functools

import pandas as pd

from bedrock.utils.io.gcp import load_from_gcs
from bedrock.utils.io.local_extract_input_data import local_dir_for_gcs_sub_bucket

GCS_USEEIO_NOWCAST_DIR = "extract/input-data/USEEIO_nowcasted_MUTs"
LOCAL_USEEIO_NOWCAST_DIR = local_dir_for_gcs_sub_bucket(GCS_USEEIO_NOWCAST_DIR)

# Years for which upstream USEEIO has produced nowcasted tables. 2024 is NOT
# in this set — upstream has not run the pipeline for that year yet.
USEEIO_NOWCAST_YEARS: tuple[int, ...] = (2017, 2018, 2019, 2020, 2021, 2022, 2023)

# Cols 0..401 of U_out and U_imports_out are industries; cols 402+ are
# Final-Demand columns (F010, F02N, …) and VA columns (V00100, V00200,
# V00300). See upstream R script ``load_suts_from_r.py`` for the slicing.
USEEIO_NOWCAST_INDUSTRY_COUNT = 402


def _validate_year(year: int) -> None:
    if year not in USEEIO_NOWCAST_YEARS:
        raise ValueError(
            f"USEEIO nowcast not available for {year}. "
            f"Available years: {USEEIO_NOWCAST_YEARS}"
        )


def _load_csv(name: str) -> pd.DataFrame:
    return load_from_gcs(
        name=name,
        sub_bucket=GCS_USEEIO_NOWCAST_DIR,
        local_dir=str(LOCAL_USEEIO_NOWCAST_DIR),
        loader=lambda pth: pd.read_csv(pth, index_col=0),
    )


@functools.cache
def load_useeio_nowcast_V_usa(year: int) -> pd.DataFrame:
    """Make table in bedrock's ``industry × commodity`` convention.

    Upstream files are ``commodity × industry`` (the transposed Make);
    this loader transposes back so the return matches ``load_2017_V_usa``.
    """
    _validate_year(year)
    raw = _load_csv(f"V_out_{year}.csv")
    V = raw.T
    V.index = V.index.astype(str)
    V.columns = V.columns.astype(str)
    return V


@functools.cache
def load_useeio_nowcast_Utot_intermediate_usa(year: int) -> pd.DataFrame:
    """Total intermediate Use, commodity × industry (FD and VA columns dropped).

    USEEIO ``U_out`` is the full Use matrix (commodity + 3 VA rows × {industry +
    FD + VA cols}). For A-matrix derivation we want only the
    ``commodity × industry`` intermediate-Use block — first ``402`` columns,
    full set of rows (VA rows are filtered out at the reindex-to-Vnorm step
    in the transform layer since they don't appear in Vnorm's commodity axis).
    """
    _validate_year(year)
    raw = _load_csv(f"U_out_{year}.csv")
    df = raw.iloc[:, :USEEIO_NOWCAST_INDUSTRY_COUNT].copy()
    df.index = df.index.astype(str)
    df.columns = df.columns.astype(str)
    return df


@functools.cache
def load_useeio_nowcast_Uimp_intermediate_usa(year: int) -> pd.DataFrame:
    """Imports portion of intermediate Use, commodity × industry."""
    _validate_year(year)
    raw = _load_csv(f"U_imports_out_{year}.csv")
    df = raw.iloc[:, :USEEIO_NOWCAST_INDUSTRY_COUNT].copy()
    df.index = df.index.astype(str)
    df.columns = df.columns.astype(str)
    return df
