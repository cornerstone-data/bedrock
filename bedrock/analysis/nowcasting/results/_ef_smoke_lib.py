"""Shared helpers for local nowcast EF smoke scripts under ``results/``."""

from __future__ import annotations

import time

import numpy as np
import pandas as pd

from bedrock.publish.cache_reset import clear_all_publish_caches
from bedrock.utils.config.usa_config import reset_usa_config, set_global_usa_config
from bedrock.utils.math.formulas import (
    compute_d,
    compute_L_matrix,
    compute_M_matrix,
    compute_n,
)
from bedrock.utils.snapshots import releases
from bedrock.utils.snapshots.loader import load_snapshot
from bedrock.utils.validation.analysis.ef_hist_panels import KEY_USA_SECTORS

# Eight deck callouts + two extras for a ~10-sector series.
SECTORS: tuple[str, ...] = (
    *KEY_USA_SECTORS,
    '325110',  # Petrochemical manufacturing
    '336111',  # Automobile manufacturing
)

SECTOR_LABELS: dict[str, str] = {
    '221100': 'Electric power',
    '211000': 'Oil & gas extraction',
    '1121A0': 'Beef cattle',
    '212100': 'Coal mining',
    '481000': 'Air transport',
    '1111B0': 'Grain farming',
    '324110': 'Petroleum refineries',
    '484000': 'Truck transport',
    '325110': 'Petrochemicals',
    '336111': 'Automobiles',
}

NOWCAST_YEARS: tuple[int, ...] = (2018, 2019, 2020, 2021, 2022, 2023)
COMPARE_DOLLAR_YEAR = 2024
NOWCAST_2023_SNAPSHOT = '01f89de5c930639e5729ab5ce7b97c78c0dd05fc'
V03_SNAPSHOT = releases.v0_3_0


def clear_year_caches() -> None:
    clear_all_publish_caches()
    from bedrock.extract.iot import nowcast_mut_storage as nms  # noqa: PLC0415

    nms.latest_nowcast_mut_vintage.cache_clear()
    nms._load_stored_table.cache_clear()


def config_stem(year: int) -> str:
    return f'2025_usa_cornerstone_v0_4_nowcast_{year}'


def _as_float_series(values: pd.DataFrame | pd.Series[float]) -> pd.Series[float]:
    """EF vector as float Series (mypy-friendly)."""
    if isinstance(values, pd.DataFrame):
        col = values.iloc[:, 0]
        idx = values.index
    else:
        col = values
        idx = values.index
    return pd.Series(pd.to_numeric(col, errors='coerce'), index=idx, dtype=float)


def efs_from_live_config(year: int) -> tuple[pd.Series[float], pd.Series[float]]:
    """D and N for one nowcast year via live B + A (not a stored snapshot)."""
    from bedrock.transform.eeio.derived import (  # noqa: PLC0415
        derive_Aq_usa,
        derive_B_usa_non_finetuned,
    )

    clear_year_caches()
    reset_usa_config()
    set_global_usa_config(f'{config_stem(year)}.yaml')

    t0 = time.time()
    B = derive_B_usa_non_finetuned()
    aq = derive_Aq_usa()
    L = compute_L_matrix(A=aq.Adom + aq.Aimp)
    M = compute_M_matrix(B=B, L=L)
    D = _as_float_series(compute_d(B=B))
    N = _as_float_series(compute_n(M=M))
    print(
        f'  year={year}: B/A/N in {time.time() - t0:.1f}s  N.sum={float(N.sum()):.4g}'
    )
    return D, N


def efs_from_snapshot(key: str) -> tuple[pd.Series[float], pd.Series[float]]:
    B = load_snapshot('B_USA_non_finetuned', key)
    Adom = load_snapshot('Adom_USA', key)
    Aimp = load_snapshot('Aimp_USA', key)
    L = compute_L_matrix(A=Adom + Aimp)
    M = compute_M_matrix(B=B, L=L)
    D = _as_float_series(compute_d(B=B))
    N = _as_float_series(compute_n(M=M))
    return D, N


def perc_diff(new: pd.Series[float], old: pd.Series[float]) -> pd.Series[float]:
    return ((new - old) / old.replace(0, np.nan)).replace(
        [np.inf, -np.inf], np.nan
    ).fillna(0.0) * 100.0
