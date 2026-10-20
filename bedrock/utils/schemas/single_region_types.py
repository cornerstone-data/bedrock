from __future__ import annotations

import dataclasses as dc

import pandas as pd


@dc.dataclass
class SingleRegionEEIOMatrixSet:
    Adom: pd.DataFrame
    Aimp: pd.DataFrame
    B: pd.DataFrame
    q: pd.Series[float]


@dc.dataclass
class SingleRegionAqMatrixSet:
    Adom: pd.DataFrame
    Aimp: pd.DataFrame
    scaled_q: pd.Series[float]


@dc.dataclass
class SingleRegionAMatrixSet:
    Adom: pd.DataFrame
    Aimp: pd.DataFrame


@dc.dataclass
class SingleRegionUMatrixSet:
    Udom: pd.DataFrame
    Uimp: pd.DataFrame


@dc.dataclass
class SingleRegionYtotAndTradeVectorSet:
    ytot: pd.Series[float]
    exports: pd.Series[float]
    imports: pd.Series[float]


@dc.dataclass
class SingleRegionYVectorSet:
    ydom: pd.Series[float]
    yimp: pd.Series[float]
