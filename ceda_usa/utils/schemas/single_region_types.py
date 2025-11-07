from __future__ import annotations

import dataclasses as dc

import pandas as pd
from pandera.typing import DataFrame

from ceda_usa.utils.schemas.single_region_schemas import AMatrix, BMatrix, UMatrix


# TODO: move everything to this type and then delete `types.py`
@dc.dataclass
class SingleRegionEEIOMatrixSet:
    Adom: DataFrame[AMatrix]
    Aimp: DataFrame[AMatrix]
    B: DataFrame[BMatrix]
    # TODO: figure out how to use the actual pandera schema here
    q: pd.Series[float]


# TODO: move everything to this type and then delete `types.py`
@dc.dataclass
class SingleRegionAqMatrixSet:
    Adom: DataFrame[AMatrix]
    Aimp: DataFrame[AMatrix]
    # TODO: figure out how to use the actual pandera schema here
    scaled_q: pd.Series[float]


# TODO: move everything to this type and then delete `types.py`
@dc.dataclass
class SingleRegionAMatrixSet:
    Adom: DataFrame[AMatrix]
    Aimp: DataFrame[AMatrix]


# TODO: move everything to this type and then delete `types.py`
@dc.dataclass
class SingleRegionUMatrixSet:
    Udom: DataFrame[UMatrix]
    Uimp: DataFrame[UMatrix]


# TODO: move everything to this type and then delete `types.py`
# TODO: figure out how to use the actual pandera schema here
@dc.dataclass
class SingleRegionYtotAndTradeVectorSet:
    ytot: pd.Series[float]
    exports: pd.Series[float]
    imports: pd.Series[float]


# TODO: move everything to this type and then delete `types.py`
# TODO: figure out how to use the actual pandera schema here
@dc.dataclass
class SingleRegionYVectorSet:
    ydom: pd.Series[float]
    yimp: pd.Series[float]
