from __future__ import annotations

import functools

import pandas as pd

from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_B_non_finetuned,
    derive_cornerstone_detail_Ytot_matrix_set,
    derive_cornerstone_Y_and_trade_scaled,
    derive_cornerstone_y_nab,
    derive_cornerstone_ydom_and_yimp,
)
from bedrock.utils.schemas.single_region_types import (
    SingleRegionAqMatrixSet,
    SingleRegionYtotAndTradeVectorSet,
    SingleRegionYVectorSet,
)


@functools.cache
def derive_B_usa_non_finetuned() -> pd.DataFrame:
    return derive_cornerstone_B_non_finetuned()


def derive_Y_and_trade_matrix_usa_from_summary_target_year_ytot_and_structural_reflection() -> (
    SingleRegionYtotAndTradeVectorSet
):
    return derive_cornerstone_Y_and_trade_scaled()


@functools.cache
def derive_y_for_national_accounting_balance_usa() -> pd.Series[float]:
    return derive_cornerstone_y_nab()


def derive_ydom_and_yimp_usa() -> SingleRegionYVectorSet:
    return derive_cornerstone_ydom_and_yimp()


@functools.cache
def derive_Aq_usa() -> SingleRegionAqMatrixSet:
    return derive_cornerstone_Aq_scaled()


@functools.cache
def derive_v7_detail_Ytot_usa_matrix_set() -> SingleRegionYtotAndTradeVectorSet:
    return derive_cornerstone_detail_Ytot_matrix_set()
