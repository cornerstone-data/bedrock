from __future__ import annotations

import functools

import numpy as np
import pandas as pd
import pandera.pandas as pa
import pandera.typing as pt

from bedrock.utils.economic.inflation import (
    obtain_inflation_factors_from_reference_data,
)
from bedrock.utils.schemas.single_region_schemas import VMatrix
from bedrock.utils.taxonomy.mappings.ceda_v7__ceda_v5 import (
    CEDA_V5_TO_CEDA_V7_CODES,
)

get_price_index = functools.cache(
    lambda: obtain_inflation_factors_from_reference_data()
)


def inflate_usa_U_to_target_year(
    U: pd.DataFrame,
    original_year: int,
    target_year: int,
) -> pd.DataFrame:
    assert U.shape == (400, 400)
    if "33391A" in U.index:
        U = U.rename(index=CEDA_V5_TO_CEDA_V7_CODES, columns=CEDA_V5_TO_CEDA_V7_CODES)

    price_index = get_price_index()
    price_ratio_base_to_target = price_index[target_year] / price_index[original_year]

    return U.multiply(price_ratio_base_to_target, axis=0)


@pa.check_output(VMatrix.to_schema())
def inflate_usa_V_to_target_year(
    V: pt.DataFrame[VMatrix],
    original_year: int,
    target_year: int,
) -> pt.DataFrame[VMatrix]:
    assert V.shape == (400, 400)

    price_index = get_price_index()
    price_ratio_base_to_target = price_index[target_year] / price_index[original_year]

    # TODO: Must confirm this.
    # V is industry x commodity. Should we inflate how much of
    # each commodity the industry makes? Or inflate the relative
    # value of the industry?
    # My initial guess was opposite of U since the dimension
    # is opposite..
    return pt.DataFrame[VMatrix](V.multiply(price_ratio_base_to_target, axis=1))


def inflate_A_matrix(
    A: pd.DataFrame, original_year: int, target_year: int
) -> pd.DataFrame:
    price_index = obtain_inflation_factors_from_reference_data()

    price_ratio = price_index[target_year] / price_index[original_year]
    return pd.DataFrame(
        (np.diag(price_ratio) @ A @ np.diag(1 / price_ratio)).values,
        index=A.index,
        columns=A.columns,
    )


def inflate_B_matrix(
    B: pd.DataFrame, original_year: int, target_year: int
) -> pd.DataFrame:
    price_index = obtain_inflation_factors_from_reference_data()

    price_ratio = price_index[original_year] / price_index[target_year]
    return B * price_ratio.loc[B.columns].values


def inflate_q_or_y(
    q_or_y: pd.Series[float], original_year: int, target_year: int
) -> pd.Series[float]:
    price_index = obtain_inflation_factors_from_reference_data()

    price_ratio = price_index[target_year] / price_index[original_year]
    return q_or_y * price_ratio.loc[q_or_y.index]
