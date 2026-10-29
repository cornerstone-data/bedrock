"""BEA summary-level IO helpers for Cornerstone year-scaling and related callers.

``io_2012`` remains available for analysis that needs 2012 detail tables.
"""

from __future__ import annotations

import functools

import numpy as np
import pandas as pd

from bedrock.extract.iot.io_2017 import (
    load_summary_Uimp_usa,
    load_summary_Utot_usa,
    load_summary_V_usa,
    load_summary_Yimp_usa,
    load_summary_Ytot_usa,
)
from bedrock.utils.math.formulas import (
    compute_A_matrix,
    compute_q,
    compute_Unorm_matrix,
    compute_Vnorm_matrix,
    compute_x,
)
from bedrock.utils.math.handle_negatives import (
    handle_negative_matrix_values,
    handle_negative_vector_values,
)
from bedrock.utils.schemas.single_region_types import (
    SingleRegionYtotAndTradeVectorSet,
)
from bedrock.utils.taxonomy.bea.matrix_mappings import (
    USA_SUMMARY_MUT_YEARS,
)
from bedrock.utils.taxonomy.bea.v2017_industry_summary import (
    USA_2017_SUMMARY_INDUSTRY_CODES,
)
from bedrock.utils.taxonomy.bea.v2017_summary_final_demand import (
    USA_2017_SUMMARY_TOTAL_EXPORTS_CODE,
    USA_2017_SUMMARY_TOTAL_IMPORTS_CODE,
)


@functools.cache
def derive_summary_Adom_usa(year: USA_SUMMARY_MUT_YEARS) -> pd.DataFrame:
    Udom_norm = handle_negative_matrix_values(
        compute_Unorm_matrix(
            U=load_summary_Utot_usa(year) - load_summary_Uimp_usa(year),
            x=derive_summary_x_usa(year),
        )
    )
    Vnorm = compute_Vnorm_matrix(
        V=load_summary_V_usa(year), q=derive_summary_q_usa(year)
    )
    A = compute_A_matrix(U_norm=Udom_norm, V_norm=Vnorm).loc[
        USA_2017_SUMMARY_INDUSTRY_CODES, USA_2017_SUMMARY_INDUSTRY_CODES
    ]
    A.index.name = 'commodity_supply'
    A.columns.name = 'commodity_consumption'

    return A


@functools.cache
def derive_summary_Aimp_usa(year: USA_SUMMARY_MUT_YEARS) -> pd.DataFrame:
    Uimp_norm = handle_negative_matrix_values(
        compute_Unorm_matrix(
            U=load_summary_Uimp_usa(year), x=derive_summary_x_usa(year)
        )
    )
    Vnorm = compute_Vnorm_matrix(
        V=load_summary_V_usa(year), q=derive_summary_q_usa(year)
    )
    A = compute_A_matrix(U_norm=Uimp_norm, V_norm=Vnorm).loc[
        USA_2017_SUMMARY_INDUSTRY_CODES, USA_2017_SUMMARY_INDUSTRY_CODES
    ]
    A.index.name = 'commodity_supply'
    A.columns.name = 'commodity_consumption'

    return A


@functools.cache
def derive_summary_q_usa(year: USA_SUMMARY_MUT_YEARS) -> pd.Series[float]:
    return compute_q(V=load_summary_V_usa(year))


def derive_summary_Ytot_usa_matrix_set(
    year: USA_SUMMARY_MUT_YEARS,
) -> SingleRegionYtotAndTradeVectorSet:
    Ytot_with_trade_usa = load_summary_Ytot_usa(year)

    # NOTE: original y values have some negative values, but we enforce
    # that ytot and exports are positive. Otherwise, this distorts scaling
    # logic that relies on these vectors as reference.
    ytot = handle_negative_vector_values(
        Ytot_with_trade_usa.drop(
            columns=[
                USA_2017_SUMMARY_TOTAL_EXPORTS_CODE,
                USA_2017_SUMMARY_TOTAL_IMPORTS_CODE,
            ]
        ).sum(axis=1)
    )

    exports = handle_negative_vector_values(
        Ytot_with_trade_usa[USA_2017_SUMMARY_TOTAL_EXPORTS_CODE]
    )

    # TODO: we use the `SingleRegionYtotAndTradeVectorSet` type here
    # but don't validate ytot/exports/imports agains the single region schemas.
    # This is because the latter use detail-level codes whereas these
    # series use summary-level codes. We possibly want a different
    # type for summary-level codes?
    return SingleRegionYtotAndTradeVectorSet(
        ytot=ytot,
        exports=exports,
        # TODO: some commodities in the Use matrix have positive imports. These do
        # not appear in the Import matrix. We do not know why yet.
        imports=(
            -1
            * Ytot_with_trade_usa[USA_2017_SUMMARY_TOTAL_IMPORTS_CODE].apply(
                lambda x: np.min(x, 0)
            )
        ),
    )


def derive_summary_Yimp_usa(
    year: USA_SUMMARY_MUT_YEARS,
) -> pd.DataFrame:
    return load_summary_Yimp_usa(year).drop(
        columns=[
            USA_2017_SUMMARY_TOTAL_EXPORTS_CODE,
            USA_2017_SUMMARY_TOTAL_IMPORTS_CODE,
        ]
    )


@functools.cache
def derive_summary_x_usa(year: USA_SUMMARY_MUT_YEARS) -> pd.Series[float]:
    return compute_x(V=load_summary_V_usa(year))
