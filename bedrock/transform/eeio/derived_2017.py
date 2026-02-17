from __future__ import annotations

import functools

import numpy as np
import pandas as pd
import pandera.pandas as pa
import pandera.typing as pt

from bedrock.extract.iot.io_2012 import (
    load_2012_PI_usa,
    load_2012_pR_usa,
    load_2012_UR_usa,
    load_2012_URdom_usa,
    load_2012_VR_usa,
    load_2012_YR_usa,
)
from bedrock.extract.iot.io_2017 import (
    load_2017_Uimp_usa,
    load_2017_Utot_usa,
    load_2017_V_usa,
    load_2017_value_added_usa,
    load_2017_Ytot_usa,
    load_summary_Uimp_usa,
    load_summary_Utot_usa,
    load_summary_V_usa,
    load_summary_Yimp_usa,
    load_summary_Ytot_usa,
)
from bedrock.transform.eeio.derived_2017_helpers import (
    EXPECTED_COMMODITIES_DROPPED,
    derive_2017_scrap_weight,
    derive_2017_U_weight,
    derive_2017_V_weight,
    derive_2017_Y_weight,
)
from bedrock.utils.economic.inflate_to_target_year import inflate_usa_V_to_target_year
from bedrock.utils.math.formulas import (
    compute_A_matrix,
    compute_g,
    compute_q,
    compute_Unorm_matrix,
    compute_Vnorm_matrix,
    compute_y_imp,
)
from bedrock.utils.math.handle_negatives import (
    handle_negative_matrix_values,
    handle_negative_vector_values,
)
from bedrock.utils.math.structural_reflection import (
    structural_reflect_matrix,
    structural_reflect_vector,
)
from bedrock.utils.schemas.single_region_schemas import (
    AMatrix,
    ExportsVectorSchema,
    GVectorSchema,
    ImportsVectorSchema,
    QVectorSchema,
    UMatrix,
    VMatrix,
    YVectorSchema,
)
from bedrock.utils.schemas.single_region_types import (
    SingleRegionAqMatrixSet,
    SingleRegionUMatrixSet,
    SingleRegionYtotAndTradeVectorSet,
)
from bedrock.utils.taxonomy.bea.matrix_mappings import (
    USA_SUMMARY_MUT_YEARS,
)
from bedrock.utils.taxonomy.bea.v2017_final_demand import (
    USA_2017_FINAL_DEMAND_EXPORT_CODE,
    USA_2017_FINAL_DEMAND_IMPORT_CODE,
    USA_2017_FINAL_DEMAND_PERSONAL_CONSUMPTION_EXPENDITURE_CODE,
)
from bedrock.utils.taxonomy.bea.v2017_industry_summary import (
    USA_2017_SUMMARY_INDUSTRY_CODES,
)
from bedrock.utils.taxonomy.bea.v2017_summary_final_demand import (
    USA_2017_SUMMARY_TOTAL_EXPORTS_CODE,
    USA_2017_SUMMARY_TOTAL_IMPORTS_CODE,
)
from bedrock.utils.taxonomy.usa_taxonomy_correspondence_helpers import (
    load_usa_2017_commodity__ceda_v7_correspondence,
    load_usa_2017_industry__ceda_v7_correspondence,
)


def derive_2017_Aq_usa() -> SingleRegionAqMatrixSet:
    Vnorm_scrap_corrected = derive_2017_Vnorm_scrap_corrected()
    g = derive_2017_g_usa()

    # generate domestic and import portion of the standard Make and Use tables
    # domestic/import direct requirements (after redefinition)
    Uset = derive_2017_U_set_usa()
    Udom_norm = compute_Unorm_matrix(U=Uset.Udom, g=g)
    Uimp_norm = compute_Unorm_matrix(U=Uset.Uimp, g=g)

    # domestic & import direct requirements (after redefinition) in industry technology
    Adom = compute_A_matrix(U_norm=Udom_norm, V_norm=Vnorm_scrap_corrected)
    Aimp = compute_A_matrix(U_norm=Uimp_norm, V_norm=Vnorm_scrap_corrected)
    assert (Adom >= 0).all().all(), "Adom_USA has negative values."
    assert (Aimp >= 0).all().all(), "Aimp_USA has negative values."

    # NOTE: turning this off for now, there is only one sector (S00102) with total industry inputs exceeding 1
    # _total_industry_output = compute_total_industry_inputs(A=Adom + Aimp)
    # assert (
    #     _total_industry_output < 1
    # ).all(), f"USA has industry with total industry inputs exceed 1:{list(_total_industry_output[_total_industry_output>1].index)}"

    q = derive_2017_q_usa()
    assert (q >= 0).all(), "q_USA has negative values."
    return SingleRegionAqMatrixSet(
        Adom=pt.DataFrame[AMatrix](Adom),
        Aimp=pt.DataFrame[AMatrix](Aimp),
        scaled_q=QVectorSchema.validate(q),
    )


@functools.cache
def derive_2017_U_set_usa() -> SingleRegionUMatrixSet:
    Uset_with_negatives = derive_2017_U_with_negatives()

    Udom = handle_negative_matrix_values(Uset_with_negatives.Udom)
    Uimp = handle_negative_matrix_values(Uset_with_negatives.Uimp)

    assert not (Udom < 0).any().any(), "Udom_USA has negative values."
    assert not (Uimp < 0).any().any(), "Uimp_USA has negative values."

    return SingleRegionUMatrixSet(
        Udom=pt.DataFrame[UMatrix](Udom),
        Uimp=pt.DataFrame[UMatrix](Uimp),
    )


@functools.cache
@pa.check_output(VMatrix.to_schema())
def derive_2017_V_usa() -> pt.DataFrame[VMatrix]:
    V_2017 = load_2017_V_usa()
    V_2017_structural_reflected = structural_reflect_matrix(
        row_corresp_df=load_usa_2017_industry__ceda_v7_correspondence(),
        col_corresp_df=load_usa_2017_commodity__ceda_v7_correspondence(),
        df_base=V_2017,
        df_weights=derive_2017_V_weight(V_2012=load_2012_VR_usa(), V_2017=V_2017),
        expected_col_dropped=EXPECTED_COMMODITIES_DROPPED,
    )

    V_2017_structural_reflected.index.name = "sector"
    V_2017_structural_reflected.columns.name = "sector"
    return pt.DataFrame[VMatrix](V_2017_structural_reflected)


@functools.cache
@pa.check_output(GVectorSchema)
def derive_2017_g_usa() -> pd.Series[float]:
    return compute_g(V=derive_2017_V_usa())


@pa.check_output(QVectorSchema)
def derive_2017_q_usa() -> pd.Series[float]:
    return compute_q(V=derive_2017_V_usa())


def derive_q_from_U_usa_and_Ytot_usa() -> pd.Series[float]:
    U_usa = derive_2017_U_set_usa()
    detail_2017_YandTradeset = derive_2017_Ytot_usa_matrix_set()
    # The identity we're using here is q - Utot = Ytot - imports + exports
    # so q = Utot + Ytot - imports + exports
    return (
        (U_usa.Udom + U_usa.Uimp).sum(axis=1)
        + detail_2017_YandTradeset.ytot
        + detail_2017_YandTradeset.exports
        # `detail_2017_YandTradeset.imports` is positive, so we need to subtract it
        - detail_2017_YandTradeset.imports
    )


@functools.cache
@pa.check_output(VMatrix.to_schema())
def derive_2017_Vnorm_scrap_corrected(
    apply_inflation: bool = False, target_year: int = 0
) -> pt.DataFrame[VMatrix]:
    V_usa = derive_2017_V_usa()

    if apply_inflation:
        V_usa = inflate_usa_V_to_target_year(
            V=V_usa, original_year=2017, target_year=target_year
        )

    q = compute_q(V=V_usa)
    Vnorm = compute_Vnorm_matrix(V=V_usa, q=q)

    scrap_2017 = load_2017_V_usa().loc[:, "S00401"]
    scrap_faction = structural_reflect_vector(
        corresp_df=load_usa_2017_industry__ceda_v7_correspondence(),
        ser_base=scrap_2017,
        ser_weights=derive_2017_scrap_weight(
            scrap_2012=load_2012_PI_usa() @ load_2012_pR_usa(),
            scrap_2017=scrap_2017,
        ),
    )

    V_scrap_corrected = Vnorm.divide(1.0 - (scrap_faction / q).fillna(0.0))
    return pt.DataFrame[VMatrix](V_scrap_corrected)


@functools.cache
def derive_2017_U_with_negatives() -> SingleRegionUMatrixSet:
    Utot_usa = load_2017_Utot_usa()
    Uimp_usa = load_2017_Uimp_usa()
    Udom_usa = Utot_usa - Uimp_usa

    corresp_industry_ceda = load_usa_2017_industry__ceda_v7_correspondence()
    corresp_commodity_ceda = load_usa_2017_commodity__ceda_v7_correspondence()

    Udom = structural_reflect_matrix(
        row_corresp_df=corresp_commodity_ceda,
        col_corresp_df=corresp_industry_ceda,
        df_base=Udom_usa,
        df_weights=derive_2017_U_weight(U_2012=load_2012_URdom_usa(), U_2017=Udom_usa),
        expected_row_dropped=EXPECTED_COMMODITIES_DROPPED,
    )

    Uimp = structural_reflect_matrix(
        row_corresp_df=corresp_commodity_ceda,
        col_corresp_df=corresp_industry_ceda,
        df_base=Uimp_usa,
        df_weights=derive_2017_U_weight(
            U_2012=load_2012_UR_usa() - load_2012_URdom_usa(), U_2017=Uimp_usa
        ),
        expected_row_dropped=EXPECTED_COMMODITIES_DROPPED,
    )

    Udom.index.name = "sector"
    Udom.columns.name = "sector"
    Uimp.index.name = "sector"
    Uimp.columns.name = "sector"

    return SingleRegionUMatrixSet(
        Udom=pt.DataFrame[UMatrix](Udom), Uimp=pt.DataFrame[UMatrix](Uimp)
    )


def derive_detail_y_imp_usa() -> pd.Series[float]:
    return compute_y_imp(
        imports=derive_2017_Ytot_usa_matrix_set().imports,
        Uimp=derive_2017_U_set_usa().Uimp,
    )


def derive_2017_Y_personal_consumption_expenditure_usa() -> pd.Series[float]:
    return _derive_detail_Ytot_with_trade_usa()[
        USA_2017_FINAL_DEMAND_PERSONAL_CONSUMPTION_EXPENDITURE_CODE
    ]


def derive_2017_Ytot_usa_matrix_set() -> SingleRegionYtotAndTradeVectorSet:
    Ytot_with_trade_usa = _derive_detail_Ytot_with_trade_usa()

    return SingleRegionYtotAndTradeVectorSet(
        ytot=YVectorSchema.validate(
            handle_negative_vector_values(
                Ytot_with_trade_usa.drop(
                    columns=[
                        USA_2017_FINAL_DEMAND_EXPORT_CODE,
                        USA_2017_FINAL_DEMAND_IMPORT_CODE,
                    ]
                ).sum(axis=1)
            )
        ),
        exports=ExportsVectorSchema.validate(
            Ytot_with_trade_usa[USA_2017_FINAL_DEMAND_EXPORT_CODE]
        ),
        imports=ImportsVectorSchema.validate(
            (
                -1
                * Ytot_with_trade_usa[USA_2017_FINAL_DEMAND_IMPORT_CODE].apply(
                    lambda x: np.min(x, 0)
                )
            )
        ),
    )


def _derive_detail_Ytot_with_trade_usa() -> pd.DataFrame:
    Ytot_with_trade_usa_orig = load_2017_Ytot_usa()
    Y_2012 = load_2012_YR_usa()
    corresp_commodity_ceda = load_usa_2017_commodity__ceda_v7_correspondence()
    Ytot_with_trade_usa = structural_reflect_matrix(
        row_corresp_df=corresp_commodity_ceda,
        col_corresp_df=pd.DataFrame(
            np.eye(len(Ytot_with_trade_usa_orig.columns)),
            index=Ytot_with_trade_usa_orig.columns,
            columns=Ytot_with_trade_usa_orig.columns,
        ),
        df_base=Ytot_with_trade_usa_orig,
        df_weights=derive_2017_Y_weight(Y_2012=Y_2012, Y_2017=Ytot_with_trade_usa_orig),
        expected_row_dropped=EXPECTED_COMMODITIES_DROPPED,
    )
    Ytot_with_trade_usa.index.name = "sector"

    return Ytot_with_trade_usa


def derive_detail_VA_usa() -> pd.DataFrame:
    "Derives the value added portion of the 2017 detail Use tables in the ceda_v7 schema"
    VA = load_2017_value_added_usa()
    corresp_industry = load_usa_2017_industry__ceda_v7_correspondence()

    # Calculating weights by aggregating the 2017 VA values along the column axis to align with CEDA-schema industries
    VA_weights = VA @ corresp_industry.T

    VA_ceda_usa = structural_reflect_matrix(
        row_corresp_df=pd.DataFrame(
            np.eye(len(VA.index)),
            index=VA.index,
            columns=VA.index,
        ),
        col_corresp_df=corresp_industry,
        df_base=VA,
        df_weights=VA_weights,
    )
    return VA_ceda_usa


@functools.cache
def derive_summary_Adom_usa(year: USA_SUMMARY_MUT_YEARS) -> pd.DataFrame:
    Udom_norm = handle_negative_matrix_values(
        compute_Unorm_matrix(
            U=load_summary_Utot_usa(year) - load_summary_Uimp_usa(year),
            g=_derive_summary_g_usa(year),
        )
    )
    Vnorm = compute_Vnorm_matrix(
        V=load_summary_V_usa(year), q=derive_summary_q_usa(year)
    )
    A = compute_A_matrix(U_norm=Udom_norm, V_norm=Vnorm).loc[
        USA_2017_SUMMARY_INDUSTRY_CODES, USA_2017_SUMMARY_INDUSTRY_CODES
    ]
    A.index.name = "commodity_supply"
    A.columns.name = "commodity_consumption"

    return A


@functools.cache
def derive_summary_Aimp_usa(year: USA_SUMMARY_MUT_YEARS) -> pd.DataFrame:
    Uimp_norm = handle_negative_matrix_values(
        compute_Unorm_matrix(
            U=load_summary_Uimp_usa(year), g=_derive_summary_g_usa(year)
        )
    )
    Vnorm = compute_Vnorm_matrix(
        V=load_summary_V_usa(year), q=derive_summary_q_usa(year)
    )
    A = compute_A_matrix(U_norm=Uimp_norm, V_norm=Vnorm).loc[
        USA_2017_SUMMARY_INDUSTRY_CODES, USA_2017_SUMMARY_INDUSTRY_CODES
    ]
    A.index.name = "commodity_supply"
    A.columns.name = "commodity_consumption"

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
def _derive_summary_g_usa(year: USA_SUMMARY_MUT_YEARS) -> pd.Series[float]:
    return compute_g(V=load_summary_V_usa(year))
