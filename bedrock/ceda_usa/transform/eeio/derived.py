from __future__ import annotations

import functools
import logging

import pandas as pd
import pandera.pandas as pa
import pandera.typing as pt

from bedrock.ceda_usa.config.usa_config import get_usa_config
from bedrock.ceda_usa.extract.iot.io_2012 import (
    load_2012_PC_usa,
    load_2012_PI_usa,
    load_2012_UR_usa,
    load_2012_URdom_usa,
    load_2012_YR_usa,
)
from bedrock.ceda_usa.extract.iot.io_2017 import load_summary_Uimp_usa
from bedrock.ceda_usa.transform.allocation.derived import derive_E_usa
from bedrock.ceda_usa.transform.eeio.derived_2017 import (
    derive_2017_Aq_usa,
    derive_2017_g_usa,
    derive_2017_U_set_usa,
    derive_2017_Vnorm_scrap_corrected,
    derive_2017_Ytot_usa_matrix_set,
    derive_summary_Yimp_usa,
    derive_summary_Ytot_usa_matrix_set,
)
from bedrock.ceda_usa.transform.eeio.scale_abq_via_summary import (
    scale_detail_A_based_on_summary_A,
    scale_detail_B_based_on_summary_q,
    scale_detail_q_based_on_summary_q,
)
from bedrock.ceda_usa.utils.constants import (
    USA_2017_FINAL_DEMAND_EXPORT_CODE,
    USA_2017_FINAL_DEMAND_IMPORT_CODE,
    USA_2017_SUMMARY_INDUSTRY_CODES,
)
from bedrock.ceda_usa.utils.disaggregation import disaggregate_vector
from bedrock.ceda_usa.utils.formulas import (
    compute_B_ind_matrix,
    compute_B_matrix,
    compute_y_for_national_accounting_balance,
    compute_y_imp,
)
from bedrock.ceda_usa.utils.handle_negatives import handle_negative_vector_values
from bedrock.ceda_usa.utils.inflate_to_target_year import (
    inflate_A_matrix,
    inflate_B_matrix,
    inflate_q_or_y,
)
from bedrock.ceda_usa.utils.schemas.single_region_schemas import (
    AMatrix,
    BMatrix,
    ExportsVectorSchema,
    ImportsVectorSchema,
    QVectorSchema,
    UMatrix,
    YVectorSchema,
)
from bedrock.ceda_usa.utils.schemas.single_region_types import (
    SingleRegionAqMatrixSet,
    SingleRegionUMatrixSet,
    SingleRegionYtotAndTradeVectorSet,
    SingleRegionYVectorSet,
)
from bedrock.ceda_usa.utils.split_using_aggregated_weights import (
    split_vector_using_agg_ratio,
)
from bedrock.ceda_usa.utils.taxonomy.bea_v2017_to_ceda_v7_helpers import (
    get_bea_v2017_summary_to_ceda_corresp_df,
)
from bedrock.ceda_usa.utils.taxonomy.mappings.ceda_v7__ceda_v5 import (
    CEDA_V5_TO_CEDA_V7_CODES,
)

logger = logging.getLogger(__name__)


@pa.check_output(BMatrix.to_schema())
def derive_B_usa_non_finetuned() -> pt.DataFrame[BMatrix]:
    E_usa = derive_E_usa()
    # B_usa_2017 has 2022 emissions but 2017 economic data
    b_usa_2017 = derive_B_usa_via_vnorm(E_usa=E_usa)
    # Scale the economic data part of B_usa_2017 to 2022
    B_usa = inflate_B_matrix(
        scale_detail_B_based_on_summary_q(
            B=b_usa_2017,
            original_year=get_usa_config().usa_detail_original_year,
            target_year=get_usa_config().usa_io_data_year,
        ),
        original_year=get_usa_config().usa_io_data_year,
        target_year=get_usa_config().ceda_base_year,
    )

    return pt.DataFrame[BMatrix](B_usa)


def derive_Y_and_trade_matrix_usa_from_summary_target_year_ytot_and_structural_reflection() -> (
    SingleRegionYtotAndTradeVectorSet
):
    """
    We get detail Y and Trade Matrix in the following steps:
    - get `target_year` summary Y and Trade Matrix and 2017 detail Y and Trade Matrix
    - structurally reflect `target_year` summary Y and Trade Matrix into `target_year` detail Y and Trade Matrix using 2017 detail Y and Trade Matrix
    - split the reflected `target_year` detail Y and Trade Matrix into `target_year` detail Y and Trade Matrix,
      using weights derived from `target_year` summary Y and Trade Matrix
    """
    detail_2017_YandTradeset = derive_2017_Ytot_usa_matrix_set()

    summary_to_ceda_corresp_df = get_bea_v2017_summary_to_ceda_corresp_df()

    summary_Y_matrix_set = derive_summary_Ytot_usa_matrix_set(
        get_usa_config().usa_io_data_year
    )

    ytot = inflate_q_or_y(
        disaggregate_vector(
            base_series=summary_Y_matrix_set.ytot,
            weight_series=detail_2017_YandTradeset.ytot,
            corresp_df=summary_to_ceda_corresp_df,
        ),
        original_year=get_usa_config().usa_io_data_year,
        target_year=get_usa_config().ceda_base_year,
    )
    exports = inflate_q_or_y(
        disaggregate_vector(
            base_series=summary_Y_matrix_set.exports,
            weight_series=detail_2017_YandTradeset.exports,
            corresp_df=summary_to_ceda_corresp_df,
        ),
        original_year=get_usa_config().usa_io_data_year,
        target_year=get_usa_config().ceda_base_year,
    )
    imports = inflate_q_or_y(
        handle_negative_vector_values(
            disaggregate_vector(
                base_series=summary_Y_matrix_set.imports,
                weight_series=detail_2017_YandTradeset.imports,
                corresp_df=summary_to_ceda_corresp_df,
            )
        ),
        original_year=get_usa_config().usa_io_data_year,
        target_year=get_usa_config().ceda_base_year,
    )

    return SingleRegionYtotAndTradeVectorSet(
        ytot=YVectorSchema.validate(ytot),
        exports=ExportsVectorSchema.validate(exports),
        imports=ImportsVectorSchema.validate(imports),
    )


@pa.check_output(YVectorSchema)
def derive_y_for_national_accounting_balance_usa() -> pd.Series[float]:
    """
    We get Y for national accounting balance via the following equations:

        y_nab = y_dom + exports
        y_nab = (y_tot - y_imp) + exports
        y_nab = (y_tot - (imports - Uimp_row_sum)) + exports

    Because we only have Uimp in 2017 detail, we get 2022 detail y in the
    following steps:
    - calculate 2017 detail y_nab
    - calculate 2022 summary y_nab
    - scale 2017 detail y_nab to 2022 detail y_nab using 2022 summary y_nab
    """
    detail_2017_YandTradeset = derive_2017_Ytot_usa_matrix_set()

    y_national_acct_balance_detail_2017 = compute_y_for_national_accounting_balance(
        y_tot=detail_2017_YandTradeset.ytot,
        y_imp=compute_y_imp(
            imports=detail_2017_YandTradeset.imports,
            Uimp=derive_2017_U_set_usa().Uimp,
        ),
        exports=detail_2017_YandTradeset.exports,
    )

    summary_2022_Y_matrix_set = derive_summary_Ytot_usa_matrix_set(
        get_usa_config().usa_io_data_year
    )

    y_national_acct_balance_summary_2022 = compute_y_for_national_accounting_balance(
        y_tot=summary_2022_Y_matrix_set.ytot,
        y_imp=compute_y_imp(
            imports=summary_2022_Y_matrix_set.imports,
            Uimp=load_summary_Uimp_usa(get_usa_config().usa_io_data_year).loc[
                USA_2017_SUMMARY_INDUSTRY_CODES, USA_2017_SUMMARY_INDUSTRY_CODES
            ],
        ),
        exports=summary_2022_Y_matrix_set.exports,
    )

    summary_to_ceda_corresp_df = get_bea_v2017_summary_to_ceda_corresp_df()
    y_national_acct_balance_detail_2022 = inflate_q_or_y(
        disaggregate_vector(
            corresp_df=summary_to_ceda_corresp_df,
            base_series=y_national_acct_balance_summary_2022,
            weight_series=y_national_acct_balance_detail_2017,
        ),
        original_year=get_usa_config().usa_io_data_year,
        target_year=get_usa_config().ceda_base_year,
    )

    # NOTE: original y values have some negative values
    # that will distort the scaling process that uses the ytot here in non-US countries.
    # We make a data assumption here to set them to 0 in order to make the scaling process valid.
    # TODO: this is a temporary solution, we need a y pandera type to enforce this data assumption.
    return handle_negative_vector_values(y_national_acct_balance_detail_2022)


def derive_ydom_and_yimp_usa() -> SingleRegionYVectorSet:
    """
    This function is only used in derivation of Y_oecd, where we need y_dom and y_imp separately
    to populate the diagonal and off-diagonal elements of Y_oecd.
    We get ydom and yimp in the following steps:
    1. Get 2022 detail ytot
    2. Get 2022 summary ydom and yimp
    3. Split 2022 detail ytot to ydom and yimp using 2022 summary ydom and yimp ratios
    """
    # Load summary 2022 ytot and yimp
    summary_2022_ytot = derive_summary_Ytot_usa_matrix_set(2022).ytot
    summary_2022_yimp = derive_summary_Yimp_usa(2022).sum(axis=1)
    # Derive ydom over ytot ratio
    # NOTE: in case some yimp values are larger than ytot values, causing ydom to be negative,
    # which could be reasonable but we don't want to take negative ydom values.
    # We handle this by setting ydom to 0 when yimp is larger than ytot.
    summary_2022_ydom_over_ytot_ratio = handle_negative_vector_values(
        1 - (summary_2022_yimp / summary_2022_ytot).fillna(0.0)
    )

    # Derive 2022 detail ytot
    detail_2022_ytot = disaggregate_vector(
        corresp_df=get_bea_v2017_summary_to_ceda_corresp_df(),
        base_series=summary_2022_ytot,
        weight_series=derive_2017_Ytot_usa_matrix_set().ytot,
    )
    # Split detail 2022 ytot to ydom and yimp
    ydom, yimp = split_vector_using_agg_ratio(
        base_series=detail_2022_ytot,
        agg_ratio_series=summary_2022_ydom_over_ytot_ratio,
        corresp_df=get_bea_v2017_summary_to_ceda_corresp_df(),
    )
    return SingleRegionYVectorSet(
        ydom=YVectorSchema.validate(ydom), yimp=YVectorSchema.validate(yimp)
    )


def derive_Aq_usa() -> SingleRegionAqMatrixSet:
    """
    This function derives Aq_usa in `target_year` USD.

    For Adom and Aimp, we get `target_year` detail matrices by
    year-scaling 2017 detail Adom and Aimp separately using the
    `target_year` summary Adom and Aimp, respectively

    For q, we get 2017 detail q from 2017 detail IOTs, then inflate it to `target_year` USD.
    """
    detail_2017_Aq_set = derive_2017_Aq_usa()

    target_year = get_usa_config().usa_io_data_year
    original_year = get_usa_config().usa_detail_original_year

    Adom = scale_detail_A_based_on_summary_A(
        A=detail_2017_Aq_set.Adom,
        target_year=target_year,
        original_year=original_year,
        dom_or_imp_or_total="dom",
    )
    Aimp = scale_detail_A_based_on_summary_A(
        A=detail_2017_Aq_set.Aimp,
        target_year=target_year,
        original_year=original_year,
        dom_or_imp_or_total="imp",
    )

    q = scale_detail_q_based_on_summary_q(
        q=detail_2017_Aq_set.scaled_q,
        target_year=target_year,
        original_year=original_year,
    )
    assert q is not None, "q in derive_Aq_usa() is None"

    # NOTE: the Adom/Aimp/q being passed in are already in `target_year`.
    # We just need to inflate them to CEDA base year.
    # TODO: type inflate_A_matrix as DataFrame[AMatrix]
    Adom = inflate_A_matrix(  # type: ignore[assignment]
        Adom,
        target_year=get_usa_config().ceda_base_year,
        original_year=target_year,
    )
    Aimp = inflate_A_matrix(  # type: ignore[assignment]
        Aimp,
        target_year=get_usa_config().ceda_base_year,
        original_year=target_year,
    )
    q = inflate_q_or_y(
        q, target_year=get_usa_config().ceda_base_year, original_year=target_year
    )

    return SingleRegionAqMatrixSet(
        Adom=pt.DataFrame[AMatrix](Adom),
        Aimp=pt.DataFrame[AMatrix](Aimp),
        scaled_q=QVectorSchema.validate(q),
    )


def derive_B_usa_via_vnorm(*, E_usa: pd.DataFrame) -> pd.DataFrame:
    g = derive_2017_g_usa()
    Vnorm = derive_2017_Vnorm_scrap_corrected()

    Bi = compute_B_ind_matrix(E=E_usa, g=g)
    Bc = compute_B_matrix(B_ind=Bi, V_norm=Vnorm)

    Bc.columns.name = "sector"
    Bc.index.name = "ghg"
    return Bc


def derive_v5_U_usa() -> SingleRegionUMatrixSet:
    URtot_usa = load_2012_UR_usa()
    URdom_usa = load_2012_URdom_usa()

    PI = load_2012_PI_usa()
    PC = load_2012_PC_usa()

    # squarize all matrices by using 400 commodity or industry classification
    URimp_usa = URtot_usa - URdom_usa

    URdom = PC.T @ URdom_usa @ PI.T
    URimp = PC.T @ URimp_usa @ PI.T

    URdom.rename(
        columns=CEDA_V5_TO_CEDA_V7_CODES, index=CEDA_V5_TO_CEDA_V7_CODES, inplace=True
    )
    URimp.rename(
        columns=CEDA_V5_TO_CEDA_V7_CODES, inplace=True, index=CEDA_V5_TO_CEDA_V7_CODES
    )

    return SingleRegionUMatrixSet(
        Udom=pt.DataFrame[UMatrix](URdom), Uimp=pt.DataFrame[UMatrix](URimp)
    )


@functools.cache
def derive_v5_detail_Ytot_usa_matrix_set() -> SingleRegionYtotAndTradeVectorSet:
    """
    Derive US Ytot and trade vectors using v5 USA IO tables.

    NOTE: Ytot_usa can't be negative, because we need to use it to ABSR Ytot of other countries.
    """

    Ytot_with_trade_usa = load_2012_PC_usa().T @ load_2012_YR_usa()

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
            -1
            * Ytot_with_trade_usa[USA_2017_FINAL_DEMAND_IMPORT_CODE].apply(
                lambda x: min(x, 0)
            )
        ),
    )


@functools.cache
def derive_v7_detail_Ytot_usa_matrix_set() -> SingleRegionYtotAndTradeVectorSet:
    """
    Derive US Ytot and trade vectors using v7 USA IO tables.

    NOTE: Ytot_usa can't be negative, because we need to use it to ABSR Ytot of other countries.
    """
    detail_2017_YandTradeset = derive_2017_Ytot_usa_matrix_set()

    summary_to_ceda_corresp_df = get_bea_v2017_summary_to_ceda_corresp_df()

    summary_Y_matrix_set = derive_summary_Ytot_usa_matrix_set(year=2022)

    ytot = disaggregate_vector(
        base_series=summary_Y_matrix_set.ytot,
        weight_series=detail_2017_YandTradeset.ytot,
        corresp_df=summary_to_ceda_corresp_df,
    )
    exports = disaggregate_vector(
        base_series=summary_Y_matrix_set.exports,
        weight_series=detail_2017_YandTradeset.exports,
        corresp_df=summary_to_ceda_corresp_df,
    )
    imports = handle_negative_vector_values(
        disaggregate_vector(
            base_series=summary_Y_matrix_set.imports,
            weight_series=detail_2017_YandTradeset.imports,
            corresp_df=summary_to_ceda_corresp_df,
        )
    )

    return SingleRegionYtotAndTradeVectorSet(
        ytot=YVectorSchema.validate(ytot),
        exports=ExportsVectorSchema.validate(exports),
        imports=ImportsVectorSchema.validate(imports),
    )
