from __future__ import annotations

import functools
import logging

import pandas as pd
import pandera.typing as pt

from bedrock.extract.iot.io_2012 import (
    load_2012_PC_usa,
    load_2012_PI_usa,
    load_2012_UR_usa,
    load_2012_URdom_usa,
    load_2012_YR_usa,
)
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_B_non_finetuned,
    derive_cornerstone_detail_Ytot_matrix_set,
    derive_cornerstone_Y_and_trade_scaled,
    derive_cornerstone_y_nab,
    derive_cornerstone_ydom_and_yimp,
)
from bedrock.utils.emissions.characterization import build_ghg_characterization_matrix
from bedrock.utils.emissions.ghg import GHG
from bedrock.utils.math.handle_negatives import handle_negative_vector_values
from bedrock.utils.schemas.single_region_schemas import (
    ExportsVectorSchema,
    ImportsVectorSchema,
    UMatrix,
    YVectorSchema,
)
from bedrock.utils.schemas.single_region_types import (
    SingleRegionAqMatrixSet,
    SingleRegionUMatrixSet,
    SingleRegionYtotAndTradeVectorSet,
    SingleRegionYVectorSet,
)
from bedrock.utils.taxonomy.bea.v2017_final_demand import (
    USA_2017_FINAL_DEMAND_EXPORT_CODE,
    USA_2017_FINAL_DEMAND_IMPORT_CODE,
)
from bedrock.utils.taxonomy.mappings.ceda_v7__ceda_v5 import CEDA_V5_TO_CEDA_V7_CODES

logger = logging.getLogger(__name__)


@functools.cache
def derive_B_usa_non_finetuned() -> pd.DataFrame:
    return derive_cornerstone_B_non_finetuned()


def derive_Y_and_trade_matrix_usa_from_summary_target_year_ytot_and_structural_reflection() -> (
    SingleRegionYtotAndTradeVectorSet
):
    """Detail Y and trade vectors, structurally reflected to the target year."""
    return derive_cornerstone_Y_and_trade_scaled()


@functools.cache
def derive_y_for_national_accounting_balance_usa() -> pd.Series[float]:
    """y for the national accounting balance: y_nab = (y_tot - y_imp) + exports."""
    return derive_cornerstone_y_nab()


def derive_ydom_and_yimp_usa() -> SingleRegionYVectorSet:
    """ydom and yimp split, used to populate diagonal/off-diagonal Y_oecd."""
    return derive_cornerstone_ydom_and_yimp()


@functools.cache
def derive_Aq_usa() -> SingleRegionAqMatrixSet:
    """Year-scaled and inflated A matrices and q (model_base_year USD)."""
    return derive_cornerstone_Aq_scaled()


@functools.cache
def derive_C_usa() -> pd.DataFrame:
    """Trivial `(1, |GHG|)` row-summer for cornerstone B (already in CO2e).

    KNOWN DIVERGENCE FROM USEEIOR: bedrock B is in `kgCO2e/USD` while
    useeior B is in physical `kg gas/USD`. See
    `bedrock.utils.emissions.characterization` for the resolution path.
    """
    return build_ghg_characterization_matrix(list(GHG))


@functools.cache
def derive_D_usa() -> pd.DataFrame:
    """Direct impact per commodity = `C @ B` -- here equivalent to `B.sum(axis=0)`.

    KNOWN DIVERGENCE FROM USEEIOR: bedrock's D is structurally a single
    `Greenhouse Gases` indicator over commodities because bedrock's B is
    already CO2e. See `bedrock.utils.emissions.characterization`.
    """
    D = derive_C_usa() @ derive_B_usa_non_finetuned()
    D.columns.name = 'sector'
    return D


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
    """US Ytot and trade vectors (non-negative ytot, used to ABSR other countries)."""
    return derive_cornerstone_detail_Ytot_matrix_set()
