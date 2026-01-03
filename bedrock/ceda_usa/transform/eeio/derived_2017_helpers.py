"""
U and V were separated from the rest of the derivations since
they are further upstream than other matrices and go into the
allocation model. This breaks the ciruclar dependency between
derivations of E, A, B, and q from the allocation model.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from bedrock.extract.iot.io_2017 import USA_2017_FINAL_DEMAND_INDEX
from bedrock.ceda_usa.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from bedrock.ceda_usa.utils.taxonomy.mappings.ceda_v7__ceda_v5 import (
    CEDA_V5_TO_CEDA_V7_CODES,
)
from bedrock.ceda_usa.utils.taxonomy.usa_taxonomy_correspondence_helpers import (
    load_usa_2017_commodity__ceda_v7_correspondence,
    load_usa_2017_industry__ceda_v7_correspondence,
)

# these sectors were presented in 2012 IO tables but aggregated to 1 sector in 2017 tables
# adopt their values in 2012 tables to use as weights
EXPANDED_SECTORS_2012_TO_2017 = pd.Index(["335221", "335222", "335224", "335228"])
AGGREGATED_SECTORS_2012_TO_2017 = pd.Index(["335220"])
EXPECTED_COMMODITIES_DROPPED = {
    # these commodities are intentionally not mapped to CEDA v7 sectors as they are not goods/services a company can buy
    "S00401",  # Scrap
    "S00402",  # Used and secondhand goods
    "S00300",  # Noncomparable imports
    "S00900",  # Rest of the world adjustment
}


def derive_2017_U_weight(U_2012: pd.DataFrame, U_2017: pd.DataFrame) -> pd.DataFrame:
    """
    This function derives Utot and Uimp matrix to be used in structurally reflect the original 2017 Utot and Uimp,
    therefore the weight matrix is primarily created based on 2017 U.
    However, there are a few cases where more granular weights are needed from 2012 U,
    so it was used as a complementary source of weights.
    Returned U_weight matrix should have CEDA_v7_commodity as index and CEDA_v7_industry as columns.
    """
    corresp_commodity = load_usa_2017_commodity__ceda_v7_correspondence()
    corresp_industry = load_usa_2017_industry__ceda_v7_correspondence()

    # use U_2017 as base weights
    U_weight_base = corresp_commodity.loc[:, U_2017.index] @ U_2017 @ corresp_industry.T
    U_weight = U_weight_base.copy()

    # modify a sector in U_2012 to match the sector in U_2017
    U_2012_mod = (
        U_2012.copy()
        .rename(CEDA_V5_TO_CEDA_V7_CODES, axis=0)
        .rename(CEDA_V5_TO_CEDA_V7_CODES, axis=1)
        .loc[U_weight.index, U_weight.columns]
    )

    # find the difference between the expanded sectors and the rest
    idx_unchanged = U_2012_mod.index.difference(EXPANDED_SECTORS_2012_TO_2017)
    col_unchanged = U_2012_mod.columns.difference(EXPANDED_SECTORS_2012_TO_2017)

    # allocate the aggregated values from U_2017 to the expanded sectors
    # and check totals to ensure the allocations are correct
    U_weight.loc[EXPANDED_SECTORS_2012_TO_2017, col_unchanged] = (
        U_2012_mod.loc[EXPANDED_SECTORS_2012_TO_2017, col_unchanged]
        .div(
            U_2012_mod.loc[EXPANDED_SECTORS_2012_TO_2017, col_unchanged].sum(axis=0),
            axis=1,
        )
        .fillna(1 / EXPANDED_SECTORS_2012_TO_2017.size)
    ).mul(
        U_2017.loc[AGGREGATED_SECTORS_2012_TO_2017, col_unchanged].squeeze(), axis=1  # type: ignore
    )
    assert np.isclose(
        U_weight.loc[EXPANDED_SECTORS_2012_TO_2017, col_unchanged].sum(axis=0),
        U_2017.loc[AGGREGATED_SECTORS_2012_TO_2017, col_unchanged].squeeze(),  # type: ignore
        atol=1e-3,
    ).all(), "Row allocations in U_weight have incorrect values for expanded sectors."

    U_weight.loc[idx_unchanged, EXPANDED_SECTORS_2012_TO_2017] = (
        U_2012_mod.loc[idx_unchanged, EXPANDED_SECTORS_2012_TO_2017]
        .div(
            U_2012_mod.loc[idx_unchanged, EXPANDED_SECTORS_2012_TO_2017].sum(axis=1),
            axis=0,
        )
        .fillna(1 / EXPANDED_SECTORS_2012_TO_2017.size)
    ).mul(
        U_2017.loc[idx_unchanged, AGGREGATED_SECTORS_2012_TO_2017].squeeze(), axis=0  # type: ignore
    )
    assert np.isclose(
        U_weight.loc[idx_unchanged, EXPANDED_SECTORS_2012_TO_2017].sum(axis=1),
        U_2017.loc[idx_unchanged, AGGREGATED_SECTORS_2012_TO_2017].squeeze(),  # type: ignore
        atol=1e-3,
    ).all(), (
        "Column allocations in U_weight have incorrect values for expanded sectors."
    )

    U_weight.loc[EXPANDED_SECTORS_2012_TO_2017, EXPANDED_SECTORS_2012_TO_2017] = (
        (
            U_2012_mod.loc[EXPANDED_SECTORS_2012_TO_2017, EXPANDED_SECTORS_2012_TO_2017]
            / U_2012_mod.loc[
                EXPANDED_SECTORS_2012_TO_2017, EXPANDED_SECTORS_2012_TO_2017
            ]
            .sum()
            .sum()
        ).fillna(0.0)
    ).mul(
        U_2017.loc[
            AGGREGATED_SECTORS_2012_TO_2017, AGGREGATED_SECTORS_2012_TO_2017
        ].squeeze()
    )
    assert np.isclose(
        U_weight.loc[EXPANDED_SECTORS_2012_TO_2017, EXPANDED_SECTORS_2012_TO_2017]
        .sum()
        .sum(),
        U_2017.loc[AGGREGATED_SECTORS_2012_TO_2017, AGGREGATED_SECTORS_2012_TO_2017].squeeze(),  # type: ignore
        atol=1e-3,
    ), "Core allocations in U_weight have incorrect values for expanded sectors."

    assert U_weight.shape == (
        corresp_commodity.shape[0],
        corresp_industry.shape[0],
    ), "U_weight has incorrect shape."
    assert not (U_weight.isna().any().any()), "U_weight has NaN values."
    assert U_weight.index.equals(
        pd.Index(CEDA_V7_SECTORS)
    ), f"U_weight has incorrect index: {U_weight.index.difference(CEDA_V7_SECTORS)} not CEDA v7 sectors."
    assert U_weight.columns.equals(
        pd.Index(CEDA_V7_SECTORS)
    ), f"U_weight has incorrect columns: {U_weight.columns.difference(CEDA_V7_SECTORS)} not CEDA v7 sectors."
    # here only check unchanged sectors as expanded sectors are already checked above
    assert np.isclose(
        U_weight.loc[idx_unchanged, col_unchanged].sum().sum(),
        U_weight_base.loc[idx_unchanged, col_unchanged].sum().sum(),
        atol=1e-3,
    ), "U_weight has incorrect sum."

    return U_weight


def derive_2017_V_weight(V_2012: pd.DataFrame, V_2017: pd.DataFrame) -> pd.DataFrame:
    """
    This function derives V matrix (Make) to be used in structurally reflect the original 2017 V (Make),
    therefore the weight matrix is primarily created based on 2017 V (Make).
    However, there are a few cases where more granular weights are needed from 2012 V (Make),
    so it was used as a complementary source of weights.
    Returned V_weight matrix should have CEDA_v7_commodity as index and CEDA_v7_industry as columns.
    """
    corresp_commodity = load_usa_2017_commodity__ceda_v7_correspondence()
    corresp_industry = load_usa_2017_industry__ceda_v7_correspondence()

    # use V_2017 as base weights
    V_weight_base = corresp_industry @ V_2017 @ corresp_commodity.T
    V_weight = V_weight_base.copy()

    # modify a sector in V_2012 to match the sector in V_2017
    V_2012_mod = (
        # transpose to make V_2012 (industry x commodity) compatible with V_2017 (commmodity x industry)
        V_2012.T.copy()
        .rename(CEDA_V5_TO_CEDA_V7_CODES, axis=0)
        .rename(CEDA_V5_TO_CEDA_V7_CODES, axis=1)
        .loc[V_weight.index, V_weight.columns]
    )

    # find the difference between the expanded sectors and the rest
    idx_unchanged = V_2012_mod.index.difference(EXPANDED_SECTORS_2012_TO_2017)
    col_unchanged = V_2012_mod.columns.difference(EXPANDED_SECTORS_2012_TO_2017)

    # allocate the aggregated values from V_2017 to the expanded sectors
    # and check totals to ensure the allocations are correct
    V_weight.loc[EXPANDED_SECTORS_2012_TO_2017, col_unchanged] = (
        V_2012_mod.loc[EXPANDED_SECTORS_2012_TO_2017, col_unchanged]
        .div(
            V_2012_mod.loc[EXPANDED_SECTORS_2012_TO_2017, col_unchanged].sum(axis=0),
            axis=1,
        )
        .fillna(1 / EXPANDED_SECTORS_2012_TO_2017.size)
    ).mul(
        V_2017.loc[AGGREGATED_SECTORS_2012_TO_2017, col_unchanged].squeeze(), axis=1  # type: ignore
    )
    assert np.isclose(
        V_weight.loc[EXPANDED_SECTORS_2012_TO_2017, col_unchanged].sum(axis=0),
        V_2017.loc[AGGREGATED_SECTORS_2012_TO_2017, col_unchanged].squeeze(),  # type: ignore
        atol=1e-3,
    ).all(), "Row allocations in V_weight have incorrect values for expanded sectors."

    V_weight.loc[idx_unchanged, EXPANDED_SECTORS_2012_TO_2017] = (
        V_2012_mod.loc[idx_unchanged, EXPANDED_SECTORS_2012_TO_2017]
        .div(
            V_2012_mod.loc[idx_unchanged, EXPANDED_SECTORS_2012_TO_2017].sum(axis=1),
            axis=0,
        )
        .fillna(1 / EXPANDED_SECTORS_2012_TO_2017.size)
    ).mul(
        V_2017.loc[idx_unchanged, AGGREGATED_SECTORS_2012_TO_2017].squeeze(), axis=0  # type: ignore
    )
    assert np.isclose(
        V_weight.loc[idx_unchanged, EXPANDED_SECTORS_2012_TO_2017].sum(axis=1),
        V_2017.loc[idx_unchanged, AGGREGATED_SECTORS_2012_TO_2017].squeeze(),  # type: ignore
        atol=1e-3,
    ).all(), (
        "Column allocations in V_weight have incorrect values for expanded sectors."
    )

    V_weight.loc[EXPANDED_SECTORS_2012_TO_2017, EXPANDED_SECTORS_2012_TO_2017] = (
        (
            V_2012_mod.loc[EXPANDED_SECTORS_2012_TO_2017, EXPANDED_SECTORS_2012_TO_2017]
            / V_2012_mod.loc[
                EXPANDED_SECTORS_2012_TO_2017, EXPANDED_SECTORS_2012_TO_2017
            ]
            .sum()
            .sum()
        ).fillna(0.0)
    ).mul(
        V_2017.loc[
            AGGREGATED_SECTORS_2012_TO_2017, AGGREGATED_SECTORS_2012_TO_2017
        ].squeeze()
    )
    assert np.isclose(
        V_weight.loc[EXPANDED_SECTORS_2012_TO_2017, EXPANDED_SECTORS_2012_TO_2017]
        .sum()
        .sum(),
        V_2017.loc[AGGREGATED_SECTORS_2012_TO_2017, AGGREGATED_SECTORS_2012_TO_2017].squeeze(),  # type: ignore
        atol=1e-3,
    ), "Core allocations in V_weight have incorrect values for expanded sectors."

    assert V_weight.shape == (
        corresp_commodity.shape[0],
        corresp_industry.shape[0],
    ), "V_weight has incorrect shape."
    assert not (V_weight.isna().any().any()), "V_weight has NaN values."
    assert V_weight.index.equals(
        pd.Index(CEDA_V7_SECTORS)
    ), f"V_weight has incorrect index: {V_weight.index.difference(CEDA_V7_SECTORS)} not CEDA v7 sectors."
    assert V_weight.columns.equals(
        pd.Index(CEDA_V7_SECTORS)
    ), f"V_weight has incorrect columns: {V_weight.columns.difference(CEDA_V7_SECTORS)} not CEDA v7 sectors."
    # here only check unchanged sectors as expanded sectors are already checked above
    assert np.isclose(
        V_weight.loc[idx_unchanged, col_unchanged].sum().sum(),
        V_weight_base.loc[idx_unchanged, col_unchanged].sum().sum(),
        atol=1e-3,
    ), "V_weight has incorrect sum."

    return V_weight


def derive_2017_scrap_weight(
    scrap_2012: pd.Series[float], scrap_2017: pd.Series[float]
) -> pd.Series[float]:
    """
    This function derives scrap matrix (Make) to be used in structurally reflect the original 2017 scrap (Make),
    therefore the weight matrix is primarily created based on 2017 scrap (Make).
    However, there are a few cases where more granular weights are needed from 2012 scrap (Make),
    so it was used as a complementary source of weights.
    Returned scrap_weight matrix should have CEDA_v7_commodity as index and CEDA_v7_industry as columns.
    """
    corresp_industry = load_usa_2017_industry__ceda_v7_correspondence()

    # use scrap_2017 as base weights
    scrap_weight_base = corresp_industry.loc[:, scrap_2017.index] @ scrap_2017
    scrap_weight = scrap_weight_base.copy()

    # modify a sector in scrap_2012 to match the sector in scrap_2017
    scrap_2012_mod = (
        # transpose to make scrap_2012 (industry x commodity) compatible with scrap_2017 (commmodity x industry)
        scrap_2012.copy().rename(CEDA_V5_TO_CEDA_V7_CODES, axis=0)[scrap_weight.index]
    )

    # find the difference between the expanded sectors and the rest
    idx_unchanged = scrap_2012_mod.index.difference(EXPANDED_SECTORS_2012_TO_2017)

    # allocate the aggregated values from scrap_2017 to the expanded sectors
    # and check totals to ensure the allocations are correct
    scrap_weight.loc[EXPANDED_SECTORS_2012_TO_2017] = (
        scrap_2012_mod.loc[EXPANDED_SECTORS_2012_TO_2017]
        .div(
            scrap_2012_mod.loc[EXPANDED_SECTORS_2012_TO_2017].sum(),
        )
        .fillna(1 / EXPANDED_SECTORS_2012_TO_2017.size)
    ).mul(
        scrap_2017.loc[AGGREGATED_SECTORS_2012_TO_2017].squeeze(),  # type: ignore
    )

    assert np.isclose(
        scrap_weight.loc[EXPANDED_SECTORS_2012_TO_2017].sum(),
        scrap_2017.loc[AGGREGATED_SECTORS_2012_TO_2017],
        atol=1e-3,
    ).all(), "Allocations in scrap_weight have incorrect values for expanded sectors."
    assert len(scrap_weight) == len(
        corresp_industry.index
    ), "scrap_weight has incorrect shape."
    assert not (scrap_weight.isna().any()), "scrap_weight has NaN values."
    assert scrap_weight.index.equals(
        pd.Index(CEDA_V7_SECTORS)
    ), f"scrap_weight has incorrect index: {scrap_weight.index.difference(CEDA_V7_SECTORS)} not CEDA v7 sectors."
    # here only check unchanged sectors as expanded sectors are already checked above
    assert np.isclose(
        scrap_weight.loc[idx_unchanged].sum(),
        scrap_weight_base.loc[idx_unchanged].sum(),
        atol=1e-3,
    ), "scrap_weight has incorrect sum."

    return scrap_weight


def derive_2017_Y_weight(Y_2012: pd.DataFrame, Y_2017: pd.DataFrame) -> pd.DataFrame:
    """
    This function derives Y matrix (Final Demand) to be used in structurally reflect the original 2017 Y (Final Demand),
    therefore the weight matrix is primarily created based on 2017 Y (Final Demand).
    However, there are a few cases where more granular weights are needed from 2012 Y (Final Demand),
    so it was used as a complementary source of weights.
    Returned Y_weight matrix should have CEDA_v7_commodity as index and CEDA_v7_industry as columns.
    """
    corresp_commodity = load_usa_2017_commodity__ceda_v7_correspondence()

    # use Y_2017 as base weights
    Y_weight_base = corresp_commodity.loc[:, Y_2017.index] @ Y_2017
    Y_weight = Y_weight_base.copy()

    # modify a sector in Y_2012 to match the sector in Y_2017
    Y_2012_mod = (
        Y_2012.copy()
        .rename(CEDA_V5_TO_CEDA_V7_CODES, axis=0)
        .loc[Y_weight.index, Y_weight.columns]
    )

    # find the difference between the expanded sectors and the rest
    idx_unchanged = Y_2012_mod.index.difference(EXPANDED_SECTORS_2012_TO_2017)

    # allocate the aggregated values from Y_2017 to the expanded sectors
    # and check totals to ensure the allocations are correct
    Y_weight.loc[EXPANDED_SECTORS_2012_TO_2017, :] = (
        Y_2012_mod.loc[EXPANDED_SECTORS_2012_TO_2017, :]
        .div(
            Y_2012_mod.loc[EXPANDED_SECTORS_2012_TO_2017, :].sum(axis=0),
            axis=1,
        )
        .fillna(1 / EXPANDED_SECTORS_2012_TO_2017.size)
    ).mul(
        Y_2017.loc[AGGREGATED_SECTORS_2012_TO_2017, :].squeeze(), axis=1  # type: ignore
    )
    assert np.isclose(
        Y_weight.loc[EXPANDED_SECTORS_2012_TO_2017, :].sum(axis=0),
        Y_2017.loc[AGGREGATED_SECTORS_2012_TO_2017, :].squeeze(),  # type: ignore
        atol=1e-3,
    ).all(), "Row allocations in Y_weight have incorrect values for expanded sectors."

    assert Y_weight.shape == (
        corresp_commodity.shape[0],
        Y_2017.shape[1],
    ), "Y_weight has incorrect shape."
    assert not (Y_weight.isna().any().any()), "Y_weight has NaN values."
    assert Y_weight.index.equals(
        pd.Index(CEDA_V7_SECTORS)
    ), f"Y_weight has incorrect index: {Y_weight.index.difference(CEDA_V7_SECTORS)} not CEDA v7 sectors."
    assert Y_weight.columns.equals(
        pd.Index(USA_2017_FINAL_DEMAND_INDEX)
    ), f"Y_weight has incorrect columns: {Y_weight.columns.difference(USA_2017_FINAL_DEMAND_INDEX)} not CEDA v7 sectors."
    # here only check unchanged sectors as expanded sectors are already checked above
    assert np.isclose(
        Y_weight.loc[idx_unchanged, :].sum().sum(),
        Y_weight_base.loc[idx_unchanged, :].sum().sum(),
        atol=1e-3,
    ), "Y_weight has incorrect sum."

    return Y_weight
