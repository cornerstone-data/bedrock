from __future__ import annotations

import logging
import typing as ta

import numpy as np
import pandas as pd

T = ta.TypeVar("T", bound=ta.Union[int, float])

logger = logging.getLogger(__name__)


def structural_reflect_matrix(
    row_corresp_df: pd.DataFrame,
    col_corresp_df: pd.DataFrame,
    df_base: pd.DataFrame,
    df_weights: pd.DataFrame,
    normalize: bool = True,
    fallback_df_weights: ta.Optional[pd.DataFrame] = None,
    expected_row_dropped: ta.Optional[ta.AbstractSet[str]] = None,
    expected_col_dropped: ta.Optional[ta.AbstractSet[str]] = None,
) -> pd.DataFrame:
    """
    Reshape df_base (a matrix), usually through a compbination of aggregation and disaggregation
    across rows using col_correspondance and columns using col_correspondance, weight,
    and alternative weight (i.e. q) dfs via "structural reflection".

    Structural reflection is a concept that takes the structure of one economy and imputes it
    on another to reshape to the target cardinality of structure. In the case of CEDA, our
    target cardinality is 400. Different countries have different industries that their IO
    tables are reported in. E.g. OECD countries report across 44 industries. We use the
    correspondence matrix to figure out which industries to disaggregate each of the 44 OECD
    industries into and use the weighting relationship from the US to disaggregate the IO
    tables into the CEDA structure reflecting the US makeup. In cases where the US does not
    have a weighting for a particular intersection, we use a fallback, which should be non-zero
    for all industries.

    row_corresp_df: a matrix of 1s and 0s that corresponds between the rows of target matrix
        to be reshaped into and the rows of df_base to be reshaped from
    col_corresp_df: a matrix of 1s and 0s that corresponds between the columns of target matrix
        to be reshaped into and the columns of df_base to be reshaped from
    df_base: a matrix of values to be reshaped
    df_weights: a matrix of weights to be used for structural reflection. This is the structure
        we are reflecting.
    normalize: a boolean to enable skipping normalization of df_weights when we want equal allocation.
    fallback_df_weights: a fallback option of weights to be used

    ! This function has been np-ified to speed up computation. !
    """
    # It is possible that source_df and target_df both have different index and columns,
    # meaning there can be two source index and two target index.
    # Therefore, we are not checking if the index and columns from correspondance dfs are equal.
    row_source_idx = row_corresp_df.columns
    row_target_idx = row_corresp_df.index
    col_source_idx = col_corresp_df.columns
    col_target_idx = col_corresp_df.index
    expected_row_dropped = expected_row_dropped or set()
    expected_col_dropped = expected_col_dropped or set()

    assert (df_base.index == row_source_idx).all()
    assert (df_base.columns == col_source_idx).all()
    assert (df_weights.index == row_target_idx).all()
    assert (df_weights.columns == col_target_idx).all()
    if fallback_df_weights is not None:
        assert (fallback_df_weights.index == row_target_idx).all()
        assert (fallback_df_weights.columns == col_target_idx).all()

    # Initialize the DataFrame for results
    structural_reflected = np.zeros(
        shape=(row_corresp_df.shape[0], col_corresp_df.shape[0])
    )

    # Convert weight_df to numpy array for faster computation
    weight_values = df_weights.values
    row_corresp_values = row_corresp_df.values
    col_corresp_values = col_corresp_df.values

    n_rows, n_cols = df_base.shape
    for i in range(n_rows):
        for j in range(n_cols):
            val = float(df_base.iat[i, j])  # type: ignore[arg-type]
            val_idx = df_base.index[i]
            val_col = df_base.columns[j]
            if val == 0:
                continue

            # Compute the outer product using row and column correspondence values
            rc_corresp_ij = np.outer(
                row_corresp_values[:, i],
                col_corresp_values[:, j],
            )

            # Case 1: Normal weights
            sr_m_ij = rc_corresp_ij * weight_values
            total_ij = sr_m_ij.sum().sum()
            if total_ij != 0:
                structural_reflected += val * sr_m_ij / (total_ij if normalize else 1)
                continue

            if fallback_df_weights is None:
                # okay to drop val if its index or column is expected to be dropped
                # here we only print warning if val isn't supposed to be dropped
                if val_idx in expected_row_dropped or val_col in expected_col_dropped:
                    continue
                logger.warning(
                    f"skipping reflection of {val} at ({val_idx}, {val_col}) due to no (weighted) correspondence"
                )
                continue

            # Case 2: Alternative weights
            alt_sr_m_ij = rc_corresp_ij * fallback_df_weights
            alt_total_ij = alt_sr_m_ij.sum().sum()
            if alt_total_ij != 0:
                structural_reflected += val * alt_sr_m_ij / alt_total_ij
            else:
                logger.warning(
                    "neither default nor fallback weight works, expect value losses"
                )

    # Validation and logging
    sr_sum = structural_reflected.sum().sum()

    # !!! Correspondence matrices may map some source sectors to 0 target sectors, effectively removing them and their weight.
    # Thus we expect to only retain part of the base matrix sum.

    base_sum = (
        df_base.loc[
            df_base.index.difference(
                row_corresp_df.columns[(row_corresp_df.sum(axis=0) < 1)]
            ),
            df_base.columns.difference(
                col_corresp_df.columns[(col_corresp_df.sum(axis=0) < 1)]
            ),
        ]
        .sum()
        .sum()
    )
    # only check sum if we are normalizing, because not normalizing means we propagate sr_m_ij as is,
    # which will increase totals in the SR-ed matrix that will vary case-by-case.
    if not np.isclose(sr_sum, base_sum, rtol=0.0001):
        # TODO: change this to raise RuntimeError after we are confident it won't break integration tests
        logger.warning(
            f"structural reflection failed base:{base_sum} != structural_reflected:{sr_sum} ... {sr_sum/base_sum:.2%}",
        )
    return pd.DataFrame(
        structural_reflected, index=row_corresp_df.index, columns=col_corresp_df.index
    )


def structural_reflect_symmetric(
    corresp_df: pd.DataFrame,
    df_base: pd.DataFrame,
    df_weights: pd.DataFrame,
    normalize: bool = True,
    fallback_df_weights: ta.Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    return structural_reflect_matrix(
        row_corresp_df=corresp_df,
        col_corresp_df=corresp_df,
        df_base=df_base,
        df_weights=df_weights,
        normalize=normalize,
        fallback_df_weights=fallback_df_weights,
    )


def structural_reflect_vector(
    corresp_df: pd.DataFrame,
    ser_base: pd.Series[float],
    ser_weights: pd.Series[float],
    normalize: bool = True,
    expected_row_dropped: ta.Optional[ta.AbstractSet[str]] = None,
) -> pd.Series[float]:
    base = ser_base.to_frame()
    weights = ser_weights.to_frame()
    expected_row_dropped = expected_row_dropped or set()

    base.columns = weights.columns = pd.Index(["__unused__"], name=None)  # fake index
    srm = structural_reflect_matrix(
        row_corresp_df=corresp_df,  # real one
        # TODO: I think this is a bug, but I don't know if it matters much to the results, will come back to this
        # it should be `np.eye(len(weights.columns))` instead of `1.0`
        col_corresp_df=pd.DataFrame(1.0, index=weights.columns, columns=base.columns),
        df_base=base,
        df_weights=weights,
        normalize=normalize,
        expected_row_dropped=expected_row_dropped,
    )

    assert srm.shape[1] == 1
    sr_ser = srm.squeeze()
    assert isinstance(sr_ser, pd.Series)
    sr_ser.name = None

    return sr_ser
