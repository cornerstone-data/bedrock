from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger(__name__)


def split_vector_using_agg_ratio(
    base_series: pd.Series[float],
    agg_ratio_series: pd.Series[float],
    corresp_df: pd.DataFrame,
) -> tuple[pd.Series[float], pd.Series[float]]:
    """
    Split a vector into two vectors based on an aggregated vector of ratios.
    """
    assert corresp_df.isin([0, 1]).all().all(), "correspondence matrix must be binary"
    assert (agg_ratio_series >= 0).all() and (
        agg_ratio_series <= 1
    ).all(), "aggregated ratio vector must be between 0 and 1"
    assert (
        agg_ratio_series.index == corresp_df.columns
    ).all(), "aggregated ratio index must have the same sectors as the correspondence matrix columns"

    agg_ratio_broadcasted_to_detail_sectors = corresp_df.mul(
        agg_ratio_series, axis=1
    ).sum(axis=1)

    portion_1_series = base_series * agg_ratio_broadcasted_to_detail_sectors
    portion_2_series = base_series * (1 - agg_ratio_broadcasted_to_detail_sectors)

    return portion_1_series, portion_2_series
