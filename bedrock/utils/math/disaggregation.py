from __future__ import annotations

import logging
import math
import typing as ta

import pandas as pd

T = ta.TypeVar("T", bound=ta.Union[int, float])


logger = logging.getLogger(__name__)


def disaggregate_vector(
    corresp_df: pd.DataFrame,
    base_series: pd.Series[T],  # aggregated
    weight_series: pd.Series[T],  # disaggregated
    alt_weight_series: ta.Optional[pd.Series[T]] = None,
) -> pd.Series[T]:
    """
    disaggregate base_ser (a vector) using correspondance and weight
    """
    assert corresp_df.isin([0, 1]).all().all(), "correspondence matrix must be binary"
    assert (
        max(corresp_df.sum(axis=1)) == 1
    ), "correspondence matrix must map each sector to at most one target sector"
    assert (corresp_df.index == weight_series.index).all()
    assert (corresp_df.columns == base_series.index).all()

    # apply weights to corresp — then make sure that column sums are 1
    weighted_corresp = corresp_df.multiply(weight_series, axis=0)

    zero_idx = weighted_corresp.sum(axis=0) == 0

    weighted_corresp.loc[:, zero_idx] = (
        corresp_df.loc[:, zero_idx]
        if alt_weight_series is None
        else corresp_df.multiply(alt_weight_series, axis=0).loc[:, zero_idx]
    )

    if not (weighted_corresp.sum(axis=0) > 0).all():
        logger.warning(
            "during disaggregation: some weighted corresp columns have zero weight "
        )

    # ? (methodological improvement):
    # ? instead of normalizing, we could use a alternative weight like q
    weighted_normed_corresp = weighted_corresp.divide(
        weighted_corresp.sum(axis=0), axis=1
    ).fillna(0)

    if not ((weighted_normed_corresp.sum(axis=0) - 1) < 1e-6).all():
        msg = "weighted_normed_corresp column sums are not 1"
        raise RuntimeError(msg)

    disaggd = weighted_normed_corresp @ base_series

    # validation
    disaggd_sum = disaggd.sum()
    base_sum = base_series.sum()

    if not math.isclose(disaggd_sum, base_sum, rel_tol=0.001):
        msg = f"vector disaggregation is not close {disaggd_sum} != {base_sum}"
        raise RuntimeError(msg)

    return disaggd
