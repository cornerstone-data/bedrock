"""The final-demand summary conditioning (nowcast_fd_conditioning).

The invariants, on a constructed final-demand frame against the real
published summary workbook: conditioned columns aggregate to the published
summary allocation wherever a group is reachable, unconditioned columns and
zero cells never move, and the guards hold factors at 1 instead of producing
nonsense ratios.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from bedrock.extract.iot.io_2017 import _load_usa_summary_sut
from bedrock.transform.iot.nowcast_fd_conditioning import (
    CONDITIONED_COLUMNS,
    EMPTY_GROUP_USD,
    _commodity_to_summary,
    condition_fd_on_summary,
    summary_condition_factors,
)

YEAR = 2023


@pytest.fixture(scope='module')
def y() -> pd.DataFrame:
    """A synthetic final-demand block: every commodity, uneven positive mass
    with zeros sprinkled in, plus untouched F03000/F04000 columns."""
    codes = _commodity_to_summary().index
    rng = np.random.default_rng(7)
    frame = pd.DataFrame(
        rng.uniform(0.0, 5e9, size=(len(codes), len(CONDITIONED_COLUMNS))),
        index=codes,
        columns=list(CONDITIONED_COLUMNS),
    )
    frame.iloc[::7, 0] = 0.0  # zero cells must survive
    frame['F03000'] = 1.0e9
    frame['F04000'] = 2.0e9
    return frame


def test_reachable_groups_land_on_the_published_allocation(y: pd.DataFrame) -> None:
    out = condition_fd_on_summary(y, YEAR)
    groups = _commodity_to_summary()
    published = _load_usa_summary_sut('Use_SUT_summary', YEAR)  # type: ignore[arg-type]
    published = published.apply(pd.to_numeric, errors='coerce').fillna(0.0) * 1e6
    ours = out['F01000'].groupby(groups.reindex(out.index)).sum()
    pub = published['F010'].reindex(ours.index).fillna(0.0)
    raw = y['F01000'].groupby(groups.reindex(y.index)).sum()
    reachable = (raw.abs() >= EMPTY_GROUP_USD) & (np.sign(raw) == np.sign(pub))
    assert reachable.any()
    pd.testing.assert_series_equal(
        ours[reachable], pub[reachable], check_names=False, rtol=1e-9
    )


def test_unconditioned_columns_and_zero_cells_never_move(y: pd.DataFrame) -> None:
    out = condition_fd_on_summary(y, YEAR)
    pd.testing.assert_series_equal(out['F03000'], y['F03000'])
    pd.testing.assert_series_equal(out['F04000'], y['F04000'])
    was_zero = y['F01000'] == 0.0
    assert (out.loc[was_zero, 'F01000'] == 0.0).all()


def test_empty_and_sign_flipped_groups_hold_at_one(y: pd.DataFrame) -> None:
    frame = y.copy()
    groups = _commodity_to_summary()
    # empty a whole group and flip another's sign; both groups carry large
    # published equipment investment (machinery, motor vehicles), so without
    # the guards the factors would be inf-like or negative
    frame.loc[groups.reindex(frame.index) == '333', 'F02E00'] = 0.0
    frame.loc[groups.reindex(frame.index) == '3361MV', 'F02E00'] *= -1.0
    factors = summary_condition_factors(frame, YEAR)
    assert factors.loc['333', 'F02E00'] == 1.0
    assert factors.loc['3361MV', 'F02E00'] == 1.0


def test_years_outside_the_workbook_are_refused(y: pd.DataFrame) -> None:
    with pytest.raises(ValueError, match='summary Use SUT'):
        summary_condition_factors(y, 2025)
