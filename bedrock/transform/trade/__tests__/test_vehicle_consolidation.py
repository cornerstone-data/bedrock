"""The vehicle activity consolidation, #702.

Census's own ``336111`` / ``336112`` split is not BEA's, and Census drops the
children at 2023. Both halves of that are what these tests pin.
"""

from __future__ import annotations

import pandas as pd

from bedrock.transform.trade.utilities import (
    VEHICLE_CHILDREN,
    VEHICLE_PARENT,
    consolidate_vehicle_activities,
)


def _frame(activities: list[str], amounts: list[float]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            'ActivityProducedBy': activities,
            'FlowAmount': amounts,
            'FlowName': ['GEN_CIF_YR'] * len(activities),
        }
    )


def test_children_are_relabelled_onto_the_parent() -> None:
    """2017-2022 shape: both children present, neither survives."""
    out = consolidate_vehicle_activities(_frame(list(VEHICLE_CHILDREN), [177.0, 18.0]))
    assert set(out['ActivityProducedBy']) == {VEHICLE_PARENT}


def test_mass_is_preserved() -> None:
    """A relabel, not a reallocation - the pair total may not move."""
    frame = _frame([*VEHICLE_CHILDREN, '325412'], [177.0, 18.0, 90.0])
    out = consolidate_vehicle_activities(frame)
    assert out['FlowAmount'].sum() == frame['FlowAmount'].sum()
    assert (
        out.loc[out['ActivityProducedBy'] == VEHICLE_PARENT, 'FlowAmount'].sum()
        == 195.0
    )


def test_non_vehicle_activities_are_untouched() -> None:
    frame = _frame(['325412', '211000'], [90.0, 146.0])
    out = consolidate_vehicle_activities(frame)
    pd.testing.assert_frame_equal(out, frame)


def test_parent_only_year_is_a_no_op() -> None:
    """2023+ shape: Census already publishes only the parent."""
    frame = _frame([VEHICLE_PARENT], [237.0])
    out = consolidate_vehicle_activities(frame)
    pd.testing.assert_frame_equal(out, frame)


def test_rows_keep_their_flow_so_every_census_flow_consolidates_alike() -> None:
    """The duty rate and the mass it multiplies must consolidate the same way.

    ``duties.py`` reads ``CAL_DUT_YR`` and ``GEN_VAL_YR`` through this same
    function, so a relabel that collapsed rows across ``FlowName`` would build
    the rate on one commodity axis and apply it to another.
    """
    frame = pd.DataFrame(
        {
            'ActivityProducedBy': list(VEHICLE_CHILDREN) * 2,
            'FlowAmount': [177.0, 18.0, 2.3, 0.1],
            'FlowName': ['GEN_VAL_YR'] * 2 + ['CAL_DUT_YR'] * 2,
        }
    )
    out = consolidate_vehicle_activities(frame)
    by_flow = out.groupby('FlowName')['FlowAmount'].sum()
    assert by_flow['GEN_VAL_YR'] == 195.0
    assert round(float(by_flow['CAL_DUT_YR']), 6) == 2.4
    assert len(out) == len(frame)


def test_empty_frame_is_returned_unchanged() -> None:
    frame = _frame([], [])
    assert consolidate_vehicle_activities(frame).empty
