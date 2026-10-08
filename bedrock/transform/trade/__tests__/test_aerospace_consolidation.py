"""The aerospace activity consolidation, #701.

Census names a leaf for only 10.7% of 2017 aerospace exports and the mix of
that 10.7% is 39.6% away from the published export mix, so the leaf rows must
not stand while a family-proportional slice of ``33641X`` is added on top. What
these tests pin is that the fold happens, that it is a relabel rather than a
reallocation, and — the part that is easy to lose in a refactor — that it
applies to **exports only**, because the import side has no residual to split.
"""

from __future__ import annotations

import pandas as pd

from bedrock.extract.flowbyactivity import FlowByActivity
from bedrock.transform.trade.utilities import (
    AEROSPACE_CHILDREN,
    AEROSPACE_PARENT,
    VEHICLE_CHILDREN,
    VEHICLE_PARENT,
    consolidate_activities,
    consolidate_export_activities_fba,
    consolidate_vehicle_activities_fba,
)


def _fba(activities: list[str], amounts: list[float], flow: str) -> FlowByActivity:
    return FlowByActivity(
        pd.DataFrame(
            {
                'ActivityProducedBy': activities,
                'ActivityConsumedBy': [None] * len(activities),
                'FlowAmount': amounts,
                'FlowName': [flow] * len(activities),
                'Class': ['Money'] * len(activities),
                'Unit': ['USD'] * len(activities),
                'Location': ['00000'] * len(activities),
                'Year': [2017] * len(activities),
            }
        ),
        full_name='Census_USATrade',
        config={'year': 2017},
        convert_df_to_flowby=True,
    )


def test_every_aerospace_leaf_folds_onto_the_census_residual() -> None:
    amounts = [1.0] * len(AEROSPACE_CHILDREN)
    out = consolidate_activities(
        pd.DataFrame(
            {
                'ActivityProducedBy': list(AEROSPACE_CHILDREN),
                'FlowAmount': amounts,
            }
        ),
        AEROSPACE_CHILDREN,
        AEROSPACE_PARENT,
    )
    assert set(out['ActivityProducedBy']) == {AEROSPACE_PARENT}


def test_the_export_hook_consolidates_both_families() -> None:
    """One pass has to catch vehicles and aerospace, not whichever runs last."""
    fba = _fba(
        [*VEHICLE_CHILDREN, '336411', '336412', '325412'],
        [46.0, 11.0, 2.5, 2.4, 90.0],
        'ALL_VAL_YR_DOM',
    )
    out = pd.DataFrame(consolidate_export_activities_fba(fba))
    activities = set(out['ActivityProducedBy'].astype(str))
    assert VEHICLE_PARENT in activities
    assert AEROSPACE_PARENT in activities
    assert not activities & set(VEHICLE_CHILDREN)
    assert not activities & set(AEROSPACE_CHILDREN)
    # A relabel, not a reallocation.
    assert out['FlowAmount'].sum() == 151.9
    assert (
        out.loc[out['ActivityProducedBy'] == AEROSPACE_PARENT, 'FlowAmount'].sum()
        == 4.9
    )


def test_the_import_hook_leaves_aerospace_alone() -> None:
    """⚠️ There is no ``33641X`` on the import side.

    Census publishes every aerospace leaf directly on imports, so there is no
    residual to split and no reason to overwrite an observation; the level gap
    there is #670's. If this ever starts passing for aerospace, the import
    method has silently picked up the export hook.
    """
    fba = _fba(
        [*VEHICLE_CHILDREN, '336411', '336412'],
        [177.0, 18.0, 13.8, 21.4],
        'GEN_CIF_YR',
    )
    out = pd.DataFrame(consolidate_vehicle_activities_fba(fba))
    activities = set(out['ActivityProducedBy'].astype(str))
    assert VEHICLE_PARENT in activities
    assert AEROSPACE_PARENT not in activities
    assert {'336411', '336412'} <= activities


def test_the_hook_keeps_the_config() -> None:
    """A bare DataFrame out of a clean hook loses ``config`` and fails on year."""
    fba = _fba(['336411'], [2.5], 'ALL_VAL_YR_DOM')
    out = consolidate_export_activities_fba(fba)
    assert isinstance(out, FlowByActivity)
    assert out.config['year'] == 2017


def test_a_frame_with_neither_family_is_untouched() -> None:
    fba = _fba(['325412', '211000'], [90.0, 146.0], 'ALL_VAL_YR_DOM')
    out = pd.DataFrame(consolidate_export_activities_fba(fba))
    assert list(out['ActivityProducedBy']) == ['325412', '211000']
