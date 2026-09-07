"""The import family consolidation, #763.

#763 validated the construction on the 2012 holdout and closed without wiring
it. What these tests pin is the wiring: that a family folds onto its parent,
that the parent is reachable on the crosswalk, that mass never crosses a family
boundary, and that the duty rate is built on the same commodity axis it is
applied to.
"""

from __future__ import annotations

import pandas as pd

from bedrock.extract.flowbyactivity import FlowByActivity
from bedrock.transform.trade.utilities import (
    AEROSPACE_PARENT,
    CENSUS_CROSSWALK_CSV,
    FAMILY_PARENT_NOTE,
    census_family_parents,
    consolidate_import_activities,
    consolidate_import_activities_fba,
)


def _fba(activities: list[str], amounts: list[float]) -> FlowByActivity:
    return FlowByActivity(
        pd.DataFrame(
            {
                'ActivityProducedBy': activities,
                'ActivityConsumedBy': [None] * len(activities),
                'FlowAmount': amounts,
                'FlowName': ['GEN_CIF_YR'] * len(activities),
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


def test_a_family_folds_onto_its_four_digit_parent() -> None:
    parents = census_family_parents()
    # The furniture family is the worst mix offender on the current build.
    assert parents['337121'] == '3371'
    assert parents['337122'] == '3371'
    assert parents['337127'] == '3371'


def test_vehicles_are_now_just_one_family() -> None:
    """#702's pair, and the parent-only code Census publishes from 2023."""
    parents = census_family_parents()
    assert parents['336111'] == '3361'
    assert parents['336112'] == '3361'
    assert parents['336110'] == '3361'


def test_every_parent_is_reachable_on_the_crosswalk() -> None:
    """A relabel onto an activity the crosswalk does not carry drops the mass."""
    frame = pd.read_csv(CENSUS_CROSSWALK_CSV, dtype=str).fillna('')
    known = set(frame.loc[frame['Note'] == FAMILY_PARENT_NOTE, 'Activity'])
    assert known, 'no generated parent rows in the crosswalk'
    assert set(census_family_parents().values()) <= known


def test_no_parent_ever_leaves_its_own_family() -> None:
    """⚠️ This construction may only move mass *within* a family."""
    frame = pd.read_csv(CENSUS_CROSSWALK_CSV, dtype=str).fillna('')
    parents = frame[frame['Note'] == FAMILY_PARENT_NOTE]
    outside = parents[
        [str(s)[:4] != str(a) for a, s in zip(parents['Activity'], parents['Sector'])]
    ]
    assert outside.empty, outside.to_string()


def test_a_family_whose_level_is_wrong_is_left_alone() -> None:
    """⚠️ The construction moves mass *by the Census family level*.

    When that level is wrong the premise fails, and re-splitting smears one
    leaf's level error over siblings that were fine. ``3399`` is the case that
    forced the guard: 88% of its excess sat on ``339910`` at 2.93x published,
    and the unguarded re-split dragged five leaves from within 9% of published
    to 1.50x. ``3259`` is the extreme - ``325910`` at 55x.
    """
    parents = set(census_family_parents().values())
    for family in ('3399', '3259', '3344', '3332', '3353', '3359', '3364'):
        assert family not in parents, family


def test_scrap_and_used_goods_are_not_a_family() -> None:
    """``S00401`` / ``S00402`` share a prefix, not a product concept (#703, #768)."""
    parents = census_family_parents()
    assert not any(f.startswith('S00') for f in parents.values())


def test_single_leaf_families_get_no_parent() -> None:
    """`3346` folds three Census codes onto one commodity - #670's level problem.

    There is nothing to split, so it must not acquire a parent; giving it one
    would dress a level error as a mix error.
    """
    assert '3346' not in set(census_family_parents().values())


def test_mass_is_preserved_and_a_parentless_family_is_untouched() -> None:
    # 334613 is in 3346, which folds three Census codes onto one commodity and
    # so gets no parent - #670's level problem, deliberately out of reach here.
    frame = pd.DataFrame(
        {
            'ActivityProducedBy': ['337121', '337122', '334613'],
            'FlowAmount': [6.0, 12.0, 90.0],
        }
    )
    out = consolidate_import_activities(frame)
    assert out['FlowAmount'].sum() == frame['FlowAmount'].sum()
    assert out.loc[out['ActivityProducedBy'] == '3371', 'FlowAmount'].sum() == 18.0
    assert '334613' in set(out['ActivityProducedBy'])


def test_the_two_directions_fold_aerospace_differently() -> None:
    """⚠️ Not a contradiction with #865 - different hooks, different evidence.

    Exports fold the aerospace leaves onto ``33641X`` because Census assigns a
    leaf to only 10.7% of aerospace export mass. Imports do **not** fold
    ``3364`` at all: Census publishes every leaf directly there, and the family
    level is 1.37, which fails the guard - so it stays #670's.
    """
    parents = census_family_parents()
    assert AEROSPACE_PARENT not in parents
    assert '3364' not in set(parents.values())


def test_the_hook_keeps_the_config() -> None:
    out = consolidate_import_activities_fba(_fba(['337121'], [6.0]))
    assert isinstance(out, FlowByActivity)
    assert out.config['year'] == 2017


def test_duties_folds_families_the_same_way() -> None:
    """⚠️ The duty rate multiplies the FBS's own mass.

    If one side folds families and the other does not, the rate is built on one
    commodity axis and applied to another.
    """
    import bedrock.transform.trade.duties as duties  # noqa: PLC0415

    assert duties.consolidate_import_activities is consolidate_import_activities
