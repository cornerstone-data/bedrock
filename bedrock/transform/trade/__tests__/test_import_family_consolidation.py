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


def test_the_export_residual_folds_to_its_family_on_the_import_side() -> None:
    """⚠️ Not a contradiction with #865 - the two directions use different hooks.

    ``33641X``'s targets all sit in ``3364``, so the *import* consolidation
    would fold it like any other activity. It never arises, because Census
    publishes no ``33641X`` on imports; exports keep their own hook, which
    folds the aerospace leaves onto ``33641X`` rather than onto ``3364``.
    """
    assert census_family_parents()[AEROSPACE_PARENT] == '3364'


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
