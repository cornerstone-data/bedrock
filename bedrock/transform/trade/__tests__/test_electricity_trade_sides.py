"""Dollarized electricity trade must put its commodity on the produced side (#889).

``dollarize_electricity_trade_fba`` builds the two directions in one loop, and
the export row used to be written as the mirror of the import row -- Activity on
the consumed side. That reads as symmetry but is wrong for an export: the
crosswalk maps both Activities to ``221100`` with ``SectorType`` ``C``, so on the
export row the commodity landed in ``SectorConsumedBy``, where the activity set's
``assign_sector_consumed_by_from_clean_parameter`` overwrote it with ``F04000``.
``SectorProducedBy`` was left null, and ``groupby('SectorProducedBy')`` drops null
keys, so electricity exports never reached a commodity in any of the eight years.

The loss ran $174m (2017) to $1,436m (2023) against a $1.9-2.5tn exports column --
under one part in two thousand, which is why no column-total check caught it.
These tests pin the side assignment rather than any dollar amount.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

import bedrock.utils.mapping as mapping
from bedrock.extract.flowbyactivity import FlowByActivity
from bedrock.transform.trade.utilities import (
    ELECTRICITY_EXPORTS_ACTIVITY,
    ELECTRICITY_IMPORTS_ACTIVITY,
    dollarize_electricity_trade_fba,
)

_CROSSWALK = (
    Path(mapping.__file__).resolve().parent
    / 'activitytosectormapping'
    / 'Sector_Crosswalk_EIA_ElectricPowerAnnual.csv'
)


def test_both_activities_map_to_the_electricity_commodity_as_a_commodity() -> None:
    """The premise of the defect: SectorType ``C`` sends the code to whichever
    side the Activity was written on, so the side assignment is what decides
    whether the row survives."""
    crosswalk = pd.read_csv(_CROSSWALK, dtype=str)
    rows = crosswalk.set_index('Activity')
    for activity in (ELECTRICITY_EXPORTS_ACTIVITY, ELECTRICITY_IMPORTS_ACTIVITY):
        assert rows.at[activity, 'Sector'] == '221100'
        assert rows.at[activity, 'SectorType'] == 'C'


def _fba() -> FlowByActivity:
    """Two Table 2.14 rows, one per direction, in the shape the clean sees."""
    return FlowByActivity(
        pd.DataFrame(
            {
                'FlowName': ['electricity exports', 'electricity imports'],
                'Description': ['EIA Table 2.14 CA', 'EIA Table 2.14 CA'],
                'FlowAmount': [1.0, 2.0],
                'Unit': ['MWh', 'MWh'],
                'Class': ['Energy', 'Energy'],
                'Location': ['00000', '00000'],
                'ActivityProducedBy': [None, None],
                'ActivityConsumedBy': [None, None],
                'Year': [2022, 2022],
            }
        ),
        full_name='EIA_ElectricPowerAnnual.electricity',
        config={'year': 2022},
        convert_df_to_flowby=True,
    )


def _dollarized() -> pd.DataFrame:
    return pd.DataFrame(dollarize_electricity_trade_fba(_fba()))


def test_export_row_carries_the_commodity_on_the_produced_side() -> None:
    """The regression itself. A null here is dropped by the commodity groupby."""
    out = _dollarized()
    export = out[out['FlowName'] == 'electricity exports'].iloc[0]
    assert export['ActivityProducedBy'] == ELECTRICITY_EXPORTS_ACTIVITY, (
        'the export commodity must be on the produced side; on the consumed side '
        'the activity set overwrites it with F04000 and the row is lost (#889)'
    )
    assert export['ActivityConsumedBy'] is None or pd.isna(
        export['ActivityConsumedBy']
    ), 'the consumed side is the activity set\'s to fill with F04000'


def test_import_row_is_unchanged() -> None:
    out = _dollarized()
    imports = out[out['FlowName'] == 'electricity imports'].iloc[0]
    assert imports['ActivityProducedBy'] == ELECTRICITY_IMPORTS_ACTIVITY
    assert imports['ActivityConsumedBy'] is None or pd.isna(
        imports['ActivityConsumedBy']
    )


def test_neither_direction_leaves_the_produced_side_empty() -> None:
    """Stated as the invariant rather than per-direction, so a future third
    direction cannot reintroduce the defect quietly."""
    out = _dollarized()
    assert out['ActivityProducedBy'].notna().all(), (
        'every dollarized trade row must reach a commodity through the produced '
        f'side; got {out[["FlowName", "ActivityProducedBy"]].to_dict("records")}'
    )
