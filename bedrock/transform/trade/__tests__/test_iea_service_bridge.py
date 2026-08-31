"""The S00300 pass-through in the IEA service-imports bridge (#766, #771).

``_bridge_iea_services`` sums the IEA leaves the imports crosswalk routes to
``S00300`` into one direct row - the fitted bridge itself never emits S-coded
rows, and the first version of the imports mirror dropped these leaves
entirely, zeroing the whole supply of the largest T11 residual row. The sum
construction is only exact while every one of those leaves maps to ``S00300``
alone; these tests pin that invariant on the committed crosswalk.
"""

from __future__ import annotations

import pandas as pd

from bedrock.transform.trade.utilities import IEA_IMPORTS_CROSSWALK_CSV


def _crosswalk() -> pd.DataFrame:
    return pd.read_csv(IEA_IMPORTS_CROSSWALK_CSV, dtype=str)


def test_noncomparable_leaves_exist() -> None:
    crosswalk = _crosswalk()
    leaves = set(crosswalk.loc[crosswalk['Sector'] == 'S00300', 'Activity'])
    assert leaves, 'no IEA leaves route to S00300 - the pass-through is dead'


def test_noncomparable_leaves_map_to_s00300_alone() -> None:
    """A dual-mapped leaf would make the plain-sum pass-through over-count."""
    crosswalk = _crosswalk()
    leaves = set(crosswalk.loc[crosswalk['Sector'] == 'S00300', 'Activity'])
    other = crosswalk[
        crosswalk['Activity'].isin(leaves) & (crosswalk['Sector'] != 'S00300')
    ]
    assert other.empty, (
        'these S00300 leaves also map to commodities, so summing their full '
        f'value into S00300 double-counts: {sorted(other["Activity"])}'
    )
