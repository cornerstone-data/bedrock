"""The S00300 pass-through in the IEA service-imports bridge (#766, #771).

``_bridge_iea_services`` sums the IEA leaves the imports crosswalk routes to
``S00300`` into one direct row - the fitted bridge itself never emits S-coded
rows, and the first version of the imports mirror dropped these leaves
entirely, zeroing the whole supply of the largest T11 residual row. The sum
construction is only exact while every one of those leaves maps to ``S00300``
alone; these tests pin that invariant on the committed crosswalk.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

import bedrock.extract.iot.io_2017 as io_2017
from bedrock.extract.flowbyactivity import FlowByActivity
from bedrock.transform.trade import utilities
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


def test_summary_controlled_rows_take_the_published_summary_split_by_2017(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """#658: controlled rows replace the bridge with the published summary row."""
    bridge = tmp_path / 'bridge.csv'
    pd.DataFrame(
        {
            'category': ['Cat', 'Cat'],
            'commodity': ['111000', '621100'],
            'share': [1.0, 1.0],
        }
    ).to_csv(bridge, index=False)
    detail = pd.DataFrame(
        {'F04000': [10.0, 30.0, 10.0]}, index=pd.Index(['111000', '621100', '621200'])
    )
    monkeypatch.setattr(io_2017, '_load_2017_detail_supply_use_usa', lambda _t: detail)
    monkeypatch.setattr(
        utilities,
        '_published_summary_trade',
        lambda flow, year: pd.Series({'621': 100.0}),
    )
    fba = FlowByActivity(
        pd.DataFrame(
            {
                'FlowName': ['Exports'],
                'ActivityProducedBy': ['Cat'],
                'FlowAmount': [5e6],
                'Year': [2017],
            }
        ),
        full_name='BEA_IEA',
        config={},
    )
    out = pd.DataFrame(
        utilities._bridge_iea_services(
            fba,
            flow='Exports',
            bridge_csv=str(bridge),
            anchor_table='Use_SUT_detail',
            anchor_column='F04000',
            summary_controls={'621': ('621100', '621200')},
        )
    ).set_index('ActivityProducedBy')['FlowAmount']
    # 621100 leaves the bridge for 75/25 of the summary row by 2017 F04000;
    # 621200, which the bridge never fed, gets its share too.
    assert out.to_dict() == pytest.approx(
        {'111000': 10e6, '621100': 75e6, '621200': 25e6}
    )


# --- suppressed IEA categories (#904, #1172) ---------------------------------

NAN = float('nan')


def _filled(values: list[float]) -> list[float]:
    panel = pd.DataFrame(
        [values], index=['leaf'], columns=range(2019, 2019 + len(values))
    )
    return utilities.interpolate_suppressed(panel).iloc[0].tolist()


def test_interior_gap_is_geometric_between_positive_endpoints() -> None:
    assert _filled([100.0, NAN, NAN, 800.0]) == pytest.approx(
        [100.0, 200.0, 400.0, 800.0]
    )


def test_interior_gap_with_a_zero_endpoint_is_linear() -> None:
    assert _filled([30.0, NAN, 0.0]) == pytest.approx([30.0, 15.0, 0.0])


def test_edge_gaps_and_unobserved_rows_stay_suppressed() -> None:
    assert _filled([NAN, 5.0, 6.0, NAN]) == pytest.approx(
        [NAN, 5.0, 6.0, NAN], nan_ok=True
    )
    assert all(pd.isna(_filled([NAN, NAN, NAN])))


def test_only_categories_the_fba_flags_as_suppressed_are_filled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A published zero stays zero; a flagged one is filled from its neighbours."""
    panel = pd.DataFrame(
        {2020: [100.0, 0.0], 2021: [NAN, NAN], 2022: [400.0, 0.0]},
        index=['Suppressed', 'TrueZero'],
    )
    monkeypatch.setattr(utilities, '_iea_published_panel', lambda flow: panel)
    fba = pd.DataFrame(
        {
            'FlowName': ['Imports', 'Imports'],
            'ActivityProducedBy': ['Suppressed', 'TrueZero'],
            'FlowAmount': [0.0, 0.0],
            'Suppressed': ['(D)', None],
        }
    )
    now = utilities._iea_category_totals(fba, 'Imports')

    out = utilities._fill_suppressed_categories(now, fba, 'Imports', 2021)

    assert out.to_dict() == pytest.approx({'Suppressed': 200.0, 'TrueZero': 0.0})
