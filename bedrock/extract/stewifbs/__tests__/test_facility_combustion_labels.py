"""GHGRP fuel labeling and pre-2021 SCC gate — core facility_combustion contracts."""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from bedrock.extract.stewifbs import facility_combustion as fc


def test_nei_onsite_scc_gate_labels_only_when_use_scc() -> None:
    """Contemporaneous SCC digits only when use_scc; else everything is Other."""
    nei_raw = pd.DataFrame(
        {
            'FacilityID': ['A'],
            'FlowName': ['Carbon Dioxide'],
            'Process': ['10200601'],  # external NG
            'FlowAmount': [10.0],
        }
    )
    sectors = pd.DataFrame(
        {'NAICS': ['324110'], 'State': ['TX'], 'sector': ['324110']},
        index=['A'],
    )
    labeled = fc.nei_onsite_co2(nei_raw.copy(), sectors, use_scc=True)
    gated = fc.nei_onsite_co2(nei_raw.copy(), sectors, use_scc=False)
    assert list(labeled['Flowable']) == ['Natural Gas']
    assert list(gated['Flowable']) == ['Other']
    assert float(gated['CO2e'].sum()) == float(labeled['CO2e'].sum()) == 10.0


def test_ghgrp_fuel_labels_subpart_w_plant_and_nei_frs_shares(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Subpart W + plant NG label first; remainder takes NEI FRS fuel shares."""
    year = 2022
    monkeypatch.setattr(
        fc.ghgrp_subpart_w,
        'subpart_W_combustion',
        lambda *_: pd.DataFrame(
            {
                'FacilityID': ['W1'],
                'fuel_type': ['Natural Gas'],
                'fuel_class': ['purchased'],
                'CO2e': [80.0],
            }
        ),
    )
    monkeypatch.setattr(
        fc.ghgrp_subpart_w,
        'self_supplying_facilities',
        lambda *_: {'P1'},
    )

    ghgrp = pd.DataFrame(
        [
            {
                'FacilityID': 'W1',
                'FRS_ID': 'FRS_W',
                'CO2e': 100.0,
                'fuel_class': 'unclassified',
                'Flowable': 'Other',
                'sector': '211000',
                'NAICS': '211111',
                'State': 'TX',
                'source': 'GHGRP',
            },
            {
                'FacilityID': 'P1',
                'FRS_ID': 'FRS_P',
                'CO2e': 50.0,
                'fuel_class': 'unclassified',
                'Flowable': 'Other',
                'sector': '211000',
                'NAICS': '211112',
                'State': 'TX',
                'source': 'GHGRP',
            },
            {
                'FacilityID': 'R1',
                'FRS_ID': 'FRS_R',
                'CO2e': 40.0,
                'fuel_class': 'unclassified',
                'Flowable': 'Other',
                'sector': '324110',
                'NAICS': '324110',
                'State': 'LA',
                'source': 'GHGRP',
            },
        ]
    )
    per_facility = ghgrp.set_index('FacilityID')['CO2e']
    subpart_c = pd.Series({'P1': 45.0, 'W1': 100.0, 'R1': 40.0}, name='CO2e')
    subpart_c.index.name = 'FacilityID'

    nei = pd.DataFrame(
        [
            {
                'FacilityID': 'N1',
                'FRS_ID': 'FRS_R',
                'Flowable': 'Coal',
                'fuel_class': 'purchased',
                'CO2e': 25.0,
                'sector': '324110',
                'NAICS': '324110',
                'source': 'NEI',
            },
            {
                'FacilityID': 'N1',
                'FRS_ID': 'FRS_R',
                'Flowable': 'Petroleum',
                'fuel_class': 'purchased',
                'CO2e': 75.0,
                'sector': '324110',
                'NAICS': '324110',
                'source': 'NEI',
            },
        ]
    )

    out = fc.ghgrp_fuel_labels(ghgrp, nei, per_facility, subpart_c, year, use_scc=True)
    by_id = out.groupby('FacilityID')

    # W: 80 NG from subpart W + 20 process Other remainder of facility total.
    w = by_id.get_group('W1')
    assert abs(float(w['CO2e'].sum()) - 100.0) < 1e-9
    assert abs(float(w.loc[w['Flowable'] == 'Natural Gas', 'CO2e'].sum()) - 80.0) < 1e-9

    # Plant: self_supplied NG from subpart C (45), plus process Other (5).
    p = by_id.get_group('P1')
    assert list(p.loc[p['fuel_class'] == 'self_supplied', 'Flowable']) == [
        'Natural Gas'
    ]
    assert (
        abs(float(p.loc[p['fuel_class'] == 'self_supplied', 'CO2e'].sum()) - 45.0)
        < 1e-9
    )

    # Remainder R1 split 25/75 Coal/Petroleum from NEI FRS shares.
    r = by_id.get_group('R1')
    assert abs(float(r.loc[r['Flowable'] == 'Coal', 'CO2e'].sum()) - 10.0) < 1e-9
    assert abs(float(r.loc[r['Flowable'] == 'Petroleum', 'CO2e'].sum()) - 30.0) < 1e-9

    # Without SCC and with NEI still Other, remainder keeps Other (no FRS fuels).
    nei_other = nei.assign(Flowable='Other', fuel_class='unclassified')
    gated = fc.ghgrp_fuel_labels(
        ghgrp, nei_other, per_facility, subpart_c, year, use_scc=False
    )
    r_gated = gated.loc[gated['FacilityID'] == 'R1']
    assert set(r_gated['Flowable']) == {'Other'}
    assert abs(float(r_gated['CO2e'].sum()) - 40.0) < 1e-9


def test_build_applies_pre2021_twins_when_nei_before_scc_year(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """nei_year < 2021 runs twin Flowable apply; 2021+ does not."""
    calls: list[int] = []

    def fake_apply(
        rows: pd.DataFrame, tables: dict[str, Any] | None = None
    ) -> pd.DataFrame:
        calls.append(1)
        return rows

    monkeypatch.setattr(fc, 'apply_pre2021_fuel_flowable_shares', fake_apply)
    monkeypatch.setattr(
        fc.stewi,
        'getInventory',
        lambda inventory, year, **k: pd.DataFrame(
            {
                'FacilityID': ['F1'],
                'Process': ['C'] if inventory == 'GHGRP' else ['10200601'],
                'FlowName': (
                    ['Carbon Dioxide'] if inventory == 'NEI' else ['Carbon Dioxide']
                ),
                'FlowAmount': [1.0],
            }
        ),
    )
    empty_sectors = (
        pd.DataFrame(columns=['NAICS', 'State', 'sector']).set_index(
            pd.Index([], name='FacilityID')
        ),
        pd.DataFrame(columns=['NAICS', 'State', 'sector']).set_index(
            pd.Index([], name='FacilityID')
        ),
    )
    monkeypatch.setattr(fc, 'facility_sectors', lambda *a, **k: empty_sectors)
    monkeypatch.setattr(
        fc.facilitymatcher,
        'get_matches_for_inventories',
        lambda *_: pd.DataFrame(columns=['FacilityID', 'Source', 'FRS_ID']),
    )
    monkeypatch.setattr(
        fc,
        'ghgrp_fuel_labels',
        lambda ghgrp, *a, **k: ghgrp.assign(FRS_ID=pd.NA),
    )
    monkeypatch.setattr(fc, 'filter_to_model_geography', lambda df, **k: df)
    monkeypatch.setattr(
        fc.ghgrp_subpart_w,
        'lease_and_plant_fuel',
        lambda *a, **k: pd.DataFrame(columns=['FacilityID', 'CO2e']),
    )

    calls.clear()
    fc.build_facility_combustion(2018, nei_year=2018)
    # Twins applied to NEI only (not GHGRP).
    assert calls == [1]

    calls.clear()
    fc.build_facility_combustion(2021, nei_year=2021)
    assert calls == []
