"""Facility combustion union filters: prefer-GHGRP, exclude, keep_flowables, mobile SCC."""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.extract.stewifbs import facility_combustion as fc


def test_nei_onsite_drops_mobile_scc() -> None:
    nei_raw = pd.DataFrame(
        {
            'FacilityID': ['A', 'B', 'C'],
            'FlowName': ['Carbon Dioxide'] * 3,
            'Process': ['10100101', '22750000', '30100101'],
            'FlowAmount': [10.0, 99.0, 5.0],
        }
    )
    sectors = pd.DataFrame(
        {
            'NAICS': ['324110', '488190', '325110'],
            'State': ['TX', 'TX', 'TX'],
            'sector': ['324110', '488A00', '325110'],
        },
        index=['A', 'B', 'C'],
    )
    out = fc.nei_onsite_co2(nei_raw, sectors, use_scc=True)
    assert set(out['FacilityID']) == {'A', 'C'}
    assert float(out['CO2e'].sum()) == 15.0


def test_union_prefer_ghgrp_filters_and_lease_append(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Overlapping FRS keeps GHGRP; drop 221100; keep_flowables; lease still appended."""
    year = 2022

    nei_raw = pd.DataFrame(
        {
            'FacilityID': ['N_overlap', 'N_only'],
            'FlowName': ['Carbon Dioxide', 'Carbon Dioxide'],
            'Process': ['10200601', '10200601'],
            'FlowAmount': [50.0e9, 20.0e9],
        }
    )
    ghgrp_flows = pd.DataFrame(
        {
            'FacilityID': ['G_overlap', 'G_power', 'G_other'],
            'Process': ['C', 'C', 'C'],
            'FlowName': ['Carbon Dioxide'] * 3,
            'FlowAmount': [100.0e9, 80.0e9, 10.0e9],
        }
    )

    def fake_inventory(inventory, inv_year, **kwargs):
        if inventory == 'NEI':
            return nei_raw.copy()
        return ghgrp_flows.copy()

    nei_sectors = pd.DataFrame(
        {
            'NAICS': ['324110', '311221'],
            'State': ['TX', 'IA'],
            'sector': ['324110', '311221'],
        },
        index=['N_overlap', 'N_only'],
    )
    ghgrp_sectors = pd.DataFrame(
        {
            'NAICS': ['324110', '221100', '325180'],
            'State': ['TX', 'TX', 'LA'],
            'sector': ['324110', '221100', '325180'],
        },
        index=['G_overlap', 'G_power', 'G_other'],
    )
    monkeypatch.setattr(fc.stewi, 'getInventory', fake_inventory)
    monkeypatch.setattr(
        fc, 'facility_sectors', lambda *a, **k: (nei_sectors, ghgrp_sectors)
    )
    monkeypatch.setattr(
        fc.facilitymatcher,
        'get_matches_for_inventories',
        lambda *_: pd.DataFrame(
            {
                'FacilityID': [
                    'G_overlap',
                    'G_power',
                    'G_other',
                    'N_overlap',
                    'N_only',
                ],
                'Source': ['GHGRP', 'GHGRP', 'GHGRP', 'NEI', 'NEI'],
                'FRS_ID': ['FRS1', 'FRS_POW', 'FRS3', 'FRS1', 'FRS_NEI'],
            }
        ),
    )

    # Skip address fallback (empty left/right after FRS links).
    monkeypatch.setattr(
        fc.stewi,
        'getInventoryFacilities',
        lambda *a, **k: pd.DataFrame(
            columns=['FacilityID', 'State', 'Address', 'FacilityName', 'NAICS']
        ),
    )

    def fake_ghgrp_fuel_labels(ghgrp, nei, *a, **k):
        # Prefer-GHGRP path: labeled GHGRP rows with FRS; Other on G_other for keep test.
        return pd.DataFrame(
            [
                {
                    'FacilityID': 'G_overlap',
                    'FRS_ID': 'FRS1',
                    'Flowable': 'Natural Gas',
                    'fuel_class': 'purchased',
                    'CO2e': 100.0e9,
                    'sector': '324110',
                    'NAICS': '324110',
                    'State': 'TX',
                    'source': 'GHGRP',
                },
                {
                    'FacilityID': 'G_power',
                    'FRS_ID': 'FRS_POW',
                    'Flowable': 'Natural Gas',
                    'fuel_class': 'purchased',
                    'CO2e': 80.0e9,
                    'sector': '221100',
                    'NAICS': '221100',
                    'State': 'TX',
                    'source': 'GHGRP',
                },
                {
                    'FacilityID': 'G_other',
                    'FRS_ID': 'FRS3',
                    'Flowable': 'Other',
                    'fuel_class': 'unclassified',
                    'CO2e': 10.0e9,
                    'sector': '325180',
                    'NAICS': '325180',
                    'State': 'LA',
                    'source': 'GHGRP',
                },
            ]
        )

    monkeypatch.setattr(fc, 'ghgrp_fuel_labels', fake_ghgrp_fuel_labels)
    monkeypatch.setattr(
        fc,
        'filter_to_model_geography',
        lambda df, **k: df,
    )
    monkeypatch.setattr(
        fc.ghgrp_subpart_w,
        'lease_and_plant_fuel',
        lambda *a, **k: pd.DataFrame(
            {'FacilityID': ['G_overlap'], 'CO2e': [5.0e9]}
        ),
    )

    out = fc.build_facility_combustion(
        year,
        exclude_sectors=('221100',),
        keep_flowables=('Natural Gas', 'Natural Gas - lease and plant'),
    )

    sources = out.groupby('source')['FacilityID'].apply(set).to_dict()
    # Overlapping FRS1: NEI N_overlap excluded; GHGRP kept.
    assert 'G_overlap' in set(out['FacilityID'])
    assert 'N_overlap' not in set(out['FacilityID'])
    assert 'N_only' in set(out['FacilityID'])
    assert 'G_power' not in set(out['FacilityID'])
    assert 'Other' not in set(out['Flowable'].astype(str))
    assert 'Natural Gas - lease and plant' in set(out['Flowable'].astype(str))
    assert float(
        out.loc[
            out['Flowable'] == 'Natural Gas - lease and plant', 'CO2e'
        ].sum()
    ) == 5.0e9
