"""Tests for pre-2021 Flowable twin shares (diagnostics cascade reuse)."""

from __future__ import annotations

from typing import Any

import pandas as pd

from bedrock.extract.stewifbs.facility_combustion import (
    apply_pre2021_fuel_flowable_shares,
)


def _simple_tables() -> dict[str, Any]:
    """Bare tables (no twin sets) — direct FacilityID / NAICS / sector lookup."""
    return {
        'facility': pd.DataFrame(
            {
                'Coal': [0.2],
                'Natural Gas': [0.5],
                'Petroleum': [0.3],
                'Other': [0.0],
            },
            index=['F1'],
        ),
        'naics': pd.DataFrame(
            {
                'Coal': [1.0],
                'Natural Gas': [0.0],
                'Petroleum': [0.0],
                'Other': [0.0],
            },
            index=['325110'],
        ),
        'sector': pd.DataFrame(
            {
                'Coal': [0.0],
                'Natural Gas': [0.0],
                'Petroleum': [1.0],
                'Other': [0.0],
            },
            index=['331110'],
        ),
    }


def test_facility_twin_splits_other_conserves_level() -> None:
    rows = pd.DataFrame(
        [
            {
                'FacilityID': 'F1',
                'NAICS': '324110',
                'sector': '324110',
                'Flowable': 'Other',
                'fuel_class': 'unclassified',
                'CO2e': 100.0,
            },
            {
                'FacilityID': 'F1',
                'NAICS': '324110',
                'sector': '324110',
                'Flowable': 'Natural Gas - lease and plant',
                'fuel_class': 'lease and plant',
                'CO2e': 40.0,
            },
        ]
    )
    out = apply_pre2021_fuel_flowable_shares(rows, _simple_tables())
    assert abs(out['CO2e'].sum() - 140.0) < 1e-9
    assert (
        float(
            out.loc[out['Flowable'] == 'Natural Gas - lease and plant', 'CO2e'].iloc[0]
        )
        == 40.0
    )
    by_flow = out.groupby('Flowable')['CO2e'].sum()
    assert abs(by_flow['Coal'] - 20.0) < 1e-9
    assert abs(by_flow['Natural Gas'] - 50.0) < 1e-9


def test_naics_sector_fallback_and_no_twin_leaves_other() -> None:
    rows = pd.DataFrame(
        [
            {
                'FacilityID': 'X',
                'NAICS': '325110',
                'sector': '325110',
                'Flowable': 'Other',
                'fuel_class': 'unclassified',
                'CO2e': 10.0,
            },
            {
                'FacilityID': 'Y',
                'NAICS': '999999',
                'sector': '331110',
                'Flowable': 'Other',
                'fuel_class': 'unclassified',
                'CO2e': 10.0,
            },
            {
                'FacilityID': 'Z',
                'NAICS': '111111',
                'sector': '1111A0',
                'Flowable': 'Other',
                'fuel_class': 'unclassified',
                'CO2e': 5.0,
            },
        ]
    )
    out = apply_pre2021_fuel_flowable_shares(rows, _simple_tables())
    assert list(out.loc[out['FacilityID'] == 'X', 'Flowable']) == ['Coal']
    assert list(out.loc[out['FacilityID'] == 'Y', 'Flowable']) == ['Petroleum']
    assert list(out.loc[out['FacilityID'] == 'Z', 'Flowable']) == ['Other']
