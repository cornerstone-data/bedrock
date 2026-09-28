"""Facilities T3-11 prep: mfg split, lease carve, petroleum annex — one pipeline."""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from bedrock.extract.flowbyactivity import FlowByActivity
from bedrock.extract.umd import UMD_GHGIA


def _minimal_fba(rows: list[dict[str, Any]], config: dict[str, Any]) -> FlowByActivity:
    frame = pd.DataFrame(rows)
    for col, default in (
        ('Class', 'Emission'),
        ('SourceName', 'UMD_GHGIA'),
        ('Unit', 'MMT'),
        ('FlowType', 'ELEMENTARY_FLOW'),
        ('ActivityConsumedBy', None),
        ('Compartment', None),
        ('Context', ''),
        ('Location', '00000'),
        ('LocationSystem', 'FIPS'),
        ('MeasureofSpread', None),
        ('Spread', None),
        ('DistributionType', None),
        ('Min', None),
        ('Max', None),
        ('DataReliability', 1.0),
        ('TemporalCorrelation', 1.0),
        ('GeographicalCorrelation', 1.0),
        ('TechnologicalCorrelation', 1.0),
        ('DataCollection', 1.0),
        ('Description', ''),
        ('Flowable', 'CO2'),
        ('FlowName', 'CO2'),
    ):
        if col not in frame.columns:
            frame[col] = default
    return FlowByActivity(frame, full_name='UMD_GHGIA_T_3_11', config=config)


def test_prepare_carves_lease_and_splits_petroleum(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mfg rename + lease carve + annex petroleum suffixes; totals conserved."""
    # Facility union: half of each fuel is manufacturing (31–33).
    union = pd.DataFrame(
        [
            {
                'FacilityID': 'M1',
                'Flowable': 'Natural Gas',
                'CO2e': 40.0e9,
                'sector': '324110',
                'NAICS': '324110',
            },
            {
                'FacilityID': 'N1',
                'Flowable': 'Natural Gas',
                'CO2e': 60.0e9,
                'sector': '211000',
                'NAICS': '211111',
            },
            {
                'FacilityID': 'M2',
                'Flowable': 'Coal',
                'CO2e': 50.0e9,
                'sector': '327310',
                'NAICS': '327310',
            },
            {
                'FacilityID': 'N2',
                'Flowable': 'Coal',
                'CO2e': 50.0e9,
                'sector': '212100',
                'NAICS': '212111',
            },
        ]
    )
    monkeypatch.setattr(
        'bedrock.extract.epa.EPA_GHGI.build_facility_combustion',
        lambda *a, **k: union.copy(),
    )

    # Lease/plant: 30 Mt CO2e carved from remaining Natural Gas Industrial.
    monkeypatch.setattr(
        UMD_GHGIA.stewi,
        'getInventory',
        lambda *a, **k: pd.DataFrame(
            {
                'FacilityID': ['W1'],
                'Process': ['C'],
                'FlowName': ['Carbon Dioxide'],
                'FlowAmount': [1.0],
            }
        ),
    )
    monkeypatch.setattr(
        UMD_GHGIA.ghgrp_subpart_w,
        'lease_and_plant_fuel',
        lambda *a, **k: pd.DataFrame({'FacilityID': ['W1'], 'CO2e': [30.0e9]}),
    )

    annex = pd.DataFrame(
        {
            'FlowName': ['CO2', 'CO2'],
            'ActivityProducedBy': [
                'Still Gas Industrial',
                'Distillate Fuel Oil Industrial',
            ],
            'FlowAmount': [60.0, 40.0],
        }
    )
    monkeypatch.setattr(
        UMD_GHGIA,
        'load_fba_w_standardized_units',
        lambda **k: annex.copy(),
    )

    fba = _minimal_fba(
        [
            {
                'ActivityProducedBy': 'Natural Gas Industrial',
                'FlowAmount': 100.0,
                'Year': 2022,
            },
            {
                'ActivityProducedBy': 'Coal Industrial',
                'FlowAmount': 100.0,
                'Year': 2022,
            },
            {
                'ActivityProducedBy': 'Petroleum Industrial',
                'FlowAmount': 100.0,
                'Year': 2022,
            },
        ],
        config={
            'clean_parameter': {
                'year': 2022,
                'annex_fba': 'UMD_GHGIA_T_A5_1_S6',
            }
        },
    )
    total_in = float(fba['FlowAmount'].sum())
    out = UMD_GHGIA.prepare_facilities_industrial_combustion(fba)

    assert abs(float(out['FlowAmount'].sum()) - total_in) < 1e-9
    activities = set(out['ActivityProducedBy'].astype(str))
    assert 'Natural Gas Industrial - Manufacturing' in activities
    assert 'Coal Industrial - Manufacturing' in activities
    assert 'Natural Gas Industrial - Lease and Plant' in activities
    assert 'Petroleum Industrial - Still Gas' in activities
    assert 'Petroleum Industrial - Distillate Fuel Oil' in activities
    assert 'Petroleum Industrial' not in activities

    # Mfg ratios from union: NG 40%, Coal 50%.
    ng_mfg = float(
        out.loc[
            out['ActivityProducedBy'] == 'Natural Gas Industrial - Manufacturing',
            'FlowAmount',
        ].sum()
    )
    assert abs(ng_mfg - 40.0) < 1e-9
    coal_mfg = float(
        out.loc[
            out['ActivityProducedBy'] == 'Coal Industrial - Manufacturing',
            'FlowAmount',
        ].sum()
    )
    assert abs(coal_mfg - 50.0) < 1e-9

    lease = float(
        out.loc[
            out['ActivityProducedBy'] == 'Natural Gas Industrial - Lease and Plant',
            'FlowAmount',
        ].sum()
    )
    assert abs(lease - 30.0) < 1e-9

    still = float(
        out.loc[
            out['ActivityProducedBy'] == 'Petroleum Industrial - Still Gas',
            'FlowAmount',
        ].sum()
    )
    distillate = float(
        out.loc[
            out['ActivityProducedBy']
            == 'Petroleum Industrial - Distillate Fuel Oil',
            'FlowAmount',
        ].sum()
    )
    assert abs(still - 60.0) < 1e-9
    assert abs(distillate - 40.0) < 1e-9
