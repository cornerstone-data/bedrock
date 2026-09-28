"""facility_combustion_to_sector emits facility NAICS on SectorProducedBy."""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.extract.stewifbs import stewiFBS


def test_sector_produced_by_is_naics(monkeypatch: pytest.MonkeyPatch) -> None:
    union = pd.DataFrame(
        [
            {
                'FacilityID': 'F1',
                'NAICS': '324110',
                'sector': '324110',
                'Flowable': 'Natural Gas',
                'CO2e': 10.0,
                'State': 'TX',
                'source': 'GHGRP',
                'fuel_class': 'purchased',
                'year': 2022,
            },
            {
                'FacilityID': 'F2',
                'NAICS': 311221.0,
                'sector': '311221',
                'Flowable': 'Coal',
                'CO2e': 5.0,
                'State': 'IA',
                'source': 'NEI',
                'fuel_class': 'purchased',
                'year': 2022,
            },
        ]
    )
    monkeypatch.setattr(
        stewiFBS,
        'build_facility_combustion',
        lambda *a, **k: union.copy(),
    )
    fbs = stewiFBS.facility_combustion_to_sector(
        {
            'inventory_dict': {'GHGRP': '2022', 'NEI': '2022'},
            'year': 2022,
            'target_schema_year': 2017,
        },
        full_name='GHGRP_NEI_Facilities',
    )
    assert set(fbs['SectorProducedBy'].astype(str)) == {'324110', '311221'}
    assert all(fbs['SectorSourceName'].astype(str).str.startswith('NAICS_'))
