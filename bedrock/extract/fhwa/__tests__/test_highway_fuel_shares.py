"""Unit tests for FHWA highway_fuel_shares (Energy_highway_fuel_shares FBS).

Shares are synthetic; getFlowByActivity and the GSA crosswalk are mocked.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from bedrock.extract.fhwa.FHWA import (
    _emit_highway_shares,
    _normalize,
    fhwa_mf21_parse,
    highway_fuel_shares,
)
from bedrock.extract.flowbyactivity import FlowByActivity


def _seed_fba(*, nonpublic: float = 100.0, **clean_parameter: Any) -> FlowByActivity:
    return FlowByActivity(
        pd.DataFrame(
            [
                {
                    'ActivityConsumedBy': 'Private and Commercial',
                    'FlowName': 'Special fuel highway',
                    'FlowAmount': nonpublic,
                    'Location': '00000',
                },
                {
                    'ActivityConsumedBy': 'Federal Civilian',
                    'FlowName': 'Special fuel highway',
                    'FlowAmount': 10.0,
                    'Location': '00000',
                },
            ]
        ),
        full_name='FHWA_Highway_Statistics',
        config={'year': 2024, 'clean_parameter': clean_parameter},
    )


def _ffr_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                'ActivityConsumedBy': 'Total Civilian Agencies',
                'FlowName': 'Gasoline',
                'FlowAmount': 40.0,
            },
            {
                'ActivityConsumedBy': 'Total U.S. Postal Service',
                'FlowName': 'Gasoline',
                'FlowAmount': 10.0,
            },
            {
                'ActivityConsumedBy': 'Total Civilian Agencies',
                'FlowName': 'Diesel',
                'FlowAmount': 20.0,
            },
            {
                'ActivityConsumedBy': 'Total U.S. Postal Service',
                'FlowName': 'Diesel',
                'FlowAmount': 5.0,
            },
        ]
    )


def _mv7_rows() -> pd.DataFrame:
    rows = []
    for flow, fed, scm in (
        ('Automobiles', 100.0, 200.0),
        ('Buses', 10.0, 50.0),
        ('Trucks', 50.0, 150.0),
    ):
        rows.append(
            {
                'Description': 'Table MV-7',
                'FlowName': flow,
                'ActivityConsumedBy': 'Federal',
                'FlowAmount': fed,
            }
        )
        rows.append(
            {
                'Description': 'Table MV-7',
                'FlowName': flow,
                'ActivityConsumedBy': 'State, County and Municipal',
                'FlowAmount': scm,
            }
        )
    return pd.DataFrame(rows)


def _mv10_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                'Description': 'Table MV-10',
                'FlowName': 'Buses',
                'ActivityConsumedBy': 'Federal',
                'FlowAmount': 10.0,
            },
            {
                'Description': 'Table MV-10',
                'FlowName': 'Buses',
                'ActivityConsumedBy': 'State, County and Municipal',
                'FlowAmount': 40.0,
            },
            {
                'Description': 'Table MV-10',
                'FlowName': 'Buses',
                'ActivityConsumedBy': 'Private and Commercial',
                'FlowAmount': 50.0,
            },
        ]
    )


def _mf21_gasoline() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                'Description': 'Table MF-21',
                'FlowName': 'Gasoline highway',
                'ActivityConsumedBy': 'Federal Civilian',
                'FlowAmount': 20.0,
            },
            {
                'Description': 'Table MF-21',
                'FlowName': 'Gasoline highway',
                'ActivityConsumedBy': 'State, County and Municipal',
                'FlowAmount': 30.0,
            },
            {
                'Description': 'Table MF-21',
                'FlowName': 'Gasoline highway',
                'ActivityConsumedBy': 'Private and Commercial',
                'FlowAmount': 50.0,
            },
        ]
    )


def _gsa_crosswalk() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                'Activity': 'Total Civilian Agencies',
                'SectorSourceName': 'BEA_2017_Code',
                'Sector': 'S00600',
            },
            {
                'Activity': 'Total U.S. Postal Service',
                'SectorSourceName': 'NAICS_2017_Code',
                'Sector': '491',
            },
        ]
    )


def _patch_loaders(monkeypatch: pytest.MonkeyPatch) -> None:
    ffr = _ffr_rows()
    mv7 = _mv7_rows()
    mv10 = _mv10_rows()
    mf21 = _mf21_gasoline()

    def fake_get(
        source_name: str, year: int, download_FBA_if_missing: bool = True
    ) -> pd.DataFrame:
        if source_name == 'GSA_FFR':
            return ffr
        if source_name == 'FHWA_Highway_Statistics':
            # Callers filter by Description; return the union of tables.
            return pd.concat([mv7, mv10, mf21], ignore_index=True)
        raise AssertionError(f'unexpected source {source_name!r} year={year}')

    monkeypatch.setattr('bedrock.extract.flowbyactivity.getFlowByActivity', fake_get)
    monkeypatch.setattr(
        'bedrock.utils.mapping.sectormapping.get_activitytosector_mapping',
        lambda _name: _gsa_crosswalk(),
    )


def _highway_params(**overrides: Any) -> dict[str, Any]:
    params: dict[str, Any] = {
        'flowable': 'Gasoline',
        'ffr_fuels': ['Gasoline'],
        'nonpublic_source': 'mf21',
        'ffr': {'source_name': 'GSA_FFR', 'year': 2024},
        'mv7': {
            'source_name': 'FHWA_Highway_Statistics',
            'year': 2024,
            'selection_fields': {
                'Description': 'Table MV-7',
                'FlowName': ['Automobiles', 'Buses', 'Trucks'],
            },
        },
        'mv10': {
            'source_name': 'FHWA_Highway_Statistics',
            'year': 2024,
            'selection_fields': {
                'Description': 'Table MV-10',
                'FlowName': 'Buses',
            },
        },
        'mf21': {
            'source_name': 'FHWA_Highway_Statistics',
            'year': 2024,
            'selection_fields': {
                'Description': 'Table MF-21',
                'FlowName': 'Gasoline highway',
            },
        },
    }
    params.update(overrides)
    return params


def test_normalize_scales_to_one() -> None:
    assert sum(_normalize({'a': 1.0, 'b': 3.0}).values()) == pytest.approx(1.0)


def test_normalize_rejects_zero_total() -> None:
    with pytest.raises(ValueError, match='normalize'):
        _normalize({'a': 0.0, 'b': 0.0})


def test_emit_highway_shares_unit_and_sum() -> None:
    seed = _seed_fba()
    out = _emit_highway_shares(
        seed,
        flowable='Gasoline',
        vehicle_class='Trucks',
        fed=0.2,
        state_county_municipal=0.3,
        nonpublic=0.5,
        fed_agency=[
            ('Total Civilian Agencies', 0.75),
            ('Total U.S. Postal Service', 0.25),
        ],
        private_activity='Trucks - Private Use',
    )
    assert (out['Unit'] == 'share').all()
    assert out['Flowable'].iloc[0] == 'Gasoline; Trucks'
    assert set(out['ActivityConsumedBy']) >= {
        'Total Civilian Agencies',
        'Total U.S. Postal Service',
        'State, County and Municipal',
        'Trucks - Private Use',
    }
    assert float(out['FlowAmount'].sum()) == pytest.approx(1.0)


def test_highway_fuel_shares_mf21_each_flowable_sums_to_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_loaders(monkeypatch)
    out = highway_fuel_shares(_seed_fba(**_highway_params()), download_sources_ok=False)
    for flowable, total in out.groupby('Flowable')['FlowAmount'].sum().items():
        assert total == pytest.approx(1.0), flowable
    assert set(out['Flowable']) == {
        'Gasoline; Automobiles',
        'Gasoline; Buses',
        'Gasoline; Trucks',
    }
    assert (out['Unit'] == 'share').all()


def test_highway_fuel_shares_attributed_nonpublic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_loaders(monkeypatch)
    params = _highway_params(
        flowable='Diesel',
        ffr_fuels=['Diesel'],
        nonpublic_source='attributed_nonpublic',
    )
    params.pop('mf21')
    out = highway_fuel_shares(
        _seed_fba(nonpublic=80.0, **params), download_sources_ok=False
    )
    for flowable, total in out.groupby('Flowable')['FlowAmount'].sum().items():
        assert total == pytest.approx(1.0), flowable
    assert set(out['Flowable']) == {
        'Diesel; Automobiles',
        'Diesel; Buses',
        'Diesel; Trucks',
    }


def test_highway_fuel_shares_gasoline_proxy_for_diesel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pre-2024 path: Diesel Flowables built from gasoline MF-21 + FFR fuels."""
    _patch_loaders(monkeypatch)
    out = highway_fuel_shares(
        _seed_fba(
            **_highway_params(
                flowable='Diesel',
                ffr_fuels=['Gasoline'],
                nonpublic_source='mf21',
            )
        ),
        download_sources_ok=False,
    )
    for flowable, total in out.groupby('Flowable')['FlowAmount'].sum().items():
        assert total == pytest.approx(1.0), flowable
    assert set(out['Flowable']) == {
        'Diesel; Automobiles',
        'Diesel; Buses',
        'Diesel; Trucks',
    }


def test_legacy_mf21_has_no_gov_special_fuel() -> None:
    """Pre-2024 MF-21 only publishes special fuel for Private and Commercial."""
    # Column layout matches _MF21_COLUMNS_LEGACY indices on a Total row.
    total: list[Any] = [None] * 14
    total[0] = 'Total'
    total[1] = 50  # gasoline private
    total[2] = 10  # gasoline federal
    total[3] = 20  # gasoline state
    total[4] = 30  # gasoline public total
    total[5] = 80  # gasoline total
    total[6] = 1
    total[7] = 1
    total[8] = 2
    total[9] = 100
    total[11] = 100
    total[12] = 40  # special fuel private only
    total[13] = 120
    raw = pd.DataFrame([total])
    out = fhwa_mf21_parse(df_list=[raw], source='FHWA_Highway_Statistics', year='2020')
    special = out[out['FlowName'] == 'Special fuel highway']
    assert set(special['ActivityConsumedBy']) == {'Private and Commercial'}
    gasoline = out[out['FlowName'] == 'Gasoline highway']
    assert {
        'Federal Civilian',
        'State, County and Municipal',
        'Private and Commercial',
    } <= set(gasoline['ActivityConsumedBy'])
