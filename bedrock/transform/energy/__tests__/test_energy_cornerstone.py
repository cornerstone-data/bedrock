from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from bedrock.extract.eia.EIA_MECS import (
    keep_chemical_manufacturing,
    keep_industrial_petroleum_consumers,
    multiply_bea_by_mecs_petroleum_energy_fraction,
    scale_household_petroleum_to_transport_share,
)
from bedrock.extract.flowbyactivity import FlowByActivity, getFlowByActivity
from bedrock.transform.flowbyclean import define_parentincompletechild_descendants
from bedrock.transform.flowbysector import FlowBySector, getFlowBySector
from bedrock.utils.config import settings
from bedrock.utils.config.common import load_yaml_dict


def test_parentincompletechild_residualizes_nested_naics() -> None:
    # map_to_sectors runs this after FlowName → Flowable; activity sets already
    # split Table 2.1 vs 3.1, so define only sees one table at a time.
    fba = FlowByActivity(
        pd.DataFrame(
            [
                {
                    'ActivityConsumedBy': '311',
                    'Flowable': 'Coal',
                    'Location': '00000',
                    'FlowAmount': 110.0,
                },
                {
                    'ActivityConsumedBy': '3112',
                    'Flowable': 'Coal',
                    'Location': '00000',
                    'FlowAmount': 65.0,
                },
                {
                    'ActivityConsumedBy': '311221',
                    'Flowable': 'Coal',
                    'Location': '00000',
                    'FlowAmount': 55.0,
                },
            ]
        ),
        full_name='EIA_MECS_Energy',
        config={'year': 2018},
    )
    fba = fba.assign(group_id=fba.index, group_total=fba.FlowAmount)
    out = define_parentincompletechild_descendants(fba)
    assert float(out.loc[out.ActivityConsumedBy == '311', 'FlowAmount'].iloc[0]) == 45.0
    assert (
        float(out.loc[out.ActivityConsumedBy == '3112', 'FlowAmount'].iloc[0]) == 10.0
    )
    assert (
        float(out.loc[out.ActivityConsumedBy == '311221', 'FlowAmount'].iloc[0]) == 55.0
    )


def test_parentincompletechild_groups_by_flowable_not_description() -> None:
    fba = FlowByActivity(
        pd.DataFrame(
            [
                {
                    'ActivityConsumedBy': '311',
                    'Flowable': 'Approximate Enclosed Floorspace of All Buildings Onsite',
                    'Description': 'Food Manufacturing',
                    'Location': '00000',
                    'FlowAmount': 110.0,
                },
                {
                    'ActivityConsumedBy': '3112',
                    'Flowable': 'Approximate Enclosed Floorspace of All Buildings Onsite',
                    'Description': 'Grain and oilseed milling Manufacturing',
                    'Location': '00000',
                    'FlowAmount': 65.0,
                },
                {
                    'ActivityConsumedBy': '311221',
                    'Flowable': 'Approximate Enclosed Floorspace of All Buildings Onsite',
                    'Description': 'Wet corn milling Manufacturing',
                    'Location': '00000',
                    'FlowAmount': 55.0,
                },
                {
                    'ActivityConsumedBy': '311',
                    'Flowable': 'Establishments',
                    'Description': 'Food Manufacturing',
                    'Location': '00000',
                    'FlowAmount': 20.0,
                },
                {
                    'ActivityConsumedBy': '3112',
                    'Flowable': 'Establishments',
                    'Description': 'Grain and oilseed milling Manufacturing',
                    'Location': '00000',
                    'FlowAmount': 8.0,
                },
            ]
        ),
        full_name='EIA_MECS_Land',
        config={'year': 2014},
    )
    fba = fba.assign(group_id=fba.index, group_total=fba.FlowAmount)
    out = define_parentincompletechild_descendants(fba)
    floorspace = out.query(
        "Flowable == 'Approximate Enclosed Floorspace of All Buildings Onsite'"
    )
    assert (
        float(
            floorspace.loc[floorspace.ActivityConsumedBy == '311', 'FlowAmount'].iloc[0]
        )
        == 45.0
    )
    assert (
        float(
            floorspace.loc[floorspace.ActivityConsumedBy == '3112', 'FlowAmount'].iloc[
                0
            ]
        )
        == 10.0
    )
    establishments = out.query("Flowable == 'Establishments'")
    assert (
        float(
            establishments.loc[
                establishments.ActivityConsumedBy == '311', 'FlowAmount'
            ].iloc[0]
        )
        == 12.0
    )


def test_multiply_bea_by_mecs_petroleum_energy_fraction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mecs = pd.DataFrame(
        [
            {
                'ActivityConsumedBy': '327310',
                'FlowName': 'Other',
                'Description': 'Table 2.1',
                'Location': '00000',
                'FlowAmount': 100.0,
                'Unit': 'TBtu',
                'Class': 'Energy',
                'Compartment': None,
            },
            {
                'ActivityConsumedBy': '327310',
                'FlowName': 'Other',
                'Description': 'Table 3.1',
                'Location': '00000',
                'FlowAmount': 50.0,
                'Unit': 'TBtu',
                'Class': 'Energy',
                'Compartment': None,
            },
            {
                'ActivityConsumedBy': '325',
                'FlowName': 'Other',
                'Description': 'Table 2.1',
                'Location': '00000',
                'FlowAmount': 80.0,
                'Unit': 'TBtu',
                'Class': 'Energy',
                'Compartment': None,
            },
            {
                'ActivityConsumedBy': '325',
                'FlowName': 'Other',
                'Description': 'Table 3.1',
                'Location': '00000',
                'FlowAmount': 20.0,
                'Unit': 'TBtu',
                'Class': 'Energy',
                'Compartment': None,
            },
        ]
    )
    monkeypatch.setattr(
        'bedrock.extract.eia.EIA_MECS.getFlowByActivity',
        lambda *_args, **_kwargs: mecs,
    )

    def _map_to_sectors(
        self: FlowByActivity,
        target_year: int | None = None,
        external_config_path: str | None = None,
    ) -> FlowByActivity:
        return self.assign(SectorConsumedBy=self['ActivityConsumedBy'])

    monkeypatch.setattr(FlowByActivity, 'map_to_sectors', _map_to_sectors)
    bea = FlowByActivity(
        pd.DataFrame(
            [
                {
                    'ActivityConsumedBy': '327310',
                    'SectorConsumedBy': '327310',
                    'FlowAmount': 10.0,
                },
                {
                    'ActivityConsumedBy': '1111A0',
                    'SectorConsumedBy': '1111A0',
                    'FlowAmount': 5.0,
                },
                {
                    'ActivityConsumedBy': '325611',
                    'SectorConsumedBy': '325611',
                    'FlowAmount': 10.0,
                },
                {
                    'ActivityConsumedBy': '481000',
                    'SectorConsumedBy': '481000',
                    'FlowAmount': 8.0,
                },
            ]
        ),
        config={
            'activity_to_sector_mapping': 'Cornerstone_2025',
            'target_schema_year': 2017,
            'industry_spec': {'default': 'NAICS_3'},
            'clean_source': {
                'EIA_MECS_Energy': {
                    'year': 2018,
                    'selection_fields': {
                        'Location': '00000',
                        'FlowName': 'Other',
                        'Description': ['Table 2.1', 'Table 3.1'],
                    },
                    'exclusion_fields': {'ActivityConsumedBy': '31-33'},
                }
            },
        },
    )
    out = multiply_bea_by_mecs_petroleum_energy_fraction(bea)
    by_sector = out.set_index('SectorConsumedBy')['FlowAmount']
    assert by_sector['327310'] == pytest.approx(10.0 * 50.0 / 150.0)
    assert by_sector['1111A0'] == pytest.approx(5.0)
    assert by_sector['325611'] == pytest.approx(10.0)
    assert by_sector['481000'] == pytest.approx(8.0)


def test_keep_industrial_petroleum_consumers() -> None:
    fba = FlowByActivity(
        pd.DataFrame(
            [
                {'ActivityConsumedBy': '327310', 'FlowAmount': 10.0},
                {'ActivityConsumedBy': '481000', 'FlowAmount': 8.0},
                {'ActivityConsumedBy': '221200', 'FlowAmount': 3.0},
                {'ActivityConsumedBy': '541000', 'FlowAmount': 4.0},
            ]
        ),
    )
    out = keep_industrial_petroleum_consumers(fba)
    kept = set(out['ActivityConsumedBy'])
    assert kept == {'327310', '221200'}


def test_scale_household_petroleum_to_transport_share(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        'bedrock.extract.eia.EIA_MECS.household_petroleum_transport_fraction',
        lambda _config: 0.4,
    )
    fba = FlowByActivity(
        pd.DataFrame(
            [
                {'ActivityConsumedBy': 'F01000', 'FlowAmount': 20.0},
                {'ActivityConsumedBy': '481000', 'FlowAmount': 10.0},
            ]
        ),
        config={
            'year': 2018,
            'clean_source': {
                'UMD_GHGIA_T_A5_1_S6': {
                    'year': 2018,
                    'selection_fields': {
                        'Unit': 'TBtu',
                        'ActivityConsumedBy': 'Residential',
                        'FlowName': [
                            'Kerosene',
                            'LPG (Propane)',
                            'Distillate Fuel Oil',
                        ],
                    },
                }
            },
            'clean_parameter': {'year': 2018},
        },
    )
    out = scale_household_petroleum_to_transport_share(fba)
    by_activity = out.set_index('ActivityConsumedBy')['FlowAmount']
    assert by_activity['F01000'] == pytest.approx(8.0)
    assert by_activity['481000'] == pytest.approx(10.0)


@pytest.mark.parametrize(
    ('year', 'split_year', 'umd_annex'),
    [
        (2018, 2017, 'UMD_GHGIA_T_A5_1_S6'),
        (2022, 2022, 'UMD_GHGIA_T_A5_1_S2'),
    ],
)
def test_energy_manufacturing_yaml_nowcast_hybrid_nests(
    year: int, split_year: int, umd_annex: str
) -> None:
    method = load_yaml_dict(f'Energy_manufacturing_national_nowcast_{year}', 'FBS')
    assert method['year'] == year
    assert method['mecs_year'] == year
    assert method['split_year'] == split_year
    energy_dir = Path(__file__).resolve().parents[1]
    yaml_text = (
        energy_dir / f'Energy_manufacturing_national_nowcast_{year}.yaml'
    ).read_text(encoding='utf-8')
    assert 'allocation/' not in yaml_text
    assert 'BEA_Detail_Use_AfterRedef' not in yaml_text

    use = method['source_names']['Nowcast_Detail_Use_AfterRedef']
    assert use['year'] == year
    use_sets = use['activity_sets']
    assert set(use_sets) == {'petrol', 'neu_coal_coke', 'neu_transport'}
    assert use_sets['petrol']['clean_fba'] is keep_industrial_petroleum_consumers
    assert (
        use_sets['petrol']['clean_fba_after_attribution']
        is multiply_bea_by_mecs_petroleum_energy_fraction
    )
    transport = use_sets['neu_transport']
    assert umd_annex in transport['clean_source']
    assert (
        transport['clean_fba_after_attribution']
        is scale_household_petroleum_to_transport_share
    )

    mecs = method['source_names']['EIA_MECS_Energy']
    mecs_sets = mecs['activity_sets']
    assert set(mecs_sets) == {
        'table_3_1_coal',
        'table_3_1_ng',
        'table_2_1',
        'table_2_1_ng',
    }
    assert mecs_sets['table_2_1_ng']['clean_fba'] is keep_chemical_manufacturing
    for set_name in mecs_sets:
        attr = mecs_sets[set_name]['attribution_source'][
            'Nowcast_Detail_Use_AfterRedef'
        ]
        assert attr['year'] == split_year
        assert attr['attribution_method'] == 'equal'


def test_keep_chemical_manufacturing_resets_index() -> None:
    fba = FlowByActivity(
        pd.DataFrame(
            {
                'ActivityConsumedBy': ['311', '325110', '325180'],
                'FlowAmount': [1.0, 2.0, 3.0],
            }
        )
    )
    fba.index = pd.Index([47, 89, 102])
    out = keep_chemical_manufacturing(fba)
    assert list(out.index) == [0, 1]
    assert list(out['ActivityConsumedBy']) == ['325110', '325180']


def _normalized_sector_shares(
    df: pd.DataFrame, sector_col: str = 'SectorConsumedBy'
) -> pd.Series[float]:
    grouped = df.groupby(sector_col, as_index=True)['FlowAmount'].sum()
    total = float(grouped.sum())
    if total == 0:
        return grouped * 0.0
    return grouped / total


def _allocation_ceda_to_fbs_buckets(fba: pd.DataFrame) -> pd.DataFrame:
    '''Map Allocation CEDA FBA rows to the same Flowable + Class buckets as Energy FBS.'''
    flowable_map = {
        'Coal': 'Coal',
        'Natural Gas': 'Natural Gas',
        'Petroleum': 'Petroleum',
        'Coal and Coke': 'Coal and Coke',
        'Transport': 'Transport',
    }

    def to_class(row: pd.Series) -> str:
        desc = str(row['Description'])
        flow = str(row['FlowName'])
        if flow in ('Coal', 'Natural Gas') and desc == 'energy':
            return 'Energy'
        if flow == 'Petroleum' and desc == 'energy':
            return 'Money'
        if (
            flow in ('Petroleum', 'Natural Gas', 'Coal and Coke')
            and desc == 'non energy'
        ):
            return 'Other'
        if flow == 'Transport' and desc == 'non energy':
            return 'Money'
        raise ValueError(f"Unexpected FBA bucket: {flow!r} / {desc!r}")

    out = fba.copy()
    out['Flowable'] = out['FlowName'].map(flowable_map).fillna(out['FlowName'])
    out['Class'] = out.apply(to_class, axis=1)
    return out


@pytest.mark.eeio_integration
def test_energy_cornerstone_2018_share_diffs_vs_allocation_ceda(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    '''Report per-bucket share diffs; fail only if an Energy nest bucket is empty.'''
    fbs_dir = tmp_path / 'fbs'
    fbs_dir.mkdir()
    monkeypatch.setattr(settings, 'FBS_DIR', fbs_dir)

    method = 'Energy_manufacturing_national_2018'
    FlowBySector.generateFlowBySector(method, download_sources_ok=True)
    energy = getFlowBySector(method)

    ceda_fba = getFlowByActivity('EIA_MECS_Energy_Allocation_CEDA', 2018)
    ceda = _allocation_ceda_to_fbs_buckets(ceda_fba)
    ceda = ceda.rename(columns={'ActivityConsumedBy': 'SectorConsumedBy'})

    buckets = [
        ('Coal', 'Energy'),
        ('Natural Gas', 'Energy'),
        ('Petroleum', 'Money'),
        ('Petroleum', 'Other'),
        ('HGL', 'Other'),
        ('Natural Gas', 'Other'),
        ('Transport', 'Money'),
    ]
    empty = []
    for flowable, klass in buckets:
        e_sub = energy.query('Flowable == @flowable and Class == @klass')
        c_sub = ceda.query('Flowable == @flowable and Class == @klass')
        if e_sub.empty:
            empty.append(f'{flowable}/{klass}')
            continue
        e_shares = _normalized_sector_shares(e_sub)
        c_shares = _normalized_sector_shares(c_sub)
        sectors = e_shares.index.union(c_shares.index)
        abs_diff = (
            e_shares.reindex(sectors, fill_value=0.0)
            - c_shares.reindex(sectors, fill_value=0.0)
        ).abs()
        print(
            f'{flowable}/{klass}: energy_rows={len(e_sub)} ceda_rows={len(c_sub)} '
            f'max_abs_share_diff={float(abs_diff.max()) if len(abs_diff) else 0:.4f}'
        )
    assert not empty, f'Energy FBS missing nest buckets: {empty}'
