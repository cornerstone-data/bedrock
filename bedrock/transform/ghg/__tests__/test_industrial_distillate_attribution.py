"""Table 3-11 industrial distillate weighted by physical fuel volumes.

The Cornerstone GHG method attributes industrial distillate by
``Energy_industrial_distillate_national_2024`` (EIA farm and off-highway
sales, Economic Census mining diesel, MECS) instead of refinery purchases, and
scales farm VIUS truck miles so farm fuel is not counted on-road as well.
These are pure-function and config checks; none of them loads a source.
"""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.extract.bts.BTS_VIUS import scale_business_types
from bedrock.extract.census.Census_EC import mining_diesel_gallons
from bedrock.extract.eia.EIA_FOKS import eia_foks_parse
from bedrock.extract.eia.EIA_MECS import (
    GALLONS_DISTILLATE_PER_TBTU,
    mecs_distillate_to_gallons,
)
from bedrock.extract.flowbyactivity import FlowByActivity
from bedrock.utils.config.common import load_yaml_dict


def _fba(rows: list[dict[str, object]], **config: object) -> FlowByActivity:
    return FlowByActivity(pd.DataFrame(rows), full_name='test', config=config)


def test_eia_foks_parse_returns_gallons_by_end_use() -> None:
    df = eia_foks_parse(source='EIA_FOKS', year='2020')
    amounts = df.set_index('ActivityConsumedBy')['FlowAmount']
    assert set(amounts.index) == {'Farm', 'Off-Highway', 'On-Highway'}
    assert amounts['Farm'] == pytest.approx(3_630_350_000)
    assert (df['Unit'] == 'gal').all()


def test_eia_foks_parse_rejects_missing_year() -> None:
    with pytest.raises(ValueError, match='2021'):
        eia_foks_parse(source='EIA_FOKS', year='2021')


def test_mining_diesel_gallons_keeps_mining_diesel_only() -> None:
    fba = _fba(
        [
            # mining diesel
            {
                'ActivityProducedBy': '32411021',
                'ActivityConsumedBy': '212312',
                'FlowAmount': 420.0,
                'FlowName': 'Fuel consumed',
                'Unit': 'USD',
            },
            # mining gasoline
            {
                'ActivityProducedBy': '32411015',
                'ActivityConsumedBy': '212312',
                'FlowAmount': 100.0,
                'FlowName': 'Fuel consumed',
                'Unit': 'USD',
            },
            # manufacturing diesel
            {
                'ActivityProducedBy': '32411021',
                'ActivityConsumedBy': '331110',
                'FlowAmount': 999.0,
                'FlowName': 'Fuel consumed',
                'Unit': 'USD',
            },
        ],
        clean_parameter={'usd_per_gallon': 4.2},
    )
    out = mining_diesel_gallons(fba)
    assert len(out) == 1
    row = out.iloc[0]
    assert row['FlowAmount'] == pytest.approx(100.0)
    assert row['Unit'] == 'gal'
    assert row['FlowName'] == 'Distillate Fuel Oil'
    # the material code must not survive to map as refinery NAICS 324110
    assert pd.isna(row['ActivityProducedBy'])


def test_mecs_distillate_to_gallons_converts_tbtu() -> None:
    fba = _fba(
        [{'ActivityConsumedBy': '324', 'FlowAmount': 2.0, 'Unit': 'Trillion Btu'}]
    )
    out = mecs_distillate_to_gallons(fba)
    assert out['FlowAmount'].iloc[0] == pytest.approx(2.0 * GALLONS_DISTILLATE_PER_TBTU)
    assert out['Unit'].iloc[0] == 'gal'


def test_mecs_distillate_to_gallons_rejects_other_units() -> None:
    fba = _fba(
        [{'ActivityConsumedBy': '324', 'FlowAmount': 1.0, 'Unit': 'million bbl'}]
    )
    with pytest.raises(ValueError):
        mecs_distillate_to_gallons(fba)


def test_scale_business_types_scales_only_named_types() -> None:
    fba = _fba(
        [
            {
                'ActivityConsumedBy': 'Agriculture (Crop and Animal Production)',
                'FlowAmount': 100.0,
            },
            {
                'ActivityConsumedBy': 'For-hire Transportation (of Goods or People)',
                'FlowAmount': 100.0,
            },
        ],
        clean_parameter={
            'business_type_scale': {'Agriculture (Crop and Animal Production)': 0.4}
        },
    )
    out = scale_business_types(fba)
    assert out['FlowAmount'].tolist() == pytest.approx([40.0, 100.0])


def test_scale_business_types_rejects_unknown_type() -> None:
    fba = _fba(
        [{'ActivityConsumedBy': 'Retail Trade', 'FlowAmount': 1.0}],
        clean_parameter={'business_type_scale': {'Agriculture': 0.4}},
    )
    with pytest.raises(ValueError, match='Agriculture'):
        scale_business_types(fba)


def test_transportation_method_splits_industrial_petroleum() -> None:
    method = load_yaml_dict(
        'GHG_national_Cornerstone_2024',
        flowbytype='FBS',
    )
    sets = method['source_names']['UMD_GHGIA_T_3_11']['activity_sets']
    assert sets['petroleum_use']['selection_fields']['PrimaryActivity'] == [
        'Petroleum Industrial - Motor Gasoline'
    ]
    distillate = sets['petroleum_distillate']
    assert distillate['selection_fields']['PrimaryActivity'] == [
        'Petroleum Industrial - Distillate Fuel Oil'
    ]
    assert (
        'Energy_industrial_distillate_national_2024' in distillate['attribution_source']
    )
