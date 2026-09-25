"""CH4 fossil / non-fossil classification for AR6 GWP selection."""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.utils.emissions.ch4_classification import (
    apply_ch4_non_fossil_flowable,
    ch4_non_fossil_mask,
    inventory_table_stem,
)


@pytest.mark.parametrize(
    'meta,expected_stem',
    [
        ('UMD_GHGIA_T_2_S1.direct', 'UMD_GHGIA_T_2_S1'),
        ('EPA_GHGI_T_2_1.direct', 'EPA_GHGI_T_2_1'),
        ('UMD_GHGIA_T_5_3', 'UMD_GHGIA_T_5_3'),
    ],
)
def test_inventory_table_stem_drops_activity_set_suffix(
    meta: str, expected_stem: str
) -> None:
    assert inventory_table_stem(meta) == expected_stem


@pytest.mark.parametrize(
    'meta,sector,expected',
    [
        ('UMD_GHGIA_T_2_S1.direct', '562000', True),
        ('EPA_GHGI_T_2_1.direct', '562000', True),
        ('UMD_GHGIA_T_2_S1.direct', '221300', True),
        ('UMD_GHGIA_T_2_S1.direct', '1111B0', True),
        # Activity-set suffix must not drive the rule: fossil sector stays fossil.
        ('UMD_GHGIA_T_2_S1.electric_power', '211000', False),
        ('UMD_GHGIA_T_2_S1.direct', '211000', False),
        # T_2_10 must not match T_2_1.
        ('EPA_GHGI_T_2_10.something', '562000', False),
        ('UMD_GHGIA_T_5_3.animals', '112000', True),
        ('EPA_GHGI_T_5_6.direct', '112A00', True),
        ('UMD_GHGIA_T_3_11.petroleum_industrial', '324110', False),
    ],
)
def test_ch4_non_fossil_mask(meta: str, sector: str, expected: bool) -> None:
    mask = ch4_non_fossil_mask(
        pd.Series([meta]),
        pd.Series([sector]),
    )
    assert bool(mask.iloc[0]) is expected


def test_apply_ch4_non_fossil_flowable_remaps_landfill_umd() -> None:
    df = pd.DataFrame(
        {
            'Flowable': ['CH4_fossil', 'CH4_fossil', 'CH4_fossil'],
            'MetaSources': [
                'UMD_GHGIA_T_2_S1.direct',
                'EPA_GHGI_T_2_10.direct',
                'UMD_GHGIA_T_3_11.petroleum_industrial',
            ],
            'SectorProducedBy': ['562000', '562000', '324110'],
        }
    )
    apply_ch4_non_fossil_flowable(df)
    assert df['Flowable'].tolist() == [
        'CH4_non_fossil',
        'CH4_fossil',
        'CH4_fossil',
    ]
