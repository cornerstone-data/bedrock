"""Unit tests for electricity diagnostics config ladders."""

from __future__ import annotations

import pytest

from bedrock.analysis.electricity.current.diagnostics.ladders import (
    get_ladder,
    list_ladders,
)


def test_list_and_get_ladders() -> None:
    ids = list_ladders()
    assert ids == [
        'bea_v03_mixed_units',
        'bea_v03_reaggregation',
        'nowcast_2024_reaggregation',
    ]
    for ladder_id in ids:
        assert get_ladder(ladder_id).id == ladder_id


def test_nowcast_ladder_steps_vintage_baselines() -> None:
    ladder = get_ladder('nowcast_2024_reaggregation')
    assert ladder.terminal == 'reaggregation'
    assert ladder.step_ids == (
        'footing',
        'reallocation',
        'three_way',
        'reaggregation',
    )
    assert ladder.steps == (
        (
            'footing',
            '2025_usa_cornerstone_v0_4_nowcast_2024_electricity_footing',
        ),
        (
            'reallocation',
            '2025_usa_cornerstone_v0_4_nowcast_2024_electricity_reallocation',
        ),
        (
            'three_way',
            '2025_usa_cornerstone_v0_4_nowcast_2024_electricity_disaggregation',
        ),
        (
            'reaggregation',
            '2025_usa_cornerstone_v0_4_nowcast_2024_electricity_reaggregation',
        ),
    )
    assert ladder.nowcast_mut_vintage == 'v0.3.0_4276083'
    assert ladder.plain_baseline == '2025_usa_cornerstone_v0_4_nowcast_2024'
    assert ladder.production_baseline == '2025_usa_cornerstone_v0_4'
    assert ladder.plain_baseline != ladder.production_baseline
    assert ladder.terminal_config.endswith('_electricity_reaggregation')


def test_bea_mixed_terminal() -> None:
    ladder = get_ladder('bea_v03_mixed_units')
    assert ladder.terminal == 'mixed_units'
    assert ladder.terminal_config == (
        '2025_usa_cornerstone_v0_3_electricity_mixed_units'
    )
    assert ladder.plain_baseline is None
    assert ladder.production_baseline == '2025_usa_cornerstone_v0_3'


def test_bea_reaggregation_ladder() -> None:
    ladder = get_ladder('bea_v03_reaggregation')
    assert ladder.terminal == 'reaggregation'
    assert 'mixed_units' not in ladder.step_ids
    assert ladder.config_for_step('reaggregation') == (
        '2025_usa_cornerstone_v0_3_electricity_reaggregation'
    )


def test_invalid_ladder_id() -> None:
    with pytest.raises(ValueError, match='unknown ladder'):
        get_ladder('not_a_real_ladder')


def test_config_for_missing_step() -> None:
    ladder = get_ladder('nowcast_2024_reaggregation')
    with pytest.raises(ValueError, match='has no step'):
        ladder.config_for_step('mixed_units')
