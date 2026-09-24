"""Unit tests for #896 gate classification, claim-map, and --anchor-span."""

from __future__ import annotations

import pytest

from bedrock.analysis.electricity.current.eia_gtd.electricity_row_896 import (
    ClaimSets,
    GateYear,
    _parse_anchor_span,
    assign_band,
    band_membership,
    classify_gate,
    make_gate_decision,
    share_parallel,
)


def _sets(**kwargs: frozenset[str]) -> ClaimSets:
    empty: frozenset[str] = frozenset()
    return ClaimSets(
        manufacturing=kwargs.get('manufacturing', empty),
        services_transport=kwargs.get('services_transport', empty),
        agriculture=kwargs.get('agriculture', empty),
        mining=kwargs.get('mining', empty),
        utilities=kwargs.get('utilities', empty),
        trade=kwargs.get('trade', empty),
        trade_unseeded=kwargs.get('trade_unseeded', empty),
    )


def test_parse_anchor_span_ok() -> None:
    assert _parse_anchor_span('2022-2023') == (2022, 2023)
    assert _parse_anchor_span('2020-2021') == (2020, 2021)


def test_parse_anchor_span_rejects_bad() -> None:
    with pytest.raises(ValueError, match='YYYY-YYYY'):
        _parse_anchor_span('2022')
    with pytest.raises(ValueError, match='year_a < year_b'):
        _parse_anchor_span('2023-2022')
    with pytest.raises(ValueError, match='year_a < year_b'):
        _parse_anchor_span('2022-2022')


def test_assign_band_partition_priority() -> None:
    sets = _sets(
        manufacturing=frozenset({'311111'}),
        services_transport=frozenset({'622000'}),
        agriculture=frozenset({'1111A0'}),
        mining=frozenset({'211000'}),
        utilities=frozenset({'221100'}),
        trade=frozenset({'445000'}),
        trade_unseeded=frozenset({'425000'}),
    )
    assert assign_band('445000', sets) == 'trade'
    assert assign_band('425000', sets) == 'trade_unseeded'
    assert assign_band('1111A0', sets) == 'agriculture'
    assert assign_band('211000', sets) == 'mining'
    assert assign_band('221100', sets) == 'utilities'
    assert assign_band('311111', sets) == 'manufacturing'
    assert assign_band('622000', sets) == 'services_transport'
    assert assign_band('23A000', sets) == 'held_2017'


def test_trade_and_trade_unseeded_disjoint_membership() -> None:
    sets = _sets(
        trade=frozenset({'445000', '423100'}),
        trade_unseeded=frozenset({'425000', '4200ID'}),
    )
    membership = band_membership(
        ['445000', '423100', '425000', '4200ID', '311111'], sets
    )
    trade = {i for i, b in membership.items() if b == 'trade'}
    unseeded = {i for i, b in membership.items() if b == 'trade_unseeded'}
    assert trade & unseeded == set()
    assert trade == {'445000', '423100'}
    assert unseeded == {'425000', '4200ID'}


def test_share_parallel_rules() -> None:
    assert share_parallel(-0.0032, -0.0030) is True
    assert share_parallel(-0.0032, 0.0030) is False
    assert share_parallel(-0.0001, -0.0001) is False  # below 0.05pp floor
    assert share_parallel(-0.010, -0.005) is False  # ratio off by >30%


def test_classify_stable_quiet_span() -> None:
    assert (
        classify_gate(
            delta_realized_usd=1e9,
            delta_realized_share=0.0001,
            target_fraction_of_level=None,
            parallel=False,
        )
        == 'stable'
    )


def test_classify_near_zero_level_large_share() -> None:
    assert (
        classify_gate(
            delta_realized_usd=1e9,
            delta_realized_share=-0.003,
            target_fraction_of_level=None,
            parallel=True,
        )
        == 'target_collapse'
    )
    assert (
        classify_gate(
            delta_realized_usd=1e9,
            delta_realized_share=-0.003,
            target_fraction_of_level=None,
            parallel=False,
        )
        == 'allocation'
    )


def test_classify_target_collapse_and_ambiguous() -> None:
    assert (
        classify_gate(
            delta_realized_usd=-50e9,
            delta_realized_share=-0.003,
            target_fraction_of_level=0.85,
            parallel=True,
        )
        == 'target_collapse'
    )
    assert (
        classify_gate(
            delta_realized_usd=-50e9,
            delta_realized_share=-0.003,
            target_fraction_of_level=0.55,
            parallel=True,
        )
        == 'ambiguous'
    )
    assert (
        classify_gate(
            delta_realized_usd=-50e9,
            delta_realized_share=-0.003,
            target_fraction_of_level=0.20,
            parallel=True,
        )
        == 'allocation'
    )


def test_make_gate_decision_sensitivity_fields() -> None:
    ya = GateYear(
        year=2022,
        target_usd=600e9,
        realized_u_usd=590e9,
        all_intermediate_usd=30_000e9,
        target_share=600e9 / 30_000e9,
        realized_share=590e9 / 30_000e9,
        supply_use_gap_usd=10e9,
        mut_vintage='test',
    )
    yb = GateYear(
        year=2023,
        target_usd=540e9,
        realized_u_usd=520e9,
        all_intermediate_usd=30_000e9,
        target_share=540e9 / 30_000e9,
        realized_share=520e9 / 30_000e9,
        supply_use_gap_usd=20e9,
        mut_vintage='test',
    )
    d = make_gate_decision(ya, yb)
    assert d.delta_realized_usd == pytest.approx(-70e9)
    assert d.target_fraction_of_level == pytest.approx(60e9 / 70e9)
    assert d.classification in {
        'target_collapse',
        'allocation',
        'ambiguous',
        'stable',
    }
    assert d.classification_at_0_60 in {
        'target_collapse',
        'allocation',
        'ambiguous',
        'stable',
    }
    assert d.classification_at_0_80 in {
        'target_collapse',
        'allocation',
        'ambiguous',
        'stable',
    }
