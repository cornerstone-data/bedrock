"""Unit tests for class-MWh targets, leftover T&D, and nibble vs clipped tables."""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.analysis.electricity.current.eia_gtd.purchaser_tables import (
    class_mwh_targets_frame,
    class_nibble_frame,
    leftover_td_class_frame,
    leftover_td_purchaser_frame,
    leftover_td_usd,
    manufacturing_mecs_vs_dollar_frame,
    optional_implied_cents_kwh_frame,
    p_share_from_allocation,
)
from bedrock.transform.eeio.electricity_gtd_allocation import EIAPurchaserAllocation


def _alloc(
    *,
    electricity_purchases: dict[str, float],
    classes: dict[str, str],
    mwh: dict[str, float],
    gen: dict[str, float] | None = None,
    clipped: dict[str, bool] | None = None,
    p: float = 50.0,
    egrid_mwh: float = 100.0,
    td_share: float = 0.25,
) -> EIAPurchaserAllocation:
    idx = list(electricity_purchases)
    electricity_purchases = pd.Series(electricity_purchases, dtype=float)
    if gen is None:
        gen = {k: 0.4 * v for k, v in electricity_purchases.items()}
    gen_s = pd.Series(gen, dtype=float).reindex(idx).astype(float)
    leftover = electricity_purchases - gen_s
    clip = pd.Series(False, index=idx, dtype=bool)
    if clipped:
        for k, v in clipped.items():
            clip[k] = v
    return EIAPurchaserAllocation(
        electricity_purchases=electricity_purchases,
        end_use_class=pd.Series(classes),
        mwh=pd.Series(mwh, dtype=float),
        gen_dollars=gen_s,
        t_dollars=leftover * td_share,
        d_dollars=leftover * (1.0 - td_share),
        clipped=clip,
        p=p,
        egrid_mwh=egrid_mwh,
        td_share=td_share,
    )


def test_class_mwh_identity_matches_class_targets() -> None:
    alloc = _alloc(
        electricity_purchases={'F01000': 100.0, '1111A0': 200.0, 'F04000': 50.0},
        classes={
            'F01000': 'Residential',
            '1111A0': 'Industrial',
            'F04000': 'Exports',
        },
        mwh={'F01000': 10.0, '1111A0': 20.0, 'F04000': 5.0},
    )
    targets = {
        'Residential': 10.0,
        'Commercial': 0.0,
        'Industrial': 20.0,
        'Transportation': 0.0,
        'Exports': 5.0,
    }
    frame = class_mwh_targets_frame(alloc, eia_year=2024, targets=targets)
    by_class = frame.set_index('end_use_class')
    assert by_class.loc['Residential', 'allocator_mwh'] == pytest.approx(10.0)
    assert by_class.loc['Industrial', 'allocator_mwh'] == pytest.approx(20.0)
    assert by_class.loc['Exports', 'allocator_mwh'] == pytest.approx(5.0)
    assert not bool(by_class.loc['Residential', 'nibble'])
    assert not bool(by_class.loc['Industrial', 'nibble'])
    assert by_class.loc['Residential', 'ratio_vs_class_target'] == pytest.approx(1.0)


def test_leftover_td_is_electricity_purchases_minus_gen_dollars() -> None:
    alloc = _alloc(
        electricity_purchases={'F01000': 100.0, '1111A0': 40.0},
        classes={'F01000': 'Residential', '1111A0': 'Industrial'},
        mwh={'F01000': 1.0, '1111A0': 0.4},
        gen={'F01000': 25.0, '1111A0': 10.0},
        td_share=0.2,
    )
    leftover = leftover_td_usd(alloc)
    assert leftover['F01000'] == pytest.approx(75.0)
    assert leftover['1111A0'] == pytest.approx(30.0)
    purchasers = leftover_td_purchaser_frame(alloc).set_index('purchaser')
    assert purchasers.loc['F01000', 'leftover_td'] == pytest.approx(75.0)
    assert purchasers.loc['F01000', 't_dollars'] == pytest.approx(15.0)
    assert purchasers.loc['F01000', 'd_dollars'] == pytest.approx(60.0)
    assert purchasers['leftover_td'].to_numpy() == pytest.approx(
        (purchasers['t_dollars'] + purchasers['d_dollars']).to_numpy()
    )
    by_class = leftover_td_class_frame(alloc).set_index('end_use_class')
    assert by_class.loc['Residential', 'leftover_td'] == pytest.approx(75.0)
    assert by_class.loc['Industrial', 'leftover_td'] == pytest.approx(30.0)


def test_nibble_is_class_totals_clipped_is_purchaser_only() -> None:
    alloc = _alloc(
        electricity_purchases={'F01000': 10.0, '452000': 80.0, '1111A0': 50.0},
        classes={
            'F01000': 'Residential',
            '452000': 'Commercial',
            '1111A0': 'Industrial',
        },
        mwh={'F01000': 8.0, '452000': 20.0, '1111A0': 20.0},
        clipped={'F01000': False, '452000': True, '1111A0': False},
    )
    targets = {
        'Residential': 10.0,
        'Commercial': 20.0,
        'Industrial': 20.0,
        'Transportation': 0.0,
        'Exports': 0.0,
    }
    nibble = class_nibble_frame(alloc, eia_year=2024, targets=targets).set_index(
        'end_use_class'
    )
    assert bool(nibble.loc['Residential', 'nibble'])
    assert not bool(nibble.loc['Commercial', 'nibble'])
    assert nibble.loc['Commercial', 'n_clipped_purchasers'] == 1
    assert nibble.loc['Residential', 'n_clipped_purchasers'] == 0
    purchasers = leftover_td_purchaser_frame(alloc).set_index('purchaser')
    assert bool(purchasers.loc['452000', 'clipped'])
    assert not bool(purchasers.loc['F01000', 'clipped'])


def test_optional_implied_cents_kwh_is_electricity_purchases_over_mwh() -> None:
    alloc = _alloc(
        electricity_purchases={'F01000': 160.0},
        classes={'F01000': 'Residential'},
        mwh={'F01000': 1.0},
    )
    frame = optional_implied_cents_kwh_frame(
        alloc, {'Residential': 16.0, 'Commercial': 12.0}
    ).set_index('end_use_class')
    assert frame.loc['Residential', 'implied_cents_kwh'] == pytest.approx(16.0)
    assert frame.loc['Residential', 'table_24_cents_kwh'] == pytest.approx(16.0)
    assert 'Exports' not in frame.index


def test_p_share_from_allocation_recovers_generation_share() -> None:
    alloc = _alloc(
        electricity_purchases={'F01000': 200.0, '1111A0': 100.0},
        classes={'F01000': 'Residential', '1111A0': 'Industrial'},
        mwh={'F01000': 2.0, '1111A0': 1.0},
        p=40.0,
        egrid_mwh=150.0,
    )
    assert p_share_from_allocation(alloc) == pytest.approx(40.0 * 150.0 / 300.0)


def test_manufacturing_mecs_vs_dollar_flags() -> None:
    mecs = _alloc(
        electricity_purchases={'331110': 0.0, '1111A0': 50.0, 'F01000': 80.0},
        classes={
            '331110': 'Industrial',
            '1111A0': 'Industrial',
            'F01000': 'Residential',
        },
        mwh={'331110': 10.0, '1111A0': 5.0, 'F01000': 8.0},
        gen={'331110': 0.0, '1111A0': 40.0, 'F01000': 20.0},
        clipped={'331110': True, '1111A0': False, 'F01000': False},
    )
    dollars = _alloc(
        electricity_purchases={'331110': 0.0, '1111A0': 50.0, 'F01000': 80.0},
        classes={
            '331110': 'Industrial',
            '1111A0': 'Industrial',
            'F01000': 'Residential',
        },
        mwh={'331110': 0.0, '1111A0': 20.0, 'F01000': 8.0},
        gen={'331110': 0.0, '1111A0': 20.0, 'F01000': 20.0},
    )
    frame = manufacturing_mecs_vs_dollar_frame(mecs, dollars).set_index('purchaser')
    assert 'F01000' not in frame.index
    assert bool(frame.loc['331110', 'manufacturing'])
    assert bool(frame.loc['331110', 'zero_electricity_purchases_mecs_assignee'])
    assert bool(frame.loc['331110', 'clipped_mecs'])
    assert not bool(frame.loc['1111A0', 'manufacturing'])
    assert bool(frame.loc['1111A0', 'cross_pool_overflow_recipient'])
    assert frame.loc['331110', 'mecs_mwh'] == pytest.approx(10.0)
    assert frame.loc['331110', 'dollar_mwh'] == pytest.approx(0.0)
