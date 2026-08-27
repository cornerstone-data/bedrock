"""Unit tests for D0 class-MWh, leftover T&D, and nibble vs clipped tables."""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.analysis.electricity.current.eia_gtd.purchaser_tables import (
    class_nibble_frame,
    d0_class_mwh_frame,
    leftover_td_class_frame,
    leftover_td_purchaser_frame,
    leftover_td_usd,
    optional_implied_cents_kwh_frame,
)
from bedrock.transform.eeio.electricity_gtd_allocation import EIAPurchaserAllocation


def _alloc(
    *,
    bills: dict[str, float],
    classes: dict[str, str],
    mwh: dict[str, float],
    gen: dict[str, float] | None = None,
    clipped: dict[str, bool] | None = None,
    p: float = 50.0,
    egrid_mwh: float = 100.0,
    td_share: float = 0.25,
) -> EIAPurchaserAllocation:
    idx = list(bills)
    bill = pd.Series(bills, dtype=float)
    if gen is None:
        gen = {k: 0.4 * v for k, v in bills.items()}
    gen_s = pd.Series(gen, dtype=float).reindex(idx).astype(float)
    leftover = bill - gen_s
    clip = pd.Series(False, index=idx, dtype=bool)
    if clipped:
        for k, v in clipped.items():
            clip[k] = v
    return EIAPurchaserAllocation(
        bill=bill,
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


def test_d0_identity_matches_class_targets() -> None:
    alloc = _alloc(
        bills={'F01000': 100.0, '1111A0': 200.0, 'F04000': 50.0},
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
    frame = d0_class_mwh_frame(alloc, eia_year=2024, targets=targets)
    by_class = frame.set_index('end_use_class')
    assert by_class.loc['Residential', 'allocator_mwh'] == pytest.approx(10.0)
    assert by_class.loc['Industrial', 'allocator_mwh'] == pytest.approx(20.0)
    assert by_class.loc['Exports', 'allocator_mwh'] == pytest.approx(5.0)
    assert not bool(by_class.loc['Residential', 'nibble'])
    assert not bool(by_class.loc['Industrial', 'nibble'])
    assert by_class.loc['Residential', 'ratio_vs_d0'] == pytest.approx(1.0)


def test_leftover_td_is_bill_minus_gen_dollars() -> None:
    alloc = _alloc(
        bills={'F01000': 100.0, '1111A0': 40.0},
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
        bills={'F01000': 10.0, '452000': 80.0, '1111A0': 50.0},
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


def test_optional_implied_cents_kwh_is_bill_over_mwh() -> None:
    alloc = _alloc(
        bills={'F01000': 160.0},
        classes={'F01000': 'Residential'},
        mwh={'F01000': 1.0},
    )
    frame = optional_implied_cents_kwh_frame(
        alloc, {'Residential': 16.0, 'Commercial': 12.0}
    ).set_index('end_use_class')
    assert frame.loc['Residential', 'implied_cents_kwh'] == pytest.approx(16.0)
    assert frame.loc['Residential', 'table_24_cents_kwh'] == pytest.approx(16.0)
    assert 'Exports' not in frame.index
