"""Unit tests for #990 Phase 2T attribution arithmetic (no live extracts)."""

from __future__ import annotations

import pytest

from bedrock.analysis.electricity.current.eia_gtd.target_attribution_221100 import (
    SpanDecision,
    TargetAttributionRow,
    YearLevels,
    _published_band_ok,
    build_span_rows,
    check_span_identity,
    decide_span,
    overall_outcome,
)


def _levels(
    year: int,
    *,
    t016: float,
    y_pce: float,
    y_other: float,
    bea_go: float | None = None,
    eia_res: float | None = None,
) -> YearLevels:
    sigma = y_pce + y_other
    interior = t016 - sigma
    return YearLevels(
        year=year,
        t016_usd=t016,
        y_pce_usd=y_pce,
        y_other_usd=y_other,
        sigma_y_usd=sigma,
        interior_usd=interior,
        bea_go_usd=bea_go if bea_go is not None else t016,
        eia_residential_usd=eia_res if eia_res is not None else y_pce,
    )


def test_signed_identity_and_residual() -> None:
    ya = _levels(2022, t016=400e9, y_pce=200e9, y_other=50e9)
    yb = _levels(2023, t016=340e9, y_pce=210e9, y_other=40e9)
    rows = build_span_rows(ya, yb)
    assert check_span_identity(rows) == []

    by = {r.component: r for r in rows}
    # ΔT016 = −60bn; ΔY_PCE = +10bn → contrib −10bn; ΔY_other = −10bn → contrib +10bn
    assert by['T016'].delta_usd == pytest.approx(-60e9)
    assert by['Y_PCE'].delta_usd == pytest.approx(-10e9)
    assert by['Y_other'].delta_usd == pytest.approx(10e9)
    assert by['interior_row_target'].delta_usd == pytest.approx(-60e9)
    assert by['residual_unexplained'].delta_usd == pytest.approx(0.0)


def test_y_other_is_sigma_minus_pce() -> None:
    ya = _levels(2022, t016=100e9, y_pce=30e9, y_other=20e9)
    assert ya.y_other_usd == pytest.approx(ya.sigma_y_usd - ya.y_pce_usd)


def test_small_interior_fraction_none() -> None:
    ya = _levels(2022, t016=10e9, y_pce=1e9, y_other=1e9)
    yb = _levels(2023, t016=11e9, y_pce=1.1e9, y_other=1e9)  # interior Δ ~$0.9bn
    rows = build_span_rows(ya, yb)
    assert abs(rows[3].delta_usd) < 5e9  # interior
    assert all(r.fraction_of_delta_target is None for r in rows)
    assert check_span_identity(rows) == []


def test_check_refuses_non_none_fraction_when_small() -> None:
    bad = [
        TargetAttributionRow(2022, 2023, 'T016', 1e9, 0.5, 'x'),
        TargetAttributionRow(2022, 2023, 'Y_PCE', 0.0, None, 'x'),
        TargetAttributionRow(2022, 2023, 'Y_other', 0.0, None, 'x'),
        TargetAttributionRow(2022, 2023, 'interior_row_target', 1e9, None, 'x'),
        TargetAttributionRow(2022, 2023, 'residual_unexplained', 0.0, None, 'x'),
    ]
    fails = check_span_identity(bad)
    assert any('fraction must be None' in f for f in fails)


def test_check_does_not_require_five_row_sum() -> None:
    """Parts identity uses T016+Y_PCE+Y_other only — not all five rows."""
    ya = _levels(2022, t016=400e9, y_pce=200e9, y_other=50e9)
    yb = _levels(2023, t016=300e9, y_pce=200e9, y_other=50e9)
    rows = build_span_rows(ya, yb)
    # Summing all five would double-count interior; identity still passes.
    assert check_span_identity(rows) == []
    five = sum(r.delta_usd for r in rows)
    interior = next(r for r in rows if r.component == 'interior_row_target').delta_usd
    assert five != pytest.approx(interior)


def test_attributed_when_t016_majority_tracks_bea() -> None:
    ya = _levels(2022, t016=420e9, y_pce=200e9, y_other=50e9, bea_go=415e9)
    yb = _levels(2023, t016=350e9, y_pce=205e9, y_other=48e9, bea_go=348e9)
    rows = build_span_rows(ya, yb)
    d = decide_span(rows, ya, yb)
    assert d.outcome == 'Attributed'
    assert d.dominant == 'T016'
    assert d.dominant_fraction is not None
    assert abs(d.dominant_fraction) >= 0.50


def test_fix_when_y_other_dominant() -> None:
    # Interior drop driven by Y_other rise (contrib −ΔY_other large).
    ya = _levels(2022, t016=400e9, y_pce=100e9, y_other=50e9)
    yb = _levels(2023, t016=395e9, y_pce=100e9, y_other=120e9)
    rows = build_span_rows(ya, yb)
    d = decide_span(rows, ya, yb)
    assert d.outcome == 'Fix'
    assert d.dominant == 'Y_other'


def test_fix_when_published_band_fails() -> None:
    ya = _levels(2022, t016=420e9, y_pce=200e9, y_other=50e9, bea_go=420e9)
    yb = _levels(
        2023, t016=350e9, y_pce=200e9, y_other=50e9, bea_go=410e9
    )  # BEA only −10
    rows = build_span_rows(ya, yb)
    d = decide_span(rows, ya, yb)
    assert d.outcome == 'Fix'
    assert 'published YoY band failed' in d.reason


def test_named_defect_forces_fix() -> None:
    ya = _levels(2022, t016=420e9, y_pce=200e9, y_other=50e9)
    yb = _levels(2023, t016=350e9, y_pce=200e9, y_other=50e9)
    rows = build_span_rows(ya, yb)
    d = decide_span(rows, ya, yb, derivation_defect='nowcast.py: unsourced T007')
    assert d.outcome == 'Fix'
    assert 'named derivation defect' in d.reason


def test_overall_needs_all_spans_attributed() -> None:
    ok = SpanDecision(2022, 2023, 'Attributed', 'T016', 0.9, 'ok')
    bad = SpanDecision(2023, 2024, 'Fix', 'Y_PCE', 0.6, 'band fail')
    assert overall_outcome([ok, ok]) == 'Attributed'
    assert overall_outcome([ok, bad]) == 'Fix'


def test_published_band_helpers() -> None:
    assert _published_band_ok(-70e9, -68e9)
    assert _published_band_ok(-70e9, -65e9)  # within $5bn
    assert not _published_band_ok(-70e9, 10e9)  # sign flip
    assert not _published_band_ok(-70e9, -20e9)  # far absolute + relative
