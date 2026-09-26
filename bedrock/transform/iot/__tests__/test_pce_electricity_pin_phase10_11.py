"""Hermetic tests for #1008 Phase 10/11 warm pairs, jobs, weighted EIA selector."""

from __future__ import annotations

import math
import typing as ta
from contextlib import nullcontext
from unittest.mock import MagicMock, patch

import pytest

from bedrock.analysis.electricity.current.eia_gtd.pce_electricity_pin import (
    CANDIDATES,
    PcePinDisplacementRow,
    PcePinEiaBandRow,
    PcePinT11Row,
    _run_balances,
    _summarize,
    warm,
    warm_year_arm_pairs,
)


def _t11(candidate: str, year: int, *, ok: bool = True) -> PcePinT11Row:
    return PcePinT11Row(
        candidate=candidate,
        year=year,
        t11_max_abs_residual=0.0 if ok else 1e9,
        skipped='-',
        ok=ok,
        rebase_eia=False,
        baseline_vintage='f709829',
    )


def _eia(
    candidate: str,
    year_a: int,
    year_b: int,
    *,
    shipped: float,
    eia: float,
    band_ok: bool,
) -> PcePinEiaBandRow:
    return PcePinEiaBandRow(
        candidate=candidate,
        year_a=year_a,
        year_b=year_b,
        shipped_yoy_usd=shipped,
        eia_residential_yoy_usd=eia,
        band_ok=band_ok,
        source_note='mut_from_balanced',
        artifact_vintage='test',
        rebase_eia=False,
        baseline_vintage='f709829',
    )


def _trade(candidate: str, abs_bn: float) -> PcePinDisplacementRow:
    return PcePinDisplacementRow(
        candidate=candidate,
        year_a=2021,
        year_b=2022,
        band='trade',
        share_effect_baseline_bn=0.0,
        share_effect_candidate_bn=abs_bn,
        delta_share_effect_bn=abs_bn,
        rebase_eia=False,
        baseline_vintage='f709829',
    )


def test_warm_year_arm_pairs_both_supported() -> None:
    with patch(
        'bedrock.analysis.electricity.current.eia_gtd.pce_electricity_pin.'
        '_rebase_eia_supported',
        return_value=True,
    ):
        pairs = warm_year_arm_pairs([2022, 2023], 'both')
    assert pairs == [
        (2022, False),
        (2023, False),
        (2022, True),
        (2023, True),
    ]


def test_warm_year_arm_pairs_skips_true_when_unsupported() -> None:
    with patch(
        'bedrock.analysis.electricity.current.eia_gtd.pce_electricity_pin.'
        '_rebase_eia_supported',
        return_value=False,
    ):
        pairs = warm_year_arm_pairs([2022], 'both')
    assert pairs == [(2022, False)]


def test_warm_rejects_jobs_gt_one() -> None:
    with pytest.raises(ValueError, match='jobs 1'):
        warm([2022], rebase_eia_mode='false', jobs=2)


def test_warm_calls_assemble_once_per_pair() -> None:
    calls: list[int] = []

    def _fake_assemble(year: int, **kwargs: object) -> object:
        del kwargs
        calls.append(int(year))
        return MagicMock()

    with (
        patch(
            'bedrock.analysis.electricity.current.eia_gtd.pce_electricity_pin.'
            '_rebase_eia_supported',
            return_value=True,
        ),
        patch(
            'bedrock.analysis.electricity.current.eia_gtd.pce_electricity_pin.assemble',
            side_effect=_fake_assemble,
        ),
        patch(
            'bedrock.analysis.electricity.current.eia_gtd.pce_electricity_pin.'
            'temp_usa_config',
            return_value=nullcontext(),
        ),
    ):
        timings = warm([2022, 2023], rebase_eia_mode='both', jobs=1)
    assert [t[:2] for t in timings] == [
        (2022, False),
        (2023, False),
        (2022, True),
        (2023, True),
    ]
    assert calls == [2022, 2023, 2022, 2023]


def test_run_balances_jobs2_matches_jobs1_keys() -> None:
    years = [2022, 2023]
    modes = ('none', 'tier1_fixed')

    def _fake_balance(
        year: int, *, pce_constraint: str = 'none', **kwargs: object
    ) -> MagicMock:
        del kwargs
        yb = MagicMock()
        yb.year = year
        yb.mode = pce_constraint
        return yb

    with patch(
        'bedrock.analysis.electricity.current.eia_gtd.pce_electricity_pin.balance_year',
        side_effect=_fake_balance,
    ):
        sequential = _run_balances(years, modes, rebase_eia=False, jobs=1)

    def _worker(
        year: int, modes_t: tuple[str, ...], rebase_eia: bool
    ) -> dict[str, MagicMock]:
        del rebase_eia
        return {m: _fake_balance(year, pce_constraint=m) for m in modes_t}

    class _ImmediatePool:
        def __init__(self, max_workers: int | None = None) -> None:
            del max_workers

        def __enter__(self) -> '_ImmediatePool':
            return self

        def __exit__(self, *args: object) -> ta.Literal[False]:
            del args
            return False

        def submit(self, fn: object, *a: object, **kw: object) -> MagicMock:
            fut = MagicMock()
            fut.result.side_effect = lambda: fn(*a, **kw)  # type: ignore[operator]
            return fut

        def shutdown(self, wait: bool = True, cancel_futures: bool = False) -> None:
            del wait, cancel_futures

    with (
        patch(
            'bedrock.analysis.electricity.current.eia_gtd.pce_electricity_pin.'
            '_balance_year_modes_worker',
            side_effect=_worker,
        ),
        patch(
            'bedrock.analysis.electricity.current.eia_gtd.pce_electricity_pin.'
            'ProcessPoolExecutor',
            _ImmediatePool,
        ),
        patch(
            'bedrock.analysis.electricity.current.eia_gtd.pce_electricity_pin.'
            'as_completed',
            side_effect=lambda futs: list(futs),
        ),
    ):
        parallel = _run_balances(years, modes, rebase_eia=False, jobs=2)

    assert set(sequential) == set(parallel)
    assert set(sequential) == {
        ('none', 2022),
        ('none', 2023),
        ('tier1_fixed', 2022),
        ('tier1_fixed', 2023),
    }


def test_run_balances_jobs2_fail_fast_no_partial() -> None:
    def _worker(
        year: int, modes_t: tuple[str, ...], rebase_eia: bool
    ) -> dict[str, MagicMock]:
        del modes_t, rebase_eia
        if year == 2023:
            raise RuntimeError('boom')
        return {'none': MagicMock(year=year)}

    class _ImmediatePool:
        def __init__(self, max_workers: int | None = None) -> None:
            del max_workers
            self._shutdown = False

        def __enter__(self) -> '_ImmediatePool':
            return self

        def __exit__(self, *args: object) -> ta.Literal[False]:
            del args
            return False

        def submit(self, fn: object, *a: object, **kw: object) -> MagicMock:
            fut = MagicMock()

            def _result() -> object:
                return fn(*a, **kw)  # type: ignore[operator]

            fut.result.side_effect = _result
            return fut

        def shutdown(self, wait: bool = True, cancel_futures: bool = False) -> None:
            del wait, cancel_futures
            self._shutdown = True

    with (
        patch(
            'bedrock.analysis.electricity.current.eia_gtd.pce_electricity_pin.'
            '_balance_year_modes_worker',
            side_effect=_worker,
        ),
        patch(
            'bedrock.analysis.electricity.current.eia_gtd.pce_electricity_pin.'
            'ProcessPoolExecutor',
            _ImmediatePool,
        ),
        patch(
            'bedrock.analysis.electricity.current.eia_gtd.pce_electricity_pin.'
            'as_completed',
            side_effect=lambda futs: list(futs),
        ),
    ):
        with pytest.raises(RuntimeError, match='boom'):
            _run_balances([2022, 2023], ('none',), rebase_eia=False, jobs=2)


def test_summarize_prefers_lower_weighted_miss_not_binary_band() -> None:
    """Large miss on a low-weight year loses to a better weighted score."""
    candidates = list(CANDIDATES)
    t11 = [_t11(c, y) for c in candidates for y in (2021, 2022, 2023)]
    # A: big miss on low-w year 2022; small miss on high-w 2023
    # B: moderate miss on high-w 2023 only (better weighted)
    # C: worse weighted than B
    eia = [
        _eia('tier1_fixed', 2021, 2022, shipped=10.0, eia=0.0, band_ok=False),
        _eia('tier1_fixed', 2022, 2023, shipped=1.0, eia=0.0, band_ok=True),
        _eia('row_side_target', 2021, 2022, shipped=1.0, eia=0.0, band_ok=False),
        _eia('row_side_target', 2022, 2023, shipped=2.0, eia=0.0, band_ok=False),
        _eia('eia_band', 2021, 2022, shipped=1.0, eia=0.0, band_ok=False),
        _eia('eia_band', 2022, 2023, shipped=20.0, eia=0.0, band_ok=False),
    ]
    disp = [
        _trade('tier1_fixed', 0.5),
        _trade('row_side_target', 0.5),
        _trade('eia_band', 0.01),  # would win under old trade-decider
    ]
    weights = {2022: 1.0, 2023: 100.0}
    # A weighted = (1*10 + 100*1)/101 ≈ 1.089
    # B weighted = (1*1 + 100*2)/101 ≈ 1.990
    # Wait - want B to win. Flip: A has huge miss on high weight.
    eia = [
        _eia('tier1_fixed', 2021, 2022, shipped=1.0, eia=0.0, band_ok=True),
        _eia('tier1_fixed', 2022, 2023, shipped=50.0, eia=0.0, band_ok=False),
        _eia('row_side_target', 2021, 2022, shipped=20.0, eia=0.0, band_ok=False),
        _eia('row_side_target', 2022, 2023, shipped=1.0, eia=0.0, band_ok=False),
        _eia('eia_band', 2021, 2022, shipped=1.0, eia=0.0, band_ok=False),
        _eia('eia_band', 2022, 2023, shipped=40.0, eia=0.0, band_ok=False),
    ]
    # A: (1*1 + 100*50)/101 ≈ 49.5
    # B: (1*20 + 100*1)/101 ≈ 1.19  ← winner
    # C: (1*1 + 100*40)/101 ≈ 39.6  and lowest trade — must not win
    summary = _summarize(
        candidates,
        t11,
        eia,
        disp,
        [],
        rebase_eia=False,
        baseline_vintage='f709829',
        allow_select=True,
        none_abs_delta_by_year=weights,
    )
    selected = [s for s in summary if s.selected]
    assert len(selected) == 1
    assert selected[0].candidate == 'row_side_target'
    assert all(
        not math.isfinite(s.eia_weighted_abs_miss) or s.eia_weighted_abs_miss >= 0
        for s in summary
    )
    # binary band alone does not select A (A has one span ok)
    assert selected[0].eia_all_spans_ok is False


def test_summarize_tiebreak_trade_then_tier1() -> None:
    candidates = list(CANDIDATES)
    t11 = [_t11(c, y) for c in candidates for y in (2022, 2023)]
    # Equal weighted miss for all
    eia = [_eia(c, 2022, 2023, shipped=5.0, eia=0.0, band_ok=False) for c in candidates]
    disp = [
        _trade('tier1_fixed', 0.2),
        _trade('row_side_target', 0.1),
        _trade('eia_band', 0.1),
    ]
    summary = _summarize(
        candidates,
        t11,
        eia,
        disp,
        [],
        rebase_eia=False,
        baseline_vintage='f709829',
        allow_select=True,
        none_abs_delta_by_year={2023: 1.0},
    )
    selected = next(s for s in summary if s.selected)
    # B and C tie on trade 0.1; tier1_fixed preferred among equal trade after
    # filtering to best trade — B and C both 0.1, A 0.2 out. Then _TIE_BREAK
    # prefers tier1_fixed only among the tied pool — pool is B and C, so
    # tier1_fixed (0) beats row_side (1) beats eia_band (2) → row_side wins
    # among {B, C}.
    assert selected.candidate == 'row_side_target'

    # Equal weighted + equal trade → tier1_fixed
    disp2 = [
        _trade('tier1_fixed', 0.1),
        _trade('row_side_target', 0.1),
        _trade('eia_band', 0.1),
    ]
    summary2 = _summarize(
        candidates,
        t11,
        eia,
        disp2,
        [],
        rebase_eia=False,
        baseline_vintage='f709829',
        allow_select=True,
        none_abs_delta_by_year={2023: 1.0},
    )
    assert next(s for s in summary2 if s.selected).candidate == 'tier1_fixed'


def test_summarize_true_arm_computes_score_never_selects() -> None:
    candidates = list(CANDIDATES)
    t11 = [_t11(c, y) for c in candidates for y in (2022, 2023)]
    eia = [_eia(c, 2022, 2023, shipped=3.0, eia=0.0, band_ok=False) for c in candidates]
    summary = _summarize(
        candidates,
        t11,
        eia,
        [_trade(c, 0.1) for c in candidates],
        [],
        rebase_eia=True,
        baseline_vintage='f709829',
        allow_select=False,
        none_abs_delta_by_year={2023: 2.0},
    )
    assert all(not s.eligible and not s.selected for s in summary)
    assert all(math.isfinite(s.eia_weighted_abs_miss) for s in summary)
    assert summary[0].eia_weighted_abs_miss == 3.0  # |3-0| with w=2 → 3
