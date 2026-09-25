"""Grade trade electricity seed candidates for #899.

Candidate A (``uniform_eia_commercial``) and B (``qcew_payroll``) are seed-index
estimators. ``carry`` is the pre-pin baseline (``trade_electricity_pin=False``).
``pinned_a2017`` is the true pin (default-on production path). Slack bars and
the choose/wire rule live in the #896 plan (§2A.3 / §2A.3b).

::

    uv run python -m bedrock.analysis.nowcasting.trade_electricity_seed_grade \\
        --csv [--check]
"""

from __future__ import annotations

import argparse
import csv
import math
import typing as ta
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd

from bedrock.analysis.nowcasting.trade_electricity_seed import (
    ELECTRICITY_ROW,
    Candidate,
    held_missing_qcew_count,
    qcew_detail_payroll,
    trade_electricity_seed,
    trade_seed_set,
)

GRADE_YEAR_A = 2017
GRADE_YEAR_B = 2024
CV_YEARS = tuple(range(2018, 2023))  # 2018-22 inclusive
CRISIS_YEARS = [2022, 2023, 2024]
FLOOR_M = 10.0  # $M absolute floor for uniform_yoy_spread industries
IDIO_PP = 25.0
CV_MAX = 0.35
CV_MIN_INDUSTRIES = 5
ANTI_CARRY_ATOL_M = 1e-6
SANITY_SPREAD_PP = 0.01
B_SPREAD_PP = 5.0

SEED_CANDIDATES: tuple[Candidate, ...] = ('uniform_eia_commercial', 'qcew_payroll')
GradeCandidate = ta.Literal[
    'carry',
    'uniform_eia_commercial',
    'qcew_payroll',
    'pinned_a2017',
]
ALL_GRADE_CANDIDATES: tuple[GradeCandidate, ...] = (
    'carry',
    'uniform_eia_commercial',
    'qcew_payroll',
    'pinned_a2017',
)
BASELINE_CANDIDATES: frozenset[str] = frozenset({'carry', 'pinned_a2017'})


class TradeElectricityGradeSummary(ta.NamedTuple):
    candidate: str
    grade_year_a: int
    grade_year_b: int
    max_uniform_yoy_spread: float
    physical_proxy_cv: float
    idiosyncratic_flag_count: int
    held_missing_qcew_count: int
    weighted_corr_mut_2018_22: float
    eia_level_gap_pct_2017: float
    pass_slack_bars: bool
    mut_vintage: str


class TradeElectricityGradeSpan(ta.NamedTuple):
    candidate: str
    year_a: int
    year_b: int
    uniform_yoy_spread: float
    pass_span_spread: bool
    mut_vintage: str


class TradeElectricityDisplacementRow(ta.NamedTuple):
    band: str
    year_a: int
    year_b: int
    share_effect_bn_unpinned: float
    share_effect_bn_pinned: float
    delta_bn: float
    mut_vintage: str


def _industries() -> list[str]:
    return sorted(trade_seed_set())


def _use2017_trade_row() -> 'pd.Series[float]':
    from bedrock.analysis.nowcasting.trade_electricity_seed import (  # noqa: PLC0415
        _use_2017_detail,
    )

    industries = _industries()
    row = _use_2017_detail().loc[ELECTRICITY_ROW].reindex(industries).astype(float)
    return pd.Series(row.to_numpy(), index=industries, dtype=float)


def _precontrol_panel(
    candidate: GradeCandidate, years: list[int]
) -> dict[int, pd.Series]:
    """year -> Series of seed $M on 221100 × seed-set (pre column control)."""
    if candidate in BASELINE_CANDIDATES:
        # Identity Use2017 — status quo / pin coefficient (spread ~0 by construction).
        base = _use2017_trade_row()
        return {year: base.copy() for year in years}
    seed_cand = ta.cast(Candidate, candidate)
    out: dict[int, pd.Series] = {}
    for year in years:
        seed = trade_electricity_seed(year, candidate=seed_cand)
        row = seed.loc[ELECTRICITY_ROW]
        out[year] = pd.Series(row.astype(float), index=seed.columns, dtype=float)
    return out


def _yoy_pct(a: float, b: float) -> float:
    if a == 0.0 or math.isnan(a) or math.isnan(b):
        return float('nan')
    return 100.0 * (b / a - 1.0)


def uniform_yoy_spread(
    panel: dict[int, pd.Series], year_a: int, year_b: int
) -> tuple[float, int]:
    """max|industry_yoy% − mean_yoy%| over industries with |cell| ≥ $10M both ends.

    Returns ``(spread_pp, n_above_floor)``. Empty floor set → ``(nan, 0)``.
    """
    sa = panel[year_a]
    sb = panel[year_b]
    yoy: list[float] = []
    for industry in sa.index:
        va = float(sa[industry])
        vb = float(sb[industry])
        if abs(va) < FLOOR_M or abs(vb) < FLOOR_M:
            continue
        pct = _yoy_pct(va, vb)
        if math.isnan(pct):
            continue
        yoy.append(pct)
    if not yoy:
        return float('nan'), 0
    mean = float(np.mean(yoy))
    spread = max(abs(v - mean) for v in yoy)
    return spread, len(yoy)


def idiosyncratic_flag_count(panel: dict[int, pd.Series]) -> int:
    """Industries whose 2017→24 growth differs from mean growth by > 25 pp."""
    s0 = panel[GRADE_YEAR_A]
    s1 = panel[GRADE_YEAR_B]
    growths: dict[str, float] = {}
    for industry in s0.index:
        g = _yoy_pct(float(s0[industry]), float(s1[industry]))
        if not math.isnan(g):
            growths[industry] = g
    if not growths:
        return 0
    mean = float(np.mean(list(growths.values())))
    return sum(1 for g in growths.values() if abs(g - mean) > IDIO_PP)


def _pass_spread(candidate: GradeCandidate, spread: float) -> bool:
    if candidate in BASELINE_CANDIDATES:
        return (not math.isnan(spread)) and spread <= SANITY_SPREAD_PP
    if math.isnan(spread):
        return False
    limit = SANITY_SPREAD_PP if candidate == 'uniform_eia_commercial' else B_SPREAD_PP
    return spread <= limit


def eia_level_gap_pct_2017() -> float:
    """(Use2017 trade×221100 $bn − Table 2.3 commercial $bn) / commercial × 100."""
    from bedrock.analysis.electricity.current.eia_gtd.electricity_row_control import (  # noqa: PLC0415
        eia_epa_table_2_3_revenue_bn,
    )

    use_bn = float(_use2017_trade_row().sum()) / 1_000.0
    _, commercial_bn, _, _ = eia_epa_table_2_3_revenue_bn(GRADE_YEAR_A)
    if commercial_bn == 0.0 or math.isnan(commercial_bn):
        raise ValueError('EPA Table 2.3 commercial revenue is 0/NaN at 2017')
    return 100.0 * (use_bn - commercial_bn) / commercial_bn


def _resolve_mut_vintage(mut_vintage: str | None) -> str:
    from bedrock.analysis.nowcasting.results._ef_smoke_lib import (  # noqa: PLC0415
        resolved_mut_vintage,
    )

    return mut_vintage or resolved_mut_vintage()


def weighted_corr_mut(candidate: GradeCandidate, mut_vintage: str) -> float:
    """Weighted Pearson corr of pre-control seed $M vs MUT elec $M, 2018–22."""
    if candidate in BASELINE_CANDIDATES:
        return float('nan')
    from bedrock.analysis.electricity.current.eia_gtd.electricity_row_control import (  # noqa: PLC0415
        load_year,
    )
    from bedrock.utils.economic.units import (  # noqa: PLC0415
        MILLION_CURRENCY_TO_CURRENCY,
    )

    industries = _industries()
    weights = _use2017_trade_row().clip(lower=0.0)
    seed_cand = ta.cast(Candidate, candidate)
    xs: list[float] = []
    ys: list[float] = []
    ws: list[float] = []
    for year in CV_YEARS:
        seed = trade_electricity_seed(year, candidate=seed_cand).loc[ELECTRICITY_ROW]
        panel = load_year(year, mut_vintage)
        mut_m = (
            panel.elec.reindex(industries).astype(float) / MILLION_CURRENCY_TO_CURRENCY
        )
        for industry in industries:
            w = float(weights.get(industry, 0.0) or 0.0)
            if w <= 0.0:
                continue
            x = float(seed.get(industry, float('nan')))
            y = float(mut_m.get(industry, float('nan')))
            if math.isnan(x) or math.isnan(y):
                continue
            xs.append(x)
            ys.append(y)
            ws.append(w)
    if len(xs) < 2:
        return float('nan')
    x_arr = np.asarray(xs, dtype=float)
    y_arr = np.asarray(ys, dtype=float)
    w_arr = np.asarray(ws, dtype=float)
    w_arr = w_arr / w_arr.sum()
    mx = float(np.sum(w_arr * x_arr))
    my = float(np.sum(w_arr * y_arr))
    cov = float(np.sum(w_arr * (x_arr - mx) * (y_arr - my)))
    vx = float(np.sum(w_arr * (x_arr - mx) ** 2))
    vy = float(np.sum(w_arr * (y_arr - my) ** 2))
    if vx <= 0.0 or vy <= 0.0:
        return float('nan')
    return cov / math.sqrt(vx * vy)


def _overlay_on_base(
    base: pd.DataFrame, year: int, candidate: Candidate
) -> pd.DataFrame:
    """Overlay candidate 221100×trade cells onto a composed_seed frame ($M → USD)."""
    from bedrock.utils.economic.units import (  # noqa: PLC0415
        MILLION_CURRENCY_TO_CURRENCY,
    )

    overlay = trade_electricity_seed(year, candidate=candidate)
    columns = [c for c in overlay.columns if c in base.columns]
    rows = [r for r in overlay.index if r in base.index]
    out = base.copy()
    out.loc[rows, columns] = (
        overlay.loc[rows, columns].astype(float) * MILLION_CURRENCY_TO_CURRENCY
    )
    return out


@contextmanager
def _injected_composed_seed(candidate: Candidate) -> ta.Iterator[None]:
    """Patch defining-module ``composed_seed`` so assemble_use_seed sees the overlay."""
    from bedrock.transform.iot import nowcast_intermediate as ni  # noqa: PLC0415

    production = ni.composed_seed

    def _patched(year: int) -> pd.DataFrame:
        return _overlay_on_base(production(year), year, candidate)

    with mock.patch.object(ni, 'composed_seed', _patched):
        yield


def physical_proxy_cv(candidate: GradeCandidate) -> tuple[float, int]:
    """CV of (Step-3 dump electricity $M / QCEW payroll) pooled 2018–22.

    * ``carry`` — ``trade_electricity_pin=False`` (pre-pin status quo).
    * ``pinned_a2017`` — pin on.
    * A/B — seed overlay with pin off.

    Returns ``(cv, n_distinct_industries_with_payroll)``. Empty / thin panel →
    ``(nan, n)``.
    """
    from bedrock.transform.iot.nowcast import (  # noqa: PLC0415
        derive_initial_U_intermediate,
    )
    from bedrock.utils.economic.units import (  # noqa: PLC0415
        MILLION_CURRENCY_TO_CURRENCY,
    )

    industries = _industries()
    ratios: list[float] = []
    seen: set[str] = set()
    payroll_by_year = {
        year: qcew_detail_payroll(year, peer_year=GRADE_YEAR_A)
        .reindex(industries)
        .astype(float)
        for year in CV_YEARS
    }

    def _collect(pin: bool) -> None:
        for year in CV_YEARS:
            interior_m = (
                derive_initial_U_intermediate(year, trade_electricity_pin=pin).astype(
                    float
                )
                / MILLION_CURRENCY_TO_CURRENCY
            )
            elec = pd.Series(
                interior_m.loc[ELECTRICITY_ROW].astype(float), dtype=float
            ).reindex(industries)
            pay = payroll_by_year[year]
            for industry in industries:
                p = float(pay.get(industry, 0.0) or 0.0)
                if p <= 0.0 or math.isnan(p):
                    continue
                e = float(elec.get(industry, float('nan')))
                if math.isnan(e):
                    continue
                ratios.append(e / p)
                seen.add(industry)

    if candidate == 'carry':
        _collect(pin=False)
    elif candidate == 'pinned_a2017':
        _collect(pin=True)
    elif candidate == 'uniform_eia_commercial':
        with _injected_composed_seed('uniform_eia_commercial'):
            _collect(pin=False)
    else:
        with _injected_composed_seed('qcew_payroll'):
            _collect(pin=False)

    n = len(seen)
    if n < CV_MIN_INDUSTRIES or not ratios:
        return float('nan'), n
    arr = np.asarray(ratios, dtype=float)
    mean = float(arr.mean())
    if mean == 0.0 or math.isnan(mean):
        return float('nan'), n
    return float(arr.std(ddof=0) / mean), n


def anti_carry_ok(candidate: Candidate, year: int = 2022) -> bool:
    """At least one seed-set cell differs from unpatched composed_seed (non-base)."""
    from bedrock.transform.iot import nowcast_intermediate as ni  # noqa: PLC0415
    from bedrock.utils.economic.units import (  # noqa: PLC0415
        MILLION_CURRENCY_TO_CURRENCY,
    )

    if year == GRADE_YEAR_A:
        raise ValueError('anti-carry is not required at base_year')
    industries = _industries()
    baseline = (
        ni.composed_seed(year).loc[ELECTRICITY_ROW].reindex(industries).astype(float)
    )
    with _injected_composed_seed(candidate):
        injected = (
            ni.composed_seed(year)
            .loc[ELECTRICITY_ROW]
            .reindex(industries)
            .astype(float)
        )
    delta_m = (injected - baseline).abs() / MILLION_CURRENCY_TO_CURRENCY
    return bool((delta_m > ANTI_CARRY_ATOL_M).any())


def identity_2017_ok(candidate: Candidate) -> bool:
    """Wired cells equal Use2017 at base_year (within atol)."""
    seed = trade_electricity_seed(GRADE_YEAR_A, candidate=candidate)
    industries = list(seed.columns)
    base = _use2017_trade_row().reindex(industries).astype(float)
    got = seed.loc[ELECTRICITY_ROW].reindex(industries).astype(float)
    return bool((got - base).abs().max() <= ANTI_CARRY_ATOL_M)


def year_panel_from_interior_usd(interior_usd: pd.DataFrame) -> ta.Any:
    """Fitted intermediate interior (USD) → ``YearPanel`` for band share-effects."""
    from bedrock.analysis.electricity.current.eia_gtd.electricity_row_control import (  # noqa: PLC0415
        YearPanel,
    )

    if ELECTRICITY_ROW not in interior_usd.index:
        raise ValueError('interior missing electricity row 221100')
    industries = list(interior_usd.columns)
    elec = pd.Series(
        interior_usd.loc[ELECTRICITY_ROW].astype(float).to_numpy(),
        index=industries,
        dtype=float,
    )
    coltot = pd.Series(
        interior_usd.astype(float).sum(axis=0).to_numpy(),
        index=industries,
        dtype=float,
    )
    zeros = pd.Series(0.0, index=industries, dtype=float)
    return YearPanel(
        make=zeros,
        industry_output=0.0,
        elec=elec,
        coltot=coltot,
        y=zeros,
        all_intermediate=float(coltot.sum()),
    )


def displacement_rows(mut_vintage: str) -> list[TradeElectricityDisplacementRow]:
    """Band share-effect Δ (pinned − unpinned) on fitted interiors for crisis spans.

    Uses :func:`~bedrock.transform.iot.nowcast_interior_fit.fit_interior` directly
    (intermediate only) — not full ``assemble_use_seed`` — so Y/VA assembly is not
    paid twice per year.
    """
    from bedrock.analysis.electricity.current.eia_gtd.electricity_row_896 import (  # noqa: PLC0415
        build_band_rows,
        build_claim_sets,
    )
    from bedrock.transform.iot.nowcast_interior_fit import (  # noqa: PLC0415
        fit_interior,
    )

    panels: dict[bool, dict[int, ta.Any]] = {False: {}, True: {}}
    for pin in (False, True):
        for year in CRISIS_YEARS:
            interior = fit_interior(year, trade_electricity_pin=pin).interior.astype(
                float
            )
            panels[pin][year] = year_panel_from_interior_usd(interior)

    sets = build_claim_sets()
    unpinned_rows = build_band_rows(panels[False], CRISIS_YEARS, mut_vintage, sets)
    pinned_rows = build_band_rows(panels[True], CRISIS_YEARS, mut_vintage, sets)
    pinned_by_key = {
        (r.band, r.year_a, r.year_b): r.share_effect_bn for r in pinned_rows
    }
    out: list[TradeElectricityDisplacementRow] = []
    for row in unpinned_rows:
        pinned_bn = float(pinned_by_key[(row.band, row.year_a, row.year_b)])
        unpinned_bn = float(row.share_effect_bn)
        out.append(
            TradeElectricityDisplacementRow(
                band=row.band,
                year_a=row.year_a,
                year_b=row.year_b,
                share_effect_bn_unpinned=unpinned_bn,
                share_effect_bn_pinned=pinned_bn,
                delta_bn=pinned_bn - unpinned_bn,
                mut_vintage=mut_vintage,
            )
        )
    return out


def grade_candidate(
    candidate: GradeCandidate, mut_vintage: str
) -> tuple[TradeElectricityGradeSummary, list[TradeElectricityGradeSpan]]:
    years = list(range(GRADE_YEAR_A, GRADE_YEAR_B + 1))
    panel = _precontrol_panel(candidate, years)

    spans: list[TradeElectricityGradeSpan] = []
    spreads: list[float] = []
    empty_floor = False
    for year_a, year_b in zip(years[:-1], years[1:]):
        spread, n_floor = uniform_yoy_spread(panel, year_a, year_b)
        if n_floor == 0:
            empty_floor = True
        spreads.append(spread)
        spans.append(
            TradeElectricityGradeSpan(
                candidate=candidate,
                year_a=year_a,
                year_b=year_b,
                uniform_yoy_spread=spread,
                pass_span_spread=_pass_spread(candidate, spread),
                mut_vintage=mut_vintage,
            )
        )

    max_spread = max((s for s in spreads if not math.isnan(s)), default=float('nan'))
    idio = idiosyncratic_flag_count(panel)
    held = (
        0
        if candidate != 'qcew_payroll'
        else max(held_missing_qcew_count(y) for y in years)
    )
    cv, cv_n = physical_proxy_cv(candidate)
    gap = eia_level_gap_pct_2017()
    corr = weighted_corr_mut(candidate, mut_vintage)

    if candidate in BASELINE_CANDIDATES:
        # Record-only: absolute Slack bars are not a choose/wire input.
        pass_bars = False
    else:
        spread_ok = (not empty_floor) and all(s.pass_span_spread for s in spans)
        cv_ok = (not math.isnan(cv)) and cv <= CV_MAX and cv_n >= CV_MIN_INDUSTRIES
        idio_ok = idio == 0
        held_ok = held == 0
        if candidate == 'uniform_eia_commercial':
            pass_bars = spread_ok and idio_ok and cv_ok
        else:
            pass_bars = spread_ok and idio_ok and cv_ok and held_ok

    summary = TradeElectricityGradeSummary(
        candidate=candidate,
        grade_year_a=GRADE_YEAR_A,
        grade_year_b=GRADE_YEAR_B,
        max_uniform_yoy_spread=max_spread,
        physical_proxy_cv=cv,
        idiosyncratic_flag_count=idio,
        held_missing_qcew_count=held,
        weighted_corr_mut_2018_22=corr,
        eia_level_gap_pct_2017=gap,
        pass_slack_bars=pass_bars,
        mut_vintage=mut_vintage,
    )
    return summary, spans


def run_checks(
    summaries: list[TradeElectricityGradeSummary],
    spans: list[TradeElectricityGradeSpan],
    displacements: list[TradeElectricityDisplacementRow] | None = None,
) -> int:
    """Return failure count. Absolute Slack FAIL only for A/B; carry/pin record-only."""
    failures = 0

    if not trade_seed_set():
        print('FAIL  trade electricity seed-set is empty after 4200ID filter')
        failures += 1
        return failures

    for summary in summaries:
        cand = summary.candidate
        is_baseline = cand in BASELINE_CANDIDATES

        if not is_baseline:
            seed_cand = ta.cast(Candidate, cand)
            try:
                if not identity_2017_ok(seed_cand):
                    print(f'FAIL  {cand}: 2017 identity (seed != Use2017)')
                    failures += 1
            except Exception as exc:  # noqa: BLE001 — CLI surface
                print(f'FAIL  {cand}: 2017 identity raised: {exc}')
                failures += 1

            try:
                if not anti_carry_ok(seed_cand):
                    print(f'FAIL  {cand}: anti-carry (injected equals carry at 2022)')
                    failures += 1
            except Exception as exc:  # noqa: BLE001
                print(f'FAIL  {cand}: anti-carry raised: {exc}')
                failures += 1

        cand_spans = [s for s in spans if s.candidate == cand]
        if any(math.isnan(s.uniform_yoy_spread) for s in cand_spans):
            print(f'FAIL  {cand}: empty $10M floor set on at least one YoY span')
            failures += 1

        if math.isnan(summary.physical_proxy_cv):
            print(
                f'FAIL  {cand}: physical_proxy_cv is NaN '
                f'(need ≥{CV_MIN_INDUSTRIES} seed-set industries with payroll > 0)'
            )
            failures += 1
        elif (not is_baseline) and summary.physical_proxy_cv > CV_MAX:
            print(
                f'FAIL  {cand}: physical_proxy_cv '
                f'{summary.physical_proxy_cv:.4f} > {CV_MAX}'
            )
            failures += 1

        if is_baseline:
            print(
                f'RECORD  {cand}: cv={summary.physical_proxy_cv:.4f} '
                f'(absolute Slack not a wire gate)'
            )
            continue

        if cand == 'uniform_eia_commercial':
            if summary.max_uniform_yoy_spread > SANITY_SPREAD_PP:
                print(
                    f'FAIL  {cand}: sanity uniform_yoy_spread '
                    f'{summary.max_uniform_yoy_spread:.6f} pp > {SANITY_SPREAD_PP}'
                )
                failures += 1
            if summary.idiosyncratic_flag_count != 0:
                print(
                    f'FAIL  {cand}: sanity idiosyncratic_flag_count '
                    f'{summary.idiosyncratic_flag_count} != 0'
                )
                failures += 1
        else:
            if summary.max_uniform_yoy_spread > B_SPREAD_PP:
                print(
                    f'FAIL  {cand}: uniform_yoy_spread '
                    f'{summary.max_uniform_yoy_spread:.4f} pp > {B_SPREAD_PP}'
                )
                failures += 1
            if summary.idiosyncratic_flag_count != 0:
                print(
                    f'FAIL  {cand}: idiosyncratic_flag_count '
                    f'{summary.idiosyncratic_flag_count} != 0'
                )
                failures += 1
            if summary.held_missing_qcew_count > 0:
                print(
                    f'FAIL  {cand}: held_missing_qcew_count '
                    f'{summary.held_missing_qcew_count} > 0'
                )
                failures += 1

        if not summary.pass_slack_bars:
            print(f'FAIL  {cand}: pass_slack_bars is False')
            failures += 1
        else:
            print(f'PASS  {cand}: pass_slack_bars')

    if displacements is not None:
        if not displacements:
            print('FAIL  displacement table is empty')
            failures += 1
        else:
            spans_seen = {(r.year_a, r.year_b) for r in displacements}
            expected = {(2022, 2023), (2023, 2024)}
            if spans_seen != expected:
                print(f'FAIL  displacement spans {spans_seen} != {expected}')
                failures += 1
            else:
                print(f'PASS  displacement: {len(displacements)} band×span rows')

    print(f'check: {failures} failure(s) over {len(summaries)} candidates')
    return failures


def _write_csv(
    path_summary: str,
    path_spans: str,
    summaries: list[TradeElectricityGradeSummary],
    spans: list[TradeElectricityGradeSpan],
    path_displacement: str | None = None,
    displacements: list[TradeElectricityDisplacementRow] | None = None,
) -> None:
    with open(path_summary, 'w', newline='', encoding='utf-8') as fh:
        writer = csv.DictWriter(
            fh, fieldnames=list(TradeElectricityGradeSummary._fields)
        )
        writer.writeheader()
        for summary_row in summaries:
            writer.writerow(summary_row._asdict())
    with open(path_spans, 'w', newline='', encoding='utf-8') as fh:
        writer = csv.DictWriter(fh, fieldnames=list(TradeElectricityGradeSpan._fields))
        writer.writeheader()
        for span_row in spans:
            writer.writerow(span_row._asdict())
    if path_displacement is not None and displacements is not None:
        with open(path_displacement, 'w', newline='', encoding='utf-8') as fh:
            writer = csv.DictWriter(
                fh, fieldnames=list(TradeElectricityDisplacementRow._fields)
            )
            writer.writeheader()
            for row in displacements:
                writer.writerow(row._asdict())


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--csv', action='store_true')
    parser.add_argument('--check', action='store_true')
    parser.add_argument('--mut-vintage', default=None)
    parser.add_argument(
        '--candidate',
        choices=list(ALL_GRADE_CANDIDATES) + ['all'],
        default='all',
        help='Grade one candidate or all four (default: carry+A+B+pin).',
    )
    parser.add_argument(
        '--skip-displacement',
        action='store_true',
        help='Skip fitted pin-off vs pin-on band displacement (slow).',
    )
    args = parser.parse_args(argv)

    mut_vintage = _resolve_mut_vintage(args.mut_vintage)
    chosen: tuple[GradeCandidate, ...] = (
        ALL_GRADE_CANDIDATES
        if args.candidate == 'all'
        else (ta.cast(GradeCandidate, args.candidate),)
    )

    # Fail fast on EPA / empty seed-set / zero Use2017 before the heavy CV path.
    industries = _industries()
    if not industries:
        print('FAIL  trade electricity seed-set is empty after 4200ID filter')
        raise SystemExit(1)
    try:
        for year in range(GRADE_YEAR_A, GRADE_YEAR_B + 1):
            trade_electricity_seed(year, candidate='uniform_eia_commercial')
    except Exception as exc:  # noqa: BLE001
        print(f'FAIL  Candidate A builder / EPA Table 2.3: {exc}')
        raise SystemExit(1) from exc

    summaries: list[TradeElectricityGradeSummary] = []
    spans: list[TradeElectricityGradeSpan] = []
    for candidate in chosen:
        print(f'grading {candidate} on {GRADE_YEAR_A}-{GRADE_YEAR_B} …')
        summary, cand_spans = grade_candidate(candidate, mut_vintage)
        summaries.append(summary)
        spans.extend(cand_spans)
        print(
            f'  max_spread={summary.max_uniform_yoy_spread:.6f} pp  '
            f'cv={summary.physical_proxy_cv:.4f}  '
            f'idio={summary.idiosyncratic_flag_count}  '
            f'held_qcew={summary.held_missing_qcew_count}  '
            f'pass={summary.pass_slack_bars}'
        )

    displacements: list[TradeElectricityDisplacementRow] | None = None
    if not args.skip_displacement:
        print('measuring residual displacement (fitted pin-off vs pin-on) …')
        displacements = displacement_rows(mut_vintage)
        for row in displacements:
            if abs(row.delta_bn) >= 0.5:
                print(
                    f'  {row.band} {row.year_a}→{row.year_b}: '
                    f'Δ={row.delta_bn:+.2f} bn '
                    f'(pinned={row.share_effect_bn_pinned:+.2f}, '
                    f'unpinned={row.share_effect_bn_unpinned:+.2f})'
                )

    if args.csv:
        out_dir = Path(__file__).resolve().parent
        summary_path = str(out_dir / 'trade_electricity_grade_summary.csv')
        spans_path = str(out_dir / 'trade_electricity_grade_spans.csv')
        disp_path = str(out_dir / 'trade_electricity_displacement.csv')
        _write_csv(
            summary_path,
            spans_path,
            summaries,
            spans,
            path_displacement=disp_path if displacements is not None else None,
            displacements=displacements,
        )
        print(f'wrote {summary_path}')
        print(f'wrote {spans_path}')
        if displacements is not None:
            print(f'wrote {disp_path}')

    # Prefer A if both pass (choose rule); print recommendation.
    by_name = {s.candidate: s for s in summaries}
    a = by_name.get('uniform_eia_commercial')
    b = by_name.get('qcew_payroll')
    pin = by_name.get('pinned_a2017')
    if a is not None and a.pass_slack_bars:
        print('choose: wire uniform_eia_commercial (Candidate A)')
    elif b is not None and b.pass_slack_bars:
        print('choose: wire qcew_payroll (Candidate B; A failed)')
    elif pin is not None:
        print(
            'choose: do not wire A/B; true pin (pinned_a2017) is the production '
            'commercial path (default-on)'
        )
    else:
        print('choose: do not wire; keep carry + failed-grade CSV')

    if args.check:
        raise SystemExit(
            1 if run_checks(summaries, spans, displacements=displacements) else 0
        )


if __name__ == '__main__':
    main()
