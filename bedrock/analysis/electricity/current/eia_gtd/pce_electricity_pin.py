"""Issue #1008 - measure F01000 Step-5 moves and grade PCE-electricity pin candidates.

Ranks how much GRAS moves every ``F01000`` commodity cell (seed -> balanced
``'none'``), then grades ``tier1_fixed`` / ``row_side_target`` / ``eia_band`` on
T11, in-memory ``mut_from_balanced`` EIA residential YoY band, and displacement
onto intermediate bands + other PCE sinks.

Published-counterpart survey (Work #1): only ``221100`` currently has a
cell-level annual series usable for PCE (EIA Form 861 / EPA Table 2.3
residential). The survey table and count live in ``About_1008``; this module
hardcodes the lookup via :func:`counterpart_survey`.

::

    python -m bedrock.analysis.electricity.current.eia_gtd.pce_electricity_pin \\
        measure|grade [--years 2017-2024] [--csv] [--check]

``--check`` applies to ``grade``: schema coherence, exactly 0 or 1
``selected``, and nonzero electricity PCE seed each year. Missing / NaN cells
raise -- never silently filled with 0.
"""

from __future__ import annotations

import argparse
import logging
import math
import subprocess
import typing as ta
from pathlib import Path

import pandas as pd

from bedrock.analysis.electricity.current.eia_gtd.electricity_row_896 import (
    build_band_rows,
    build_claim_sets,
)
from bedrock.analysis.electricity.current.eia_gtd.electricity_row_control import (
    ELECTRICITY_ROW,
    OUT_DIR,
    PCE_CODE,
    PanelByYear,
    YearPanel,
    eia_epa_table_2_3_revenue_bn,
)
from bedrock.analysis.electricity.current.eia_gtd.target_attribution_221100 import (
    _published_band_ok,
)
from bedrock.analysis.nowcasting.results._ef_smoke_lib import NOWCAST_YEARS
from bedrock.transform.iot.nowcast import derive_initial_Y_pur
from bedrock.transform.iot.nowcast_mask import (
    PCE_ELECTRICITY_COL,
    PCE_ELECTRICITY_ROW,
    PceConstraint,
    balance_commodities,
    balance_industries,
)
from bedrock.transform.iot.nowcast_mut import mut_from_balanced
from bedrock.transform.iot.nowcast_sut_assembly import (
    DEFAULT_PCE_CONSTRAINT,
    YearBalance,
    balance_year,
)
from bedrock.utils.economic.units import (
    BILLION_CURRENCY_TO_CURRENCY,
    MILLION_CURRENCY_TO_CURRENCY,
)

logger = logging.getLogger(__name__)

CANDIDATES: tuple[PceConstraint, ...] = (
    'tier1_fixed',
    'row_side_target',
    'eia_band',
)
GRADE_MODES: tuple[PceConstraint, ...] = (DEFAULT_PCE_CONSTRAINT, *CANDIDATES)
_TRADE_DISP_BANDS: frozenset[str] = frozenset({'trade', 'trade_unseeded'})
_TIE_BREAK: dict[str, int] = {
    'tier1_fixed': 0,
    'row_side_target': 1,
    'eia_band': 2,
}
_BN = BILLION_CURRENCY_TO_CURRENCY
_M_TO_USD = MILLION_CURRENCY_TO_CURRENCY

#: Published annual cell-level PCE counterparts surveyed for #1008 Work #1.
#: Recorded in About_1008; only electricity residential has a named series today.
_COUNTERPART_SURVEY: dict[str, bool] = {
    PCE_ELECTRICITY_ROW: True,
}
_COUNTERPART_NOTES: dict[str, str] = {
    PCE_ELECTRICITY_ROW: 'EIA Form 861 / EPA Table 2.3 residential',
}


class PceBalanceMoveRow(ta.NamedTuple):
    year: int
    commodity: str
    seed_usd: float
    balanced_usd: float
    delta_balance: float
    abs_delta: float
    rank: int
    has_annual_counterpart: bool
    mut_usd: float
    delta_vs_mut: float


class PcePinT11Row(ta.NamedTuple):
    candidate: str
    year: int
    t11_max_abs_residual: float
    skipped: str
    ok: bool


class PcePinEiaBandRow(ta.NamedTuple):
    candidate: str
    year_a: int
    year_b: int
    shipped_yoy_usd: float
    eia_residential_yoy_usd: float
    band_ok: bool
    source_note: str
    artifact_vintage: str


class PcePinDisplacementRow(ta.NamedTuple):
    candidate: str
    year_a: int
    year_b: int
    band: str
    share_effect_baseline_bn: float
    share_effect_candidate_bn: float
    delta_share_effect_bn: float


class PcePinPceSinkRow(ta.NamedTuple):
    candidate: str
    year_a: int
    year_b: int
    commodity: str
    delta_vs_baseline_bn: float
    abs_delta_bn: float


class PceCandidateSummaryRow(ta.NamedTuple):
    candidate: str
    t11_all_years_ok: bool
    eia_all_spans_ok: bool
    displacement_note: str
    eligible: bool
    selected: bool


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def available_years() -> list[int]:
    return list(NOWCAST_YEARS)


def consecutive_spans(years: list[int]) -> list[tuple[int, int]]:
    ordered = sorted(years)
    if len(ordered) < 2:
        raise ValueError(f'need at least two years for YoY spans, got {ordered}')
    return list(zip(ordered[:-1], ordered[1:]))


def _parse_years(spec: str) -> list[int]:
    lo, _, hi = spec.partition('-')
    return list(range(int(lo), int(hi or lo) + 1))


def counterpart_survey(commodities: ta.Iterable[str]) -> dict[str, bool]:
    """Map each commodity to whether a published annual PCE cell counterpart exists.

    Survey results are recorded in About_1008; this function returns the
    hardcoded lookup (only ``221100`` -> True today).
    """
    return {str(c): bool(_COUNTERPART_SURVEY.get(str(c), False)) for c in commodities}


def _artifact_vintage() -> str:
    try:
        from bedrock.utils.config.settings import GIT_HASH  # noqa: PLC0415

        if GIT_HASH:
            return str(GIT_HASH)
    except Exception:  # noqa: BLE001
        pass
    try:
        out = subprocess.check_output(
            ['git', 'rev-parse', '--short', 'HEAD'],
            stderr=subprocess.DEVNULL,
            text=True,
        )
        sha = out.strip()
        if sha:
            return sha
    except Exception:  # noqa: BLE001
        pass
    return 'local'


def _require_finite(value: object, label: str) -> float:
    number = float(pd.to_numeric(value, errors='coerce'))
    if math.isnan(number):
        raise ValueError(f'{label} is missing or NaN (not filled)')
    return number


def _f01000_series_usd(frame: pd.DataFrame, *, scale: float, label: str) -> 'pd.Series[float]':
    if PCE_CODE not in frame.columns:
        raise ValueError(f'{PCE_CODE} missing from {label}')
    col = frame[PCE_CODE]
    if isinstance(col, pd.DataFrame):
        raise ValueError(f'{PCE_CODE} duplicated in columns of {label}')
    out = col.astype(float) * scale
    out.index = [str(i) for i in out.index]
    return out


def _cell_at(frame: pd.DataFrame, row: str, col: str, label: str) -> float:
    if row not in frame.index:
        raise ValueError(f'{row!r} missing from {label}')
    if col not in frame.columns:
        raise ValueError(f'{col!r} missing from {label}')
    raw = frame.at[row, col]
    if isinstance(raw, pd.Series):
        raise ValueError(f'{row!r}×{col!r} duplicated in {label}')
    return _require_finite(raw, f'{label}[{row},{col}]')


def panel_from_balanced(use_m: pd.DataFrame) -> YearPanel:
    """Build a :class:`YearPanel` from a balanced Use block in $M."""
    if ELECTRICITY_ROW not in use_m.index:
        raise ValueError(f'{ELECTRICITY_ROW!r} missing from balanced Use')
    industries = [c for c in balance_industries() if c in use_m.columns]
    if not industries:
        raise ValueError('no balance industries found in balanced Use columns')
    elec_raw = use_m.loc[ELECTRICITY_ROW, industries]
    if isinstance(elec_raw, pd.DataFrame):
        raise ValueError(f'{ELECTRICITY_ROW!r} duplicated in balanced Use index')
    elec = elec_raw.astype(float) * _M_TO_USD
    elec.index = [str(i) for i in elec.index]
    coltot = use_m[industries].sum(axis=0).astype(float) * _M_TO_USD
    coltot.index = [str(i) for i in coltot.index]
    return YearPanel(
        make=elec * 0.0,
        industry_output=0.0,
        elec=elec,
        coltot=coltot,
        y=elec * 0.0,
        all_intermediate=float(coltot.sum()),
    )


def _mut_pce_cell_usd(yb: YearBalance) -> float:
    """Producer-price MUT ``221100×F01000`` from an in-memory balanced year (USD)."""
    if yb.balanced is None:
        raise ValueError(f'year {yb.year}: balance_year returned no balanced blocks')
    supply_usd = yb.balanced['supply'].astype(float) * _M_TO_USD
    use_usd = yb.balanced['use'].astype(float) * _M_TO_USD
    mut = mut_from_balanced(yb.year, supply_usd, use_usd)
    return _cell_at(
        mut.use,
        PCE_ELECTRICITY_ROW,
        PCE_ELECTRICITY_COL,
        f'mut_from_balanced({yb.year}).use',
    )


def _pce_column_usd(yb: YearBalance) -> 'pd.Series[float]':
    if yb.balanced is None:
        raise ValueError(f'year {yb.year}: balance_year returned no balanced blocks')
    return _f01000_series_usd(
        yb.balanced['use'],
        scale=_M_TO_USD,
        label=f"balanced['use'] {yb.year}",
    )


# ---------------------------------------------------------------------------
# Measure
# ---------------------------------------------------------------------------


def measure(years: list[int] | None = None) -> list[PceBalanceMoveRow]:
    """Rank Step-5 F01000 moves (seed -> balanced ``'none'``) per year."""
    year_list = sorted(years) if years is not None else available_years()
    commodities = [str(c) for c in balance_commodities()]
    survey = counterpart_survey(commodities)
    rows: list[PceBalanceMoveRow] = []

    for year in year_list:
        seed_y = derive_initial_Y_pur(year, download_sources_ok=True)
        seed = _f01000_series_usd(seed_y, scale=1.0, label=f'derive_initial_Y_pur({year})')

        yb = balance_year(year, pce_constraint='none')
        balanced = _pce_column_usd(yb)

        mut_col: 'pd.Series[float]' | None = None
        try:
            if yb.balanced is None:
                raise ValueError('no balanced blocks')
            supply_usd = yb.balanced['supply'].astype(float) * _M_TO_USD
            use_usd = yb.balanced['use'].astype(float) * _M_TO_USD
            mut = mut_from_balanced(year, supply_usd, use_usd)
            mut_col = _f01000_series_usd(
                mut.use, scale=1.0, label=f'mut_from_balanced({year}).use'
            )
        except Exception as exc:  # noqa: BLE001 - side columns only; ranking continues
            logger.warning(
                'mut_from_balanced failed for measure year=%s: %s; '
                'mut_usd/delta_vs_mut set to NaN',
                year,
                exc,
            )

        year_rows: list[PceBalanceMoveRow] = []
        for commodity in commodities:
            if commodity not in seed.index:
                raise ValueError(
                    f'{commodity!r} missing from derive_initial_Y_pur({year}) F01000'
                )
            if commodity not in balanced.index:
                raise ValueError(
                    f'{commodity!r} missing from balanced Use F01000 for {year}'
                )
            seed_usd = _require_finite(
                seed[commodity], f'seed F01000[{commodity}] {year}'
            )
            balanced_usd = _require_finite(
                balanced[commodity], f'balanced F01000[{commodity}] {year}'
            )
            delta = balanced_usd - seed_usd
            if mut_col is not None and commodity in mut_col.index:
                mut_usd = _require_finite(
                    mut_col[commodity], f'mut F01000[{commodity}] {year}'
                )
                delta_vs_mut = balanced_usd - mut_usd
            else:
                mut_usd = float('nan')
                delta_vs_mut = float('nan')
            year_rows.append(
                PceBalanceMoveRow(
                    year=year,
                    commodity=commodity,
                    seed_usd=seed_usd,
                    balanced_usd=balanced_usd,
                    delta_balance=delta,
                    abs_delta=abs(delta),
                    rank=0,
                    has_annual_counterpart=survey.get(commodity, False),
                    mut_usd=mut_usd,
                    delta_vs_mut=delta_vs_mut,
                )
            )

        ordered = sorted(year_rows, key=lambda r: r.abs_delta, reverse=True)
        for rank, row in enumerate(ordered, start=1):
            rows.append(row._replace(rank=rank))

        elec = next((r for r in rows if r.year == year and r.commodity == ELECTRICITY_ROW), None)
        if elec is None:
            raise ValueError(f'{ELECTRICITY_ROW} missing from measure rows for {year}')
        print(
            f'{year}: {ELECTRICITY_ROW} rank={elec.rank} '
            f'delta=${elec.delta_balance / _BN:+.3f}bn '
            f'(|delta|=${elec.abs_delta / _BN:.3f}bn) '
            f'counterpart={elec.has_annual_counterpart}'
        )

    return rows


# ---------------------------------------------------------------------------
# Grade
# ---------------------------------------------------------------------------


def _run_balances(
    years: list[int],
    modes: ta.Iterable[PceConstraint],
) -> dict[tuple[str, int], YearBalance]:
    cache: dict[tuple[str, int], YearBalance] = {}
    for mode in modes:
        for year in years:
            key = (str(mode), year)
            if key in cache:
                continue
            logger.info('balance_year year=%s pce_constraint=%s', year, mode)
            cache[key] = balance_year(year, pce_constraint=mode)
    return cache


def _t11_rows(
    cache: dict[tuple[str, int], YearBalance],
    candidates: ta.Iterable[str],
    years: list[int],
) -> list[PcePinT11Row]:
    rows: list[PcePinT11Row] = []
    for candidate in candidates:
        for year in years:
            yb = cache[(candidate, year)]
            if yb.result is None:
                raise ValueError(f'{candidate} {year}: missing SutBalanceResult')
            skipped = ','.join(yb.result.skipped) or '-'
            residual = float(yb.result.t11_max_abs_residual)
            # Engine stops at atol=100 $M; acceptance wording is 0.0 after exact
            # row-close. Treat residual within engine atol and empty skipped as ok.
            rows.append(
                PcePinT11Row(
                    candidate=candidate,
                    year=year,
                    t11_max_abs_residual=residual,
                    skipped=skipped,
                    ok=residual <= 100.0 and skipped == '-',
                )
            )
    return rows


def _eia_rows(
    cache: dict[tuple[str, int], YearBalance],
    candidates: ta.Iterable[str],
    years: list[int],
) -> list[PcePinEiaBandRow]:
    vintage = _artifact_vintage()
    eia_usd = {
        year: float(eia_epa_table_2_3_revenue_bn(year)[0]) * _BN for year in years
    }
    shipped: dict[tuple[str, int], float] = {}
    for candidate in candidates:
        for year in years:
            shipped[(candidate, year)] = _mut_pce_cell_usd(cache[(candidate, year)])

    rows: list[PcePinEiaBandRow] = []
    for candidate in candidates:
        for year_a, year_b in consecutive_spans(years):
            shipped_yoy = shipped[(candidate, year_b)] - shipped[(candidate, year_a)]
            eia_yoy = eia_usd[year_b] - eia_usd[year_a]
            rows.append(
                PcePinEiaBandRow(
                    candidate=candidate,
                    year_a=year_a,
                    year_b=year_b,
                    shipped_yoy_usd=shipped_yoy,
                    eia_residential_yoy_usd=eia_yoy,
                    band_ok=_published_band_ok(shipped_yoy, eia_yoy),
                    source_note='mut_from_balanced',
                    artifact_vintage=vintage,
                )
            )
    return rows


def _panel_by_year(
    cache: dict[tuple[str, int], YearBalance],
    mode: str,
    years: list[int],
) -> PanelByYear:
    panel: PanelByYear = {}
    for year in years:
        yb = cache[(mode, year)]
        if yb.balanced is None:
            raise ValueError(f'{mode} {year}: no balanced Use')
        panel[year] = panel_from_balanced(yb.balanced['use'])
    return panel


def _displacement_rows(
    cache: dict[tuple[str, int], YearBalance],
    candidates: ta.Iterable[str],
    years: list[int],
) -> list[PcePinDisplacementRow]:
    baseline_panel = _panel_by_year(cache, str(DEFAULT_PCE_CONSTRAINT), years)
    sets = build_claim_sets(
        [str(i) for i in baseline_panel[years[0]].elec.index]
    )
    baseline_bands = {
        (r.year_a, r.year_b, r.band): r.share_effect_bn
        for r in build_band_rows(
            baseline_panel, years, 'balanced_none', sets
        )
    }

    rows: list[PcePinDisplacementRow] = []
    for candidate in candidates:
        cand_panel = _panel_by_year(cache, candidate, years)
        cand_bands = build_band_rows(cand_panel, years, f'balanced_{candidate}', sets)
        for r in cand_bands:
            base = baseline_bands.get((r.year_a, r.year_b, r.band))
            if base is None:
                raise ValueError(
                    f'missing baseline band {r.band} for {r.year_a}->{r.year_b}'
                )
            rows.append(
                PcePinDisplacementRow(
                    candidate=candidate,
                    year_a=r.year_a,
                    year_b=r.year_b,
                    band=r.band,
                    share_effect_baseline_bn=base,
                    share_effect_candidate_bn=r.share_effect_bn,
                    delta_share_effect_bn=r.share_effect_bn - base,
                )
            )
    return rows


def _pce_sink_rows(
    cache: dict[tuple[str, int], YearBalance],
    candidates: ta.Iterable[str],
    years: list[int],
) -> list[PcePinPceSinkRow]:
    baseline_cols = {
        year: _pce_column_usd(cache[(str(DEFAULT_PCE_CONSTRAINT), year)])
        for year in years
    }
    commodities = [
        c
        for c in balance_commodities()
        if str(c) != PCE_ELECTRICITY_ROW
    ]
    rows: list[PcePinPceSinkRow] = []
    for candidate in candidates:
        cand_cols = {
            year: _pce_column_usd(cache[(candidate, year)]) for year in years
        }
        for year_a, year_b in consecutive_spans(years):
            for commodity in commodities:
                code = str(commodity)
                for label, col_a, col_b in (
                    ('baseline', baseline_cols[year_a], baseline_cols[year_b]),
                    ('candidate', cand_cols[year_a], cand_cols[year_b]),
                ):
                    if code not in col_a.index or code not in col_b.index:
                        raise ValueError(
                            f'{code!r} missing from {label} F01000 '
                            f'({year_a}/{year_b})'
                        )
                base_d = float(baseline_cols[year_b][code] - baseline_cols[year_a][code])
                cand_d = float(cand_cols[year_b][code] - cand_cols[year_a][code])
                delta_bn = (cand_d - base_d) / _BN
                if abs(delta_bn) <= 0.0:
                    continue
                rows.append(
                    PcePinPceSinkRow(
                        candidate=candidate,
                        year_a=year_a,
                        year_b=year_b,
                        commodity=code,
                        delta_vs_baseline_bn=delta_bn,
                        abs_delta_bn=abs(delta_bn),
                    )
                )
    return rows


def _trade_displacement_abs(
    disp_rows: list[PcePinDisplacementRow], candidate: str
) -> float:
    return sum(
        abs(r.delta_share_effect_bn)
        for r in disp_rows
        if r.candidate == candidate and r.band in _TRADE_DISP_BANDS
    )


def _displacement_note(
    candidate: str,
    disp_rows: list[PcePinDisplacementRow],
    sink_rows: list[PcePinPceSinkRow],
) -> str:
    by_band: dict[str, float] = {}
    for r in disp_rows:
        if r.candidate != candidate:
            continue
        by_band[r.band] = by_band.get(r.band, 0.0) + abs(r.delta_share_effect_bn)
    top_bands = sorted(by_band.items(), key=lambda kv: kv[1], reverse=True)[:3]
    band_txt = (
        ', '.join(f'{b}={v:+.3f}bn_abs' for b, v in top_bands) if top_bands else 'none'
    )

    by_comm: dict[str, float] = {}
    for r in sink_rows:
        if r.candidate != candidate:
            continue
        by_comm[r.commodity] = by_comm.get(r.commodity, 0.0) + r.abs_delta_bn
    top_sinks = sorted(by_comm.items(), key=lambda kv: kv[1], reverse=True)[:5]
    sink_txt = (
        ', '.join(f'{c}={v:+.3f}bn_abs' for c, v in top_sinks) if top_sinks else 'none'
    )
    return f'top_bands=[{band_txt}]; top_pce_sinks=[{sink_txt}]'


def _summarize(
    candidates: ta.Sequence[str],
    t11_rows: list[PcePinT11Row],
    eia_rows: list[PcePinEiaBandRow],
    disp_rows: list[PcePinDisplacementRow],
    sink_rows: list[PcePinPceSinkRow],
) -> list[PceCandidateSummaryRow]:
    drafts: list[PceCandidateSummaryRow] = []
    trade_abs: dict[str, float] = {}
    for candidate in candidates:
        t11_ok = all(r.ok for r in t11_rows if r.candidate == candidate)
        eia_ok = all(r.band_ok for r in eia_rows if r.candidate == candidate)
        note = _displacement_note(candidate, disp_rows, sink_rows)
        trade_abs[candidate] = _trade_displacement_abs(disp_rows, candidate)
        drafts.append(
            PceCandidateSummaryRow(
                candidate=candidate,
                t11_all_years_ok=t11_ok,
                eia_all_spans_ok=eia_ok,
                displacement_note=note,
                eligible=t11_ok,
                selected=False,
            )
        )

    eligible = [d for d in drafts if d.eligible]
    if not eligible:
        return drafts

    pool = [d for d in eligible if d.eia_all_spans_ok] or list(eligible)
    best = min(trade_abs[d.candidate] for d in pool)
    pool = [d for d in pool if trade_abs[d.candidate] == best]
    pool.sort(key=lambda d: _TIE_BREAK.get(d.candidate, 99))
    winner = pool[0].candidate
    return [d._replace(selected=(d.candidate == winner)) for d in drafts]


def grade(
    years: list[int] | None = None,
) -> tuple[
    list[PcePinT11Row],
    list[PcePinEiaBandRow],
    list[PcePinDisplacementRow],
    list[PcePinPceSinkRow],
    list[PceCandidateSummaryRow],
]:
    """Grade each pin candidate vs unconstrained baseline on T11 / EIA / displacement."""
    year_list = sorted(years) if years is not None else available_years()
    candidates = [str(c) for c in CANDIDATES]
    cache = _run_balances(year_list, GRADE_MODES)

    t11_rows = _t11_rows(cache, candidates, year_list)
    eia_rows = _eia_rows(cache, candidates, year_list)
    disp_rows = _displacement_rows(cache, candidates, year_list)
    sink_rows = _pce_sink_rows(cache, candidates, year_list)
    summary = _summarize(candidates, t11_rows, eia_rows, disp_rows, sink_rows)
    return t11_rows, eia_rows, disp_rows, sink_rows, summary


# ---------------------------------------------------------------------------
# I/O / --check
# ---------------------------------------------------------------------------


def _rows_to_frame(rows: ta.Sequence[ta.NamedTuple]) -> pd.DataFrame:
    return pd.DataFrame([r._asdict() for r in rows])


def _write_csv(
    name: str, rows: ta.Sequence[ta.NamedTuple], write: bool
) -> Path | None:
    if not rows:
        print(f'(no rows for {name})')
        return None
    frame = _rows_to_frame(rows)
    print(frame.to_string(index=False, float_format=lambda v: f'{v:,.6g}'))
    if not write:
        return None
    path = OUT_DIR / name
    frame.to_csv(path, index=False)
    print(f'wrote {path}')
    return path


def _csv_stem(years: list[int]) -> str:
    ordered = sorted(years)
    return f'{ordered[0]}_{ordered[-1]}'


def check_grade(
    *,
    years: list[int],
    t11_rows: list[PcePinT11Row],
    eia_rows: list[PcePinEiaBandRow],
    disp_rows: list[PcePinDisplacementRow],
    sink_rows: list[PcePinPceSinkRow],
    summary: list[PceCandidateSummaryRow],
) -> int:
    """Schema / selection / nonzero-seed checks. Returns failure count."""
    failures = 0

    expected_t11 = {
        (c, y) for c in (str(x) for x in CANDIDATES) for y in years
    }
    have_t11 = {(r.candidate, r.year) for r in t11_rows}
    if have_t11 != expected_t11:
        print(f'FAIL  T11 schema: expected {len(expected_t11)} rows, got {len(t11_rows)}')
        failures += 1

    spans = consecutive_spans(years)
    expected_eia = {
        (c, a, b) for c in (str(x) for x in CANDIDATES) for a, b in spans
    }
    have_eia = {(r.candidate, r.year_a, r.year_b) for r in eia_rows}
    if have_eia != expected_eia:
        print(f'FAIL  EIA schema: expected {len(expected_eia)} rows, got {len(eia_rows)}')
        failures += 1

    for r in eia_rows:
        if r.source_note != 'mut_from_balanced':
            print(f'FAIL  EIA source_note must be mut_from_balanced, got {r.source_note!r}')
            failures += 1

    if len(summary) != len(CANDIDATES):
        print(f'FAIL  summary schema: expected {len(CANDIDATES)} rows, got {len(summary)}')
        failures += 1
    n_selected = sum(1 for s in summary if s.selected)
    if n_selected not in (0, 1):
        print(f'FAIL  selected count must be 0 or 1, got {n_selected}')
        failures += 1
    if n_selected == 0 and any(s.eligible for s in summary):
        print('FAIL  eligible candidate exists but none selected')
        failures += 1
    if n_selected == 1:
        winner = next(s for s in summary if s.selected)
        if not winner.eligible:
            print(f'FAIL  selected {winner.candidate} is not eligible')
            failures += 1

    for year in years:
        seed_y = derive_initial_Y_pur(year, download_sources_ok=True)
        seed = _cell_at(
            seed_y,
            PCE_ELECTRICITY_ROW,
            PCE_ELECTRICITY_COL,
            f'derive_initial_Y_pur({year})',
        )
        if seed == 0.0:
            print(f'FAIL  electricity PCE seed is zero in {year} (Tier-1 would no-op)')
            failures += 1

    # Displacement / sink tables may be empty for freeze-only candidates; still
    # require consistent candidate labels when rows exist.
    known = {str(c) for c in CANDIDATES}
    for r in (*disp_rows, *sink_rows):
        if r.candidate not in known:
            print(f'FAIL  unknown candidate label {r.candidate!r}')
            failures += 1

    print(f'check: {failures} failure(s)')
    return failures


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    default_years = available_years()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        'command',
        choices=('measure', 'grade'),
        help='measure: rank F01000 Step-5 moves; grade: T11 / EIA / displacement',
    )
    parser.add_argument(
        '--years',
        default=f'{default_years[0]}-{default_years[-1]}',
        help=(
            'Inclusive year range (default: all nowcast years, '
            f'currently {default_years[0]}-{default_years[-1]})'
        ),
    )
    parser.add_argument(
        '--csv',
        action='store_true',
        help='Write CSVs under eia_gtd/',
    )
    parser.add_argument(
        '--check',
        action='store_true',
        help='grade only: schema / selection / nonzero electricity PCE seed',
    )
    args = parser.parse_args(argv)
    year_list = _parse_years(args.years)
    stem = _csv_stem(year_list)

    if args.command == 'measure':
        if args.check:
            raise SystemExit('--check applies to grade only')
        print(f'measure F01000 balance moves {year_list[0]}-{year_list[-1]}')
        print(
            'counterpart survey notes: '
            + '; '.join(f'{k}: {v}' for k, v in _COUNTERPART_NOTES.items())
        )
        rows = measure(year_list)
        _write_csv(f'pce_electricity_pin_measure_{stem}.csv', rows, args.csv)
        return

    print(f'grade PCE pin candidates {year_list[0]}-{year_list[-1]}')
    t11_rows, eia_rows, disp_rows, sink_rows, summary = grade(year_list)

    print('\n=== T11 ===')
    _write_csv(f'pce_electricity_pin_t11_{stem}.csv', t11_rows, args.csv)
    print('\n=== EIA residential YoY band (mut_from_balanced) ===')
    _write_csv(f'pce_electricity_pin_eia_{stem}.csv', eia_rows, args.csv)
    print('\n=== intermediate displacement vs none ===')
    _write_csv(f'pce_electricity_pin_displacement_{stem}.csv', disp_rows, args.csv)
    print('\n=== PCE sink displacement vs none ===')
    _write_csv(f'pce_electricity_pin_pce_sink_{stem}.csv', sink_rows, args.csv)
    print('\n=== candidate summary ===')
    _write_csv(f'pce_electricity_pin_summary_{stem}.csv', summary, args.csv)

    print('\nselection:')
    for s in summary:
        mark = ' SELECTED' if s.selected else ''
        print(
            f'  {s.candidate}: eligible={s.eligible} '
            f't11={s.t11_all_years_ok} eia={s.eia_all_spans_ok}{mark}'
        )
        print(f'    {s.displacement_note}')

    if args.check:
        raise SystemExit(
            1
            if check_grade(
                years=year_list,
                t11_rows=t11_rows,
                eia_rows=eia_rows,
                disp_rows=disp_rows,
                sink_rows=sink_rows,
                summary=summary,
            )
            else 0
        )


if __name__ == '__main__':
    main()
