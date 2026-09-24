"""Issue #990 / Phase 2T — attribute the electricity intermediate-row target.

Standalone reportable checker for ``T016 − ΣY`` on commodity ``221100``.
Do **not** add a standing mode to ``electricity_row_896``. Not a CI gate —
thresholds are fitted to the #990 question. Keep this module so marginal
Attributed verdicts stay re-runnable when new years land.

::

    python -m bedrock.analysis.electricity.current.eia_gtd.target_attribution_221100 \\
        [--years 2017-2024] --csv [--check]

Default ``--years`` is all nowcast years currently available (see
``NOWCAST_YEARS``); every consecutive YoY pair in that range is reported.
Live extracts only (``download_sources_ok=True``). No MUT pin. Missing / NaN
``T016`` or ``F01000`` raises — never silently filled.
"""

from __future__ import annotations

import argparse
import logging
import math
import typing as ta
from datetime import date
from pathlib import Path

import pandas as pd

from bedrock.analysis.electricity.current.eia_gtd.electricity_row_control import (
    ELECTRICITY_ROW,
    OUT_DIR,
    PCE_CODE,
    eia_epa_table_2_3_revenue_bn,
)
from bedrock.analysis.nowcasting.results._ef_smoke_lib import NOWCAST_YEARS

logger = logging.getLogger(__name__)

#: #990 focus spans (labeling / close-out); CLI default runs all consecutive pairs.
FOCUS_SPANS: tuple[tuple[int, int], ...] = ((2022, 2023), (2023, 2024))
COMPONENTS: tuple[str, ...] = (
    'T016',
    'Y_PCE',
    'Y_other',
    'interior_row_target',
    'residual_unexplained',
)
SOURCE_NOTES: dict[str, str] = {
    'T016': 'derive_initial_supply_bridge.T016',
    'Y_PCE': 'derive_initial_Y_pur.F01000',
    'Y_other': 'derive_initial_Y_pur.ΣY−F01000',
    'interior_row_target': 'interior_row_targets',
    'residual_unexplained': 'residual',
}

_ATOL_USD = 5e7  # $0.05bn
_FRAC_DENOM_USD = 5e9  # $5bn
_DOM_FRAC_MIN = 0.50
_PUB_ABS_USD = 5e9  # $5bn absolute published band
_PUB_RTOL = 0.15
_BN_TO_USD = 1e9
_BEA_M_TO_USD = 1e6


def available_years() -> list[int]:
    """Years with nowcast configs today; grows when ``NOWCAST_YEARS`` gains 2025+."""
    return list(NOWCAST_YEARS)


def consecutive_spans(years: list[int]) -> list[tuple[int, int]]:
    """Every consecutive YoY pair in ``years`` (sorted ascending)."""
    ordered = sorted(years)
    if len(ordered) < 2:
        raise ValueError(f'need at least two years for YoY spans, got {ordered}')
    return list(zip(ordered[:-1], ordered[1:]))


def _parse_years(spec: str) -> list[int]:
    lo, _, hi = spec.partition('-')
    return list(range(int(lo), int(hi or lo) + 1))


def csv_name_for_years(years: list[int]) -> str:
    ordered = sorted(years)
    return f'target_attribution_221100_{ordered[0]}_{ordered[-1]}.csv'


class TargetAttributionRow(ta.NamedTuple):
    year_a: int
    year_b: int
    component: str
    delta_usd: float
    fraction_of_delta_target: float | None
    source_note: str


class YearLevels(ta.NamedTuple):
    """Per-year levels in USD for commodity 221100."""

    year: int
    t016_usd: float
    y_pce_usd: float
    y_other_usd: float
    sigma_y_usd: float
    interior_usd: float
    bea_go_usd: float
    eia_residential_usd: float


class SpanDecision(ta.NamedTuple):
    year_a: int
    year_b: int
    outcome: str  # 'Attributed' | 'Fix'
    dominant: str | None
    dominant_fraction: float | None
    reason: str


# ---------------------------------------------------------------------------
# Levels
# ---------------------------------------------------------------------------


def load_year_levels(year: int) -> YearLevels:
    """Load live Supply / FD / published counterparts for one year (USD)."""
    from bedrock.transform.iot.derived_intermediate_and_value_added import (  # noqa: PLC0415
        detail_gross_output_panel,
    )
    from bedrock.transform.iot.nowcast import (  # noqa: PLC0415
        derive_initial_supply_bridge,
        derive_initial_Y_pur,
    )
    from bedrock.transform.iot.nowcast_interior_fit import (  # noqa: PLC0415
        interior_row_targets,
    )

    bridge = derive_initial_supply_bridge(year, download_sources_ok=True)
    y = derive_initial_Y_pur(year, download_sources_ok=True)

    if ELECTRICITY_ROW not in bridge.index:
        raise ValueError(f'{ELECTRICITY_ROW} missing from supply bridge {year}')
    t016_raw = bridge.loc[ELECTRICITY_ROW, 'T016']
    if isinstance(t016_raw, pd.Series):
        raise ValueError(f'T016 duplicated for {ELECTRICITY_ROW} in {year}')
    t016 = float(pd.to_numeric(t016_raw, errors='coerce'))
    if math.isnan(t016):
        raise ValueError(
            f'T016 is NaN for {ELECTRICITY_ROW} in {year} '
            '(unsourced bridge — not filled)'
        )

    if ELECTRICITY_ROW not in y.index:
        raise ValueError(f'{ELECTRICITY_ROW} missing from Y in {year}')
    y_row = y.loc[ELECTRICITY_ROW]
    if isinstance(y_row, pd.DataFrame):
        raise ValueError(f'{ELECTRICITY_ROW} duplicated in Y {year}')
    if PCE_CODE not in y_row.index:
        raise ValueError(f'{PCE_CODE} missing from Y for {ELECTRICITY_ROW} in {year}')
    y_pce = float(pd.to_numeric(y_row[PCE_CODE], errors='coerce'))
    if math.isnan(y_pce):
        raise ValueError(f'{PCE_CODE} is NaN for {ELECTRICITY_ROW} in {year}')

    # Mirror interior_row_targets: ΣY = row sum with fillna(0) on FD cells.
    sigma_y = float(y_row.astype(float).fillna(0.0).sum())
    y_other = sigma_y - y_pce

    interior = float(interior_row_targets(year).loc[ELECTRICITY_ROW])
    if math.isnan(interior):
        raise ValueError(f'interior_row_targets NaN for {ELECTRICITY_ROW} in {year}')

    bea_cell = detail_gross_output_panel(ec_adjusted=False).loc[ELECTRICITY_ROW, year]
    bea_go_m = float(pd.to_numeric(bea_cell, errors='coerce'))
    if math.isnan(bea_go_m):
        raise ValueError(f'BEA UGO305-A missing for {ELECTRICITY_ROW} in {year}')
    bea_go_usd = bea_go_m * _BEA_M_TO_USD

    eia_res_usd = float(eia_epa_table_2_3_revenue_bn(year)[0]) * _BN_TO_USD

    return YearLevels(
        year=year,
        t016_usd=t016,
        y_pce_usd=y_pce,
        y_other_usd=y_other,
        sigma_y_usd=sigma_y,
        interior_usd=interior,
        bea_go_usd=bea_go_usd,
        eia_residential_usd=eia_res_usd,
    )


# ---------------------------------------------------------------------------
# Span rows + identity
# ---------------------------------------------------------------------------


def _fraction(delta: float, interior_delta: float) -> float | None:
    if abs(interior_delta) < _FRAC_DENOM_USD:
        return None
    return delta / interior_delta


def build_span_rows(ya: YearLevels, yb: YearLevels) -> list[TargetAttributionRow]:
    """Signed component rows for one crisis span (exactly one per component)."""
    d_t016 = yb.t016_usd - ya.t016_usd
    d_y_pce = yb.y_pce_usd - ya.y_pce_usd
    d_y_other = yb.y_other_usd - ya.y_other_usd
    d_interior = yb.interior_usd - ya.interior_usd

    # Target = T016 − ΣY → contributions: +ΔT016, −ΔY_PCE, −ΔY_other
    c_t016 = d_t016
    c_y_pce = -d_y_pce
    c_y_other = -d_y_other
    residual = d_interior - (c_t016 + c_y_pce + c_y_other)

    parts: list[tuple[str, float]] = [
        ('T016', c_t016),
        ('Y_PCE', c_y_pce),
        ('Y_other', c_y_other),
        ('interior_row_target', d_interior),
        ('residual_unexplained', residual),
    ]
    return [
        TargetAttributionRow(
            year_a=ya.year,
            year_b=yb.year,
            component=name,
            delta_usd=delta,
            fraction_of_delta_target=_fraction(delta, d_interior),
            source_note=SOURCE_NOTES[name],
        )
        for name, delta in parts
    ]


def check_span_identity(rows: list[TargetAttributionRow]) -> list[str]:
    """Return human-readable failures; empty list means pass.

    Asserts parts identity and residual atol only — does **not** sum all five
    component rows.
    """
    by_comp = {r.component: r for r in rows}
    missing = [c for c in COMPONENTS if c not in by_comp]
    if missing:
        return [f'missing components: {missing}']

    interior = by_comp['interior_row_target'].delta_usd
    parts_sum = (
        by_comp['T016'].delta_usd
        + by_comp['Y_PCE'].delta_usd
        + by_comp['Y_other'].delta_usd
    )
    residual = by_comp['residual_unexplained'].delta_usd
    failures: list[str] = []

    if abs(parts_sum - interior) > _ATOL_USD:
        failures.append(
            f'parts identity failed: T016+Y_PCE+Y_other={parts_sum:.3e} '
            f'vs interior={interior:.3e}'
        )
    if abs(residual) > _ATOL_USD:
        failures.append(f'|residual_unexplained|={abs(residual):.3e} > {_ATOL_USD:.3e}')
    if abs(residual - (interior - parts_sum)) > _ATOL_USD:
        failures.append('residual field inconsistent with interior − parts')

    if abs(interior) < _FRAC_DENOM_USD:
        for r in rows:
            if r.fraction_of_delta_target is not None:
                failures.append(
                    f'fraction must be None when |interior| < $5bn '
                    f'(got {r.component}={r.fraction_of_delta_target})'
                )
    return failures


# ---------------------------------------------------------------------------
# Fix vs Attributed
# ---------------------------------------------------------------------------


def _published_band_ok(bedrock_delta: float, published_delta: float) -> bool:
    """Same-sign published YoY band: abs ≤ $5bn or relative ≤ 15% when |bedrock|≥$5bn."""
    if bedrock_delta == 0.0 and published_delta == 0.0:
        return True
    if bedrock_delta * published_delta < 0:
        return False
    if abs(published_delta - bedrock_delta) <= _PUB_ABS_USD:
        return True
    if abs(bedrock_delta) >= _FRAC_DENOM_USD:
        return abs(published_delta / bedrock_delta - 1.0) <= _PUB_RTOL
    return False


def decide_span(
    rows: list[TargetAttributionRow],
    ya: YearLevels,
    yb: YearLevels,
    *,
    derivation_defect: str | None = None,
) -> SpanDecision:
    """Locked Fix vs Attributed rule for one crisis span."""
    by_comp = {r.component: r for r in rows}
    a, b = ya.year, yb.year

    if derivation_defect:
        return SpanDecision(
            year_a=a,
            year_b=b,
            outcome='Fix',
            dominant=None,
            dominant_fraction=None,
            reason=f'named derivation defect: {derivation_defect}',
        )

    candidates = ('T016', 'Y_PCE', 'Y_other')
    fracs = {
        c: by_comp[c].fraction_of_delta_target
        for c in candidates
        if by_comp[c].fraction_of_delta_target is not None
    }
    if not fracs:
        return SpanDecision(
            year_a=a,
            year_b=b,
            outcome='Fix',
            dominant=None,
            dominant_fraction=None,
            reason='all component fractions None (|interior Δ| < $5bn)',
        )

    dom = max(fracs, key=lambda c: abs(fracs[c] or 0.0))
    dom_frac = fracs[dom]
    assert dom_frac is not None

    if abs(dom_frac) < _DOM_FRAC_MIN:
        return SpanDecision(
            year_a=a,
            year_b=b,
            outcome='Fix',
            dominant=dom,
            dominant_fraction=dom_frac,
            reason=(
                f'|fraction({dom})|={abs(dom_frac):.3f} < {_DOM_FRAC_MIN} '
                '(no clear majority)'
            ),
        )

    if dom == 'Y_other':
        return SpanDecision(
            year_a=a,
            year_b=b,
            outcome='Fix',
            dominant=dom,
            dominant_fraction=dom_frac,
            reason=(
                'Y_other is dominant but has no single published BEA FD residual '
                'series — cannot Attribute without a named series'
            ),
        )

    # Underlying series Δ (not contribution sign) for published band.
    if dom == 'T016':
        bedrock_d = yb.t016_usd - ya.t016_usd
        published_d = yb.bea_go_usd - ya.bea_go_usd
        cite = 'BEA UGO305-A'
    else:  # Y_PCE
        bedrock_d = yb.y_pce_usd - ya.y_pce_usd
        published_d = yb.eia_residential_usd - ya.eia_residential_usd
        cite = 'EIA EPA Table 2.3 residential revenue'

    if not _published_band_ok(bedrock_d, published_d):
        return SpanDecision(
            year_a=a,
            year_b=b,
            outcome='Fix',
            dominant=dom,
            dominant_fraction=dom_frac,
            reason=(
                f'published YoY band failed for {dom} vs {cite}: '
                f'bedrock_d=${bedrock_d / 1e9:.2f}bn, '
                f'published_d=${published_d / 1e9:.2f}bn'
            ),
        )

    return SpanDecision(
        year_a=a,
        year_b=b,
        outcome='Attributed',
        dominant=dom,
        dominant_fraction=dom_frac,
        reason=(
            f'{dom} majority (|fraction|={abs(dom_frac):.3f}); '
            f'published YoY tracks {cite} '
            f'(bedrock_d=${bedrock_d / 1e9:.2f}bn, '
            f'published_d=${published_d / 1e9:.2f}bn)'
        ),
    )


def overall_outcome(
    decisions: list[SpanDecision],
    *,
    spans: ta.Iterable[tuple[int, int]] | None = None,
) -> str:
    """Attributed only if every selected span is Attributed.

    Default: all decisions in the run. Pass ``spans=FOCUS_SPANS`` for the #990
    close-out subset.
    """
    if spans is not None:
        want = set(spans)
        decisions = [d for d in decisions if (d.year_a, d.year_b) in want]
    if not decisions:
        return 'Fix'
    if all(d.outcome == 'Attributed' for d in decisions):
        return 'Attributed'
    return 'Fix'


# ---------------------------------------------------------------------------
# I/O / CLI
# ---------------------------------------------------------------------------


def write_csv(
    rows: list[TargetAttributionRow],
    path: Path | None = None,
    *,
    years: list[int] | None = None,
) -> Path:
    year_list = years or sorted({r.year_a for r in rows} | {r.year_b for r in rows})
    out = path or (OUT_DIR / csv_name_for_years(year_list))
    header = (
        f'# target_attribution_221100 run_date={date.today().isoformat()} '
        f'years={year_list[0]}-{year_list[-1]} '
        'live_extract=True interior_row_targets=download_sources_ok '
        '(not a pinned MUT / not frozen gate CSV)\n'
    )
    frame = pd.DataFrame([r._asdict() for r in rows])
    with out.open('w', encoding='utf-8', newline='') as fh:
        fh.write(header)
        frame.to_csv(fh, index=False)
    logger.info('Wrote %s', out)
    return out


def run(
    *,
    years: list[int] | None = None,
    write: bool = False,
    check: bool = False,
    derivation_defect: str | None = None,
) -> tuple[list[TargetAttributionRow], list[SpanDecision], list[YearLevels]]:
    year_list = sorted(years) if years is not None else available_years()
    spans = consecutive_spans(year_list)
    levels = {y: load_year_levels(y) for y in year_list}

    all_rows: list[TargetAttributionRow] = []
    decisions: list[SpanDecision] = []
    check_failures: list[str] = []

    for a, b in spans:
        rows = build_span_rows(levels[a], levels[b])
        all_rows.extend(rows)
        fails = check_span_identity(rows)
        if fails:
            check_failures.extend(f'{a}->{b}: {f}' for f in fails)
        decisions.append(
            decide_span(rows, levels[a], levels[b], derivation_defect=derivation_defect)
        )

    if check and check_failures:
        for msg in check_failures:
            print(f'FAIL  {msg}')
        raise SystemExit(1)

    if write:
        write_csv(all_rows, years=year_list)

    return all_rows, decisions, [levels[y] for y in year_list]


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    default_years = available_years()
    parser = argparse.ArgumentParser(
        description=(
            'Phase 2T / #990: attribute electricity interior target '
            '(all consecutive YoY pairs by default)'
        )
    )
    parser.add_argument(
        '--years',
        default=f'{default_years[0]}-{default_years[-1]}',
        help=(
            'Inclusive year range (default: all available nowcast years, '
            f'currently {default_years[0]}-{default_years[-1]})'
        ),
    )
    parser.add_argument(
        '--csv',
        action='store_true',
        help='Write target_attribution_221100_<lo>_<hi>.csv under eia_gtd/',
    )
    parser.add_argument(
        '--check',
        action='store_true',
        help='Fail on parts-identity / residual / fraction rules',
    )
    parser.add_argument(
        '--derivation-defect',
        default=None,
        help='Optional named defect (module+symptom) forcing Fix on all spans',
    )
    args = parser.parse_args(argv)
    year_list = _parse_years(args.years)

    rows, decisions, levels = run(
        years=year_list,
        write=args.csv,
        check=args.check,
        derivation_defect=args.derivation_defect,
    )

    print('Year levels ($bn):')
    for lv in levels:
        print(
            f'  {lv.year}: T016={lv.t016_usd / 1e9:.2f}  '
            f'Y_PCE={lv.y_pce_usd / 1e9:.2f}  '
            f'Y_other={lv.y_other_usd / 1e9:.2f}  '
            f'interior={lv.interior_usd / 1e9:.2f}  '
            f'BEA_GO={lv.bea_go_usd / 1e9:.2f}  '
            f'EIA_res={lv.eia_residential_usd / 1e9:.2f}'
        )

    print('\nComponent delta ($bn) / fraction:')
    for r in rows:
        frac = (
            f'{r.fraction_of_delta_target:+.3f}'
            if r.fraction_of_delta_target is not None
            else 'None'
        )
        print(
            f'  {r.year_a}->{r.year_b}  {r.component:22s}  '
            f'{r.delta_usd / 1e9:+8.2f}  frac={frac}'
        )

    print('\nSpan decisions:')
    for d in decisions:
        mark = ' *#990-focus*' if (d.year_a, d.year_b) in FOCUS_SPANS else ''
        print(
            f'  {d.year_a}->{d.year_b}: {d.outcome}  '
            f'dom={d.dominant}  |{d.reason}{mark}'
        )
    print(f'\nOverall (all spans in run): {overall_outcome(decisions)}')
    print(
        f'#990 focus {FOCUS_SPANS[0][0]}-{FOCUS_SPANS[-1][-1]}: '
        f'{overall_outcome(decisions, spans=FOCUS_SPANS)}'
    )
    if args.check:
        print('PASS  identity checks')


if __name__ == '__main__':
    main()
