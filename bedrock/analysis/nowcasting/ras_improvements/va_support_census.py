"""#808 VA-dominated Use column support / residual census.

Assemble once per year, soft engine only (full split→offset→engine→restore).
Does **not** call ``balance_year``. Soft≈hard on these residuals — no dual
protocol.

Example::

    uv run python -m bedrock.analysis.nowcasting.ras_improvements.va_support_census \\
        --years 2018,2021,2022,2023 --force
"""

from __future__ import annotations

import argparse
import json
import traceback
from pathlib import Path
from typing import Any

import pandas as pd

from bedrock.transform.iot.nowcast_mask import (
    VA_OPEN_SUPPORT_INDUSTRIES,
    VA_ROWS,
    balance_commodities,
)
from bedrock.transform.iot.nowcast_sut_assembly import (
    assemble,
    assert_post_balance_hygiene,
)
from bedrock.transform.iot.nowcast_sut_gras import engine
from bedrock.utils.economic.balance.mask import SutMask
from bedrock.utils.economic.balance.offset import (
    offset_targets,
    restore_fixed_blocks,
    split_fixed_blocks,
)
from bedrock.utils.economic.balance.targets import Target, TargetSet

PACKAGE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = PACKAGE_DIR.parent / 'output' / 'va_support'

NAMED = frozenset({'531HSO', '531ORE', 'GSLGE', 'GSLGO', 'GSLGH', 'S00600', '814000'})
assert VA_OPEN_SUPPORT_INDUSTRIES == NAMED - {'814000'}
HOUSEHOLDS = '814000'
V00100 = 'V00100'
V00300 = 'V00300'

INDUSTRY_CSV_COLUMNS = (
    'year',
    'industry',
    't1_abs',
    't17_abs',
    't18_abs',
    't1_minus_t18_usd_m',
    'free_v00300',
    'free_v00100',
    'n_free_signflex_non_va',
    'abs_sum_seed_offsets',
    't18_skip_gate',
    'hygiene_ok',
    'hygiene_error',
)

SUMMARY_CSV_COLUMNS = (
    'year',
    'named_t1_total_abs',
    'named_t17_total_abs',
    'named_t18_total_abs',
    'b2_candidate_cells',
    'b2_cells_opened',
    'b1_gate',
    'hygiene_ok',
    'error',
)

_DEFAULT_YEARS = '2018,2021,2022,2023'
_B1_ABS_TOL = 50.0


def resolve_output_dir(*, create: bool = False) -> Path:
    if create:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return OUTPUT_DIR


def _target_by_name(targets: TargetSet, name: str) -> Target:
    for target in targets:
        if target.name == name:
            return target
    raise KeyError(f'target {name!r} not in TargetSet')


def t18_skip_gate(
    *,
    free_v00300: bool,
    abs_sum_seed_offsets: float,
) -> str:
    """Generic-path closer label (not B1 special-case)."""
    if not free_v00300:
        return 'v00300_frozen'
    if abs_sum_seed_offsets == 0.0:
        return 'abs_sum_zero'
    return 'none'


def classify_b1_gate(
    *,
    free_v00100: bool | None,
    t1_minus_t18_usd_m: float | None,
    t18_abs: float | None,
    year_error: bool = False,
) -> str:
    """Baseline ``b1_gate`` for ``814000``. First match wins (plan A4)."""
    if (
        year_error
        or free_v00100 is None
        or t1_minus_t18_usd_m is None
        or t18_abs is None
    ):
        return 'needs_decision'
    if not free_v00100:
        return 'needs_decision'
    if t18_abs <= _B1_ABS_TOL:
        return 'skip_immaterial'
    gap = abs(float(t1_minus_t18_usd_m))
    if gap <= max(_B1_ABS_TOL, 0.05 * float(t18_abs)) and t18_abs > _B1_ABS_TOL:
        return 'allow_b1'
    return 'needs_decision'


def _support_counts(
    seed_use: pd.DataFrame,
    mask: SutMask,
    industry: str,
) -> tuple[bool, bool, int, float]:
    free = mask.free
    sign_flex = mask.sign_lock.eq(0)
    free_v00300 = bool(free.loc[V00300, industry]) if V00300 in free.index else False
    free_v00100 = bool(free.loc[V00100, industry]) if V00100 in free.index else False
    va = set(VA_ROWS)
    n_free = 0
    abs_sum = 0.0
    for row in seed_use.index:
        row_s = str(row)
        if row_s in va:
            continue
        if bool(free.loc[row_s, industry]) and bool(sign_flex.loc[row_s, industry]):
            n_free += 1
            abs_sum += abs(float(seed_use.loc[row_s, industry]))  # type: ignore[arg-type]
    return free_v00300, free_v00100, n_free, abs_sum


def b2_candidate_cells(mask: SutMask) -> int:
    """Count of ``structural_zero`` in commodity × VA_OPEN_SUPPORT_INDUSTRIES."""
    commodities = [c for c in balance_commodities() if c in mask.structural_zero.index]
    industries = [
        j
        for j in sorted(VA_OPEN_SUPPORT_INDUSTRIES)
        if j in mask.structural_zero.columns
    ]
    if not commodities or not industries:
        return 0
    block = mask.structural_zero.loc[commodities, industries]
    return int(block.to_numpy().sum())


def measure_b2_cells_opened(use_mask: SutMask) -> tuple[int, int]:
    """Return ``(b2_candidate_cells, b2_cells_opened)`` vs production Use mask.

    Candidates are counted on the **pre-B2** pattern structural layer (so the
    metric stays defined after ``build_sut_mask`` clears). Opened = candidates
    that are no longer structural zero on the production mask. If the clear
    helper is absent, opened is 0.
    """
    import bedrock.transform.iot.nowcast_mask as nowcast_mask  # noqa: PLC0415

    pre = nowcast_mask.structural_zero_mask('use') & ~nowcast_mask.fixed_value_mask(
        'use'
    )
    commodities = [c for c in balance_commodities() if c in pre.index]
    industries = [j for j in sorted(VA_OPEN_SUPPORT_INDUSTRIES) if j in pre.columns]
    if not commodities or not industries:
        return 0, 0
    pre_block = pre.loc[commodities, industries]
    candidates = int(pre_block.to_numpy().sum())
    if getattr(nowcast_mask, 'clear_va_open_support_structural_zeros', None) is None:
        return candidates, 0
    prod = use_mask.structural_zero.reindex(
        index=commodities, columns=industries, fill_value=False
    )
    opened = int((pre_block & ~prod).to_numpy().sum())
    return candidates, opened


def industry_support_rows(
    *,
    year: int,
    restored: dict[str, pd.DataFrame],
    seeds: dict[str, pd.DataFrame],
    masks: dict[str, SutMask],
    targets: TargetSet,
    hygiene_ok: bool,
    hygiene_error: str,
) -> list[dict[str, Any]]:
    """One row per NAMED industry."""
    t1 = _target_by_name(targets, 'T1')
    t17 = _target_by_name(targets, 'T17')
    t18 = _target_by_name(targets, 'T18')
    err_t1 = (t1.evaluate(restored) - t1.values).abs()
    err_t17 = (t17.evaluate(restored) - t17.values).abs()
    err_t18 = (t18.evaluate(restored) - t18.values).abs()
    seed_use = seeds['use']
    mask = masks['use']
    rows: list[dict[str, Any]] = []
    for industry in sorted(NAMED):
        if industry not in seed_use.columns:
            continue
        free_v00300, free_v00100, n_free, abs_sum = _support_counts(
            seed_use, mask, industry
        )
        t1_val = float(t1.values.loc[industry]) if industry in t1.values.index else 0.0
        t18_val = (
            float(t18.values.loc[industry]) if industry in t18.values.index else 0.0
        )
        rows.append(
            {
                'year': year,
                'industry': industry,
                't1_abs': (
                    float(err_t1.loc[industry]) if industry in err_t1.index else 0.0
                ),
                't17_abs': (
                    float(err_t17.loc[industry]) if industry in err_t17.index else 0.0
                ),
                't18_abs': (
                    float(err_t18.loc[industry]) if industry in err_t18.index else 0.0
                ),
                't1_minus_t18_usd_m': t1_val - t18_val,
                'free_v00300': free_v00300,
                'free_v00100': free_v00100,
                'n_free_signflex_non_va': n_free,
                'abs_sum_seed_offsets': abs_sum,
                't18_skip_gate': t18_skip_gate(
                    free_v00300=free_v00300, abs_sum_seed_offsets=abs_sum
                ),
                'hygiene_ok': hygiene_ok,
                'hygiene_error': hygiene_error,
            }
        )
    return rows


def run_year(year: int) -> dict[str, Any]:
    """Assemble → split → offset → soft engine → restore → census rows."""
    assembled = assemble(int(year), fitted=True)
    frozen, free = split_fixed_blocks(assembled.seeds, assembled.masks)
    residual = offset_targets(assembled.targets, frozen)
    out = engine(free, residual, assembled.masks, impose_soft=True)
    restored = restore_fixed_blocks(out.blocks, frozen)
    hygiene_ok = True
    hygiene_error = ''
    try:
        assert_post_balance_hygiene(assembled.year, restored, assembled.masks)
    except ValueError as err:
        hygiene_ok = False
        hygiene_error = str(err)

    industries = industry_support_rows(
        year=assembled.year,
        restored=restored,
        seeds=assembled.seeds,
        masks=assembled.masks,
        targets=assembled.targets,
        hygiene_ok=hygiene_ok,
        hygiene_error=hygiene_error,
    )
    b2_cand, b2_opened = measure_b2_cells_opened(assembled.masks['use'])
    hh = next((r for r in industries if r['industry'] == HOUSEHOLDS), None)
    b1_gate = classify_b1_gate(
        free_v00100=bool(hh['free_v00100']) if hh else None,
        t1_minus_t18_usd_m=float(hh['t1_minus_t18_usd_m']) if hh else None,
        t18_abs=float(hh['t18_abs']) if hh else None,
        year_error=False,
    )
    return {
        'year': assembled.year,
        'hygiene_ok': hygiene_ok,
        'hygiene_error': hygiene_error,
        'industries': industries,
        'b2_candidate_cells': b2_cand,
        'b2_cells_opened': b2_opened,
        'b1_gate': b1_gate,
        't11_max_abs_residual': float(out.t11_max_abs_residual),
    }


def _summary_row(result: dict[str, Any]) -> dict[str, Any]:
    if 'error' in result and 'industries' not in result:
        return {
            'year': result['year'],
            'named_t1_total_abs': '',
            'named_t17_total_abs': '',
            'named_t18_total_abs': '',
            'b2_candidate_cells': '',
            'b2_cells_opened': '',
            'b1_gate': 'needs_decision',
            'hygiene_ok': False,
            'error': result['error'],
        }
    industries = result.get('industries') or []
    return {
        'year': result['year'],
        'named_t1_total_abs': sum(float(r['t1_abs']) for r in industries),
        'named_t17_total_abs': sum(float(r['t17_abs']) for r in industries),
        'named_t18_total_abs': sum(float(r['t18_abs']) for r in industries),
        'b2_candidate_cells': result.get('b2_candidate_cells', 0),
        'b2_cells_opened': result.get('b2_cells_opened', 0),
        'b1_gate': result.get('b1_gate', 'needs_decision'),
        'hygiene_ok': bool(result.get('hygiene_ok', False)),
        'error': result.get('error', ''),
    }


def _load_json(path: Path) -> list[dict[str, Any]]:
    if path.exists():
        return json.loads(path.read_text())
    return []


def _write_json(path: Path, results: list[dict[str, Any]]) -> None:
    path.write_text(json.dumps(results, indent=2))


def _write_summary(out_dir: Path, results: list[dict[str, Any]]) -> None:
    rows = [_summary_row(r) for r in results]
    frame = pd.DataFrame(rows, columns=list(SUMMARY_CSV_COLUMNS))
    frame.to_csv(out_dir / 'va_support_summary.csv', index=False)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--years', default=_DEFAULT_YEARS)
    parser.add_argument(
        '--force',
        action='store_true',
        help='Drop listed years from JSON/CSVs and re-run',
    )
    args = parser.parse_args(argv)
    years = [int(y.strip()) for y in args.years.split(',') if y.strip()]
    out_dir = resolve_output_dir(create=True)
    json_path = out_dir / 'va_support_census.json'
    results = _load_json(json_path)

    if args.force:
        drop = set(years)
        results = [r for r in results if int(r.get('year', -1)) not in drop]
        for year in years:
            csv_path = out_dir / f'industry_va_support_{year}.csv'
            if csv_path.exists():
                csv_path.unlink()
        _write_json(json_path, results)
        print(f'--force: dropped years {sorted(drop)}', flush=True)

    done = {int(r['year']) for r in results if 'error' not in r and 'industries' in r}
    failures = 0
    for year in years:
        if year in done:
            print(f'{year}: already in va_support_census.json, skipping', flush=True)
            continue
        print(f'\n=== {year}: va_support_census starting ===', flush=True)
        try:
            row = run_year(year)
        except BaseException as err:  # noqa: BLE001
            failures += 1
            print(
                f'{year}: FAILED {type(err).__name__}: {err}\n'
                f'{traceback.format_exc()}',
                flush=True,
            )
            row = {'year': year, 'error': f'{type(err).__name__}: {err}'}
        else:
            frame = pd.DataFrame(row['industries'], columns=list(INDUSTRY_CSV_COLUMNS))
            csv_name = f'industry_va_support_{year}.csv'
            frame.to_csv(out_dir / csv_name, index=False)
            row['csv'] = csv_name
            print(
                f'{year}: b1_gate={row["b1_gate"]} '
                f'named_t1={sum(r["t1_abs"] for r in row["industries"]):.1f} '
                f'named_t18={sum(r["t18_abs"] for r in row["industries"]):.1f} '
                f'b2_cand={row["b2_candidate_cells"]} '
                f'opened={row["b2_cells_opened"]} '
                f'hygiene_ok={row["hygiene_ok"]} '
                f't11={row["t11_max_abs_residual"]:.3g}',
                flush=True,
            )

        results = [r for r in results if int(r.get('year', -1)) != year]
        results.append(row)
        results.sort(key=lambda r: int(r['year']))
        _write_json(json_path, results)
        _write_summary(out_dir, results)

    _write_summary(out_dir, results)
    print(f'Wrote {out_dir / "va_support_summary.csv"}', flush=True)
    print(f'Wrote {json_path}', flush=True)
    return 1 if failures else 0


if __name__ == '__main__':
    raise SystemExit(main())
