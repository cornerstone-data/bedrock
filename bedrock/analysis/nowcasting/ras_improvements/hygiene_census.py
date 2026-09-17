"""#839 hygiene census via ``balance_year`` for selected years.

Reports pre-sweep zero-pattern leak (2a), illicit-sign negatives (2b),
and post-sweep confirmation that sub-eps illicit residue is gone (item 1).

Example::

    uv run python -m bedrock.analysis.nowcasting.ras_improvements.hygiene_census \\
        --years 2018,2021,2023
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import traceback

from bedrock.analysis.nowcasting.ras_improvements.common import (
    illicit_mask,
    resolve_artifact_dir,
)
from bedrock.transform.iot.nowcast_mask import published_2017_panel
from bedrock.transform.iot.nowcast_sut_assembly import (
    RESIDUE_EPS_USD_M,
    balance_year,
    save_balance,
    sweep_offset_residue,
    zero_pattern_leak,
)


def _write_results(results: list[dict[str, object]]) -> None:
    out_dir = resolve_artifact_dir(create=True)
    out = out_dir / 'hygiene_census.json'
    out.write_text(json.dumps(results, indent=2))
    print(f'Wrote {out}', flush=True)


def _load_results() -> list[dict[str, object]]:
    path = resolve_artifact_dir() / 'hygiene_census.json'
    if path.exists():
        return json.loads(path.read_text())
    return []


def _census_year(year: int) -> dict[str, object]:
    t0 = time.perf_counter()
    print(f'\n=== {year}: balance_year starting ===', flush=True)
    balance = balance_year(year, impose_soft=True)
    assert balance.balanced is not None
    elapsed = time.perf_counter() - t0
    print(f'{year}: balanced in {elapsed / 60:.1f} min', flush=True)

    pattern = published_2017_panel('use')
    use = balance.balanced['use']
    use_mask = balance.masks['use']
    rows: dict[str, object] = {
        'year': year,
        'balance_minutes': round(elapsed / 60, 2),
    }

    for block in ('use', 'supply'):
        n_cells, mass = zero_pattern_leak(balance.balanced[block], balance.masks[block])
        rows[f'{block}_zero_leak_cells'] = n_cells
        rows[f'{block}_zero_leak_mass_usd_m'] = mass
        print(
            f'{year} 2a {block}: {n_cells} cells, mass={mass:.6g} $M',
            flush=True,
        )

    illicit = illicit_mask(use, use_mask, pattern)
    n_illicit = int(illicit.to_numpy().sum())
    illicit_vals = use.where(illicit, 0.0)
    mass_illicit = float(illicit_vals.abs().sum().sum())
    below = illicit & (use.abs() < RESIDUE_EPS_USD_M)
    at = illicit & (use.abs() == RESIDUE_EPS_USD_M)
    above = illicit & (use.abs() > RESIDUE_EPS_USD_M)
    n_below = int(below.to_numpy().sum())
    n_at = int(at.to_numpy().sum())
    n_above = int(above.to_numpy().sum())
    max_abs = float(illicit_vals.abs().max().max()) if n_illicit else 0.0
    rows.update(
        {
            'use_illicit_negatives': n_illicit,
            'use_illicit_mass_usd_m': mass_illicit,
            'use_illicit_below_eps': n_below,
            'use_illicit_at_eps': n_at,
            'use_illicit_above_eps': n_above,
            'use_illicit_max_abs_usd_m': max_abs,
        }
    )
    print(
        f'{year} 2b use illicit: {n_illicit} cells '
        f'(below={n_below}, at={n_at}, above={n_above}), '
        f'mass={mass_illicit:.6g} $M, max|x|={max_abs:.6g} $M',
        flush=True,
    )

    cleaned, n_swept = sweep_offset_residue(use, use_mask, pattern, RESIDUE_EPS_USD_M)
    illicit_after = illicit_mask(cleaned, use_mask, pattern)
    n_illicit_after = int(illicit_after.to_numpy().sum())
    n_below_after = int(
        (illicit_after & (cleaned.abs() < RESIDUE_EPS_USD_M)).to_numpy().sum()
    )
    rows.update(
        {
            'use_residue_swept_cells': n_swept,
            'use_illicit_after_sweep': n_illicit_after,
            'use_illicit_below_eps_after_sweep': n_below_after,
        }
    )
    print(
        f'{year} item1 sweep: swept={n_swept}, '
        f'illicit_after={n_illicit_after}, '
        f'illicit_below_eps_after={n_below_after}',
        flush=True,
    )

    out_dir = resolve_artifact_dir(create=True)
    written = save_balance(
        balance,
        out_dir,
        protocol='soft (impose_soft=True), max_outer=20',
        upload=False,
    )
    rows['saved_files'] = [str(p.name) for p in written]
    print(f'{year}: saved {len(written)} files under {out_dir}', flush=True)
    return rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--years', default='2018,2021,2023')
    args = parser.parse_args(argv)
    years = [int(y.strip()) for y in args.years.split(',') if y.strip()]

    results = _load_results()
    done = {r['year'] for r in results if 'error' not in r}
    failures = 0
    for year in years:
        if year in done:
            print(f'{year}: already in hygiene_census.json, skipping', flush=True)
            continue
        try:
            row: dict[str, object] = _census_year(year)
        except BaseException as err:  # noqa: BLE001
            failures += 1
            print(
                f'{year}: FAILED {type(err).__name__}: {err}\n'
                f'{traceback.format_exc()}',
                flush=True,
            )
            row = {'year': year, 'error': f'{type(err).__name__}: {err}'}
        results = [r for r in results if r.get('year') != year]
        results.append(row)

        def _year_key(row: dict[str, object]) -> int:
            return int(str(row['year']))

        results.sort(key=_year_key)
        _write_results(results)
    print(json.dumps(results, indent=2), flush=True)
    return 1 if failures else 0


if __name__ == '__main__':
    sys.exit(main())
