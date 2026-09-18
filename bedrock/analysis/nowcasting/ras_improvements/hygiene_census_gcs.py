"""#839 hygiene census on saved BalancedSUT (GCS or local).

Does not re-run GRAS — use :mod:`hygiene_census` for a full ``balance_year`` run.

Example::

    uv run python -m bedrock.analysis.nowcasting.ras_improvements.hygiene_census_gcs \\
        --years 2018,2021,2023
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd
from google.cloud import storage

from bedrock.analysis.nowcasting.ras_improvements.common import (
    GCS_BALANCED_SUT,
    illicit_mask,
    resolve_artifact_dir,
)
from bedrock.transform.iot.nowcast_mask import published_2017_panel
from bedrock.transform.iot.nowcast_sut_assembly import (
    RESIDUE_EPS_USD_M,
    assemble_masks,
    sweep_offset_residue,
    zero_pattern_leak,
)
from bedrock.utils.io.gcp import download_gcs_file


def _latest_blob_name(year: int, kind: str) -> str:
    prefix = {
        'use': f'{GCS_BALANCED_SUT}/Balanced_Detail_Use_SUT_{year}_',
        'supply': f'{GCS_BALANCED_SUT}/Balanced_Detail_Supply_{year}_',
    }[kind]
    client = storage.Client()
    bucket = client.bucket('cornerstone-default')
    blobs = [b for b in bucket.list_blobs(prefix=prefix) if b.name.endswith('.parquet')]
    if not blobs:
        raise FileNotFoundError(f'no GCS parquet under {prefix}')
    blobs.sort(key=lambda b: b.updated or b.time_created)
    return blobs[-1].name


def _download_balanced(year: int) -> dict[str, pd.DataFrame]:
    out_dir = resolve_artifact_dir(create=True)
    frames: dict[str, pd.DataFrame] = {}
    for kind, _artifact in (
        ('use', 'Balanced_Detail_Use_SUT'),
        ('supply', 'Balanced_Detail_Supply'),
    ):
        gcs_name = _latest_blob_name(year, kind)
        local_name = Path(gcs_name).name
        local = out_dir / local_name
        if not local.exists():
            print(f'downloading gs://cornerstone-default/{gcs_name}', flush=True)
            download_gcs_file(local_name, GCS_BALANCED_SUT, str(local))
        else:
            print(f'using local {local}', flush=True)
        frames[kind] = pd.read_parquet(local)
        print(
            f'{year} {kind}: {frames[kind].shape} from {local_name}',
            flush=True,
        )
    return frames


def _census_year(year: int) -> dict[str, object]:
    print(f'\n=== {year}: load balanced + masks ===', flush=True)
    balanced = _download_balanced(year)
    masks = assemble_masks(year)
    pattern = published_2017_panel('use')
    supply_pattern = published_2017_panel('supply')

    for block in ('use', 'supply'):
        mask = masks[block]
        frame = balanced[block]
        if not (
            frame.index.equals(mask.structural_zero.index)
            and frame.columns.equals(mask.structural_zero.columns)
        ):
            balanced[block] = frame.reindex(
                index=mask.structural_zero.index,
                columns=mask.structural_zero.columns,
                fill_value=0.0,
            )
            print(f'{year} {block}: reindexed to current mask labels', flush=True)

    use = balanced['use']
    use_mask = masks['use']
    rows: dict[str, object] = {'year': year, 'source': 'GCS BalancedSUT'}

    for block in ('use', 'supply'):
        block_pattern = pattern if block == 'use' else supply_pattern
        if not (
            balanced[block].index.equals(block_pattern.index)
            and balanced[block].columns.equals(block_pattern.columns)
        ):
            block_pattern = block_pattern.reindex(
                index=balanced[block].index,
                columns=balanced[block].columns,
                fill_value=0.0,
            )
            if block == 'use':
                pattern = block_pattern
            else:
                supply_pattern = block_pattern
        n_cells, mass = zero_pattern_leak(balanced[block], block_pattern)
        rows[f'{block}_zero_leak_cells'] = n_cells
        rows[f'{block}_zero_leak_mass_usd_m'] = mass
        print(
            f'{year} 2a {block}: {n_cells} cells, mass={mass:.6g} $M',
            flush=True,
        )

    if not (use.index.equals(pattern.index) and use.columns.equals(pattern.columns)):
        pattern = pattern.reindex(index=use.index, columns=use.columns, fill_value=0.0)

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
    total_signed = float(illicit_vals.sum().sum())
    rows.update(
        {
            'use_illicit_negatives': n_illicit,
            'use_illicit_mass_usd_m': mass_illicit,
            'use_illicit_signed_sum_usd_m': total_signed,
            'use_illicit_below_eps': n_below,
            'use_illicit_at_eps': n_at,
            'use_illicit_above_eps': n_above,
            'use_illicit_max_abs_usd_m': max_abs,
        }
    )
    print(
        f'{year} 2b use illicit: {n_illicit} cells '
        f'(below={n_below}, at={n_at}, above={n_above}), '
        f'mass={mass_illicit:.6g} $M, signed_sum={total_signed:.6g} $M, '
        f'max|x|={max_abs:.6g} $M',
        flush=True,
    )

    cleaned, n_swept = sweep_offset_residue(use, use_mask, pattern, RESIDUE_EPS_USD_M)
    illicit_after = illicit_mask(cleaned, use_mask, pattern)
    n_illicit_after = int(illicit_after.to_numpy().sum())
    n_below_after = int(
        (illicit_after & (cleaned.abs() < RESIDUE_EPS_USD_M)).to_numpy().sum()
    )
    illicit_swept = int((below & (cleaned == 0.0)).to_numpy().sum())
    rows.update(
        {
            'use_residue_swept_cells': n_swept,
            'use_illicit_cells_swept': illicit_swept,
            'use_illicit_after_sweep': n_illicit_after,
            'use_illicit_below_eps_after_sweep': n_below_after,
        }
    )
    print(
        f'{year} item1 sweep: swept_illicit_below_eps={n_swept}, '
        f'illicit_cells_zeroed={illicit_swept}/{n_below}, '
        f'illicit_after={n_illicit_after}, '
        f'illicit_below_eps_after={n_below_after}',
        flush=True,
    )
    return rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--years', default='2018,2021,2023')
    args = parser.parse_args(argv)
    years = [int(y.strip()) for y in args.years.split(',') if y.strip()]
    results = []
    failures = 0
    for year in years:
        try:
            results.append(_census_year(year))
        except Exception as err:  # noqa: BLE001
            failures += 1
            print(f'{year}: FAILED {type(err).__name__}: {err}', flush=True)
            results.append({'year': year, 'error': f'{type(err).__name__}: {err}'})
    out_dir = resolve_artifact_dir(create=True)
    out = out_dir / 'hygiene_census_gcs.json'
    out.write_text(json.dumps(results, indent=2))
    print(f'\nWrote {out}', flush=True)
    print(json.dumps(results, indent=2), flush=True)
    return 1 if failures else 0


if __name__ == '__main__':
    sys.exit(main())
