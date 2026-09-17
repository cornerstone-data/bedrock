"""Summarize #839 hygiene: fresh local balance vs older GCS vintage ``163db0e``.

Example::

    uv run python -m bedrock.analysis.nowcasting.ras_improvements.hygiene_summary
"""

from __future__ import annotations

import json
import sys

import pandas as pd

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

FRESH_HASH = 'b25b8ac'
OLD_HASH = '163db0e'


def main() -> int:
    out = resolve_artifact_dir(create=True)
    print(f'artifact dir: {out}', flush=True)
    print(f'=== Fresh local balance ({FRESH_HASH}) — post save_balance ===', flush=True)
    for year in (2018, 2021, 2023):
        use_path = out / f'Balanced_Detail_Use_SUT_{year}_v0.3.0_{FRESH_HASH}.parquet'
        supply_path = out / f'Balanced_Detail_Supply_{year}_v0.3.0_{FRESH_HASH}.parquet'
        meta_path = (
            out / f'Balanced_Detail_Use_SUT_{year}_v0.3.0_{FRESH_HASH}_metadata.json'
        )
        if not use_path.exists():
            print(
                f'{year}: missing {use_path.name} (run hygiene_census first)',
                flush=True,
            )
            continue
        use = pd.read_parquet(use_path)
        supply = pd.read_parquet(supply_path)
        meta = json.loads(meta_path.read_text())
        masks = assemble_masks(year)
        pattern = published_2017_panel('use')
        n_u, mass_u = zero_pattern_leak(use, masks['use'])
        n_s, mass_s = zero_pattern_leak(supply, masks['supply'])
        ill = illicit_mask(use, masks['use'], pattern)
        tm = meta['tool_meta']
        print(
            f'{year}: sidecar swept={tm["residue_swept_cells"]} '
            f'eps={tm["residue_eps_usd_m"]}',
            flush=True,
        )
        print(
            f'  2a use leak={n_u} mass={mass_u:.6g}; '
            f'supply leak={n_s} mass={mass_s:.6g}',
            flush=True,
        )
        print(
            f'  post-save illicit={int(ill.to_numpy().sum())} '
            f'below_eps='
            f'{int((ill & (use.abs() < RESIDUE_EPS_USD_M)).to_numpy().sum())}',
            flush=True,
        )

    print(flush=True)
    print(f'=== Pre-sweep on older GCS vintage {OLD_HASH} ===', flush=True)
    for year in (2018, 2021, 2023):
        use_path = out / f'Balanced_Detail_Use_SUT_{year}_v0.3.0_{OLD_HASH}.parquet'
        if not use_path.exists():
            download_gcs_file(use_path.name, GCS_BALANCED_SUT, str(use_path))
        use = pd.read_parquet(use_path)
        masks = assemble_masks(year)
        use = use.reindex(
            index=masks['use'].structural_zero.index,
            columns=masks['use'].structural_zero.columns,
            fill_value=0.0,
        )
        pattern = published_2017_panel('use').reindex(
            index=use.index, columns=use.columns, fill_value=0.0
        )
        sp_path = out / f'Balanced_Detail_Supply_{year}_v0.3.0_{OLD_HASH}.parquet'
        if not sp_path.exists():
            download_gcs_file(sp_path.name, GCS_BALANCED_SUT, str(sp_path))
        supply = pd.read_parquet(sp_path).reindex(
            index=masks['supply'].structural_zero.index,
            columns=masks['supply'].structural_zero.columns,
            fill_value=0.0,
        )
        n_u, mass_u = zero_pattern_leak(use, masks['use'])
        n_s, mass_s = zero_pattern_leak(supply, masks['supply'])
        ill = illicit_mask(use, masks['use'], pattern)
        n_ill = int(ill.to_numpy().sum())
        below = ill & (use.abs() < RESIDUE_EPS_USD_M)
        n_below = int(below.to_numpy().sum())
        mass_ill = float(use.where(ill, 0.0).abs().sum().sum())
        signed = float(use.where(ill, 0.0).sum().sum())
        max_abs = float(use.where(ill, 0.0).abs().max().max()) if n_ill else 0.0
        cleaned, n_swept = sweep_offset_residue(use)
        ill_a = illicit_mask(cleaned, masks['use'], pattern)
        n_below_a = int((ill_a & (cleaned.abs() < RESIDUE_EPS_USD_M)).to_numpy().sum())
        print(
            f'{year}: 2a use cells={n_u} mass={mass_u:.6g}; '
            f'supply cells={n_s} mass={mass_s:.6g}',
            flush=True,
        )
        print(
            f'  2b illicit={n_ill} (below_eps={n_below}) '
            f'mass={mass_ill:.6g} signed={signed:.6g} max|x|={max_abs:.6g}',
            flush=True,
        )
        print(
            f'  item1 after sweep: illicit_below_eps={n_below_a} '
            f'(swept_all={n_swept}, illicit_zeroed={n_below})',
            flush=True,
        )
    return 0


if __name__ == '__main__':
    sys.exit(main())
