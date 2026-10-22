"""Rebuild a Cornerstone GHG FBS and draft the pin bump.

Steps this script covers locally (upload is still manual)::

1. ``FlowBySector.generateFlowBySector`` for the year stem.
2. Confirm gas x sector totals match the currently pinned parquet (and that
   ``ActivityProducedBy`` is present when the method retains activities).
3. Write SHA256 + a draft ``cornerstone_ghg_fbs_2024_pin.json`` beside the
   parquet, and print the GCS upload / config paths to edit.

Usage::

    uv run python -m bedrock.analysis.ghg_fbs_pin.prepare_pin
    uv run python -m bedrock.analysis.ghg_fbs_pin.prepare_pin \\
        --method GHG_national_Cornerstone_2024 --skip-gas-check
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import time
from pathlib import Path

from bedrock.analysis.ghg_fbs_pin.compare_gas_e import compare

OUTPUT_DIR = Path(__file__).resolve().parent / 'output'
PIN_TEMPLATE = {
    'method': '',
    'gcs_sub_bucket': 'transform/output_data',
    'filename': '',
    'sha256': '',
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _configs_pinning_stem(method: str) -> list[str]:
    """Config YAML paths whose cornerstone_ghg_fbs_filename mentions *method* or NFT."""
    configs = Path(__file__).resolve().parents[2] / 'utils' / 'config' / 'configs'
    repo_root = Path(__file__).resolve().parents[3]
    hits: list[str] = []
    # Canonical / NFT 2024 pins that the year-stem retained rebuild replaces.
    needles = (
        f'{method}_v',
        'GHG_national_Cornerstone_nowcast_facilities_transportation_2024_v',
    )
    for path in sorted(configs.glob('*.yaml')):
        text = path.read_text(encoding='utf-8')
        if 'cornerstone_ghg_fbs_filename' not in text:
            continue
        if any(n in text for n in needles):
            hits.append(str(path.relative_to(repo_root)))
    return hits


def main() -> None:
    parser = argparse.ArgumentParser(description=(__doc__ or '').splitlines()[0])
    parser.add_argument(
        '--method',
        default=None,
        help='GHG FBS method stem (default: active config year stem).',
    )
    parser.add_argument(
        '--skip-gas-check',
        action='store_true',
        help='Skip compare against the currently pinned parquet.',
    )
    parser.add_argument(
        '--skip-rebuild',
        action='store_true',
        help='Use the newest local FBS_DIR parquet for --method instead of regenerating.',
    )
    args = parser.parse_args()

    from bedrock.transform.allocation.derived import (  # noqa: PLC0415
        _load_cornerstone_ghg_fbs_from_gcs,
        _select_cornerstone_ghg_fbs_base_name,
    )
    from bedrock.transform.flowbysector import FlowBySector  # noqa: PLC0415
    from bedrock.utils.config.settings import FBS_DIR  # noqa: PLC0415

    method = args.method or _select_cornerstone_ghg_fbs_base_name()
    OUTPUT_DIR.mkdir(exist_ok=True)

    if args.skip_rebuild:
        candidates = sorted(
            FBS_DIR.glob(f'{method}_v*.parquet'),
            key=lambda p: p.stat().st_mtime,
        )
        if not candidates:
            raise SystemExit(f'No local parquet for {method!r} under {FBS_DIR}')
        written = candidates[-1]
        print(f'Using existing {written.name}')
    else:
        started = time.time()
        FlowBySector.generateFlowBySector(method, download_sources_ok=True)
        print(f'{method} rebuilt in {time.time() - started:.0f}s')
        written_candidates = [
            p
            for p in FBS_DIR.glob(f'{method}_v*')
            if p.stat().st_mtime >= started and p.suffix == '.parquet'
        ]
        if not written_candidates:
            raise SystemExit(
                f'Rebuild finished but no new {method}_v*.parquet under {FBS_DIR}'
            )
        written = max(written_candidates, key=lambda p: p.stat().st_mtime)

    retained = __import__('pandas').read_parquet(written)
    if 'ActivityProducedBy' not in retained.columns:
        raise SystemExit(
            f'{written.name} lacks ActivityProducedBy; retain_activity_columns '
            'did not reach the output'
        )
    n_act = retained['ActivityProducedBy'].nunique(dropna=True)
    print(f'ActivityProducedBy: {n_act} distinct values, {len(retained)} rows')

    if not args.skip_gas_check:
        reference = _load_cornerstone_ghg_fbs_from_gcs(base_name=method)
        bad = compare(reference, retained)
        if not bad.empty:
            out = OUTPUT_DIR / f'{method}_gas_sector_diffs.csv'
            (bad / 1e9).round(6).to_csv(out)
            raise SystemExit(
                f'Gas x sector totals differ from the current pin '
                f'({len(bad)} cells). Wrote {out}'
            )
        print('Gas x sector totals match the current pin.')

    dest = OUTPUT_DIR / written.name
    if written.resolve() != dest.resolve():
        shutil.copy2(written, dest)
        meta = written.with_suffix('.json')
        if meta.is_file():
            shutil.copy2(meta, OUTPUT_DIR / meta.name)
    digest = _sha256(dest)

    pin = dict(PIN_TEMPLATE)
    pin['method'] = method
    pin['filename'] = dest.name
    pin['sha256'] = digest
    pin_path = OUTPUT_DIR / 'cornerstone_ghg_fbs_2024_pin.DRAFT.json'
    pin_path.write_text(json.dumps(pin, indent=2) + '\n', encoding='utf-8')

    print()
    print('Draft pin:')
    print(json.dumps(pin, indent=2))
    print()
    print('Next:')
    print(
        f'  1. Upload {dest.name} (+ metadata JSON if present) to '
        'gs://cornerstone-default/transform/output_data/'
    )
    print(
        '  2. Copy draft pin over bedrock/utils/snapshots/'
        'cornerstone_ghg_fbs_2024_pin.json'
    )
    print(
        '  3. Set cornerstone_ghg_fbs_filename on canonical + waterfall configs '
        f'to {dest.name!r}. Candidates:'
    )
    for rel in _configs_pinning_stem(method):
        print(f'       - {rel}')
    print(
        '  4. uv run pytest bedrock/transform/__tests__/test_fbs.py '
        '-m fbs_regeneration -v'
    )
    print(f'Draft written to {pin_path}')


if __name__ == '__main__':
    main()
