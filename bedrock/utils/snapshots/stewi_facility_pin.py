"""Pinned stewi + facilitymatcher outputs for facilities FBS builds."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

import facilitymatcher.globals as fm_globals
import stewi.globals as stewi_globals

from bedrock.utils.io.gcp import download_gcs_file

log = logging.getLogger(__name__)

_SNAPSHOT_BASE = Path(__file__).resolve().parent
DEFAULT_STEWI_FACILITY_PIN = _SNAPSHOT_BASE / 'stewi_facility_pin.json'

#: Set to ``0`` / ``false`` / ``off`` to skip pin download + checks.
_ENV_PIN = 'BEDROCK_STEWI_FACILITY_PIN'


def pin_enforced() -> bool:
    """True unless ``BEDROCK_STEWI_FACILITY_PIN`` disables pinning."""
    raw = os.environ.get(_ENV_PIN, '1').strip().lower()
    return raw not in {'0', 'false', 'off', 'no'}


def load_stewi_facility_pin(
    pin_json_path: str | Path | None = None,
) -> dict[str, Any]:
    """Load the committed stewi / facilitymatcher pin JSON."""
    path = Path(pin_json_path or DEFAULT_STEWI_FACILITY_PIN)
    raw = json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(raw, dict):
        raise ValueError(
            f'stewi facility pin must be an object, got {type(raw).__name__}'
        )
    for key in ('facilitymatcher', 'stewi'):
        if key not in raw:
            raise ValueError(f'Missing key {key!r} in stewi facility pin {path!r}')
    return raw


def pinned_object_specs(pin: dict[str, Any] | None = None) -> list[dict[str, str]]:
    """Flatten pin to GCS object specs: gcs_sub_bucket, filename, local_subdir."""
    pin = pin if pin is not None else load_stewi_facility_pin()
    specs: list[dict[str, str]] = []
    fm = pin['facilitymatcher']
    fm_prefix = str(fm['gcs_prefix']).strip('/')
    for name in fm['files']:
        specs.append(
            {
                'gcs_sub_bucket': fm_prefix,
                'filename': str(name),
                'local_root': 'facilitymatcher',
                'local_subdir': '',
            }
        )
        if str(name).endswith('.parquet'):
            meta_name = str(name).removesuffix('.parquet') + '_metadata.json'
            specs.append(
                {
                    'gcs_sub_bucket': fm_prefix,
                    'filename': meta_name,
                    'local_root': 'facilitymatcher',
                    'local_subdir': '',
                }
            )
    stewi = pin['stewi']
    stewi_prefix = str(stewi['gcs_prefix']).strip('/')
    categories = [str(c) for c in stewi['categories']]
    inventories: dict[str, str] = stewi['inventories']
    for inv_year, version_hash in inventories.items():
        stem = f'{inv_year}_{version_hash}'
        for cat in categories:
            specs.append(
                {
                    'gcs_sub_bucket': f'{stewi_prefix}/{cat}',
                    'filename': f'{stem}.parquet',
                    'local_root': 'stewi',
                    'local_subdir': cat,
                }
            )
        specs.append(
            {
                'gcs_sub_bucket': stewi_prefix,
                'filename': f'{stem}_metadata.json',
                'local_root': 'stewi',
                'local_subdir': '',
            }
        )
    return specs


def _local_base(root_name: str) -> Path:
    if root_name == 'stewi':
        return Path(stewi_globals.paths.local_path)
    if root_name == 'facilitymatcher':
        return Path(fm_globals.output_dir)
    raise ValueError(f'unknown local_root {root_name!r}')


def local_path_for_spec(spec: dict[str, str]) -> Path:
    base = _local_base(spec['local_root'])
    sub = spec.get('local_subdir') or ''
    return base / sub / spec['filename'] if sub else base / spec['filename']


def ensure_pinned_inventories(
    *,
    pin: dict[str, Any] | None = None,
    download: bool = True,
) -> list[Path]:
    """Ensure every pinned parquet/metadata file exists locally.

    Downloads from GCS when missing. Raises if a required object cannot be
    obtained. No-op when :func:`pin_enforced` is false.
    """
    if not pin_enforced():
        log.info('%s disabled; skipping stewi/facilitymatcher pin ensure', _ENV_PIN)
        return []
    pin = pin if pin is not None else load_stewi_facility_pin()
    paths: list[Path] = []
    missing: list[str] = []
    for spec in pinned_object_specs(pin):
        # Metadata JSON is best-effort (one per inventory, not per category).
        is_meta = spec['filename'].endswith('_metadata.json')
        dest = local_path_for_spec(spec)
        if dest.is_file():
            paths.append(dest)
            continue
        if not download:
            if not is_meta:
                missing.append(f'{spec["gcs_sub_bucket"]}/{spec["filename"]}')
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            download_gcs_file(
                spec['filename'],
                spec['gcs_sub_bucket'],
                str(dest),
            )
        except Exception as exc:
            if is_meta:
                log.warning('Pinned metadata miss %s: %s', spec['filename'], exc)
                continue
            missing.append(f'{spec["gcs_sub_bucket"]}/{spec["filename"]}: {exc}')
            continue
        if dest.is_file():
            paths.append(dest)
            log.info('Pinned download %s', dest)
        elif not is_meta:
            missing.append(f'{spec["gcs_sub_bucket"]}/{spec["filename"]}')
    if missing:
        raise FileNotFoundError(
            'Pinned stewi/facilitymatcher files missing from GCS or local disk:\n  '
            + '\n  '.join(missing)
        )
    return paths


def check_stewi_facility_pin(
    *,
    pin: dict[str, Any] | None = None,
) -> list[str]:
    """Return human-readable problems (empty list means OK).

    Checks local presence of pinned parquets and flags competing local versions
    for the same inventory year (stewi would prefer the newer stem).
    """
    pin = pin if pin is not None else load_stewi_facility_pin()
    problems: list[str] = []
    stewi_root = _local_base('stewi')
    for spec in pinned_object_specs(pin):
        if spec['filename'].endswith('_metadata.json'):
            continue
        dest = local_path_for_spec(spec)
        gcs_uri = (
            f'gs://cornerstone-default/{spec["gcs_sub_bucket"]}/{spec["filename"]}'
        )
        if not dest.is_file():
            problems.append(f'missing local {dest} (pin {gcs_uri})')
            continue
        if spec['local_root'] != 'stewi' or not spec.get('local_subdir'):
            continue
        # Competing versions: same INV_YEAR prefix, different version_hash.
        stem = spec['filename'].removesuffix('.parquet')
        # GHGRP_2022_v1.2.0_d850466 -> name_data GHGRP_2022
        parts = stem.rsplit('_', 2)
        if len(parts) < 3:
            continue
        name_data = parts[0]
        cat_dir = stewi_root / spec['local_subdir']
        if not cat_dir.is_dir():
            continue
        rivals = sorted(
            p.name
            for p in cat_dir.glob(f'{name_data}_v*.parquet')
            if p.name != spec['filename']
        )
        if rivals:
            problems.append(
                f'competing local versions for {name_data} in {cat_dir}: '
                f'pin={spec["filename"]} also={rivals}'
            )
    return problems


def main() -> None:
    """CLI: ensure pin downloads, then print check results."""
    logging.basicConfig(level=logging.INFO)
    ensure_pinned_inventories()
    problems = check_stewi_facility_pin()
    if problems:
        print('PIN CHECK FAILED')
        for p in problems:
            print(' ', p)
        raise SystemExit(1)
    print('PIN CHECK OK')


if __name__ == '__main__':
    main()
