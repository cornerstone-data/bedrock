"""Pinned stewi + facilitymatcher outputs for facilities FBS builds."""

from __future__ import annotations

import hashlib
import json
import logging
import os
from pathlib import Path
from typing import Any

import facilitymatcher.globals as fm_globals
import stewi.globals as stewi_globals
from esupy.processed_data_mgmt import FileMeta

from bedrock.utils.config.settings import FBS_DIR, WRITE_FORMAT
from bedrock.utils.io.gcp import download_gcs_file
from bedrock.utils.io.read import find_file

log = logging.getLogger(__name__)

_SNAPSHOT_BASE = Path(__file__).resolve().parent
DEFAULT_STEWI_FACILITY_PIN = _SNAPSHOT_BASE / 'stewi_facility_pin.json'

#: Set to ``0`` / ``false`` / ``off`` to skip pin download + checks.
_ENV_PIN = 'BEDROCK_STEWI_FACILITY_PIN'

_MATCHER_STEMS = (
    'FacilityMatchList_forStEWI',
    'FRS_NAICSforStEWI',
)


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


def pin_manifest_sha256(pin_json_path: str | Path | None = None) -> str:
    """SHA256 of the committed pin JSON bytes (provenance, not GCS objects)."""
    path = Path(pin_json_path or DEFAULT_STEWI_FACILITY_PIN)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _newest_parquet_stem(directory: Path, name_data: str) -> str | None:
    if not directory.is_dir():
        return None
    matches = sorted(
        directory.glob(f'{name_data}_v*.parquet'),
        key=lambda p: p.stat().st_ctime,
        reverse=True,
    )
    return matches[0].name if matches else None


def _inventory_name_data(inventory: str, year: str | int) -> str:
    return f'{inventory}_{year}'


def resolve_inventory_local_files(
    inventory_dict: dict[str, str | int],
    *,
    pin: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Map each inventory year to local parquet stems actually present.

    Prefer the pin stem when that file exists; otherwise the newest ctime
    match (same rule stewi / :func:`find_file` use).
    """
    pin = pin if pin is not None else load_stewi_facility_pin()
    categories = [str(c) for c in pin['stewi']['categories']]
    pin_inv: dict[str, str] = pin['stewi']['inventories']
    stewi_root = _local_base('stewi')
    out: dict[str, Any] = {}
    for inventory, year in inventory_dict.items():
        name_data = _inventory_name_data(str(inventory), year)
        pin_vh = pin_inv.get(name_data)
        cats: dict[str, str | None] = {}
        for cat in categories:
            pinned_name = f'{name_data}_{pin_vh}.parquet' if pin_vh else None
            cat_dir = stewi_root / cat
            if pinned_name and (cat_dir / pinned_name).is_file():
                cats[cat] = pinned_name
            else:
                cats[cat] = _newest_parquet_stem(cat_dir, name_data)
        matches_pin = bool(pin_vh) and all(
            cats.get(cat) == f'{name_data}_{pin_vh}.parquet' for cat in categories
        )
        out[name_data] = {
            'pin_version_hash': pin_vh,
            'categories': cats,
            'matches_pin': matches_pin,
        }
    return out


def resolve_facilitymatcher_local_files(
    *,
    pin: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Local facilitymatcher parquet stems vs the pin."""
    pin = pin if pin is not None else load_stewi_facility_pin()
    fm_dir = _local_base('facilitymatcher')
    pinned = [str(n) for n in pin['facilitymatcher']['files']]
    local_by_stem: dict[str, str | None] = {}
    for stem in _MATCHER_STEMS:
        pinned_for_stem = next((n for n in pinned if n.startswith(f'{stem}_')), None)
        if pinned_for_stem and (fm_dir / pinned_for_stem).is_file():
            local_by_stem[stem] = pinned_for_stem
            continue
        local_by_stem[stem] = _newest_parquet_stem(fm_dir, stem)
    return {
        'pinned': pinned,
        'local': local_by_stem,
        'matches_pin': all(local_by_stem.get(stem) in pinned for stem in _MATCHER_STEMS)
        and all(local_by_stem.values()),
    }


def resolve_energy_fbs_filename(mecs_method: str) -> str | None:
    """Filename of the Energy FBS ``find_file`` would load for *mecs_method*."""
    meta = FileMeta()
    meta.name_data = mecs_method
    meta.ext = WRITE_FORMAT or 'parquet'
    try:
        path = find_file(meta, str(FBS_DIR))
    except FileNotFoundError:
        return None
    return Path(path).name


def facility_attribution_source_metadata(
    inventory_dict: dict[str, str | int],
    *,
    mecs_method: str | None = None,
    name_data: str | None = None,
    pin_json_path: str | Path | None = None,
) -> dict[str, Any]:
    """Provenance for facilities / Hybrid FBS attribution sources.

    Used when FBS metadata would otherwise record
    ``No metadata found for GHGRP_NEI_Facilities`` (or Hybrid): records the
    stewi inventory stems, facilitymatcher stems, pin manifest, and optional
    Energy FBS filename actually present locally.
    """
    pin_path = Path(pin_json_path or DEFAULT_STEWI_FACILITY_PIN)
    pin = load_stewi_facility_pin(pin_path)
    tool_meta: dict[str, Any] = {
        'inventories': resolve_inventory_local_files(inventory_dict, pin=pin),
        'facilitymatcher': resolve_facilitymatcher_local_files(pin=pin),
        'stewi_facility_pin': {
            'path': str(pin_path),
            'sha256': pin_manifest_sha256(pin_path),
            'enforced': pin_enforced(),
        },
    }
    if mecs_method:
        tool_meta['energy_fbs'] = {
            'method': mecs_method,
            'filename': resolve_energy_fbs_filename(mecs_method),
        }
    return {
        'tool': 'bedrock',
        'category': 'facility_attribution',
        'name_data': name_data or 'facility_attribution',
        'tool_meta': tool_meta,
    }


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
