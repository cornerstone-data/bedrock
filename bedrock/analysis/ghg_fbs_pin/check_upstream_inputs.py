"""Preflight: pinned Energy FBS + NowcastMUT vintages for a GHG rebuild.

``find_file`` loads the newest ``ctime`` match for a stem under ``FBS_DIR``.
Energy FBS pins lock exact filenames. Nowcast Use attribution is melted from
NowcastMUT (``flowsa/NowcastMUT``); the FBA parse path uses
``latest_nowcast_mut_vintage`` and ignores USAConfig, so this pin locks the
MUT vintage (and requires local Nowcast FBAs for that year to be absent so
regen cannot reuse a stale melt).

Usage::

    uv run python -m bedrock.analysis.ghg_fbs_pin.check_upstream_inputs
    uv run python -m bedrock.analysis.ghg_fbs_pin.check_upstream_inputs \\
        --method GHG_national_Cornerstone_2024
    uv run python -m bedrock.analysis.ghg_fbs_pin.check_upstream_inputs \\
        --download
    uv run python -m bedrock.analysis.ghg_fbs_pin.check_upstream_inputs \\
        --quarantine-rivals
"""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path
from typing import Any

from bedrock.extract.iot.nowcast_mut_storage import (
    GCS_NOWCAST_MUT_DIR,
    STORED_MUT_TABLES,
    latest_nowcast_mut_vintage,
    nowcast_mut_artifact_name,
    nowcast_mut_artifact_stem,
)
from bedrock.utils.config.settings import FBA_DIR, FBS_DIR

DEFAULT_PIN = Path(__file__).resolve().parent / 'upstream_inputs_pin.json'

_DEFAULT_GCS_FBS = 'transform/output_data'
_DEFAULT_GCS_MUT = GCS_NOWCAST_MUT_DIR


def load_pin(path: Path | None = None) -> dict[str, Any]:
    pin_path = path or DEFAULT_PIN
    raw = json.loads(pin_path.read_text(encoding='utf-8'))
    if not isinstance(raw, dict) or 'methods' not in raw:
        raise ValueError(f'upstream inputs pin must have a methods object: {pin_path}')
    return raw


def _stem_rivals(directory: Path, stem: str, pinned_name: str) -> list[str]:
    if not directory.is_dir():
        return []
    return sorted(
        p.name for p in directory.glob(f'{stem}_v*.parquet') if p.name != pinned_name
    )


def _newest_local(directory: Path, stem: str) -> str | None:
    if not directory.is_dir():
        return None
    matches = sorted(
        directory.glob(f'{stem}_v*.parquet'),
        key=lambda p: p.stat().st_ctime,
        reverse=True,
    )
    return matches[0].name if matches else None


def _iter_fbs_entries(
    method_block: dict[str, Any],
) -> list[tuple[str, str | None]]:
    """Yield (stem, filename_or_none) for Energy FBS pins."""
    block = method_block.get('fbs') or {}
    if not isinstance(block, dict):
        raise ValueError("'fbs' must be an object of stem -> filename")
    out: list[tuple[str, str | None]] = []
    for stem, filename in block.items():
        name = None if filename in (None, '') else str(filename)
        out.append((str(stem), name))
    return out


def _gcs_fbs_prefix(pin: dict[str, Any]) -> str:
    return str(pin.get('gcs_sub_bucket_fbs') or _DEFAULT_GCS_FBS).strip('/')


def _gcs_mut_prefix(pin: dict[str, Any]) -> str:
    return str(pin.get('gcs_sub_bucket_mut') or _DEFAULT_GCS_MUT).strip('/')


def _mut_spec(method_block: dict[str, Any]) -> dict[str, Any] | None:
    raw = method_block.get('nowcast_mut')
    if raw in (None, {}):
        return None
    if not isinstance(raw, dict):
        raise ValueError("'nowcast_mut' must be an object")
    for key in ('year', 'stage', 'vintage'):
        if key not in raw:
            raise ValueError(f"'nowcast_mut' missing {key!r}")
    tables = raw.get('tables') or list(STORED_MUT_TABLES)
    return {
        'year': int(raw['year']),
        'stage': str(raw['stage']),
        'vintage': str(raw['vintage']),
        'tables': [str(t) for t in tables],
    }


def _local_nowcast_fbas(year: int) -> list[Path]:
    """Cached Nowcast Use FBAs for *year* under FBA_DIR (poison for MUT pin)."""
    if not Path(FBA_DIR).is_dir():
        return []
    return sorted(
        Path(FBA_DIR).glob(f'Nowcast_Detail_Use_AfterRedef_{year}_v*.parquet')
    )


def _mut_rivals(year: int, stage: str, table: str, pinned_name: str) -> list[str]:
    stem = nowcast_mut_artifact_stem(table, year=year, stage=stage)  # type: ignore[arg-type]
    directory = Path(FBS_DIR)
    if not directory.is_dir():
        return []
    return sorted(
        p.name for p in directory.glob(f'{stem}_v*.parquet') if p.name != pinned_name
    )


def check_nowcast_mut(
    method: str,
    spec: dict[str, Any],
    *,
    pin: dict[str, Any],
    download: bool = False,
) -> list[str]:
    """Require pinned MUT tables locally; fail on wrong latest vintage or FBAs."""
    problems: list[str] = []
    year = spec['year']
    stage = spec['stage']
    vintage = spec['vintage']
    gcs_prefix = _gcs_mut_prefix(pin)
    directory = Path(FBS_DIR)

    try:
        latest = latest_nowcast_mut_vintage(year=year, stage=stage)
    except Exception as exc:
        problems.append(
            f'{method}: could not resolve latest NowcastMUT vintage for '
            f'{year}/{stage}: {exc}'
        )
        latest = None
    if latest is not None and latest != vintage:
        problems.append(
            f'{method}: pinned NowcastMUT vintage {vintage!r} but '
            f'latest_nowcast_mut_vintage({year}, {stage!r}) is {latest!r}; '
            f'FBA regen would melt {latest!r}'
        )

    for table in spec['tables']:
        filename = nowcast_mut_artifact_name(
            table,
            year=year,
            stage=stage,
            vintage=vintage,
        )
        dest = directory / filename
        gcs_uri = f'gs://cornerstone-default/{gcs_prefix}/{filename}'
        if not dest.is_file():
            if download:
                try:
                    from bedrock.utils.io.gcp import download_gcs_file  # noqa: PLC0415

                    dest.parent.mkdir(parents=True, exist_ok=True)
                    download_gcs_file(filename, gcs_prefix, str(dest))
                except Exception as exc:
                    problems.append(
                        f'{method}: missing MUT {dest} (pin {gcs_uri}); '
                        f'download failed: {exc}'
                    )
                    continue
                if not dest.is_file():
                    problems.append(
                        f'{method}: missing MUT {dest} after download ({gcs_uri})'
                    )
                    continue
            else:
                problems.append(f'{method}: missing MUT {dest} (pin {gcs_uri})')
                continue
        rivals = _mut_rivals(year, stage, table, filename)
        if rivals:
            problems.append(
                f'{method}: competing local NowcastMUT for {table} {year}/{stage} '
                f'in {directory}: pin={filename} also={rivals}'
            )

    fbas = _local_nowcast_fbas(year)
    if fbas:
        problems.append(
            f'{method}: local Nowcast Use FBAs for {year} would be reused instead '
            f'of melting pinned MUT {vintage!r}: {[p.name for p in fbas]} '
            f'(run --quarantine-rivals)'
        )
    return problems


def check_method(
    method: str,
    method_block: dict[str, Any],
    *,
    pin: dict[str, Any] | None = None,
    download: bool = False,
) -> list[str]:
    """Return human-readable problems for one method (empty means OK)."""
    pin = pin or {}
    problems: list[str] = []
    gcs_fbs = _gcs_fbs_prefix(pin)
    for stem, filename in _iter_fbs_entries(method_block):
        directory = Path(FBS_DIR)
        if not filename:
            problems.append(
                f'{method}: fbs stem {stem!r} has no pinned filename '
                f'(run --discover --write)'
            )
            continue
        dest = directory / filename
        gcs_uri = f'gs://cornerstone-default/{gcs_fbs}/{filename}'
        if not dest.is_file():
            if download:
                try:
                    from bedrock.utils.io.gcp import download_gcs_file  # noqa: PLC0415

                    dest.parent.mkdir(parents=True, exist_ok=True)
                    download_gcs_file(filename, gcs_fbs, str(dest))
                except Exception as exc:
                    problems.append(
                        f'{method}: missing {dest} (pin {gcs_uri}); '
                        f'download failed: {exc}'
                    )
                    continue
                if not dest.is_file():
                    problems.append(
                        f'{method}: missing {dest} after download ({gcs_uri})'
                    )
                    continue
            else:
                problems.append(f'{method}: missing {dest} (pin {gcs_uri})')
                continue
        rivals = _stem_rivals(directory, stem, filename)
        if rivals:
            problems.append(
                f'{method}: competing local fbs for {stem} in {directory}: '
                f'pin={filename} also={rivals}'
            )

    mut = _mut_spec(method_block)
    if mut is not None:
        problems.extend(check_nowcast_mut(method, mut, pin=pin, download=download))
    return problems


def check_upstream_inputs(
    *,
    pin: dict[str, Any] | None = None,
    methods: list[str] | None = None,
    download: bool = False,
) -> list[str]:
    """Return all problems across selected methods (+ optional stewi check)."""
    pin = pin if pin is not None else load_pin()
    method_map: dict[str, Any] = pin['methods']
    selected = methods if methods is not None else sorted(method_map)
    problems: list[str] = []
    for method in selected:
        if method not in method_map:
            problems.append(f'unknown method {method!r} (not in pin methods)')
            continue
        problems.extend(
            check_method(method, method_map[method], pin=pin, download=download)
        )
    if pin.get('require_stewi_facility_pin', True):
        from bedrock.utils.snapshots.stewi_facility_pin import (  # noqa: PLC0415
            check_stewi_facility_pin,
            ensure_pinned_inventories,
            pin_enforced,
        )

        if pin_enforced():
            if download:
                try:
                    ensure_pinned_inventories(download=True)
                except FileNotFoundError as exc:
                    problems.append(str(exc))
            problems.extend(check_stewi_facility_pin())
    return problems


def discover_filenames(
    pin: dict[str, Any],
    *,
    methods: list[str] | None = None,
    refresh: bool = False,
) -> dict[str, Any]:
    """Fill null/empty Energy FBS filenames from newest local match."""
    method_map: dict[str, Any] = pin['methods']
    selected = methods if methods is not None else sorted(method_map)
    for method in selected:
        block = method_map[method]
        entries = block.setdefault('fbs', {})
        directory = Path(FBS_DIR)
        for stem, filename in list(entries.items()):
            if filename and not refresh:
                continue
            newest = _newest_local(directory, stem)
            entries[stem] = newest if newest is not None else filename
    return pin


def quarantine_rivals(
    pin: dict[str, Any],
    *,
    methods: list[str] | None = None,
    quarantine_dirname: str = '_quarantine',
) -> list[Path]:
    """Move Energy FBS rivals, other MUT vintages, and Nowcast Use FBAs aside."""
    method_map: dict[str, Any] = pin['methods']
    selected = methods if methods is not None else sorted(method_map)
    moved: list[Path] = []

    def _move(src: Path, qdir: Path) -> None:
        nonlocal moved
        if not src.is_file():
            return
        qdir.mkdir(parents=True, exist_ok=True)
        dest = qdir / src.name
        if dest.exists():
            dest = qdir / f'{src.stem}__{src.stat().st_mtime_ns}{src.suffix}'
        shutil.move(str(src), str(dest))
        moved.append(dest)
        meta = src.with_name(f'{src.stem}_metadata.json')
        if meta.is_file():
            meta_dest = qdir / meta.name
            if meta_dest.exists():
                meta_dest = (
                    qdir / f'{meta.stem}__{meta.stat().st_mtime_ns}{meta.suffix}'
                )
            shutil.move(str(meta), str(meta_dest))
            moved.append(meta_dest)

    for method in selected:
        block = method_map[method]
        for stem, filename in _iter_fbs_entries(block):
            if not filename:
                continue
            directory = Path(FBS_DIR)
            for rival in _stem_rivals(directory, stem, filename):
                _move(directory / rival, directory / quarantine_dirname)

        mut = _mut_spec(block)
        if mut is None:
            continue
        year = mut['year']
        stage = mut['stage']
        vintage = mut['vintage']
        for table in mut['tables']:
            pinned = nowcast_mut_artifact_name(
                table,
                year=year,
                stage=stage,
                vintage=vintage,
            )
            for rival in _mut_rivals(year, stage, table, pinned):
                _move(Path(FBS_DIR) / rival, Path(FBS_DIR) / quarantine_dirname)
        for fba in _local_nowcast_fbas(year):
            _move(fba, Path(FBA_DIR) / quarantine_dirname)
    return moved


def _print_ok(pin: dict[str, Any], methods: list[str]) -> None:
    for method in methods:
        block = pin['methods'][method]
        print(f'OK {method}')
        for stem, filename in _iter_fbs_entries(block):
            print(f'  fbs {stem}: {filename}')
        mut = _mut_spec(block)
        if mut is not None:
            print(
                f'  nowcast_mut {mut["year"]}/{mut["stage"]}: {mut["vintage"]} '
                f'tables={mut["tables"]}'
            )


def main() -> None:
    parser = argparse.ArgumentParser(description=(__doc__ or '').splitlines()[0])
    parser.add_argument(
        '--pin',
        type=Path,
        default=DEFAULT_PIN,
        help='Path to upstream_inputs_pin.json',
    )
    parser.add_argument(
        '--method',
        action='append',
        dest='methods',
        default=None,
        help='Method stem to check (repeatable; default: all in pin)',
    )
    parser.add_argument(
        '--download',
        action='store_true',
        help='Download missing pinned objects from GCS (still fails on rivals)',
    )
    parser.add_argument(
        '--discover',
        action='store_true',
        help='Fill empty Energy FBS filenames from newest local parquet',
    )
    parser.add_argument(
        '--write',
        action='store_true',
        help='With --discover, write updated filenames back to the pin JSON',
    )
    parser.add_argument(
        '--refresh',
        action='store_true',
        help='With --discover, overwrite existing filenames with newest local',
    )
    parser.add_argument(
        '--quarantine-rivals',
        action='store_true',
        help=(
            'Move Energy FBS rivals, other MUT vintages, and Nowcast Use FBAs '
            'into _quarantine/ then re-check'
        ),
    )
    args = parser.parse_args()

    pin = load_pin(args.pin)
    methods = args.methods or sorted(pin['methods'])

    if args.discover:
        pin = discover_filenames(pin, methods=methods, refresh=args.refresh)
        if args.write:
            args.pin.write_text(
                json.dumps(pin, indent=2) + '\n',
                encoding='utf-8',
            )
            print(f'Wrote {args.pin}')
        else:
            print(json.dumps(pin, indent=2))
            print('(pass --write to save)')

    if args.quarantine_rivals:
        moved = quarantine_rivals(pin, methods=methods)
        print(f'Quarantined {len(moved)} files under */_quarantine/')
        for path in moved:
            print(f'  {path}')

    problems = check_upstream_inputs(
        pin=pin,
        methods=methods,
        download=args.download,
    )
    if problems:
        print('Upstream input pin check FAILED:')
        for line in problems:
            print(f'  - {line}')
        raise SystemExit(1)
    _print_ok(pin, methods)
    if pin.get('require_stewi_facility_pin', True):
        print('stewi_facility_pin: OK')


if __name__ == '__main__':
    main()
