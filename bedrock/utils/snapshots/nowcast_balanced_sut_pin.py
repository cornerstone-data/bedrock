"""Pinned 2024 balanced nowcast Supply and Use tables."""

from __future__ import annotations

import hashlib
import json
import sys
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from bedrock.utils.config.settings import FBS_DIR
from bedrock.utils.io.gcp import download_gcs_file

_SNAPSHOT_BASE = Path(__file__).resolve().parent

DEFAULT_NOWCAST_BALANCED_PIN = _SNAPSHOT_BASE / 'nowcast_balanced_sut_2024_pin.json'

GCS_BALANCED_SUT_DIR = 'flowsa/BalancedSUT'

#: Methodology flags the rebuild reads. The pin does not follow
#: ``nowcast_mut_vintage`` or ``.SNAPSHOT_KEY``.
PIN_USA_CONFIG = '2025_usa_cornerstone_v0_5'
PIN_YEAR = 2024
PIN_BLOCKS = ('use', 'supply')


def load_nowcast_balanced_pin(
    pin_json_path: str | Path | None = None,
) -> dict[str, object]:
    """Load the committed balanced-SUT pin (year, GCS location, filename, SHA256)."""
    path = Path(pin_json_path or DEFAULT_NOWCAST_BALANCED_PIN)
    raw = json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(raw, dict):
        raise ValueError(
            f'Balanced SUT pin JSON must be an object, got {type(raw).__name__}'
        )
    for key in ('year', 'gcs_sub_bucket', 'usa_config', 'tables'):
        if key not in raw:
            raise ValueError(f'Missing key {key!r} in balanced SUT pin JSON {path!r}')
    year = int(raw['year'])
    if year != PIN_YEAR:
        raise ValueError(f'Balanced SUT pin year must be {PIN_YEAR}, got {year}')
    sub_bucket = str(raw['gcs_sub_bucket']).strip()
    if sub_bucket != GCS_BALANCED_SUT_DIR:
        raise ValueError(
            f'Balanced SUT pin gcs_sub_bucket must be {GCS_BALANCED_SUT_DIR!r}, '
            f'got {sub_bucket!r}'
        )
    usa_config = str(raw['usa_config']).strip()
    if usa_config != PIN_USA_CONFIG:
        raise ValueError(
            f'Balanced SUT pin usa_config must be {PIN_USA_CONFIG!r}, got {usa_config!r}'
        )
    tables_raw = raw['tables']
    if not isinstance(tables_raw, dict):
        raise ValueError(
            f'Balanced SUT pin tables must be an object, got {type(tables_raw).__name__}'
        )
    tables: dict[str, dict[str, str]] = {}
    for block in PIN_BLOCKS:
        entry = tables_raw.get(block)
        if not isinstance(entry, dict):
            raise ValueError(f'Balanced SUT pin is missing table {block!r}')
        filename = str(entry.get('filename', '')).strip()
        sha = str(entry.get('sha256', '')).strip().lower()
        if not filename:
            raise ValueError(f'Balanced SUT pin table {block!r} is missing filename')
        if len(sha) != 64 or any(c not in '0123456789abcdef' for c in sha):
            raise ValueError(
                f'Balanced SUT pin table {block!r} sha256 must be 64-char hex'
            )
        if sha == '0' * 64:
            raise ValueError(
                f'Balanced SUT pin {path!r} still has a placeholder sha256 for {block!r}'
            )
        tables[block] = {'filename': filename, 'sha256': sha}
    return {
        'year': year,
        'gcs_sub_bucket': sub_bucket,
        'usa_config': usa_config,
        'tables': tables,
    }


def file_sha256_hex(path: str | Path) -> str:
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for chunk in iter(lambda: handle.read(65536), b''):
            digest.update(chunk)
    return digest.hexdigest()


def download_pinned_nowcast_balanced(
    pin: dict[str, object],
    local_dir: str | Path,
) -> dict[str, Path]:
    """Download the pinned balanced parquets and verify SHA256."""
    local_dir = Path(local_dir)
    local_dir.mkdir(parents=True, exist_ok=True)
    sub_bucket = str(pin['gcs_sub_bucket'])
    tables = pin['tables']
    if not isinstance(tables, dict):
        raise ValueError('pin tables must be an object')
    paths: dict[str, Path] = {}
    for block in PIN_BLOCKS:
        entry = tables[block]
        if not isinstance(entry, dict):
            raise ValueError(f'pin table {block!r} must be an object')
        filename = str(entry['filename'])
        expected = str(entry['sha256'])
        local_path = local_dir / filename
        if local_path.is_file() and file_sha256_hex(local_path).lower() != expected:
            local_path.unlink()
        if not local_path.is_file():
            download_gcs_file(filename, sub_bucket, str(local_path))
        got = file_sha256_hex(local_path).lower()
        if got != expected:
            raise ValueError(
                f'Pinned balanced SUT SHA256 mismatch for {filename!r}: '
                f'got {got}, expected {expected}'
            )
        paths[block] = local_path
    return paths


def clear_bedrock_caches() -> None:
    """Drop ``functools.cache`` results so a prior call cannot satisfy a rebuild."""
    for module in list(sys.modules.values()):
        if module is None or not getattr(module, '__name__', '').startswith('bedrock.'):
            continue
        for obj in list(vars(module).values()):
            cache_clear = getattr(obj, 'cache_clear', None)
            if cache_clear is not None:
                cache_clear()


def _point_fbs_dir(new: Path, match: Path) -> None:
    import bedrock.utils.config.settings as settings  # noqa: PLC0415

    settings.FBS_DIR = new
    for module in list(sys.modules.values()):
        if module is None:
            continue
        if getattr(module, 'FBS_DIR', None) == match:
            module.FBS_DIR = new  # type: ignore[attr-defined]


@contextmanager
def isolated_fbs_dir() -> Iterator[Path]:
    """Point FBS reads and writes at an empty directory for the duration.

    Modules that bind ``FBS_DIR`` at import keep that binding, so replacing
    ``settings.FBS_DIR`` alone leaves ``flowby`` and ``flowbysector`` on the
    developer cache. This retargets every already-imported binding, and
    restores bindings taken while the directory was active.
    """
    original = Path(FBS_DIR)
    with tempfile.TemporaryDirectory(prefix='nowcast_balanced_pin_') as raw:
        tmp = Path(raw)
        _point_fbs_dir(tmp, original)
        try:
            yield tmp
        finally:
            _point_fbs_dir(original, tmp)
