"""Pinned FlowBySector parquets for integration tests."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from bedrock.utils.io.gcp import download_gcs_file

_SNAPSHOT_BASE = Path(__file__).resolve().parent

#: Regen golden for ``GHG_national_Cornerstone_nowcast_facilities_2024``.
DEFAULT_FBS_PIN = _SNAPSHOT_BASE / 'cornerstone_ghg_fbs_2024_pin.json'


def load_fbs_pin(pin_json_path: str | Path | None = None) -> dict[str, str]:
    """Load a committed FBS pin (method, GCS location, filename, SHA256)."""
    path = Path(pin_json_path or DEFAULT_FBS_PIN)
    raw = json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(raw, dict):
        raise ValueError(f'FBS pin JSON must be an object, got {type(raw).__name__}')
    required = ('method', 'gcs_sub_bucket', 'filename', 'sha256')
    missing = [k for k in required if k not in raw]
    if missing:
        raise ValueError(f'Missing keys {missing!r} in FBS pin JSON {path!r}')
    out = {k: str(raw[k]).strip() for k in required}
    exp = out['sha256'].lower()
    if len(exp) != 64 or any(c not in '0123456789abcdef' for c in exp):
        raise ValueError('sha256 must be a 64-char lowercase hex string')
    if exp == '0' * 64:
        raise ValueError(
            f'FBS pin {path!r} still has a placeholder sha256; set it after the '
            'blessed parquet is on GCS'
        )
    out['sha256'] = exp
    return out


def _file_sha256_hex(path: str | Path) -> str:
    digest = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            digest.update(chunk)
    return digest.hexdigest()


def download_pinned_fbs(
    pin: dict[str, str],
    local_dir: str | Path,
) -> Path:
    """Download the pinned parquet and verify SHA256 before returning the path."""
    local_dir = Path(local_dir)
    local_dir.mkdir(parents=True, exist_ok=True)
    local_path = local_dir / pin['filename']
    if local_path.is_file():
        got = _file_sha256_hex(local_path)
        if got.lower() != pin['sha256']:
            local_path.unlink()
    if not local_path.is_file():
        download_gcs_file(pin['filename'], pin['gcs_sub_bucket'], str(local_path))
    got = _file_sha256_hex(local_path)
    if got.lower() != pin['sha256']:
        raise ValueError(
            f'Pinned FBS SHA256 mismatch for {pin["filename"]!r}: '
            f'got {got}, expected {pin["sha256"]}'
        )
    return local_path


# Legacy import names used by analysis scripts.
load_cornerstone_ghg_fbs_pin = load_fbs_pin
download_pinned_cornerstone_ghg_fbs = download_pinned_fbs
