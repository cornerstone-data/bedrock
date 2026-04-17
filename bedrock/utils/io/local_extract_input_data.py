"""
Local cache under ``bedrock/extract/input_data`` with **underscore** ``source``
stems (e.g. ``EIA_MECS_Energy``). GCS uses ``extract/input-data`` and hyphenated
names; see ``local_dir_for_gcs_sub_bucket`` to map a GCS prefix to this layout.
"""

from __future__ import annotations

import os
import posixpath
from collections.abc import Mapping
from typing import Any

from bedrock.utils.io.gcp_paths import GCS_EXTRACT_INPUT_DIR

# bedrock/utils/io/_file_-> bedrock package root
_BEDROCK_PKG = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

EXTRACT_INPUT_DATA_ROOT = os.path.join(_BEDROCK_PKG, "extract", "input_data")


def _local_extract_input_relpath(
    data_source_name: str,
    year: int | str | None = None,
) -> str:
    """Relative path under ``EXTRACT_INPUT_DATA_ROOT`` using yaml-style underscores."""
    if year is None:
        return data_source_name
    year_str = str(year).strip()
    if not year_str:
        return data_source_name
    return posixpath.join(data_source_name, year_str)


def local_extract_input_dir(
    data_source_name: str,
    year: int | str | None = None,
) -> str:
    """
    Local directory for ``data_source_name`` / optional ``year``

    Ensures the directory exists (``os.makedirs(..., exist_ok=True)``).
    """
    rel = _local_extract_input_relpath(data_source_name, year)
    parts = [p for p in rel.replace("\\", "/").split("/") if p]
    pth = os.path.join(EXTRACT_INPUT_DATA_ROOT, *parts)
    os.makedirs(pth, exist_ok=True)
    return pth


def load_local_extract_input_dir(kwargs: Mapping[str, Any]) -> str:
    """Local dir for extract kwargs with ``source`` and optional ``year``."""
    return local_extract_input_dir(str(kwargs["source"]), kwargs.get("year"))


def local_dir_for_gcs_sub_bucket(gcs_sub_bucket: str) -> str:
    """
    Map a GCS object prefix to a path under ``EXTRACT_INPUT_DATA_ROOT``.

    For prefixes under ``extract/input-data/``, path segments after that prefix
    are converted for local use (hyphens in source keys → underscores). Year
    segments are unchanged.

    Other prefixes (e.g. ``ceda-usa/...``) are appended under
    ``EXTRACT_INPUT_DATA_ROOT`` without segment rewriting.
    """
    norm = gcs_sub_bucket.strip("/").replace("\\", "/")
    prefix = GCS_EXTRACT_INPUT_DIR.strip("/")
    if norm == prefix:
        pth = EXTRACT_INPUT_DATA_ROOT
    elif norm.startswith(prefix + "/"):
        rel = norm[len(prefix) + 1 :]
        parts = [p for p in rel.split("/") if p]
        local_parts = [p.replace("-", "_") for p in parts]
        pth = (
            os.path.join(EXTRACT_INPUT_DATA_ROOT, *local_parts)
            if local_parts
            else EXTRACT_INPUT_DATA_ROOT
        )
    else:
        rel = norm
        parts = [p for p in rel.split("/") if p]
        pth = (
            os.path.join(EXTRACT_INPUT_DATA_ROOT, *parts)
            if parts
            else EXTRACT_INPUT_DATA_ROOT
        )
    os.makedirs(pth, exist_ok=True)
    return pth
