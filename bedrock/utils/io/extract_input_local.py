"""
Local cache layout under ``bedrock/extract/input_data``, mirroring GCS keys under
``extract/input_data`` (``{source}/`` or ``{source}/{year}/`` when a year is used).
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any

from bedrock.utils.io.gcp_paths import GCS_EXTRACT_INPUT_DIR, gcs_extract_input_path

# bedrock/utils/io/_file_-> bedrock package root
_BEDROCK_PKG = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

EXTRACT_INPUT_DATA_ROOT = os.path.join(_BEDROCK_PKG, "extract", "input_data")


def local_extract_input_dir(
    data_source_name: str,
    year: int | str | None = None,
) -> str:
    """
    Local directory matching ``gcs_extract_input_path`` (same ``source`` / ``year`` rules).
    """
    rel = gcs_extract_input_path(data_source_name, year)
    return local_dir_for_gcs_sub_bucket(rel)


def load_local_extract_input_dir(kwargs: Mapping[str, Any]) -> str:
    """Local dir for extract kwargs with ``source`` and optional ``year``."""
    return local_extract_input_dir(str(kwargs["source"]), kwargs.get("year"))


def local_dir_for_gcs_sub_bucket(gcs_sub_bucket: str) -> str:
    """
    Map a GCS object prefix to a path under ``EXTRACT_INPUT_DATA_ROOT``.

    If ``gcs_sub_bucket`` starts with ``GCS_EXTRACT_INPUT_DIR``, that prefix is
    stripped and the remainder is joined under ``EXTRACT_INPUT_DATA_ROOT``.
    Otherwise the full posix path is joined under the root as given.
    """
    norm = gcs_sub_bucket.strip("/").replace("\\", "/")
    prefix = GCS_EXTRACT_INPUT_DIR.strip("/")
    if norm == prefix:
        return EXTRACT_INPUT_DATA_ROOT
    if norm.startswith(prefix + "/"):
        rel = norm[len(prefix) + 1 :]
    else:
        rel = norm
    parts = [p for p in rel.split("/") if p]
    return os.path.join(EXTRACT_INPUT_DATA_ROOT, *parts) if parts else EXTRACT_INPUT_DATA_ROOT
