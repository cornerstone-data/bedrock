import posixpath
from collections.abc import Mapping
from typing import Any

# TODO: update/drop? after files moved on GCS
GCS_CEDA_USA_DIR = "ceda-usa"
GCS_SNAPSHOT_DIR = posixpath.join(GCS_CEDA_USA_DIR, "snapshots")

GCS_EXTRACT_DIR = "extract"
# GCS object prefix uses hyphens; local cache uses ``extract/input_data`` and underscores
# (see ``extract_input_local``).
GCS_EXTRACT_INPUT_DIR = posixpath.join(GCS_EXTRACT_DIR, "input-data")
GCS_EXTRACT_TAXONOMY_DIR = posixpath.join(GCS_EXTRACT_INPUT_DIR, "taxonomy")
GCS_V5_INPUT_DIR = posixpath.join(GCS_EXTRACT_INPUT_DIR, "v5")


def gcs_extract_input_path(
    data_source_name: str,
    year: int | str | None = None,
) -> str:
    """
    Prefix for raw extract inputs on GCS under
    ``extract/input-data/{source}/`` with ``source`` as hyphenated keys
    (e.g. ``EIA_MECS_Energy`` → ``EIA-MECS-Energy``).

    If ``year`` is omitted, empty, or whitespace-only, there is no year subfolder
    (used e.g. for ``USA_AllTables_MakeUse``, ``USA_AllTablesSUP``, ``BEA_PriceIndex``).

    If ``year`` is set, the path is ``extract/input-data/{source}/{year}/``.
    """
    base = posixpath.join(GCS_EXTRACT_INPUT_DIR, data_source_name.replace("_", "-"))
    if year is None:
        return base
    year_str = str(year).strip()
    if not year_str:
        return base
    return posixpath.join(base, year_str)


def gcs_extract_input_sub_bucket_from_kwargs(kwargs: Mapping[str, Any]) -> str:
    """
    GCS sub-bucket under ``gcs_extract_input_path`` using ``kwargs`` shaped like
    extract pipeline calls (``source`` yaml stem, optional ``year``).
    """
    return gcs_extract_input_path(str(kwargs["source"]), kwargs.get("year"))
