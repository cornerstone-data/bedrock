import posixpath
from collections.abc import Mapping
from typing import Any

# TODO: update/drop? after files moved on GCS
GCS_CEDA_USA_DIR = "ceda-usa"
GCS_CEDA_INPUT_DIR = posixpath.join(GCS_CEDA_USA_DIR, "input")
GCS_CEDA_V5_INPUT_DIR = posixpath.join(GCS_CEDA_INPUT_DIR, "v5")

GCS_EXTRACT_DIR = "extract"
GCS_EXTRACT_INPUT_DIR = posixpath.join(GCS_EXTRACT_DIR, "input_data")


def gcs_extract_input_path(
    data_source_name: str,
    year: int | str | None = None,
) -> str:
    """
    Prefix for raw extract inputs on GCS under ``extract/input_data/{data_source_name}/``.

    If ``year`` is omitted, empty, or whitespace-only, there is no year subfolder
    (used e.g. for ``USA_AllTables_MakeUse``, ``USA_AllTablesSUP``, ``BEA_PriceIndex``).

    If ``year`` is set, the path is ``extract/input_data/{data_source_name}/{year}/``.
    """
    base = posixpath.join(GCS_EXTRACT_INPUT_DIR, data_source_name)
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


