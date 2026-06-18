"""USEEIOR v1.8.0 waste disaggregation weight CSVs (remote fetch + local cache)."""

from __future__ import annotations

import functools
import pathlib
import urllib.request

from bedrock.extract.disaggregation.waste_weight_config import WASTE_INPUTS_REL
from bedrock.utils.config.usa_config import EEIOWasteDisaggConfig

_DISAGG_ROOT = pathlib.Path(__file__).resolve().parent

USEEIOR_V180_SUBDIR = "useeior_v1.8.0"
USEEIOR_V180_REL_PREFIX = f"{WASTE_INPUTS_REL}/{USEEIOR_V180_SUBDIR}"
USEEIOR_V180_INPUT_DIR = _DISAGG_ROOT / "waste_disagg_inputs" / USEEIOR_V180_SUBDIR

USEEIOR_V180_USE_FILENAME = "WasteDisaggregationDetail2017_Use.csv"
USEEIOR_V180_MAKE_FILENAME = "WasteDisaggregationDetail2017_Make.csv"

USEEIOR_V180_WASTE_USE_URL = (
    "https://raw.githubusercontent.com/cornerstone-data/useeior/v1.8.0/"
    "inst/extdata/disaggspecs/WasteDisaggregationDetail2017_Use.csv"
)
USEEIOR_V180_WASTE_MAKE_URL = (
    "https://raw.githubusercontent.com/cornerstone-data/useeior/v1.8.0/"
    "inst/extdata/disaggspecs/WasteDisaggregationDetail2017_Make.csv"
)
_URL_TO_FILENAME: dict[str, str] = {
    USEEIOR_V180_WASTE_USE_URL: USEEIOR_V180_USE_FILENAME,
    USEEIOR_V180_WASTE_MAKE_URL: USEEIOR_V180_MAKE_FILENAME,
}
USEEIOR_V180_WASTE_SOURCE_NAME = "useeior_v1.8.0_WasteDisaggregationDetail2017"
USEEIOR_V180_WASTE_YEAR = 2017


@functools.cache
def download_waste_weights_to_cache(url: str) -> str:
    """Ensure a USEEIOR v1.8.0 weight CSV exists under waste_disagg_inputs."""
    filename = _URL_TO_FILENAME.get(url) or pathlib.Path(url).name
    local_path = USEEIOR_V180_INPUT_DIR / filename
    if local_path.is_file():
        return str(local_path)
    USEEIOR_V180_INPUT_DIR.mkdir(parents=True, exist_ok=True)
    try:
        urllib.request.urlretrieve(url, str(local_path))
    except Exception as exc:
        raise RuntimeError(
            f"Failed downloading waste disaggregation file from {url} to {local_path}. "
            "Check network access or copy the CSVs into "
            f"{USEEIOR_V180_INPUT_DIR}."
        ) from exc
    return str(local_path)


def _ensure_useeior_v1_8_weight_files() -> None:
    download_waste_weights_to_cache(USEEIOR_V180_WASTE_USE_URL)
    download_waste_weights_to_cache(USEEIOR_V180_WASTE_MAKE_URL)


def useeior_v1_8_waste_disagg_config() -> EEIOWasteDisaggConfig:
    """Weight file paths and metadata for USEEIOR v1.8.0 waste disagg specs."""
    _ensure_useeior_v1_8_weight_files()
    return EEIOWasteDisaggConfig(
        use_weights_file=f"{USEEIOR_V180_REL_PREFIX}/{USEEIOR_V180_USE_FILENAME}",
        make_weights_file=f"{USEEIOR_V180_REL_PREFIX}/{USEEIOR_V180_MAKE_FILENAME}",
        year=USEEIOR_V180_WASTE_YEAR,
        source_name=USEEIOR_V180_WASTE_SOURCE_NAME,
    )
