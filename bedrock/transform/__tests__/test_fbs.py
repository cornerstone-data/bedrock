from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from bedrock.transform.flowbysector import FlowBySector, getFlowBySector
from bedrock.utils.config import settings
from bedrock.utils.snapshots.fbs_pin import (
    download_pinned_cornerstone_ghg_fbs,
    load_cornerstone_ghg_fbs_pin,
)
from bedrock.utils.validation.validation import compare_FBS

_SKIP_FBS_COMPARE_COLUMNS = ['ProducedBySectorType', 'ConsumedBySectorType']
# Pinned parquet may store these as object/None; regen uses float64/NaN per schema.
_NUMERIC_FBS_COMPARE_COLUMNS = ('Spread', 'Min', 'Max')


def _prepare_fbs_for_pin_compare(df: pd.DataFrame) -> pd.DataFrame:
    out = df.drop(columns=_SKIP_FBS_COMPARE_COLUMNS, errors='ignore').copy()
    for col in _NUMERIC_FBS_COMPARE_COLUMNS:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors='coerce').fillna(0.0)
    return out


@pytest.fixture
def isolated_fbs_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """Empty FBS output dir so regen cannot reuse a developer-local FBS cache.

    FBA_DIR stays at the default extract/output_data path so upstream FBAs
    load from GCS when available instead of being regenerated in an empty tmp dir.
    """
    fbs_dir = tmp_path / 'fbs'
    fbs_dir.mkdir()
    monkeypatch.setattr(settings, 'FBS_DIR', fbs_dir)
    yield fbs_dir


@pytest.mark.eeio_integration
def test_generate_cornerstone_ghg_fbs_2024_matches_pinned_reference(
    isolated_fbs_dir: Path,
) -> None:
    """Regenerated GHG_national_Cornerstone_2024 matches the pinned GCS parquet."""
    fbs_dir = isolated_fbs_dir
    pin = load_cornerstone_ghg_fbs_pin()
    method = pin['method']

    pinned_path = download_pinned_cornerstone_ghg_fbs(pin, fbs_dir)
    fbs_reference = pd.read_parquet(pinned_path)

    FlowBySector.generateFlowBySector(method, download_sources_ok=True)
    fbs_regenerated = getFlowBySector(method)

    df_m = compare_FBS(fbs_reference, fbs_regenerated, ignore_metasources=False)

    assert_frame_equal(
        _prepare_fbs_for_pin_compare(fbs_reference),
        _prepare_fbs_for_pin_compare(fbs_regenerated),
        check_like=True,
    )
    assert len(df_m) == 0
