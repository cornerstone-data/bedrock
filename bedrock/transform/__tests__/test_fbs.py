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


@pytest.fixture
def isolated_flow_dirs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[Path]:
    """Empty FBA/FBS output dirs so regen cannot reuse developer-local caches."""
    fba_dir = tmp_path / 'fba'
    fbs_dir = tmp_path / 'fbs'
    fba_dir.mkdir()
    fbs_dir.mkdir()
    monkeypatch.setattr(settings, 'FBA_DIR', fba_dir)
    monkeypatch.setattr(settings, 'FBS_DIR', fbs_dir)
    yield fbs_dir


@pytest.mark.eeio_integration
def test_generate_cornerstone_ghg_fbs_2024_matches_pinned_reference(
    isolated_flow_dirs: Path,
) -> None:
    """Regenerated GHG_national_Cornerstone_2024 matches the pinned GCS parquet."""
    fbs_dir = isolated_flow_dirs
    pin = load_cornerstone_ghg_fbs_pin()
    method = pin['method']

    pinned_path = download_pinned_cornerstone_ghg_fbs(pin, fbs_dir)
    fbs_reference = pd.read_parquet(pinned_path)

    FlowBySector.generateFlowBySector(method, download_sources_ok=True)
    fbs_regenerated = getFlowBySector(method)

    df_m = compare_FBS(fbs_reference, fbs_regenerated, ignore_metasources=False)

    assert_frame_equal(
        fbs_reference.drop(columns=_SKIP_FBS_COMPARE_COLUMNS, errors='ignore'),
        fbs_regenerated.drop(columns=_SKIP_FBS_COMPARE_COLUMNS, errors='ignore'),
        check_like=True,
    )
    assert len(df_m) == 0
