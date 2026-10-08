"""Weekday integration: rebuilt 2024 balanced Supply and Use match the pin."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

import bedrock.utils.config.common as common
from bedrock.transform.iot.nowcast_sut_assembly import (
    balance_year,
    prepared_balanced_blocks,
)
from bedrock.utils.config.usa_config import reset_usa_config, set_global_usa_config
from bedrock.utils.snapshots.nowcast_balanced_sut_pin import (
    PIN_BLOCKS,
    PIN_USA_CONFIG,
    PIN_YEAR,
    clear_bedrock_caches,
    download_pinned_nowcast_balanced,
    isolated_fbs_dir,
    load_nowcast_balanced_pin,
)


@pytest.fixture
def nowcast_balanced_pin_inputs() -> Iterator[None]:
    """v0.5 methodology flags and an empty FBS directory for one rebuild."""
    reset_usa_config(should_reset_env_var=True)
    set_global_usa_config(PIN_USA_CONFIG)
    common.download_fba_on_api_error = True
    try:
        with isolated_fbs_dir():
            clear_bedrock_caches()
            yield
    finally:
        reset_usa_config(should_reset_env_var=True)
        common.download_fba_on_api_error = False


@pytest.mark.nowcast_integration
def test_balance_2024_matches_pinned_sut(
    tmp_path: Path,
    nowcast_balanced_pin_inputs: None,
) -> None:
    """Regenerated 2024 balanced Supply and Use match the pinned parquets.

    The pin records the filenames and SHA256. It does not follow
    ``nowcast_mut_vintage`` or ``.SNAPSHOT_KEY``. Comparison uses the frames
    ``save_balance`` writes, including the Use residue sweep.
    """
    assert nowcast_balanced_pin_inputs is None
    pin = load_nowcast_balanced_pin()
    pinned_paths = download_pinned_nowcast_balanced(pin, tmp_path)
    balanced = balance_year(PIN_YEAR)
    prepared = prepared_balanced_blocks(balanced)

    for block in PIN_BLOCKS:
        reference = pd.read_parquet(pinned_paths[block])
        rebuilt, _n_swept = prepared[block]
        assert_frame_equal(reference, rebuilt)
