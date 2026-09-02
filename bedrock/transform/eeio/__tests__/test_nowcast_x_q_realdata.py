"""Real-data acceptance: the nowcast x source is lossless at the benchmark.

Under ``2025_usa_cornerstone_v0_4_nowcast_2017`` the router loads the 2017
after-redefinition Make from the NowcastMUT store. Its row sums must equal the
published 2017 after-redef Make's within BEA's $1M publication floor, and the
Cornerstone-schema x must be that same vector carried through the schema
expansion. Together they say the switch from the gross-output series to the
Make moves nothing at 2017; anywhere else x moves only where Step 7 says.

Skips rather than fails when the 2017 after-redef artifact is not on GCS: the
v0.4 2017 YAML documents that it may be absent.
"""

from __future__ import annotations

from collections.abc import Iterator

import numpy as np
import pandas as pd
import pytest

from bedrock.extract.iot.detail_io import load_detail_V_usa
from bedrock.extract.iot.io_2017 import load_2017_V_after_redef_usa
from bedrock.publish.cache_reset import clear_all_publish_caches
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_x,
    derive_cornerstone_x_after_redefinition,
)
from bedrock.utils.config.usa_config import reset_usa_config, set_global_usa_config
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY

ATOL_USD = 1.0 * MILLION_CURRENCY_TO_CURRENCY


@pytest.fixture
def nowcast_2017_config() -> Iterator[None]:
    reset_usa_config(should_reset_env_var=True)
    clear_all_publish_caches()
    set_global_usa_config('2025_usa_cornerstone_v0_4_nowcast_2017.yaml')
    try:
        yield
    finally:
        clear_all_publish_caches()
        reset_usa_config(should_reset_env_var=True)


def _router_make_or_skip() -> pd.DataFrame:
    try:
        return load_detail_V_usa()
    except (FileNotFoundError, ValueError) as exc:
        pytest.skip(f'2017 after-redef NowcastMUT artifact unavailable: {exc}')


@pytest.mark.realdata
def test_nowcast_2017_make_x_matches_the_published_after_redef_make(
    nowcast_2017_config: None,
) -> None:
    x_router = _router_make_or_skip().sum(axis=1)
    x_published = load_2017_V_after_redef_usa().sum(axis=1)

    np.testing.assert_allclose(
        x_router.reindex(x_published.index).to_numpy(dtype=float),
        x_published.to_numpy(dtype=float),
        rtol=0.0,
        atol=ATOL_USD,
    )


@pytest.mark.realdata
def test_nowcast_2017_cornerstone_x_is_the_make_row_sum(
    nowcast_2017_config: None,
) -> None:
    x_bea = _router_make_or_skip().sum(axis=1)

    x_cs = derive_cornerstone_x_after_redefinition()

    pd.testing.assert_series_equal(x_cs, derive_cornerstone_x())
    # Schema expansion re-partitions industries (waste children, government
    # enterprises into 221100/485000) without creating or destroying output.
    assert abs(float(x_cs.sum()) - float(x_bea.sum())) <= ATOL_USD
