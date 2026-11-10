"""Real-data acceptance: the nowcast x source at the 2017 anchor.

Under ``2025_usa_cornerstone_v0_4_nowcast_2017`` the router loads the 2017
after-redefinition Make from the NowcastMUT store. That table is the
**balanced pipeline product**, not the published table: the balance holds
published totals and identities while its cells deviate where the seeds'
evidence says so, and the pipeline's redefinition pattern is applied to those
balanced cells. So the anchor contract is *tracks*, not *equals*: row-sum
totals within 0.05% of the published after-redef Make (measured 0.009%) and
a value-weighted per-industry difference within 1% (measured 0.25%).

The schema contract is conservation net of the one deliberate drop: the
commodity correspondence weights scrap (``S00401``) to zero - the
established treatment, compensated by the scrap-corrected market-share
matrix - so Cornerstone x must equal the Make row-sum total minus exactly
the mass the correspondence's weights discard, to the $1M floor.

Skips rather than fails when the 2017 after-redef artifact is not on GCS.

⚠️ **The build is pinned here, not resolved as the latest upload.** The config
omits ``nowcast_mut_vintage``, so the loader takes whatever 2017 Make was
uploaded last. On 2026-11-09 that became ``v1.0.0_e5fa35b``, which
deliberately moves 2017 electricity output to the EIA level (now the v1.0
build). Its 2017 totals sit $57bn (0.17%) off the published Make, and the test
failed on every branch. :data:`PINNED_VINTAGE` is the last v1.0 build with
electricity output on BEA's 2017 level.
"""

from __future__ import annotations

from collections.abc import Iterator

import numpy as np
import pandas as pd
import pytest

from bedrock.extract.iot.detail_io import load_detail_V_usa
from bedrock.extract.iot.io_2017 import load_2017_V_after_redef_usa
from bedrock.publish.cache_reset import clear_all_publish_caches
from bedrock.transform.eeio.usa_expansion import commodity_corresp
from bedrock.transform.eeio.usa_matrices import (
    derive_x,
    derive_x_after_redefinition,
)
from bedrock.utils.config.usa_config import (
    _load_usa_config_from_file_name,
    reset_usa_config,
    set_global_usa_config_object,
)
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY

ATOL_USD = 1.0 * MILLION_CURRENCY_TO_CURRENCY

CONFIG = '2025_usa_cornerstone_v0_4_nowcast_2017.yaml'

#: The v1.0 release build, as pinned in ``2026_usa_cornerstone_v1_0_2017``.
PINNED_VINTAGE = 'v1.0.0_b17908f'


@pytest.fixture
def nowcast_2017_config() -> Iterator[None]:
    reset_usa_config(should_reset_env_var=True)
    clear_all_publish_caches()
    config = _load_usa_config_from_file_name(CONFIG).model_copy(
        update={'nowcast_mut_vintage': PINNED_VINTAGE}
    )
    set_global_usa_config_object(config, source_label=CONFIG)
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
def test_nowcast_2017_make_x_tracks_the_published_after_redef_make(
    nowcast_2017_config: None,
) -> None:
    x_router = _router_make_or_skip().sum(axis=1)
    x_published = load_2017_V_after_redef_usa().sum(axis=1)
    ours = x_router.reindex(x_published.index).to_numpy(dtype=float)
    published = x_published.to_numpy(dtype=float)

    total_gap = abs(float(ours.sum()) - float(published.sum()))
    assert total_gap <= 0.0005 * float(published.sum()), (
        f'2017 anchor totals diverged: ${total_gap / 1e9:.2f}B '
        f'({total_gap / published.sum():.4%}); measured 0.009% when set'
    )
    weighted_l1 = float(np.abs(ours - published).sum()) / float(published.sum())
    assert weighted_l1 <= 0.01, (
        f'2017 anchor weighted per-industry difference {weighted_l1:.3%} '
        'exceeds 1%; measured 0.25% when set'
    )


@pytest.mark.realdata
def test_nowcast_2017_cornerstone_x_is_the_make_row_sum(
    nowcast_2017_config: None,
) -> None:
    make = _router_make_or_skip()
    x_bea = make.sum(axis=1)

    x_cs = derive_x_after_redefinition()

    pd.testing.assert_series_equal(x_cs, derive_x())
    # Schema expansion re-partitions industries (waste children, government
    # enterprises into 221100/485000) without creating or destroying output,
    # net of the one deliberate drop: commodity-correspondence weights below
    # one discard that share of the column (scrap S00401 at weight zero).
    weights = commodity_corresp().sum(axis=0)
    discarded = sum(
        float(make[str(code)].sum()) * (1.0 - float(weight))
        for code, weight in weights.items()
        if str(code) in make.columns and abs(weight - 1.0) > 1e-9
    )
    expected = float(x_bea.sum()) - discarded
    assert abs(float(x_cs.sum()) - expected) <= ATOL_USD
