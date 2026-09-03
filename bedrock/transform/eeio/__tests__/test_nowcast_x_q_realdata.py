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
"""

from __future__ import annotations

from collections.abc import Iterator

import numpy as np
import pandas as pd
import pytest

from bedrock.extract.iot.detail_io import load_detail_V_usa
from bedrock.extract.iot.io_2017 import load_2017_V_after_redef_usa
from bedrock.publish.cache_reset import clear_all_publish_caches
from bedrock.transform.eeio.cornerstone_expansion import commodity_corresp
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

    x_cs = derive_cornerstone_x_after_redefinition()

    pd.testing.assert_series_equal(x_cs, derive_cornerstone_x())
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
