"""PR1.1: materializer smoke + writer parity (extract-owned; real ``derive_*``)."""

from __future__ import annotations

from collections.abc import Iterator

import pandas as pd
import pytest

from bedrock.extract.disaggregation.electricity_disagg_cornerstone_materializer import (
    intermediate_use_totals,
    materialize_electricity_disagg_cornerstone_frames,
)
from bedrock.publish.__tests__._helpers import setup_config, teardown
from bedrock.publish.excel import writer as writer_module
from bedrock.transform.eeio.derived_cornerstone import get_waste_disagg_weights


@pytest.fixture
def cornerstone_full_model_config() -> Iterator[None]:
    setup_config('2025_usa_cornerstone_full_model.yaml')
    yield
    teardown()


@pytest.mark.skipif(
    get_waste_disagg_weights() is None,
    reason='Waste disaggregation weight CSVs not available',
)
def test_materialize_electricity_disagg_cornerstone_frames_smoke(
    cornerstone_full_model_config: object,
) -> None:
    writer_module.clear_publish_caches()
    frames = materialize_electricity_disagg_cornerstone_frames()
    assert set(frames) == {'V', 'Udom', 'Uimp', 'VA', 'Y', 'E'}
    V, Udom, Uimp, VA, Y, E = (
        frames['V'],
        frames['Udom'],
        frames['Uimp'],
        frames['VA'],
        frames['Y'],
        frames['E'],
    )
    assert Udom.shape == Uimp.shape
    assert Udom.index.equals(Uimp.index)
    assert Udom.columns.equals(V.index)
    assert Udom.index.equals(V.columns)
    assert VA.shape[1] == V.shape[0]
    assert Y.index.equals(Udom.index)
    assert E.shape[1] == V.shape[0]
    assert E.columns.equals(V.index)


@pytest.mark.skipif(
    get_waste_disagg_weights() is None,
    reason='Waste disaggregation weight CSVs not available',
)
def test_intermediate_use_totals_matches_u_dom_plus_u_imp(
    cornerstone_full_model_config: object,
) -> None:
    writer_module.clear_publish_caches()
    frames = materialize_electricity_disagg_cornerstone_frames()
    tot = intermediate_use_totals()
    pd.testing.assert_frame_equal(
        tot, frames['Udom'] + frames['Uimp'], check_names=True, rtol=0.0, atol=0.0
    )


@pytest.mark.skipif(
    get_waste_disagg_weights() is None,
    reason='Waste disaggregation weight CSVs not available',
)
def test_writer_getters_match_materializer_v_and_u_intermediate(
    cornerstone_full_model_config: object,
) -> None:
    writer_module.clear_publish_caches()
    frames = materialize_electricity_disagg_cornerstone_frames()
    pd.testing.assert_frame_equal(
        writer_module._get_V(),
        frames['V'],
        check_names=True,
        rtol=0.0,
        atol=0.0,
        check_flags=False,
    )
    extended_u = writer_module._get_U()
    n_r, n_c = frames['Udom'].shape
    pd.testing.assert_frame_equal(
        extended_u.iloc[:n_r, :n_c],
        frames['Udom'] + frames['Uimp'],
        check_names=True,
        rtol=0.0,
        atol=0.0,
        check_flags=False,
    )
    extended_ud = writer_module._get_Udom()
    assert extended_ud.iloc[:n_r, :n_c].equals(frames['Udom'])
