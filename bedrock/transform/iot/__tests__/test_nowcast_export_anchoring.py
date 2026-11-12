"""The computers family's exports sit on the published 2017 cells (#1270)."""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.transform.iot.nowcast_export_anchoring import anchor_family_exports

M = 1e6
IDX = ['334111', '334118', '334119', '336111']


def _anchor(now: list[float], base: list[float], pub: list[float]) -> pd.Series:
    return anchor_family_exports(
        pd.Series(now, index=IDX) * M,
        pd.Series(base, index=IDX) * M,
        pd.Series(pub, index=IDX) * M,
    )


def test_anchored_rows_are_published_2017_moved_by_own_growth() -> None:
    out = _anchor([10.0, 11.0, 0.0, 30.0], [8.0, 9.0, 0.0, 25.0], [7.0, 6.0, 0.0, 23.0])
    assert out['334111'] == pytest.approx(7.0 * 10.0 / 8.0 * M)
    assert out['334118'] == pytest.approx(6.0 * 11.0 / 9.0 * M)


def test_2017_reproduces_the_published_cells() -> None:
    out = _anchor([8.0, 9.0, 0.0, 25.0], [8.0, 9.0, 0.0, 25.0], [7.0, 6.0, 0.0, 23.0])
    assert out['334111'] == pytest.approx(7.0 * M)
    assert out['334118'] == pytest.approx(6.0 * M)


def test_rows_outside_the_family_keep_census() -> None:
    out = _anchor([10.0, 11.0, 0.0, 30.0], [8.0, 9.0, 0.0, 25.0], [7.0, 6.0, 0.0, 23.0])
    assert out['336111'] == pytest.approx(30.0 * M)


def test_census_only_and_published_only_rows() -> None:
    out = _anchor([10.0, 4.0, 0.0, 30.0], [8.0, 3.0, 0.0, 25.0], [7.0, 0.0, 2.0, 23.0])
    assert out['334118'] == pytest.approx(4.0 * M)  # published 2017 empty: Census
    assert out['334119'] == pytest.approx(2.0 * M)  # Census 2017 empty: held flat
