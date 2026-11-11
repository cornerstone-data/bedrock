"""The Supply seed is checked against the summary Supply workbook.

``Detail_Supply_Mix`` is cached under a git-commit version, so a cleared cache
can download a copy built from an older BEA release. These tests use small
synthetic blocks and stub the readers, so they need no network.
"""

from __future__ import annotations

import functools
import typing as ta

import pandas as pd
import pytest

import bedrock.transform.iot.nowcast_supply_go_control as gc
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY

MILLION = MILLION_CURRENCY_TO_CURRENCY


def _children(group: str, parents: dict[str, str], n: int = 2) -> list[str]:
    children = sorted(code for code, parent in parents.items() if parent == group)
    assert len(children) >= n, f'{group} has fewer than {n} detail codes'
    return children[:n]


@functools.cache
def _codes() -> tuple[list[str], list[str]]:
    """Two detail commodities and two detail industries under ``111CA``.

    Built on first use, not at import: the mappings load from GCS, and an
    import-time read breaks collection in the hermetic CI job.
    """
    return (
        _children('111CA', gc._commodity_parent()),
        _children('111CA', gc._industry_parent()),
    )


def _block(values: list[list[float]]) -> pd.DataFrame:
    """A detail block in USD from values in million USD."""
    commodities, industries = _codes()
    return pd.DataFrame(values, index=commodities, columns=industries) * MILLION


def _published(million_usd: float) -> pd.DataFrame:
    """A one-cell summary Supply table, million USD, shaped like the workbook."""
    return pd.DataFrame({'111CA': [million_usd, million_usd]}, index=['111CA', 'T017'])


def test_a_block_on_the_workbook_has_no_mismatch() -> None:
    block = _block([[10.0, 20.0], [30.0, 40.0]])
    assert gc._summary_mismatch(block, _published(100.0)).empty


def test_a_rounding_miss_under_one_million_is_not_a_mismatch() -> None:
    block = _block([[10.0, 20.0], [30.0, 40.4]])
    assert gc._summary_mismatch(block, _published(100.0)).empty


def test_a_stale_cell_is_reported_with_its_difference() -> None:
    block = _block([[10.0, 20.0], [30.0, 45.0]])
    mismatch = gc._summary_mismatch(block, _published(100.0))
    assert list(mismatch.index) == [('111CA', '111CA')]
    assert mismatch['diff_usd'].iloc[0] == pytest.approx(5.0 * MILLION)


def test_a_summary_cell_the_block_cannot_place_is_not_compared() -> None:
    block = _block([[0.0, 0.0], [0.0, 0.0]])
    assert gc._summary_mismatch(block, _published(100.0)).empty


# --- raw_supply_block: read, check, rebuild, check again --------------------


def _on_workbook() -> pd.DataFrame:
    return _block([[10.0, 20.0], [30.0, 40.0]])


def _stale() -> pd.DataFrame:
    return _block([[10.0, 20.0], [30.0, 90.0]])


@pytest.fixture
def stub_seed(
    monkeypatch: pytest.MonkeyPatch,
) -> ta.Iterator[ta.Callable[..., list[str]]]:
    """Stub the readers; return a setter that queues the blocks each read returns."""
    calls: list[str] = []

    def install(*reads: pd.DataFrame) -> list[str]:
        queue = list(reads)

        def read(method: str, download_sources_ok: bool) -> pd.DataFrame:
            calls.append(f'read:{method}:{download_sources_ok}')
            return queue.pop(0)

        def rebuild(year: int) -> None:
            calls.append(f'rebuild:{year}')

        monkeypatch.setattr(gc, '_read_supply_mix_block', read)
        monkeypatch.setattr(gc, '_rebuild_supply_mix', rebuild)
        monkeypatch.setattr(
            gc, '_published_summary_supply', lambda y: _published(100.0)
        )
        return calls

    gc.raw_supply_block.cache_clear()
    yield install
    gc.raw_supply_block.cache_clear()


def test_a_block_on_the_workbook_is_returned_without_a_rebuild(
    stub_seed: ta.Callable[..., list[str]],
) -> None:
    calls = stub_seed(_on_workbook())
    pd.testing.assert_frame_equal(gc.raw_supply_block(2022, True), _on_workbook())
    assert calls == ['read:Detail_Supply_Mix_2022:True']


def test_a_stale_block_is_rebuilt_and_read_again_locally(
    stub_seed: ta.Callable[..., list[str]],
) -> None:
    calls = stub_seed(_stale(), _on_workbook())
    pd.testing.assert_frame_equal(gc.raw_supply_block(2022, True), _on_workbook())
    assert calls == [
        'read:Detail_Supply_Mix_2022:True',
        'rebuild:2022',
        'read:Detail_Supply_Mix_2022:False',
    ]


def test_a_block_still_stale_after_the_rebuild_raises(
    stub_seed: ta.Callable[..., list[str]],
) -> None:
    calls = stub_seed(_stale(), _stale())
    with pytest.raises(ValueError, match='still disagrees'):
        gc.raw_supply_block(2022, True)
    assert calls[1] == 'rebuild:2022'
