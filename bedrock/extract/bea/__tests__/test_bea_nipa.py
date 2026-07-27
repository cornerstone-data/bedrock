"""Tests for the BEA_NIPA extractor's flat-file caching.

Synthetic archive, no network and no GCS: what is pinned down here is that the
two ways in produce the same frames, that the url path leaves the archive behind
for the next run, and that a cache miss with nothing in the bucket says what to
do about it.
"""

from __future__ import annotations

import io
import os
import typing as ta
import zipfile

import pandas as pd
import pytest

import bedrock.extract.bea.BEA_NIPA as bea_nipa

CONFIG: dict[str, ta.Any] = {
    'files': ['nipadataA.txt', 'SeriesRegister.txt', 'TablesRegister.txt']
}

SERIES_REGISTER = (
    '%SeriesCode,SeriesLabel,TableId:LineNo,SeriesCodeParents\n'
    'A000RC,"Grand total",T90900D:1,NONE\n'
)
TABLES_REGISTER = 'TableId,TableTitle\nT90900D,"Table 9.9D. Something"\n'
DATA = '%SeriesCode,Period,Value\nA000RC,2017,"1,234"\n'
#: in the archive but not in `files`, so it must not be read
UNWANTED = '%SeriesCode,Period,Value\nA000RC,2017Q1,"1"\n'


def _archive_bytes() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w') as archive:
        archive.writestr('SeriesRegister.txt', SERIES_REGISTER)
        archive.writestr('TablesRegister.txt', TABLES_REGISTER)
        archive.writestr('nipadataA.txt', DATA)
        archive.writestr('nipadataQ.txt', UNWANTED)
    return buffer.getvalue()


class _Response:
    def __init__(self, content: bytes) -> None:
        self.content = content


@pytest.fixture
def cache_dir(tmp_path: ta.Any, monkeypatch: pytest.MonkeyPatch) -> str:
    """Redirect the extract-input cache into a temporary directory."""
    directory = str(tmp_path / 'BEA_NIPA')
    os.makedirs(directory, exist_ok=True)
    monkeypatch.setattr(
        bea_nipa, 'local_extract_input_dir', lambda source, year=None: directory
    )
    return directory


def _codes(frames: list[pd.DataFrame]) -> set[str]:
    return {c for frame in frames for c in frame.columns}


def test_call_caches_the_archive_and_reads_it(cache_dir: str) -> None:
    frames = bea_nipa.bea_nipa_call(
        resp=_Response(_archive_bytes()), source='BEA_NIPA', config=CONFIG
    )
    assert os.path.exists(os.path.join(cache_dir, 'FlatFiles.ZIP')), (
        'the url path must leave the archive behind, or nothing is cached'
    )
    assert len(frames) == 3, 'only the three files named in the yaml'
    # the columns the parser identifies each frame by
    assert {'SeriesCode', 'Table_and_Line', 'TableTitle', 'Period'} <= _codes(frames)


def test_load_gcs_reads_the_cache_without_touching_gcs(
    cache_dir: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail(*args: object, **kwargs: object) -> str:
        raise AssertionError('GCS was called even though the archive was cached')

    monkeypatch.setattr(bea_nipa, 'download_extract_input_from_gcs_if_not_exists', fail)
    with open(os.path.join(cache_dir, 'FlatFiles.ZIP'), 'wb') as f:
        f.write(_archive_bytes())

    frames = bea_nipa.bea_nipa_load_gcs(
        source='BEA_NIPA', year=None, config=CONFIG, url='https://example/x.ZIP'
    )
    assert len(frames) == 3


def test_both_paths_produce_the_same_frames(
    cache_dir: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from_url = bea_nipa.bea_nipa_call(
        resp=_Response(_archive_bytes()), source='BEA_NIPA', config=CONFIG
    )
    monkeypatch.setattr(
        bea_nipa,
        'download_extract_input_from_gcs_if_not_exists',
        lambda *a, **k: '',
    )
    from_cache = bea_nipa.bea_nipa_load_gcs(source='BEA_NIPA', year=None, config=CONFIG)
    for a, b in zip(from_url, from_cache):
        pd.testing.assert_frame_equal(a, b)


def test_missing_everywhere_says_how_to_fetch_it(
    cache_dir: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # a bucket with nothing in it: the helper swallows the miss and returns,
    # leaving the caller to notice there is still no file
    monkeypatch.setattr(
        bea_nipa,
        'download_extract_input_from_gcs_if_not_exists',
        lambda *a, **k: '',
    )
    with pytest.raises(FileNotFoundError) as excinfo:
        bea_nipa.bea_nipa_load_gcs(source='BEA_NIPA', year=None, config=CONFIG)
    message = str(excinfo.value)
    assert 'FlatFiles.ZIP' in message
    assert 'extract/input-data/BEA_NIPA' in message, 'name the bucket path'
    assert 'extract_data_from_raw_sources' in message, 'and the way out'


def test_local_path_sits_under_the_source_name() -> None:
    path = bea_nipa.flat_files_local_path()
    assert os.path.basename(path) == 'FlatFiles.ZIP'
    assert os.path.basename(os.path.dirname(path)) == 'BEA_NIPA'
    # the archive compare_NIPA_to_IOT reads, so the two must agree on one path
    assert os.path.basename(os.path.dirname(os.path.dirname(path))) == 'input_data'
