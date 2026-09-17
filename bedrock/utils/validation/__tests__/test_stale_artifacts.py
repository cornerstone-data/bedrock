"""``delete_stale`` deletes the build that is stale, not every build sharing
its name.

The failure this pins is the one that makes the whole sweep worse than useless:
a rebuild sits beside the stale artifact it replaces and shares its
``name_data``.  The rebuild is *not* in ``problems`` -- it is current, which is
the point of having rebuilt it -- but matching on ``f"{name}_v"`` matched it
anyway, so ``--delete`` removed the fix and kept nothing.  Caught on a real
sweep of 60 superseded ``UMD_GHGIA`` artifacts that would have taken 60 fresh
ones with them.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from bedrock.utils.validation import stale_artifacts as sa

STALE = 'Foo_2022_v0.3.0_aaaaaaa'
FRESH = 'Foo_2022_v0.3.0_bbbbbbb'


def _artifact(directory: Path, stem: str) -> None:
    (directory / f'{stem}.parquet').write_bytes(b'not really parquet')
    (directory / f'{stem}_metadata.json').write_text('{}', encoding='utf-8')


@pytest.fixture
def scope(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    out = tmp_path / 'output_data'
    out.mkdir()
    monkeypatch.setitem(sa.SCOPES, 'transform', (out,))
    return out


def test_deletes_only_the_flagged_version(scope: Path) -> None:
    _artifact(scope, STALE)
    _artifact(scope, FRESH)

    removed = sa.delete_stale([{'artifact': 'Foo_2022', 'stem': STALE}], 'transform')

    assert {p.name for p in removed} == {
        f'{STALE}.parquet',
        f'{STALE}_metadata.json',
    }
    # the whole point: the rebuild survives
    assert (scope / f'{FRESH}.parquet').exists()
    assert (scope / f'{FRESH}_metadata.json').exists()


def test_removes_the_parquet_and_its_sidecar_together(scope: Path) -> None:
    """Leaving the parquet behind means the rebuild silently reloads it."""
    _artifact(scope, STALE)

    sa.delete_stale([{'artifact': 'Foo_2022', 'stem': STALE}], 'transform')

    assert not (scope / f'{STALE}.parquet').exists()
    assert not (scope / f'{STALE}_metadata.json').exists()


def test_a_row_without_a_stem_deletes_nothing(scope: Path) -> None:
    """No stem is "I cannot tell which build", which must not mean "all of them".

    Rows are built by :func:`find_stale`, but a caller assembling them by hand
    should not be able to wipe a name by omitting the field.
    """
    _artifact(scope, STALE)
    _artifact(scope, FRESH)

    removed = sa.delete_stale([{'artifact': 'Foo_2022'}], 'transform')

    assert removed == []
    assert (scope / f'{STALE}.parquet').exists()
    assert (scope / f'{FRESH}.parquet').exists()


def test_find_stale_rows_carry_the_versioned_stem() -> None:
    """The contract ``delete_stale`` depends on, held at the producing end."""
    problems = sa.find_stale('')
    assert problems, 'expected at least one problem on this tree'
    for row in problems:
        assert row.get('stem'), f'row without a stem: {row}'
        # a stem is the versioned filename, so it is not the bare name_data
        assert row['stem'].startswith(row['artifact'])
