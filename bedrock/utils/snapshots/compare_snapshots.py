"""Check that two snapshot prefixes hold the same outputs.

Release step A5 runs it on the canonical snapshot and the 2024 member of the year
series. Both come from one SHA and from configs that resolve to the same fields, so a
difference means the build does not reproduce on that SHA::

    uv run python -m bedrock.utils.snapshots.compare_snapshots \\
        <sha> <sha>_2026_usa_cornerstone_v1_0_2024

Every object in :data:`~bedrock.utils.snapshots.names.SNAPSHOT_NAMES` must have the
same index, columns and dtypes. Values must agree within :data:`RTOL` and
:data:`ATOL`: two CI runners can differ in the last bits of floating-point sums, and
that is not a failure to reproduce.
"""

from __future__ import annotations

import sys
import typing as ta

import click
import pandas as pd

from bedrock.utils.snapshots.loader import load_snapshot
from bedrock.utils.snapshots.names import SNAPSHOT_NAMES, SnapshotName

RTOL = 1e-9
ATOL = 1e-9


def compare_snapshots(
    key: str, other: str, names: ta.Iterable[SnapshotName] = SNAPSHOT_NAMES
) -> dict[SnapshotName, str]:
    """Each object in *names* that differs between the two prefixes, with the reason.

    An empty result means the two prefixes match.
    """
    differences: dict[SnapshotName, str] = {}
    for name in names:
        reason = _difference(load_snapshot(name, key), load_snapshot(name, other))
        if reason is not None:
            differences[name] = reason
    return differences


@click.command(help='Fail if two snapshot prefixes do not hold the same outputs.')
@click.argument('key')
@click.argument('other')
def main(key: str, other: str) -> None:
    differences = compare_snapshots(key, other)
    for name in SNAPSHOT_NAMES:
        click.echo(f'{name}: {differences.get(name, "same")}')
    if differences:
        click.echo(f'{len(differences)} of {len(SNAPSHOT_NAMES)} objects differ.')
        sys.exit(1)
    click.echo(f'All {len(SNAPSHOT_NAMES)} objects match.')


def _difference(frame: pd.DataFrame, other: pd.DataFrame) -> str | None:
    """Why *frame* and *other* differ, or ``None`` when they match."""
    try:
        pd.testing.assert_frame_equal(
            frame, other, check_exact=False, rtol=RTOL, atol=ATOL
        )
    except AssertionError as error:
        return ' '.join(str(error).split())
    return None


if __name__ == '__main__':
    main()
