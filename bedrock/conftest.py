"""Marks the test suite's real-data tests so CI can run the hermetic ones first.

Most of this suite is deliberately built on real sources rather than synthetic
frames - see the header of ``transform/iot/__tests__/test_nowcast_product_taxes``
for why. That is the right call for what the tests guard, and it is also why the
``test`` job takes minutes: the time goes into fetching and building FBAs and
FBSs, not into the assertions.

The split here is for *feedback*, not for a shorter gate. ``test-fast`` runs the
hermetic tests in about a minute with no GCS credentials, so a broken import, a
type error or a bad kernel change reports back long before the full run finishes.
``test`` still runs everything, so nothing is weakened by a test being classified
wrongly here.

⚠️ **The default is ``realdata``, and that direction is deliberate.** A new test
is slow until someone has checked it is not, so mis-classification costs a minute
of CI rather than a gap in the gate. :data:`_HERMETIC` is the allowlist, and it is
meant to grow as directories are verified.
"""

from __future__ import annotations

from pathlib import Path

import pytest

#: Directories whose tests build their own frames and touch no source. Verified
#: by reading their imports: they take types and pure functions, never a loader.
_HERMETIC: tuple[str, ...] = (
    'bedrock/utils/economic/balance/__tests__',
    'bedrock/utils/taxonomy/bea/__tests__',
    'bedrock/utils/taxonomy/mappings/__tests__',
    'bedrock/utils/io/__tests__',
    'bedrock/utils/mapping/__tests__',
    'bedrock/utils/config/__tests__',
)

_ROOT = Path(__file__).resolve().parent.parent


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Add ``realdata`` to every test outside :data:`_HERMETIC`."""
    for item in items:
        try:
            rel = Path(str(item.fspath)).resolve().relative_to(_ROOT).as_posix()
        except ValueError:  # pragma: no cover - a test collected from outside the repo
            rel = ''
        if not rel.startswith(_HERMETIC):
            item.add_marker(pytest.mark.realdata)
