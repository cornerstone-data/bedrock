"""No test in this directory may rebuild an FBA from a live source.

These are ``realdata`` tests and they are meant to be: the defects they guard
are properties of the real sources, not of a synthetic frame. But *reading* a
published artifact and *rebuilding one from a vendor endpoint* are different
dependencies, and only the second makes the suite a client of somebody else's
website at test time.

⚠️ **What that cost, once.** ``getFlowByActivity`` defaults to
``download_FBA_if_missing=False``, so its load order is *local, then generate*.
On a cache miss it therefore rebuilt ``Census_AIES`` from census.gov mid-run,
the endpoint returned something that was not a workbook, and **108 tests failed
with** ``ValueError: Excel file format cannot be determined, you must specify an
engine manually`` -- raised from inside ``pandas``, with nothing in the message
naming Census, the network, or the source. The same commit's parent had passed
minutes earlier with the artifact already cached. A network hiccup read as a
logic regression for half an hour.

So generation is switched off here. The load order becomes *local, then the
published GCS artifact*, and if neither has it the test **skips with the source
named** rather than reaching for the network. A skip that says which artifact is
missing is worth more than a red suite that says a workbook could not be parsed.

⚠️ This is scoped to this directory on purpose. ``bedrock/extract`` has tests
whose subject *is* generation, and they must keep it.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, NoReturn

import pytest

from bedrock.extract import flowbyactivity


@pytest.fixture(autouse=True, scope='session')
def _no_live_source_rebuilds() -> Iterator[None]:
    """Replace FBA generation with a skip that names the missing artifact."""
    original = flowbyactivity.generateFlowByActivity

    def _refuse(*_args: Any, **kwargs: Any) -> NoReturn:
        source = kwargs.get('source', '<unknown source>')
        year = kwargs.get('year', '<unknown year>')
        pytest.skip(
            f'{source} {year} is not cached locally and was not in GCS, and '
            f'these tests do not rebuild an FBA from its live source. Seed it '
            f'with getFlowByActivity({source!r}, {year!r}, '
            f'download_FBA_if_missing=True), or publish the artifact.'
        )

    flowbyactivity.generateFlowByActivity = _refuse
    try:
        yield
    finally:
        flowbyactivity.generateFlowByActivity = original
