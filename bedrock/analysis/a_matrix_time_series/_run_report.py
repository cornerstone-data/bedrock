"""Shared helper for appending tabs to the analysis run-report Sheet.

The run-report Sheet ID is written by ``derive_A_time_series`` (Step 1)
to ``LAST_RUN_SHEET_ID_PATH``. Every later step appends one or more
result tabs to that Sheet. Each step previously inlined the same
guard / read / try-update / log block; this module is the single
canonical version.
"""

from __future__ import annotations

import logging

import pandas as pd

from bedrock.analysis.a_matrix_time_series.constants import LAST_RUN_SHEET_ID_PATH
from bedrock.utils.io.gcp import update_sheet_tab

logger = logging.getLogger(__name__)


def publish_tabs(tabs: dict[str, pd.DataFrame]) -> None:
    """Append ``tabs`` to the run-report Sheet, no-op if it doesn't exist.

    Skipped with a warning if ``LAST_RUN_SHEET_ID_PATH`` is missing (Step 1
    hasn't run, or it ran without Drive auth). Network errors during the
    update are caught and logged so the local CSV/PNG artifacts always
    succeed even when Sheet publishing fails.
    """
    if not LAST_RUN_SHEET_ID_PATH.exists():
        logger.warning(
            "No %s found — skipping Sheet publish. Run derive_A_time_series "
            "first (with valid Drive auth) to create the run report.",
            LAST_RUN_SHEET_ID_PATH,
        )
        return
    sheet_id = LAST_RUN_SHEET_ID_PATH.read_text().strip()
    try:
        for tab_name, df in tabs.items():
            update_sheet_tab(sheet_id, tab_name, df)
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "Sheet publish skipped (%s: %s). Local artifacts still complete.",
            type(e).__name__,
            e,
        )
        return
    logger.info("Updated %d tab(s) on sheet %s", len(tabs), sheet_id)
