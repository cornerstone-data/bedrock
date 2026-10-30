"""Which BEA annual update each industry-accounts input comes from (#1157).

BEA revises gross output and the summary supply-use tables together, once a
year, in the annual update of the industry accounts it releases each
September. The nowcast balances summary supply-use controls against gross
output, so the two must come from the same update: a control from an older
update carries BEA's revisions as error (in 2022, $1.32T of commodity output,
summed absolute, between the 2023 and 2025 updates).

File names, GCS folders and release dates do not identify the update reliably.
``BEA_DATA_VERSION`` is a BEA Data Archive label ("2025Q2"), the supply-use
workbooks carry no release stamp, and an archive can be re-downloaded at any
time. So each source is tied to its update by what it contains: **an annual
update published in September of year Y carries data through Y - 1.** Checked
against every workbook on hand:

========================================  ==========  =============
source                                    last year   annual update
========================================  ==========  =============
gross output ``UGO305-A``, ``2025Q2``     2024        2025
gross output ``UGO305-A``, ``2026Q2``     2025        2026
summary supply-use, 1997-2024 workbooks   2024        2025
summary supply-use, 2017-2022 workbooks   2022        2023
========================================  ==========  =============
"""

from __future__ import annotations

import functools
import logging
import os

import pandas as pd

from bedrock.extract.iot.constants import GCS_USA_SUP_DIR
from bedrock.extract.iot.gdp import (
    BEA_DATA_VERSION,
    BeaDataVersion,
    load_go_detail_years,
)
from bedrock.extract.iot.io_2017 import LOCAL_USA_SUP_DIR, SUMMARY_SUT_MAPPING
from bedrock.utils.io.gcp import load_from_gcs

logger = logging.getLogger(__name__)

#: The newest BEA annual update of the industry accounts, released 30 September
#: 2026. Inputs on an older update still pass the check, with a warning.
NEWEST_BEA_ANNUAL_UPDATE = 2026


def annual_update_from_last_year(last_year: int) -> int:
    """The annual update that publishes data through *last_year*."""
    return int(last_year) + 1


@functools.cache
def gross_output_annual_update(version: BeaDataVersion = BEA_DATA_VERSION) -> int:
    """The annual update of the detail gross output table (``UGO305-A``)."""
    return annual_update_from_last_year(max(load_go_detail_years(version)))


def _workbook_years(name: str) -> list[int]:
    sheets = load_from_gcs(
        name=name,
        sub_bucket=GCS_USA_SUP_DIR,
        local_dir=LOCAL_USA_SUP_DIR,
        loader=lambda pth: pd.DataFrame({'sheet': pd.ExcelFile(pth).sheet_names}),
    )['sheet']
    return [int(s) for s in sheets.astype(str) if s.strip().isdigit()]


@functools.cache
def summary_sut_annual_updates() -> dict[str, int]:
    """The annual update of each summary supply-use workbook the loader reads."""
    return {
        name: annual_update_from_last_year(max(_workbook_years(name)))
        for name in SUMMARY_SUT_MAPPING.values()
    }


def assert_same_bea_annual_update(
    gross_output: int | None = None,
    summary_sut: dict[str, int] | None = None,
) -> int:
    """Raise unless gross output and the summary supply-use tables share an update.

    Returns that update. Logs a warning when it is older than
    :data:`NEWEST_BEA_ANNUAL_UPDATE`. The arguments exist for tests; by default
    both sides are read from the inputs the nowcast loads.
    """
    go = gross_output_annual_update() if gross_output is None else gross_output
    sut = summary_sut_annual_updates() if summary_sut is None else summary_sut
    off = {
        os.path.basename(name): update for name, update in sut.items() if update != go
    }
    if off:
        raise ValueError(
            f'BEA annual update mismatch: gross output ({BEA_DATA_VERSION}) is the '
            f'{go} annual update, but these summary supply-use workbooks are not: '
            f'{off}. Move both to the same update (#1157).'
        )
    if go < NEWEST_BEA_ANNUAL_UPDATE:
        logger.warning(
            'Gross output and the summary supply-use tables are both the %d BEA '
            'annual update; the newest is %d.',
            go,
            NEWEST_BEA_ANNUAL_UPDATE,
        )
    return go
