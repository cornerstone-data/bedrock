"""Which BEA annual update each industry-accounts input comes from (#1157).

BEA revises gross output, the summary supply-use tables and the components of
value added together, once a year, in the annual update of the industry accounts
it releases each September. The nowcast balances summary supply-use controls
against gross output, and splits value added with the components table, so all
three must come from the same update: a control from an older update carries
BEA's revisions as error (in 2022, $1.32T of commodity output, summed absolute,
between the 2023 and 2025 updates).

File names, GCS folders and release dates do not identify the update reliably.
``BEA_DATA_VERSION`` is a BEA Data Archive label ("2026Q2"), the supply-use
workbooks carry no release stamp, and BEA's quarterly GDP-by-industry releases
reuse the archive name. So each source is tied to its update by what it
contains: **an annual update published in September of year Y carries data
through Y - 1.** Checked against every workbook on hand:

=============================================  ==========  =============
source                                         last year   annual update
=============================================  ==========  =============
gross output ``UGO305-A``, ``2025Q2``          2024        2025
gross output ``UGO305-A``, ``2026Q2``          2025        2026
summary supply-use, 1997-2024 workbooks        2024        2025
summary supply-use, 1997-2025 workbooks        2025        2026
summary supply-use, 2017-2022 workbooks        2022        2023
``TVA113-A``, ``GDPbyInd.zip`` (June 2026)     2024        2025
``TVA113-A``, ``GDPbyInd_2026Q2.zip``          2025        2026
=============================================  ==========  =============

The June 2026 archive is a quarterly release: it was published in 2026 but its
annual table still ends in 2024, so by content it is the 2025 update.
"""

from __future__ import annotations

import functools
import logging
import os
import zipfile

import pandas as pd

from bedrock.extract.bea.BEA_GDPbyIndustry import (
    _HEADER_ROW,
    GDP_BY_INDUSTRY_ZIP,
    gdp_by_industry_load_gcs,
    gdp_by_industry_local_path,
)
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

#: The components-of-value-added sheet the ``NIPA_VA_surplus`` methods read.
VALUE_ADDED_COMPONENTS_SHEET = 'TVA113-A'


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


@functools.cache
def value_added_components_annual_update() -> int:
    """The annual update of ``TVA113-A`` in the GDP-by-industry archive."""
    path = gdp_by_industry_local_path()
    if not os.path.exists(path):
        # fetches the archive from GCS into the local cache
        gdp_by_industry_load_gcs(
            source='BEA_GDPbyIndustry',
            config={'files': ['ValueAdded.xlsx'], 'sheets': []},
        )
    with zipfile.ZipFile(path) as archive, archive.open('ValueAdded.xlsx') as f:
        header = pd.read_excel(
            f,
            sheet_name=VALUE_ADDED_COMPONENTS_SHEET,
            header=None,
            skiprows=_HEADER_ROW,
            nrows=1,
        ).iloc[0]
    years = [int(v) for v in pd.to_numeric(header, errors='coerce').dropna()]
    return annual_update_from_last_year(max(y for y in years if y > 1900))


def industry_accounts_annual_updates() -> dict[str, int]:
    """The annual update of every industry-accounts input, keyed by source."""
    return {
        f'gross output UGO305-A ({BEA_DATA_VERSION})': gross_output_annual_update(),
        **summary_sut_annual_updates(),
        f'{VALUE_ADDED_COMPONENTS_SHEET} ({GDP_BY_INDUSTRY_ZIP})': (
            value_added_components_annual_update()
        ),
    }


def assert_same_bea_annual_update(updates: dict[str, int] | None = None) -> int:
    """Raise unless every industry-accounts input is one BEA annual update.

    Returns that update. Logs a warning when it is older than
    :data:`NEWEST_BEA_ANNUAL_UPDATE`. *updates* maps each source to its update
    and exists for tests; by default it is read from the inputs the nowcast loads.
    """
    if updates is None:
        updates = industry_accounts_annual_updates()
    distinct = set(updates.values())
    if len(distinct) != 1:
        by_update: dict[int, list[str]] = {}
        for source, update in updates.items():
            by_update.setdefault(update, []).append(os.path.basename(source))
        raise ValueError(
            'BEA annual update mismatch: the industry-accounts inputs come from '
            f'different annual updates: {dict(sorted(by_update.items()))}. Move '
            'them to the same update (#1157).'
        )
    (update,) = distinct
    if update < NEWEST_BEA_ANNUAL_UPDATE:
        logger.warning(
            'The industry-accounts inputs are all the %d BEA annual update; the '
            'newest is %d.',
            update,
            NEWEST_BEA_ANNUAL_UPDATE,
        )
    return update
