# EPA_GHGRP_SubpartW.py (bedrock)
# !/usr/bin/env python3
# coding=utf-8

"""The GHGRP tables ``stewi`` does not import: subpart W, unit by unit.

``stewi`` builds the GHGRP inventory at **subpart** granularity - one number per
facility per subpart - which is all its schema needs and all most consumers want.
Two Envirofacts views underneath it carry what that aggregation drops, and both
are needed to say whether a facility **bought** the fuel it burned:

``ef_w_combust_large_units``
    subpart W combustion, one row per combustion unit type and fuel: facility,
    reporting year, industry segment, **fuel type**, **quantity of fuel burned**,
    unit of measure, and CO2, CH4 and N2O. The subpart W counterpart of
    ``C_FUEL_LEVEL_INFORMATION``, and the only place in the GHGRP where the
    reporter says whether the gas came off a pipeline or out of the ground.

``ef_w_facility_overview``
    the industry segment each subpart W reporter operates in. Needed for the
    segments that report their combustion under subpart C, where the fuel label
    is a Table C-1 emission factor and says nothing about who owned the fuel.

Onshore production, gathering and boosting, and natural gas distribution report
their stationary combustion under subpart W rather than subpart C, so anything
built by filtering ``Process == 'C'`` holds none of their fuel. `#927
<https://github.com/cornerstone-data/bedrock/issues/927>`_.

This module **acquires and caches** those views. It makes no modelling decision:
the fuel classification, the sector axis and the emissions scale all live in
:mod:`bedrock.transform.ghg.ghgrp_subpart_w`.

Years
-----

2017 and 2018 come from the live API. 2019-2024 come from the FOIA'd Envirofacts
export named by ``GHGRP_EF_VIEWS_ARCHIVE``, the same variable ``stewi`` takes its
``-A`` argument from (#931): EPA stopped publishing after 2023 so 2024 exists
nowhere else, and taking the whole block from one export keeps a single vintage
across those years. A machine without the export falls back to the API for every
year it can, and says so.

Both paths cache to ``extract/input_data/GHGRP/<year>/<VIEW>.csv``, so a run
after the first makes no request at all.
"""

from __future__ import annotations

import io
import os
import zipfile
from pathlib import Path, PurePosixPath

import pandas as pd
from esupy.remote import make_url_request

from bedrock.utils.io.local_extract_input_data import local_extract_input_dir
from bedrock.utils.logging.flowsa_log import log

#: Envirofacts, the service holding the GHGRP's reported tables. ``stewi`` reads
#: the same service; this module goes to it directly because the views below are
#: not among the tables it imports.
ENVIROFACTS_URL = 'https://data.epa.gov/efservice'

#: Subpart W combustion, one row per combustion unit type and fuel.
W_COMBUSTION_VIEW = 'ef_w_combust_large_units'

#: Each subpart W reporter's industry segment.
W_FACILITY_VIEW = 'ef_w_facility_overview'

#: Reporting years taken from the FOIA'd export rather than from the API.
ARCHIVE_YEARS: tuple[int, ...] = tuple(range(2019, 2025))

#: Years EPA never published, so the API cannot serve them however it is asked.
#: Distinct from ``stewi``'s notion of a locally built year - this one is about
#: the source, that one is about the inventory built from it.
UNPUBLISHED_YEARS: tuple[int, ...] = (2024,)

#: Environment variable naming the FOIA'd export (#931).
ARCHIVE_ENV = 'GHGRP_EF_VIEWS_ARCHIVE'


def envirofacts_archive() -> Path | None:
    """The FOIA'd Envirofacts export, if this machine has it."""
    named = os.environ.get(ARCHIVE_ENV)
    if not named:
        return None
    path = Path(named)
    return path if path.is_file() else None


def _view_from_archive(view: str, year: int) -> pd.DataFrame | None:
    """One Envirofacts view for one year, read out of the FOIA'd export.

    The export holds every reporting year it covers in one CSV per view, so the
    year filter happens here rather than in the file name.
    """
    archive = envirofacts_archive()
    if archive is None:
        return None
    with zipfile.ZipFile(archive) as bundle:
        members = [
            name
            for name in bundle.namelist()
            if PurePosixPath(name).name.lower() == f'{view}.csv'
        ]
        if not members:
            raise FileNotFoundError(
                f'{view}.csv is not in {archive.name}. The export holds 387 '
                f'views; check the name against its listing.'
            )
        with bundle.open(members[0]) as handle:
            frame = pd.read_csv(handle, low_memory=False, encoding_errors='replace')
    frame.columns = frame.columns.str.upper()
    served = pd.to_numeric(frame['REPORTING_YEAR'], errors='coerce')
    wanted = frame[served == year]
    # An export that does not reach this year is the same situation as no export
    # at all - say nothing, and let the caller fall through to the API.
    return wanted.copy() if not wanted.empty else None


def _view_from_api(view: str, year: int) -> pd.DataFrame:
    """One Envirofacts view for one year, downloaded whole.

    ⚠️ **Do not page this.** Envirofacts accepts the ``ROWS/start:end`` segment
    and then ignores it for these views, returning the entire year to every
    request - so a four-page loop returns each row four times and every total
    comes out 4x. The row count is checked against the service's own ``COUNT``
    endpoint instead, which is what catches that if it ever changes.
    """
    counted = make_url_request(
        f'{ENVIROFACTS_URL}/{view}/reporting_year/{year}/COUNT/JSON', timeout=120
    ).json()
    expected = int(counted[0]['TOTALQUERYRESULTS'])
    response = make_url_request(
        f'{ENVIROFACTS_URL}/{view}/reporting_year/{year}/CSV', timeout=600
    )
    frame = pd.read_csv(
        io.BytesIO(response.content), low_memory=False, encoding_errors='replace'
    )
    frame.columns = frame.columns.str.upper()
    if len(frame) != expected:
        raise ValueError(
            f'Envirofacts served {len(frame)} rows of {view} for {year} against '
            f'a reported count of {expected}. A row count that is a multiple of '
            f'the count means the service has started paging - fetch whole '
            f'years, never ranges.'
        )
    return frame


def ghgrp_view(view: str, year: int) -> pd.DataFrame | None:
    """An Envirofacts GHGRP view for one reporting year, cached locally.

    Resolution order, and why:

    1. ``extract/input_data/GHGRP/<year>/<VIEW>.csv`` - already fetched.
    2. the FOIA'd export, for :data:`ARCHIVE_YEARS`.
    3. the live API, which still serves these views through 2023.

    Returns ``None`` when a year can be reached no way at all - 2024 on a machine
    without the export - so that a span degrades to the years it has rather than
    failing.
    """
    cached = Path(local_extract_input_dir('GHGRP', year)) / f'{view.upper()}.csv'
    if cached.is_file():
        served = pd.read_csv(cached, low_memory=False, encoding_errors='replace')
        served.columns = served.columns.str.upper()
        return served

    frame: pd.DataFrame | None = None
    if year in ARCHIVE_YEARS:
        frame = _view_from_archive(view, year)
        if frame is None:
            log.info(
                'GHGRP %d: no FOIA export on this machine, so %s comes from the '
                'API instead. Set %s to the EF_Views zip to read it from the '
                'export, which is the only source for years EPA never published.',
                year,
                view,
                ARCHIVE_ENV,
            )
    if frame is None:
        if year in UNPUBLISHED_YEARS:
            log.warning(
                'GHGRP %d skipped for %s: EPA never published it, and this '
                'machine has no FOIA export to read it from. Set %s.',
                year,
                view,
                ARCHIVE_ENV,
            )
            return None
        frame = _view_from_api(view, year)

    frame.to_csv(cached, index=False)
    log.info('GHGRP %d: cached %d rows of %s to %s', year, len(frame), view, cached)
    return frame


def facility_id(column: pd.Series) -> pd.Series:
    """Envirofacts facility ids -> the form ``stewi`` keys facilities on.

    The FOIA export writes them as ``1004628.0000000000`` and the API writes
    plain integers, while ``stewi`` carries the string of the integer. All three
    have to land on one key or a join against a ``stewi`` facility file silently
    drops every row.
    """
    numeric = pd.to_numeric(column, errors='coerce').astype('Int64')
    return numeric.astype('string').astype(object).where(numeric.notna())
