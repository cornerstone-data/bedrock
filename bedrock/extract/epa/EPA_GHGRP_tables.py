"""Cached GHGRP subpart tables, read from where ``stewi`` already put them.

``stewi`` downloads the Envirofacts subpart views it needs to build the GHGRP
inventory and leaves them as CSVs under ``GHGRP Data Files/tables/<year>/``.
Two of them answer questions the inventory itself cannot, because ``stewi``
aggregates to facility and subpart and drops the detail:

``C_FUEL_LEVEL_INFORMATION``
    subpart C broken out by **fuel**, which is the only place the GHGRP names
    refinery still gas (it calls it ``Fuel Gas``). It explains 98% of subpart C
    CO2; the rest is Tier 4, where CO2 comes from CEMS and is reported outside
    this table.

``Y_SUBPART_LEVEL_INFORMATION``
    petroleum refinery process emissions, whose dominant source is catalytic
    cracking catalyst regeneration - coke burned off the catalyst in place.

⚠️ **These are read, never fetched.** A year ``stewi`` has not downloaded
returns ``None`` and the caller says so, rather than reaching for the network:
the Envirofacts API no longer serves several of the years the archive covers,
and a silent partial year is worse than a named gap. ``stewi`` populates them
as a side effect of building the inventory for that year.

Contrast :mod:`bedrock.extract.epa.EPA_GHGRP_SubpartW`, which *does* fetch,
because the subpart W views it needs are ones ``stewi`` never downloads.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from stewi.GHGRP import OUTPUT_PATH

from bedrock.utils.logging.flowsa_log import log

#: Subpart C by fuel type. Columns used here: ``FACILITY_ID``, ``FUEL_TYPE``
#: and the per-tier CO2 columns.
C_FUEL_TABLE = 'C_FUEL_LEVEL_INFORMATION'

#: Subpart Y, petroleum refineries, at subpart level: ``FACILITY_ID``,
#: ``GHG_NAME``, ``GHG_QUANTITY``. ⚠️ No source-category breakout, so catalyst
#: regeneration cannot be separated from flaring and the other refinery
#: processes in this view.
Y_SUBPART_TABLE = 'Y_SUBPART_LEVEL_INFORMATION'


def tables_dir() -> Path:
    """Where ``stewi`` keeps its downloaded Envirofacts views."""
    return Path(OUTPUT_PATH) / 'tables'


def ghgrp_table(table: str, year: int) -> pd.DataFrame | None:
    """One cached subpart view for *year*, or ``None`` if it is not on disk.

    :param table: view name without extension, e.g. :data:`C_FUEL_TABLE`
    :param year: reporting year
    """
    path = tables_dir() / str(year) / f'{table}.csv'
    if not path.is_file():
        log.warning(
            'GHGRP %d: %s is not in the stewi table cache, so any carve-out '
            'drawn from it is EMPTY for this year rather than small. Build the '
            'GHGRP inventory for %d to populate it.',
            year,
            table,
            year,
        )
        return None
    frame = pd.read_csv(path, dtype=str, low_memory=False)
    log.info('GHGRP %d: read %d rows of %s', year, len(frame), table)
    return frame
