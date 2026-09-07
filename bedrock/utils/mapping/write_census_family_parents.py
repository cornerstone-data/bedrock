"""Write the Census family-parent rows the import within-family re-split needs (#763).

[#763](https://github.com/cornerstone-data/bedrock/issues/763) validated a
construction on the 2012 holdout and closed without wiring it: *each family's
within-split anchored on the published 2017 mix, moved by the Census family
level*. This script generates the crosswalk half of that wiring.

Why anything is needed at all: the import method already splits **1:m** Census
activities by frozen 2017 Supply ``MCIF``, which *is* anchor-and-move. The
problem is that almost every family is mapped **1:1 or m:1**, so no attribution
weight is ever applied and Census's own within-family split stands unchallenged
— and that split is not BEA's. Giving each family a parent activity converts the
whole family into one 1:m row, which is the same device #702 used for vehicles
and #865 for aerospace exports.

The parent code is the four-digit family (``3371``), which cannot collide with a
Census activity: ``census_usatrade_parse`` keeps only ``\\d{6}``, ``\\d{5}X`` and
``\\d{4}XX``.

⚠️ **Generated rows carry ``Note = FAMILY_PARENT``.** Nothing else in the file
uses that marker, and ``trade.utilities.census_family_parents`` reads it to
decide which activities to relabel — so the crosswalk and the clean function
cannot disagree about what a family is.

Rerun after a crosswalk change, then commit the diff::

    uv run python -m bedrock.utils.mapping.write_census_family_parents
"""

from __future__ import annotations

import sys

import pandas as pd

from bedrock.extract.iot.io_2017 import _load_2017_detail_supply_use_usa

CROSSWALK = (
    'bedrock/utils/mapping/activitytosectormapping/Sector_Crosswalk_Census_USATrade.csv'
)

#: Marks a generated parent row. Also the contract with the clean function.
MARKER = 'FAMILY_PARENT'

#: The source whose activities get parents. Only the Census goods crosswalk.
SOURCE = 'Census_USATrade'
SCHEMA = 'BEA_2017_Code'


def family_of(code: str) -> str:
    """The four-digit family a BEA detail commodity belongs to."""
    return str(code)[:4]


def leaf_rows() -> pd.DataFrame:
    """The crosswalk without any previously generated parent rows."""
    frame = pd.read_csv(CROSSWALK, dtype=str).fillna('')
    return frame[frame['Note'] != MARKER]


def published_mcif() -> pd.Series:
    """Published 2017 Supply ``MCIF`` by commodity, millions of dollars."""
    supply = _load_2017_detail_supply_use_usa('Supply_detail')
    supply.columns = supply.columns.str.strip()
    return pd.to_numeric(supply['MCIF'], errors='coerce').fillna(0.0)


def activity_families(frame: pd.DataFrame) -> dict[str, str]:
    """Each Census activity's family, for activities that sit in exactly one.

    ⚠️ An activity whose targets straddle two families is **excluded**, not
    forced. Relabelling it would move mass between families, which is a level
    change, and this construction may only move mass *within* one.
    """
    census = frame[
        (frame['ActivitySourceName'] == SOURCE) & (frame['SectorSourceName'] == SCHEMA)
    ]
    targets = census.groupby('Activity')['Sector'].apply(lambda s: sorted(set(s)))
    out = {}
    for activity, sectors in targets.items():
        families = {family_of(s) for s in sectors}
        if len(families) == 1:
            out[str(activity)] = families.pop()
    return out


def build() -> pd.DataFrame:
    """The parent rows: one per (family, leaf) for every multi-leaf family."""
    frame = leaf_rows()
    census = frame[
        (frame['ActivitySourceName'] == SOURCE) & (frame['SectorSourceName'] == SCHEMA)
    ]
    targets = census.groupby('Activity')['Sector'].apply(lambda s: sorted(set(s)))
    mcif = published_mcif()

    families: dict[str, list[str]] = {}
    for activity, family in activity_families(frame).items():
        families.setdefault(family, []).append(activity)

    rows = []
    for family, activities in sorted(families.items()):
        reached = {s for a in activities for s in targets[a]}
        published = {
            c for c in mcif.index if family_of(c) == family and float(mcif[c]) > 0
        }
        leaves = sorted(reached | published)
        if len(leaves) < 2:
            # Nothing to split. `3346` is the standing example: three Census
            # codes fold onto one commodity, which is #670's level problem and
            # is not reachable from here.
            continue
        for leaf in leaves:
            rows.append(
                {
                    'ActivitySourceName': SOURCE,
                    'Activity': family,
                    'SectorSourceName': SCHEMA,
                    'Sector': leaf,
                    'SectorType': 'C',
                    'Note': MARKER,
                }
            )
    return pd.DataFrame(rows)


def main() -> int:
    existing = leaf_rows()
    parents = build()
    out = pd.concat([existing, parents], ignore_index=True)
    out.to_csv(CROSSWALK, index=False)
    families = parents['Activity'].nunique()
    print(f'{len(parents)} parent rows across {families} families -> {CROSSWALK}')
    print(parents.groupby('Activity').size().to_string())
    return 0


if __name__ == '__main__':
    sys.exit(main())
