"""
Emit the three ``Sector_Crosswalk_*`` files the ``Margins_Transport`` FBS needs.

The judgement about which BEA commodity a rail STCC code, a SAS commodity
group, a pipeline margin item or an SCTG group belongs to already lives in the
``Crosswalk_*_to_BEA_2017.csv`` files (and, for SCTG, in the ported
``NAICS_Crosswalk_FAF_Mode_and_SCTG.csv``). Those are the source of truth and
are written by their own scripts, where the reasoning is recorded row by row.

This script only re-expresses them in the six-column shape
``get_activitytosector_mapping`` reads, so that the FBS attribution machinery
can do the group-to-commodity split rather than a second implementation of it
inside ``nowcast_transport_margins``. It invents nothing: every row here is a
row there, and re-running it after editing a source crosswalk is the whole
maintenance story.

⚠️ **Only the commodity side is emitted.** The mode that gives the margin up -
truck ``484000``, rail ``482000``, pipeline ``486000``, water ``483000``, air
``481000`` - is not in these files. It is assigned to ``SectorConsumedBy``
after attribution, from each activity set's ``clean_parameter``, because
populating a ConsumedBy sector before attribution would capture
``PrimarySector`` on a TECHNOSPHERE_FLOW source and silently corrupt the split
(the same trap NIPA_final_dom_uses documents at issue #539).

Run from the repository root::

    uv run python bedrock/utils/mapping/write_margins_sector_crosswalks.py
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

MAPPING_DIR = Path(__file__).resolve().parent
CROSSWALK_DIR = MAPPING_DIR / 'activitytosectormapping'

#: The column order ``get_activitytosector_mapping`` expects.
COLUMNS = [
    'ActivitySourceName',
    'Activity',
    'SectorSourceName',
    'Sector',
    'SectorType',
    'Note',
]

SECTOR_SOURCE = 'BEA_2017_Code'

#: The truck group SAS publishes but BEA discards. It carries no commodity
#: information at all, and ``load_truck_group_revenue`` drops it before the
#: shares are formed, so a crosswalk row for it would be unreachable.
TRUCK_OTHER_GOODS = 'Other goods'


def _write(frame: pd.DataFrame, name: str) -> None:
    path = CROSSWALK_DIR / name
    frame[COLUMNS].sort_values(['Activity', 'Sector']).to_csv(path, index=False)
    print(f'{name}: {len(frame)} rows, {frame["Activity"].nunique()} activities')


def census_sas() -> pd.DataFrame:
    """
    SAS commodity groups (truck) and pipeline margin items, in one file.

    Both ride on ``Census_SAS``, so they share a crosswalk; the activity sets
    keep them apart by which table they came from. The two key spaces cannot
    collide - the groups are prose and the items are NAICS.
    """
    truck = pd.read_csv(MAPPING_DIR / 'Crosswalk_SAS_Group_to_BEA_2017.csv', dtype=str)
    truck = truck[truck['sas_group'] != TRUCK_OTHER_GOODS]
    truck_rows = pd.DataFrame(
        {
            'ActivitySourceName': 'Census_SAS',
            'Activity': truck['sas_group'],
            'SectorSourceName': SECTOR_SOURCE,
            'Sector': truck['bea_2017_commodity'],
            'SectorType': '',
            'Note': truck['bea_2017_description'],
        }
    )

    pipeline = pd.read_csv(
        MAPPING_DIR / 'Crosswalk_Pipeline_Margin_Items_to_BEA_2017.csv', dtype=str
    )
    pipeline_rows = pd.DataFrame(
        {
            'ActivitySourceName': 'Census_SAS',
            'Activity': pipeline['sas_naics'],
            'SectorSourceName': SECTOR_SOURCE,
            'Sector': pipeline['bea_2017_commodity'],
            'SectorType': '',
            'Note': pipeline['bea_2017_description'],
        }
    )

    return pd.concat([truck_rows, pipeline_rows], ignore_index=True)


def stb_crsr() -> pd.DataFrame:
    """
    5-digit STCC to BEA 2017 commodity.

    ⚠️ **The excluded codes are dropped here rather than carried as blanks.**
    In ``Crosswalk_STCC5_to_BEA_2017.csv`` they are kept with an empty target so
    the exclusion stays visible; a crosswalk row with no ``Sector`` would just
    strand revenue, so the FBS drops those codes and lets the rest renormalise -
    which is what ``rail_revenue_by_commodity`` does too.
    """
    rail = pd.read_csv(MAPPING_DIR / 'Crosswalk_STCC5_to_BEA_2017.csv', dtype=str)
    rail = rail[rail['bea_2017_commodity'].notna()]
    return pd.DataFrame(
        {
            'ActivitySourceName': 'STB_CRSR',
            'Activity': rail['stcc5'],
            'SectorSourceName': SECTOR_SOURCE,
            'Sector': rail['bea_2017_commodity'],
            'SectorType': '',
            'Note': rail['stcc_description'],
        }
    )


def bts_faf() -> pd.DataFrame:
    """
    SCTG group to BEA 2017 commodity, lifted out of the ported FAF crosswalk.

    ``NAICS_Crosswalk_FAF_Mode_and_SCTG.csv`` targets NAICS in its ``Sector``
    column and carries the BEA code in ``Note``; this promotes the BEA code to
    ``Sector`` so the FBS can meet a BEA-coded attribution source (#546).
    """
    faf = pd.read_csv(
        CROSSWALK_DIR / 'NAICS_Crosswalk_FAF_Mode_and_SCTG.csv', dtype=str
    ).fillna('')
    sctg = faf[(faf['ActivitySourceName'] == 'FAF_SCTG') & (faf['Note'] != '')]
    return (
        pd.DataFrame(
            {
                'ActivitySourceName': 'BTS_FAF',
                'Activity': sctg['Activity'],
                'SectorSourceName': SECTOR_SOURCE,
                'Sector': sctg['Note'],
                'SectorType': '',
                'Note': sctg['Activity_Description'],
            }
        )
        .drop_duplicates(subset=['Activity', 'Sector'])
        .reset_index(drop=True)
    )


def main() -> None:
    _write(census_sas(), 'Sector_Crosswalk_Census_SAS.csv')
    _write(stb_crsr(), 'Sector_Crosswalk_STB_CRSR.csv')
    _write(bts_faf(), 'Sector_Crosswalk_BTS_FAF.csv')


if __name__ == '__main__':
    main()
