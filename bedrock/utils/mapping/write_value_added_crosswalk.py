"""
Emit ``Sector_Crosswalk_BEA_NIPA_VA.csv``, the activity map the ``NIPA_VA_*``
FBS methods attribute through (#538).

The value-added rows have no NIPA table at BEA detail grain -- nothing in NIPA
reaches 402 industries -- so every activity set is *one* NIPA line reaching a
*set* of BEA detail industries, and the split within that set comes from an
attribution source rather than from here. This file therefore says only which
industries a NIPA line is allowed to reach. It carries no weights and makes no
allocation.

⚠️ **The activity names are aliases, not NIPA's own labels.** Three of the
lines these methods read are titled ``Taxes on production and imports`` in three
different tables (``T30500``, ``T70405``, ``T70305``), and BEA's series labels
collide across tables generally. Each activity set therefore assigns a
distinctive ``ActivityProducedBy`` with ``assign_fields`` before mapping, the
same way ``NIPA_final_dom_uses`` aliases its four ``(part of NNN)`` PCE lines,
and those aliases are what appear here.

⚠️ **Government is excluded by name, not left to a zero weight.** BEA books no
taxes on production to any of the ten government industry codes -- an accounting
rule, since a tax levied by government and remitted by a government producer
nets out. The 2017 weights happen to be zero there too, so the rule would appear
to hold on its own; excluding the codes makes it hold *because* it is a rule.
The same argument is made at greater length in
:mod:`~bedrock.analysis.nowcasting.tax_axis_conversion`.

Run from the repository root::

    uv run python bedrock/utils/mapping/write_value_added_crosswalk.py
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES

MAPPING_DIR = Path(__file__).resolve().parent
CROSSWALK_DIR = MAPPING_DIR / 'activitytosectormapping'

CROSSWALK = 'Sector_Crosswalk_BEA_NIPA_VA.csv'

#: The column order ``get_activitytosector_mapping`` expects.
COLUMNS = [
    'ActivitySourceName',
    'Activity',
    'SectorSourceName',
    'Sector',
    'SectorType',
    'Note',
]

ACTIVITY_SOURCE = 'BEA_NIPA'
SECTOR_SOURCE = 'BEA_2017_Code'

#: General government and government enterprises. No taxes on production.
GOVERNMENT_PREFIXES = ('S00', 'G')

#: The alias ``NIPA_VA_othertax_<year>``'s single activity set assigns.  One
#: activity because there is one control: NIPA states no "other taxes on
#: production excluding housing and farm" line, so the housing and farm lookups
#: are carried in the weight vector rather than as controls of their own.  See
#: :func:`~bedrock.transform.nipa.value_added_weights.other_taxes_weights`.
OTHERTAX = 'T00OTOP all industries'


def industries() -> list[str]:
    """The 402 BEA 2017 detail industry codes."""
    return [str(code) for code in USA_2017_INDUSTRY_CODES]


def government_industries() -> list[str]:
    """The ten government industry codes, general and enterprise."""
    return [code for code in industries() if code.startswith(GOVERNMENT_PREFIXES)]


def taxable_industries() -> list[str]:
    """The 392 industries other taxes on production may reach.

    402 less the ten government codes.  The housing pair and the ten farm codes
    stay in: their published lookups enter as *weights* rather than as separate
    controls, so those industries are still reached by this activity.
    """
    government = set(government_industries())
    return [code for code in industries() if code not in government]


def _rows(activity: str, sectors: list[str], note: str) -> list[dict[str, str]]:
    return [
        {
            'ActivitySourceName': ACTIVITY_SOURCE,
            'Activity': activity,
            'SectorSourceName': SECTOR_SOURCE,
            'Sector': sector,
            'SectorType': '',
            'Note': note,
        }
        for sector in sectors
    ]


def build() -> pd.DataFrame:
    """Every activity alias against the industries it may reach."""
    rows = _rows(
        OTHERTAX,
        taxable_industries(),
        'T30500 LA000365 + LA000237, every industry but the ten government codes',
    )
    return pd.DataFrame(rows, columns=COLUMNS)


def main() -> None:
    frame = build()
    path = CROSSWALK_DIR / CROSSWALK
    frame.sort_values(['Activity', 'Sector']).to_csv(path, index=False)
    print(f'{CROSSWALK}: {len(frame)} rows, {frame["Activity"].nunique()} activities')
    for activity, group in frame.groupby('Activity'):
        print(f'  {activity:<26} {len(group):>4} industries')


if __name__ == '__main__':
    main()
