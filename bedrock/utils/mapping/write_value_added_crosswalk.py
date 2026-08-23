"""
Emit ``Sector_Crosswalk_BEA_NIPA_VA.csv``, the activity map the ``NIPA_VA_*``
FBS methods attribute through (#538).

The value-added rows have no NIPA table at BEA detail grain -- nothing in NIPA
reaches 402 industries -- so every activity set is one NIPA line reaching a
*set* of BEA detail industries, and the split within that set comes from an
attribution source rather than from here. This file therefore says only which
industries a NIPA line is allowed to reach. It carries no weights and makes no
allocation.

The three rows it serves
------------------------

``T00OTOP`` is one activity reaching 391 industries, because its control is one
number. ⚠️ **Government is excluded by name, not left to a zero weight.** BEA
books no taxes on production to any government producer -- an accounting rule,
since a tax levied by government and remitted by a government producer nets out.
The 2017 weights happen to be zero there too, so the rule would appear to hold
on its own; excluding the codes makes it hold *because* it is a rule. Argued at
greater length in :mod:`~bedrock.analysis.nowcasting.tax_axis_conversion`.

⚠️ **Eleven codes, not the canonical ten.** ``tax_axis_conversion``'s prefix rule
(``S00``, ``G``) misses the US Postal Service, ``491000``, which is a federal
government enterprise with a BEA code shaped like an industry's. Its published
``T00OTOP`` is zero like the rest, so naming it changes no 2017 number -- and
that is the point: the rule now holds by rule rather than by the weight
happening to be zero. Surfaced by the #587 diagnostic.

``V00100`` is 69 activities, because NIPA ``T60200D`` states compensation for 69
industry groups and each is its own control. ✅ **Those 69 leaves partition the
71 BEA summary industries exactly** -- no gaps, no overlaps -- which is what
makes the expansion below sound rather than approximate: 63 of them equal a
summary industry's published compensation *to the dollar*, and the rest agree
within BEA's own rounding. The mapping is therefore verified numerically, not by
name matching, in
:mod:`~bedrock.analysis.nowcasting.compensation_allocation`.

``V00300`` is one activity reaching all 402 industries, and unlike the other two
that is *not* because its control is a single number -- it is eight, across five
tables. It is because none of the eight brings a usable industry axis. Their
tables publish on **mutually incompatible partitions** (net interest 20 groups,
nonfarm proprietors 21, corporate profits financial/nonfinancial plus 12
industries, corporate capital consumption 63), so a component-wise build cannot
be reconciled to one industry partition without a common refinement that is
coarser than any of them. ⚠️ It also reaches all ten government codes and one
industry with a **negative** surplus (``S00201`` public transit, -36,919), so no
step here may assume positivity. The reasoning, and why the one source that
would give it a real axis is deliberately not used, are in
:mod:`~bedrock.analysis.nowcasting.compensation_allocation`.

How a NIPA group reaches BEA detail
-----------------------------------

Each leaf maps to one or two BEA **summary** industries, and each summary
industry expands to its detail children through
``NAICS_to_BEA_Crosswalk_2017``. Those children cover all 402 detail industries
with nothing left over, so a summary-level partition is a detail-level partition.

⚠️ **Activity names come from the FBA, not from the NIPA archive.**
``bea_nipa_parse`` strips BEA's trailing footnote numbers and normalises slashes
and ``n.e.c.``, so a name taken from the raw table can fail to match the name the
attribution machinery sees. This script reads ``getFlowByActivity('BEA_NIPA')``
for exactly that reason.

⚠️ **Four lines need aliases, because their NIPA names collide.** ``T60200D``
titles both federal and state-and-local general government *Compensation of
general government employees*, and both enterprise lines *Government
enterprises*. Those four get a distinctive ``ActivityProducedBy`` assigned in
the method with ``assign_fields`` before mapping -- the same device
``NIPA_final_dom_uses`` uses for its ``(part of NNN)`` PCE lines -- and the
aliases are what appear here. The other 65 leaf names are unique within the
selection and are used as published.

Run from the repository root::

    uv run python bedrock/utils/mapping/write_value_added_crosswalk.py
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from bedrock.utils.config.common import load_crosswalk
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES
from bedrock.utils.taxonomy.bea.v2017_industry_summary import (
    USA_2017_SUMMARY_INDUSTRY_CODES,
)

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

BENCHMARK_YEAR = 2017

#: General government and government enterprises. No taxes on production.
GOVERNMENT_PREFIXES = ('S00', 'G')

#: The US Postal Service, a federal government enterprise whose BEA code does
#: not start with ``S00`` or ``G``.  ⚠️ Named separately because the prefix rule
#: misses it: ``tax_axis_conversion``'s canonical "ten government industry
#: codes" are the prefix matches, and postal is an eleventh.  Its published
#: ``T00OTOP`` is zero like the other ten, so excluding it changes no number in
#: 2017 -- which is exactly why it is worth naming. Left to the weight it would
#: hold only by coincidence, and the #587 diagnostic is what surfaced it.
POSTAL_SERVICE = '491000'

#: The alias ``NIPA_VA_othertax_<year>``'s single activity set assigns.  One
#: activity because there is one control: NIPA states no "other taxes on
#: production excluding housing and farm" line, so the housing and farm lookups
#: would have to enter as weights rather than as controls of their own.
OTHERTAX = 'T00OTOP all industries'

#: The alias ``NIPA_VA_surplus_<year>``'s activity sets assign.  One activity for
#: all eight assembly lines, because none of them has an industry axis this build
#: uses -- see the module's ``V00300`` note below.
SURPLUS = 'V00300 all industries'

#: ``T60200D`` compensation table, whose 69 leaf lines are ``V00100``'s controls.
#: ``T60300D`` wages is line-for-line identical, so this serves both.
COMPENSATION_TABLE = 'T60200D'

#: Each ``T60200D`` leaf line against the BEA summary industries it covers.
#:
#: Hand-written, then verified: the values partition the 71 summary industries
#: exactly, and 63 of the lines equal a summary industry's published
#: compensation to the dollar.  Where a line is a *parent* of finer lines the
#: finer ones are taken (retail 39-42 rather than 38, transport 44-51 rather
#: than 43), and where BEA is finer than NIPA the parent is taken (wholesale 35,
#: since NIPA's durable/nondurable split at 36-37 has no summary counterpart).
#:
#: ⚠️ ``89``/``90`` (civilian and military) and ``94``/``95`` (education and
#: other) are *different cuts* of lines 88 and 93, not finer versions of them --
#: NIPA's state and local "Education" is 716,832 against the SUT's ``GSLGE``
#: 731,648, so they do not correspond.  The parent is taken in both cases and
#: the split left to the benchmark weights.
COMPENSATION_LINES: dict[int, tuple[str, ...]] = {
    5: ('111CA',),
    6: ('113FF',),
    8: ('211',),
    9: ('212',),
    10: ('213',),
    11: ('22',),
    12: ('23',),
    15: ('321',),
    16: ('327',),
    17: ('331',),
    18: ('332',),
    19: ('333',),
    20: ('334',),
    21: ('335',),
    22: ('3361MV',),
    23: ('3364OT',),
    24: ('337',),
    25: ('339',),
    27: ('311FT',),
    28: ('313TT',),
    29: ('315AL',),
    30: ('322',),
    31: ('323',),
    32: ('324',),
    33: ('325',),
    34: ('326',),
    35: ('42',),
    39: ('441',),
    40: ('445',),
    41: ('452',),
    42: ('4A0',),
    44: ('481',),
    45: ('482',),
    46: ('483',),
    47: ('484',),
    48: ('485',),
    49: ('486',),
    50: ('487OS',),
    51: ('493',),
    53: ('511',),
    54: ('512',),
    55: ('513',),
    56: ('514',),
    58: ('521CI',),
    59: ('523',),
    60: ('524',),
    61: ('525',),
    # NIPA "Real estate" is the SUT's housing pair plus other real estate.
    # 531HSO carries no compensation at all, so the whole line lands on
    # 531HST + 531ORE once the benchmark weights are applied.
    63: ('HS', 'ORE'),
    64: ('532RL',),
    66: ('5411',),
    67: ('5415',),
    68: ('5412OP',),
    69: ('55',),
    71: ('561',),
    72: ('562',),
    73: ('61',),
    75: ('621',),
    76: ('622',),
    77: ('623',),
    78: ('624',),
    80: ('711AS',),
    81: ('713',),
    83: ('721',),
    84: ('722',),
    85: ('81',),
    # T31005 splits this 430,318 into S00500 246,097 and S00600 184,220 exactly,
    # and so does the benchmark, so the split is left to the weights rather than
    # spending a second control on it.
    88: ('GFGD', 'GFGN'),
    91: ('GFE',),
    93: ('GSLG',),
    96: ('GSLE',),
}

#: The four lines whose NIPA names collide, and the alias each is given.
COMPENSATION_ALIASES: dict[int, str] = {
    88: 'V00100 federal general government',
    91: 'V00100 federal government enterprises',
    93: 'V00100 state and local general government',
    96: 'V00100 state and local government enterprises',
}


def industries() -> list[str]:
    """The 402 BEA 2017 detail industry codes."""
    return [str(code) for code in USA_2017_INDUSTRY_CODES]


def government_industries() -> list[str]:
    """The eleven codes BEA books no taxes on production to.

    The ten that match ``GOVERNMENT_PREFIXES``, plus :data:`POSTAL_SERVICE`.
    """
    matched = [code for code in industries() if code.startswith(GOVERNMENT_PREFIXES)]
    return [code for code in industries() if code in matched or code == POSTAL_SERVICE]


def taxable_industries() -> list[str]:
    """The 391 industries other taxes on production may reach.

    402 less the eleven government codes.  The housing pair and the ten farm
    codes stay in: their published lookups would enter as *weights* rather than
    as separate controls, so those industries are still reached by this
    activity.
    """
    government = set(government_industries())
    return [code for code in industries() if code not in government]


def summary_to_detail() -> dict[str, list[str]]:
    """Each BEA summary industry against its detail children.

    From ``NAICS_to_BEA_Crosswalk_2017``, restricted to the 402 industry codes.
    Asserted to be a partition, because the whole expansion rests on it.
    """
    crosswalk = load_crosswalk('NAICS_to_BEA_Crosswalk_2017')
    pairs = (
        crosswalk[['BEA_2017_Summary_Code', 'BEA_2017_Detail_Code']]
        .dropna()
        .drop_duplicates()
    )
    detail = set(industries())
    children = {
        str(summary): sorted(
            code for code in set(group['BEA_2017_Detail_Code']) if code in detail
        )
        for summary, group in pairs.groupby('BEA_2017_Summary_Code')
    }
    covered = [code for codes in children.values() for code in codes]
    missing = detail - set(covered)
    if missing:
        raise ValueError(f'detail industries with no summary parent: {sorted(missing)}')
    if len(covered) != len(set(covered)):
        raise ValueError('a detail industry has more than one summary parent')
    return children


def compensation_activity_names() -> dict[int, str]:
    """Each leaf line against the activity name the crosswalk keys on.

    The four colliding lines take their alias; the rest take the name the FBA
    carries, which is BEA's label after ``bea_nipa_parse``'s cleaning.
    """
    from bedrock.extract.flowbyactivity import getFlowByActivity  # noqa: PLC0415

    fba = getFlowByActivity('BEA_NIPA', BENCHMARK_YEAR)
    rows = fba[fba['Description'].str.startswith(f'{COMPENSATION_TABLE}:')]
    lines = rows['Description'].str.split(' - ').str[-1].astype(int)
    published = dict(zip(lines, rows['ActivityProducedBy']))

    names = {}
    for line in COMPENSATION_LINES:
        if line in COMPENSATION_ALIASES:
            names[line] = COMPENSATION_ALIASES[line]
            continue
        if line not in published:
            raise KeyError(
                f'{COMPENSATION_TABLE} line {line} is not in the '
                f'{BENCHMARK_YEAR} BEA_NIPA FBA; regenerate it, or the table '
                f'has been renumbered'
            )
        names[line] = str(published[line])

    unaliased = [n for line, n in names.items() if line not in COMPENSATION_ALIASES]
    duplicated = {n for n in unaliased if unaliased.count(n) > 1}
    if duplicated:
        raise ValueError(
            f'these {COMPENSATION_TABLE} leaf names collide and need an alias in '
            f'COMPENSATION_ALIASES: {sorted(duplicated)}'
        )
    return names


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


def compensation_rows() -> list[dict[str, str]]:
    """One activity per ``T60200D`` leaf, reaching its summary's detail children."""
    children = summary_to_detail()
    names = compensation_activity_names()

    used = [code for codes in COMPENSATION_LINES.values() for code in codes]
    if sorted(used) != sorted(USA_2017_SUMMARY_INDUSTRY_CODES):
        extra = sorted(set(used) - set(USA_2017_SUMMARY_INDUSTRY_CODES))
        absent = sorted(set(USA_2017_SUMMARY_INDUSTRY_CODES) - set(used))
        raise ValueError(
            f'COMPENSATION_LINES does not partition the 71 summary industries. '
            f'Not summary codes: {extra}. Unreached: {absent}.'
        )

    rows = []
    for line, summaries in COMPENSATION_LINES.items():
        sectors = [code for summary in summaries for code in children[summary]]
        rows += _rows(
            names[line],
            sectors,
            f'{COMPENSATION_TABLE} line {line} -> {"+".join(summaries)}',
        )
    return rows


def build() -> pd.DataFrame:
    """Every activity against the industries it may reach."""
    rows = [
        *_rows(
            OTHERTAX,
            taxable_industries(),
            'T30500 LA000365 + LA000237, every industry but the ten government codes',
        ),
        *compensation_rows(),
        *_rows(
            SURPLUS,
            industries(),
            'T11000 W272RC/A041RC/A108RC/A030RC + T70700 B029RC + T70900 A048RC '
            '+ T61600D A445RC + T70500 A262RC, every industry',
        ),
    ]
    frame = pd.DataFrame(rows, columns=COLUMNS)
    if frame.duplicated(subset=['Activity', 'Sector']).any():
        raise ValueError('an activity reaches the same sector twice')
    return frame


def main() -> None:
    frame = build()
    path = CROSSWALK_DIR / CROSSWALK
    frame.sort_values(['Activity', 'Sector']).to_csv(path, index=False)
    print(f'{CROSSWALK}: {len(frame)} rows, {frame["Activity"].nunique()} activities')
    compensation = frame[frame['Note'].str.startswith(COMPENSATION_TABLE)]
    for label, count in (
        (OTHERTAX, int((frame['Activity'] == OTHERTAX).sum())),
        (
            f'V00100, {compensation["Activity"].nunique()} NIPA leaf groups',
            len(compensation),
        ),
        (SURPLUS, int((frame['Activity'] == SURPLUS).sum())),
    ):
        print(f'  {label:<46} {count:>4} industries')


if __name__ == '__main__':
    main()
