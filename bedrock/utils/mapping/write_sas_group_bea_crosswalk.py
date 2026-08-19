"""Generate the SAS Table 8 group -> BEA 2017 commodity crosswalk for truck (#611).

Truck is **67.8% of `TRANS`**, the largest single piece of the transport margin,
and BEA allocates it on revenue by commodity group:

    *"For truck, we use Service Annual Survey Table 8 which gives us revenue by
    product shipped but at a very aggregated level... The data are commodity data
    in SAS Table 8 and we do use the published data."*
    - W. Nicolls, BEA, 2026-08-11 and 2026-08-17

**Eleven groups partition Total Motor Carrier Revenue to the dollar** - 270,154
$M in 2017 - and ten of them carry a commodity identity. This file maps those ten
onto BEA 2017 detail commodities.

⚠️ **"Other goods" is discarded, not distributed.** It is 32.4% of motor carrier
revenue, and BEA does not use it:

    *"We do not use the 'other' commodity from SAS Table 8 since we have no
    information on what commodities it contains. Distributing it pro rata to the
    other 10 would not change the result since we are creating weights with the
    data to distribute our truck margins rather than explicitly using the values
    from SAS table 8."* - W. Nicolls, BEA, 2026-08-17

Because only *shares* are spent, dropping "Other goods" and renormalising over
the ten is arithmetically identical to spreading it pro rata across them. That
equivalence is BEA's own statement, so this is the method rather than an
approximation of it. ⚠️ Do not later "improve" it by giving "Other goods" a
commodity identity: that would make the allocator diverge from the published
column it is anchored on.

⚠️ **The hazardous-materials row is a cross-cut, not an eleventh group.** It
re-slices the same revenue by whether the load was hazardous, so summing it with
the commodity groups double-counts. It is ignored here.

The groups are coarse - ten of them over ~200 goods commodities - so each maps to
a *set*, and the within-set split falls to the published transport column, the
same default the pipeline sets use. That is a much heavier lift than on rail,
where revenue is observed per commodity and no within-set choice arises.

⚠️ **Two placements are judgement, because the group names do not say.**
Processed food (``311``) is put under *Agricultural products* and beverages and
tobacco under *Grains, alcohol, and tobacco products*, on the reading that the
second group names *products* rather than crops. The alternative - reading the
ten groups as raw commodities only, so all processed food falls into "Other
goods" - would give food manufacturing **no truck margin at all**, which cannot
be right for a mode carrying two thirds of the column.
"""

import pandas as pd

from bedrock.transform.iot.nowcast_transport_margins import (
    published_transport_by_commodity,
)
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_DESC as DESC

CROSSWALK_PATH = 'bedrock/utils/mapping/Crosswalk_SAS_Group_to_BEA_2017.csv'

#: The group SAS publishes that BEA discards. Kept here so the exclusion is
#: explicit in the CSV rather than looking like an oversight.
OTHER_GOODS = 'Other goods'

#: Prefix rules per group. A commodity joins a group if its BEA 2017 detail code
#: starts with one of the prefixes, unless an explicit code claims it first.
GROUP_PREFIXES: dict[str, tuple[str, ...]] = {
    'Agricultural products': (
        '1112',
        '1113',
        '1114',
        '1119',
        '112',
        '114',
        '311',
    ),
    'Grains, alcohol, and tobacco products': ('1111B0', '311210', '312'),
    'Stone, nonmetallic minerals, and metallic ores': (
        '21223',
        '2122A0',
        '21231',
        '2123A0',
        '327',
    ),
    'Coal and petroleum products': ('211000', '212100', '324'),
    'Pharmaceutical and chemical products': ('325',),
    'Wood products, textiles, and leathers': (
        '113',
        '313',
        '314',
        '315',
        '316',
        '321',
        '322',
        '323',
    ),
    'Base metal and machinery': ('331', '332', '333'),
    'Electronic and precision instruments and motorized vehicles': (
        '334',
        '335',
        '336',
    ),
    'Used household and office goods': ('S00402',),
    'New furniture and miscellaneous manufactured products': (
        '326',
        '337',
        '339',
        '511',
        '512',
        'S00401',
    ),
}

#: Codes that a prefix rule would otherwise place wrongly.
EXPLICIT: dict[str, str] = {
    # oilseed farming is an agricultural product; grain farming is its own group
    '1111A0': 'Agricultural products',
}

NOTES: dict[str, str] = {
    'Agricultural products': 'Farm output plus processed food (311). See the module '
    'docstring: reading the ten groups as raw commodities only would leave food '
    'manufacturing with no truck margin at all.',
    'Grains, alcohol, and tobacco products': 'Grain farming, flour milling and malt, '
    'beverages and tobacco. The group names products, not crops, so the processed '
    'forms sit here rather than under Agricultural products.',
    'Coal and petroleum products': 'Includes 211000 oil and gas extraction, which is '
    'also where the pipeline mode concentrates - the bound check is what keeps the '
    'two from colliding.',
    'Used household and office goods': 'Maps to the single BEA commodity S00402, '
    'which receives 23,868 $M of TRANS in 2017.',
    'New furniture and miscellaneous manufactured products': 'Furniture, '
    'miscellaneous manufacturing, plastics and rubber products, S00401 scrap, and printed and recorded media (511, 512). Media is the one placement with no natural group: books, newspapers and recordings plainly move by truck, and leaving them unassigned would give them no truck margin at all.',
}


def assign_group(code: str) -> str | None:
    """Which SAS group *code* belongs to, or None if no rule claims it."""
    if code in EXPLICIT:
        return EXPLICIT[code]
    best: tuple[int, str] | None = None
    for group, prefixes in GROUP_PREFIXES.items():
        for prefix in prefixes:
            if code.startswith(prefix) and (best is None or len(prefix) > best[0]):
                best = (len(prefix), group)
    return None if best is None else best[1]


def main() -> None:
    published = published_transport_by_commodity()
    receiving = published[published != 0].index

    rows = []
    for code in sorted(receiving):
        group = assign_group(code)
        if group is None:
            rows.append((OTHER_GOODS, code, DESC.get(code, '?'), 'UNASSIGNED'))
            continue
        rows.append((group, code, DESC.get(code, '?'), NOTES.get(group, '')))

    out = pd.DataFrame(
        rows,
        columns=['sas_group', 'bea_2017_commodity', 'bea_2017_description', 'basis'],
    ).sort_values(['sas_group', 'bea_2017_commodity'])

    unassigned = out[out['basis'] == 'UNASSIGNED']
    covered = published.reindex(
        out.loc[out['basis'] != 'UNASSIGNED', 'bea_2017_commodity']
    ).sum()
    print(
        f'{len(receiving)} commodities receive TRANS; '
        f'{len(out) - len(unassigned)} assigned to {out["sas_group"].nunique() - (1 if len(unassigned) else 0)} groups, '
        f'{len(unassigned)} unassigned'
    )
    print(
        f'assigned commodities carry {covered / published.sum():.1%} of published TRANS'
    )
    if len(unassigned):
        worst = published.reindex(unassigned['bea_2017_commodity']).sort_values(
            ascending=False
        )
        for code, value in worst.head(12).items():
            print(
                f'  UNASSIGNED {code} {value / 1e6:9,.0f}  {DESC.get(code, "?")[:44]}'
            )

    out.to_csv(CROSSWALK_PATH, index=False)
    print('written')


if __name__ == '__main__':
    main()
