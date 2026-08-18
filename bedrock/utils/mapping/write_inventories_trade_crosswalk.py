"""Generate the trade-industry -> BEA commodity crosswalk for `F03000` (#529).

BEA's merchandise-trade rule for change in private inventories is the simplest
of its four: *"Wholesale and retail industries hold products that they sell. So
the 'what held' is based on the type of industry they operate in"*
(D. Hill, BEA, 2025-05-09). Wholesale plus retail is 126% of the column gross,
so this one crosswalk carries the branch that matters.

**The concept map is the judgment; the expansion is mechanical.** NIPA's trade
lines in `U50705BU1` are NAICS wholesale (423x/424x) and retail (44x/45x)
categories, and NAICS itself defines what each distributes. So each line is
mapped to the NAICS *goods* ranges its own definition names, and those are
expanded to BEA 2017 detail commodities through
`NAICS_to_BEA_Crosswalk_2017.csv`. Nothing here is fitted to the 2017 column.

⚠️ **Built by concept, never by value proximity.** The margins work records the
counter-example: matching on value alone paired `Ships` with switchgear and
`Electronics` with ship building, both nonsense
(`margins_estimation_plan.md`). The three spot checks at the end are checks,
not inputs - they were chosen before the map was written.

Regenerate with:

    uv run python -m scripts.write_inventories_trade_crosswalk
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, cast

import pandas as pd

from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES

#: This script lives in the mapping tree it reads from and writes to, so anchor
#: on its own directory rather than counting parents up to the repo root - that
#: count silently breaks if the file moves, which it already did once.
MAPPING = Path(__file__).resolve().parent
NAICS_BEA = MAPPING / 'naics' / 'NAICS_to_BEA_Crosswalk_2017.csv'
OUT = MAPPING / 'activitytosectormapping' / 'Sector_Crosswalk_BEA_NIPA_Inventories.csv'

#: Durable and nondurable goods NAICS, for the agent/broker lines that carry no
#: commodity specialisation of their own.
_DURABLE = ('321', '327', '331', '332', '333', '334', '335', '336', '337', '339')
_NONDURABLE = (
    '311',
    '312',
    '313',
    '314',
    '315',
    '316',
    '322',
    '323',
    '324',
    '325',
    '326',
)
#: General-merchandise retail spans consumer goods without specialising.
_GENERAL_MERCH = ('311', '312', '315', '316', '325', '334', '335', '337', '339')

#: NIPA line -> (NAICS category it is, NAICS goods it distributes, note).
#: Goods ranges are prefixes, matched against NAICS_2017_Code.
CONCEPT_MAP: dict[str, tuple[str, tuple[str, ...], str]] = {
    # --- wholesale, merchant durable (NAICS 423) ---------------------------
    'C4211': ('4231', ('3361', '3362', '3363'), 'Motor vehicles and parts'),
    'C4212': ('4232', ('337', '3141'), 'Furniture and home furnishings'),
    'C4213': ('4233', ('321', '327', '3323'), 'Lumber and construction materials'),
    'C42143': ('42343', ('3341', '5112'), 'Computers and software'),
    'C4214X': (
        '4234',
        ('3345', '3391', '3333'),
        'Other professional and commercial equipment',
    ),
    'C4215': ('4235', ('331', '2122', '2123'), 'Metals and minerals except petroleum'),
    'C4216': (
        '4236',
        ('335', '3342', '3343', '3344'),
        'Electrical and electronic goods',
    ),
    'C4217': (
        '4237',
        ('3327', '3324', '3329'),
        'Hardware, plumbing and heating equipment',
    ),
    'C4218': ('4238', ('333',), 'Machinery, equipment and supplies'),
    'C4219': ('4239', ('3399', '3149', '3169'), 'Miscellaneous durable goods'),
    # --- wholesale, merchant nondurable (NAICS 424) ------------------------
    'C4221': ('4241', ('322',), 'Paper and paper products'),
    'C4222': ('4242', ('3254',), "Drugs and druggists' sundries"),
    'C4223': ('4243', ('313', '314', '315', '316'), 'Apparel, piece goods and notions'),
    'C4224': ('4244', ('311',), 'Grocery and related products'),
    # 4245 is farm product raw materials; NAICS 42459 covers forestry (113) and
    # fishery (114) products alongside crops and livestock.
    'C4225': ('4245', ('111', '112', '113', '114'), 'Farm product raw materials'),
    'C4226': ('4246', ('325',), 'Chemical and allied products'),
    'C4227': ('4247', ('324',), 'Petroleum and petroleum products'),
    'C4228': ('4248', ('3121',), 'Beer, wine and distilled alcoholic beverages'),
    # 42492 is book, periodical and newspaper merchant wholesalers, so published
    # media (5111) belongs here rather than only with the printers (323).
    'C4229': (
        '4249',
        ('3122', '3169', '3259', '5111'),
        'Miscellaneous nondurable goods',
    ),
    # --- wholesale, agents and brokers -------------------------------------
    'C42ND': ('4251', _DURABLE, 'Nonmerchant wholesale, durable goods'),
    'C42NN': ('4251', _NONDURABLE, 'Nonmerchant wholesale, nondurable goods'),
    # --- retail -------------------------------------------------------------
    'N631RC': ('441', ('3361', '3362', '3363'), 'Motor vehicle and parts dealers'),
    'C4423': (
        '442',
        ('337', '3141', '3352', '3341', '3343'),
        'Furniture, furnishings, electronics and appliance stores',
    ),
    'C444': (
        '444',
        ('321', '327', '3323', '1113'),
        'Building material and garden equipment dealers',
    ),
    'N542RC': ('445', ('311', '3121'), 'Food and beverage stores'),
    'C448': ('448', ('315', '316'), 'Clothing and clothing accessories stores'),
    # ⚠️ 2017 takes the PARENT, N543RC, not the C4521/C4529 children. The two
    # children carry a reclassification in 2017 rather than inventory movement:
    # +21,237 against -24,518, gross 13.9x their own net, where in every other
    # published year 2018-2024 they share a sign and gross equals net exactly.
    # Their parent (-3,281) is ordinary and sits inside the other years' range.
    # Both children map to the same commodity set anyway, so the parent's set is
    # theirs unchanged and no coverage is lost. See the plan, open question 4.
    'N543RC': ('452', _GENERAL_MERCH, 'General merchandise stores'),
    'C4521': ('4521', _GENERAL_MERCH, 'Department stores'),
    'C4529': ('4529', _GENERAL_MERCH, 'Other general merchandise stores'),
    # "Other retail stores" spans the specialist formats: bookstores (45121),
    # video and record stores (45122) and used merchandise stores (45331).
    'C44X': ('44X', _GENERAL_MERCH + ('5111', '512'), 'Other retail stores'),
}

#: `C4229` "Chemical and allied products" excludes drugs, which are `C4222`.
EXCLUDE: dict[str, tuple[str, ...]] = {'C4226': ('3254',)}

#: The crosswalk keys on ``ActivityProducedBy``, which is the NIPA line *name*,
#: not its series code - that is what the FBA carries.
#:
#: ⚠️ ``C42ND`` and ``C42NN`` are published as bare "Durable goods industries"
#: and "Nondurable goods industries", and those names recur at four levels of
#: `U50705BU1` (lines 4/39/42/65 and 17/40/54/66). They cannot be matched on
#: name, so the method renames them via ``assign_fields`` before attribution -
#: the same treatment `FD_IP_equipment_residential` gives U50505 line 46. The
#: disambiguated names below are what the method must assign.
NAMES: dict[str, str] = {
    'C4211': 'Motor vehicles, parts, and supplies wholesalers',
    'C4212': 'Furniture and home furnishings wholesalers',
    'C4213': 'Lumber and other construction materials wholesalers',
    'C42143': 'Computers and software wholesalers',
    'C4214X': 'Other professional and commercial equipment wholesalers',
    'C4215': 'Metal and mineral (except petroleum) wholesalers',
    'C4216': 'Electrical goods wholesalers',
    'C4217': 'Hardware and plumbing and heating equipment wholesalers',
    'C4218': 'Machinery, equipment, and supplies wholesalers',
    'C4219': 'Miscellaneous durable goods wholesalers',
    'C4221': 'Paper and paper products wholesalers',
    'C4222': "Drugs and druggists' sundries wholesalers",
    'C4223': 'Apparel, piece goods, and notions wholesalers',
    'C4224': 'Grocery and related products wholesalers',
    'C4225': 'Farm product raw material wholesalers',
    'C4226': 'Chemical and allied products wholesalers',
    'C4227': 'Petroleum and petroleum products wholesalers',
    'C4228': 'Beer, wine, and distilled alcoholic beverages wholesalers',
    'C4229': 'Miscellaneous nondurable goods wholesalers',
    'C42ND': 'Nonmerchant wholesale, durable goods',  # assigned, not published
    'C42NN': 'Nonmerchant wholesale, nondurable goods',  # assigned, not published
    'N631RC': 'Motor vehicle and parts dealers',
    'C4423': 'Furniture, furnishings, electronics, and appliance stores',
    'C444': 'Building material and garden equipment and supplies dealers',
    'N542RC': 'Food and beverage stores',
    'C448': 'Clothing and clothing accessories stores',
    'N543RC': 'General merchandise stores',
    'C4521': 'Department stores',
    'C4529': 'Other general merchandise stores',
    'C44X': 'Other retail stores',
}

#: Two BEA specials have no NAICS of their own but do have a named trade format:
#: recyclable-material wholesaling (NAICS 42393) sits inside `C4219`, and used
#: merchandise stores (NAICS 45331) sit inside `C44X`.
EXTRA: dict[str, tuple[str, ...]] = {'C4219': ('S00401',), 'C44X': ('S00402',)}


def build() -> pd.DataFrame:
    cw = pd.read_csv(NAICS_BEA, dtype=str).dropna(
        subset=['BEA_2017_Detail_Code', 'NAICS_2017_Code']
    )
    valid = set(USA_2017_COMMODITY_CODES)
    rows = []
    for code, (naics_cat, goods, label) in CONCEPT_MAP.items():
        hit = cw[cw['NAICS_2017_Code'].str.startswith(tuple(goods))]
        commodities = set(hit['BEA_2017_Detail_Code'])
        for drop in EXCLUDE.get(code, ()):
            drop_hit = cw[cw['NAICS_2017_Code'].str.startswith(drop)]
            commodities -= set(drop_hit['BEA_2017_Detail_Code'])
        commodities |= set(EXTRA.get(code, ()))
        commodities &= valid
        for commodity in sorted(commodities):
            rows.append(
                {
                    'ActivitySourceName': 'BEA_NIPA',
                    'Activity': NAMES[code],
                    'SectorSourceName': 'BEA_2017_Code',
                    'Sector': commodity,
                    'SectorType': '',
                    'Note': f'{label}; NAICS {naics_cat}; {code}',
                }
            )
    return pd.DataFrame(rows)


def main() -> None:
    df = build()
    with open(OUT, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                'ActivitySourceName',
                'Activity',
                'SectorSourceName',
                'Sector',
                'SectorType',
                'Note',
            ],
            lineterminator='\r\n',
        )
        w.writeheader()
        # to_dict types its keys Hashable, since DataFrame columns can be any
        # hashable; DictWriter wants str. They are str here by construction -
        # the fieldnames above and the frame's columns are the same literals.
        w.writerows(cast('list[dict[str, Any]]', df.to_dict('records')))

    per = df.groupby('Activity')['Sector'].nunique().sort_values()
    print(f'wrote {OUT.relative_to(MAPPING.parents[2])}')
    print(
        f'  {len(df)} rows, {df.Activity.nunique()} trade lines, '
        f'{df.Sector.nunique()} distinct commodities'
    )
    print(
        f'  commodities per line: min {per.min()}, median {int(per.median())}, '
        f'max {per.max()}'
    )
    empty = [c for c in CONCEPT_MAP if NAMES[c] not in set(df.Activity)]
    print(f'  lines with no commodities: {empty or "none"}')

    # Spot checks from inventories_estimation_plan.md, chosen before the map was
    # written. Concept first, value second - never the other way round.
    checks = [
        (NAMES['C4222'], '325412', "drugs wholesalers -> pharmaceutical preparations"),
        (NAMES['C4227'], '324110', 'petroleum wholesalers -> petroleum refineries'),
        (NAMES['N631RC'], '336111', 'motor vehicle dealers -> automobiles'),
        (NAMES['N631RC'], '336112', 'motor vehicle dealers -> light trucks'),
    ]
    print('  spot checks:')
    for act, sector, why in checks:
        got = ((df.Activity == act) & (df.Sector == sector)).any()
        print(f'    {"OK " if got else "MISS"}  {act} -> {sector}  ({why})')


if __name__ == '__main__':
    main()
