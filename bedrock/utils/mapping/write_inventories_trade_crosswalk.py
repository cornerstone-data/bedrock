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
#: code -> (NAICS the line covers, goods NAICS it distributes, label).
#: The first element is usually one prefix, but a line spanning several
#: takes a tuple - see C44X.
CONCEPT_MAP: dict[str, tuple[str | tuple[str, ...], tuple[str, ...], str]] = {
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
    # NAICS 4238 is not only machinery. It contains 42386 transportation
    # equipment and supplies (except motor vehicle) merchant wholesalers -
    # aircraft, ships and railroad equipment - which carried 53,543 of product
    # lines in 2017, 38,923 of it aircraft. Without 3364/3365/3366 those
    # commodities were reachable only through the nonmerchant agent and broker
    # line, which spreads across 151 commodities.
    #
    # ⚠️ This is a reach fix, NOT the explanation for 336411. Aircraft are sold
    # direct from manufacturer to airline, so the inventory is held by the
    # maker, not a distributor: line 14 C336OT other transportation equipment
    # manufacturing is -5,696 against a published 336411 of -6,314, essentially
    # the whole cell. Adding these prefixes moved 336411 by 54. The cell is a
    # manufacturing-branch object.
    'C4218': (
        '4238',
        ('333', '3364', '3365', '3366'),
        'Machinery, equipment and supplies',
    ),
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
    # ⚠️ NIPA's C4521/C4529 are 2012 NAICS. The 2017 revision renumbered the
    # whole 452 group: department stores are 4522 and the rest is 4523, general
    # merchandise including warehouse clubs and supercenters. Neither 4521 nor
    # 4529 exists in NAICS 2017 - the prefix guard below catches them.
    #
    # That renumbering is almost certainly why these two lines carry a
    # reclassification in 2017 rather than inventory movement, which is the
    # separate reason 2017 takes their parent N543RC instead. See
    # Inventories_2017.yaml.
    'C4521': ('4522', _GENERAL_MERCH, 'Department stores'),
    'C4529': ('4523', _GENERAL_MERCH, 'Other general merchandise stores'),
    # "Other retail stores" spans the specialist formats: bookstores (45121),
    # video and record stores (45122) and used merchandise stores (45331).
    # ⚠️ '44X' was a pseudo-code, and nothing downstream could match it: the
    # PxI weighting matches holding industries with startswith, and no real
    # NAICS begins '44X'. The line therefore drew no weights at all and fell
    # back to an equal split, stranding S00402 at 54 against a published
    # 3,969. The formats it actually spans are 451 sporting goods, hobby,
    # book and music stores, 453 miscellaneous store retailers (which is
    # where used merchandise sits) and 454 nonstore retailers.
    'C44X': (
        ('451', '453', '454'),
        _GENERAL_MERCH + ('5111', '512'),
        'Other retail stores',
    ),
}

#: `C4229` "Chemical and allied products" excludes drugs, which are `C4222`.
#: The non-trade branches. Manufacturing follows BEA's finished-goods and
#: work-in-process rule - an industry holds its own products - so each line maps
#: to the commodities its NAICS produces. Mining takes only mining commodities:
#: the published F03000 contains **no utilities or construction commodities at
#: all**, so the branch labelled "mining, utilities and construction" lands
#: entirely on mining in commodity space.
#:
#: ⚠️ C336MV and C336OT are pseudo-codes - no NAICS begins with either, and the
#: prefix guard below rejects them. NIPA splits transportation equipment into
#: motor vehicles and everything else, so they are given the NAICS ranges they
#: stand for. Keeping them apart matters: C336OT is -5,696 against a published
#: 336411 of -6,314, and folding it into the parent spreads an aerospace
#: movement across pickup trucks.
NON_TRADE_MAP: dict[str, tuple[str | tuple[str, ...], tuple[str, ...], str]] = {
    'C321': ('321', ('321',), 'Wood products'),
    'C327': ('327', ('327',), 'Nonmetallic mineral products'),
    'C331': ('331', ('331',), 'Primary metals'),
    'C332': ('332', ('332',), 'Fabricated metal products'),
    'C333': ('333', ('333',), 'Machinery'),
    'C334': ('334', ('334',), 'Computer and electronic products'),
    'C335': ('335', ('335',), 'Electrical equipment and appliances'),
    'C336MV': (
        ('3361', '3362', '3363'),
        ('3361', '3362', '3363'),
        'Motor vehicles and parts',
    ),
    'C336OT': (
        ('3364', '3365', '3366', '3369'),
        ('3364', '3365', '3366', '3369'),
        'Other transportation equipment',
    ),
    'C337': ('337', ('337',), 'Furniture'),
    'C339': ('339', ('339',), 'Miscellaneous durable goods'),
    'C311': ('311', ('311',), 'Food'),
    'C312': ('312', ('312',), 'Beverages and tobacco'),
    'C313': ('313', ('313',), 'Textile mills'),
    'C314': ('314', ('314',), 'Textile product mills'),
    'C315': ('315', ('315',), 'Apparel'),
    'C316': ('316', ('316',), 'Leather and allied products'),
    'C322': ('322', ('322',), 'Paper'),
    'C323': ('323', ('323',), 'Printing'),
    'C324': ('324', ('324',), 'Petroleum and coal products'),
    'C325': ('325', ('325',), 'Chemicals'),
    'C326': ('326', ('326',), 'Plastics and rubber products'),
    # ⚠️ Split unresolved, deferred to #660 - these reach the right commodity
    # set but carry no rule for dividing within it.
    'N541RC': ('21', ('211', '212', '213'), 'Mining, utilities and construction'),
    'B018RC': ('111', ('111', '112'), 'Farm'),
}

#: Names as U50705BU1 and T50705B publish them, which is what the FBA carries.
NON_TRADE_NAMES: dict[str, str] = {
    'C321': 'Wood product manufacturing',
    'C327': 'Nonmetallic mineral product manufacturing',
    'C331': 'Primary metal manufacturing',
    'C332': 'Fabricated metal product manufacturing',
    'C333': 'Machinery manufacturing',
    'C334': 'Computer and electronic product manufacturing',
    'C335': 'Electrical equipment, appliance, and component manufacturing',
    'C336MV': 'Motor vehicle and parts manufacturing',
    'C336OT': 'Other transportation equipment manufacturing',
    'C337': 'Furniture and related product manufacturing',
    'C339': 'Miscellaneous durable goods manufacturing',
    'C311': 'Food manufacturing',
    'C312': 'Beverage and tobacco product manufacturing',
    'C313': 'Textile mills',
    'C314': 'Textile product mills',
    'C315': 'Apparel manufacturing',
    'C316': 'Leather and allied product manufacturing',
    'C322': 'Paper manufacturing',
    'C323': 'Printing and related support activities',
    'C324': 'Petroleum and coal product manufacturing',
    'C325': 'Chemical manufacturing',
    'C326': 'Plastics and rubber product manufacturing',
    'N541RC': 'Mining, utilities, and construction',
    'B018RC': 'Farm',
}

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
    for code, (naics_cat, goods, label) in {**CONCEPT_MAP, **NON_TRADE_MAP}.items():
        # A line may span several NAICS prefixes; render them slash-joined so
        # the Note stays parseable by anything deriving weights from it.
        naics_note = '/'.join(naics_cat) if isinstance(naics_cat, tuple) else naics_cat
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
                    'Activity': {**NAMES, **NON_TRADE_NAMES}[code],
                    'SectorSourceName': 'BEA_2017_Code',
                    'Sector': commodity,
                    'SectorType': '',
                    'Note': f'{label}; NAICS {naics_note}; {code}',
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
    all_names = {**NAMES, **NON_TRADE_NAMES}
    empty = [
        c
        for c in {**CONCEPT_MAP, **NON_TRADE_MAP}
        if all_names[c] not in set(df.Activity)
    ]
    print(f'  lines with no commodities: {empty or "none"}')

    _assert_naics_are_real()

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


def _assert_naics_are_real() -> None:
    """Every NAICS this map advertises must be a real 2017 prefix.

    ⚠️ This guards a failure that is silent rather than loud, and which has now
    bitten three times. NIPA publishes some alphanumeric codes - ``44X`` for
    other retail stores, ``336MV`` and ``336OT`` for the two halves of
    transportation equipment - that look like NAICS but are not. Anything
    matching holding industries with ``startswith`` then finds **nothing** and
    carries on: the retail line silently fell back to an equal split, stranding
    ``S00402`` at 54 against a published 3,969, and both transport-equipment
    children contributed zero to the manufacturing allocation.

    An empty match is indistinguishable from "no data" unless something checks,
    so this checks. The ``goods`` tuples drive the crosswalk itself and are
    already covered by the empty-line report above; what this adds is the
    ``naics_cat`` field, which is documentation in this file but is parsed
    downstream to derive attribution weights.
    """
    names = pd.read_csv(MAPPING / 'naics' / 'Sector_2017_Names.csv', dtype=str)
    real = set(names['NAICS_2017_Code'].dropna())

    def covers(prefix: str) -> bool:
        return any(code.startswith(prefix) for code in real)

    bad: list[str] = []
    for code, (naics_cat, _goods, _label) in {
        **CONCEPT_MAP,
        **NON_TRADE_MAP,
    }.items():
        prefixes = naics_cat if isinstance(naics_cat, tuple) else (naics_cat,)
        bad.extend(
            f'{code}: {p!r} matches no 2017 NAICS' for p in prefixes if not covers(p)
        )
    if bad:
        raise ValueError(
            'CONCEPT_MAP advertises NAICS prefixes that do not exist, which '
            'would silently yield no weights downstream: ' + '; '.join(bad)
        )
    checked = len(CONCEPT_MAP) + len(NON_TRADE_MAP)
    print(f'  NAICS prefixes checked against the 2017 list: all {checked} ok')


if __name__ == '__main__':
    main()
