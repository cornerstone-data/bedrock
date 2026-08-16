"""Flag inherited product -> NAICS matches that are conceptually wrong (#529).

⚠️ **The product identifier is not NAPCS.** The Economic Census Products by
Industry API calls its field ``NAPCS2017`` and labels it *"2017 NAPCS collection
code"*, but it is a separate code set from the published NAPCS classification -
none of the 620 trade product codes appear in the official 2017 NAPCS structure
or definitions, and joining on title recovers only 11% of goods value. It is
called the **Census_2017_PxI_product_code** here to keep the two apart. See
``bedrock/extract/census/Census_EC_PxI.yaml`` for the measurements.

Matching is therefore keyed on the product **description**, not the code: the
codes also churn across vintages (525 new in 2022, 548 gone) with the large new
ones being recodings of products already present in 2017.

The seed in ``bedrock/utils/mapping/census_pxi/pxi_product_naics_seed_2017.csv`` comes from two
places: a manual matching effort (May 2025, 30 products) and a GovAI agent run
(247). Per
the direction on 2026-08-16 the GovAI half is **provisional** - accepted only
until something contradicts it - and any row associating a good with whoever
*sells* it is disqualified outright.

Three kinds of flag, in descending order of how mechanical they are:

``seller``
    The NAICS is wholesale or retail (42/44/45). A trade industry is never what
    a good *is*, so this is a rule rather than a judgement. It is the same
    inversion that disqualified the derived NAPCS -> NAICS -> BEA composition:
    "Wholesale sales of refined petroleum products" resolving to construction.

``bad_code``
    The NAICS is absent from the Census 2017 reference list. Some are prior
    vintages (``333313`` is 2012), some are malformed (``332100`` is not a
    six-digit industry). A few may be real 2017 codes the reference omits, so
    these are flagged for checking rather than deleted.

``concept``
    Read by hand against the product description. These are the ones no check
    catches, and the reason the GovAI rows cannot be taken on trust: the code is
    valid, is not a trade industry, and is still wrong.

Scrap and used goods are called out separately because they are not NAICS
problems at all - BEA carries them as ``S00401`` and ``S00402``, which have no
NAICS counterpart, so no NAICS match for them can ever be right.
"""

from __future__ import annotations

import pandas as pd

SEED = 'bedrock/utils/mapping/census_pxi/pxi_product_naics_seed_2017.csv'
OUT = 'bedrock/utils/mapping/census_pxi/pxi_product_naics_seed_2017_review.csv'


#: (product, assigned NAICS) -> why it is wrong, and where it should point.
#: Keyed on the pair so a product that is fine under one match is not flagged
#: because another row got it wrong.
CONCEPT_FLAGS: dict[tuple[str, str], str] = {
    # --- scrap and recyclables: BEA specials, no NAICS can be right ---
    ('recyclable ferrous metal scrap', '331110'):
        'scrap -> BEA S00401; a steel mill consumes scrap, it is not scrap',
    ('recyclable nonferrous metal scrap', '331420'):
        'scrap -> BEA S00401',
    ('recyclable paper and paperboard', '322130'):
        'scrap -> BEA S00401; paperboard mills consume it',
    ('recyclable plastics and rubber', '325992'):
        'scrap -> BEA S00401; photographic film manufacturing is unrelated',
    ('recyclable textiles, including rags and textile scraps', '313310'):
        'scrap -> BEA S00401; finishing mills are unrelated',

    # --- fresh vs processed: the description says the opposite of the match ---
    ('fresh fruit and vegetables', '311421'):
        '"fresh" excludes canning; -> 1112/1113 farming',
    ('fish and seafood, except canned and frozen fish and seafood', '3117'):
        '"except canned and frozen" excludes packaging; -> 1141/1125',
    ('fresh fish and seafood', '311710'):
        '"fresh" excludes packaging; -> 1141 fishing / 1125 aquaculture',

    # --- farm output matched to processors ---
    ('grains, beans, and seeds', '311119'):
        'farm output -> 1111 oilseed and grain farming, not animal food mfg',
    ('hides, skins, and pelts', '311119'):
        'slaughter byproduct -> 311611 / 316110, not animal food mfg',
    ('livestock', '311511'):
        'live animals -> 1121/1122/1124, not fluid milk mfg',
    ('eggs and dairy (except ice cream)', '311511'):
        'eggs are 1123; only the dairy half fits 311511',
    ('pulpwood', '321113'):
        'standing/roundwood -> 1133 logging, not sawmills',
    ('nonlumber forest products, including hewn posts, poles, and railroad ties',
     '321114'):
        '-> 1133 logging rather than wood preservation',

    # --- product matched to an input or a service ---
    ('liquefied petroleum (lp)', '486110'):
        'LP is a product (324110/325120); 486110 is pipeline transport',
    ('materials and supplies for tobacco manufacturing', '312230'):
        'inputs to tobacco mfg (leaf, 111910), not tobacco products',
    ('materials and supplies for glass products manufacturing', '327212'):
        'inputs to glass mfg, not pressed and blown glass',

    # --- plainly the wrong industry ---
    ('harness and saddlery equipment', '316992'):
        '-> 316990/316999; 316992 is handbags and purses',
    ('cutlery, except disposable plastics', '339910'):
        '-> 332215 cutlery and flatware, not jewelry and silverware',
    ('fire extinguishers and fire safety equipment', '339994'):
        '-> 332999/339999; 339994 is brooms, brushes and mops',
    ('religious supplies', '339994'):
        '-> 339999; 339994 is brooms, brushes and mops',
    ('store equipment', '333415'):
        '-> 333318 commercial and service industry equipment, not HVAC',
    ('stainless steel', '331210'):
        '-> 331110/331221 mills and rolled shapes, not pipe and tube',
    ('nonpackaging paper and plastic', '326199'):
        'the paper half is 322xxx; 326199 is plastics only',

    # --- too broad for the match, or a mixed category ---
    ('alcoholic beverages', '312140'):
        'spans beer 312120 and wine 312130 as well as distilleries',
    ('wine and distilled liquor, including premixed alcoholic drinks', '31213'):
        'distilled liquor is 312140, not wineries',
}

#: Products that are broad mixed-goods categories. Per the 2026-08-16 direction
#: these are left unmapped rather than forced. They are 4.9% of 2017 trade goods
#: value ($0.62T of $12.74T), so excluding them is affordable - unlike the
#: analogous "Other goods" bucket in the truck margin, which is a third of it.
MIXED_MARKERS = (
    'not elsewhere classified',
    'other goods',
)


def main() -> None:
    seed = pd.read_csv(SEED, dtype=str)
    ref = pd.read_csv(
        'bedrock/utils/mapping/naics/Sector_2017_Names.csv', dtype=str
    )
    valid = set(ref['NAICS_2017_Code'].dropna())

    # The industry name is what a reviewer actually judges the match against -
    # "339994" says nothing, "Broom, Brush, and Mop Manufacturing" next to
    # "fire extinguishers" is self-evidently wrong. Carried through as its own
    # column rather than folded into the reason text so the file can be sorted
    # and filtered on it.
    seed['naics_name'] = seed['naics_2017'].map(
        dict(zip(ref['NAICS_2017_Code'], ref['NAICS_2017_Name']))
    )

    matched = seed['naics_2017'].notna()
    s2 = seed['naics_2017'].fillna('').str[:2]

    seed['flag'] = ''
    seed['flag_reason'] = ''

    seller = matched & s2.isin(['42', '44', '45'])
    seed.loc[seller, 'flag'] = 'seller'
    seed.loc[seller, 'flag_reason'] = (
        'maps a good to the industry that sells it, not the one that makes it'
    )

    bad = matched & ~seed['naics_2017'].isin(valid) & ~seller
    seed.loc[bad, 'flag'] = 'bad_code'
    seed.loc[bad, 'flag_reason'] = 'not in the Census 2017 NAICS reference list'

    for (product, naics), why in CONCEPT_FLAGS.items():
        hit = (seed['product'] == product) & (seed['naics_2017'] == naics)
        if not hit.any():
            print(f'  ! concept flag no longer matches a row: {product} / {naics}')
            continue
        seed.loc[hit & (seed['flag'] == ''), 'flag'] = 'concept'
        seed.loc[hit & (seed['flag'] == 'concept'), 'flag_reason'] = why

    mixed = seed['product'].str.contains('|'.join(MIXED_MARKERS), case=False,
                                         regex=True, na=False)
    seed.loc[mixed & (seed['flag'] == ''), 'flag'] = 'mixed'
    seed.loc[seed['flag'] == 'mixed', 'flag_reason'] = (
        'broad mixed-goods category; left unmapped by decision'
    )

    # Flagged rows first and grouped by flag, so a reviewer works down the file
    # rather than hunting through 241 correct ones. Within a flag, order by the
    # industry so related mistakes sit together.
    flag_order = {'seller': 0, 'bad_code': 1, 'concept': 2, 'mixed': 3, '': 4}
    seed = (
        seed.assign(_o=seed['flag'].map(flag_order))
        .sort_values(['_o', 'naics_2017', 'product'], na_position='last')
        .drop(columns='_o')
    )
    seed = seed[
        [
            'flag',
            'flag_reason',
            'product',
            'naics_2017',
            'naics_name',
            'match_source',
            'description',
            'census_2017_pxi_product_code',
        ]
    ]
    seed.to_csv(OUT, index=False, encoding='utf-8')

    print(f'{len(seed)} seed rows, {seed["product"].nunique()} products')
    counts = seed.loc[seed['flag'] != '', 'flag'].value_counts()
    for k in ('seller', 'bad_code', 'concept', 'mixed'):
        n = int(counts.get(k, 0))
        prod = seed.loc[seed['flag'] == k, 'product'].nunique()
        print(f'  {k:<9} {n:>3} rows  {prod:>3} products')
    clean = seed[(seed['flag'] == '') & matched]
    print(f'  {"clean":<9} {len(clean):>3} rows  {clean["product"].nunique():>3} products')
    print(f'wrote {OUT}')


if __name__ == '__main__':
    main()
