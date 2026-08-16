"""Build the Census PxI product -> BEA 2017 detail commodity crosswalk (#529).

The allocation source for the change-in-inventories merchandise-trade rule: for
each wholesale/retail product line the Economic Census publishes, which BEA
commodity is actually being held.

⚠️ **Keyed on the product description, not the product code.** The identifier
in ``Census_EC_PxI`` is the *Census_2017_PxI_product_code* - Census's "2017 NAPCS
collection code", which is **not** the NAPCS classification and is not stable
across vintages (525 codes new in 2022, 548 gone, with the large new ones being
recodings of products already present in 2017). Keying on the code would drop
roughly 15% of coverage at the 2022 boundary. See ``Census_EC_PxI.yaml``.

**Why not derive this mechanically.** Composing product -> NAICS -> BEA through
the published NAICS/NAPCS crosswalk fails, because those crosswalks answer "which
industries *sell* this" rather than "what *is* this": "Wholesale sales of refined
petroleum products" resolves to construction commodities, and only 3 of 620 trade
product lines - 0% of trade value - land on a single BEA commodity. The mapping
has to be by concept, which is what the seed provides.

**Inputs**

``pxi_product_naics_seed_2017.csv``
    Product -> NAICS, from a manual matching effort (30 products) and a GovAI
    agent run (247). The GovAI half is provisional.

``pxi_product_naics_seed_2017_review.csv``
    Written by ``review_pxi_product_naics_seed.py``. Flags 53 seed rows as
    unusable: ``seller`` (maps a good to whoever sells it), ``bad_code``
    (not a 2017 NAICS), ``concept`` (valid code, wrong industry - 26 of these,
    caught only by reading).

``pxi_product_corrections_2017.csv``
    Replacements for every flagged product, one row per target so a product can
    span several commodities. Targets are ``NAICS`` (resolved to BEA here) or
    ``BEA`` directly, the latter for commodities with no NAICS counterpart at
    all - ``S00401`` scrap and ``S00402`` used and secondhand goods.

**Excluded by decision**: broad mixed-goods lines ("... not elsewhere
classified", "other goods"). They are 4.9% of 2017 trade goods value, small
enough to leave unallocated rather than force - unlike the analogous "Other
goods" bucket in the truck transport margin, which is a third of it.
"""

from __future__ import annotations

import pandas as pd

SEED = 'bedrock/utils/mapping/census_pxi/pxi_product_naics_seed_2017.csv'
REVIEW = 'bedrock/utils/mapping/census_pxi/pxi_product_naics_seed_2017_review.csv'
CORRECTIONS = 'bedrock/utils/mapping/census_pxi/pxi_product_corrections_2017.csv'
NAICS_TO_BEA = 'bedrock/utils/mapping/naics/NAICS_to_BEA_Crosswalk_2017.csv'
OUT = (
    'bedrock/utils/mapping/activitytosectormapping/'
    'Sector_Crosswalk_Census_EC_PxI.csv'
)

MIXED_MARKERS = ('not elsewhere classified', 'other goods')
UNUSABLE = ('seller', 'bad_code', 'concept')


def main() -> None:
    review = pd.read_csv(REVIEW, dtype=str)
    corrections = pd.read_csv(CORRECTIONS, dtype=str)
    n2b = pd.read_csv(NAICS_TO_BEA, dtype=str).dropna(
        subset=['NAICS_2017_Code', 'BEA_2017_Detail_Code']
    )

    mixed = review['product'].str.contains(
        '|'.join(MIXED_MARKERS), case=False, regex=True, na=False
    )
    review = review[~mixed]

    kept = review[
        review['flag'].isna() & review['naics_2017'].notna()
    ][['product', 'naics_2017']].assign(origin='seed')

    corr_naics = corrections[corrections['target_scheme'] == 'NAICS'].rename(
        columns={'target': 'naics_2017'}
    )[['product', 'naics_2017']].assign(origin='corrected')
    corr_bea = corrections[corrections['target_scheme'] == 'BEA'].rename(
        columns={'target': 'bea'}
    )[['product', 'bea']].assign(origin='direct-BEA')

    # A corrected product replaces its seed rows outright rather than adding to
    # them - the seed row is the one that was wrong.
    replaced = set(corrections['product'])
    kept = kept[~kept['product'].isin(replaced)]

    via_naics = pd.concat([kept, corr_naics], ignore_index=True)
    resolved = via_naics.merge(
        n2b[['NAICS_2017_Code', 'BEA_2017_Detail_Code']],
        left_on='naics_2017',
        right_on='NAICS_2017_Code',
        how='left',
    ).rename(columns={'BEA_2017_Detail_Code': 'bea'})

    unresolved = resolved[resolved['bea'].isna()]
    if len(unresolved):
        print(f'  ! {len(unresolved)} rows did not resolve NAICS -> BEA:')
        for _, r in unresolved.head(10).iterrows():
            print(f'      {r["naics_2017"]}  {r["product"][:56]}')

    out = pd.concat(
        [
            resolved.dropna(subset=['bea'])[
                ['product', 'bea', 'origin', 'naics_2017']
            ],
            corr_bea.assign(naics_2017=pd.NA),
        ],
        ignore_index=True,
    ).drop_duplicates(subset=['product', 'bea'])

    out['Note'] = out.apply(
        lambda r: (
            f'{r["origin"]}'
            + (f'; via NAICS {r["naics_2017"]}' if pd.notna(r['naics_2017'])
               else '; no NAICS counterpart')
        ),
        axis=1,
    )
    crosswalk = pd.DataFrame(
        {
            'ActivitySourceName': 'Census_EC_PxI',
            'Activity': out['product'],
            'SectorSourceName': 'BEA_2017_Code',
            'Sector': out['bea'],
            'SectorType': '',
            'Note': out['Note'],
        }
    ).sort_values(['Activity', 'Sector'])

    crosswalk.to_csv(OUT, index=False, encoding='utf-8')

    per = crosswalk.groupby('Activity')['Sector'].nunique()
    print(f'{len(crosswalk)} rows, {crosswalk["Activity"].nunique()} products')
    print(f'  1 BEA commodity  : {(per == 1).sum()}')
    print(f'  2-3              : {per.between(2, 3).sum()}')
    print(f'  4+               : {(per >= 4).sum()}  max {per.max()}')
    print(f'  direct-BEA rows  : {(out["origin"] == "direct-BEA").sum()}')
    print(f'  corrected rows   : {(out["origin"] == "corrected").sum()}')
    print(f'wrote {OUT}')


if __name__ == '__main__':
    main()
