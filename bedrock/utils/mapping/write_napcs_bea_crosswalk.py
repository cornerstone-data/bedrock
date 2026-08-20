"""
Build the NAPCS collection code -> BEA 2017 detail commodity crosswalk (#615).

This is the shared asset for every product-based commodity output build: the
Economic Census (`Census_EC_PxI`, 2017 and 2022) and the Annual Survey of
Manufactures (`asm/value2017`, 2018-) are keyed on the **same** 2017 NAPCS
collection codes, so one crosswalk serves all of them.

**Nothing here is judgement except the overrides file.** The chain is published
crosswalks end to end:

```
NAPCS collection code -> 2012 EC product code -> NAICS -> BEA 2017 commodity
```

Census's *2017 NAPCS-Based Collection Code to 2012 Product Code* concordance
supplies the first link, and 2012 Economic Census product codes **embed NAICS in
their first six digits** (``21111131`` is NAICS ``211111`` plus a product
suffix), which supplies the second.

⚠️ **The codes in the "2012 Code" columns are not all 2012 vintage.** 172 of the
473 manufacturing codes are **2007** codes - ``311222`` Soybean Processing became
``311224`` in *2012* - because a product line carried forward unchanged keeps the
NAICS it was created under. Resolving only against a 2012 column silently drops
them, which cost 12% of manufacturing commodity output and left 32 commodities
with no source at all. The vintage chain below is what fixes that, and the order
matters: try each published vintage before falling back.

⚠️ **The chain stops at 2007.** Extending it to ``NAICS_2002_Code`` recovers
**zero** additional codes across all sectors, so a 2002 step is dead weight. The
three codes no vintage resolves are hand-mapped in
``pxi_naics_vintage_overrides.csv`` with the evidence for each.

**Splitting rather than taking the mode.** A NAPCS code whose 2012 product codes
span several BEA commodities is split across them, weighted by how many resolve
to each. Only 2.1% of codes are multi-target so this is a small effect (weighted
error 14.1% to 13.8% on 2017), but taking the mode starves any commodity that is
never a modal target, and 33 were.

Validated by ``test_napcs_bea_crosswalk.py`` against the published 2017 detail
Supply table.

Run: ``uv run python bedrock/utils/mapping/write_napcs_bea_crosswalk.py``
"""

from __future__ import annotations

import pandas as pd

#: Census's published concordance. Downloaded from
#: https://www2.census.gov/programs-surveys/economic-census/technical-documentation/
#: napcs/2017_NAPCS-Based_Collection_Code_to_2012_Product_Code_20200312_no_highlight.xlsx
SOURCE = (
    'bedrock/extract/input_data/taxonomy/'
    '2017_NAPCS_Collection_Code_to_2012_Product_Code.xlsx'
)
SHEET = 'NEW 2017 NAPCS to 2012 Products'
NAICS_TO_BEA = 'bedrock/utils/mapping/naics/NAICS_to_BEA_Crosswalk_2017.csv'
YEAR_CONCORDANCE = 'bedrock/utils/mapping/naics/NAICS_Year_Concordance.csv'
OVERRIDES = 'bedrock/utils/mapping/census_pxi/pxi_naics_vintage_overrides.csv'
OUT = 'bedrock/utils/mapping/census_pxi/napcs_to_bea_2017.csv'

CODE = '2017 NAPCS Based Collection Code'
DESCRIPTION = '2017 NAPCS Based Description'

#: Vintages to try, in order. ``NAICS_2002_Code`` is deliberately absent - it
#: resolves nothing that 2007 does not.
VINTAGES = ('NAICS_2012_Code', 'NAICS_2017_Code', 'NAICS_2007_Code')


def naics_to_bea() -> dict[str, dict[str, str]]:
    """One NAICS -> BEA map per vintage, keyed by the vintage's column name."""
    crosswalk = pd.read_csv(NAICS_TO_BEA, dtype=str)
    year_concordance = pd.read_csv(YEAR_CONCORDANCE, dtype=str)

    maps: dict[str, dict[str, str]] = {}
    for column in ('NAICS_2012_Code', 'NAICS_2017_Code'):
        rows = crosswalk.dropna(subset=[column, 'BEA_2017_Detail_Code'])
        maps[column] = (
            rows.assign(key=rows[column].str.strip())
            .groupby('key')['BEA_2017_Detail_Code']
            .first()
            .str.strip()
            .to_dict()
        )

    # 2007 has no column in the BEA crosswalk, so it routes through 2017. A 2007
    # code that split into several 2017 codes is only usable when they all land
    # on the same BEA commodity - otherwise the choice would be ours, not the
    # concordance's, and it is left for the overrides file.
    rows = year_concordance.dropna(subset=['NAICS_2007_Code', 'NAICS_2017_Code'])
    to_2017 = (
        rows.assign(
            key=rows['NAICS_2007_Code'].str.strip(),
            value=rows['NAICS_2017_Code'].str.strip(),
        )
        .groupby('key')['value']
        .apply(lambda codes: sorted(set(codes)))
        .to_dict()
    )
    maps['NAICS_2007_Code'] = {}
    for code, successors in to_2017.items():
        targets = {maps['NAICS_2017_Code'].get(s) for s in successors}
        targets.discard(None)
        if len(targets) == 1:
            maps['NAICS_2007_Code'][code] = targets.pop()
    return maps


def resolve(naics: str, maps: dict[str, dict[str, str]], overrides: dict[str, str]):
    """A NAICS code from the Census file to (BEA commodity, how it resolved)."""
    if naics in overrides:
        return overrides[naics], 'override'
    for vintage in VINTAGES:
        if naics in maps[vintage]:
            return maps[vintage][naics], vintage
    return None, 'unresolved'


def build() -> pd.DataFrame:
    source = pd.read_excel(SOURCE, sheet_name=SHEET, dtype=str)
    product_columns = [c for c in source.columns if c.startswith('2012 Code')]
    pairs = source.melt(
        id_vars=[CODE, DESCRIPTION], value_vars=product_columns, value_name='product'
    ).dropna(subset=['product'])

    # "(PT)" marks a partial mapping; the NAICS is still the first six digits
    pairs['naics'] = (
        pairs['product'].str.replace(r'\(PT\)', '', regex=True).str.strip().str[:6]
    )
    pairs = pairs[pairs['naics'].str.match(r'^\d{6}$')]

    maps = naics_to_bea()
    override_rows = pd.read_csv(OVERRIDES, dtype=str)
    overrides = dict(
        zip(
            override_rows['naics_in_census_file'].str.strip(),
            override_rows['bea_2017_commodity'].str.strip(),
        )
    )
    resolved = {n: resolve(n, maps, overrides) for n in pairs['naics'].unique()}
    pairs['bea'] = pairs['naics'].map(lambda n: resolved[n][0])
    pairs['resolution'] = pairs['naics'].map(lambda n: resolved[n][1])

    mapped = pairs.dropna(subset=['bea'])
    counts = mapped.groupby([CODE, 'bea']).size().reset_index(name='n')
    counts['weight'] = counts['n'] / counts.groupby(CODE)['n'].transform('sum')

    how = mapped.groupby([CODE, 'bea'])['resolution'].first().reset_index()
    description = mapped.groupby(CODE)[DESCRIPTION].first()

    crosswalk = counts.merge(how, on=[CODE, 'bea']).assign(
        napcs_description=lambda d: d[CODE].map(description)
    )
    return (
        crosswalk.rename(columns={CODE: 'napcs_code', 'bea': 'bea_2017_commodity'})[
            [
                'napcs_code',
                'bea_2017_commodity',
                'weight',
                'resolution',
                'napcs_description',
            ]
        ]
        .sort_values(['napcs_code', 'bea_2017_commodity'])
        .reset_index(drop=True)
    )


def main() -> None:
    crosswalk = build()
    crosswalk.to_csv(OUT, index=False, encoding='utf-8')
    codes = crosswalk['napcs_code'].nunique()
    multi = (crosswalk.groupby('napcs_code').size() > 1).sum()
    print(f'wrote {OUT}')
    print(
        f'  {len(crosswalk)} rows | {codes} NAPCS codes | {multi} split across commodities'
    )
    print(f'  {crosswalk["bea_2017_commodity"].nunique()} BEA commodities reached')
    print('  resolution:')
    for how, n in crosswalk['resolution'].value_counts().items():
        print(f'    {how:<18} {n}')


if __name__ == '__main__':
    main()
