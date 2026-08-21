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

**Two outputs.** ``napcs_to_bea_2017.csv`` is the analytical form, carrying the
split ``weight`` and how each code resolved. ``Sector_Crosswalk_Census_ASM_PxI.csv``
is the same content in ``activity_to_sector_mapping`` shape for the FBS methods -
without the weights, which that format cannot carry. See
:func:`write_sector_crosswalk` for what that costs.

**Validate with** ``--validate``, which builds 2017 commodity output from the
Economic Census through this crosswalk and scores it against the published 2017
detail Supply block - the answer. That is the check the accuracy claims above
rest on, and it is a flag rather than a unit test because it needs the
``Census_EC_PxI`` FBA and the BEA workbooks (see
``analysis/nowcasting/README.md`` on why diagnostics are CLI flags here).

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
#: The same content in ``activity_to_sector_mapping`` shape, for FBS methods
#: that read a PxI source directly rather than joining the csv by hand.
SECTOR_CROSSWALK = (
    'bedrock/utils/mapping/activitytosectormapping/Sector_Crosswalk_Census_ASM_PxI.csv'
)

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
        targets = {
            mapped
            for s in successors
            if (mapped := maps['NAICS_2017_Code'].get(s)) is not None
        }
        if len(targets) == 1:
            maps['NAICS_2007_Code'][code] = targets.pop()
    return maps


def resolve(
    naics: str, maps: dict[str, dict[str, str]], overrides: dict[str, str]
) -> tuple[str | None, str]:
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


def write_sector_crosswalk(crosswalk: pd.DataFrame) -> pd.DataFrame:
    """Re-express the crosswalk as an ``activity_to_sector_mapping`` file.

    ``Commodity_output_manufacturing`` reads ``Census_ASM_PxI`` through the
    standard FBS machinery, which joins its crosswalk on the **activity**
    columns. ``move_asm_product_to_activity`` puts the NAPCS code there, so the
    ``Activity`` here is the 10-digit collection code and the ``Sector`` is the
    BEA 2017 detail commodity.

    ⚠️ **The ``weight`` column does not survive the trip.** The
    activity-to-sector format carries no weight, so a code split across several
    commodities is attributed **equally** by ``attribution_method: equal``. That
    is exact for 138 of the 156 split codes - the splits are mostly two-way and
    already 0.5/0.5 - and approximate for the other 18. Measured on the 2018 and
    2021 manufacturing build, equal attribution costs **0.16-0.19 percentage
    points** of weighted error (4.47% -> 4.63% in 2018, 6.72% -> 6.91% in 2021)
    and changes the level not at all. That is the price of staying on the
    standard machinery, and it is recorded here so the choice is visible rather
    than inferred from a missing column.

    ⚠️ **Also usable by ``Census_EC_PxI``** - the Economic Census is keyed on the
    same 2017 collection codes - by pointing that source's
    ``activity_to_sector_mapping`` at this file. It is not named for both
    sources because ``Sector_Crosswalk_Census_EC_PxI`` already exists and is
    keyed on the product *description* for the trade work, and because the 2022
    vintage question (#650) is unsettled: 525 codes are new in 2022 and 548
    gone, so a code-keyed join is only known good for 2017.
    """
    out = pd.DataFrame(
        {
            'ActivitySourceName': 'Census_ASM_PxI',
            'Activity': crosswalk['napcs_code'],
            'SectorSourceName': 'BEA_2017_Code',
            'Sector': crosswalk['bea_2017_commodity'],
            'SectorType': '',
            'Note': crosswalk.apply(
                lambda r: (
                    f'{r["resolution"]}; weight {r["weight"]:.3f}; '
                    f'{r["napcs_description"]}'
                ),
                axis=1,
            ),
        }
    ).sort_values(['Activity', 'Sector'])
    out.to_csv(SECTOR_CROSSWALK, index=False, encoding='utf-8')
    return out


def validate() -> pd.DataFrame:
    """Score 2017 commodity output built through this crosswalk against BEA.

    The Economic Census is the benchmark-year product data, and the published
    2017 detail Supply block is what BEA made of it, so this is the one place
    the crosswalk can be checked against a real answer rather than against a
    total it was constructed to reproduce.

    ⚠️ **Suppression is recovered first.** 53% of ``Census_EC_PxI`` rows publish
    as zero; scored raw the crosswalk looks 2.5x worse than it is, and the
    apparent error is the withholding rather than the mapping. That confusion is
    what stalled the manufacturing work for four separate mapping attempts, all
    of which converged at 28-30% because they were all measuring the same
    suppression.

    ⚠️ **Restricted to commodities the crosswalk claims.** ASM and the Economic
    Census product files cover manufacturing and mining; scoring the whole
    Supply block would charge this crosswalk for services it never mapped.
    """
    from bedrock.analysis.nowcasting.frozen_mix_diagnostic import (  # noqa: PLC0415
        detail_block,
    )
    from bedrock.extract.census.Census_EC import (  # noqa: PLC0415
        estimate_suppressed_ec_pxi,
    )
    from bedrock.extract.flowbyactivity import getFlowByActivity  # noqa: PLC0415

    crosswalk = pd.read_csv(OUT, dtype={'napcs_code': str, 'bea_2017_commodity': str})
    pxi = estimate_suppressed_ec_pxi(getFlowByActivity('Census_EC_PxI', 2017))
    mapped = pxi.merge(
        crosswalk, left_on='FlowName', right_on='napcs_code', how='inner'
    )
    # USD -> millions, the Supply table's unit. Getting this wrong once produced
    # a 100,000,000% error that read as a data problem rather than a unit one.
    built = (
        mapped.assign(value=mapped['FlowAmount'] * mapped['weight'])
        .groupby('bea_2017_commodity')['value']
        .sum()
        / 1e6
    )
    published = detail_block().sum(axis=1)

    commodities = sorted(set(built.index) & set(published.index))
    scored = pd.DataFrame(
        {
            'built': built.reindex(commodities),
            'published': published.reindex(commodities),
        }
    )
    scored['ratio'] = scored['built'] / scored['published'].replace(0, pd.NA)
    error = (scored['built'] - scored['published']).abs().sum() / scored[
        'published'
    ].sum()

    print(f'\nvalidation: {len(scored)} commodities scored against 2017 detail')
    print(f'  level        {scored["built"].sum() / scored["published"].sum():.3f}')
    print(f'  weighted err {error:.1%}')
    print(f'  within +-25% {int(scored["ratio"].between(0.75, 1.25).sum())}')
    print(f'  unbuilt      {int((scored["built"] <= 0).sum())}')
    worst = scored.assign(gap=(scored['built'] - scored['published']).abs()).nlargest(
        8, 'gap'
    )
    print('  largest gaps:')
    for code, row in worst.iterrows():
        print(
            f'    {code:<8} built {row["built"]:>12,.0f}  '
            f'published {row["published"]:>12,.0f}  ratio {row["ratio"]:.2f}'
        )
    return scored


def main() -> None:
    crosswalk = build()
    crosswalk.to_csv(OUT, index=False, encoding='utf-8')
    sector_crosswalk = write_sector_crosswalk(crosswalk)
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
    print(f'wrote {SECTOR_CROSSWALK}')
    per = sector_crosswalk.groupby('Activity')['Sector'].nunique()
    print(
        f'  {len(sector_crosswalk)} rows | 1 commodity: {(per == 1).sum()} | '
        f'2+: {(per > 1).sum()} | max {per.max()}'
    )


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--validate',
        action='store_true',
        help='after writing, score 2017 commodity output built through the '
        'crosswalk against the published detail Supply block',
    )
    args = parser.parse_args()
    main()
    if args.validate:
        validate()
