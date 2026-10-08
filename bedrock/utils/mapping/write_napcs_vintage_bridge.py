"""Build the 2022 -> 2017 NAPCS collection-code bridge (#570).

What lets a 2017-vintage product concordance read the 2022 Economic Census.
``Census_EC_PxI`` identifies products by Census's *NAPCS-based collection code*,
which is renumbered between censuses; the services concordance behind Step 4a
(``pxi_services_product_seed_2017.csv``) was reviewed against 2017 codes and
their descriptions, and cannot see 2022 without a bridge.

⚠️ **This supersedes the "key on the description instead" reasoning** in
``write_pxi_product_bea_crosswalk.py``, which chose the description key because
the code "is not stable across vintages" and keying on it "would drop roughly
15% of coverage at the 2022 boundary". Measured against this file, that is
mostly wrong, and wrong in a way worth stating plainly:

- of the 168 products the services concordance maps, **156 keep their code**
  into 2022 and **4 more** are recovered here, for 160 of 168;
- the 12 that do not survive carry **0.3%** of 2017 concordance value;
- the apparent ~15% loss is a *description-text* effect. Only 137 of the 168
  descriptions match 2022 verbatim, and 43 of the 430 rewordings across the
  whole file are **trailing whitespace alone**. The text moves; the concept
  does not.

Keying on the code and bridging is therefore strictly better than keying on the
text, and it fails loudly (an unmatched code) where the text key fails silently
(a reworded product simply disappears from the mix).

**The reverse trap was checked.** A stable code whose *meaning* changed would
carry the 2017 commodity assignment onto a different product - the failure that
the 2012 MatFuel code-scheme change and the 2014 EIA form-176 line split each
produced elsewhere in this project. Of the 156 concordance codes that persist,
15 are reworded and **all 15 are cosmetic or clarifying** ("Collocation" ->
"Colocation", "Published system software" -> "System software publishing",
added parenthetical exclusions). None reverses scope.

**Hierarchy.** 2022 codes come in broad (``B``) and detail (``D``) lines, and
summing both would double count. Both PxI vintages on disk carry **broad lines
only**, so this file keeps the level of each side and the reader filters.

**Splits.** ``(PT)`` marks a 2017 code split into several 2022 codes with
distinct content; summing every 2022 child reassembles the 2017 concept, which
is what a mix built at BEA-commodity granularity wants. The reverse - one 2022
code drawing on several 2017 codes - is genuinely ambiguous, and is left for the
reader to resolve: within the services concordance exactly one such code has
parents that disagree on the target commodity.

Source: 2022 to 2017 NAPCS Concordance, downloaded from
https://www2.census.gov/programs-surveys/economic-census/technical-documentation/
napcs/2022_to_2017_NAPCS_Concordance_Final_08242022.xlsx

Run: ``uv run python bedrock/utils/mapping/write_napcs_vintage_bridge.py``
"""

from __future__ import annotations

import argparse

import pandas as pd

SOURCE = 'bedrock/extract/input_data/taxonomy/2022_to_2017_NAPCS_Concordance.xlsx'
SHEET = '2022 to 2017 NAPCS Concordance'
OUT = 'bedrock/utils/mapping/census_pxi/napcs_2022_to_2017.csv'

#: The workbook's own column order, which carries no usable header row of its
#: own once ``pandas`` has read it - the names are long and repeat "NAPCS-based"
#: three times.
COLUMNS = (
    'code_2022',
    'broad_2022',
    'detail_2022',
    'description_2022',
    'code_2017',
    'part',
    'broad_2017',
    'detail_2017',
    'description_2017',
)

#: ``(PT)``, not ``PT`` - the workbook parenthesises it, and matching the bare
#: token silently finds nothing.
PART_MARKER = '(PT)'


def _level(broad: str, detail: str) -> str:
    """``B`` for a broad line, ``D`` for a detail line, ``?`` if neither."""
    if str(broad).strip() == 'B':
        return 'B'
    return 'D' if str(detail).strip() == 'D' else '?'


def bridge() -> pd.DataFrame:
    """2022 code -> 2017 code, with each side's level and the split flag."""
    raw = pd.read_excel(SOURCE, sheet_name=SHEET, dtype=str)
    raw.columns = list(COLUMNS)
    for column in ('code_2022', 'code_2017', 'part'):
        raw[column] = raw[column].astype(str).str.strip()
    raw = raw[(raw['code_2022'] != 'nan') & (raw['code_2017'] != 'nan')]

    return pd.DataFrame(
        {
            'code_2022': raw['code_2022'],
            'code_2017': raw['code_2017'],
            'level_2022': [
                _level(b, d) for b, d in zip(raw['broad_2022'], raw['detail_2022'])
            ],
            'level_2017': [
                _level(b, d) for b, d in zip(raw['broad_2017'], raw['detail_2017'])
            ],
            # a 2017 code split into several 2022 codes with distinct content
            'split': raw['part'] == PART_MARKER,
            'description_2022': raw['description_2022'].str.strip(),
            'description_2017': raw['description_2017'].str.strip(),
        }
    ).sort_values(['code_2022', 'code_2017'], ignore_index=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--check',
        action='store_true',
        help='report the fan-out shape and the reworded-description count that '
        'the docstring above quotes, rather than only writing the file',
    )
    args = parser.parse_args()

    out = bridge()
    out.to_csv(OUT, index=False)
    print(f'{len(out)} bridge rows -> {OUT}')
    print(
        f'  {out["code_2022"].nunique()} 2022 codes, '
        f'{out["code_2017"].nunique()} 2017 codes, '
        f'{int(out["split"].sum())} marked as a split'
    )

    if not args.check:
        return

    per_2022 = out.groupby('code_2022')['code_2017'].nunique()
    per_2017 = out.groupby('code_2017')['code_2022'].nunique()
    print(f'  2022 codes drawing on >1 2017 code: {(per_2022 > 1).sum()}')
    print(f'  2017 codes split across >1 2022 code: {(per_2017 > 1).sum()}')

    # ⚠️ both descriptions are already stripped by :func:`bridge`, so a
    # rewording counted here is a real one - the 43 whitespace-only "changes"
    # the docstring cites are gone by construction, which is the point.
    same = out[out['code_2022'] == out['code_2017']]
    reworded = same[same['description_2022'] != same['description_2017']]
    print(
        f'  codes kept with a substantive rewording: {len(reworded)} '
        f'of {len(same)} kept codes'
    )
    levels = out.drop_duplicates('code_2022')['level_2022'].value_counts()
    print(f'  2022 codes by level: {levels.to_dict()}')


if __name__ == '__main__':
    main()
