"""Can the trade Business Expenses Supplement seed the wholesale and retail columns?

❌ **No, and the reason is not the one §S4 gave.**  §S4 rejected the BES on
**suppression** and left the door open: *"no-go now, reopen only behind a
suppression recovery"*.  This re-runs the test with three of that verdict's
objections removed, and the answer is a firmer no -- ⚠️ **a suppression recovery
would not fix it**, so the build §S4 pointed at should not be started.

What was wrong with the first test
------------------------------------

⚠️ **1. It was run at BEA summary, and Step 3 estimates BEA detail.**  §S4
required an item to be published for every constituent NAICS of a *summary*
column, and concluded ``4A0`` loses **0 of 13 items** because it spans nine
three-digit NAICS.  ✅ **At detail ``4A0`` is not one column** -- it is six
(:data:`RETAIL`), and five of the six are a **single** three-digit NAICS each,
needing no aggregation at all.

⚠️ **2. It scored items BEA did not use.**  §S4's account of ``452`` cites
*contract labour x4.45* as a driver.  ❌ **Contract labour is not one of BEA's
thirteen items** for trade (Table C2); it is a BES column BEA did not take.
:data:`BEA_ITEMS` is BEA's own list.

⚠️ **3. It was dollar-weighted**, and dollars are the wrong unit
(§What a column is worth).  Scored here on ``N`` as well.

✅ **And the source itself is fine on the points §S4 checked.**  Both vintages
are live, the item list is identical across them, and
:data:`WHOLESALE_2017_REVISED` is used rather than the original -- the original
is benchmarked to the **2012** Economic Census and importing it would read a
rebenchmark as economics.

❌ The result, with all three fixed
------------------------------------

Scored at 2022 against a frozen 2017, on BEA's published summary:

======  =============  =============
column   dollar gain    ``N`` gain
======  =============  =============
``42``      -3.3%          -2.5%
``441``    -24.4%         **-43.6%**
``445``    -24.0%          +5.4%
``452``   -132.7%        **-183.6%**
``4A0``     -3.3%         -31.8%
======  =============  =============

❌ **Every column loses on dollars; four of five lose on impact.**

⚠️ **The decisive column is ``441``.**  It has **13 of 13 items published in
both years**, needs no aggregation, and reaches **61.5% of its dollars and
75.7% of its impact** -- and it loses **-43.6%**.  ✅ **So neither suppression
nor reach is the binding constraint**, and neither a suppression recovery nor a
better item map can rescue this.

❌ Why it fails: the movements do not track
---------------------------------------------

Comparing each item's relative movement against the published share movement of
the BEA commodities it maps to, over 54 item-column pairs:

=============================  ==========
pairing                         correlation
=============================  ==========
``441``                            +0.32
``445``                            +0.25
``4A0``                            +0.06
``452``                            -0.00
**pooled, 54 pairs**            **+0.06**
=============================  ==========

⚠️ **The same failure mode as ERS agriculture** -- see
:mod:`~.agriculture_expense_seed`, pooled +0.18.  A survey can measure an
industry's expenses accurately and still not describe how BEA's commodity mix
moved, because BEA does not build the mix from the survey's item shares.

⚠️ **And some of the movements are not credible on their own terms.**  ``452``
general merchandise reports building rent falling **$9,037M -> $5,577M**, a 38%
*nominal* fall in five years, alongside professional services x2.83 and
communication x0.38.  That is a universe or classification effect, not how a
department store changed its purchasing.

⚠️ **The source also understates growth systematically**: BES total operating
expenses against BEA's published column, median **0.858** across the 18
addressable columns, range 0.610 to 1.113.  Better than utilities' SAS panel
(0.63-0.89) and much worse than ERS agriculture (0.988-1.047).

⚠️ What this does not establish, and what is still true
--------------------------------------------------------

⚠️ **Suppression is real, it is just not what decides this.**  At BEA detail,
in both years: retail has **1 column of 9 with all 13 items** and **3 with
none**; wholesale has **0 of 9 with all 13** and **4 with none**.  2022 is far
more suppressed than 2017 -- 42.5% of retail cells against 18.8%.

⚠️ **``425000`` has no AWTS coverage at all** ($20.5B).  AWTS surveys merchant
wholesalers; ``425`` is agents and brokers.

⚠️ **Nothing continues this after 2022.**  AIES publishes no expense cell for
42 or 44-45 at any NAICS level, so even a working seed would have ended at 2022.

⚠️ **The test is one span.**  2017 -> 2022 is the only pair the BES offers, so
the correlations above rest on cross-sectional spread within one interval rather
than on repeated observation, unlike the seven-year agriculture test.

✅ **No extractor is built.**  With the seed rejected and no successor vintage,
a ``Census_BES`` source would carry two observations and no future.  This module
downloads the workbooks directly so the finding stays reproducible.

Run::

    uv run python -m bedrock.analysis.nowcasting.trade_expense_supplement --all
"""

from __future__ import annotations

import argparse
import functools
import io

import numpy as np
import pandas as pd
import requests

BASE = 'https://www2.census.gov/programs-surveys'

RETAIL_2017 = f'{BASE}/arts/tables/2017/bes.xlsx'
RETAIL_2022 = f'{BASE}/arts/tables/2022/bes.xlsx'

#: ⚠️ **The revised 2017 wholesale file, deliberately.**  The original
#: ``2017_awts_detailopex_table5.1.xlsx`` is benchmarked to the **2012**
#: Economic Census; the revision exists precisely to move it onto 2017, which
#: is the basis the 2022 file is on.  Reaching for the obvious filename imports
#: a rebenchmark as if it were economics.
WHOLESALE_2017_REVISED = (
    f'{BASE}/awts/tables/2017/2017_awts_detailopex_table5.1_revised.xlsx'
)
WHOLESALE_2022 = f'{BASE}/awts/tables/2022/2022_awts_detailopex_table5.1.xlsx'

#: ⚠️ **BEA's own thirteen items** (Table C2), as BES *Amount* column offsets.
#: The BES publishes 22 expense concepts; these are the ones BEA took.
#: ❌ Contract labour (column 11), expensed equipment, other materials, expensed
#: software and commission expense are **not** among them, and §S4's account of
#: ``452`` rests on one of those.
BEA_ITEMS: dict[int, str] = {
    15: 'packaging',
    21: 'data processing',
    25: 'communication',
    27: 'repair machinery',
    29: 'repair buildings',
    31: 'rent machinery',
    33: 'rent buildings',
    35: 'electricity',
    37: 'fuels',
    39: 'water/sewer/refuse',
    41: 'transport/shipping',
    43: 'advertising',
    45: 'professional',
}

#: BEA **detail** retail column -> the BES NAICS it needs.  ✅ **Eight of nine
#: need exactly one**, which is the correction to §S4: only ``4B0000``
#: aggregates, and ``4A0``'s "nine constituent NAICS" was a summary artefact.
RETAIL: dict[str, tuple[str, ...]] = {
    '441000': ('441',),
    '444000': ('444',),
    '445000': ('445',),
    '446000': ('446',),
    '447000': ('447',),
    '448000': ('448',),
    '452000': ('452',),
    '454000': ('454',),
    '4B0000': ('442', '443', '451', '453'),
}

#: BEA **detail** wholesale column -> the BES NAICS it needs.
#: ⚠️ ``425000`` is absent: AWTS surveys merchant wholesalers and ``425`` is
#: agents and brokers, so $20.5B has no coverage at any granularity.
#: ``4200ID`` is $0M.
WHOLESALE: dict[str, tuple[str, ...]] = {
    '423100': ('4231',),
    '423400': ('4234',),
    '423600': ('4236',),
    '423800': ('4238',),
    '423A00': ('4232', '4233', '4235', '4237', '4239'),
    '424200': ('4242',),
    '424400': ('4244',),
    '424700': ('4247',),
    '424A00': ('4241', '4243', '4245', '4246', '4248', '4249'),
}

#: BES item -> BEA detail commodities, mirroring ``SAS_ITEM_TO_BEA`` where the
#: item names coincide, which is most of them.
ITEM_TO_BEA: dict[str, tuple[str, ...]] = {
    'packaging': ('322210', '326110'),
    'data processing': ('518200',),
    'communication': ('517110', '517A00', '517210'),
    'repair machinery': ('811300', '811400'),
    'repair buildings': ('230301', '230302'),
    'rent machinery': ('532100', '532400'),
    'rent buildings': ('531ORE',),
    'electricity': ('221100',),
    'fuels': ('221200', '324110'),
    'water/sewer/refuse': ('221300', '562000'),
    'transport/shipping': (
        '481000',
        '482000',
        '483000',
        '484000',
        '486000',
        '48A000',
        '492000',
        '493000',
    ),
    'advertising': ('541800',),
    'professional': (
        '541100',
        '541200',
        '541300',
        '541511',
        '541512',
        '541610',
        '5416A0',
        '541700',
        '5419A0',
    ),
}

#: ⚠️ Census suppression flags.  ``D`` withheld, ``S`` fails publication
#: standards, ``Z``/``ZZ`` rounds to nothing.  ⚠️ **The "Percent of Total"
#: column beside each amount is suppressed on exactly the same cells** -- 0 of
#: 471 amount-suppressed cells across the four files has a surviving percent --
#: so there is no arithmetic recovery, only a modelled one.
SUPPRESSION_FLAGS = frozenset({'D', 'S', 'Z', 'ZZ', 'nan', ''})

#: 'Operating Expenses, Total', the denominator of :func:`relative_index`.
TOTAL_COLUMN = 2

#: The first data row; rows 0-5 are titles and the two header bands.
FIRST_DATA_ROW = 6


@functools.cache
def _workbook(url: str) -> pd.DataFrame:
    response = requests.get(url, timeout=180, verify=False)  # noqa: S501
    response.raise_for_status()
    return pd.read_excel(io.BytesIO(response.content), header=None)


def _cell(frame: pd.DataFrame, row: int, column: int) -> float | None:
    raw = str(frame.iat[row, column]).strip()
    if raw in SUPPRESSION_FLAGS:
        return None
    try:
        return float(raw.replace(',', ''))
    except ValueError:
        return None


def amounts(url: str) -> dict[tuple[str, int], float | None]:
    """``(naics, column) -> amount``, ``None`` where Census suppressed it."""
    frame = _workbook(url)
    values: dict[tuple[str, int], float | None] = {}
    for row in range(FIRST_DATA_ROW, len(frame)):
        naics = str(frame.iat[row, 0]).strip()
        if not naics[:1].isdigit():
            continue
        for column in BEA_ITEMS:
            values[(naics, column)] = _cell(frame, row, column)
    return values


def operating_totals(url: str) -> dict[str, float]:
    """``naics -> total operating expenses``."""
    frame = _workbook(url)
    out: dict[str, float] = {}
    for row in range(FIRST_DATA_ROW, len(frame)):
        naics = str(frame.iat[row, 0]).strip()
        if not naics[:1].isdigit():
            continue
        value = _cell(frame, row, TOTAL_COLUMN)
        if value is not None:
            out[naics] = value
    return out


def suppression() -> pd.DataFrame:
    """How many of BEA's 13 items survive in **both** years, per BEA column.

    ⚠️ **At BEA detail**, which is the correction to §S4 -- and the answer is
    still bad, just not the way §S4 said: ``441000`` has all thirteen and
    ``446000``, ``454000`` and ``4B0000`` have none.
    """
    pairs = (
        (RETAIL, RETAIL_2017, RETAIL_2022, 'retail'),
        (WHOLESALE, WHOLESALE_2017_REVISED, WHOLESALE_2022, 'wholesale'),
    )
    records = []
    for mapping, early_url, late_url, label in pairs:
        early, late = amounts(early_url), amounts(late_url)
        for bea, codes in mapping.items():
            usable = [
                name
                for column, name in BEA_ITEMS.items()
                if all(
                    early.get((code, column)) is not None
                    and late.get((code, column)) is not None
                    for code in codes
                )
            ]
            records.append(
                {
                    'bea': bea,
                    'block': label,
                    'naics': '+'.join(codes),
                    'items_of_13': len(usable),
                }
            )
    return pd.DataFrame(records).set_index('bea')


def relative_index(
    codes: tuple[str, ...],
    early_url: str,
    late_url: str,
) -> pd.Series:
    """Per-BEA-commodity index carrying only relative movement, 2017 -> 2022.

    The form :mod:`~.services_transport_expense_seed` uses, with total
    operating expenses as the denominator so that the ~40% of the bill these
    thirteen items do not name still counts toward the industry's own growth.
    """
    early, late = amounts(early_url), amounts(late_url)
    early_total, late_total = operating_totals(early_url), operating_totals(late_url)
    base_total = sum(early_total.get(code, 0.0) for code in codes)
    late_sum = sum(late_total.get(code, 0.0) for code in codes)
    if not base_total or not late_sum:
        return pd.Series(dtype=float)
    overall = late_sum / base_total

    numerator: dict[str, float] = {}
    denominator: dict[str, float] = {}
    for column, name in BEA_ITEMS.items():
        first = [early.get((code, column)) for code in codes]
        second = [late.get((code, column)) for code in codes]
        if any(v is None for v in first) or any(v is None for v in second):
            continue
        base = float(sum(v for v in first if v is not None))
        if base <= 0:
            continue
        ratio = float(sum(v for v in second if v is not None)) / base / overall
        for code in ITEM_TO_BEA[name]:
            numerator[code] = numerator.get(code, 0.0) + base * ratio
            denominator[code] = denominator.get(code, 0.0) + base
    return pd.Series({c: numerator[c] / denominator[c] for c in numerator})


def _at(frame: pd.DataFrame, row: str, column: str) -> float:
    """One cell as a float; pandas types ``.at`` as a very wide union."""
    return float(np.asarray(frame.at[row, column], dtype=float))


@functools.cache
def _use() -> pd.DataFrame:
    from bedrock.analysis.nowcasting.inputs_structure import (  # noqa: PLC0415
        _use_2017_detail,
    )

    frame = _use_2017_detail()
    frame.index = frame.index.astype(str)
    return frame


def trade_seed() -> pd.DataFrame:
    """BEA's 2017 trade columns moved on the BES index, at 2022."""
    use = _use()
    columns = [c for c in (*RETAIL, *WHOLESALE) if c in use.columns]
    seed = use[columns].astype(float).copy()
    for bea, codes in {**RETAIL, **WHOLESALE}.items():
        if bea not in seed.columns:
            continue
        urls = (
            (RETAIL_2017, RETAIL_2022)
            if bea in RETAIL
            else (WHOLESALE_2017_REVISED, WHOLESALE_2022)
        )
        index = relative_index(codes, *urls)
        touched = [code for code in index.index if code in seed.index]
        if touched:
            seed.loc[touched, bea] = (
                seed.loc[touched, bea] * index.reindex(touched).to_numpy()
            )
    totals, base_totals = seed.sum(axis=0), use[columns].sum(axis=0)
    seed = seed.div(totals.where(totals != 0), axis=1).mul(base_totals, axis=1)
    return seed.fillna(0.0)


def trade_score() -> pd.DataFrame:
    """Frozen 2017 against the BES-seeded columns at 2022, on published summary.

    ❌ **Every column loses on dollars and four of five on ``N``.**  ``441`` is
    the one to read: full item coverage, no aggregation, 75.7% impact reach,
    and **-43.6%**.
    """
    from bedrock.analysis.nowcasting.intermediate_structure_drift import (  # noqa: PLC0415
        summary_intermediate,
    )
    from bedrock.analysis.nowcasting.services_transport_expense_seed import (  # noqa: PLC0415
        _summary_intensity,
        _to_summary,
    )

    use = _use()
    columns = [c for c in (*RETAIL, *WHOLESALE) if c in use.columns]
    frozen_summary = _to_summary(use[columns])
    seeded_summary = _to_summary(trade_seed())
    base, actual = summary_intermediate(2017), summary_intermediate(2022)
    intensity = _summary_intensity()

    records = []
    for column in frozen_summary.columns:
        if column not in actual.columns:
            continue
        rows = [
            r for r in base.index if r in actual.index and r in frozen_summary.index
        ]
        frozen = frozen_summary[column].reindex(rows).fillna(0.0)
        seeded = seeded_summary[column].reindex(rows).fillna(0.0)
        truth = actual[column].reindex(rows).fillna(0.0)
        if truth.sum() <= 0 or frozen.sum() <= 0 or seeded.sum() <= 0:
            continue
        truth_share = truth / truth.sum()
        record: dict[str, object] = {'column': column}
        for weighting in ('dollar', 'impact'):
            weights = (
                intensity.reindex(rows).fillna(0.0)
                if weighting == 'impact'
                else pd.Series(1.0, index=rows)
            )
            d_frozen = float(
                (weights * (frozen / frozen.sum() - truth_share).abs()).sum() / 2
            )
            d_seeded = float(
                (weights * (seeded / seeded.sum() - truth_share).abs()).sum() / 2
            )
            record[f'{weighting}_frozen'] = d_frozen
            record[f'{weighting}_seeded'] = d_seeded
            record[f'{weighting}_gain_%'] = (
                100 * (d_frozen - d_seeded) / d_frozen if d_frozen else np.nan
            )
        records.append(record)
    return pd.DataFrame(records).set_index('column')


def movement_correlation() -> pd.DataFrame:
    """Does a BES item's movement predict its BEA commodities' movement?

    ❌ **Pooled +0.06 over 54 pairs**, and that is the finding: the survey can
    measure trade's expenses well and still say nothing about how BEA moved the
    commodity mix.  Restricted to the retail columns that map one-to-one onto a
    summary column, so no aggregation blurs the comparison.
    """
    from bedrock.analysis.nowcasting.intermediate_structure_drift import (  # noqa: PLC0415
        summary_intermediate,
    )
    from bedrock.analysis.nowcasting.services_transport_expense_seed import (  # noqa: PLC0415
        _summary_maps,
    )

    commodity, industry = _summary_maps()
    early, late = amounts(RETAIL_2017), amounts(RETAIL_2022)
    early_total = operating_totals(RETAIL_2017)
    late_total = operating_totals(RETAIL_2022)
    base, actual = summary_intermediate(2017), summary_intermediate(2022)

    records = []
    for bea, codes in RETAIL.items():
        summary = str(industry.get(bea, ''))
        if summary not in actual.columns or len(codes) > 1:
            continue
        naics = codes[0]
        overall = late_total[naics] / early_total[naics]
        for column, name in BEA_ITEMS.items():
            first, second = early.get((naics, column)), late.get((naics, column))
            if first is None or second is None or first <= 0:
                continue
            targets: set[str] = set()
            for code in ITEM_TO_BEA[name]:
                found = commodity.get(code)
                parent = str(found[0] if isinstance(found, list) else found or '')
                if parent in actual.index and parent in base.index:
                    targets.add(parent)
            if not targets:
                continue
            before = sum(_at(base, t, summary) for t in targets) / float(
                base[summary].sum()
            )
            after = sum(_at(actual, t, summary) for t in targets) / float(
                actual[summary].sum()
            )
            if before <= 0:
                continue
            records.append(
                {
                    'column': summary,
                    'item': name,
                    'bes_relative': (second / first) / overall,
                    'bea_relative': after / before,
                }
            )
    return pd.DataFrame(records)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--suppression', action='store_true', help='items per column')
    parser.add_argument('--score', action='store_true', help='frozen vs seeded')
    parser.add_argument('--mechanism', action='store_true', help='do movements track')
    parser.add_argument('--all', action='store_true', help='every measurement')
    args = parser.parse_args()
    chosen = args.suppression or args.score or args.mechanism
    pd.set_option('display.width', 200)

    if args.all or args.suppression:
        table = suppression()
        print("\nBEA's 13 items, published in BOTH 2017 and 2022, at BEA detail\n")
        print(table.to_string())
        print(
            '\n  the S4 claim was that 4A0 loses all 13 because it spans nine'
            '\n  NAICS. At detail that column is six, five of them one NAICS.'
            '\n  Suppression is still real -- 3 retail and 4 wholesale columns'
            '\n  have nothing -- it just is not what decides the verdict.'
        )
    if args.all or args.score or not chosen:
        print('\nFrozen 2017 against the BES-seeded columns at 2022\n')
        print(trade_score().round(4).to_string())
        print(
            '\n  441 is the column to read: 13 of 13 items, no aggregation,'
            '\n  75.7% impact reach -- and -43.6%. Neither suppression nor'
            '\n  reach is the binding constraint, so a suppression recovery'
            '\n  would not rescue this.'
        )
    if args.all or args.mechanism:
        frame = movement_correlation()
        pooled = float(frame[['bes_relative', 'bea_relative']].corr().to_numpy()[0, 1])
        print(f'\nDo BES movements predict BEA movements? {len(frame)} pairs\n')
        per_column = [
            {
                'column': str(name),
                'pairs': len(group),
                'corr': round(
                    float(
                        group[['bes_relative', 'bea_relative']].corr().to_numpy()[0, 1]
                    ),
                    3,
                ),
            }
            for name, group in frame.groupby('column')
        ]
        print(pd.DataFrame(per_column).set_index('column').to_string())
        print(f'\n  pooled {pooled:.3f} -- the same failure mode as ERS')
        print('  agriculture (+0.18). Measuring an industry accurately is not')
        print("  the same as describing how BEA moved its commodity mix.")


if __name__ == '__main__':
    main()
