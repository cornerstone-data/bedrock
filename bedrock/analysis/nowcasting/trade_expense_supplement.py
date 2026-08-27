"""Can the trade Business Expenses Supplement seed the wholesale and retail columns?

❌ **No -- and on the sound answer key the reason is neither §S4's nor this
module's first one.**  The seed is a **wash**: on :mod:`~.benchmark_holdout`,
seeding the observed 2012 block with the BES's
2012 -> 2017 movement and scoring against the observed 2017 block gives
**+0.3% on dollars and -5.6% on impact**, with **9 of 18 and 8 of 18** columns
winning.  A coin flip, not a catastrophe.

⚠️ **This module's earlier verdict (-43.6%, -183.6%, "the movements do not
track at +0.06") was taken against BEA's published 2018-2024 summary**, which
is the 2017 benchmark carried forward -- the key that had already reversed the
agriculture no-go.  ❌ **Both of those figures are withdrawn as evidence.**
They are kept below because the *conclusion* they were used for -- do not build
the suppression recovery -- survives the regrade, and it is worth knowing what
the wrong key does to a number.

✅ **The mechanism is present.**  Regraded at BEA **detail** against the
**observed** 2017 block, a BES item's movement correlates **+0.62 pooled over
164 item-column pairs**, and per item as high as **+0.89** (rent of machinery),
+0.81 (water and sewer), +0.75 (communication).  ❌ The +0.06 was an artefact of
scoring against a summary carry-forward.

❌ Why a working mechanism still buys nothing
----------------------------------------------

❌ **The BES tracks BEA on the items that carry almost no impact, and fails on
the three that carry 60% of it** (:func:`item_carriers`):

=====================  ======  ==========  =========
item                     corr    ``N`` %    $ %
=====================  ======  ==========  =========
electricity             -0.19    **38.0**      4.4
rent of buildings       +0.21    **13.2**     16.3
transport / shipping    +0.01     **8.9**      9.7
advertising             +0.47       2.2        7.6
professional            +0.49       1.9        8.0
water / sewer           +0.81       1.6        0.6
communication           +0.75       0.5        1.5
rent of machinery       +0.89       0.4        0.9
=====================  ======  ==========  =========

⚠️ **So the mean correlation is whatever you weight it by**: **0.469**
unweighted, **0.281** dollar-weighted, **0.028** weighted by ``N``.  ✅ That
single line is the verdict, and it is the sharpest statement yet of
§Prioritise on ``N``: a source can look good on an unweighted average of items
and be worth nothing, because impact is concentrated in three of them.

⚠️ **And it is not that electricity is measured badly.**  The BES gets its
*average* movement almost exactly right -- x0.894 against BEA's x0.901 -- it
simply cannot say **which** trade column's electricity share moved, which is
the only thing a mix seed needs.  ⚠️ Building rent is the opposite failure: the
BES says x0.969 where BEA observed **x1.237**, a real movement in 13% of the
column's impact that the survey misses outright.

❌ **It is not an overshoot either** (:func:`holdout_shrinkage`).  Raising the
index to a power below 1 shrinks every movement toward 1 without changing a
sign; the impact gain rises monotonically to **+0.44% at power 0.1**, which is
a seed that barely moves anything.  ⚠️ The log-log slope of observed on index
is 0.72 (0.45 dollar-weighted) over 397 commodity pairs, so the overshoot is
real -- it is just not what costs the seed its gain.

✅ **Do not build the suppression recovery** -- this survives the regrade
------------------------------------------------------------------------------

§S4 left the door open: *"no-go now, reopen only behind a suppression
recovery"*.  ❌ **That build would still be wasted.**  ``441000`` motor vehicle
dealers has **13 of 13 items published in both 2012 and 2017**, needs no
aggregation, and reaches **61.5% of its dollars and 75.7% of its impact** --
and it scores **-16.8%** on the holdout.  ✅ Neither suppression nor reach is
the binding constraint, so recovering suppressed cells buys nothing.

⚠️ **The percent-of-total column is suppressed on exactly the same cells** -- 0
of 471 amount-suppressed cells has a surviving percent -- so no arithmetic
recovery exists, only a modelled one.

What was wrong with §S4's test, and still is
----------------------------------------------

These three corrections stand; they are simply not what decides the verdict.

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
(§What a column is worth) -- as the ``N``-weighted correlation above shows more
sharply than anything else in this package.

⚠️ **4. And a fourth, this module's own**: it said *"2017 -> 2022 is the only
pair the BES offers"*.  ❌ **Wrong** -- the BES is quinquennial, and the 2012
release exists in both directories under names the 2017 and 2022 files do not
predict (:data:`RETAIL_2012`, :data:`WHOLESALE_2012_REVISED`).  ⚠️ **2007 is
published too**, so a second holdout span is available if this is ever reopened.

⚠️ What the old key said, kept for the record
-----------------------------------------------

Scored at 2022 against a frozen 2017 on BEA's published **summary**
(:func:`trade_score`) -- ❌ **the wrong key, not evidence**:

======  =============  =============
column   dollar gain    ``N`` gain
======  =============  =============
``42``      -3.3%          -2.5%
``441``    -24.4%         **-43.6%**
``445``    -24.0%          +5.4%
``452``   -132.7%        **-183.6%**
``4A0``     -3.3%         -31.8%
======  =============  =============

⚠️ **The gap between -43.6% there and -16.8% on the holdout for the same column
is the size of the answer-key effect**, on a source whose real verdict is
"no signal".

⚠️ What is still true, and what this does not establish
--------------------------------------------------------

⚠️ **Suppression is real.**  At BEA detail across 2017 and 2022 retail has **1
column of 9 with all 13 items** and **3 with none**; wholesale **0 of 9 and 4
with none**, and 2022 is far more suppressed than 2017 (42.5% of retail cells
against 18.8%).  ✅ **2012 is the least suppressed vintage of the three** --
**eight of the nine** retail columns hold 12 or 13 items and only ``446000``
holds none --
which is why the holdout can be run at all, and why its result is the
strongest test this source will get.

⚠️ **``425000`` has no AWTS coverage at all** ($20.5B).  AWTS surveys merchant
wholesalers; ``425`` is agents and brokers.

⚠️ **Some 2017 -> 2022 movements are not credible on their own terms.**  ``452``
general merchandise reports building rent falling **$9,037M -> $5,577M**, a 38%
*nominal* fall in five years, alongside professional services x2.83 and
communication x0.38.  That is a universe or classification effect, not how a
department store changed its purchasing.

⚠️ **The source understates growth systematically**: BES total operating
expenses against BEA's published column, median **0.858** across the 18
addressable columns, range 0.610 to 1.113.  ⚠️ Irrelevant to the holdout, which
renormalises to the observed column total and tries only the mix -- but it
matters to any use of the BES that is not purely relative.

⚠️ **Nothing continues this after 2022.**  AIES publishes no expense cell for
42 or 44-45 at any NAICS level, so even a working seed would have ended at 2022.

⚠️ **The holdout is one span**, 2012 -> 2017, with no counterpart to the 2021-22
price surge; and at benchmark years BEA built these columns from this survey, so
it shows whether the BES reproduces BEA's **benchmark process**, not whether
either is right about the world.

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

#: ✅ **The 2012 vintage, for :mod:`~.benchmark_holdout`.**  The BES is
#: quinquennial (years ending 2 and 7), so the 2012 -> 2017 span the holdout
#: needs is observed on both sides.  ⚠️ **Neither file is where the 2017 and
#: 2022 names suggest**: the 2012 releases are ``.xls``, the wholesale table is
#: ``detailopexpenses`` rather than ``detailopex``, and both directories carry
#: two versions of the same table.
#:
#: ⚠️ **Take the version benchmarked to its own Economic Census**, the same rule
#: :data:`WHOLESALE_2017_REVISED` follows.  The ARTS
#: ``2012_arts_detailed_operating_expenses.xls`` is adjusted to *preliminary*
#: 2012 results and ``bes.xls`` to the **final** ones;
#: ``2012_awts_detailopexpenses_table5.1.xls`` is adjusted to the **2007**
#: census and only ``2012r`` moves onto 2012.  Pairing an original with the
#: revised 2017 file would read two rebenchmarks as economics.
RETAIL_2012 = f'{BASE}/arts/tables/2012/bes.xls'
WHOLESALE_2012_REVISED = (
    f'{BASE}/awts/tables/2012/2012r_awts_detailopexpenses_table5.1.xls'
)

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


#: ⚠️ **The first data row moves between vintages** -- row 6 in the 2017 and
#: 2022 workbooks, row 4 in the 2012 ones, which carry one fewer title line.
#: The item columns themselves are identical across all four, so only this
#: offset has to be found rather than assumed.
def _first_data_row(frame: pd.DataFrame) -> int:
    for row in range(len(frame)):
        if str(frame.iat[row, 0]).strip()[:1].isdigit():
            return row
    return len(frame)


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
    for row in range(_first_data_row(frame), len(frame)):
        naics = str(frame.iat[row, 0]).strip()
        if not naics.isdigit():
            continue
        for column in BEA_ITEMS:
            values[(naics, column)] = _cell(frame, row, column)
    return values


def operating_totals(url: str) -> dict[str, float]:
    """``naics -> total operating expenses``."""
    frame = _workbook(url)
    out: dict[str, float] = {}
    for row in range(_first_data_row(frame), len(frame)):
        naics = str(frame.iat[row, 0]).strip()
        if not naics.isdigit():
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


def _holdout_urls(bea: str) -> tuple[str, str]:
    """The 2012 -> 2017 workbook pair for a BEA detail trade column."""
    if bea in RETAIL:
        return RETAIL_2012, RETAIL_2017
    return WHOLESALE_2012_REVISED, WHOLESALE_2017_REVISED


def trade_holdout(weighting: str = 'impact') -> pd.DataFrame:
    """The test that decides this seed, per :mod:`~.benchmark_holdout`.

    Seed the **observed 2012** benchmark detail block with the BES's
    2012 -> 2017 movement and score against the **observed 2017** block, at
    detail.  ⚠️ **:func:`trade_score` grades against BEA's published 2018-2024,
    which is the 2017 benchmark carried forward**, and that key already
    reversed the agriculture verdict -- so this function, not that one, is what
    the no-go has to rest on.

    ⚠️ **The span is not the one the seed is wanted for.**  The BES is
    quinquennial, so 2012 -> 2017 and 2017 -> 2022 are the only two spans it
    offers; this scores the first and :func:`trade_score` the second.
    """
    from bedrock.analysis.nowcasting.benchmark_holdout import (  # noqa: PLC0415
        holdout_score,
    )

    mapping = {**RETAIL, **WHOLESALE}

    def index_for(column: str) -> 'pd.Series[float]':
        codes = mapping.get(column)
        if codes is None:
            return pd.Series(dtype=float)
        return relative_index(codes, *_holdout_urls(column))

    return holdout_score(index_for, list(mapping), weighting)


def holdout_shrinkage(
    powers: tuple[float, ...] = (1.0, 0.75, 0.5, 0.35, 0.25, 0.1),
) -> pd.DataFrame:
    """Is the index right in direction and merely too large?  ❌ **No.**

    Raising the index to a power below 1 shrinks every movement toward 1
    without changing a single sign, so if the BES pointed the right way and
    only overshot, some power would win.  ⚠️ **The gain rises monotonically as
    the seed disappears** -- best on impact at power 0.1, which is a seed that
    barely moves anything -- so there is no amplitude that rescues it.

    ⚠️ The log-log slope of the observed movement on the index is **0.72**
    unweighted and **0.45** dollar-weighted over 397 commodity-level pairs, so
    the overshoot is real; it is simply not what costs the seed its gain.
    """
    from bedrock.analysis.nowcasting.benchmark_holdout import (  # noqa: PLC0415
        aggregate,
        holdout_score,
    )

    mapping = {**RETAIL, **WHOLESALE}
    records = []
    for power in powers:

        def index_for(column: str, power: float = power) -> 'pd.Series[float]':
            codes = mapping.get(column)
            if codes is None:
                return pd.Series(dtype=float)
            index = relative_index(codes, *_holdout_urls(column))
            return index ** power if len(index) else index

        record: dict[str, object] = {'power': power}
        for weighting in ('dollar', 'impact'):
            summary = aggregate(holdout_score(index_for, list(mapping), weighting))
            record[f'{weighting}_gain_%'] = summary['gain_%']
            record[f'{weighting}_wins'] = summary['wins']
        records.append(record)
    return pd.DataFrame(records).set_index('power')


def holdout_pairs() -> pd.DataFrame:
    """Every (item, BEA column) pair on the holdout span, BES against observed.

    ✅ **This is where the withdrawn +0.06 goes.**  Regraded at BEA detail
    against the **observed** 2017 block rather than BEA's summary
    carry-forward, the same comparison correlates **+0.62 over 164 pairs**.
    ⚠️ **The mechanism was never the problem** -- see :func:`item_carriers`
    for what is.
    """
    from bedrock.analysis.nowcasting.benchmark_holdout import (  # noqa: PLC0415
        BASE,
        TARGET,
        block,
        intensity,
    )

    early, late = block(BASE), block(TARGET)
    weights = intensity()
    records = []
    for bea, codes in {**RETAIL, **WHOLESALE}.items():
        if bea not in early.columns or bea not in late.columns:
            continue
        first_url, second_url = _holdout_urls(bea)
        before, after = amounts(first_url), amounts(second_url)
        base_total = sum(operating_totals(first_url).get(code, 0.0) for code in codes)
        late_total = sum(operating_totals(second_url).get(code, 0.0) for code in codes)
        if not base_total or not late_total:
            continue
        overall = late_total / base_total
        early_column, late_column = early[bea], late[bea]
        weighted = late_column * weights.reindex(late_column.index).fillna(0.0)
        for column, name in BEA_ITEMS.items():
            first = [before.get((code, column)) for code in codes]
            second = [after.get((code, column)) for code in codes]
            if any(v is None for v in first) or any(v is None for v in second):
                continue
            base = float(sum(v for v in first if v is not None))
            if base <= 0:
                continue
            targets = [c for c in ITEM_TO_BEA[name] if c in early_column.index]
            start = float(early_column.loc[targets].sum()) / float(early_column.sum())
            if start <= 0:
                continue
            records.append(
                {
                    'item': name,
                    'bes': float(sum(v for v in second if v is not None))
                    / base
                    / overall,
                    'observed': float(late_column.loc[targets].sum())
                    / float(late_column.sum())
                    / start,
                    'share_$': float(late_column.loc[targets].sum())
                    / float(late_column.sum()),
                    'share_N': float(weighted.loc[targets].sum())
                    / float(weighted.sum()),
                }
            )
    return pd.DataFrame(records)


def item_carriers() -> pd.DataFrame:
    """❌ **The reason for the no-go: the BES tracks where it does not matter.**

    :func:`holdout_pairs` grouped by item: how well each item's movement
    correlates with the **observed** movement of the BEA commodities it maps
    to, next to how much of the average trade column's dollars and ``N`` those
    commodities carry.

    ❌ **Electricity is 38% of the average column's impact and correlates
    -0.19**; transportation is 8.9% at **+0.01**; building rent is 13.2% at
    +0.21.  ✅ The items that track well -- rent of machinery +0.89, water and
    sewer +0.81, communication +0.75 -- are worth **0.4%, 1.6% and 0.5%** of
    the column's impact between them.

    ⚠️ **So the mean correlation is whatever you weight it by**: 0.469
    unweighted, 0.281 dollar-weighted, **0.028 weighted by ``N``**.  That is
    the whole verdict, and it is why :func:`trade_holdout` is a wash on dollars
    and negative on impact while the item correlations look healthy.
    """
    pairs = holdout_pairs()
    rows = [
        {
            'item': str(name),
            'columns': len(group),
            'corr': float(np.corrcoef(group['bes'], group['observed'])[0, 1]),
            'share_$_%': 100 * float(group['share_$'].mean()),
            'share_N_%': 100 * float(group['share_N'].mean()),
            'mean_bes': float(group['bes'].mean()),
            'mean_observed': float(group['observed'].mean()),
        }
        for name, group in pairs.groupby('item')
    ]
    frame = pd.DataFrame(rows).set_index('item')
    return frame.sort_values('share_N_%', ascending=False)


def trade_score() -> pd.DataFrame:
    """Frozen 2017 against the BES-seeded columns at 2022, on published summary.

    ⚠️ **The wrong answer key, kept for the record.**  BEA's 2018-2024 tables
    are the 2017 benchmark carried forward, and summary averages away the
    detail a seed moves; :func:`trade_holdout` is the sound test and says
    **-5.6%** where this says -43.6% on the same column.  ❌ **Do not quote
    these numbers as evidence** -- see the module docstring.
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

    ⚠️ **Withdrawn: the +0.06 over 54 pairs this returns is an artefact of the
    key.**  It compares against BEA's published *summary* movement 2017 -> 2022,
    which collapses several items onto one code and is a carry-forward of the
    2017 benchmark besides.  ✅ :func:`holdout_pairs` runs the same comparison
    at detail against the observed 2017 block and gets **+0.62 over 164
    pairs**.  Kept because the size of the gap is itself the lesson.
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
    parser.add_argument(
        '--holdout', action='store_true', help='THE test: 2012 -> observed 2017'
    )
    parser.add_argument(
        '--carriers', action='store_true', help='which items carry the N'
    )
    parser.add_argument('--shrink', action='store_true', help='is it only an overshoot')
    parser.add_argument('--suppression', action='store_true', help='items per column')
    parser.add_argument('--score', action='store_true', help='frozen vs seeded')
    parser.add_argument('--mechanism', action='store_true', help='do movements track')
    parser.add_argument('--all', action='store_true', help='every measurement')
    args = parser.parse_args()
    chosen = (
        args.holdout
        or args.carriers
        or args.shrink
        or args.suppression
        or args.score
        or args.mechanism
    )
    pd.set_option('display.width', 200)

    if args.all or args.holdout or not chosen:
        from bedrock.analysis.nowcasting.benchmark_holdout import (  # noqa: PLC0415
            aggregate,
        )

        print('\nTHE test: seed observed 2012, score against observed 2017\n')
        for weighting in ('dollar', 'impact'):
            summary = aggregate(trade_holdout(weighting))
            print(
                f'  {weighting:>7}  frozen {summary["frozen"]:.4f} -> seeded '
                f'{summary["seeded"]:.4f}   gain {summary["gain_%"]:+.1f}%   '
                f'{int(summary["wins"])}/{int(summary["columns"])} columns win'
            )
        print()
        print(trade_holdout('impact')[['frozen', 'seeded', 'gain_%']].round(4))

    if args.all or args.carriers or not chosen:
        frame = item_carriers()
        print('\nPer item: does it track, and how much N does it carry\n')
        print(frame.round(3).to_string())
        for label, key in (('dollar', 'share_$_%'), ('N', 'share_N_%')):
            mean = float((frame['corr'] * frame[key]).sum() / frame[key].sum())
            print(f'  mean correlation weighted by {label}: {mean:+.3f}')
        print(f'  unweighted: {frame["corr"].mean():+.3f}')
        pairs = holdout_pairs()
        pooled = float(np.corrcoef(pairs['bes'], pairs['observed'])[0, 1])
        print(f'  pooled over {len(pairs)} pairs: {pooled:+.3f}')
        print(
            '\n  electricity alone is 38% of the average column and does not'
            '\n  track. The items that do track are worth a few percent of N'
            '\n  between them, which is the whole verdict.'
        )
    if args.all or args.shrink:
        print('\nHoldout gain as the index is shrunk toward 1\n')
        print(holdout_shrinkage().round(2).to_string())
        print(
            '\n  the gain rises as the seed vanishes, so the index is not'
            '\n  right-but-too-large; there is no amplitude that wins.'
        )
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
    if args.all or args.score:
        print('\nFrozen 2017 against the BES-seeded columns at 2022\n')
        print(trade_score().round(4).to_string())
        print(
            '\n  the WRONG KEY: BEA carried forward from 2017, at summary.'
            '\n  --holdout scores the same 441 at -16.8%, and the block at'
            '\n  -5.6%. What survives from here is only that 441 has 13 of'
            '\n  13 items and 75.7% impact reach and still loses, so the'
            '\n  suppression recovery must not be built.'
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
        print(f'\n  pooled {pooled:.3f} -- WITHDRAWN. Against BEA summary,')
        print('  carried forward from 2017. --carriers runs the same')
        print('  comparison at detail against the OBSERVED 2017 block and')
        print('  gets +0.62 over 164 pairs: the mechanism is there, it just')
        print('  is not there on the items that carry the impact.')


if __name__ == '__main__':
    main()
