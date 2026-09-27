"""A 2012 -> 2017 ``MATFUEL`` material concordance, and the holdout it unlocks (#988, #989).

The census materials seed (:func:`~bedrock.analysis.nowcasting.inputs_structure.materials_seed`)
has shipped ungraded. The benchmark holdout
(:mod:`~bedrock.analysis.nowcasting.benchmark_holdout`) needs a 2012 census mix,
and #989 found the 2012 vintage exists but sits on a different material code
list: 1,274 codes against 2017's 291, of which only 123 are shared, carrying
17% of 2012 mass. Scored as-is, the seed loses badly. #989 read that loss as
the recoding, not the seed.

Why the code lists disagree
---------------------------

The two lists share a structure and differ in resolution. Both are
industry-specific "materials consumed by kind" lists, and both lead with
NAICS-like digits for the material's product. 2012 publishes finer product
lines (``33120074``, ``21221009``). 2017 publishes coarser aggregates
(``33110090``, ``21220000``) and folds more into "all other materials".

That difference in resolution is enough to fake movement. A fine 2012 code
resolves ``direct`` to one BEA commodity through
:func:`~bedrock.analysis.nowcasting.inputs_structure.classify`. The coarse 2017
code it rolled into often resolves ``group`` and is split on 2017 Use weights,
or is residual and dropped. The two vintages then reach different commodities
through different machinery, and the difference reads as a mix change.

The concordance
---------------

This module rolls each 2012 code up to the 2017 code the **same purchasing
industry** reports and that contains it, so both vintages go through identical
placement. For each ``(industry, 2012 code)``, in order:

1. ``exact``: the code is on the 2017 list.
2. ``industry_prefix``: the longest common code prefix, of at least
   :data:`MIN_PREFIX` digits, among the codes this BEA industry reports in
   2017. Ties go to the larger 2017 cost.
3. The unmatched remainder is treated one of two ways. Both are graded,
   because the choice is empirical:

   * ``global``: the longest common prefix over the whole 2017 list;
   * ``residual``: 2017's "all other materials" (``00970099``), which placement
     drops. This asserts that 2017 folded the item into its residual, so both
     vintages now exclude it.

4. ``unmapped``: anything left keeps its 2012 code.

No labels are used. ``ecnmatfuel`` 2012 publishes no material label variable
(``ecnbasic`` 2012 has ``_TTL`` labels, but only for NAICS and establishment
fields), and the shared metadata list labels only 128 of the 1,262 codes.

The industry axis is reconciled too: 2012 NAICS is taken to 2017 NAICS through
``2012_to_2017_NAICS.csv`` before it is placed on a BEA industry.

What it finds (2026-09-27, recovery off in both vintages, impact-weighted)
-------------------------------------------------------------------------

The concordance works as a concordance. The unmapped run reproduces #989
(manufacturing -138%, median column churn 30.8pp). With the ``residual``
fallback, and the 47 special codes matched on footprint (``00999830``, $294bn
of refinery crude, lands on crude), churn falls to **20.0pp on common
support**, against 10.9pp for 2017 -> 2022 on the same measure. The remainder
fits #988's three-point finding that 2017 is the anomalous vintage.

⚠️ **But the seed still does not beat a frozen benchmark on this key:**

* **Full strength, as the live seed runs: -66.4%** (31 of 232 columns win).
* **Shrinkage:** the best ``alpha`` is about 0.1, at +0.5%, which is a wash.
  Every stronger setting loses: -10.6% at 0.5, -28.5% at 0.75.
* **Direction:** BEA's 2012 -> 2017 cell movement runs at **0.04x** the
  census's, with a weighted correlation of 0.10 and sign agreement of 59%.
* **#988's churn gate does not rescue it.** It helps only once it holds nearly
  every column (211 of 232 at a 5pp bar, +0.7%, which is the frozen benchmark
  again). Holding 178 at a 10pp bar still loses 18%.

⚠️ **What the key can and cannot say.** The key is BEA's 2017 benchmark, which
carries its prior benchmark far more than the census (97% of columns sit
closer to BEA 2012 than to census 2017, #988). So this measures agreement with
BEA's benchmark process, which is the right target for a nowcast of BEA's next
table, and not agreement with the economy. It is also one span, 2012 -> 2017,
with no price surge in it.

Run::

    uv run python -m bedrock.analysis.nowcasting.ec2012_matfuel_concordance --check
"""

from __future__ import annotations

import argparse
import functools
import typing as ta
from pathlib import Path

import numpy as np
import pandas as pd

from bedrock.analysis.nowcasting import benchmark_holdout as bh
from bedrock.analysis.nowcasting.inputs_structure import (
    MINING_SEEDED,
    _manufacturing_bea_industries,
    _split_weights,
    bea_industry,
    classify,
    group_members,
    materials,
)
from bedrock.extract.census.Census_EC import MATFUEL_RESIDUAL_CODES

BASE, TARGET = 2012, 2017

#: Shortest shared code prefix read as containment. Four digits is a NAICS
#: industry group for the product, the coarsest level at which the two lists'
#: product digits still name the same thing.
MIN_PREFIX = 4

#: The 2017 residual the ``residual`` fallback sends an unmatched 2012 code to.
RESIDUAL_2017 = '00970099'

Fallback = ta.Literal['global', 'residual', 'none']
FALLBACKS: tuple[Fallback, ...] = ('none', 'global', 'residual')

_NAICS_2012_TO_2017 = (
    Path(__file__).resolve().parents[2]
    / 'extract'
    / 'external_data'
    / '2012_to_2017_NAICS.csv'
)


@functools.cache
def _naics_2012_to_2017() -> dict[str, str]:
    """2012 NAICS-6 -> 2017 NAICS-6. A split 2012 industry takes its first child."""
    frame = pd.read_csv(_NAICS_2012_TO_2017, dtype=str, skiprows=2)
    frame = frame.rename(columns=lambda c: str(c).strip())
    old = [c for c in frame.columns if c.startswith('2012 NAICS Code')][0]
    new = [c for c in frame.columns if c.startswith('2017 NAICS Code')][0]
    frame = frame[[old, new]].dropna()
    frame = frame[
        frame[old].str.fullmatch(r'\d{6}') & frame[new].str.fullmatch(r'\d{6}')
    ]
    return frame.drop_duplicates(old).set_index(old)[new].to_dict()


def _bea_column(year: int, naics: str) -> str | None:
    """The BEA detail industry a vintage's MATFUEL industry buys in."""
    if year == BASE:
        naics = _naics_2012_to_2017().get(naics, naics)
    return bea_industry(naics)


@functools.cache
def _raw(year: int) -> pd.DataFrame:
    """The vintage's MATFUEL rows with no suppression recovery, on BEA columns.

    ⚠️ **Recovery is off in both vintages.** ``estimate_suppressed_ec_matfuel``
    crashes on 2012's shape (#991), and grading needs the same treatment on
    both sides, so neither vintage gets filled cells.
    """
    fba = materials(year, recover=False)
    fba = fba[['material', 'industry', 'FlowAmount']].copy()
    fba['material'] = fba['material'].astype(str)
    fba['industry'] = fba['industry'].astype(str)
    fba['column'] = [_bea_column(year, n) for n in fba['industry']]
    return fba.dropna(subset=['column'])


def _common_prefix(a: str, b: str) -> int:
    n = 0
    for x, y in zip(a, b, strict=False):
        if x != y:
            break
        n += 1
    return n


def _best(code: str, candidates: pd.Series) -> tuple[str, int] | None:
    """The candidate sharing the longest prefix with ``code``; ties by cost."""
    best: tuple[int, float, str] | None = None
    for other, cost in candidates.items():
        length = _common_prefix(code, str(other))
        if length < MIN_PREFIX:
            continue
        key = (length, float(cost), str(other))
        if best is None or key[:2] > best[:2]:
            best = key
    return None if best is None else (best[2], best[0])


#: Cosine similarity of industry footprints needed to call two codes the same.
MIN_FOOTPRINT = 0.8


@functools.cache
def footprint_matches() -> dict[str, tuple[str, float]]:
    """2012 special codes that place nowhere, matched on who buys them.

    2012 carries 47 ``00...`` codes that no NAICS prefix reaches, $365bn and
    12% of the vintage, against $0.4bn unplaced in 2017. The largest is
    ``00999830``, $294bn in refineries, where 2017 books domestic and foreign
    crude. With no 2012 labels, the match is the 2017 code whose cost across
    BEA columns points the same way (cosine at least :data:`MIN_FOOTPRINT`),
    among codes that place somewhere.
    """
    early, late = _raw(BASE), _raw(TARGET)
    special = early[[classify(c)[0] == 'unplaced' for c in early['material']]]
    left = special.pivot_table(
        index='column', columns='material', values='FlowAmount', aggfunc='sum'
    )
    placeable = late[[classify(c)[0] in ('direct', 'group') for c in late['material']]]
    right = placeable.pivot_table(
        index='column', columns='material', values='FlowAmount', aggfunc='sum'
    )
    rows = sorted(set(left.index) | set(right.index))
    left = left.reindex(rows).fillna(0.0)
    right = right.reindex(rows).fillna(0.0)
    a = left / np.sqrt((left**2).sum()).replace(0, np.nan)
    b = right / np.sqrt((right**2).sum()).replace(0, np.nan)
    similarity = a.T.fillna(0.0) @ b.fillna(0.0)
    out = {}
    for code, scores in similarity.iterrows():
        best = scores.idxmax()
        if float(scores[best]) >= MIN_FOOTPRINT:
            out[str(code)] = (str(best), float(scores[best]))
    return out


@functools.cache
def concordance(fallback: Fallback = 'residual') -> pd.DataFrame:
    """One row per ``(BEA column, 2012 code)``: the 2017 code it rolls into, and why.

    ``fallback='none'`` returns every code unchanged, which is #989's run.
    """
    early, late = _raw(BASE), _raw(TARGET)
    late_codes = set(late['material'])
    listed = late[~late['material'].isin(MATFUEL_RESIDUAL_CODES)]
    by_column = {
        column: frame.groupby('material')['FlowAmount'].sum()
        for column, frame in listed.groupby('column')
    }
    everywhere = listed.groupby('material')['FlowAmount'].sum()

    pairs = early.groupby(['column', 'material'])['FlowAmount'].sum().reset_index()
    rows = []
    for column, code, cost in pairs.itertuples(index=False):
        if fallback == 'none':
            rows.append((column, code, code, 'unchanged', 8, cost))
            continue
        if code in late_codes or code in MATFUEL_RESIDUAL_CODES:
            rows.append((column, code, code, 'exact', 8, cost))
            continue
        if code in footprint_matches():
            rows.append(
                (column, code, footprint_matches()[code][0], 'footprint', 0, cost)
            )
            continue
        found = _best(code, by_column.get(column, pd.Series(dtype=float)))
        if found is not None:
            rows.append((column, code, found[0], 'industry_prefix', found[1], cost))
            continue
        if fallback == 'global':
            found = _best(code, everywhere)
            if found is not None:
                rows.append((column, code, found[0], 'global_prefix', found[1], cost))
                continue
        elif fallback == 'residual':
            rows.append((column, code, RESIDUAL_2017, 'to_residual', 0, cost))
            continue
        rows.append((column, code, code, 'unmapped', 0, cost))
    return pd.DataFrame(
        rows,
        columns=['column', 'code_2012', 'code_2017', 'rule', 'prefix', 'cost_2012'],
    )


def _place(frame: pd.DataFrame) -> pd.DataFrame:
    """``commodity x BEA column`` dollars through the live placement rules.

    ``direct`` passes through, ``group`` is split on the purchasing column's
    2017 Use weights, and ``residual`` and ``unplaced`` are dropped, exactly as
    :func:`~bedrock.analysis.nowcasting.inputs_structure.place_on_commodities`
    does. The split is the same 2017 structure in both vintages, so within a
    group it creates no movement.
    """
    summed = frame.groupby(['column', 'material'])['FlowAmount'].sum()
    cells: dict[tuple[str, str], float] = {}
    for key_pair, cost in summed.items():
        column, code = ta.cast(tuple[str, str], key_pair)
        tier, bea = classify(code)
        if tier == 'direct' and bea is not None:
            cells[(bea, column)] = cells.get((bea, column), 0.0) + float(cost)
        elif tier == 'group':
            weights = _split_weights(group_members(code), column, 'column')
            if weights is None:
                continue
            for member, weight in weights.items():
                key = (str(member), column)
                cells[key] = cells.get(key, 0.0) + float(cost) * float(weight)
    placed = pd.Series(cells, dtype=float)
    placed.index = pd.MultiIndex.from_tuples(placed.index, names=['bea', 'column'])
    return placed.unstack('column').fillna(0.0)


@functools.cache
def census_blocks(fallback: Fallback = 'residual') -> tuple[pd.DataFrame, pd.DataFrame]:
    """The 2012 (concorded) and 2017 census blocks on one BEA axis."""
    early = _raw(BASE).copy()
    if fallback != 'none':
        mapping = concordance(fallback).set_index(['column', 'code_2012'])['code_2017']
        early['material'] = [
            mapping.get((col, code), code)
            for col, code in zip(early['column'], early['material'], strict=True)
        ]
    first, second = _place(early), _place(_raw(TARGET))
    rows = sorted(set(first.index) | set(second.index))
    cols = sorted(set(first.columns) & set(second.columns))
    return (
        first.reindex(index=rows, columns=cols).fillna(0.0),
        second.reindex(index=rows, columns=cols).fillna(0.0),
    )


def _common_shares(fallback: Fallback) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Both vintages' shares, renormalised over the cells both of them observe.

    ⚠️ **A code list that adds an item dilutes every other cell.** 2017 lists
    items as their own lines that 2012 left inside "all other materials"
    (poultry and paperboard containers in ``311410``, 14pp between them). Taken
    over each vintage's full support, those new lines lower every shared cell's
    share, so the index reads low even where nothing moved. Renormalising over
    the common support compares like with like.
    """
    first, second = census_blocks(fallback)
    both = (first > 0) & (second > 0)
    a, b = first.where(both, 0.0), second.where(both, 0.0)
    return (
        a / a.sum().replace(0, np.nan),
        b / b.sum().replace(0, np.nan),
    )


def churn(fallback: Fallback = 'residual', common: bool = True) -> pd.Series:
    """Half-L1 distance between the two census mixes, per BEA column, in pp."""
    if common:
        a, b = _common_shares(fallback)
    else:
        first, second = census_blocks(fallback)
        a = first / first.sum().replace(0, np.nan)
        b = second / second.sum().replace(0, np.nan)
    return (100 * (a - b).abs().sum() / 2).dropna()


def graded_columns() -> list[str]:
    """Manufacturing plus the seeded mining columns: what the live seed moves."""
    return [*_manufacturing_bea_industries(), *MINING_SEEDED]


def index_for(
    fallback: Fallback = 'residual', masked: bool = True, common: bool = False
) -> bh.IndexFor:
    """The census 2012 -> 2017 share index per column, the seed's own form.

    ``masked`` keeps only cells both vintages observe, so a cell absent from
    one census holds its benchmark, as the live seed does. Unmasked lets a cell
    present in 2012 and absent in 2017 fall to zero. ``common`` takes the
    shares over the common support (:func:`_common_shares`); it implies masking.
    """
    if common:
        a, b = _common_shares(fallback)
        masked = True
    else:
        first, second = census_blocks(fallback)
        a = first / first.sum().replace(0, np.nan)
        b = second / second.sum().replace(0, np.nan)

    def one(column: str) -> pd.Series:
        if column not in a.columns:
            return _EMPTY
        base, late = a[column], b[column]
        keep = (base > 0) & (late > 0) if masked else base > 0
        ratio = (late[keep] / base[keep]).replace([np.inf, -np.inf], np.nan).dropna()
        return ratio

    return one


def grade(
    fallback: Fallback = 'residual', masked: bool = True, common: bool = False
) -> pd.DataFrame:
    """:func:`~bedrock.analysis.nowcasting.benchmark_holdout.holdout_score` on this seed."""
    scored = bh.holdout_score(index_for(fallback, masked, common), graded_columns())
    scored['frame'] = [
        'mining' if c in MINING_SEEDED else 'manufacturing' for c in scored.index
    ]
    return scored


_EMPTY = pd.Series(dtype=float)


def _damped(base: bh.IndexFor, alpha: float) -> bh.IndexFor:
    return lambda column: base(column) ** alpha


def _gated(base: bh.IndexFor, held: set[str]) -> bh.IndexFor:
    return lambda column: _EMPTY if column in held else base(column)


def shrinkage(
    fallback: Fallback = 'residual',
    alphas: tuple[float, ...] = (0, 0.1, 0.2, 0.3, 0.5, 0.75, 1),
) -> pd.DataFrame:
    """The grade with the census index raised to ``alpha``, on common support.

    ⚠️ **BEA's 2017 benchmark follows the prior benchmark, not the census.** On
    the census-covered set BEA moves 8.5pp over 2012 -> 2017 against the census's
    29.8pp, and 97% of columns sit closer to BEA 2012 than to census 2017 (#988).
    A key that adopted a fraction of the census movement punishes the full index
    even when its direction is right. So the question this answers is whether
    the census carries signal at *any* strength: a best ``alpha`` above zero says
    it does, and the gain at that ``alpha`` says how much.
    """
    base = index_for(fallback, common=True)
    records = []
    for alpha in alphas:

        scored = bh.holdout_score(_damped(base, alpha), graded_columns())
        man = scored[[c not in MINING_SEEDED for c in scored.index]]
        records.append({'alpha': alpha, **bh.aggregate(man)})
    return pd.DataFrame(records).set_index('alpha')


def churn_gate(
    fallback: Fallback = 'residual',
    thresholds: tuple[float, ...] = (5, 10, 15, 20, 30, 1000),
) -> pd.DataFrame:
    """#988's remedy tested: hold every column whose census churn exceeds a bar.

    The full-strength index on common support, with columns above the churn
    threshold held at their 2012 benchmark. ``1000`` holds nothing.
    """
    base = index_for(fallback, common=True)
    moved = churn(fallback, common=True)
    records = []
    for bar in thresholds:
        held = set(moved.index[moved > bar])

        scored = bh.holdout_score(_gated(base, held), graded_columns())
        man = scored[[c not in MINING_SEEDED for c in scored.index]]
        records.append(
            {
                'churn_bar_pp': bar,
                'held_columns': len(held & set(man.index)),
                **bh.aggregate(man),
            }
        )
    return pd.DataFrame(records).set_index('churn_bar_pp')


def direction(fallback: Fallback = 'residual') -> dict[str, float]:
    """Do census and BEA move the same cells the same way, 2012 -> 2017?

    Per manufacturing cell both censuses and both benchmarks observe, the
    census log share ratio (common support) against BEA's, weighted by the
    cell's 2017 BEA dollars.
    """
    a, b = _common_shares(fallback)
    early, late = bh.block(BASE), bh.block(TARGET)
    xs: list[float] = []
    ys: list[float] = []
    ws: list[float] = []
    for column in a.columns:
        if column not in early.columns or column[:2] not in ('31', '32', '33'):
            continue
        e = early[column] / early[column].sum()
        t = late[column] / late[column].sum()
        for code in a.index[(a[column] > 0) & (b[column] > 0)]:
            if e.get(code, 0) > 0 and t.get(code, 0) > 0:
                later = float(ta.cast(float, b.at[code, column]))
                earlier = float(ta.cast(float, a.at[code, column]))
                xs.append(float(np.log(later / earlier)))
                ys.append(float(np.log(float(t[code]) / float(e[code]))))
                ws.append(float(ta.cast(float, late.at[code, column])))
    x, y, w = np.array(xs), np.array(ys), np.array(ws)
    mx, my = np.average(x, weights=w), np.average(y, weights=w)
    cov = np.average((x - mx) * (y - my), weights=w)
    corr = cov / np.sqrt(
        np.average((x - mx) ** 2, weights=w) * np.average((y - my) ** 2, weights=w)
    )
    slope = cov / np.average((x - mx) ** 2, weights=w)
    agree = float(np.average(np.sign(x) == np.sign(y), weights=w))
    return {
        'cells': float(len(x)),
        'weighted_corr': float(corr),
        'slope_bea_on_census': float(slope),
        'sign_agreement': agree,
    }


def coverage(fallback: Fallback) -> pd.DataFrame:
    """Share of 2012 cost handled by each concordance rule."""
    table = concordance(fallback)
    out = table.groupby('rule')['cost_2012'].sum()
    return (out / out.sum()).rename('share_of_2012_cost').to_frame()


def summary() -> pd.DataFrame:
    """One row per (fallback, masking): median churn and the graded gain."""
    records = []
    for fallback in FALLBACKS:
        for support, masked, common in (
            ('full, unmasked', False, False),
            ('full, masked', True, False),
            ('common', True, True),
        ):
            moved = churn(fallback, common=common)
            scored = grade(fallback, masked, common)
            for frame in ('manufacturing', 'mining'):
                verdict = bh.aggregate(scored[scored['frame'] == frame])
                records.append(
                    {
                        'fallback': fallback,
                        'support': support,
                        'frame': frame,
                        'median_churn_pp': float(moved.median()),
                        **verdict,
                    }
                )
    return pd.DataFrame(records)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true', help='fail on a broken run')
    args = parser.parse_args()
    pd.set_option('display.width', 200)

    for fallback in FALLBACKS[1:]:
        print(f'\n=== concordance coverage, fallback={fallback} ===')
        print(coverage(fallback).round(4).to_string())
    table = summary()
    print('\n=== 2012 -> 2017 holdout, impact-weighted ===')
    print(table.round(4).to_string(index=False))
    print('\n=== direction: census against BEA, per cell ===')
    print(direction())
    print('\n=== shrinkage: index ** alpha, manufacturing, common support ===')
    print(shrinkage().round(4).to_string())
    print('\n=== churn gate: hold columns above the bar ===')
    print(churn_gate().round(4).to_string())

    if args.check:
        none = table[table['fallback'] == 'none']
        assert (none['columns'] > 0).all(), 'the unmapped run scored no columns'
        for fallback in FALLBACKS[1:]:
            covered = coverage(fallback)['share_of_2012_cost']
            assert covered.sum() > 0.999, f'{fallback}: concordance drops cost'
        print('\ncheck: ok')


if __name__ == '__main__':
    main()
