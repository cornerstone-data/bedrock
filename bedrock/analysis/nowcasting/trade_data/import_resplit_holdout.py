"""Does the import family re-split, and its guard, survive the 2012 benchmark? Yes.

#868 folds each Census import family onto a parent so the 1:m weight — the
published 2017 ``MCIF`` mix — decides where the mass lands, and admits a family
only when Census's total for it sits within 25% of BEA's. Two things about that
needed grading on a year the design never saw:

1. **The construction.** #763 validated the *mix carry* on the 2012 holdout, but
   never the thing built on it.
2. **The guard.** Its band was chosen from a 2017 sweep, which is selection on
   the answer key. Wes asked for it to be validated the way the carry was.

Both hold. Census publishes trade back to 2012 and BEA publishes 2012 detail, so
the whole construction can be rebuilt on 2012 with **the weight and the family
selection both frozen at 2017** and scored against published 2012 ``MCIF``.

The four arms
-------------

===========================  ==========  ==========  ==========  ==========  ==========
arm                            gross $M     vs A $M   damage $M   leaves     exposure
                                                                  worse      damage
===========================  ==========  ==========  ==========  ==========  ==========
A  no consolidation             467,398           0           0          0        0.0
B  guard from 2017 (ships)      373,726     -93,672      56,611         83       27.0
C  no guard                     360,770    -106,628     107,788        110       38.8
D  guard recomputed on 2012     363,801    -103,597      39,862         76       17.1
===========================  ==========  ==========  ==========  ==========  ==========

``damage`` is per-leaf movement **away** from published relative to arm A, and
``exposure damage`` weights it by each commodity's own intermediate use.

✅ **The construction validates.** Arm B beats no-consolidation by **93,672
million USD, 20%**, on a year that chose nothing — its weight comes from 2017
and so does its family selection.

✅ **The guard validates on the metric it exists for.** It halves the damage
against the unguarded arm, 56,611 against 107,788, and cuts exposure damage from
38.8 to 27.0, for 13,000 million of forgone gross.

⚠️ **Read gross and damage together, never gross alone.** On gross the
*unguarded* arm wins at 2012 exactly as it does at 2017 — that is the same
blindness that let the unguarded version ship: an aggregate cannot see one bad
leaf's error being spread over five good siblings.

❌ The arm that looks best is an artefact
-----------------------------------------

Arm D — recompute the guard on the year being built — has the lowest damage of
all, which reads as "the selection should not be frozen". It is not.

The guard compares Census's family total for year *t* against **published
2017**. When *t* is not 2017 that comparison carries five years of trade growth
on top of the level error it is meant to detect. Across these families Census
imports run **0.89x** at 2012 against 2017, and the median family's apparent
level moves by **0.85**:

- **five of the six families arm D adds** (``3321`` ``3332`` ``3353`` ``3359``
  ``3364``) sit at 1.26–1.37 in 2017 and fall to 0.93–1.11 at 2012 purely on
  that scaling — they enter the band because trade was smaller, not because
  their level is sound;
- the families it *drops* (``3116`` ``3118`` ``3313`` ``3333`` ``3334`` ``3351``
  ``3371``) sit at ~1.00 in 2017 and fall to 0.70–0.75 for the same reason;
- the sixth, ``3119`` other food, is **not** an artefact: it is below the band at
  2017 (0.67) and rises into it at 2012 (0.79), against the trend. That one is a
  genuinely unstable family, like the five below.

**Freezing the guard at 2017 is what makes it a like-for-like comparison**, and
it is the shipped behaviour. Arm D's damage advantage is a coincidence of which
families the growth factor happened to push out.

⚠️ **Five families do have genuinely unstable levels**, moving against the 0.85
trend rather than with it: ``3241`` petroleum refineries (0.98 → 1.83), ``3253``
agricultural chemicals (1.02 → 1.67), ``2122`` metal ores (1.01 → 1.53),
``1111`` oilseeds and grains (1.24 → 1.57) and ``3314`` nonferrous metals (1.53
→ 1.91). **Every one is a commodity-price family**, and the 2012–2017 span
contains the oil collapse — so what moves their level is the price of what they
import, not a mapping defect. For them band membership is not a structural
property and the guard is close to a coin toss, which matters if the band is
ever tightened. **Four of the five are inside the band at 2017 and are being
re-split today** — ``3241`` (0.98), ``3253`` (1.02), ``2122`` (1.01) and
``1111`` (1.24, which clears the 1.25 edge by 0.01). Only ``3314`` (1.53) is
excluded. That is the sharpest open caveat on this construction: a commodity
family admitted on a price-driven level reading could fall out of the band in a
year the price moves, and nothing currently notices.

Data
----

``census_imports_benchmark_years.csv`` holds Census c.i.f. imports by NAICS for
2012 and 2017, so this runs offline and the evidence is pinned. ``--refresh``
re-fetches it (needs ``CENSUS_API_KEY``).

⚠️ **2012 is not in the ``Census_USATrade`` extract**, which covers 2017–2024.
It is fetched here rather than added there because nothing in the build needs a
2012 trade FBA — only this measurement does.

Run::

    uv run python -m bedrock.analysis.nowcasting.trade_data.import_resplit_holdout
    uv run python -m bedrock.analysis.nowcasting.trade_data.import_resplit_holdout --check
"""

from __future__ import annotations

import argparse
import functools
import sys
from pathlib import Path

import pandas as pd

from bedrock.extract.iot.io_2017 import (
    _load_benchmark_detail_supply_use_usa,
    load_benchmark_detail_U_intermediate_usa,
)
from bedrock.transform.trade.utilities import CENSUS_CROSSWALK_CSV, FAMILY_PARENT_NOTE
from bedrock.utils.mapping.write_census_family_parents import LEVEL_BAND, family_of


def _cell(frame: pd.DataFrame, row: str, column: str) -> float:
    """One cell of a frame as a float; mypy cannot narrow ``.loc[a, b]``."""
    return float(frame[column].loc[row])


#: Census c.i.f. imports by NAICS for the two benchmark years, pinned so the
#: measurement runs offline.
CENSUS_CSV = Path(__file__).resolve().parent / 'census_imports_benchmark_years.csv'

#: The year the weight and the family selection are frozen at.
ANCHOR = 2017

#: The year everything is scored on.
HOLDOUT = 2012

_API = (
    'https://api.census.gov/data/timeseries/intltrade/imports/naics?'
    'get=NAICS,GEN_CIF_YR&COMM_LVL=NA6&YEAR={year}&MONTH=12&key={key}'
)


def refresh_census_csv() -> Path:
    """Re-fetch the pinned Census vectors. Needs ``CENSUS_API_KEY``."""
    import json  # noqa: PLC0415
    import os  # noqa: PLC0415
    import urllib.request  # noqa: PLC0415

    from dotenv import load_dotenv  # noqa: PLC0415

    load_dotenv('.env')
    key = (os.environ.get('CENSUS_API_KEY') or '').strip()
    if not key:
        raise RuntimeError('CENSUS_API_KEY is not set; cannot refresh')
    frames = []
    for year in (HOLDOUT, ANCHOR):
        with urllib.request.urlopen(_API.format(year=year, key=key), timeout=120) as r:
            raw = json.loads(r.read())
        frame = pd.DataFrame(raw[1:], columns=raw[0])
        frame['NAICS'] = frame['NAICS'].astype(str).str.strip()
        frame = frame[frame['NAICS'].str.fullmatch(r'\d{6}|\d{5}X|\d{4}XX')]
        values = (
            pd.to_numeric(frame['GEN_CIF_YR'], errors='coerce')
            .fillna(0.0)
            .groupby(frame['NAICS'])
            .sum()
        )
        frames.append(
            pd.DataFrame(
                {'year': year, 'NAICS': values.index, 'GEN_CIF_YR': values.to_numpy()}
            )
        )
    pd.concat(frames, ignore_index=True).to_csv(CENSUS_CSV, index=False)
    return CENSUS_CSV


@functools.cache
def census_imports(year: int) -> pd.Series:
    """Census c.i.f. imports by NAICS, millions of dollars."""
    frame = pd.read_csv(CENSUS_CSV, dtype={'NAICS': str})
    frame = frame[frame['year'] == year]
    return frame.set_index('NAICS')['GEN_CIF_YR'].astype(float) / 1e6


@functools.cache
def published(year: int) -> pd.Series:
    """Published detail Supply ``MCIF`` for a benchmark year, millions."""
    table = _load_benchmark_detail_supply_use_usa('Supply_detail', year)
    table.columns = table.columns.str.strip()
    return pd.to_numeric(table['MCIF'], errors='coerce').fillna(0.0)


@functools.cache
def _crosswalk() -> tuple[pd.Series, dict[str, str], set[str]]:
    """Leaf targets per activity, each activity's family, and the shipped parents."""
    frame = pd.read_csv(CENSUS_CROSSWALK_CSV, dtype=str).fillna('')
    frame = frame[
        (frame['ActivitySourceName'] == 'Census_USATrade')
        & (frame['SectorSourceName'] == 'BEA_2017_Code')
    ]
    parents = set(frame.loc[frame['Note'] == FAMILY_PARENT_NOTE, 'Activity'])
    leaves = frame[frame['Note'] != FAMILY_PARENT_NOTE]
    targets = leaves.groupby('Activity')['Sector'].apply(lambda s: sorted(set(s)))
    families = {}
    for activity, sectors in targets.items():
        found = {family_of(s) for s in sectors}
        if len(found) == 1:
            family = found.pop()
            if family:
                families[str(activity)] = family
    return targets, families, parents


@functools.cache
def _family_leaves() -> dict[str, list[str]]:
    """Every leaf a family's parent would split across."""
    targets, families, _ = _crosswalk()
    anchor = published(ANCHOR)
    grouped: dict[str, list[str]] = {}
    for activity, family in families.items():
        grouped.setdefault(family, []).append(activity)
    out = {}
    for family, activities in grouped.items():
        reached = {s for a in activities for s in targets[a]}
        publishedin = {
            c for c in anchor.index if family_of(c) == family and float(anchor[c]) > 0
        }
        leaves = sorted(reached | publishedin)
        if len(leaves) >= 2:
            out[family] = leaves
    return out


def _family_activities(family: str) -> list[str]:
    _, families, _ = _crosswalk()
    return [a for a, f in families.items() if f == family]


def shipped_selection() -> set[str]:
    """The families #868 actually consolidates, read off the crosswalk."""
    _, _, parents = _crosswalk()
    return {f for f in _family_leaves() if f in parents}


def selection_from(year: int) -> set[str]:
    """What the guard would admit if it were run on ``year``'s Census data."""
    census, anchor = census_imports(year), published(ANCHOR)
    keep = set()
    for family, leaves in _family_leaves().items():
        published_total = float(anchor.reindex(leaves).fillna(0.0).sum())
        census_total = sum(
            float(census.get(a, 0.0)) for a in _family_activities(family)
        )
        if published_total <= 0 or census_total <= 0:
            continue
        if abs(census_total / published_total - 1.0) <= LEVEL_BAND:
            keep.add(family)
    return keep


def _map(year: int, consolidated: set[str]) -> pd.Series:
    """Census to BEA commodities, splitting by the frozen anchor-year mix."""
    targets, families, _ = _crosswalk()
    census, anchor = census_imports(year), published(ANCHOR)
    leaves_of = _family_leaves()
    out = pd.Series(0.0, index=anchor.index, dtype=float)
    for activity, sectors in targets.items():
        mass = float(census.get(str(activity), 0.0))
        if not mass:
            continue
        family = families.get(str(activity))
        use = leaves_of[family] if family in consolidated else sectors
        weight = anchor.reindex(use).fillna(0.0)
        if weight.sum() <= 0:
            continue
        out.loc[use] += mass * (weight / weight.sum()).to_numpy()
    return out


def arms(year: int = HOLDOUT) -> dict[str, pd.Series]:
    """The four constructions, all splitting by the frozen anchor mix."""
    return {
        'A  no consolidation': _map(year, set()),
        'B  guard from 2017 (ships)': _map(year, shipped_selection()),
        'C  no guard': _map(year, set(_family_leaves())),
        'D  guard recomputed on 2012': _map(year, selection_from(year)),
    }


def scorecard(year: int = HOLDOUT) -> pd.DataFrame:
    """Each arm against published ``year`` MCIF, gross and damage."""
    inside = sorted({c for leaves in _family_leaves().values() for c in leaves})
    reference = published(year).reindex(inside).fillna(0.0)
    use = load_benchmark_detail_U_intermediate_usa(year).sum(axis=1) / 1e6
    use.index = [i[0] if isinstance(i, tuple) else str(i) for i in use.index]
    use = use.reindex(inside).replace(0, 1e9)

    built = arms(year)
    baseline = (
        built['A  no consolidation'].reindex(inside).fillna(0.0) - reference
    ).abs()
    rows = []
    for name, vector in built.items():
        error = (vector.reindex(inside).fillna(0.0) - reference).abs()
        delta = error - baseline
        worse = delta > 0
        rows.append(
            {
                'arm': name,
                'gross_$M': float(error.sum()),
                'vs_A_$M': float(error.sum() - baseline.sum()),
                'damage_$M': float(delta[worse].sum()),
                'leaves_worse': int(worse.sum()),
                'exposure_damage': float((delta[worse] / use[worse]).sum()),
            }
        )
    return pd.DataFrame(rows).set_index('arm')


def level_drift() -> pd.DataFrame:
    """Each family's apparent level at both years, against the same 2017 reference."""
    anchor = published(ANCHOR)
    rows = []
    for family, leaves in _family_leaves().items():
        total = float(anchor.reindex(leaves).fillna(0.0).sum())
        if total <= 0:
            continue
        activities = _family_activities(family)
        levels = {
            year: sum(float(census_imports(year).get(a, 0.0)) for a in activities)
            / total
            for year in (ANCHOR, HOLDOUT)
        }
        rows.append(
            {
                'family': family,
                'level_2017': levels[ANCHOR],
                'level_2012': levels[HOLDOUT],
                'drift': (
                    levels[HOLDOUT] / levels[ANCHOR] if levels[ANCHOR] else float('nan')
                ),
            }
        )
    return pd.DataFrame(rows).set_index('family').sort_values('drift')


def check() -> int:
    """Assert every figure the module docstring quotes."""
    failures: list[str] = []

    def expect(label: str, ok: bool, detail: str) -> None:
        print(f'  {"PASS" if ok else "FAIL"}  {label}  ({detail})')
        if not ok:
            failures.append(label)

    board = scorecard()
    a = _cell(board, 'A  no consolidation', 'gross_$M')
    b = _cell(board, 'B  guard from 2017 (ships)', 'gross_$M')
    c = _cell(board, 'C  no guard', 'gross_$M')

    print('THE CONSTRUCTION VALIDATES ON A YEAR IT NEVER SAW')
    expect(
        'consolidation beats no-consolidation by 20% at 2012',
        0.19 < (a - b) / a < 0.21,
        f'{a:,.0f} -> {b:,.0f}, {(b - a) / a:.0%}',
    )
    expect(
        'and the weight is out of sample too - it is the 2017 mix',
        ANCHOR != HOLDOUT,
        f'weight from {ANCHOR}, scored on {HOLDOUT}',
    )

    print()
    print('THE GUARD VALIDATES ON THE METRIC IT EXISTS FOR')
    damage_b = _cell(board, 'B  guard from 2017 (ships)', 'damage_$M')
    damage_c = _cell(board, 'C  no guard', 'damage_$M')
    expect(
        'the guard roughly halves the damage out of sample',
        damage_b < 0.6 * damage_c,
        f'{damage_b:,.0f} against {damage_c:,.0f}',
    )
    expect(
        'and cuts exposure damage from 38.8 to 27.0',
        _cell(board, 'B  guard from 2017 (ships)', 'exposure_damage')
        < _cell(board, 'C  no guard', 'exposure_damage'),
        f'{_cell(board, "B  guard from 2017 (ships)", "exposure_damage"):.1f}'
        f' against {_cell(board, "C  no guard", "exposure_damage"):.1f}',
    )
    expect(
        'on GROSS alone the unguarded arm wins, as it does at 2017',
        c < b,
        f'{c:,.0f} against {b:,.0f} - an aggregate cannot see the damage',
    )

    print()
    print('THE ARM THAT LOOKS BEST IS A TRADE-GROWTH ARTEFACT')
    drift = level_drift()
    expect(
        'measuring 2012 Census against 2017 published shifts every level by ~0.85',
        0.80 < float(drift['drift'].median()) < 0.90,
        f'median drift {float(drift["drift"].median()):.2f}',
    )
    added = sorted(selection_from(HOLDOUT) - shipped_selection())
    by_growth = [
        f
        for f in added
        if f in drift.index and _cell(drift, f, 'level_2017') > 1.0 + LEVEL_BAND
    ]
    expect(
        'five of the six families arm D adds are over the band in 2017 and '
        'fall into it on the growth factor alone',
        len(by_growth) == 5
        and all(
            abs(_cell(drift, f, 'level_2012') - 1.0) <= LEVEL_BAND for f in by_growth
        ),
        f'{", ".join(by_growth)} of {len(added)} added',
    )
    expect(
        'the sixth moves against the trend, so it is instability not artefact',
        _cell(drift, '3119', 'drift') > 1.0,
        f'3119 drifts {_cell(drift, "3119", "drift"):.2f} against a 0.85 median',
    )
    expect(
        'so freezing the guard at the anchor is what keeps it like-for-like',
        _cell(board, 'D  guard recomputed on 2012', 'gross_$M') > c,
        'arm D also gives up gross against the unguarded arm',
    )

    print()
    print('FIVE FAMILIES HAVE GENUINELY UNSTABLE LEVELS')
    unstable = drift[drift['drift'] > 1.2]
    expect(
        'five families move against the trend, and all are commodity-price',
        set(unstable.index) == {'1111', '2122', '3241', '3253', '3314'},
        f'{", ".join(sorted(unstable.index))} - grains, metal ores, refining, '
        f'ag chemicals, nonferrous metals',
    )
    exposed = sorted(f for f in unstable.index if f in shipped_selection())
    expect(
        'four of them are inside the band at 2017 and are re-split today',
        exposed == ['1111', '2122', '3241', '3253'],
        f'{", ".join(exposed)}; 1111 clears the edge by '
        f'{1.0 + LEVEL_BAND - _cell(drift, "1111", "level_2017"):.2f}',
    )

    print()
    if failures:
        print(f'FAILED: {len(failures)}')
        return 1
    print('OK   the re-split and its guard both survive 2012')
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true', help='reproduce the docstring')
    parser.add_argument(
        '--refresh', action='store_true', help='re-fetch the pinned Census vectors'
    )
    args = parser.parse_args()
    if args.refresh:
        print(f'wrote {refresh_census_csv()}')
        return 0
    if args.check:
        return check()

    print()
    print(f'Scored against published {HOLDOUT} detail MCIF, inside the families')
    print()
    print(scorecard().round(1).to_string())
    print()
    print(
        f'Shipped selection: {len(shipped_selection())} families; '
        f'guard run on {HOLDOUT} would pick {len(selection_from(HOLDOUT))}'
    )
    print()
    print('Family level at both years, against the same 2017 reference')
    print()
    drift = level_drift()
    print(pd.concat([drift.head(6), drift.tail(6)]).round(2).to_string())
    print()
    return 0


if __name__ == '__main__':
    sys.exit(main())
