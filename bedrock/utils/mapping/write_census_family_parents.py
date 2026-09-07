"""Write the Census family-parent rows the import within-family re-split needs (#763).

[#763](https://github.com/cornerstone-data/bedrock/issues/763) validated a
construction on the 2012 holdout and closed without wiring it: *each family's
within-split anchored on the published 2017 mix, moved by the Census family
level*. This script generates the crosswalk half of that wiring.

Why anything is needed at all: the import method already splits **1:m** Census
activities by frozen 2017 Supply ``MCIF``, which *is* anchor-and-move. The
problem is that almost every family is mapped **1:1 or m:1**, so no attribution
weight is ever applied and Census's own within-family split stands unchallenged
— and that split is not BEA's. Giving each family a parent activity converts the
whole family into one 1:m row, which is the same device #702 used for vehicles
and #865 for aerospace exports.

The parent code is the four-digit family (``3371``), which cannot collide with a
Census activity: ``census_usatrade_parse`` keeps only ``\\d{6}``, ``\\d{5}X`` and
``\\d{4}XX``.

⚠️ **Generated rows carry ``Note = FAMILY_PARENT``.** Nothing else in the file
uses that marker, and ``trade.utilities.census_family_parents`` reads it to
decide which activities to relabel — so the crosswalk and the clean function
cannot disagree about what a family is.

The level guard
---------------

❌ **A family is only re-split if Census's total for it is close to BEA's.** The
construction is "anchored on the published mix, **moved by the Census family
level**", and that premise fails when the family total is contaminated: the
re-split then takes one leaf's level error and smears it over siblings that
were fine.

``3399`` is the case that forced this. **88% of its excess sat on one leaf** —
``339910`` jewelry at 2.93x published — and re-splitting dragged five leaves
that were within 9% of published to 1.50x:

========  ==========  ==========  ==========
leaf      published   unguarded   ratio was
========  ==========  ==========  ==========
339910       13,262      19,886        2.93
339920        7,732      11,594        1.07
339930       19,427      29,130        1.07
339940        1,939       2,907        1.09
339950          282         423        1.06
339990       15,182      22,765        1.07
========  ==========  ==========  ==========

Family *gross* barely moves there (28,880 to 28,912), which is why an aggregate
check misses it entirely — one bad row becomes six mediocre ones, and
``339930``'s exposure goes from 161% to 1049% of its own intermediate use.

Measured over every family, the guard costs little and prevents a lot:

=====================  ==========  ============  ==========  ==============
band on \|level - 1\|   families    gross saved   damage      leaves pushed
=====================  ==========  ============  ==========  ==============
0.20                           48      107,684      11,737              57
**0.25 (shipped)**             52      136,228      15,148              68
0.35                           58      149,818      24,228              82
no guard                       65      150,726      76,499              95
=====================  ==========  ============  ==========  ==============

**0.25 keeps 90% of the available gain and removes 80% of the damage**, and it
is the widest band that still excludes ``3359``, the second-worst family by
error added per dollar of intermediate use. The threshold is a judgement; the
table above is here so the next person can move it with their eyes open.

✅ **Validated out of sample on 2012.** With the weight and the family selection
both frozen at 2017, the construction beats no-consolidation by **93,672 million
USD (20%)** against published 2012 ``MCIF``, and the guard **halves the damage**
the unguarded version does (56,611 against 107,788). See
``analysis/nowcasting/trade_data/import_resplit_holdout.py --check``, which also
shows why recomputing the guard per year looks better and is an artefact.

⚠️ **The one open caveat is commodity-price families.** ``3241`` refineries,
``3253`` agricultural chemicals, ``2122`` metal ores and ``1111`` grains all
move their level by more than 20% between the two benchmarks on price alone, and
all four are inside the band today - ``1111`` by 0.01. Their band membership is
not a structural property.

⚠️ **A family the guard excludes is not abandoned — it is
[#670](https://github.com/cornerstone-data/bedrock/issues/670)'s.** Its level is
wrong, which is exactly what that issue owns, and re-splitting inside it would
disguise the level error as six smaller mix errors.

Rerun after a crosswalk change, then commit the diff::

    uv run python -m bedrock.utils.mapping.write_census_family_parents
"""

from __future__ import annotations

import sys

import pandas as pd

from bedrock.extract.iot.io_2017 import _load_2017_detail_supply_use_usa

CROSSWALK = (
    'bedrock/utils/mapping/activitytosectormapping/Sector_Crosswalk_Census_USATrade.csv'
)

#: Marks a generated parent row. Also the contract with the clean function.
MARKER = 'FAMILY_PARENT'

#: The source whose activities get parents. Only the Census goods crosswalk.
SOURCE = 'Census_USATrade'
SCHEMA = 'BEA_2017_Code'

#: The import flow whose family totals the guard is measured on.
IMPORT_FLOW = 'GEN_CIF_YR'

#: The anchor year, whose published MCIF is both the split weight and the
#: reference the family level is measured against.
ANCHOR = 2017

#: How far Census's family total may sit from BEA's before the family is left
#: alone. See "The level guard" above for what this buys and what it costs.
LEVEL_BAND = 0.25

#: The years ``--audit`` re-reads the guard's decision on.
AUDIT_YEARS: tuple[int, ...] = (2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024)


def family_of(code: str) -> str:
    """The four-digit family a BEA detail commodity belongs to.

    ⚠️ ``S00*`` codes are **not** a family. Scrap, used and secondhand goods
    (``S00401`` / ``S00402``) share a four-character prefix without sharing a
    product concept, and their import treatment is #703's and #768's, not a
    within-family split. Returning an empty family keeps them out.
    """
    text = str(code)
    return '' if text.startswith('S00') else text[:4]


def leaf_rows() -> pd.DataFrame:
    """The crosswalk without any previously generated parent rows."""
    frame = pd.read_csv(CROSSWALK, dtype=str).fillna('')
    return frame[frame['Note'] != MARKER]


def published_mcif() -> pd.Series:
    """Published 2017 Supply ``MCIF`` by commodity, millions of dollars."""
    supply = _load_2017_detail_supply_use_usa('Supply_detail')
    supply.columns = supply.columns.str.strip()
    return pd.to_numeric(supply['MCIF'], errors='coerce').fillna(0.0)


def activity_families(frame: pd.DataFrame) -> dict[str, str]:
    """Each Census activity's family, for activities that sit in exactly one.

    ⚠️ An activity whose targets straddle two families is **excluded**, not
    forced. Relabelling it would move mass between families, which is a level
    change, and this construction may only move mass *within* one.
    """
    census = frame[
        (frame['ActivitySourceName'] == SOURCE) & (frame['SectorSourceName'] == SCHEMA)
    ]
    targets = census.groupby('Activity')['Sector'].apply(lambda s: sorted(set(s)))
    out = {}
    for activity, sectors in targets.items():
        families = {family_of(s) for s in sectors}
        if len(families) == 1:
            family = families.pop()
            if family:
                out[str(activity)] = family
    return out


def census_family_totals(activities: dict[str, list[str]]) -> dict[str, float]:
    """Census 2017 c.i.f. imports per family, millions of dollars.

    All of a family's activities map inside it, so the family total is just
    their sum - no crosswalk arithmetic is needed to get the denominator the
    guard compares against.
    """
    from bedrock.extract.flowbyactivity import getFlowByActivity  # noqa: PLC0415

    fba = getFlowByActivity(SOURCE, ANCHOR)
    flow = fba.loc[fba['FlowName'] == IMPORT_FLOW]
    amounts = (
        pd.to_numeric(flow['FlowAmount'], errors='coerce')
        .groupby(flow['ActivityProducedBy'].astype(str))
        .sum()
        / 1e6
    )
    return {
        family: float(sum(float(amounts.get(a, 0.0)) for a in acts))
        for family, acts in activities.items()
    }


def build() -> pd.DataFrame:
    """The parent rows: one per (family, leaf), for families the guard admits."""
    frame = leaf_rows()
    census = frame[
        (frame['ActivitySourceName'] == SOURCE) & (frame['SectorSourceName'] == SCHEMA)
    ]
    targets = census.groupby('Activity')['Sector'].apply(lambda s: sorted(set(s)))
    mcif = published_mcif()

    families: dict[str, list[str]] = {}
    for activity, family in activity_families(frame).items():
        families.setdefault(family, []).append(activity)

    census_totals = census_family_totals(families)
    rows, excluded = [], []
    for family, activities in sorted(families.items()):
        reached = {s for a in activities for s in targets[a]}
        published = {
            c for c in mcif.index if family_of(c) == family and float(mcif[c]) > 0
        }
        leaves = sorted(reached | published)
        if len(leaves) < 2:
            # Nothing to split. `3346` is the standing example: three Census
            # codes fold onto one commodity, which is #670's level problem and
            # is not reachable from here.
            continue
        published_total = float(sum(float(mcif.get(c, 0.0)) for c in leaves))
        census_total = census_totals.get(family, 0.0)
        if published_total <= 0 or census_total <= 0:
            excluded.append((family, float('nan')))
            continue
        level = census_total / published_total
        if abs(level - 1.0) > LEVEL_BAND:
            # Census's total does not measure this family, so "moved by the
            # Census family level" has no premise. Leave it to #670.
            excluded.append((family, level))
            continue
        for leaf in leaves:
            rows.append(
                {
                    'ActivitySourceName': SOURCE,
                    'Activity': family,
                    'SectorSourceName': SCHEMA,
                    'Sector': leaf,
                    'SectorType': 'C',
                    'Note': MARKER,
                }
            )
    if excluded:
        print(f'{len(excluded)} families left to #670 by the level guard:')
        for family, level in sorted(excluded, key=lambda r: -abs((r[1] or 0) - 1)):
            print(f'    {family}  level {level:.2f}')
    return pd.DataFrame(rows)


def _census_by_family(year: int, activities: dict[str, list[str]]) -> pd.Series:
    from bedrock.extract.flowbyactivity import getFlowByActivity  # noqa: PLC0415

    fba = getFlowByActivity(SOURCE, int(year))
    flow = fba.loc[fba['FlowName'] == IMPORT_FLOW]
    amounts = (
        pd.to_numeric(flow['FlowAmount'], errors='coerce')
        .groupby(flow['ActivityProducedBy'].astype(str))
        .sum()
        / 1e6
    )
    return pd.Series(
        {
            family: float(sum(float(amounts.get(a, 0.0)) for a in acts))
            for family, acts in activities.items()
        }
    )


def _summary_mcif(year: int) -> pd.Series:
    """Published summary Supply ``MCIF`` for a year, millions of dollars."""
    from bedrock.extract.iot.io_2017 import (  # noqa: PLC0415
        GCS_USA_SUP_DIR,
        LOCAL_USA_SUP_DIR,
    )
    from bedrock.utils.io.gcp import load_from_gcs  # noqa: PLC0415

    table = load_from_gcs(
        name='Supply_Tables_1997-2024_Summary.xlsx',
        sub_bucket=GCS_USA_SUP_DIR,
        local_dir=LOCAL_USA_SUP_DIR,
        loader=lambda pth: pd.read_excel(
            pth, sheet_name=str(year), skiprows=5, dtype={'Unnamed: 0': str}
        ),
    )
    table = table.set_index(table.columns[0])
    table.index = table.index.astype(str).str.strip()
    table.columns = table.columns.astype(str).str.strip()
    return pd.to_numeric(table['MCIF'], errors='coerce').fillna(0.0)


def band_audit() -> pd.DataFrame:
    """Does every family still sit where the guard put it, across the span?

    ⚠️ **The reference has to move with the year.** The guard reads Census's
    family total against published **2017**, so a raw per-year reading folds
    eight years of trade growth *and* composition change into a level test -
    almost every family would look "over" by 2024 and the audit would be noise.
    That is the artefact ``import_resplit_holdout`` found in its arm D.

    So each family's published 2017 base is carried forward on **its own
    published summary row**, which BEA estimates every year:

    ``expected(f, t) = published(f, 2017) x summary(row(f), t) / summary(row(f), 2017)``

    Growth and composition both cancel, because the reference is the family's
    own aggregate rather than the economy's. At 2017 it reduces to exactly the
    number the guard read, so the audit and the guard cannot disagree at the
    anchor. What survives is a family moving against its own summary row -
    which is what a price swing or a mapping defect looks like.

    Flags two things:

    - ``left`` - a family the guard **admitted** whose level leaves the band in
      a later year. Its mass is re-split on a premise that no longer holds.
    - ``entered`` - a family the guard **excluded** that comes inside the band
      later. Improvement being left on the table.
    """
    from bedrock.utils.taxonomy.mappings.bea_v2017_commodity__bea_v2017_summary import (  # noqa: PLC0415, E501
        load_bea_v2017_commodity_to_bea_v2017_summary,
    )

    summary_of = {
        k: v[0] for k, v in load_bea_v2017_commodity_to_bea_v2017_summary().items()
    }
    frame = leaf_rows()
    activities: dict[str, list[str]] = {}
    for activity, family in activity_families(frame).items():
        activities.setdefault(family, []).append(activity)

    mcif = published_mcif()
    admitted = set(
        pd.read_csv(CROSSWALK, dtype=str)
        .fillna('')
        .pipe(lambda f: f.loc[f['Note'] == MARKER, 'Activity'])
    )

    # Each family's 2017 published base, and how that base is spread across the
    # summary rows whose annual movement will carry it forward.
    base, spread = {}, {}
    for family in activities:
        leaves = {
            c: float(mcif.get(c, 0.0))
            for c in mcif.index
            if family_of(c) == family and float(mcif.get(c, 0.0)) > 0
        }
        if not leaves:
            continue
        base[family] = sum(leaves.values())
        rows: dict[str, float] = {}
        for commodity, value in leaves.items():
            row = summary_of.get(commodity)
            if row:
                rows[row] = rows.get(row, 0.0) + value
        spread[family] = rows

    summary = {year: _summary_mcif(year) for year in AUDIT_YEARS}
    levels = {}
    for year in AUDIT_YEARS:
        census = _census_by_family(year, activities)
        expected = {}
        for family, rows in spread.items():
            total = sum(rows.values())
            if not total:
                continue
            carry = sum(
                weight
                * (
                    float(summary[year].get(row, 0.0))
                    / float(summary[ANCHOR].get(row, 0.0))
                    if float(summary[ANCHOR].get(row, 0.0))
                    else 1.0
                )
                for row, weight in rows.items()
            )
            if carry > 0:
                expected[family] = carry
        levels[year] = pd.Series(
            {f: float(census.get(f, 0.0)) / e for f, e in expected.items() if e > 0}
        )
    table = pd.DataFrame(levels).dropna()

    outside = (table - 1.0).abs() > LEVEL_BAND
    rows_out = []
    for family in table.index:
        is_admitted = family in admitted
        breaches = [int(y) for y in AUDIT_YEARS if bool(outside.loc[family, y])]
        inside = [int(y) for y in AUDIT_YEARS if not bool(outside.loc[family, y])]
        if is_admitted and breaches:
            verdict, years = 'left', breaches
        elif not is_admitted and inside:
            verdict, years = 'entered', inside
        else:
            continue
        rows_out.append(
            {
                'family': family,
                'verdict': verdict,
                'level_2017': float(table.loc[family, ANCHOR]),
                'worst': float(
                    table.loc[family].iloc[
                        (table.loc[family] - 1.0).abs().to_numpy().argmax()
                    ]
                ),
                'years': ' '.join(str(y) for y in years),
            }
        )
    return (
        pd.DataFrame(rows_out).set_index('family').sort_values('verdict')
        if rows_out
        else pd.DataFrame()
    )


def audit() -> int:
    """Print the audit; non-zero if an admitted family left the band."""
    table = band_audit()
    print()
    print('Family level across the span, economy-wide trade growth removed')
    print(f'(band is |level - 1| <= {LEVEL_BAND})')
    print()
    if table.empty:
        print('OK   every family sits where the guard put it, all years')
        return 0
    print(table.round(2).to_string())
    left = table[table['verdict'] == 'left']
    print()
    if left.empty:
        print(
            f'OK   no admitted family leaves the band; '
            f'{len(table)} excluded families come inside it in some year'
        )
        return 0
    print(f'FLAGGED: {len(left)} admitted families leave the band')
    print('Their mass is re-split on a premise that does not hold in those years.')
    return 1


def main() -> int:
    import argparse  # noqa: PLC0415

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--audit',
        action='store_true',
        help='re-read the guard decision on every year, growth removed',
    )
    if parser.parse_args().audit:
        return audit()

    existing = leaf_rows()
    parents = build()
    out = pd.concat([existing, parents], ignore_index=True)
    out.to_csv(CROSSWALK, index=False)
    families = parents['Activity'].nunique()
    print(f'{len(parents)} parent rows across {families} families -> {CROSSWALK}')
    print(parents.groupby('Activity').size().to_string())
    return 0


if __name__ == '__main__':
    sys.exit(main())
