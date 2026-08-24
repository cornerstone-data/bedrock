"""What does the 2022 materials census buy Step 3?

Step 3 seeds the Use table's intermediate block from the 2017 benchmark and
carries it forward (#497).  For manufacturing, the part of that column the
annual surveys cannot refresh is the materials bill: AIES publishes all
"materials, parts and supplies (not for resale)" as **one cell**, 82.5% of the
column, so only 8.3% of manufacturing's intermediate is commodity-mappable
annually (#564).

The commodity breakout is quinquennial Economic Census, and **2022 is a second
observation of it** between the benchmark and the end of the nowcast span.  This
module measures whether that second observation is worth having, by asking three
questions in order:

1. ``--coverage`` -- **how much of the materials bill can be placed on a BEA
   commodity at all?**  A source that is mostly residual buckets cannot inform a
   commodity mix no matter how often it is published.
2. ``--movement`` -- **how much did the materials mix actually move, 2017 to
   2022?**  If it barely moved, a frozen 2017 structure was already right and
   the second observation buys nothing.  Scored with the same index of
   dissimilarity :mod:`~.intermediate_structure_drift` uses, so the two numbers
   are directly comparable.
3. ``--where`` -- **which industries moved**, so the effort has a target list.

The bridge to BEA
-----------------

``MATFUEL`` codes are 8-digit and NAICS-derived -- ``33110090`` iron and steel
ingot, ``33272203`` bolts and nuts -- so the longest NAICS prefix that appears
in ``NAICS_to_BEA_Crosswalk_2017.csv`` resolves the material.  The
seller-not-maker problem that made the trade concordance hard does not arise
here, because a material is defined by what it *is*.

Three tiers come out of that, and the distinction is the whole story:

``direct``
    the prefix reaches a NAICS that maps 1:1 onto one BEA detail commodity.
``group``
    the prefix only reaches a NAICS covering several BEA detail commodities
    (``322`` paper, for instance).  Placeable, but needs a within-group split --
    2017 Use shares are the obvious source, and that is a much lighter benchmark
    dependency than freezing the whole column.
``residual``
    :data:`~bedrock.extract.census.Census_EC.MATFUEL_RESIDUAL_CODES` -- real
    spend Census could not place.  The ceiling on this source.

⚠️ **``00772000`` "Total Materials" is the industry total and is excluded
everywhere here.**  The named codes sum to it exactly, so leaving it in doubles
the table.  See :func:`~bedrock.extract.census.Census_EC.census_EC_MatFuel_parse`.

⚠️ **The vintages sit on different NAICS bases** (``NAICS2017`` / ``NAICS2022``)
and share 345 industries and 291 materials.  ``--movement`` scores only the
shared frame and reports what that frame covers, rather than reading an absent
code as a fall to zero.

⚠️ **Suppressed cells are zero here.**  Census withholds 9.1% of 2017 cells and
7.5% of 2022 ones; the FBA records the flag and zeroes the value.  Recovery
against the ``00772000`` control is not built yet, so every share below is a
share of *published* cost.

Run::

    uv run python -m bedrock.analysis.nowcasting.materials_structure
    uv run python -m bedrock.analysis.nowcasting.materials_structure --all
"""

from __future__ import annotations

import argparse
import functools

import numpy as np
import pandas as pd

from bedrock.extract.census.Census_EC import (
    MATFUEL_RESIDUAL_CODES,
    MATFUEL_TOTAL_CODES,
)
from bedrock.extract.flowbyactivity import getFlowByActivity

#: Same file :mod:`~.pxi_mix_test` reads, and by the same repo-relative path.
NAICS_TO_BEA = 'bedrock/utils/mapping/naics/NAICS_to_BEA_Crosswalk_2017.csv'

#: The two Economic Census vintages either side of the nowcast span's midpoint.
VINTAGES = (2017, 2022)

BILLION = 1e9


@functools.cache
def _naics_to_bea() -> tuple[dict[str, str], set[str]]:
    """``NAICS -> BEA detail`` where unambiguous, plus the ambiguous NAICS set.

    A NAICS that covers several BEA detail commodities cannot place a material
    on its own; it is kept separately so a ``group`` tier can be reported rather
    than silently counted as unmapped.
    """
    crosswalk = pd.read_csv(NAICS_TO_BEA, dtype=str).dropna(
        subset=['NAICS_2017_Code', 'BEA_2017_Detail_Code']
    )
    per_naics = crosswalk.groupby('NAICS_2017_Code')['BEA_2017_Detail_Code'].nunique()
    unique = crosswalk[
        crosswalk['NAICS_2017_Code'].isin(per_naics[per_naics == 1].index)
    ]
    return (
        unique.drop_duplicates('NAICS_2017_Code')
        .set_index('NAICS_2017_Code')['BEA_2017_Detail_Code']
        .to_dict(),
        set(per_naics[per_naics > 1].index),
    )


def classify(material: str) -> tuple[str, str | None]:
    """``(tier, bea_detail_code)`` for one 8-digit ``MATFUEL`` code."""
    if material in MATFUEL_RESIDUAL_CODES:
        return 'residual', None
    unique, ambiguous = _naics_to_bea()
    for length in (6, 5, 4, 3, 2):
        prefix = material[:length]
        if prefix in unique:
            return 'direct', unique[prefix]
        if prefix in ambiguous:
            return 'group', None
    return 'unplaced', None


def materials(year: int) -> pd.DataFrame:
    """The ``Census_EC_MatFuel`` FBA for one vintage, totals removed, classified.

    ``ActivityProducedBy`` is the material and ``ActivityConsumedBy`` the
    consuming NAICS-6 industry -- the Use table's own orientation.
    """
    fba = getFlowByActivity('Census_EC_MatFuel', year)
    fba = fba[~fba['ActivityProducedBy'].isin(MATFUEL_TOTAL_CODES)].copy()
    tiers = fba['ActivityProducedBy'].map(classify)
    fba['tier'] = [tier for tier, _ in tiers]
    fba['bea'] = [code for _, code in tiers]
    return fba.rename(
        columns={'ActivityProducedBy': 'material', 'ActivityConsumedBy': 'industry'}
    )


def coverage() -> pd.DataFrame:
    """How much of each vintage's materials bill can be placed on a commodity."""
    records = []
    for year in VINTAGES:
        fba = materials(year)
        total = float(fba['FlowAmount'].sum())
        by_tier = fba.groupby('tier')['FlowAmount'].sum()
        row: dict[str, object] = {'year': year, 'published_$B': total / BILLION}
        for tier in ('direct', 'group', 'residual', 'unplaced'):
            row[f'{tier}_%'] = 100 * float(by_tier.get(tier, 0.0)) / total
        row['placeable_%'] = row['direct_%'] + row['group_%']  # type: ignore[operator]
        row['commodities'] = int(fba.loc[fba['tier'] == 'direct', 'bea'].nunique())
        row['industries'] = int(fba['industry'].nunique())
        row['materials'] = int(fba['material'].nunique())
        row['suppressed_cells'] = int(fba['Suppressed'].notna().sum())
        records.append(row)
    return pd.DataFrame(records).set_index('year')


def _matrix(
    fba: pd.DataFrame, materials_: list[str], industries: list[str]
) -> pd.DataFrame:
    return (
        fba.pivot_table(
            index='material', columns='industry', values='FlowAmount', aggfunc='sum'
        )
        .reindex(index=materials_, columns=industries)
        .fillna(0.0)
    )


def _shares(block: pd.DataFrame) -> pd.DataFrame:
    total = block.sum(axis=0)
    return block.div(total.where(total != 0, np.nan), axis=1).fillna(0.0)


def _shared_frame() -> tuple[pd.DataFrame, pd.DataFrame, pd.Series, dict[str, float]]:
    early, late = materials(VINTAGES[0]), materials(VINTAGES[1])
    industries = sorted(set(early['industry']) & set(late['industry']))
    codes = sorted(set(early['material']) & set(late['material']))
    covered = {
        f'{year}_cost_on_shared_frame_%': 100
        * float(
            frame[frame['industry'].isin(industries) & frame['material'].isin(codes)][
                'FlowAmount'
            ].sum()
        )
        / float(frame['FlowAmount'].sum())
        for year, frame in zip(VINTAGES, (early, late), strict=True)
    }
    a, b = _matrix(early, codes, industries), _matrix(late, codes, industries)
    return a, b, b.sum(axis=0), covered


def movement() -> pd.DataFrame:
    """Index of dissimilarity between the two vintages' materials mixes.

    Same metric as :mod:`~.intermediate_structure_drift`: the share of an
    industry's materials dollars sitting on the wrong material, dollar-weighted
    across industries with the column total given.
    """
    early, late, weights, covered = _shared_frame()
    per_column = (_shares(early) - _shares(late)).abs().sum(axis=0) / 2.0
    score = float((per_column * weights).sum() / weights.sum())
    row: dict[str, object] = {
        'industries': len(weights),
        'materials': early.shape[0],
        'dissimilarity': score,
        'median_column': float(per_column.median()),
        'columns_over_0.10': int((per_column > 0.10).sum()),
        'columns_over_0.25': int((per_column > 0.25).sum()),
        'columns_over_0.50': int((per_column > 0.50).sum()),
    }
    row.update(covered)
    return pd.DataFrame([row]).T.rename(columns={0: f'{VINTAGES[0]} -> {VINTAGES[1]}'})


def where(top: int = 15) -> pd.DataFrame:
    """Which industries moved, by dollars of materials spend reallocated."""
    early, late, weights, _ = _shared_frame()
    per_column = (_shares(early) - _shares(late)).abs().sum(axis=0) / 2.0
    # No industry label here: the FBA's Description column carries the *material*
    # name, and Census publishes the industry title only alongside it.
    table = pd.DataFrame(
        {
            'dissimilarity': per_column,
            f'materials_{VINTAGES[1]}_$B': weights / BILLION,
            'reallocated_$B': per_column * weights / BILLION,
        }
    )
    return table.sort_values('reallocated_$B', ascending=False).head(top)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--coverage', action='store_true', help='what can be placed')
    parser.add_argument('--movement', action='store_true', help='2017 vs 2022 mix')
    parser.add_argument('--where', action='store_true', help='which industries moved')
    parser.add_argument('--all', action='store_true', help='every measurement')
    args = parser.parse_args()
    chosen = args.coverage or args.movement or args.where
    pd.set_option('display.width', 200)

    if args.all or args.coverage or not chosen:
        print('\nHow much of the materials bill can be placed on a BEA commodity')
        print('(shares of published cost; suppressed cells are zero)\n')
        print(coverage().round(1).to_string())
    if args.all or args.movement:
        print('\nHow far the materials mix moved between the two censuses')
        print('(share of an industry of materials dollars on the wrong material)\n')
        print(movement().round(4).to_string())
    if args.all or args.where:
        print('\nWhere the movement sits\n')
        print(where().round(3).to_string())
    print()


if __name__ == '__main__':
    main()
