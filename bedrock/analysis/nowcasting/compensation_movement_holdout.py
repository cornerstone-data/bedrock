"""Does QCEW wage growth predict how detail compensation moves inside a group?

Step 2's movement series for 2018-2024 is: hold each detail industry's share of
its group's ``V00100`` at the benchmark, carry it on that industry's QCEW wage
growth, renormalise inside the group, and rescale to the NIPA control.  Until now
that could not be graded.  ``Census_SAS_Expenses``-style out-of-sample scoring
needed two things the repo did not have -- observed detail compensation at two
benchmarks, and a QCEW year to match the earlier one.

Both arrived.  ``SUPPLY-USE_2026-08-24.zip`` carries the detail Use SUT for
**2007, 2012 and 2017** on one 2017 code basis (#704), and ``V00100`` is one of
its rows; and ``BLS_QCEW.yaml`` now declares 2000-2025 (#728), so 2012 exists.

So this module runs the rule
[#704](https://github.com/cornerstone-data/bedrock/issues/704) established for
Step 3 seeds, applied to Step 2: **grade on the observed 2012 -> 2017 benchmark
span, never against BEA's carried-forward 2018-2024.**

The measurement
---------------

Within each BEA summary industry, holding the group total at its observed 2017
value, the share of the group's compensation dollars sitting on the wrong detail
industry::

    d_g = 0.5 * sum_d | s_hat[d] - s[d] |

``s_hat`` from the candidate, ``s`` observed in 2017.  Dollar-weighted across
groups, so a group is worth what it pays.

The verdict
-----------

===================  ==============  ==============  ==============
candidate            misplaced $M    % of scored     vs frozen
===================  ==============  ==============  ==============
``frozen``                  487,348           5.843          --
``qcew``                    517,328           6.202       **+6.2%**
``qcew_covered``            462,044           5.539          -5.2%
``qcew_resolvable``     **438,534**       **5.258**      **-10.0%**
===================  ==============  ==============  ==============

❌ **Applied everywhere, QCEW makes the block worse** -- +6.2% against simply
holding the 2012 shares.  ✅ **Applied where the concordance can resolve it, it
is a clear go: -10.0%.**  The difference is one carve-out, and it is decidable
from the crosswalk before any score is computed.

⚠️ **The damage is one group.**  ``GSLG`` state and local general government goes
from 4,748 misplaced under frozen shares to **71,694** under QCEW -- on its own
more than twice the entire net degradation.  Construction adds -11,446.  Both are
:func:`unresolvable_groups`: QCEW's axis cannot express BEA's split of them.

``frozen``
    2012 shares held.  The null, and what the model does today.
``qcew``
    2012 shares carried on QCEW payroll growth, renormalised.  The plan as
    written.
``qcew_resolvable``
    ✅ **the recommendation.**  The same, except in the five summary industries
    whose detail split the NAICS concordance cannot express, which keep the
    frozen share.  The carve-out is derived from
    :func:`ambiguous_naics`, not fitted to this score.
``qcew_covered``
    ❌ rejected.  Trust QCEW where its payroll covers the group's compensation.
    Scores -5.2% at a 0.75 floor but is **non-monotonic in the floor**
    (``--floors``), so it is not measuring predictive value.  See
    :func:`_covered_groups`.

⚠️ **The group total is given to every candidate.**  This scores the *shape*, not
the level, because the level comes from the NIPA control and is not what QCEW is
being asked for.  Scoring levels would mix the two and flatter whichever
candidate happened to drift the right way.

⚠️ **Single-child groups are excluded from the score.**  20 of the 71 summary
industries have one detail child, so their share is 1.0 under every candidate and
including them would dilute every number toward zero.

⚠️ **Two vintage traps had to be cleared first, and both faked a result.**  The
NAICS-2012-against-NAICS-2017 mismatch (:func:`shared_naics`) put ``541700`` at
**20.9x** growth and made raw QCEW score +92%; the construction and government
concordance ambiguity (:func:`ambiguous_naics`) would have produced an even split
and called it an answer.  Neither is visible in a total.

Run::

    uv run python -m bedrock.analysis.nowcasting.compensation_movement_holdout
    uv run python -m bedrock.analysis.nowcasting.compensation_movement_holdout --check
"""

from __future__ import annotations

import argparse
import functools
import glob
import os
import sys
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd

from bedrock.analysis.nowcasting.intermediate_structure_drift import (
    BENCHMARK_SUT_ARCHIVE,
    LOCAL_USA_SUP_DIR,
)
from bedrock.analysis.nowcasting.value_added_timeseries import _cell, detail_to_summary
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES

#: The two benchmarks the span runs between.  2007 is available in the same
#: archive and gives a second span; it has no QCEW-era caveat but does predate
#: the 2012 NAICS revision, so it is left for a follow-up.
BASE_YEAR = 2012
TARGET_YEAR = 2017

#: The Use SUT row this module grades.
COMPENSATION_ROW = 'V00100'

#: QCEW is a *wage* series and ``V00100`` is total compensation, so only the
#: growth ratio is ever used -- never a level.  This is the same "allocator,
#: never control" rule the 93.8% coverage figure established.
QCEW_SOURCE = 'BLS_QCEW'

#: Where the per-year FBA parquets land.  The user keeps pre-generated ones here
#: because QCEW comes down at county grain and each year is ~9M rows.
QCEW_GLOB = 'bedrock/extract/output_data/{source}_{year}_*.parquet'


@functools.lru_cache(maxsize=4)
def benchmark_detail_row(year: int, row: str = COMPENSATION_ROW) -> pd.Series:
    """One Use SUT row at BEA detail for a benchmark year, in millions of dollars.

    Reads the same local archive
    :func:`~.intermediate_structure_drift.benchmark_detail_intermediate` does,
    and carries the same caveat: it is a local drop with no extractor, and
    promoting it is its own task.
    """
    archive = Path(LOCAL_USA_SUP_DIR) / BENCHMARK_SUT_ARCHIVE
    if not archive.exists():
        raise FileNotFoundError(
            f'{archive} not found.  The 2007/2012/2017 detail SUT panel is a '
            'local drop with no extractor yet.'
        )
    with (
        zipfile.ZipFile(archive) as bundle,
        bundle.open('Use_SUT_Detail.xlsx') as sheet,
    ):
        frame = (
            pd.read_excel(sheet, sheet_name=str(year), skiprows=5, dtype={'Code': str})
            .set_index('Code')
            .fillna(0)
        )
    frame.columns = frame.columns.astype(str)
    series = frame.loc[row]
    assert isinstance(series, pd.Series)
    values = pd.to_numeric(
        series.reindex(list(USA_2017_INDUSTRY_CODES)), errors='coerce'
    )
    return values.fillna(0.0).astype(float)


@functools.lru_cache(maxsize=1)
def _crosswalk() -> pd.DataFrame:
    path = os.path.join(
        'bedrock',
        'utils',
        'mapping',
        'activitytosectormapping',
        'Sector_Crosswalk_BEA_2017_Detail.csv',
    )
    frame = pd.read_csv(path, dtype=str)
    return frame[frame['SectorSourceName'] == 'NAICS_2017_Code']


@functools.lru_cache(maxsize=1)
def ambiguous_naics() -> tuple[str, ...]:
    """NAICS codes the crosswalk puts under more than one BEA detail industry.

    ⚠️ **47 codes, and they are not a data defect -- they are the axis
    disagreeing.**  All of them are construction (``23*``) or government
    (``92*``).  BEA splits construction detail by *type of structure* --
    residential, healthcare, manufacturing structures -- while NAICS splits it by
    *trade*, so a plumbing contractor's payroll belongs to no single BEA
    construction industry.  Government detail splits federal/state/local and
    general/enterprise, which QCEW's ownership flag does not reach either.

    #536's plan doc predicted exactly this and named the danger: the crosswalk
    carries 236 ``23*`` rows, so **a pipeline that maps through it will not fail
    -- it will produce an even split and call it an answer.**  These codes are
    therefore dropped from the payroll rollup rather than divided, and the
    industries they would have reached fall back to their group's own growth,
    which is the frozen share.
    """
    frame = _crosswalk()
    duplicated = frame['Sector'].duplicated(keep=False)
    return tuple(sorted(set(frame.loc[duplicated, 'Sector'])))


@functools.lru_cache(maxsize=1)
def naics_to_detail() -> dict[str, str]:
    """NAICS 2017 six-digit -> BEA 2017 detail industry, unambiguous codes only.

    See :func:`ambiguous_naics` for the 47 that are excluded and why.
    """
    frame = _crosswalk()
    unambiguous = frame[~frame['Sector'].isin(ambiguous_naics())]
    return dict(zip(unambiguous['Sector'], unambiguous['Activity'], strict=True))


@functools.lru_cache(maxsize=4)
def qcew_national_payroll(year: int) -> pd.Series:
    """National QCEW annual payroll by NAICS six-digit, in millions of dollars.

    All ownerships summed.  ⚠️ **Ownership rides on ``FlowName``**
    (``Annual payroll, {Private, Federal/State/Local Government}``), not on a
    separate axis, and it does *not* separate government enterprises from general
    government -- which is why the plan routes government through NIPA instead.

    ⚠️ **Six-digit rows only.**  The table is hierarchical, so summing every
    ``ActivityProducedBy`` double-counts; the 5-, 4-, 3- and 2-digit rows are
    parents of the 6-digit ones.
    """
    paths = sorted(glob.glob(QCEW_GLOB.format(source=QCEW_SOURCE, year=year)))
    if not paths:
        raise FileNotFoundError(
            f'no QCEW FBA for {year} at {QCEW_GLOB.format(source=QCEW_SOURCE, year=year)}. '
            'Copy one in from extract/input_data/BLS_QCEW, or generate it -- '
            'BLS_QCEW.yaml declares 2000-2025 since #728.'
        )
    frame = pd.read_parquet(
        paths[0], columns=['Class', 'Location', 'ActivityProducedBy', 'FlowAmount']
    )
    national = frame[
        (frame['Class'] == 'Money') & (frame['Location'] == '00000')
    ].copy()
    national['naics'] = national['ActivityProducedBy'].astype(str)
    national = national[national['naics'].str.len() == 6]
    return national.groupby('naics')['FlowAmount'].sum() / 1e6


@functools.lru_cache(maxsize=1)
def shared_naics() -> tuple[str, ...]:
    """Six-digit codes QCEW publishes in **both** benchmark years.

    ⚠️ **This is the trap that makes an unfiltered growth ratio worthless.**
    QCEW 2012 is on **NAICS 2012** and the crosswalk is NAICS 2017: 28 codes
    exist in 2012 and not 2017, 20 the other way.  A BEA industry whose payroll
    sat on a retired code in the base year and a new one in the target year shows
    growth that is pure renumbering -- ``541700`` (R&D services) comes out at
    **20.9x** and ``454000`` (electronic shopping) at **5.2x**, because the 2012
    ``541711``/``541712`` and ``454111``-``454113`` splits were renumbered
    wholesale.

    Restricting both years to the intersection is the minimum honest fix.  It is
    the same failure the EIA 176 route was rejected for: **a form change faking a
    movement.**
    """
    base = set(qcew_national_payroll(BASE_YEAR).index)
    target = set(qcew_national_payroll(TARGET_YEAR).index)
    return tuple(sorted(base & target & set(naics_to_detail())))


@functools.lru_cache(maxsize=4)
def qcew_detail_payroll(year: int) -> pd.Series:
    """QCEW payroll by BEA detail industry on the vintage-consistent code set."""
    payroll = qcew_national_payroll(year).reindex(list(shared_naics())).fillna(0.0)
    mapping = naics_to_detail()
    rolled = payroll.groupby(pd.Series({n: mapping[n] for n in payroll.index})).sum()
    return rolled.reindex(list(USA_2017_INDUSTRY_CODES)).fillna(0.0)


def vintage_loss() -> pd.DataFrame:
    """How much payroll the vintage filter drops, per year."""
    rows = []
    for year in (BASE_YEAR, TARGET_YEAR):
        national = qcew_national_payroll(year)
        mapped = national.reindex(
            [n for n in national.index if n in naics_to_detail()]
        ).sum()
        kept = national.reindex(list(shared_naics())).sum()
        rows.append(
            {
                'year': year,
                'all_naics6_$M': float(national.sum()),
                'crosswalked_$M': float(mapped),
                'vintage_consistent_$M': float(kept),
                'pct_kept_of_crosswalked': 100.0 * float(kept) / float(mapped),
            }
        )
    return pd.DataFrame(rows).set_index('year')


def _groups() -> pd.Series:
    parents = detail_to_summary()
    return pd.Series(
        {code: parents[code] for code in USA_2017_INDUSTRY_CODES if code in parents}
    )


def candidates() -> pd.DataFrame:
    """The three candidate 2017 detail compensation vectors, plus the observed one."""
    base = benchmark_detail_row(BASE_YEAR)
    observed = benchmark_detail_row(TARGET_YEAR)
    groups = _groups().reindex(base.index)

    growth = qcew_detail_payroll(TARGET_YEAR) / qcew_detail_payroll(BASE_YEAR).replace(
        0.0, np.nan
    )
    # An industry QCEW does not reach, or reaches in only one of the two years,
    # gets its group's growth rather than a hole - that is the frozen share, not
    # a zero, and a zero would silently delete the industry.
    growth = growth.replace([np.inf, -np.inf], np.nan)

    frame = pd.DataFrame({'base': base, 'observed': observed, 'group': groups})
    frame['growth'] = growth.reindex(frame.index)
    frame['moved'] = frame['base'] * frame['growth'].fillna(1.0)

    target_total = frame.groupby('group')['observed'].transform('sum')

    def rescale(column: str) -> pd.Series:
        total = frame.groupby('group')[column].transform('sum')
        share = frame[column] / total.replace(0.0, np.nan)
        return (share * target_total).fillna(0.0)

    frame['frozen'] = rescale('base')
    frame['qcew'] = rescale('moved')
    frame['qcew_resolvable'] = np.where(
        frame['group'].isin(unresolvable_groups()), frame['frozen'], frame['qcew']
    )
    covered = _covered_groups(frame)
    frame['qcew_covered'] = np.where(
        frame['group'].isin(covered), frame['qcew'], frame['frozen']
    )
    return frame


@functools.lru_cache(maxsize=1)
def unresolvable_groups() -> tuple[str, ...]:
    """Summary industries whose detail split QCEW's axis cannot express.

    ✅ **Derived from the crosswalk, not fitted to the score.**  The 47 ambiguous
    NAICS codes of :func:`ambiguous_naics` reach 18 BEA detail industries, and
    those sit in exactly five summary industries: construction and the four
    government groups.  The rule is therefore *"carve out the groups the
    concordance cannot resolve"*, which is decidable before any holdout is run --
    and it happens to be the carve-out the plan's Phase 4 argued for on
    structural grounds.
    """
    parents = detail_to_summary()
    reached = set(
        _crosswalk().loc[_crosswalk()['Sector'].isin(ambiguous_naics()), 'Activity']
    )
    return tuple(sorted({parents[i] for i in reached if i in parents}))


def _covered_groups(frame: pd.DataFrame, floor: float = 0.75) -> set[str]:
    """Groups where QCEW payroll is at least ``floor`` of benchmark compensation.

    ❌ **Rejected as the selector, and the reason is worth keeping.**  The plan's
    Phase 4 reasoned that QCEW should be trusted where its UI-covered universe
    tracks compensation, so this derives that list from the ratio rather than
    from the sector names.  It scores -5.2% against frozen, which looks like a
    win until the floor is moved: **-9.7% at 0.70, -5.2% at 0.75, -3.5% at 0.80,
    +0.0% at 0.90.**  A selector that is *non-monotonic* in its own threshold is
    not measuring the thing it claims to; between 0.70 and 0.75 it drops five
    groups where QCEW helps most (insurance, other professional services,
    broadcasting) because their compensation includes a lot that no payroll
    series covers.  Coverage and *predictive value* are different quantities.

    Kept as a reported alternative so the comparison stays reproducible.
    """
    payroll = qcew_detail_payroll(BASE_YEAR).reindex(frame.index).fillna(0.0)
    by_group = pd.DataFrame({'group': frame['group'], 'payroll': payroll})
    by_group['comp'] = frame['base']
    ratio = by_group.groupby('group')['payroll'].sum() / by_group.groupby('group')[
        'comp'
    ].sum().replace(0.0, np.nan)
    return set(ratio[ratio >= floor].index)


def coverage_floor_sensitivity() -> pd.DataFrame:
    """The non-monotonicity that disqualifies the coverage ratio as a selector."""
    frame = candidates()
    baseline, _ = _dissimilarity(frame, 'frozen')
    rows = []
    for floor in (0.50, 0.60, 0.70, 0.75, 0.80, 0.85, 0.90):
        covered = _covered_groups(frame, floor)
        trial = frame.assign(
            trial=np.where(frame['group'].isin(covered), frame['qcew'], frame['frozen'])
        )
        misplaced, _ = _dissimilarity(trial, 'trial')
        rows.append(
            {
                'floor': floor,
                'groups_using_qcew': len(covered),
                'vs_frozen_pct': 100.0 * (misplaced - baseline) / baseline,
            }
        )
    return pd.DataFrame(rows).set_index('floor')


def _dissimilarity(frame: pd.DataFrame, column: str) -> tuple[float, float]:
    """Dollar-weighted index of dissimilarity, and the dollars it is over."""
    multi = frame.groupby('group')['observed'].transform('size') > 1
    scored = frame[multi]
    total = scored.groupby('group')['observed'].transform('sum')
    predicted = scored[column] / total.replace(0.0, np.nan)
    actual = scored['observed'] / total.replace(0.0, np.nan)
    misplaced = ((predicted - actual).abs() * total).groupby(scored['group']).sum() / 2
    dollars = float(scored['observed'].sum())
    return float(misplaced.sum()), dollars


def holdout() -> pd.DataFrame:
    """Score every candidate on the observed 2012 -> 2017 span."""
    frame = candidates()
    rows = []
    for column in ('frozen', 'qcew', 'qcew_covered', 'qcew_resolvable'):
        misplaced, dollars = _dissimilarity(frame, column)
        rows.append(
            {
                'candidate': column,
                'misplaced_$M': misplaced,
                'pct_of_scored': 100.0 * misplaced / dollars,
            }
        )
    table = pd.DataFrame(rows).set_index('candidate')
    baseline = _cell(table, 'frozen', 'misplaced_$M')
    table['vs_frozen_pct'] = 100.0 * (table['misplaced_$M'] - baseline) / baseline
    return table


def by_group() -> pd.DataFrame:
    """Where QCEW helps and where it hurts, per summary industry."""
    frame = candidates()
    multi = frame.groupby('group')['observed'].transform('size') > 1
    scored = frame[multi]
    total = scored.groupby('group')['observed'].transform('sum')
    actual = scored['observed'] / total.replace(0.0, np.nan)
    out = {}
    for column in ('frozen', 'qcew'):
        predicted = scored[column] / total.replace(0.0, np.nan)
        out[column] = ((predicted - actual).abs() * total).groupby(
            scored['group']
        ).sum() / 2
    table = pd.DataFrame(out)
    table['group_comp_$M'] = scored.groupby('group')['observed'].sum()
    table['children'] = scored.groupby('group')['observed'].size()
    table['qcew_gain_$M'] = table['frozen'] - table['qcew']
    return table.sort_values('qcew_gain_$M', ascending=False)


def coverage() -> pd.DataFrame:
    """QCEW payroll against benchmark compensation, by summary industry."""
    frame = candidates()
    payroll = qcew_detail_payroll(BASE_YEAR).reindex(frame.index).fillna(0.0)
    table = pd.DataFrame(
        {
            'qcew_payroll_$M': payroll.groupby(frame['group']).sum(),
            'V00100_$M': frame.groupby('group')['base'].sum(),
        }
    )
    table['ratio'] = table['qcew_payroll_$M'] / table['V00100_$M']
    return table.sort_values('ratio')


def check() -> int:
    """Assert the figures this module's report quotes."""
    failures: list[str] = []
    frame = candidates()

    base_total = float(frame['base'].sum())
    observed_total = float(frame['observed'].sum())
    # both are published BEA figures, so these are equalities, not ranges
    if abs(base_total - 8_575_366.0) > 5.0:
        failures.append(f'2012 V00100 total is {base_total:,.0f}M, not 8,575,366M')
    if abs(observed_total - 10_434_981.0) > 5.0:
        failures.append(f'2017 V00100 total is {observed_total:,.0f}M, not 10,434,981M')

    reached = int(frame['growth'].notna().sum())
    if reached != 379:
        failures.append(f'QCEW growth reaches {reached} industries, not 379')

    if list(unresolvable_groups()) != ['23', 'GFE', 'GFGN', 'GSLE', 'GSLG']:
        failures.append(f'carve-out changed: {list(unresolvable_groups())}')

    table = holdout()
    for name, want in (
        ('frozen', 0.0),
        ('qcew', 6.15),
        ('qcew_covered', -5.19),
        ('qcew_resolvable', -10.02),
    ):
        got = _cell(table, name, 'vs_frozen_pct')
        if abs(got - want) > 0.05:
            failures.append(
                f'{name} scores {got:+.2f}% vs frozen, expected {want:+.2f}%'
            )

    floors = coverage_floor_sensitivity()
    tight = _cell(floors, 0.75, 'vs_frozen_pct')
    loose = _cell(floors, 0.70, 'vs_frozen_pct')
    if not loose < tight:
        failures.append(
            'the coverage floor is no longer non-monotonic, so the reason for '
            'rejecting it has changed'
        )

    for failure in failures:
        print(f'FAIL {failure}')
    if not failures:
        print('OK   every figure in the module docstring reproduces')
    return 1 if failures else 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--by-group', action='store_true')
    parser.add_argument('--coverage', action='store_true')
    parser.add_argument('--floors', action='store_true')
    parser.add_argument('--vintage', action='store_true')
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    if args.check:
        sys.exit(check())

    frame = candidates()
    print(
        f'\n{BASE_YEAR} -> {TARGET_YEAR} observed detail {COMPENSATION_ROW}: '
        f'{frame["base"].sum():,.0f}M -> {frame["observed"].sum():,.0f}M'
    )
    print(
        f'QCEW growth reaches {int(frame["growth"].notna().sum())} of '
        f'{len(frame)} detail industries'
    )
    print(
        f'carved out as unresolvable by the concordance: '
        f'{list(unresolvable_groups())}'
    )
    print('\nShare of a group\'s compensation dollars on the wrong detail industry\n')
    print(holdout().round(3).to_string())
    if args.by_group:
        table = by_group()
        print('\nWhere QCEW helps most\n')
        print(table.head(12).round(1).to_string())
        print('\nWhere QCEW hurts most\n')
        print(table.tail(12).round(1).to_string())
    if args.coverage:
        print(f'\nQCEW payroll against {BASE_YEAR} compensation, worst 15 groups\n')
        print(coverage().head(15).round(3).to_string())
    if args.floors:
        print('\nThe coverage ratio is non-monotonic in its own floor, which is')
        print('why it is not the selector\n')
        print(coverage_floor_sensitivity().round(2).to_string())
    if args.vintage:
        print('\nWhat the NAICS-vintage filter drops\n')
        print(vintage_loss().round(1).to_string())
    print()


if __name__ == '__main__':
    main()
