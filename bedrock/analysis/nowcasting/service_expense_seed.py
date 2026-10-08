"""Moving a service industry's 2017 Use column on observed expense cells.

Step 3 (#497) seeds the intermediate block from the 2017 benchmark and carries
it on a price index.  #705 asked what could do better for the columns that
actually drift, tested every candidate, and found one worth building at all:
``ORE`` / ``531ORE`` Other real estate, moved on ``Census_SAS_Expenses`` (SAS
Table 5, NAICS 531).  This module is that seed and the measurements that size it.

⚠️ **The gain is 4.5% at 2022, which is at the bar rather than over it** -- the
inflation carry gets about 4% in a quiet span (§Does #497's inflation step earn
its place).  It is positive at all three scorable endpoints and the test is
biased against it, so it is real; it is not large.  The reason is
:func:`reachable`: ``ORE``'s movement is dominated by rows no SAS item names.

The form is :mod:`~.inputs_structure`'s S3b form, and for the same reason.  For
an expense item ``k`` mapped to BEA commodities ``C``::

    seed[c, j] = Use2017[c, j] * ( survey[j, k, t] / survey[j, k, 2017] ) / g[j, t]

BEA's own level and its own split across ``C`` are preserved and the survey
supplies only *relative* movement, ``g[j, t]`` being the industry's own growth
in the same items.  Two things follow.  A constant scope difference between what
Census asks and what BEA books divides out (:func:`item_scope` measures how large
those are).  And the level is left to Step 5, which owns both margins of this
block -- so this seed changes shape and nothing else, which is all Step 3 can
contribute.

Three measurements
------------------

``--scope``
    How far each SAS expense cell sits from BEA's own 2017 row for the same
    industry.  The argument for indexing rather than substituting.

``--score`` (default)
    Frozen 2017 against the seeded column, scored on BEA's published summary Use
    for 2020, 2021 and 2022.  This is the bar #705 set and the number that
    decides whether the seed earns its place.

``--leave-one-out``
    Which items carry the gain, by dropping each in turn.

``--reachable``
    ⚠️ **Why the gain is only 4%.**  How much of the column's movement sits on
    rows a SAS item names at all, and how much does not.

⚠️ **This scores on one BEA vintage, deliberately.**  ``io_2017``'s summary Use
loader pins the workbook by year -- 2017-2022 from the 2017-2022 release and
2023-2024 from the 1997-2024 one -- so differencing across that join would
measure BEA's revision as well as its drift, and the revision is not small: the
same 2022, read from both workbooks, differs by a dollar-weighted 0.0557 overall
and **0.0976 for ``ORE`` alone**, against a same-basis 2022 drift of 0.0859.
:func:`~.intermediate_structure_drift.summary_intermediate` now reads every year
from the current workbook, so the comparison is like for like; its
``--revision`` flag reproduces the numbers just quoted.

⚠️ **Part of what this seed corrects is a reclassification, not a substitution.**
BEA moved equity REITs out of funds and trusts into the real estate industry and
revised ``ORE`` back to 2019 but not to 2017, and the revision lands on the same
``55`` and ``561`` rows the drift does.  Census's 531 frame contains the same
reclassified population, which is *why* the index tracks it.  Using it moves the
nowcast onto BEA's current basis, which is right -- but it is not evidence about
how a lessor's input mix changed, and nothing here should be quoted as if it
were.

Run::

    uv run python -m bedrock.analysis.nowcasting.service_expense_seed --all
"""

from __future__ import annotations

import argparse
import functools

import numpy as np
import pandas as pd

from bedrock.extract.census.Census_SAS_Expenses import SAS_EXPENSE_UNOBSERVED_YEARS
from bedrock.extract.flowbyactivity import getFlowByActivity

#: The FBA this reads, and the years it publishes the detailed items for.
SAS_EXPENSE_SOURCE = 'Census_SAS_Expenses'
SAS_EXPENSE_YEARS = (2013, 2014, 2015, 2016, 2017, 2020, 2021, 2022)

#: The one column #705 found a source for.  ``ORE`` at BEA summary is
#: ``531ORE`` alone -- owner-occupied and tenant-occupied housing are ``531HSO``
#: and ``531HST``, in the separate ``HS`` column -- so SAS's NAICS 531, which
#: surveys employer firms and imputes no housing, lines up with it.
SEED_NAICS = '531'
SEED_INDUSTRY = '531ORE'
SEED_SUMMARY_COLUMN = 'ORE'

#: SAS Table 5 item -> the BEA detail commodity rows it buys.
#:
#: ⚠️ **Three items are deliberately absent.**  ``Expensed purchases of
#: software`` and ``Expensed equipment`` are operating expense to Census and
#: mostly *investment* to BEA, so BEA's intermediate row is a fraction of the
#: survey cell and moves for different reasons -- the same disagreement
#: :func:`~.inputs_structure.expense_scope` measures at ratios of 8.0 and 4.0 for
#: manufacturing.  ``Expensed purchases of other materials, parts, and supplies``
#: is one undifferentiated cell with no commodity to place it on.
SAS_ITEM_TO_BEA: dict[str, tuple[str, ...]] = {
    'Purchased electricity': ('221100',),
    'Purchased fuels (except motor fuels)': ('221200', '324110'),
    'Purchased fuels for transportation equipment': ('324110',),
    'Purchased freight transportation': (
        '481000',
        '482000',
        '483000',
        '484000',
        '486000',
        '48A000',
        '492000',
        '493000',
    ),
    'Purchased repairs and maintenance to machinery and equipment': (
        '811300',
        '811400',
    ),
    'Purchased repairs and maintenance to transportation equipment': ('811100',),
    # ⚠️ Repair *to buildings* is a construction commodity to BEA, not a repair
    # service -- 230301 and 230302 are maintenance and repair construction.
    'Purchased repairs and maintenance to buildings, structures, and offices': (
        '230301',
        '230302',
    ),
    'Lease and rental payments for land, buildings, structures, store spaces, '
    'and offices': ('531ORE',),
    'Purchased advertising and promotional services': ('541800',),
    'Purchased professional and technical services': (
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
    'Data processing and other purchased computer services': ('518200',),
    'Temporary staff and leased employee expense': ('561300',),
    'Purchased printing services': ('323110',),
    'Cost of insurance': ('524113', '5241XX', '524200'),
    'Professional liability insurance': ('524113', '5241XX', '524200'),
    'Medical supplies': ('339112', '339113'),
    # ⚠️ Discontinued after 2017 -- see SAS_DISCONTINUED_AFTER_2017.
    'Lease and rental payments for machinery, equipment, and other tangible '
    'items': ('532100', '532400'),
    'Purchased communication services': ('517110', '517A00', '517210'),
    'Water, sewer, refuse removal, and other utility payments': (
        '221300',
        '562000',
    ),
}

#: ⚠️ **Published through 2017 and never again.**  SAS dropped these three from
#: the questionnaire when the detailed-expense series restarted at 2020, so they
#: can never span the 2017 base and cannot contribute to a seed for any later
#: year.  They are kept in :data:`SAS_ITEM_TO_BEA` because the 2013-2017 span is
#: still scorable on them, and because a reader comparing item lists across the
#: two vintages needs to find them named rather than silently missing.
SAS_DISCONTINUED_AFTER_2017 = (
    'Lease and rental payments for machinery, equipment, and other tangible items',
    'Purchased communication services',
    'Water, sewer, refuse removal, and other utility payments',
)

#: The industry's own total, used as the denominator that turns a level movement
#: into a relative one.  Not an expense item.
SAS_TOTAL_ITEM = 'Expenses'

BILLION = 1e9
MILLION = 1e6


@functools.cache
def expense_panel() -> pd.DataFrame:
    """``naics x item x year`` SAS expense cells, in $M, from the FBA.

    ⚠️ Withheld cells arrive as zero with a ``Suppressed`` flag.  A zero
    denominator would make an index infinite, so they are dropped here rather
    than carried -- an absent movement is not a movement of zero.
    """
    frames = []
    for year in SAS_EXPENSE_YEARS:
        fba = getFlowByActivity(SAS_EXPENSE_SOURCE, year)
        keep = fba[fba['Suppressed'].isna()].copy()
        frames.append(
            pd.DataFrame(
                {
                    'naics': keep['ActivityConsumedBy'].astype(str),
                    'item': keep['FlowName'].astype(str),
                    'year': int(year),
                    # FBA is USD; this module works in $M like the Use table.
                    'value': keep['FlowAmount'].astype(float) / MILLION,
                }
            )
        )
    panel = pd.concat(frames, ignore_index=True)
    return pd.DataFrame(
        panel.groupby(['naics', 'item', 'year'], as_index=False)['value'].sum()
    )


@functools.cache
def _use_2017_detail() -> pd.DataFrame:
    """2017 benchmark detail Use intermediate block, commodity x industry, $M."""
    from bedrock.analysis.nowcasting.inputs_structure import (  # noqa: PLC0415
        _use_2017_detail as use,
    )

    return use()


def _column_shares(block: pd.DataFrame) -> pd.DataFrame:
    total = block.sum(axis=0)
    return block.div(total.where(total != 0, np.nan), axis=1).fillna(0.0)


def item_scope(naics: str = SEED_NAICS, industry: str = SEED_INDUSTRY) -> pd.DataFrame:
    """How far each survey cell sits from BEA's own 2017 row for that industry.

    **The argument for indexing rather than substituting.**  Both sides are 2017
    and both are the same industry's purchases, so matching definitions would
    give a ratio near one.  They do not, and the disagreements are structural
    rather than noise -- one Census question against several BEA rows, or a
    concept BEA books as investment.  A constant scope factor divides out of
    ``survey(t) / survey(2017)``; substituting the level would import every one
    of them.
    """
    use = _use_2017_detail()
    column = use[industry]
    panel = expense_panel()
    base = panel[(panel['naics'] == naics) & (panel['year'] == 2017)].set_index('item')[
        'value'
    ]

    records = []
    for item, codes in SAS_ITEM_TO_BEA.items():
        present = [code for code in codes if code in column.index]
        bea = float(column.reindex(present).sum())
        survey = float(base.get(item, float('nan')))
        records.append(
            {
                'item': item,
                'BEA_commodities': '+'.join(present),
                'survey_2017_$M': survey,
                'BEA_2017_$M': bea,
                'survey/BEA': survey / bea if bea else float('nan'),
                'discontinued': item in SAS_DISCONTINUED_AFTER_2017,
            }
        )
    return pd.DataFrame(records).set_index('item').sort_values('survey/BEA')


def usable_items(naics: str, year: int, base_year: int = 2017) -> list[str]:
    """Items published for both years, with a positive base.  Order is stable."""
    panel = expense_panel()
    industry = panel[panel['naics'] == naics]
    wide = industry.pivot_table(index='item', columns='year', values='value')
    if base_year not in wide.columns or year not in wide.columns:
        return []
    usable = wide[base_year].notna() & wide[year].notna() & (wide[base_year] > 0)
    return [item for item in wide.index[usable] if item in SAS_ITEM_TO_BEA]


def relative_index(
    naics: str,
    year: int,
    base_year: int = 2017,
    drop: str | None = None,
) -> pd.Series:
    """Per-BEA-commodity index carrying only *relative* movement.

    Each item's ``year / base_year`` ratio is divided by the industry's growth in
    the same set of items, so an item that grew faster than the column rises in
    share and one that grew slower falls, and the column's level is untouched.
    That is the division of labour §The finding sets out: Step 5 imposes both
    margins, so a level the seed asserts is discarded and only the shape is kept.

    ⚠️ Where several items map to the same commodity -- ``Cost of insurance`` and
    ``Professional liability insurance`` both land on ``524*`` -- the indices are
    combined weighted by each item's own base-year dollars, so the bigger
    question dominates the row rather than the two being averaged flat.
    """
    items = [i for i in usable_items(naics, year, base_year) if i != drop]
    if not items:
        return pd.Series(dtype=float)

    panel = expense_panel()
    industry = panel[panel['naics'] == naics]
    wide = industry.pivot_table(index='item', columns='year', values='value')
    base, later = wide.loc[items, base_year], wide.loc[items, year]
    overall = float(later.sum() / base.sum())

    numerator: dict[str, float] = {}
    denominator: dict[str, float] = {}
    for item in items:
        ratio = float(later[item] / base[item]) / overall
        for code in SAS_ITEM_TO_BEA[item]:
            numerator[code] = numerator.get(code, 0.0) + float(base[item]) * ratio
            denominator[code] = denominator.get(code, 0.0) + float(base[item])
    return pd.Series({c: numerator[c] / denominator[c] for c in numerator})


def ore_seed(year: int, drop: str | None = None) -> pd.DataFrame:
    """The seed: BEA's 2017 ``531ORE`` column, moved on the SAS 531 index.

    ``commodity x industry`` in $M, one column, on the same axes as the benchmark
    Use table so it drops into the Step 3 seed beside
    :func:`~.inputs_structure.nonmaterial_seed`.

    ⚠️ **Rows no item maps to hold their 2017 value**, which is what "no
    information about movement" means.  They are not dropped and not zeroed.

    ⚠️ **Raises for an unobserved year** rather than inventing one.  2018 and
    2019 have no detailed items in either SAS vintage, and 2023 onward is AIES
    (#707).
    """
    if year not in SAS_EXPENSE_YEARS:
        gap = ', '.join(str(y) for y in SAS_EXPENSE_UNOBSERVED_YEARS)
        raise ValueError(
            f'{year} is not observed for the SAS expense cells; observed years '
            f'are {list(SAS_EXPENSE_YEARS)} and {gap} publish none of the '
            f'detailed items in either vintage.'
        )
    use = _use_2017_detail()
    index = relative_index(SEED_NAICS, year, drop=drop)
    seed = use[[SEED_INDUSTRY]].copy()
    touched = [code for code in index.index if code in seed.index]
    seed.loc[touched, SEED_INDUSTRY] = (
        seed.loc[touched, SEED_INDUSTRY] * index.reindex(touched).to_numpy()
    )
    return seed


def score(
    years: tuple[int, ...] = (2020, 2021, 2022), drop: str | None = None
) -> pd.DataFrame:
    """Frozen 2017 against the seeded column, on BEA's published summary Use.

    The seed is built at BEA detail, where Step 3's estimand lives, and scored at
    summary, which is the only place a later year is published.  The metric is
    :func:`~.intermediate_structure_drift.dissimilarity` -- the share of the
    column's dollars sitting on the wrong commodity.

    ⚠️ **The test is biased against the seed.**  BEA's later summary Use carries
    the 2017 benchmark structure forward on annual indicators, and BEA built that
    structure from the *2017* vintage of this same survey.  A gain here is
    therefore stronger evidence than its size suggests.
    """
    from bedrock.analysis.nowcasting.intermediate_structure_drift import (  # noqa: PLC0415
        dissimilarity,
        summary_intermediate,
    )
    from bedrock.utils.taxonomy.mappings.bea_v2017_commodity__bea_v2017_summary import (  # noqa: PLC0415
        load_bea_v2017_commodity_to_bea_v2017_summary,
    )

    detail_to_summary = {
        str(code): (parents[0] if isinstance(parents, list) else str(parents))
        for code, parents in load_bea_v2017_commodity_to_bea_v2017_summary().items()
    }

    records = []
    for year in years:
        base = summary_intermediate(2017)[SEED_SUMMARY_COLUMN]
        actual = summary_intermediate(year)[SEED_SUMMARY_COLUMN]
        rows = [r for r in base.index if r in actual.index]
        base, actual = base.loc[rows], actual.loc[rows]

        seeded_detail = ore_seed(year, drop=drop)[SEED_INDUSTRY]
        group = pd.Series(
            {code: detail_to_summary.get(str(code)) for code in seeded_detail.index}
        ).dropna()
        seeded = seeded_detail.reindex(group.index).groupby(group).sum().reindex(rows)
        # A summary row the detail seed does not reach keeps the frozen column's
        # dollars, so the two are compared on the same support.
        seeded = seeded.where(seeded.notna(), base)

        frame = pd.DataFrame({'frozen': base, 'seeded': seeded, 'actual': actual})
        shares = _column_shares(frame)
        weights = pd.Series(1.0, index=['x'])
        frozen_score, _ = dissimilarity(
            shares[['frozen']].rename(columns={'frozen': 'x'}),
            shares[['actual']].rename(columns={'actual': 'x'}),
            weights,
        )
        seeded_score, _ = dissimilarity(
            shares[['seeded']].rename(columns={'seeded': 'x'}),
            shares[['actual']].rename(columns={'actual': 'x'}),
            weights,
        )
        records.append(
            {
                'year': year,
                'items': len(usable_items(SEED_NAICS, year)) - (drop is not None),
                'column_$M': float(actual.sum()),
                'frozen': frozen_score,
                'seeded': seeded_score,
                'gain_%': (
                    (frozen_score - seeded_score) / frozen_score * 100
                    if frozen_score
                    else float('nan')
                ),
            }
        )
    return pd.DataFrame(records).set_index('year')


def leave_one_out(year: int = 2022) -> pd.DataFrame:
    """Which items carry the gain, by dropping each in turn.

    ✅ **No single item carries it**, which is the healthy answer and was not the
    first one: an earlier cut of this measurement, taken by applying each item's
    index to whole *summary* rows, appeared to hang entirely on temporary staff.
    It did, because at summary that item multiplies all of ``561`` -- $97.8B in
    this column, of which $74.0B is ``561700`` services to buildings and
    dwellings, a lessor's janitorial and landscaping bill and nothing to do with
    temporary staff.  Mapped at detail, where it reaches only ``561300``'s
    $8.6B, the same item is worth 2.4 of the 4.5 points and the rest is spread.
    **The lesson is that a coarse commodity mapping does not merely blur a
    result, it inflates it**, because the index gets applied to dollars the
    survey item never described.
    """
    base = float(score((year,))['gain_%'].iloc[0])
    records = [{'without': '(nothing dropped)', 'gain_%': base, 'change_pp': 0.0}]
    for item in usable_items(SEED_NAICS, year):
        gain = float(score((year,), drop=item)['gain_%'].iloc[0])
        records.append({'without': item, 'gain_%': gain, 'change_pp': gain - base})
    return pd.DataFrame(records).set_index('without').sort_values('gain_%')


def reachable(year: int = 2022) -> pd.DataFrame:
    """How much of the column's movement sits on rows the seed can touch.

    **This is why the gain is 4% and not 20%, and it is the more useful result.**
    A seed can only correct movement on rows some survey item names. Splitting
    ``ORE``'s 2017 -> ``year`` share change into the reachable part and the rest
    says how much of the problem this source is even addressed to.

    ⚠️ The answer is that the largest mover is unreachable. ``55`` management of
    companies has no counterpart in the SAS item list at all, and ``561`` is
    reachable only through ``561300`` employment services, $8.6B of a $97.8B
    block whose bulk is ``561700`` services to buildings and dwellings -- a
    lessor's janitorial and landscaping bill, which no item names either.
    """
    from bedrock.analysis.nowcasting.intermediate_structure_drift import (  # noqa: PLC0415
        summary_intermediate,
    )
    from bedrock.utils.taxonomy.mappings.bea_v2017_commodity__bea_v2017_summary import (  # noqa: PLC0415
        load_bea_v2017_commodity_to_bea_v2017_summary,
    )

    detail_to_summary = {
        str(code): (parents[0] if isinstance(parents, list) else str(parents))
        for code, parents in load_bea_v2017_commodity_to_bea_v2017_summary().items()
    }
    index = relative_index(SEED_NAICS, year)
    touched = {detail_to_summary.get(str(code)) for code in index.index}

    base = summary_intermediate(2017)[SEED_SUMMARY_COLUMN]
    actual = summary_intermediate(year)[SEED_SUMMARY_COLUMN]
    rows = [r for r in base.index if r in actual.index]
    move = (actual[rows] / actual[rows].sum() - base[rows] / base[rows].sum()) * 100

    frame = pd.DataFrame(
        {
            'movement_pp': move,
            'share_2017_%': base[rows] / base[rows].sum() * 100,
            'reachable': [r in touched for r in rows],
        }
    )
    return frame.reindex(frame['movement_pp'].abs().sort_values(ascending=False).index)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--scope', action='store_true', help='survey cell against BEA 2017 row'
    )
    parser.add_argument(
        '--score', action='store_true', help='frozen against seeded, on BEA summary'
    )
    parser.add_argument(
        '--leave-one-out', action='store_true', help='which items carry the gain'
    )
    parser.add_argument(
        '--reachable', action='store_true', help='what movement the seed can touch'
    )
    parser.add_argument('--all', action='store_true', help='every measurement')
    args = parser.parse_args()
    chosen = args.scope or args.score or args.leave_one_out or args.reachable
    pd.set_option('display.width', 200)

    if args.all or args.scope:
        print('\nHow far each SAS cell sits from BEA\'s own 2017 row, NAICS 531')
        print('(the argument for indexing rather than substituting)\n')
        print(item_scope().round(2).to_string())
    if args.all or args.score or not chosen:
        print(f'\nFrozen 2017 against the SAS-seeded {SEED_SUMMARY_COLUMN} column')
        print('(index of dissimilarity on BEA\'s current summary Use)\n')
        print(score().round(4).to_string())
    if args.all or args.leave_one_out:
        print('\nWhich items carry the gain, 2022\n')
        print(leave_one_out().round(1).to_string())
    if args.all or args.reachable:
        frame = reachable()
        print(f'\nWhat movement the seed can reach, {SEED_SUMMARY_COLUMN} 2017 -> 2022')
        print('(share change in pp, and whether a SAS item names the row)\n')
        print(frame.head(12).round(2).to_string())
        totals = frame['movement_pp'].abs().groupby(frame['reachable']).sum()
        print(
            f'\n  |movement| on reachable rows   {totals.get(True, 0.0):.2f} pp'
            f'\n  |movement| on unreachable rows {totals.get(False, 0.0):.2f} pp'
        )


if __name__ == '__main__':
    main()
