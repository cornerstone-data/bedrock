"""Moving a services or transportation industry's 2017 Use column on observed expense cells.

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

The measurements
----------------

``--service``
    The whole block -- services and transportation: what the seed moves,
    and whether it helps, under both
    weightings.  ✅ **This is the headline** -- ``--score`` below predates it and
    covers ``ORE`` alone.

``--by-column``
    The block score broken out per summary column.  ⚠️ **Where to look before
    quoting the aggregate**, which a minority of columns carry.

``--agreement``
    ❌ **A rejected filter, kept as a diagnostic.** Whether each industry's own
    input bill agrees with its published column, and why nothing is excluded
    on it.

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

✅ The whole block, not just ``ORE``
--------------------------------------------

⚠️ **The module title is now too narrow.**  #705 built one column; the
re-evaluation in
[`services_transport_expense_resource.py`](services_transport_expense_resource.py) showed the source
reaches **103 BEA detail industries**, and :func:`services_transport_seed` is that seed --
**100 of them**, with utilities held back (:data:`NOT_SEEDED`).  Three things
changed to make it possible:

* ✅ ``Census_AIES_Service_Expenses`` -- a new source on
  ``timeseries/aies/exp02``, which carries the service sectors that
  ``Census_AIES_Expenses``'s ``basic`` endpoint returns as well-formed zeros;
* ✅ :func:`relative_index` now takes an injected ``panel``, so a year can be
  read from a different survey than the base;
* ✅ :func:`industry_growth` -- the denominator now runs over the industry's
  **whole** intermediate bill, not the mapped subset.  See below.

**Scored on BEA's published summary** (:func:`services_transport_score`), against a frozen
2017:

======  =================  =================
year     dollar-weighted    impact-weighted
======  =================  =================
2020          +0.5%             **+10.2%**
2021          +1.4%             **+10.8%**
2022          +2.3%              **+9.2%**
2023          -5.0%               -4.1%
======  =================  =================

⚠️ **Impact here means ``N``, not ``D``** -- total kg CO2e per dollar, direct
plus indirect, per :func:`~.services_transport_expense_resource.total_impact_intensity`.
A Use cell is an entry in ``A``, so an error in it propagates through the whole
Leontief inverse; what a row is worth getting right is its *total* embodied
emissions.  ⚠️ **On ``D`` these read +22.6/+25.1/+22.6%** -- more than twice as
large, and the wrong measure.

✅ **The seed wins under both weightings in every SAS year.**  That is the
re-evaluation's thesis holding up: the survey names purchased electricity and
purchased fuels for every industry, and those are rows the model weights
heavily.

⚠️ **Read the impact column as the result and the dollar column as a sign
check.**  +0.5% to +2.3% is inside the noise §S4 rejected other columns on;
what it establishes is only that the two weightings agree in sign.

⚠️ **The gain is carried by weight, not by count** -- 22, 21 and 17 of 33
columns individually win, and ``ORE`` alone carries **59-83%** of the
aggregate.
✅ **Two columns §S4 rejected on dollars win consistently on impact**:
``622`` (+11.4/+12.3/+27.5%) and ``722`` (+7.5/+11.2/+10.6%).
⚠️ **``5412OP``'s rejection stands** (-9.0/-7.7/-2.0%), and so does ``81``'s
(-0.8/+2.9/+0.6%) -- ⚠️ **``81`` looked like a win on ``D`` and is flat on
``N``**, which is the kind of thing the weighting change decides.
:func:`services_transport_score_by_column` is where this is read.

❌ **``22`` utilities is not seeded** -- :data:`NOT_SEEDED`, decided by Wes on
2026-08-25.  A relative share index cannot carry a price-driven reweighting,
and that is the whole of what utilities' column does over this span.
⚠️ **The column's drift is real and now unaddressed**, not solved: theta is
0.0 across this surge, so ``22`` is close to frozen.

✅ **2023 is built** (:data:`SURVEY_CHANGE_YEARS`) -- the refusal was reversed
on 2026-08-25 once the dispersion was compared against a *rebenchmark* control
rather than a within-vintage one.  ⚠️ The paragraph below is the withdrawn
reasoning, kept because the -5.0%/-4.1% it cites is the discredited key.  It is
AIES read against a SAS 2017 base, so it crosses the survey change on top of the
sas-17/sas-22 rebenchmark: the SAS -> AIES level step is a median \|log\| of
0.203 against 0.118 for a within-instrument year.  ⚠️ **The extractor and the
mapping are still worth having** -- when AIES publishes a second year, 2024
against 2023 is a within-instrument ratio and this becomes usable without the
seam.

✅ **2018 and 2019 build through the cut-list bridge** (#770, 2026-08-30).
Census consolidated the questionnaire for those two collection years --
``sas-19.xlsx``, 23 items against 40 -- and the bridge constrains at the
published aggregates and assigns within them on 2017 proportions
(:data:`CUT_ALL_OTHER_ABSORBED`, :func:`_cut_list_panel`).  ``sas-19`` is
benchmarked to the **2017 Economic Census and restates 2017**, so its ratios
cross no seam -- these two years are in that one respect *cleaner* than
2020-2022, whose 2017 base is sas-17.  What is coarser: an industry's twelve
absorbed items share one ratio, so within-group relative movement is flattened
to 2017 proportions.

✅ What BEA says it used, and the denominator bug that found
------------------------------------------------------------

BEA's account of the 2017 benchmark, for *services, transportation and
warehousing, and utilities*, names the SAS items it took: materials, parts and
supplies (not for resale); purchased electricity; purchased fuels; rental
payments for machinery and equipment; rental payments for buildings and land;
repairs and maintenance to machinery and equipment; repairs and maintenance to
buildings; advertising; printing; data processing; communication services;
water, sewer and refuse; professional and technical services; and **all other
operating expenses**.

✅ **:data:`SAS_ITEM_TO_BEA` covers thirteen of the fourteen.**  ⚠️ **The two it
cannot cover are ``Expensed purchases of other materials, parts, and supplies``
($327B) and ``All other operating expenses`` ($2,005B)** -- neither has a
commodity, and together they are **$2.33T of the panel's $13.1T**.

❌ **They were also missing from the denominator, and that was a bug.**
:func:`relative_index` divides each item's ratio by the industry's own growth,
and that growth was computed over the *mapped* items alone -- silently
asserting that the two unmappable items grew at the mapped average.
:func:`industry_growth` now runs over :func:`intermediate_items`, the whole
input bill.  ⚠️ **It is worth about +0.8pp on impact** (+4.5/+4.7/+5.0 against
+3.6/+3.8/+4.2) and it *costs* the 2020 dollar figure, which is the honest
trade: the denominator is right now, not tuned.

❌ **The scope guard this started as does not survive** -- and the way it failed
is worth keeping.  The idea was to refuse any industry whose own input bill
contradicts BEA's published column.  Scored on the **mapped subset** it named
six industries, ``22`` most of all, and excluding them was worth **5x** on
impact.  ⚠️ **That was measuring the item map, not the survey.**  On the full
bill only ``486`` fails -- and ``486`` is one of the seed's *better* columns.
✅ **A level disagreement does not predict a bad shape**, and nothing is
excluded on it; :func:`contradicting_industries` keeps the negative result
reproducible.

❌ **``22`` utilities is excluded on the mechanism instead** --
:data:`NOT_SEEDED`.  The survey's purchased-fuels line for ``22`` falls
102.5B -> 79.6B while BEA's column rises 160.4B -> 258.4B on a gas price spike
that takes ``211`` from 7.7% to 23.3% of the column.  ⚠️ **BEA used SAS for
utilities too**, so "different universe" is not available as an explanation;
what is left is that a *share* index cannot carry a *price-driven* reweighting,
because dividing out common movement is the one thing it does.  ⚠️ **That
leaves the column's drift unaddressed** -- theta is 0.0 across exactly this
surge -- so this is a refusal to make it worse, not a fix.

Run::

    uv run python -m bedrock.analysis.nowcasting.services_transport_expense_seed --all
    uv run python -m bedrock.analysis.nowcasting.services_transport_expense_seed --service
    uv run python -m bedrock.analysis.nowcasting.services_transport_expense_seed --agreement
"""

from __future__ import annotations

import argparse
import functools

import numpy as np
import pandas as pd

from bedrock.extract.census.Census_SAS_Expenses import (
    SAS_CUT_LIST_YEARS,
    restated_cut_vintage,
)
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
    'Lease and rental payments for machinery, equipment, and other tangible items': (
        '532100',
        '532400',
    ),
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

#: ⚠️ **The 2018-2019 cut list** (Census's change notes, via Wes).  For those
#: two collection years the questionnaire was consolidated: twelve detailed
#: items were absorbed into ``All other operating expenses``, the two expensed
#: items merged, and the four fringe items merged (fringe is compensation, not
#: an intermediate input, so that one never mattered here).
#:
#: The bridge is exactly the directive it implements: **constrain at the
#: published aggregate, assign within it on 2017 proportions.**
#: :func:`_cut_list_panel` synthesises a panel in which each absorbed member is
#: priced at its own sas-17 2017 value times the aggregate's within-``sas-19``
#: ratio, so :func:`relative_index` and :func:`industry_growth` run unchanged:
#: every member of a group carries the group's growth, members' 2017 dollars do
#: the assignment, and the industry's whole-bill denominator compares matched
#: definitions on both sides.
#:
#: ✅ **No benchmark seam.**  ``sas-19`` is benchmarked to the 2017 Economic
#: Census and restates 2017, so the ratio numerator and denominator are one
#: instrument on one basis -- unlike 2020+, whose 2017 base crosses the
#: sas-17/sas-22 rebenchmark.
#:
#: ✅ **The three discontinued items come back to life for exactly these two
#: years.**  Their 2017 bases exist (published through 2017) and they sit
#: inside the absorbed set, so ``517*``, ``221300``/``562000`` and
#: ``532100``/``532400`` receive movement for 2018-2019 that 2020+ can never
#: give them.
#:
#: ⚠️ What is genuinely coarser: all twelve absorbed members of an industry
#: share one ratio, so within-group *relative* movement is flattened to the
#: 2017 proportions for these two years.
CUT_ALL_OTHER = 'All other operating expenses'
CUT_ALL_OTHER_ABSORBED: tuple[str, ...] = (
    'Data processing and other purchased computer services',
    'Purchased communication services',
    'Purchased repairs and maintenance to machinery and equipment',
    'Purchased repairs and maintenance to buildings, structures, and offices',
    'Purchased electricity',
    'Purchased fuels (except motor fuels)',
    'Water, sewer, refuse removal, and other utility payments',
    'Lease and rental payments for machinery, equipment, and other tangible items',
    'Lease and rental payments for land, buildings, structures, store spaces, '
    'and offices',
    'Purchased professional and technical services',
    'Purchased advertising and promotional services',
    # ⚠️ In the absorbed set but never synthesised: taxes are NOT_INTERMEDIATE.
    # Removing a fixed 2017 share from both years of the aggregate would leave
    # the ratio unchanged, so nothing further is needed.
    'Governmental taxes and license fees',
)
CUT_EXPENSED = 'Expensed equipment, materials, parts, and supplies'
CUT_EXPENSED_MEMBERS: tuple[str, ...] = (
    'Expensed equipment',
    'Expensed purchases of other materials, parts, and supplies',
)

#: ⚠️ **Published items that are not intermediate inputs** -- labour, capital
#: consumption, taxes, interest and transfers -- plus the published total
#: itself.  Everything else the survey publishes *is* an intermediate input,
#: whether or not :data:`SAS_ITEM_TO_BEA` can place it on a commodity.
#:
#: This is the distinction :func:`industry_growth` needs and
#: :data:`SAS_ITEM_TO_BEA` cannot supply.  BEA's own account of the benchmark
#: names fourteen items for these industries, and two of them are **not
#: mappable to a commodity**: ``Expensed purchases of other materials, parts,
#: and supplies`` ($327B in 2017) and ``All other operating expenses``
#: ($2,005B, the largest intermediate item in the panel).  ⚠️ **Together they
#: are $2.33T of the panel's $13.1T**, so an industry-growth figure that omits
#: them is not the industry's growth.
NOT_INTERMEDIATE = frozenset(
    {
        SAS_TOTAL_ITEM,
        'Gross annual payroll',
        'Depreciation and amortization charges',
        'Payroll taxes, employer paid insurance premiums (except health), '
        'and other employer benefits',
        'Health insurance',
        'Defined contribution plans',
        'Defined benefit pension plans',
        "Employer's cost for fringe benefits",
        'Governmental taxes and license fees',
        'Operating interest expense',
        'Contributions, gifts, and grants paid',
    }
)

BILLION = 1e9
MILLION = 1e6
THOUSAND = 1e3


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


def usable_items(
    naics: str,
    year: int,
    base_year: int = 2017,
    panel: pd.DataFrame | None = None,
) -> list[str]:
    """Items published for both years, with a positive base.  Order is stable.

    ``panel`` lets a caller inject a wider observation set than
    :func:`expense_panel` -- the AIES service years do that.
    """
    panel = expense_panel() if panel is None else panel
    industry = panel[panel['naics'] == naics]
    wide = industry.pivot_table(index='item', columns='year', values='value')
    if base_year not in wide.columns or year not in wide.columns:
        return []
    usable = wide[base_year].notna() & wide[year].notna() & (wide[base_year] > 0)
    return [item for item in wide.index[usable] if item in SAS_ITEM_TO_BEA]


def intermediate_items(
    naics: str,
    year: int,
    base_year: int = 2017,
    panel: pd.DataFrame | None = None,
) -> list[str]:
    """Every published item that is an intermediate input, mapped or not.

    :func:`usable_items` is the subset that can be placed on a commodity; this
    is the whole input bill.  ⚠️ **The difference is not small** -- ``All other
    operating expenses`` alone is $2,005B of the panel's 2017 total and grows
    45% to 2022, and it is unmappable by construction.

    Used for the denominator of :func:`relative_index`, never its numerator: an
    item with no commodity cannot move a row, but it does belong in the
    industry's own growth.
    """
    panel = expense_panel() if panel is None else panel
    industry = panel[panel['naics'] == naics]
    wide = industry.pivot_table(index='item', columns='year', values='value')
    if base_year not in wide.columns or year not in wide.columns:
        return []
    usable = wide[base_year].notna() & wide[year].notna() & (wide[base_year] > 0)
    return [item for item in wide.index[usable] if item not in NOT_INTERMEDIATE]


def industry_growth(
    naics: str,
    year: int,
    base_year: int = 2017,
    panel: pd.DataFrame | None = None,
) -> float:
    """The industry's own growth in its whole intermediate bill.

    ⚠️ **This is the normaliser :func:`relative_index` divides by, and using the
    mapped subset for it was a bug.** BEA's account of the 2017 benchmark names
    fourteen SAS items for these industries and two of them are unmappable, so
    a mapped-only denominator answers "how did the items I can place move",
    which is not "how did this industry's input bill move".

    ⚠️ **Falls back to the mapped subset** when the survey publishes nothing
    else for the industry, which is what the AIES service years look like.
    """
    panel = expense_panel() if panel is None else panel
    items = intermediate_items(naics, year, base_year, panel=panel)
    if not items:
        items = usable_items(naics, year, base_year, panel=panel)
    if not items:
        return 1.0
    wide = panel[panel['naics'] == naics].pivot_table(
        index='item', columns='year', values='value'
    )
    base = float(wide.loc[items, base_year].sum())
    return float(wide.loc[items, year].sum()) / base if base else 1.0


def relative_index(
    naics: str,
    year: int,
    base_year: int = 2017,
    drop: str | None = None,
    panel: pd.DataFrame | None = None,
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

    ⚠️ **The numerator and the denominator run over different item sets, and
    that is deliberate.** The numerator is :func:`usable_items` -- an item with
    no commodity cannot move a row.  The denominator is
    :func:`industry_growth`, over the industry's **whole** intermediate bill,
    because that is the growth a row has to beat to gain share.  Using the
    mapped subset for both was a bug: it silently asserted that the two
    unmappable items BEA itself used -- materials and parts, and all other
    operating expenses, $2.33T between them -- grew at the mapped average.
    """
    panel = expense_panel() if panel is None else panel
    items = [i for i in usable_items(naics, year, base_year, panel=panel) if i != drop]
    if not items:
        return pd.Series(dtype=float)

    industry = panel[panel['naics'] == naics]
    wide = industry.pivot_table(index='item', columns='year', values='value')
    base, later = wide.loc[items, base_year], wide.loc[items, year]
    overall = industry_growth(naics, year, base_year, panel=panel)

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

    ✅ **2018 and 2019 build through the cut-list bridge** -- the published
    all-other aggregate constrains the absorbed items as a group, 2017
    proportions assign within it, and the ratio is taken inside ``sas-19``
    (2017-benchmarked, restated 2017) so no seam is crossed.  See
    :data:`CUT_ALL_OTHER_ABSORBED`.  2023 onward is AIES (#707).
    """
    if year not in (*SAS_EXPENSE_YEARS, *SAS_CUT_LIST_YEARS):
        raise ValueError(
            f'{year} is not observed for the SAS expense cells; observed years '
            f'are {sorted((*SAS_EXPENSE_YEARS, *SAS_CUT_LIST_YEARS))}.'
        )
    use = _use_2017_detail()
    index = relative_index(SEED_NAICS, year, drop=drop, panel=_panel_for(year))
    seed = use[[SEED_INDUSTRY]].copy()
    touched = [code for code in index.index if code in seed.index]
    seed.loc[touched, SEED_INDUSTRY] = (
        seed.loc[touched, SEED_INDUSTRY] * index.reindex(touched).to_numpy()
    )
    return seed


# ---------------------------------------------------------------------------
# The whole service block, not just ORE
# ---------------------------------------------------------------------------

#: ``Census_AIES_Service_Expenses`` flow -> the SAS Table 5 item it continues.
#: ⚠️ **AIES is a different survey**, so 2023 indexed against a 2017 SAS base
#: crosses both the sas-17/sas-22 rebenchmark *and* the survey change.  The
#: names are aligned so the two stack; that they stack is not the same as their
#: being one instrument.  See :func:`services_transport_seed`.
AIES_TO_SAS_ITEM = {
    'EXPS_ELEC_VAL': 'Purchased electricity',
    'EXPS_FUEL_VAL': 'Purchased fuels (except motor fuels)',
    'EXPS_FUEL_TRANSP_VAL': 'Purchased fuels for transportation equipment',
    'EXPS_MACH_REP_VAL': (
        'Purchased repairs and maintenance to machinery and equipment'
    ),
    'EXPS_BUILD_REP_VAL': (
        'Purchased repairs and maintenance to buildings, structures, and offices'
    ),
    'EXPS_TRANSP_REP_VAL': (
        'Purchased repairs and maintenance to transportation equipment'
    ),
    'EXPS_RENT_BUILD_VAL': (
        'Lease and rental payments for land, buildings, structures, store spaces, '
        'and offices'
    ),
    'EXPS_ADVERT_VAL': 'Purchased advertising and promotional services',
    'EXPS_PROFTECH_VAL': 'Purchased professional and technical services',
    'EXPS_DATAPROC_VAL': 'Data processing and other purchased computer services',
    'EXPS_TEMPSTAF_VAL': 'Temporary staff and leased employee expense',
    'EXPS_TRANSP_VAL': 'Purchased freight transportation',
    'EXPS_SUPPLY_MED_VAL': 'Medical supplies',
    'EXPS_INS_PREM_VAL': 'Cost of insurance',
    'EXPS_PROFLIAB_VAL': 'Professional liability insurance',
    'EXPS_PRINT_VAL': 'Purchased printing services',
}

#: The AIES source that carries the service sectors.  ⚠️ **Not**
#: ``Census_AIES_Expenses``, which reads ``timeseries/aies/basic`` where every
#: service row is a well-formed zero.
AIES_SERVICE_SOURCE = 'Census_AIES_Service_Expenses'

#: The one year AIES answers for.  2021, 2022 and 2024 return ``204 No Content``.
AIES_OBSERVED_YEARS = (2023,)

#: Every year the service panel observes: SAS full-list, the 2018-2019 cut
#: list, then AIES.
OBSERVED_YEARS = (*SAS_EXPENSE_YEARS, *SAS_CUT_LIST_YEARS, *AIES_OBSERVED_YEARS)


@functools.cache
def _aies_service_panel() -> pd.DataFrame:
    """AIES 2023 service expenses, on SAS Table 5's item names and in $M."""
    frames = []
    for year in AIES_OBSERVED_YEARS:
        fba = getFlowByActivity(AIES_SERVICE_SOURCE, year)
        keep = fba[fba['FlowName'].isin(AIES_TO_SAS_ITEM)].copy()
        frames.append(
            pd.DataFrame(
                {
                    'naics': keep['ActivityConsumedBy'].astype(str),
                    'item': keep['FlowName'].map(AIES_TO_SAS_ITEM).astype(str),
                    'year': int(year),
                    # ⚠️ **AIES publishes Thousand USD and SAS Table 5 publishes
                    # dollars**, so the two panels need different divisors to
                    # meet in $M.  Getting this wrong scales every AIES cell by
                    # 1000 and is invisible in :func:`relative_index`, which
                    # divides it out -- but not to anything that reads a level.
                    'value': keep['FlowAmount'].astype(float) / THOUSAND,
                }
            )
        )
    panel = pd.concat(frames, ignore_index=True)
    # ⚠️ A published zero is an absence of the cell for that sector, not a real
    # zero purchase -- AIES routes transport fuel to its own variable and leaves
    # ``EXPS_FUEL_VAL`` at 0 for transportation.  A zero base makes the index
    # infinite, so zeros are dropped exactly as withheld SAS cells are.
    panel = panel[panel['value'] > 0]
    return pd.DataFrame(panel.groupby(['naics', 'item', 'year'], as_index=False).sum())


#: ⚠️ **Sectors this seed deliberately does not touch**, because
#: :mod:`~.inputs_structure` already seeds them from the Economic Census
#: materials breakout and the manufacturing expense panel.  AIES ``exp02``
#: publishes manufacturing at six digits, so without this the two seeds would
#: both claim the same columns and silently disagree.
SEEDED_ELSEWHERE = ('21', '23', '31', '32', '33')

#: How far an industry's own intermediate total may fall short of its published
#: column's growth before :func:`contradicting_industries` names it.  ⚠️ **This
#: is a diagnostic threshold, not a filter** -- see there for why nothing is
#: excluded on it.
CONTRADICTION_THRESHOLD = 0.70

#: ❌ **Survey industries the seed does not touch at all**, whose BEA columns
#: hold their 2017 shape and are carried by the price step alone.
#:
#: ``22`` utilities, decided by Wes on 2026-08-25.  ⚠️ **This is a judgment
#: about the mechanism, not a rule fitted to the score** -- the score is what
#: raised the question, and :func:`contradicting_industries` records the
#: attempt to turn it into a rule and why that failed.
#:
#: What utilities' column does between 2017 and 2022 is a **price-driven
#: reweighting**: BEA's published ``22`` column rises 160.4B -> 258.4B and
#: ``211`` oil and gas goes from **7.7% to 23.3%** of it on the gas spike,
#: while the survey's purchased-fuels line moves the other way, 102.5B ->
#: 79.6B.  ⚠️ **A relative share index cannot carry that by construction** --
#: it divides out common movement, which is exactly the signal here.  That is
#: the price carry's job (§theta), and the seed was overwriting it with a worse
#: estimate: ``22`` alone contributed **-119%** of the block's aggregate gain
#: at 2020 and **-41%** at 2022, on 12-16% of the impact weight.
#:
#: ⚠️ **This is not a claim that holding 2017 is right for ``22``** -- theta is
#: 0.0 across this very surge, so the column is close to frozen and the drift
#: is real and unaddressed.  It says the survey is the wrong instrument, not
#: that the problem is solved.
NOT_SEEDED = ('22',)


def published_agreement(
    years: tuple[int, ...] = (2020, 2021, 2022),
) -> pd.DataFrame:
    """Survey growth against published column growth, per survey industry.

    For each survey industry, :func:`industry_growth` against BEA's published
    intermediate total for the summary columns that industry serves, over the
    same span.  A ratio of 1.0 means the survey and the published column agree
    about how much the industry's input bill moved.

    ✅ **The median industry sits at 1.10, 1.02 and 1.01**, so the SAS splice
    tracks the published columns well nearly everywhere.

    ⚠️ **This reads a published column *total*, never its row split.** The
    total is already observed for Step 3 -- the step controls every column to
    ``GO - VAPRO`` -- so reading it asserts nothing about the shape the seed
    estimates and the score measures.

    ⚠️ **Run this on :func:`intermediate_items`, never :func:`usable_items`.**
    Scoring the mapped subset instead measures how complete
    :data:`SAS_ITEM_TO_BEA` is and reports it as a fact about the survey --
    six industries "contradict" BEA on the mapped subset and one does on the
    full bill, and ``5122`` swings from 0.57 to 1.36.
    """
    from bedrock.analysis.nowcasting.intermediate_structure_drift import (  # noqa: PLC0415
        summary_intermediate,
    )

    _, industry_map = _summary_maps()
    serves: dict[str, set[str]] = {}
    for bea, naics in _survey_industry_candidates().items():
        summary = industry_map.get(bea)
        if summary is not None:
            serves.setdefault(naics, set()).add(summary)

    records = []
    for year in years:
        base, actual = summary_intermediate(2017), summary_intermediate(year)
        panel = _panel_for(year)
        for naics, summaries in serves.items():
            items = intermediate_items(naics, year, panel=panel)
            columns = [
                c for c in summaries if c in actual.columns and c in base.columns
            ]
            if not items or not columns:
                continue
            survey = industry_growth(naics, year, panel=panel)
            published = float(actual[columns].sum().sum() / base[columns].sum().sum())
            records.append(
                {
                    'naics': naics,
                    'year': year,
                    'survey': survey,
                    'published': published,
                    'ratio': survey / published if published else float('nan'),
                }
            )
    return pd.DataFrame(records).pivot(index='naics', columns='year', values='ratio')


def contradicting_industries(
    threshold: float = CONTRADICTION_THRESHOLD,
) -> tuple[str, ...]:
    """Survey industries failing :func:`published_agreement` in *every* year.

    ❌ **A diagnostic, and a rejected filter.** The idea was to refuse to seed
    an industry whose own input bill contradicts BEA's published column, on the
    reasoning that the two must then be describing different populations.  It
    does not survive:

    * on the full intermediate bill only **``486``** fails in all three years,
      and ``486`` is one of the seed's **better** columns (+54.7% at 2020,
      +32.7% at 2022).  A level disagreement does not predict a bad *shape*;
    * the six-industry list it produced on :func:`usable_items` was an artefact
      of :data:`SAS_ITEM_TO_BEA` being incomplete, not of the survey.

    ⚠️ **Nothing is excluded on this.** It is kept because the negative result
    is worth being able to reproduce -- and because a future extension of the
    item map should re-run it.
    """
    agreement = published_agreement()
    failing = agreement[(agreement < threshold).all(axis=1)]
    return tuple(sorted(failing.index))


@functools.cache
def _bea_to_survey_industry() -> dict[str, str]:
    """BEA detail industry -> the survey industry that describes it.

    Longest NAICS prefix wins, so ``541511`` takes SAS ``5415`` rather than
    ``54``.  ⚠️ **A BEA industry with no prefix match gets no seed at all** and
    holds its 2017 column, which is what "no information about movement" means.

    ⚠️ :data:`SEEDED_ELSEWHERE` and :data:`NOT_SEEDED` are both excluded, for
    different reasons -- see each.
    """
    return {
        bea: naics
        for bea, naics in _survey_industry_candidates().items()
        if naics not in NOT_SEEDED
    }


@functools.cache
def _survey_industry_candidates() -> dict[str, str]:
    """The same mapping, before :data:`NOT_SEEDED` is applied.

    Split out so :func:`published_agreement` can still score an industry the
    seed refuses -- a diagnostic that silently dropped ``22`` would hide the
    evidence the refusal rests on.
    """
    industries = sorted(
        set(expense_panel()['naics']) | set(_aies_service_panel()['naics']),
        key=len,
        reverse=True,
    )
    mapping = {}
    for industry in _use_2017_detail().columns:
        if str(industry)[:2] in SEEDED_ELSEWHERE:
            continue
        match = next((n for n in industries if str(industry).startswith(n)), None)
        if match is not None:
            mapping[str(industry)] = match
    return mapping


def services_transport_industries() -> list[str]:
    """The BEA detail industry columns this seed can move."""
    return sorted(_bea_to_survey_industry())


def sector_coverage(seeded_only: bool = True) -> pd.DataFrame:
    """Which BEA sectors this seed covers, on BEA's own sector taxonomy.

    ⚠️ **The module is named for the two sector groups it reaches, and this is
    the check on that name.**  Naming a block after the survey that sourced it
    -- SAS is the *Service* Annual Survey -- blurs the fact that it also
    publishes utilities and transportation, which is exactly the distinction
    that decided :data:`NOT_SEEDED`.

    ✅ **Seven BEA sectors, all services or transportation**: ``FIRE``
    ($2,422B), ``PROF`` ($1,196B), ``6`` ($1,027B), ``51`` ($753B), ``7``
    ($622B), ``48TW`` ($483B) and ``81`` ($276B).

    ⚠️ **``22`` utilities appears only with** ``seeded_only=False`` -- 3 columns
    and $160B, held at the benchmark rather than seeded.  ⚠️ **``42`` and
    ``44RT`` trade appear in neither**: no wholesale or retail rows exist in
    either source, so trade is out of reach rather than declined.
    """
    from bedrock.utils.taxonomy.bea.v2017_commodity_sector import (  # noqa: PLC0415
        BEA_2017_SECTOR_COMMODITY_CODE_DESC,
    )
    from bedrock.utils.taxonomy.mappings.bea_v2017_sector__bea_v2017_commodity import (  # noqa: PLC0415
        load_bea_v2017_sector_commodity_to_bea_v2017_commodity,
    )

    detail_to_sector = {
        str(detail): sector
        for sector, details in (
            load_bea_v2017_sector_commodity_to_bea_v2017_commodity().items()
        )
        for detail in details
    }
    columns = (
        services_transport_industries()
        if seeded_only
        else sorted(_survey_industry_candidates())
    )
    use = _use_2017_detail()
    frame = pd.DataFrame(
        {
            'sector': [detail_to_sector.get(str(c), '?') for c in columns],
            'dollars': [float(use[c].sum()) for c in columns],
        }
    )
    table = frame.groupby('sector').agg(
        columns=('dollars', 'size'), dollars_M=('dollars', 'sum')
    )
    described = {
        str(code): text for code, text in BEA_2017_SECTOR_COMMODITY_CODE_DESC.items()
    }
    table['description'] = [described.get(str(s), '?') for s in table.index]
    return table.sort_values('dollars_M', ascending=False)


@functools.cache
def _cut_list_panel(year: int) -> pd.DataFrame:
    """The synthetic panel for a cut-list year: aggregates constrained, 2017 assigns.

    Three kinds of rows for *year*, appended to :func:`expense_panel` (which
    supplies the sas-17 2017 bases):

    1. **kept items pass through** from the FBA -- temporary staff, freight,
       transport fuels and repairs, printing, insurance, medical supplies,
       software -- except the two consolidated aggregates themselves, which
       must not pair against 2017 cells with narrower definitions;
    2. **each absorbed member** is synthesised at ``base_2017(sas-17) x r_G``,
       where ``r_G`` is the aggregate's ratio *within* ``sas-19`` (its year
       against its restated 2017) -- one instrument, one benchmark;
    3. the **old all-other** itself is synthesised the same way, so the
       unmappable remainder stays inside :func:`industry_growth`'s denominator
       at a matched definition.

    An aggregate suppressed for a NAICS in either year yields no synthetic rows
    there -- the kept items still move, and rows no item names hold 2017, which
    is what partial information means.  A member whose own 2017 base is
    suppressed is skipped the same way.
    """
    if year not in SAS_CUT_LIST_YEARS:
        raise ValueError(f'{year} is not a cut-list year ({SAS_CUT_LIST_YEARS})')
    fba = getFlowByActivity(SAS_EXPENSE_SOURCE, year)
    published = fba[fba['Suppressed'].isna()]
    kept = pd.DataFrame(
        {
            'naics': published['ActivityConsumedBy'].astype(str),
            'item': published['FlowName'].astype(str),
            'year': int(year),
            'value': published['FlowAmount'].astype(float) / MILLION,
        }
    )
    kept = kept[~kept['item'].isin((CUT_ALL_OTHER, CUT_EXPENSED))]

    restated = restated_cut_vintage()
    base17 = expense_panel().query('year == 2017').set_index(['naics', 'item'])['value']

    def _ratio(naics: str, item: str) -> float | None:
        rows = restated[(restated['naics'] == naics) & (restated['item'] == item)]
        wide = rows.set_index('year')['value']
        if int(year) not in wide.index or 2017 not in wide.index:
            return None
        base = float(wide[2017])
        return float(wide[int(year)]) / base if base > 0 else None

    synthetic: list[dict[str, object]] = []
    for naics in sorted(kept['naics'].unique()):
        groups = [
            (_ratio(naics, CUT_ALL_OTHER), (*CUT_ALL_OTHER_ABSORBED, CUT_ALL_OTHER)),
            (_ratio(naics, CUT_EXPENSED), CUT_EXPENSED_MEMBERS),
        ]
        for ratio, members in groups:
            if ratio is None:
                continue
            for member in members:
                if member in NOT_INTERMEDIATE:
                    continue
                base = base17.get((naics, member))
                if base is None or not base > 0:
                    continue
                synthetic.append(
                    {
                        'naics': naics,
                        'item': member,
                        'year': int(year),
                        'value': float(base) * ratio,
                    }
                )
    return pd.concat(
        [expense_panel(), kept, pd.DataFrame(synthetic)], ignore_index=True
    )


def _panel_for(year: int) -> pd.DataFrame:
    """The observation panel for a year -- SAS, the cut-list bridge, or AIES."""
    if year in SAS_CUT_LIST_YEARS:
        return _cut_list_panel(year)
    if year in AIES_OBSERVED_YEARS:
        return pd.concat([expense_panel(), _aies_service_panel()], ignore_index=True)
    return expense_panel()


#: ⚠️ **Years that cross the survey change.**  ✅ **They are built, not
#: refused** -- reversed 2026-08-25 (Wes), and the reversal is the point of the
#: constant now.
#:
#: ❌ **The old reasoning was wrong twice.**  It cited -5.0%/-4.1% from
#: :func:`services_transport_score`, which is scored against BEA's carry-forward
#: summary (§The answer key) and is withdrawn; and it read the SAS -> AIES step's
#: dispersion as instrument noise **against the wrong control**.
#:
#: ✅ **A relative index divides out whatever is common to the industry**, so
#: only each item's deviation from its industry's own step reaches the seed.
#: Median \|log\| of that residual:
#:
#: ==========================================  ==========
#: pair                                         residual
#: ==========================================  ==========
#: within vintage, 1yr, quiet (2016->17)          0.035
#: within vintage, 1yr, surge (2021->22)          0.043
#: within vintage, **4yr**, no seam (2013->17)    0.108
#: **same instrument, CROSSES the EC rebenchmark (2017->20)**  **0.146**
#: SAS -> AIES (2022->23)                         0.164
#: ==========================================  ==========
#:
#: ✅ **0.164 is what crossing a benchmark looks like, not what a broken
#: instrument looks like** -- a known rebenchmark inside the same instrument
#: costs 0.146, so the instrument change itself adds about **0.018**.  The
#: earlier "3.6x a surge year" compared against a within-vintage pair, which is
#: the wrong control for a seam.
#:
#: ✅ **And the rebenchmark is information, not contamination** (Wes): AIES
#: replaced SAS outright at data year **2023**, and a survey launched off the
#: 2022 Economic Census resets levels *and composition* toward what that census
#: revealed.  ❌ Refusing 2023 holds the mix on a 2017-benchmarked structure --
#: the frozen-benchmark problem this step exists to fix, in miniature.
#: ⚠️ **Census documentation has not been read to confirm the 2022-EC benchmark
#: explicitly**; the timing and the dispersion signature are consistent with it.
#:
#: ⚠️ **What is genuinely lost**: the ~0.018 of item-specific movement that is
#: instrument rather than economics, and AIES covers **466 item-industry pairs
#: against 974** in the last SAS year.  Treat a 2023 movement as weaker evidence
#: than a 2020-2022 one -- but weaker is not unusable.
SURVEY_CHANGE_YEARS = AIES_OBSERVED_YEARS


def services_transport_seed(
    year: int, base_year: int = 2017, allow_survey_change: bool = True
) -> pd.DataFrame:
    """The service seed: BEA's 2017 columns moved on the survey's relative index.

    ``commodity x BEA detail industry`` in $M on the benchmark Use axes, for
    every column :func:`services_transport_industries` can reach -- **100 of them**, against
    the one ``ore_seed`` moves.

    The form is :func:`relative_index`'s, unchanged and for the reason
    ``services_transport_expense_resource`` measures: each item's ratio is divided by the
    industry's growth over the same item set, so **only relative movement
    survives** and a rebenchmark that rescales the whole block cancels.  ✅ That
    is also why the sas-17/sas-22 seam costs so little here -- shares are 30-54%
    quieter than levels within a vintage, and crossing the seam adds 3.1%.

    ⚠️ **Rows no item names hold their 2017 value.**  They are neither dropped
    nor zeroed.

    ⚠️ **The column total is held.**  Step 3 owns the level through
    ``GO - VAPRO``; this supplies shape only, so the column is renormalised back
    to its 2017 total after the index is applied.

    ⚠️ **2023 crosses a survey change as well as a rebenchmark.**  It is read
    from AIES against a SAS 2017 base -- the names are aligned
    (:data:`AIES_TO_SAS_ITEM`) but the instrument is not the same one.  Treat a
    2023 movement as weaker evidence than a 2020-2022 one.

    ✅ **2018 and 2019 build through the cut-list bridge** (#770): the
    published ``All other operating expenses`` aggregate constrains its twelve
    absorbed items as a group, their sas-17 2017 dollars assign within it, and
    the group ratio is taken inside ``sas-19`` -- 2017-benchmarked and
    restating 2017, so no seam is crossed.  Coarser than a full-list year: the
    absorbed items share one ratio per industry.  See
    :data:`CUT_ALL_OTHER_ABSORBED` and :func:`_cut_list_panel`.
    """
    if year in SURVEY_CHANGE_YEARS and not allow_survey_change:
        raise ValueError(
            f'{year} is observed by AIES rather than SAS, and indexing it '
            f'against a {base_year} SAS base crosses the survey change. That is '
            f'built by default now -- see SURVEY_CHANGE_YEARS -- because the '
            f'step is a rebenchmark rather than a broken instrument. Pass '
            f'allow_survey_change=True (the default) to build it.'
        )
    if year not in OBSERVED_YEARS:
        raise ValueError(
            f'{year} is not observed for the surveyed expense cells; observed '
            f'years are {sorted(OBSERVED_YEARS)}.'
        )
    use = _use_2017_detail()
    columns = services_transport_industries()
    seed = use[columns].astype(float).copy()
    panel = _panel_for(year)

    for industry in columns:
        naics = _bea_to_survey_industry()[industry]
        index = relative_index(naics, year, base_year=base_year, panel=panel)
        if index.empty:
            continue
        touched = [code for code in index.index if code in seed.index]
        if not touched:
            continue
        seed.loc[touched, industry] = (
            seed.loc[touched, industry] * index.reindex(touched).to_numpy()
        )
    totals, base_totals = seed.sum(axis=0), use[columns].sum(axis=0)
    seed = seed.div(totals.where(totals != 0, np.nan), axis=1).mul(base_totals, axis=1)
    return seed.fillna(0.0)


def services_transport_movement(
    years: tuple[int, ...] = (2020, 2021, 2022, 2023),
) -> pd.DataFrame:
    """What the seed moves, weighted by dollars and by emissions intensity.

    ⚠️ **The two weightings are the point.**  ``dollar_moved`` is what §S4 would
    have measured; ``impact_moved`` weights each commodity row by the shipped
    model's **total** kg CO2e per dollar (``N``, direct plus indirect), which is
    what Cornerstone cares about.  The second is consistently the larger --
    about **1.3x** -- because the rows the survey names are the rows the model
    weights.
    """
    from bedrock.analysis.nowcasting.services_transport_expense_resource import (  # noqa: PLC0415
        impact_intensity,
    )

    use = _use_2017_detail()
    columns = services_transport_industries()
    base = _column_shares(use[columns])
    weights = impact_intensity().reindex(use.index).fillna(0.0)

    records = []
    for year in years:
        seed = services_transport_seed(year, allow_survey_change=True)
        moved = _column_shares(seed) - base
        dollars = use[columns].sum(axis=0)
        impact = (weights.to_numpy()[:, None] * use[columns].to_numpy()).sum(axis=0)
        per_column = moved.abs().sum(axis=0) / 2.0
        impact_moved = (
            (moved.abs() * weights.to_numpy()[:, None]).sum(axis=0)
            / 2.0
            * use[columns].sum(axis=0)
        )
        records.append(
            {
                'year': year,
                'columns_moved': int((per_column > 1e-9).sum()),
                'dollar_moved': float((per_column * dollars).sum() / dollars.sum()),
                'impact_moved': float(impact_moved.sum() / impact.sum()),
                'source': 'AIES' if year in AIES_OBSERVED_YEARS else 'SAS',
            }
        )
    return pd.DataFrame(records).set_index('year')


@functools.cache
def _summary_maps() -> tuple[dict[str, str], dict[str, str]]:
    """BEA detail commodity -> summary, and detail industry -> summary.

    ⚠️ **A detail code with several summary parents takes the first.** The
    mapping is one-to-many only for codes BEA splits across summary rows; taking
    the first is what makes the seeded and published blocks land on the same
    axes, and both sides take it, so it cannot favour one.
    """
    from bedrock.utils.taxonomy.mappings.bea_v2017_commodity__bea_v2017_summary import (  # noqa: PLC0415
        load_bea_v2017_commodity_to_bea_v2017_summary,
    )
    from bedrock.utils.taxonomy.mappings.bea_v2017_industry__bea_v2017_summary import (  # noqa: PLC0415
        load_bea_v2017_industry_to_bea_v2017_summary,
    )

    def first(parents: object) -> str:
        return str(parents[0]) if isinstance(parents, list) else str(parents)

    commodity = {
        str(code): first(parents)
        for code, parents in load_bea_v2017_commodity_to_bea_v2017_summary().items()
    }
    industry = {
        str(code): first(parents)
        for code, parents in load_bea_v2017_industry_to_bea_v2017_summary().items()
    }
    return commodity, industry


def _to_summary(block: pd.DataFrame) -> pd.DataFrame:
    """A detail commodity x industry block aggregated to summary on both axes."""
    commodity, industry = _summary_maps()
    rows = pd.Series({c: commodity.get(str(c)) for c in block.index}).dropna()
    cols = pd.Series({c: industry.get(str(c)) for c in block.columns}).dropna()
    grouped = block.reindex(index=rows.index, columns=cols.index)
    return grouped.groupby(rows).sum().T.groupby(cols).sum().T


@functools.cache
def _summary_intensity() -> pd.Series:
    """kg CO2e per dollar by summary commodity row.

    Aggregated from detail on each row's own 2017 intermediate dollars, so it is
    the dollar-weighted mean intensity of the detail rows the summary row holds
    -- not an unweighted average over codes.
    """
    from bedrock.analysis.nowcasting.services_transport_expense_resource import (  # noqa: PLC0415
        impact_intensity,
    )

    use = _use_2017_detail()
    commodity, _ = _summary_maps()
    intensity = impact_intensity().reindex(use.index).fillna(0.0)
    rows = pd.Series({c: commodity.get(str(c)) for c in use.index}).dropna()
    numerator = (intensity * use.sum(axis=1)).groupby(rows).sum()
    denominator = use.sum(axis=1).groupby(rows).sum()
    return (numerator / denominator.where(denominator != 0)).fillna(0.0)


def _column_scores(year: int, weighting: str) -> pd.DataFrame:
    """Per summary column: frozen and seeded dissimilarity, and the column's weight.

    The scoring :func:`services_transport_score` aggregates, returned per column rather
    than summed, so a named column can be looked up on its own.  ``weight`` is
    what the aggregate weights each column by -- the column's published
    intermediate dollars, times intensity under ``impact``.

    ⚠️ **A column is skipped, not scored zero, when it is missing or empty on
    either side.** 34 of the 103 detail columns survive to summary with a
    published counterpart; the rest share a summary parent with a column
    :data:`SEEDED_ELSEWHERE` covers, or are not published for the year.
    """
    from bedrock.analysis.nowcasting.intermediate_structure_drift import (  # noqa: PLC0415
        summary_intermediate,
    )

    use = _use_2017_detail()
    columns = services_transport_industries()
    frozen_summary = _to_summary(use[columns])
    seeded_summary = _to_summary(
        services_transport_seed(year, allow_survey_change=True)
    )
    base, actual = summary_intermediate(2017), summary_intermediate(year)
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
        weights = (
            intensity.reindex(rows).fillna(0.0)
            if weighting == 'impact'
            else pd.Series(1.0, index=rows)
        )
        truth_share = truth / truth.sum()
        d_frozen = float(
            (weights * (frozen / frozen.sum() - truth_share).abs()).sum() / 2
        )
        d_seeded = float(
            (weights * (seeded / seeded.sum() - truth_share).abs()).sum() / 2
        )
        records.append(
            {
                'column': column,
                'frozen': d_frozen,
                'seeded': d_seeded,
                'gain_%': 100 * (d_frozen - d_seeded) / d_frozen if d_frozen else 0.0,
                'weight': float((weights * truth).sum()),
                'dollars': float(truth.sum()),
            }
        )
    return pd.DataFrame(records).set_index('column')


def services_transport_score_by_column(
    year: int = 2022, weighting: str = 'impact'
) -> pd.DataFrame:
    """:func:`services_transport_score` broken out per summary column, worst gain last.

    ⚠️ **The aggregate gain is not a majority verdict.** The block wins by
    +9.2% on impact at 2022, and only 17 of its 33 columns individually win --
    the gain is carried by the weight, not by the count, and ``ORE`` alone
    carries **59-83%** of it.  §S4's per-column no-goes were scored on dollars
    alone, and this is what re-scores them: ``622`` and ``722`` overturn,
    ``5412OP`` and ``81`` do not.

    ``share_of_gain_%`` apportions the aggregate: a column's contribution to the
    numerator ``sum((frozen - seeded) * weight)`` over that sum.  ⚠️ **It is
    signed and it can exceed 100%** -- losing columns carry negative shares, so
    a single winner's share is not a bound.
    """
    frame = _column_scores(year, weighting)
    contribution = (frame['frozen'] - frame['seeded']) * frame['weight']
    frame = frame.assign(
        share_of_gain_pct=100 * contribution / contribution.sum(),
        weight_share_pct=100 * frame['weight'] / frame['weight'].sum(),
    )
    return frame.sort_values('share_of_gain_pct', ascending=False)


def services_transport_score(
    years: tuple[int, ...] = (2020, 2021, 2022, 2023),
) -> pd.DataFrame:
    """Frozen 2017 against the seeded service block, on BEA's published summary.

    The counterpart of :func:`score` for the whole block rather than ``ORE``,
    and it is reported under **both** weightings because they answer different
    questions -- see ``services_transport_expense_resource``.  ``impact`` weights each
    summary commodity row by the shipped v0.3 model's kg CO2e per dollar,
    aggregated from detail on that row's own intermediate dollars.

    ✅ **The seed wins under both weightings in every SAS year** -- +10.2%,
    +10.8% and +9.2% on impact against +0.5%, +1.4% and +2.3% on dollars.
    That is the thesis of the re-evaluation holding up: the survey names the
    rows the model weights.

    ⚠️ **``impact`` is ``N``**, total kg CO2e per dollar including indirect.
    On ``D`` the same rows read +22.6/+25.1/+22.6%; ``D`` flatters the seed
    because it over-weights electricity, which is nearly all direct.

    ⚠️ **The impact figures are over the columns the seed touches**, utilities
    excluded (:data:`NOT_SEEDED`).

    ❌ **2023 loses on both** (-5.0% / -4.1%), which is why
    :data:`SURVEY_CHANGE_YEARS` exists.

    ⚠️ **The test is biased against the seed**, as :func:`score` explains: BEA
    built the 2017 benchmark structure from the 2017 vintage of this same
    survey and carries it forward, so agreeing with BEA is partly agreeing with
    the seed's own base.  A gain is stronger evidence than its size suggests --
    and the dollar-weighted gains here are small enough that the impact-weighted
    ones are the reason to build this, not the headline number.

    ⚠️ **``wins`` is a minority even in the years that win** -- 17 of 33 columns
    at 2022 against a +9.2% block gain.  The aggregate is carried by the weight,
    not by the count; :func:`services_transport_score_by_column` breaks it out per column.
    """
    records = []
    for year in years:
        for weighting in ('dollar', 'impact'):
            frame = _column_scores(year, weighting)
            total_weight = float(frame['weight'].sum())
            total_frozen = float((frame['frozen'] * frame['weight']).sum())
            total_seeded = float((frame['seeded'] * frame['weight']).sum())
            records.append(
                {
                    'year': year,
                    'weighting': weighting,
                    'columns': int(len(frame)),
                    'frozen': total_frozen / total_weight,
                    'seeded': total_seeded / total_weight,
                    'gain_%': 100 * (total_frozen - total_seeded) / total_frozen,
                    'wins': int((frame['seeded'] < frame['frozen']).sum()),
                }
            )
    return pd.DataFrame(records).set_index(['year', 'weighting'])


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
    parser.add_argument(
        '--block',
        action='store_true',
        help='the whole block: what it moves and whether it helps',
    )
    parser.add_argument(
        '--by-column',
        action='store_true',
        help='the block score broken out per summary column',
    )
    parser.add_argument(
        '--sectors',
        action='store_true',
        help='which BEA sectors the seed covers, on BEA sector taxonomy',
    )
    parser.add_argument(
        '--agreement',
        action='store_true',
        help='re-derive the scope guard: survey total vs published column total',
    )
    parser.add_argument('--all', action='store_true', help='every measurement')
    args = parser.parse_args()
    chosen = (
        args.scope
        or args.score
        or args.leave_one_out
        or args.reachable
        or args.block
        or args.by_column
        or args.agreement
        or args.sectors
    )
    pd.set_option('display.width', 200)

    if args.all or args.sectors:
        print('\nWhich BEA sectors this seed covers (BEA 2017 sector taxonomy)\n')
        print(sector_coverage().round(0).to_string())
        seeded = set(services_transport_industries())
        held_cols = [
            c for c in sorted(_survey_industry_candidates()) if c not in seeded
        ]
        held_columns = len(held_cols)
        held_dollars = sum(float(_use_2017_detail()[c].sum()) for c in held_cols) / 1000
        print(
            '\n  all seven are services or transportation, which is what the'
            '\n  module is named for. 22 utilities is held at the benchmark,'
            f'\n  not seeded: {held_columns} columns, ${held_dollars:,.0f}B.'
            '\n  42 and 44RT trade are in neither source at all -- out of'
            '\n  reach, not declined.'
        )
    if args.all or args.agreement:
        print('\nThe scope guard: survey item-set growth / published column growth')
        print('(1.0 = the survey agrees with BEA about this industry)\n')
        agreement = published_agreement()
        worst_first = agreement.loc[agreement.min(axis=1).sort_values().index]
        print(worst_first.round(2).head(12).to_string())
        derived = contradicting_industries()
        print(
            f'\n  median {agreement.median().round(2).to_dict()}'
            f'\n  below {CONTRADICTION_THRESHOLD} in every year: {list(derived)}'
            '\n  nothing is excluded on this: 486 is the only industry that'
            '\n  fails, and it is one of the seed better columns. Scoring the'
            '\n  MAPPED subset instead names six, which measures the item map.'
        )
    if args.all or args.block:
        columns = services_transport_industries()
        print(
            f'\nServices and transportation: {len(columns)} BEA detail columns, '
            f'${_use_2017_detail()[columns].sum().sum() / 1000:,.0f}B\n'
        )
        print('What the seed moves, against a frozen 2017\n')
        print(services_transport_movement().round(4).to_string())
        print(
            '\n  impact_moved is consistently ~1.3x dollar_moved on N: the'
            '\n  survey names the rows the model weights.\n'
        )
        print("Whether it helps, on BEA's published summary Use\n")
        print(services_transport_score().round(4).to_string())
        print(
            '\n  impact here is N (direct + indirect), not D. The SAS years'
            '\n  win under both weightings; on D the impact column would read'
            '\n  +22.6/+25.1/+22.6%, which over-weights electricity. 2023 is'
            '\n  refused: AIES on a SAS base crosses the survey change too.'
        )
    if args.all or args.by_column:
        for year in (2020, 2021, 2022):
            frame = services_transport_score_by_column(year, 'impact')
            wins = int((frame['seeded'] < frame['frozen']).sum())
            print(f'\nPer column, {year}, impact-weighted -- {wins}/{len(frame)} win\n')
            print(frame.round(3).to_string())
        print(
            '\n  the aggregate is carried by weight, not by count: ORE alone'
            '\n  is 59-83% of it. 622 and 722 -- S4 no-goes on dollars -- win'
            '\n  consistently on N. 5412OP and 81 do not; 81 looked like a win'
            '\n  on D, which is what changing the weighting decides.'
        )
    if args.all or args.scope:
        print("\nHow far each SAS cell sits from BEA's own 2017 row, NAICS 531")
        print('(the argument for indexing rather than substituting)\n')
        print(item_scope().round(2).to_string())
    if args.all or args.score or not chosen:
        print(f'\nFrozen 2017 against the SAS-seeded {SEED_SUMMARY_COLUMN} column')
        print("(index of dissimilarity on BEA's current summary Use)\n")
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
