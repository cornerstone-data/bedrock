"""Re-opening §S4's no-go for services -- and re-ranking the plan on impact.

§Sourcing the columns that actually drift tested SAS Table 5 against `5412OP`,
`81`, `722` and `622`, found no gain at any endpoint, and generalised that into
"#564's negative result generalises" for services.  This module re-evaluates
that, and ❌ **it does not survive** -- for four separate, measurable reasons.

⚠️ **The most important one is the weighting.**  Every ranking in the plan --
which columns drift, which sources are worth building, what "a small prize"
means -- is **dollar-weighted**.  Cornerstone is an EEIO model, so the quantity
that matters is **kg CO2e**, and :func:`impact_intensity` reads it directly from
the shipped model: the v0.3 ``B_USA_non_finetuned`` snapshot, characterised to
CO2e.  Re-weighting the same table on that basis inverts the priority order.

⚠️ 1. The dollar ranking and the impact ranking are different rankings
-----------------------------------------------------------------------

:func:`impact_by_row_group`, on the 2017 detail intermediate block:

======================  ===========  ==========  ==========
commodity rows           dollars $B   dollar %    **impact %**
======================  ===========  ==========  ==========
manufacturing 31-33          4,699       31.6%      28.0%
**utilities 22**               350       **2.4%**   **26.1%**
**agriculture 111/112**        413       **2.8%**   **23.3%**
mining 21                      510        3.4%      11.5%
*all other* (services)       8,260       55.6%       **7.2%**
transportation 48-49           624        4.2%       3.8%
======================  ===========  ==========  ==========

❌ **Utilities and agriculture are 5.2% of intermediate dollars and 49.4% of
direct impact.**  ❌ **"All other" -- 55.6% of the dollars, and every column
§S4 agonised over -- is 7.2%.**  ``ORE``, ``42``, ``5412OP``, ``81``, ``23``,
``722``, ``622`` and ``4A0`` are the plan's drifter list; collectively they sit
inside that 7.2%.

:func:`priority_rows` is the ordering to source against.  It is brutally
concentrated: ⚠️ **the top 10 commodity rows carry 65.3% of the block's direct
impact and the top 25 carry 85.1%**, led by ``221100`` electricity at **25.2%
on its own**, ``211000`` oil and gas 8.5%, ``1111B0`` grains 6.8%, ``1121A0``
cattle 6.3%, ``112120`` dairy 4.0%.

✅ **Two consequences for what to build next, and neither is what the plan says.**

* ⚠️ **Agriculture (#577) is not "a small prize".**  The plan set it aside
  because ``111CA`` barely drifts and carries few dollars.  On impact it is
  **23.3% of the block**, ERS FIWS is 89-91% commodity-mappable, annual, runs to
  2025 and is *already wired into bedrock*.  Re-read that verdict.
* ✅ **Purchased electricity in the service columns is the single highest-value
  cell in this step**, because one row is a quarter of the block's impact and
  SAS and AIES both observe it annually, per industry.

⚠️ 2. The AIES finding was an artefact of the wrong endpoint
-------------------------------------------------------------

``Census_AIES_Expenses`` queries :data:`AIES_TOTALS_ENDPOINT` and concluded that
"21, 22, 23 and 51-81 publish **nothing at all**".  That is true of ``basic``,
where every service row returns a **well-formed zero** -- the same trap
``Census_AIES`` already documents for ``TYPOP``.  ❌ **It is false of the
survey.**  :data:`AIES_DETAIL_ENDPOINT` (group ``AIES00EXP02``, *Detailed
Operating Expenses*) publishes all 41 expense variables for **13 service
sectors** in 2023 -- utilities, transportation, information, finance, real
estate, professional, administrative, education, health, arts, accommodation and
food, other services.  Purchased electricity is $50.3B for sector 22 and
purchased professional and technical services $103.6B for sector 54.

⚠️ **2023 is still the only year**; 2021, 2022 and 2024 return ``204 No
Content``.  AIES *extends* the service panel rather than being it.

⚠️ 3. The resource is 97 columns, not one
-------------------------------------------

SAS Table 5 publishes **twelve commodity-mappable items at all 63 service
industries** in both eras -- purchased electricity, purchased fuels, expensed
materials and supplies, building rent, professional and technical services,
advertising, data processing, repairs to buildings, repairs to machinery,
temporary staff, expensed equipment, expensed software -- plus six more at 7 or
14 industries.  :func:`reach` maps those onto **97 BEA detail industries holding
$6,269B, 42.2% of the intermediate block**.  The plan's built service seed
(`services_transport_expense_seed`) uses exactly one of them, ``531ORE``.

✅ **And scored the right way it looks quite different:**

=====================================  ======
dollar-weighted reach (§S4's metric)    41.4%
**impact-weighted reach**               **63.8%**
=====================================  ======

The per-column gaps are larger still: ``481000`` air transport reaches 53.8% of
dollars and **95.6% of impact**; ``721000`` accommodation 25.5% and **81.8%**;
``483000`` water transport 40.1% and **87.9%**.  ⚠️ SAS names few of the dollars
in those columns and nearly all of what matters in them.

⚠️ Those 97 columns hold 22.7% of the block's direct impact, of which SAS names
14.5 points.  **Meaningful, and not dominant** -- the rest is agriculture,
mining and manufacturing, which is exactly why the bullet above says to re-read
#577.

⚠️ 4. The benchmark seam is not the blocker it was taken for
-------------------------------------------------------------

§S4 rejected SAS Table 5 partly because ``sas-17`` is benchmarked to the 2012
Economic Census and ``sas-22`` to the 2017 one, so a ratio across the 2017-2020
gap carries a rebenchmark.  ⚠️ **That is a claim about levels**, and a
rebenchmark that rescales an industry's whole expense block **cancels exactly in
an item's share of that industry's expenses**.

:func:`seam` tests it on **matched span lengths** -- three years inside one
vintage against the three that cross the seam.  ⚠️ Matched lengths are the
point: annualising the gap divides a one-time step by three and biases the test
toward finding nothing.

==================================  =========  =========
median \\|log ratio\\|                 level      share
==================================  =========  =========
3y inside ``sas-17`` (mean of two)     0.1608     0.1320
3y **across the seam**                 0.1657     0.1547
**seam excess**                        **+3.1%**  **+17.3%**
==================================  =========  =========

✅ **The seam adds 3.1% to level movement over an equal span inside a vintage**
-- while also containing 2020, so a pandemic and a rebenchmark together move
these items about as much as three quiet years.  On the two energy items the
seam excess on *shares* is **−7.4%**: crossing it is quieter than not.

✅ **Shares are the stable object regardless** -- within a vintage they are
30-54% quieter than levels -- so a share-based index is right on its own merits
and defeats a proportional rebenchmark for free.

⚠️ 5. Wholesale and retail are NOT in either of these, on the API
-------------------------------------------------------------------

Worth recording because it is the natural next question and the answer is
counter-intuitive.  Checked directly:

* ``timeseries/aies/exp02`` 2023 carries **no NAICS 42, 44 or 45 rows at all** --
  absent, not zero.  Its 16 sectors are 22, 31-33, 48, 49, 51-54, 56, 61, 62,
  71, 72 and 81.
* ``2017/ecnbasic`` and ``2022/ecnbasic`` populate the expense variables
  (``CSTELEC``, ``PCHRPR``, ``PCHPRTE``, ``PCHTT`` ...) for sectors **21, 23 and
  31-33 only**.  42, 44-45, 22 and every service sector return a **well-formed
  zero** at every NAICS level, including the 2-digit rollup.
* No other dataset in the Census API catalogue publishes general operating
  expenses; a title search over every vintage returns only ``aies/exp01``,
  ``aies/exp02`` and three Finance-and-Insurance subject tables.

⚠️ **So the trade counterpart really is the AWTS/ARTS Business Expenses
Supplement** -- which does exist, for 2017 and 2022, and which §S4 rejected on
**suppression** rather than absence (``4A0`` loses every one of its thirteen
items).  That verdict is untouched by anything here.

⚠️ **Caveat on scope of the check.**  This is the *API*.  It does not rule out
a published table on data.census.gov that the API does not expose, nor a later
AIES release adding trade -- AIES is phasing sectors in and 2023 is its first
year.  If trade expense detail is known to exist somewhere else, that is a
different source from the two checked here and worth pointing at directly.

What this establishes, and what it does not
-------------------------------------------

✅ Services are **reopened as a sourcing question**, and the thing to build is
identified: a share-indexed seed over 97 columns whose strongest claim is
``221100``.  ✅ The plan's **priority order needs redoing on impact**, starting
with #577.

❌ It does **not** overturn §S4's per-column scores.  `5412OP`, `81`, `722` and
`622` really did lose at every endpoint on a whole-column dollar-weighted
metric, and nothing here says they win on it.  The claim is that the metric
answered the wrong question and the seam was the wrong reason.

⚠️ **The undifferentiated cell is still undifferentiated.**  ``Expensed
purchases of other materials, parts, and supplies`` is $357.7B at 63 industries
with no commodity attached, which is why `services_transport_expense_seed` omits it.
Splitting it on 2017 Use shares is the move ``place_on_commodities`` makes for
the ``group`` tier and ``group_split_holdout`` already scored that prior -- so it
is testable rather than assumed.  Not built here.

⚠️ **``B`` is a direct-intensity vector, not a total footprint.**  It weights a
row by the emissions of *making* that commodity, which is the right weight for
"where would an error in this row hurt".  It is not the Leontief-inverted
answer, and a row with small direct intensity can still matter upstream.

Run::

    uv run python -m bedrock.analysis.nowcasting.services_transport_expense_resource --all
"""

import argparse
import functools

import numpy as np
import pandas as pd

from bedrock.analysis.nowcasting.inputs_structure import _use_2017_detail
from bedrock.analysis.nowcasting.services_transport_expense_seed import (
    SAS_DISCONTINUED_AFTER_2017,
    SAS_ITEM_TO_BEA,
    SAS_TOTAL_ITEM,
)
from bedrock.extract.flowbyactivity import getFlowByActivity
from bedrock.utils.emissions.characterization import build_ghg_characterization_matrix
from bedrock.utils.snapshots.loader import load_configured_snapshot
from bedrock.utils.snapshots.names import SnapshotName

#: The shipped model's direct GHG intensity, in the v0.3 snapshot.
B_SNAPSHOT: SnapshotName = 'B_USA_non_finetuned'

#: SAS Table 5 years, both vintages.  ⚠️ 2018 and 2019 publish none of the
#: detailed items in either workbook.
SAS_YEARS = (2013, 2014, 2015, 2016, 2017, 2020, 2021, 2022)

#: The vintage the reach measurement is taken against.
REACH_YEAR = 2022

#: ⚠️ ``basic`` -- what ``Census_AIES_Expenses`` queries -- returns a
#: **well-formed zero** for every service row.  ``exp02`` is the detailed table
#: and carries them for real.  Both names are kept so the next reader does not
#: have to repeat the diagnosis.
AIES_TOTALS_ENDPOINT = 'timeseries/aies/basic'
AIES_DETAIL_ENDPOINT = 'timeseries/aies/exp02'


@functools.cache
def impact_intensity() -> pd.Series:
    """kg CO2e per dollar by BEA detail commodity, from the v0.3 ``B`` matrix.

    ⚠️ **This is the weighting the plan's rankings are missing.**  ``B`` is 7
    greenhouse gases x 400 sectors; characterising it to a single CO2e row is
    what makes it a priority ordering.
    """
    matrix = load_configured_snapshot(B_SNAPSHOT)
    intensity = (build_ghg_characterization_matrix(list(matrix.index)) @ matrix).iloc[0]
    intensity.index = intensity.index.astype(str)
    return intensity


def _use() -> pd.DataFrame:
    use = _use_2017_detail()
    use.index = use.index.astype(str)
    return use


def _weights() -> pd.Series:
    use = _use()
    return impact_intensity().reindex(use.index).fillna(0.0)


def priority_rows(top: int = 25) -> pd.DataFrame:
    """Commodity rows ordered by impact mass -- intensity times dollars used.

    ⚠️ **Intensity alone is not the priority.**  ``327310`` cement is the most
    intense row in the table at 4.66 kg/$ and carries 2.2% of the block's
    impact, because it is small; ``221100`` electricity is half as intense and
    carries **25.2%**.  What an error in a row costs is intensity *times* the
    dollars flowing through it, which is what this ranks.
    """
    use, weights = _use(), _weights()
    dollars = use.sum(axis=1)
    impact = weights * dollars
    order = impact.sort_values(ascending=False).head(top)
    table = pd.DataFrame(
        {
            'kg_CO2e_per_$': weights[order.index],
            'intermediate_$B': dollars[order.index] / 1000,
            'impact_share_%': 100 * order / impact.sum(),
            'cumulative_%': 100 * order.cumsum() / impact.sum(),
        }
    )
    return table


def impact_by_row_group() -> pd.DataFrame:
    """Dollars against impact, by broad commodity group -- the re-ranking."""
    use, weights = _use(), _weights()
    dollars = use.sum(axis=1)

    def group(code: str) -> str:
        code = str(code)
        if code[:3] in ('111', '112'):
            return 'agriculture 111/112'
        if code[:2] == '22':
            return 'utilities 22'
        if code[:2] == '21':
            return 'mining 21'
        if code[:2] in ('31', '32', '33'):
            return 'manufacturing 31-33'
        if code[:2] in ('48', '49'):
            return 'transportation 48-49'
        return 'all other (mostly services)'

    frame = (
        pd.DataFrame(
            {
                'dollars': dollars,
                'impact': weights * dollars,
                'group': [group(code) for code in use.index],
            }
        )
        .groupby('group')[['dollars', 'impact']]
        .sum()
    )
    frame['dollars_$B'] = frame['dollars'] / 1000
    frame['dollar_%'] = 100 * frame['dollars'] / frame['dollars'].sum()
    frame['impact_%'] = 100 * frame['impact'] / frame['impact'].sum()
    return frame[['dollars_$B', 'dollar_%', 'impact_%']].sort_values(
        'impact_%', ascending=False
    )


def live_items(year: int = REACH_YEAR) -> dict[str, tuple[str, ...]]:
    """Mapped SAS items still published in the current era."""
    published = set(getFlowByActivity('Census_SAS_Expenses', year).FlowName)
    return {
        item: codes
        for item, codes in SAS_ITEM_TO_BEA.items()
        if item in published and item not in SAS_DISCONTINUED_AFTER_2017
    }


def reach(year: int = REACH_YEAR) -> pd.DataFrame:
    """Per BEA detail industry: what a mapped SAS item touches, both weightings.

    ⚠️ **Read the two reach columns as different questions.**  ``dollar_%`` is
    §S4's -- how much of the column a source names.  ``impact_%`` is
    Cornerstone's.  They disagree by up to 65 percentage points on one column,
    and that disagreement is the finding.
    """
    fba = getFlowByActivity('Census_SAS_Expenses', year)
    fba['ActivityConsumedBy'] = fba.ActivityConsumedBy.astype(str)
    industries = sorted(fba.ActivityConsumedBy.unique(), key=len, reverse=True)
    items = live_items(year)
    covered = {(row.FlowName, row.ActivityConsumedBy) for row in fba.itertuples()}
    use, weights = _use(), _weights()

    records = []
    for industry in use.columns:
        naics = next((n for n in industries if str(industry).startswith(n)), None)
        if naics is None:
            continue
        column = use[industry]
        if column.sum() <= 0:
            continue
        reached = sorted(
            {
                code
                for item, codes in items.items()
                if (item, naics) in covered
                for code in codes
                if code in column.index
            }
        )
        records.append(
            {
                'bea_industry': str(industry),
                'sas': naics,
                'dollars_$M': float(column.sum()),
                'dollars_reached_$M': float(column[reached].sum()) if reached else 0.0,
                'impact': float((weights * column).sum()),
                'impact_reached': (
                    float((weights[reached] * column[reached]).sum())
                    if reached
                    else 0.0
                ),
            }
        )
    table = pd.DataFrame(records).set_index('bea_industry')
    table['dollar_%'] = 100 * table['dollars_reached_$M'] / table['dollars_$M']
    table['impact_%'] = (
        100 * table['impact_reached'] / table['impact'].where(table['impact'] != 0)
    )
    table['gap_pp'] = table['impact_%'] - table['dollar_%']
    return table.sort_values('impact_reached', ascending=False)


def scale(year: int = REACH_YEAR) -> pd.Series:
    """The headline: how much of the block, how much of its impact, both ways."""
    table = reach(year)
    use, weights = _use(), _weights()
    block_impact = float((weights * use.sum(axis=1)).sum())
    return pd.Series(
        {
            'bea_industries_covered': float(len(table)),
            'covered_columns_$B': table['dollars_$M'].sum() / 1000,
            'covered_%_of_block_dollars': (
                100 * table['dollars_$M'].sum() / float(use.sum().sum())
            ),
            'DOLLAR_weighted_reach_%': (
                100 * table['dollars_reached_$M'].sum() / table['dollars_$M'].sum()
            ),
            'IMPACT_weighted_reach_%': (
                100 * table['impact_reached'].sum() / table['impact'].sum()
            ),
            'covered_%_of_block_impact': 100 * table['impact'].sum() / block_impact,
            'named_%_of_block_impact': (
                100 * table['impact_reached'].sum() / block_impact
            ),
        }
    )


#: Matched-length spans.  ⚠️ **Matched lengths are the point** -- annualising the
#: three-year gap divides a one-time rebenchmark step by three and biases the
#: test toward finding no seam.
SEAM_SPANS = (
    ('2013 -> 2016  (3y, inside sas-17)', 2013, 2016),
    ('2014 -> 2017  (3y, inside sas-17)', 2014, 2017),
    ('2017 -> 2020  (3y, ACROSS THE SEAM)', 2017, 2020),
    ('2020 -> 2022  (2y, inside sas-22)', 2020, 2022),
    ('2014 -> 2016  (2y, inside sas-17)', 2014, 2016),
)

#: The two items that carry ``221100`` and ``324110``, the rows the impact
#: ranking cares most about.
ENERGY_ITEMS = (
    'Purchased electricity',
    'Purchased fuels (except motor fuels)',
)


def seam(energy_only: bool = False) -> pd.DataFrame:
    """Does the 2012/2017 benchmark seam show up as a step, on levels or shares?

    A rebenchmark that rescales an industry's whole expense block cancels
    exactly in an item's **share** of that industry's expenses, so the two bases
    answer different questions and both are reported.
    """
    panel = pd.concat(
        [getFlowByActivity('Census_SAS_Expenses', y).assign(Y=y) for y in SAS_YEARS],
        ignore_index=True,
    )
    panel['ActivityConsumedBy'] = panel.ActivityConsumedBy.astype(str)
    wide = panel.pivot_table(
        index=['ActivityConsumedBy', 'FlowName'], columns='Y', values='FlowAmount'
    )
    totals = wide.xs(SAS_TOTAL_ITEM, level='FlowName')
    items = list(ENERGY_ITEMS) if energy_only else list(live_items())

    def move(item: str, first: int, last: int, basis: str) -> pd.Series:
        try:
            block = wide.xs(item, level='FlowName')
        except KeyError:
            return pd.Series(dtype=float)
        if basis == 'share':
            block = block / totals.reindex(block.index)
        if first not in block or last not in block:
            return pd.Series(dtype=float)
        ratio = block[last] / block[first].where(block[first] > 0)
        return ratio.replace([np.inf, -np.inf], np.nan).dropna()

    records = []
    for label, first, last in SEAM_SPANS:
        row: dict[str, object] = {'span': label}
        for basis in ('level', 'share'):
            values = pd.concat([move(i, first, last, basis) for i in items])
            values = values[values > 0]
            row[basis] = float(np.abs(np.log(values)).median())
            row['n'] = len(values)
        records.append(row)
    table = pd.DataFrame(records).set_index('span')
    within = table.loc[[SEAM_SPANS[0][0], SEAM_SPANS[1][0]]]
    across = table.loc[SEAM_SPANS[2][0]]
    table.attrs['seam_excess'] = {
        basis: float(across[basis] / within[basis].mean() - 1)
        for basis in ('level', 'share')
    }
    return table


def main(priority: bool, reach_: bool, seam_: bool, all_: bool) -> None:
    if all_ or not (priority or reach_ or seam_):
        priority = reach_ = seam_ = True
    money = lambda x: f'{x:,.1f}'  # noqa: E731
    if priority:
        print('Dollars against impact, by commodity row group\n')
        print(impact_by_row_group().to_string(float_format=money))
        print(
            '\n  utilities + agriculture: 5.2% of the dollars, 49.4% of the'
            '\n  impact. "All other" -- every column S4 ranked -- is 7.2%.\n'
        )
        print('The priority ordering: rows by impact mass\n')
        table = priority_rows()
        print(table.to_string(float_format=lambda x: f'{x:,.3f}'))
        print(
            f'\n  top 10 rows carry {table["cumulative_%"].iloc[9]:.1f}% of the '
            f"block's direct impact, top 25 carry {table['cumulative_%'].iloc[-1]:.1f}%."
        )
    if reach_:
        print('\n\nWhat SAS Table 5 reaches, scored both ways\n')
        print(scale().to_string(float_format=money))
        print('\nThe columns where the two metrics disagree most\n')
        table = reach()
        show = table.sort_values('gap_pp', ascending=False).head(12)
        print(
            show[['sas', 'dollars_$M', 'dollar_%', 'impact_%', 'gap_pp']].to_string(
                float_format=money
            )
        )
        print('\n  SAS names few of the dollars in these columns and most of what')
        print('  matters in them. That gap is the whole argument.')
    if seam_:
        for label, energy_only in (('all mapped items', False), ('energy items', True)):
            table = seam(energy_only=energy_only)
            print(f'\n\nThe benchmark seam, matched span lengths -- {label}\n')
            print(table.to_string(float_format=lambda x: f'{x:,.4f}'))
            excess = table.attrs['seam_excess']
            print(
                f'  seam excess over a 3y within-vintage span: '
                f'level {excess["level"]:+.1%}, share {excess["share"]:+.1%}'
            )
        print('\n  so the seam is not the disqualifier S4 took it for.')
    print()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--priority', action='store_true', help='the impact ranking and row order'
    )
    parser.add_argument(
        '--reach', dest='reach_', action='store_true', help='what the source touches'
    )
    parser.add_argument(
        '--seam', dest='seam_', action='store_true', help='does the seam show as a step'
    )
    parser.add_argument('--all', dest='all_', action='store_true', help='every measure')
    main(**vars(parser.parse_args()))
