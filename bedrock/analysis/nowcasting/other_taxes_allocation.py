"""How should ``T00OTOP`` be allocated to the 402 BEA detail industries?

Step 2's third row, and the one the plan had the least to say about: *"no NIPA
table has an industry axis for this, accept a cruder method"*.  That was true
about the industry axis and wrong about the conclusion.  This module measures
the row instead of characterising it -- for 2017 where the detail answer is
published, and for 2017-2024 on the summary tables.

``T00OTOP`` is a property tax, not an income tax
------------------------------------------------

The name invites the wrong intuition.  Corporate income tax is **not** in this
row, and not in taxes on production at all -- it is a distribution of surplus
and lands inside ``V00300``.  NIPA ``T30500`` says what the row is made of, and
in 2017 it is overwhelmingly one thing:

=========================================  ==========  ========
line (state and local, ``LA000365``)          2017 $M     share
=========================================  ==========  ========
Recurrent taxes on immovable property         535,227     88.1%
Other license taxes                            43,855      7.2%
Motor vehicle licenses                         11,423      1.9%
Special assessments                            10,352      1.7%
Other                                           6,471      1.1%
**total**                                   **607,329**
=========================================  ==========  ========

Plus 1,204 of federal (``LA000237``), giving **608,533** against the Use SUT's
``T00OTOP`` of 608,542 -- nine apart, rounding.

⚠️ ``LA000237`` is titled *"Other taxes on goods and services"*, which reads
like a tax on products.  It is not treated as one here: it is the federal
remainder after ``LA000236`` taxes on product, and including it is what makes
the control close.  The arithmetic places it, not the name.

So the allocator wants capital stock, and output is a bad proxy
---------------------------------------------------------------

Scored against the published 2017 detail row, each candidate clipped at zero and
rescaled to the same total, so only the distribution is judged:

==============================  ===========  ==========================
allocator                       correlation  ``sum |est - published|``
==============================  ===========  ==========================
industry output ``T018``              0.590  **92.3%** of the row
value added ``VABAS``                 0.711  88.4%
gross operating surplus               0.927  71.0%
compensation ``V00100``               0.050  120.3%
==============================  ===========  ==========================

Even the best is unusable.  Output-proportional misses ``531HSO`` by
**-150,567** on a published 178,599, because the effective rate is not a rate at
all: ``T00OTOP`` over output runs 0.49% at the 10th percentile to **15.20%** on
``531HST``, a 30x spread around an economy-wide 1.80%.  Gross operating surplus
scores best of the four for the same reason property tax is concentrated --
both are returns to capital -- but 71% error is not a method.

Which is the useful finding, because it is a *concentrated* row
---------------------------------------------------------------

Three real-estate codes carry **46.3%** of it and twenty industries carry 68.1%
(HHI 0.109).  A row shaped like that does not need a good economy-wide
allocator; it needs its big cells looked up.  And BEA publishes them.

✅ **Housing is an exact lookup, and not only in 2017.**  ``T70405``'s
``B1031C``, taxes on production and imports for the housing sector, equals the
``531HSO`` + ``531HST`` pair to the dollar -- and equals the summary SUT's
``HS`` row to the dollar in **six of the eight years 2017-2024**.  That is 41.8%
of the row from one published line.

✅ **Farm is the same.**  ``T70305``'s ``B1017C`` is 9,408 against the ten farm
detail codes' 9,405 in 2017, and matches the summary ``111CA`` row exactly in
**seven of eight years**.

✅ **Government is zero by construction.**  All ten government industry codes
carry exactly zero -- the same accounting rule
:mod:`~bedrock.analysis.nowcasting.tax_axis_conversion` documents for
``T00TOP``: a tax levied by government and remitted by a government producer
nets out.

The remaining ~56% rides frozen 2017 detail shares, and that is *measured*
--------------------------------------------------------------------------

Frozen benchmark shares are normally an admission.  Here they are the finding,
because the composition of this row barely moves.  Measured on the summary Use
SUT, which publishes ``T00OTOP`` by summary industry every year:

=======  =============  ===============  ================================
year     level, $M      growth vs 2017   ``sum |share_y - share_2017|``
=======  =============  ===============  ================================
2017           608,535  --               --
2018           631,491  +3.8%            1.01% of the row
2019           668,074  +9.8%            1.09%
2020           692,004  +13.7%           1.54%
2021           710,925  +16.8%           2.10%
2022           751,582  +23.5%           1.85%
2023           796,170  +30.8%           2.05%
2024           854,999  **+40.5%**       **1.92%**
=======  =============  ===============  ================================

**The level grows 40.5% over seven years and the composition moves 1.9%.**
Against an output-proportional allocator's 92.3% error measured *in* the
benchmark year, frozen shares are not the cruder option -- they are two orders
of magnitude better, and the property-tax composition says why: assessed values
move with the general price level far more than they move relative to each
other.

⚠️ **The summary SUT is evidence here, never an input.**  Step 5's Decision 3
holds the summary SUT out of the target set and keeps it in the test set, so a
method taking its annual ``T00OTOP`` row as a control would consume the very
table meant to grade it.  The control comes from NIPA; the summary SUT only
grades the frozen-share assumption out of sample.  That is exactly the role
Decision 3 gives it.

The resulting method
--------------------

``NIPA_VA_othertax_<year>``, four activity sets:

1. **Housing** -- ``T70405`` ``B1031C`` to ``531HSO``/``531HST``, split on the
   frozen 2017 detail share (70.29% / 29.71%).
2. **Farm** -- ``T70305`` ``B1017C`` across the ten farm detail codes on frozen
   2017 shares.
3. **Everything else** -- ``T30500`` ``LA000365`` + ``LA000237``, less the two
   lookups, on frozen 2017 detail shares of the remaining industries.
4. **Government** -- nothing, and asserted to be nothing.

⚠️ **Two open items, both stated rather than buried.**  The owner/tenant split
of housing property tax has no published source: the frozen 2017 split is 70.29%
owner, where the gross-value-added shares ``B1300C``/``B1301C`` would say 77.07%
-- so GVA is *not* a substitute, and the frozen share is the better of the two.
And the honest allocator for the non-housing, non-farm remainder is capital
stock in structures, which means **BEA Fixed Assets** -- not in bedrock, and the
same missing extractor consumption of fixed capital wants for 40% of ``V00300``
(open question 5).  At 1.8% of output, seed-only in Step 5, and 1.9% composition
drift, it is not what the build is waiting on.

Usage::

    uv run python -m bedrock.analysis.nowcasting.other_taxes_allocation
    uv run python -m bedrock.analysis.nowcasting.other_taxes_allocation --check
"""

from __future__ import annotations

import argparse
import functools
import sys
from typing import Any, cast, get_args

import numpy as np
import pandas as pd

from bedrock.analysis.nowcasting.compare_NIPA_to_IOT import nipa_flat_table
from bedrock.extract.iot.io_2017 import (
    _load_2017_detail_supply_use_usa,
    _load_usa_summary_sut,
)
from bedrock.utils.taxonomy.bea.matrix_mappings import USA_SUMMARY_SUT_YEARS
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES

YEAR = 2017

#: The Use SUT row this module is about.
ROW = 'T00OTOP'

#: The two ``T30500`` lines that sum to it: state and local other taxes on
#: production, and the federal remainder after taxes on product.
CONTROL_LINES = (('T30500', 'LA000365'), ('T30500', 'LA000237'))

#: ``T30500``'s decomposition of the state and local line, which is 99.8% of the
#: row.  Property tax is 88.1% of it, and that is the whole argument for what
#: the allocator should be.
COMPOSITION_LINES = (
    ('LA000355', 'Recurrent taxes on immovable property'),
    ('LA000371', 'Other license taxes'),
    ('LA000356', 'Motor vehicle licenses'),
    ('S23055', 'Special assessments'),
    ('LA000361', 'Other'),
)

#: Housing: table 7.4.5's taxes on production and imports, and the two detail
#: codes it lands on -- ``531HSO`` owner-occupied, ``531HST`` tenant-occupied.
HOUSING_LINE = ('T70405', 'B1031C')
HOUSING_CODES = ('531HSO', '531HST')

#: Housing gross value added, owner and tenant.  Not the split used -- kept
#: because the report contrasts it with the frozen share, and they disagree.
HOUSING_GVA_LINES = (('T70405', 'B1300C'), ('T70405', 'B1301C'))

#: Farm: table 7.3.5's taxes on production and imports.
FARM_LINE = ('T70305', 'B1017C')

#: BEA detail farm industry code prefixes -- crop and animal production.
FARM_PREFIXES = ('111', '112')

#: Government industry codes, which carry no taxes on production at all.  Same
#: rule and same prefixes as ``tax_axis_conversion.GOVERNMENT_PREFIXES``.
GOVERNMENT_PREFIXES = ('S00', 'G')

#: Summary-table row names for the two lookups, used for the out-of-sample test.
SUMMARY_HOUSING = 'HS'
SUMMARY_FARM = '111CA'

#: An allocator worth using would have to beat this.  None of the four comes
#: close -- the best is 71% error -- so it is a bar the report fails on purpose,
#: the way ``tax_axis_conversion.USABLE_CORRELATION`` is.
USABLE_ERROR_SHARE = 0.25

#: The frozen-share method fails if summary composition drift ever exceeds this.
#: Measured 1.01-2.10% across 2018-2024.
DRIFT_BAR = 0.05

#: How far a lookup or the control may sit from the SUT before the assembly is
#: in question, as a share of the figure.  2021 is the loosest year -- a vintage
#: mismatch between the NIPA revision and the summary SUT workbook, not a
#: method error.
TOLERANCE = 0.05


@functools.cache
def _use() -> pd.DataFrame:
    """The 2017 detail Use SUT.  Read many times over, never written."""
    return _load_2017_detail_supply_use_usa('Use_SUT_detail')


def nipa(table: str, code: str, year: int = YEAR) -> float:
    """One NIPA series by code.

    By code rather than by line, because several of these tables restate a
    concept on more than one line and the root is rarely the one wanted (#536).
    """
    frame = nipa_flat_table(table, year).frame
    match = frame.loc[frame['code'] == code, 'value']
    if match.empty:
        raise KeyError(f'{code} not found in {table}@{year}')
    return float(match.iloc[0])


def industries() -> list[str]:
    """The 402 BEA 2017 detail industry codes."""
    return [str(i) for i in USA_2017_INDUSTRY_CODES]


def use_row(row: str) -> 'pd.Series[float]':
    """One Use SUT row across the 402 detail industries, $M.

    Reindexed onto the industry list rather than sliced with it, which keeps the
    return a ``Series`` for the type checker as well as at runtime -- the same
    accessor shape ``tax_axis_conversion.published_row`` uses.
    """
    series = _use().loc[row]
    assert isinstance(series, pd.Series)
    return series.reindex(industries()).astype(float).fillna(0.0)


def published_row(row: str = ROW) -> 'pd.Series[float]':
    """The ``T00OTOP`` row across the 402 detail industries, $M."""
    return use_row(row)


def farm_industries() -> list[str]:
    """The ten BEA detail crop and animal production codes."""
    return [i for i in industries() if i.startswith(FARM_PREFIXES)]


def government_industries() -> list[str]:
    """The ten government industry codes, general and enterprise."""
    return [i for i in industries() if i.startswith(GOVERNMENT_PREFIXES)]


def composition(year: int = YEAR) -> pd.DataFrame:
    """``T30500``'s breakdown of the state and local line, with shares."""
    state_local = nipa('T30500', 'LA000365', year)
    frame = pd.DataFrame(
        [
            {'code': code, 'name': name, 'value': nipa('T30500', code, year)}
            for code, name in COMPOSITION_LINES
        ]
    )
    return frame.assign(share=lambda x: x['value'] / state_local)


def control(year: int = YEAR) -> float:
    """The NIPA control for the row: state and local plus federal."""
    return sum(nipa(table, code, year) for table, code in CONTROL_LINES)


def allocator_scores() -> pd.DataFrame:
    """Score each candidate allocator against the published 2017 detail row.

    Every candidate is clipped at zero and rescaled to the published total, so
    the column total is right by construction and only the distribution is
    judged -- the same framing ``tax_axis_conversion`` uses.
    """
    published = published_row()
    candidates = {
        'industry output T018': use_row('T018'),
        'value added VABAS': use_row('VABAS'),
        'gross operating surplus V00300': use_row('V00300'),
        'compensation V00100': use_row('V00100'),
    }
    rows = []
    for name, weight in candidates.items():
        positive = weight.clip(lower=0)
        estimate = positive / positive.sum() * published.sum()
        error = estimate - published
        worst = str(error.abs().idxmax())
        rows.append(
            {
                'allocator': name,
                'correlation': float(np.corrcoef(estimate, published)[0, 1]),
                'error_share': float(error.abs().sum() / published.sum()),
                'worst_industry': worst,
                'worst_diff': float(error[worst]),
            }
        )
    return pd.DataFrame(rows)


def concentration() -> dict[str, float]:
    """How much of the row sits in how few industries."""
    published = published_row()
    total = float(published.sum())
    ordered = published.sort_values(ascending=False)
    return {
        'total': total,
        'nonzero': float((published != 0).sum()),
        'real_estate_share': float(
            published[['531HSO', '531HST', '531ORE']].sum() / total
        ),
        'top20_share': float(ordered.head(20).sum() / total),
        'hhi': float(((published / total) ** 2).sum()),
    }


def effective_rates() -> 'pd.Series[float]':
    """``T00OTOP`` over industry output, for the industries with output."""
    return (published_row() / use_row('T018').replace(0, np.nan)).dropna()


def summary_row(year: int) -> 'pd.Series[float]':
    """The summary Use SUT's ``T00OTOP`` row, industry columns only.

    ``T0*`` columns are the table's own totals and would double the row.
    """
    table = _load_usa_summary_sut('Use_SUT_summary', cast(Any, year))
    columns = [
        column
        for column in table.columns
        if column != 'Commodities/Industries' and not str(column).startswith('T0')
    ]
    series = table.loc[ROW]
    assert isinstance(series, pd.Series)
    return pd.to_numeric(series.reindex(columns), errors='coerce').fillna(0.0)


def summary_years() -> list[int]:
    """The years the summary SUT is wired up for."""
    return sorted(int(year) for year in get_args(USA_SUMMARY_SUT_YEARS))


def lookups(year: int = YEAR) -> pd.DataFrame:
    """The two published sector lines against what the SUT books.

    Every year compares against the summary row, which is the out-of-sample
    part; 2017 also compares against the detail row.
    """
    summary = summary_row(year)
    detail_housing = detail_farm = float('nan')
    if year == YEAR:
        published = published_row()
        detail_housing = float(published[list(HOUSING_CODES)].sum())
        detail_farm = float(published[farm_industries()].sum())
    return pd.DataFrame(
        [
            {
                'sector': 'housing',
                'nipa': nipa(*HOUSING_LINE, year),
                'summary_sut': float(summary[SUMMARY_HOUSING]),
                'detail_sut': detail_housing,
            },
            {
                'sector': 'farm',
                'nipa': nipa(*FARM_LINE, year),
                'summary_sut': float(summary[SUMMARY_FARM]),
                'detail_sut': detail_farm,
            },
        ]
    ).assign(diff=lambda x: x['nipa'] - x['summary_sut'])


def housing_split() -> dict[str, float]:
    """Owner's share of the housing pair, the frozen way and the GVA way.

    They disagree by 6.8 percentage points, so the choice is real rather than a
    formality -- and the frozen share is the one that reproduces 2017.
    """
    pair = published_row()[list(HOUSING_CODES)]
    gva_owner, gva_tenant = (nipa(table, code) for table, code in HOUSING_GVA_LINES)
    return {
        'frozen_owner_share': float(pair['531HSO'] / pair.sum()),
        'gva_owner_share': float(gva_owner / (gva_owner + gva_tenant)),
        'pair_total': float(pair.sum()),
    }


def share_drift() -> pd.DataFrame:
    """Composition drift of the row against 2017, on the held-out summary SUT.

    The test the frozen-share method has to pass, run on the one table Step 5
    keeps out of the target set precisely so it can grade things like this.
    """
    frame = pd.DataFrame({year: summary_row(year) for year in summary_years()})
    shares = frame / frame.sum()
    base = shares[YEAR]
    return pd.DataFrame(
        [
            {
                'year': year,
                'level': float(frame[year].sum()),
                'level_growth': float(frame[year].sum() / frame[YEAR].sum() - 1),
                'drift': float((shares[year] - base).abs().sum()),
                'control': control(int(year)),
            }
            for year in shares.columns
        ]
    )


def report() -> None:
    """Print the whole measurement."""
    published = published_row()
    conc = concentration()
    print(f'{ROW} {YEAR}: {conc["total"]:,.0f} $M over {len(industries())} industries')
    print(
        f'  nonzero {conc["nonzero"]:.0f}   real estate '
        f'{conc["real_estate_share"]:.1%}   top 20 {conc["top20_share"]:.1%}   '
        f'HHI {conc["hhi"]:.3f}'
    )

    print(f'\nWhat the row is made of -- T30500 state and local, {YEAR}:')
    for _, row in composition().iterrows():
        print(
            f'  {row["code"]:<10} {row["value"]:>10,.0f}  {row["share"]:>6.1%}  '
            f'{row["name"]}'
        )
    print(
        f'  NIPA control {control():>10,.0f} against a published row of '
        f'{published.sum():,.0f}   diff {control() - published.sum():,.0f}'
    )

    print('\nAllocator candidates, each rescaled to the published total:')
    for _, row in allocator_scores().iterrows():
        print(
            f'  {row["allocator"]:<32} corr {row["correlation"]:>6.3f}   err '
            f'{row["error_share"]:>6.1%}   worst {row["worst_industry"]:<8} '
            f'{row["worst_diff"]:>+11,.0f}'
        )
    rates = effective_rates()
    output = float(use_row('T018').sum())
    print(
        f'  effective rate: p10 {rates.quantile(0.1):.2%}  median '
        f'{rates.quantile(0.5):.2%}  p90 {rates.quantile(0.9):.2%}  max '
        f'{rates.max():.2%} ({rates.idxmax()})  economy-wide '
        f'{published.sum() / output:.2%}'
    )

    print('\nThe published lookups, against the summary SUT in every year:')
    print(
        f'  {"year":>5} {"housing NIPA":>13} {"summary":>10} {"diff":>7} | '
        f'{"farm NIPA":>10} {"summary":>9} {"diff":>6}'
    )
    for year in summary_years():
        table = lookups(year).set_index('sector')
        housing, farm = table.loc['housing'], table.loc['farm']
        print(
            f'  {year:>5} {housing["nipa"]:>13,.0f} {housing["summary_sut"]:>10,.0f} '
            f'{housing["diff"]:>7,.0f} | {farm["nipa"]:>10,.0f} '
            f'{farm["summary_sut"]:>9,.0f} {farm["diff"]:>6,.0f}'
        )
    split = housing_split()
    print(
        f'  owner share of the housing pair: frozen '
        f'{split["frozen_owner_share"]:.2%} against gross value added '
        f'{split["gva_owner_share"]:.2%} -- they disagree, and frozen wins'
    )
    government = published[government_industries()]
    print(
        f'  government: {len(government)} industry codes, summing to '
        f'{government.sum():,.0f}'
    )

    print('\nFrozen 2017 shares, graded on the held-out summary SUT:')
    for _, row in share_drift().iterrows():
        year = int(row['year'])
        growth = '     --' if year == YEAR else f'{row["level_growth"]:>+6.1%}'
        drift = '    --' if year == YEAR else f'{row["drift"]:>6.2%}'
        print(
            f'  {year}  level {row["level"]:>10,.0f}  growth {growth}  '
            f'composition drift {drift}   NIPA control {row["control"]:>10,.0f}'
        )


def check() -> int:
    """Assert the findings Step 2's ``T00OTOP`` method rests on.

    Analysis modules here carry their checks as a CLI flag rather than as unit
    tests.  A later BEA or NIPA vintage that breaks one of these breaks the
    method, so it should fail here rather than quietly inside the FBS.
    """
    failures = []
    published = published_row()

    diff = abs(control() - published.sum())
    if diff > 50:
        failures.append(
            f'the NIPA control is {diff:,.0f} from the published row; T30500 '
            f'{" + ".join(code for _, code in CONTROL_LINES)} no longer '
            f'assembles it'
        )

    for _, row in allocator_scores().iterrows():
        if row['error_share'] < USABLE_ERROR_SHARE:
            failures.append(
                f'{row["allocator"]} now reproduces the row to '
                f'{row["error_share"]:.1%}; it has become a usable allocator '
                f'and the frozen-share method should be revisited'
            )

    real_estate_share = concentration()['real_estate_share']
    if real_estate_share < 0.3:
        failures.append(
            f'real estate now holds {real_estate_share:.1%} of the row; the '
            f'property-tax reading, and the housing lookup with it, is in doubt'
        )

    government_total = float(published[government_industries()].sum())
    if government_total != 0:
        failures.append(
            f'government industries now carry {government_total:,.0f} of {ROW}; '
            f'the zero was an accounting rule, not a gap'
        )

    # Both lookups, in every year the summary table exists.  Housing is exact in
    # six of eight years and farm in seven; the misses are vintage mismatches,
    # so the bar is a share of the sector rather than the dollar.
    for year in summary_years():
        table = lookups(year).set_index('sector')
        for sector in ('housing', 'farm'):
            entry = table.loc[sector]
            assert isinstance(entry, pd.Series)
            if abs(entry['diff']) > TOLERANCE * entry['summary_sut']:
                failures.append(
                    f'{year} {sector}: NIPA {entry["nipa"]:,.0f} against a '
                    f'summary SUT {entry["summary_sut"]:,.0f}; the lookup no '
                    f'longer holds'
                )

    worst_drift = float(share_drift()['drift'].max())
    if worst_drift > DRIFT_BAR:
        failures.append(
            f'summary composition drift has reached {worst_drift:.1%}, above '
            f'the {DRIFT_BAR:.0%} the frozen-share method is justified by'
        )

    if failures:
        print(f'{len(failures)} finding(s) no longer hold:')
        for failure in failures:
            print(f'  - {failure}')
        return 1
    print(
        f'{ROW}: the control closes, no allocator is usable, both lookups hold '
        f'in every summary year, government is zero, and composition drift '
        f'stays under {DRIFT_BAR:.0%}.'
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--check',
        action='store_true',
        help='assert the findings rather than printing the report',
    )
    args = parser.parse_args()
    if args.check:
        return check()
    report()
    return 0


if __name__ == '__main__':
    sys.exit(main())
