"""Step 2 for 2018-2024: what changed once value added became an observation.

Step 2's 2017 build (#538) estimates three Use rows -- ``V00100``, ``T00OTOP``,
``V00300`` -- by taking a *level* from NIPA and a *within-group distribution*
from the 2017 benchmark.  The plan for 2018-2024 was to hold that shape and move
it: 2017 detail compensation shares carried on QCEW wage growth, renormalised
inside each of ``T60200D``'s 69 industry groups, then rescaled to the NIPA
control.  See :doc:`compensation_disaggregation_plan`.

Two sources have landed since that plan was written, and together they change
what Step 2 is estimating rather than merely improving an input to it:

``UVA205-A``
    BEA's *underlying* value added, 138 leaf industry lines, annual 1997-2024,
    allocated to the 402 BEA detail industries by
    :mod:`~bedrock.transform.iot.derived_intermediate_and_value_added` (#712).
    A ``VAPRO`` **column total** per detail industry per year.

``TVA113``
    *Components of Value Added by Industry*, 71 BEA summary industries, annual
    1997-2024, read by :mod:`~bedrock.extract.bea.BEA_GDPbyIndustry` (#538).
    A three-way split of that same ``VAPRO`` into compensation, taxes on
    production and imports less subsidies, and gross operating surplus.

**They are the same estimates at two grains, and this module proves it.**  Rolled
to the 71 summary industries, the detail ``VAPRO`` panel and ``TVA113``'s
industry rows agree to **$9 million** in the worst of 923 industry-years, and
nationally to $9 million on $29 trillion.  So they do not have to be reconciled,
traded off, or ranked -- they nest.

What Step 2 now estimates
-------------------------

For each year and each summary industry ``i``, the block gains a **column**
margin it did not have::

                 detail child d1  d2  ...  dn         row control
    V00100                                        T60200D, 69 NIPA groups
    T00OTOP                                       T30500, one control
    V00300                                        ** the residual **
    T00TOP  }  the basic-to-producer wedge        left free -- Step 5
    T00SUB  }                                     left free -- Step 5
    ---------------------------------------------------------------
    column margin   VAPRO_d1 VAPRO_d2 ... VAPRO_dn   UVA205-A, observed

The row controls are NIPA's, unchanged -- see the decision below.  What is new is
that the **column** is no longer free: ``VAPRO_d(t)`` is observed for every
detail industry in every year, so **Step 2 estimates a cross-structure, not a
level.**  That is the same reframing #497 got when
:mod:`~.intermediate_structure_drift` found Step 5 holds both margins of the
intermediate block.  A source that delivers a column total for value added now
delivers a number the model already has.

Where the freedom actually is
-----------------------------

Reported by ``--freedom``, and it is much less than 402 x 3:

- **20 of the 71 summary industries have exactly one detail child**, carrying
  **19.4%** of 2017 value added.  For those the block is 3 x 1 and both margins
  determine it -- no estimation, no allocator, no QCEW.
- **74 of the 138 underlying leaf lines *are* a single detail industry**,
  carrying **63.9%** of value added.  For those, ``VAPRO_d(t)`` is BEA's own
  published annual number and no allocation model touches it.  The remaining 64
  lines spread over 328 industries, and there the column margin is modelled.
- Only **26.9%** of value added sits in the 17 summary industries with 10 or
  more children, which is where a within-group allocator earns its keep.

What the new column control is worth
------------------------------------

``--price`` grades the assumption the old plan rested on -- that within-summary
composition can be held at 2017 -- **on BEA's own annual leaf lines**, so no
allocation model enters the measurement:

===== =============== ==========
year  misplaced $M    % of VA
===== =============== ==========
2018           93,425       0.45
2021          481,034       2.03
2024          678,415       2.32
===== =============== ==========

Small next to Step 3's 17.3% structural drift, and not nothing: $678 billion in
2024, **3.75%** of the $18.1 trillion that sits in the 24 summary industries
where the movement is measurable at all, and 81% of it in ten groups led by
other retail (11.7% of the group), wholesale trade, and construction.

⚠️ **These are a lower bound on the detail movement.**  47 summary industries are
a single leaf line, so their within-group movement contributes nothing to the
numerator and their value added contributes to the denominator; and movement of
detail *within* a leaf line is invisible to the source.

Decided 2026-08-26: ``TVA113`` stays a **test**, not an input
-------------------------------------------------------------

#538 left open whether to consume ``TVA113`` as Step 2's industry axis.  ❌ **It
is not consumed.**  The row controls stay NIPA's, as the plan below has them --
``T60200D``'s 69 groups for ``V00100``, ``T30500`` for ``T00OTOP``, the eight-line
assembly for ``V00300`` -- so summary ``V001``/``V002``/``V003`` keep their
ability to grade the build, which is what Step 5's Decision 3 reserves them for.

⚠️ **Two claims an earlier draft of this module made are therefore withdrawn.**

- ❌ *"``T00TOP`` gets an industry axis after all."*  ``TVA113``'s ``V00200``
  **is** ``T00OTOP + T00TOP - T00SUB`` by industry -- ``--topils`` verifies it at
  **max $1M across all 71** -- but a test source cannot supply a build.  #536's
  conclusion stands unchanged: the industry split of product taxes is left free
  for Step 5, and the market-share conversion (r = 0.202) stays rejected.  What
  the identity buys instead is a **grader**: it says where the money should have
  landed -- wholesale trade 210,708, other retail 104,107, food services 52,391,
  motor vehicle dealers 45,947 -- so whatever Step 5 produces for those rows can
  be scored rather than trusted.
- ❌ *"``T60200D``'s 69 groups are superseded."*  They are not.  They remain the
  compensation control.

✅ **The ``VAPRO`` column control is a separate decision and it is kept.**
``UVA205-A`` is the sibling of ``UGO305-A``, which is already T1 -- a hard
target -- so the underlying detail release is precedent, not a new class of
source.  What is spent is the value-added **total** per industry; the three-way
**split** stays in the test set.

Where the error lands -- the reason the column control is imposed
-----------------------------------------------------------------

T1 pins ``T005 + VAPRO = GO`` per industry but **not the split**, and T5 is
deliberately not imposed at all (#710).  So today the slack falls on ``T005``,
the intermediate column total -- which is *the scale of a column of* ``A``.
Income-side estimation error propagates through ``L`` into every downstream
``N``.

Pinning ``VAPRO`` to ``UVA205-A`` makes ``T005 = GO - VAPRO``, both observed from
the same BEA release, and moves the slack inside value added -- where ``V00300``
is the natural home, since BEA builds gross operating surplus as a residual too
and it is already where the statistical discrepancy lands
(:mod:`~.value_added_control_totals`).

⚠️ **``--residual`` states the honest counterargument.**  In percentage terms
``T005`` is the *more forgiving* absorber, because it is the bigger number: a 1%
compensation error is a median **0.50%** of an industry's ``T005`` and a median
**1.55%** of its ``V00300``.  That loses on a different axis -- ``V00300`` is
**terminal**.  Nothing reads gross operating surplus: it is not in ``A``, not in
``L``, not in any emission factor.  A 7% error in a number nothing reads beats a
1.4% error in a column scale that multiplies through the Leontief inverse.

⚠️ **22 industries have too little surplus to absorb much**, and they need a sign
guard rather than trust.  A 1% compensation error moves their gross surplus by
more than 10%; the worst is ``336414`` (guided missiles) at **121%**, an $81M
surplus under $9.8B of compensation.  Only **one** industry has a published
negative ``V00300`` -- ``S00201`` at -36,919 -- so a residual that manufactures
new negatives is visibly wrong and cheap to detect.

What is left for QCEW
---------------------

The movement series is still needed, unchanged in construction -- 2017 detail
shares carried on QCEW wage growth, renormalised inside ``T60200D``'s 69 groups,
rescaled to the NIPA control.  What changed is what happens to its error, and
that it can now be graded.

- ⚠️ **Its error no longer reaches ``A``.**  With ``VAPRO_d`` pinned and
  ``V00300`` the slack row, a QCEW movement that is wrong about two detail
  children of a summary industry moves gross operating surplus and stops there,
  instead of moving that column's ``T005`` and with it the scale of a column of
  the technology matrix.
- ✅ **It can be graded on the benchmark holdout** -- 2012 -> 2017 observed detail
  compensation, the rule [#704] established for Step 3 seeds -- rather than
  against BEA's carried-forward later years.  That needs QCEW 2012, which
  ``BLS_QCEW.yaml`` did not declare until #728.

Run::

    uv run python -m bedrock.analysis.nowcasting.value_added_timeseries
    uv run python -m bedrock.analysis.nowcasting.value_added_timeseries --check
"""

from __future__ import annotations

import argparse
import functools
import sys
import typing as ta

import numpy as np
import pandas as pd

from bedrock.extract.bea.BEA_GDPbyIndustry import (
    _parse_sheet,
    _read_sheets_from_zip,
    gdp_by_industry_local_path,
)
from bedrock.extract.iot.constants import (
    UNDERLYING_LINE_TO_BEA_2017_INDUSTRY_MAPPING,
)
from bedrock.extract.iot.gdp import load_va_underlying, underlying_leaf_lines
from bedrock.extract.iot.io_2017 import _load_usa_summary_sut
from bedrock.transform.iot.derived_intermediate_and_value_added import (
    detail_value_added_panel,
)
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES
from bedrock.utils.taxonomy.bea.v2017_industry_summary import (
    USA_2017_SUMMARY_INDUSTRY_CODES,
)
from bedrock.utils.taxonomy.mappings.bea_v2017_industry__bea_v2017_summary import (
    load_bea_v2017_industry_to_bea_v2017_summary,
)

#: The benchmark year both sources are anchored on, and the year the published
#: detail and summary SUTs exist for.
ANCHOR_YEAR = 2017

#: The nowcast horizon.  ``TVA113`` and ``UVA205-A`` both run 1997-2024, so the
#: span here is the model's, not the data's.
NOWCAST_YEARS = tuple(range(2018, 2025))

#: Years both sources publish and the summary SUT can grade.
RECONCILE_YEARS = tuple(range(2012, 2025))

#: ``TVA113``'s value-added row and its three component rows, as
#: :mod:`~bedrock.extract.bea.BEA_GDPbyIndustry` codes them.
VALUE_ADDED = 'VAPRO'
COMPONENTS = ('V00100', 'V00200', 'V00300')

#: BEA rounds ``TVA113`` and ``UVA205-A`` independently, and the two are
#: published in millions.  $10M on a $29T total is rounding, not disagreement --
#: the same tolerance argument :mod:`~bedrock.transform.iot` makes for the
#: 191->402 derivation, one grain up.
ROUNDING_TOLERANCE = 10.0


# --------------------------------------------------------------------------
# sources
# --------------------------------------------------------------------------


@functools.lru_cache(maxsize=1)
def components_by_line() -> pd.DataFrame:
    """``TVA113`` as line x year x code, in millions of dollars.

    Read straight from the cached release archive rather than through the FBA,
    because this is a diagnostic over thirteen years and the FBA path would
    generate thirteen parquets to answer one question.  The parsing is the
    extractor's own, so a sheet-shape change fails here the same way it would
    fail the extract.
    """
    config = {'files': ['ValueAdded.xlsx'], 'sheets': ['TVA113-A']}
    frames = _read_sheets_from_zip(gdp_by_industry_local_path(), config)
    frame = _parse_sheet(frames[0], 'TVA113-A')
    frame['FlowAmount'] = frame['FlowAmount'] / 1e6
    frame['Year'] = frame['Year'].astype(int)
    return frame


@functools.lru_cache(maxsize=1)
def detail_to_summary() -> dict[str, str]:
    """BEA detail industry -> BEA summary industry, all 402, one parent each.

    ⚠️ **The industry map, not the commodity one.**
    :func:`~bedrock.analysis.nowcasting.frozen_mix_diagnostic.detail_to_summary`
    reaches for ``load_bea_v2017_commodity_to_bea_v2017_summary``, which is right
    on the Supply table's commodity axis and drops four codes on the industry
    axis: ``331314``, ``S00101``, ``S00201`` and ``S00202``.  Rolling value added
    up through it leaves state and local government enterprises 15% short in
    every year, which reads exactly like a vintage disagreement and is not one.
    """
    mapping = load_bea_v2017_industry_to_bea_v2017_summary()
    return {detail: parents[0] for detail, parents in mapping.items() if parents}


@functools.lru_cache(maxsize=1)
def line_to_summary() -> dict[int, str]:
    """``TVA113`` industry line -> BEA summary industry code, derived not written.

    ``TVA113`` names its industries in words and gives them no codes, and it is
    hierarchical, so the leaves have to be found rather than assumed.  They are
    found by *value*: each summary industry's published 2017 ``V001`` matches
    exactly one ``TVA113`` compensation row.  All 71 match to under $0.5 million,
    and no line is claimed by two industries -- which is the same evidence
    :mod:`~.compensation_allocation` reports for ``T60200D``, and the reason the
    aggregate rows above the leaves cannot be picked up by accident.
    """
    published = summary_sut_row('V001', ANCHOR_YEAR)
    anchor = (
        components_by_line()
        .query(f'Year == {ANCHOR_YEAR}')
        .pivot_table(index='Line', columns='Code', values='FlowAmount')
    )
    mapping: dict[int, str] = {}
    for code, value in published.items():
        hits = anchor.index[np.isclose(anchor['V00100'].to_numpy(), value, atol=0.5)]
        if len(hits) != 1:
            raise ValueError(
                f'summary industry {code} matches {len(hits)} TVA113 compensation '
                f'rows at $0.5M; the table has been restated and the derivation '
                f'needs revisiting'
            )
        line = int(hits[0])
        if line in mapping:
            raise ValueError(
                f'TVA113 line {line} matches both {mapping[line]} and {code}'
            )
        mapping[line] = str(code)
    return mapping


def _cell(frame: pd.DataFrame, row: ta.Any, column: ta.Any) -> float:
    """One cell of a frame as a float.

    ``DataFrame.loc[r, c]`` is typed as pandas' full scalar union, which mypy
    will not narrow, so every arithmetic use of it needs this.
    """
    return float(np.asarray(frame.loc[row, column]).item())


def summary_sut_row(row: str, year: int = ANCHOR_YEAR) -> pd.Series:
    """One row of the published summary Use SUT, by summary industry, in $M."""
    table = _load_usa_summary_sut('Use_SUT_summary', year)  # type: ignore[arg-type]
    codes = [c for c in USA_2017_SUMMARY_INDUSTRY_CODES if c in table.columns]
    series = table.loc[row]
    assert isinstance(series, pd.Series)
    return pd.to_numeric(series.reindex(codes), errors='coerce').fillna(0.0)


@functools.lru_cache(maxsize=1)
def leaf_value_added() -> pd.DataFrame:
    """``UVA205-A``'s 138 leaf lines x year, in millions, year columns as ints.

    The leaves are the observed series.  Everything finer than this is allocated
    by :mod:`~bedrock.transform.iot.derived_intermediate_and_value_added`, so a
    measurement that wants to be free of the allocation model stops here.
    """
    frame = load_va_underlying()
    frame = frame.loc[[line for line in underlying_leaf_lines() if line in frame.index]]
    years = [c for c in frame.columns if str(c).isdigit()]
    frame = frame[years].astype(float)
    frame.columns = pd.Index([int(c) for c in years])
    return frame


@functools.lru_cache(maxsize=1)
def leaf_to_summary() -> dict[int, str]:
    """``UVA205-A`` leaf line -> BEA summary industry.

    ✅ **The nesting is clean**: every one of the 138 leaf lines sits inside a
    single summary industry, so detail c line c summary is a strict hierarchy and
    the two constraints stack rather than cross.  A straddling line would have
    made the block non-rectangular and forced the whole thing into one economy-
    wide balance.
    """
    parents = detail_to_summary()
    mapping: dict[int, str] = {}
    for line, children in UNDERLYING_LINE_TO_BEA_2017_INDUSTRY_MAPPING.items():
        summaries = {parents[child] for child in children if child in parents}
        if len(summaries) != 1:
            raise ValueError(
                f'underlying line {line} straddles summary industries '
                f'{sorted(summaries)}; the block is not rectangular'
            )
        mapping[int(line)] = summaries.pop()
    return mapping


# --------------------------------------------------------------------------
# measurements
# --------------------------------------------------------------------------


def reconcile() -> pd.DataFrame:
    """Do ``TVA113`` and the detail ``VAPRO`` panel agree, industry by industry?

    The question that decides whether the two sources can both be imposed.  The
    yaml warns they are *different release vintages* -- ``GDPbyInd.zip`` says
    June 2026, the underlying-detail archive September 2025 -- so the agreement
    is measured rather than assumed.
    """
    panel = detail_value_added_panel()
    rolled = panel.groupby(panel.index.map(detail_to_summary())).sum()
    published = (
        components_by_line()
        .query('Code == @VALUE_ADDED')
        .pivot_table(index='Line', columns='Year', values='FlowAmount')
    )
    lines = line_to_summary()
    rows = []
    for year in RECONCILE_YEARS:
        diffs = []
        for line, summary in lines.items():
            if summary not in rolled.index or year not in published.columns:
                continue
            diffs.append(_cell(rolled, summary, year) - _cell(published, line, year))
        national = float(panel[year].sum()) - float(
            published[year].reindex(list(lines)).sum()
        )
        rows.append(
            {
                'year': year,
                'industries': len(diffs),
                'max_abs_$M': max(abs(d) for d in diffs),
                'sum_abs_$M': sum(abs(d) for d in diffs),
                'national_$M': national,
            }
        )
    return pd.DataFrame(rows).set_index('year')


def topils() -> pd.DataFrame:
    """Is ``TVA113``'s ``V00200`` the SUT's ``T00OTOP + T00TOP - T00SUB``?

    If it is, the industry axis of taxes on products is *published*, and the
    market-share conversion #536 rejected does not have to be replaced -- it has
    to be dropped.  Measured in the benchmark year, where all four rows exist.
    """
    lines = line_to_summary()
    anchor = (
        components_by_line()
        .query(f'Year == {ANCHOR_YEAR}')
        .pivot_table(index='Line', columns='Code', values='FlowAmount')
    )
    published = pd.DataFrame(
        {
            'T00OTOP': summary_sut_row('T00OTOP'),
            'T00TOP': summary_sut_row('T00TOP'),
            'T00SUB': summary_sut_row('T00SUB'),
        }
    )
    published['sut_topils'] = (
        published['T00OTOP'] + published['T00TOP'] - published['T00SUB']
    )
    published['tva113_V00200'] = pd.Series(
        {summary: _cell(anchor, line, 'V00200') for line, summary in lines.items()}
    )
    published['diff'] = published['tva113_V00200'] - published['sut_topils']
    return published.sort_values('T00TOP', ascending=False)


def freedom() -> pd.DataFrame:
    """How much of value added is determined outright once both margins bind?"""
    panel = detail_value_added_panel()
    parents = pd.Series(detail_value_added_panel().index.map(detail_to_summary()))
    parents.index = panel.index
    counts = parents.value_counts()
    anchor = panel[ANCHOR_YEAR].groupby(parents).sum()
    total = float(anchor.sum())

    rows = []
    bands: tuple[tuple[str, int, int], ...] = (
        ('1 (determined)', 1, 1),
        ('2-3', 2, 3),
        ('4-9', 4, 9),
        ('10+', 10, 10_000),
    )
    for label, low, high in bands:
        chosen = counts[(counts >= low) & (counts <= high)].index
        value = float(anchor.reindex(chosen).sum())
        rows.append(
            {
                'children': label,
                'groups': len(chosen),
                'detail_industries': int(counts.reindex(chosen).sum()),
                'VA_2017_$M': value,
                'pct_of_VA': 100.0 * value / total,
            }
        )

    singles = [
        children[0]
        for children in UNDERLYING_LINE_TO_BEA_2017_INDUSTRY_MAPPING.values()
        if len(children) == 1
    ]
    observed = float(panel[ANCHOR_YEAR].reindex(singles).sum())
    rows.append(
        {
            'children': 'leaf line == 1 industry',
            'groups': len(singles),
            'detail_industries': len(singles),
            'VA_2017_$M': observed,
            'pct_of_VA': 100.0 * observed / total,
        }
    )
    return pd.DataFrame(rows).set_index('children')


def published_va_block() -> pd.DataFrame:
    """All five value-added rows plus ``VAPRO`` and ``T005``, published 2017 detail.

    2017 only, and deliberately: this is the one year in which every row of the
    identity is published, so it is the only year the headroom below can be read
    off rather than modelled.
    """
    from bedrock.analysis.nowcasting.sections import (  # noqa: PLC0415
        _use_sut_detail,
    )

    use = _use_sut_detail()
    codes = [c for c in USA_2017_INDUSTRY_CODES if c in use.columns]
    rows = ('V00100', 'T00OTOP', 'V00300', 'T00TOP', 'T00SUB', 'VAPRO', 'T005')
    columns = {}
    for row in rows:
        series = use.loc[row]
        assert isinstance(series, pd.Series)
        columns[row] = pd.to_numeric(series.reindex(codes), errors='coerce').fillna(0.0)
    block = pd.DataFrame(columns)
    block.index.name = 'industry'
    return block


def residual_headroom() -> pd.DataFrame:
    """If ``V00300`` is the slack row, how much can it absorb before it misbehaves?

    The design question behind it: **which row wears the estimation error?**  T1
    pins ``T005 + VAPRO = GO`` but not the split, so today the slack falls on
    ``T005`` -- the intermediate column total, which is the *scale of a column of
    A*.  Pinning ``VAPRO`` to ``UVA205-A`` instead moves the slack inside value
    added, where ``V00300`` is the natural home: BEA builds gross operating
    surplus as a residual too, and it is where the statistical discrepancy
    already lands (:mod:`~.value_added_control_totals`).

    ⚠️ **In percentage terms the intermediate column is the more forgiving
    absorber, and this table says so.**  A 1% error in an industry's compensation
    is a median 0.50% of its ``T005`` and a median 1.55% of its ``V00300``,
    because ``T005`` is the bigger number.  That is the honest counterargument
    and it loses on a different axis: ``V00300`` is **terminal**.  Nothing reads
    gross operating surplus -- it is not in ``A``, not in ``L``, and not in any
    emission factor.  ``T005`` multiplies through the Leontief inverse into every
    downstream ``N``.  A 7% error in a number nothing reads beats a 1.4% error in
    a column scale that propagates.

    ⚠️ **22 industries cannot absorb much.**  Their gross surplus is thin enough
    that a 1% compensation error moves it more than 10% -- worst is ``336414``
    (guided missiles) at **121%**, an $81M surplus under $9.8B of compensation.
    Only **one** industry has a published negative ``V00300`` (``S00201``, at
    -36,919), so a residual that manufactures new negatives is visibly wrong, and
    a sign census over these 22 is the cheap guard.
    """
    block = published_va_block()
    frame = block[block['VAPRO'].abs() > 0].copy()
    shock = 0.01 * frame['V00100'].abs()
    frame['surplus_share'] = frame['V00300'] / frame['VAPRO']
    frame['pct_of_surplus'] = 100.0 * shock / frame['V00300'].abs().replace(0.0, np.nan)
    frame['pct_of_T005'] = 100.0 * shock / frame['T005'].abs().replace(0.0, np.nan)
    return frame.sort_values('pct_of_surplus', ascending=False)


def price(years: ta.Sequence[int] = NOWCAST_YEARS) -> pd.DataFrame:
    """What does holding within-summary composition at 2017 cost?

    Computed on the 138 observed leaf lines, never on the 402 -- the detail split
    is the allocation model's output and grading an assumption against a model
    that shares it measures nothing.

    ``pct_of_measurable`` is the honest denominator: 47 of the 71 summary
    industries are a single leaf line, so no within-group movement is observable
    for them at all and they only dilute the economy-wide figure.
    """
    values = leaf_value_added()
    groups = pd.Series({line: leaf_to_summary()[line] for line in values.index})
    multi = groups.value_counts()
    measurable = groups.isin(multi[multi > 1].index)

    rows = []
    for year in years:
        anchor_total = values[ANCHOR_YEAR].groupby(groups).transform('sum')
        year_total = values[year].groupby(groups).transform('sum')
        shift = (values[ANCHOR_YEAR] / anchor_total) - (values[year] / year_total)
        misplaced = float((shift.abs() * year_total).groupby(groups).sum().sum() / 2)
        rows.append(
            {
                'year': year,
                'misplaced_$M': misplaced,
                'pct_of_VA': 100.0 * misplaced / float(values[year].sum()),
                'pct_of_measurable': 100.0
                * misplaced
                / float(values.loc[measurable, year].sum()),
            }
        )
    return pd.DataFrame(rows).set_index('year')


def where(year: int = 2024) -> pd.DataFrame:
    """Which summary industries carry the composition shift, and how much."""
    values = leaf_value_added()
    groups = pd.Series({line: leaf_to_summary()[line] for line in values.index})
    anchor_total = values[ANCHOR_YEAR].groupby(groups).transform('sum')
    year_total = values[year].groupby(groups).transform('sum')
    shift = (values[ANCHOR_YEAR] / anchor_total) - (values[year] / year_total)
    misplaced = (shift.abs() * year_total).groupby(groups).sum() / 2
    frame = pd.DataFrame(
        {
            'misplaced_$M': misplaced,
            'group_VA_$M': values[year].groupby(groups).sum(),
            'leaf_lines': groups.value_counts(),
        }
    )
    frame = frame[frame['leaf_lines'] > 1]
    frame['pct_of_group'] = 100.0 * frame['misplaced_$M'] / frame['group_VA_$M']
    return frame.sort_values('misplaced_$M', ascending=False)


# --------------------------------------------------------------------------
# check
# --------------------------------------------------------------------------


def check() -> int:
    """Reproduce every figure the docstring quotes.  Returns a process status."""
    failures: list[str] = []

    def expect(label: str, got: float, want: float, tol: float) -> None:
        if abs(got - want) > tol:
            failures.append(f'{label}: got {got:,.2f}, expected {want:,.2f} +-{tol}')

    recon = reconcile()
    if int(recon['industries'].min()) != 71:
        failures.append(f'reconcile covers {int(recon["industries"].min())} industries')
    worst = float(recon['max_abs_$M'].max())
    if worst > ROUNDING_TOLERANCE:
        failures.append(f'worst industry-year disagreement {worst:,.1f}M > rounding')
    national = float(recon['national_$M'].abs().max())
    if national > ROUNDING_TOLERANCE:
        failures.append(f'worst national disagreement {national:,.1f}M > rounding')

    taxes = topils()
    expect('TOPILS max |diff|', float(taxes['diff'].abs().max()), 0.0, 1.0)
    expect('wholesale T00TOP', _cell(taxes, '42', 'T00TOP'), 210_708.0, 1.0)

    free = freedom()
    expect('n=1 share of VA', _cell(free, '1 (determined)', 'pct_of_VA'), 19.4, 0.1)
    expect(
        'single-industry leaf lines',
        _cell(free, 'leaf line == 1 industry', 'groups'),
        74.0,
        0.0,
    )
    expect(
        'directly observed share of VA',
        _cell(free, 'leaf line == 1 industry', 'pct_of_VA'),
        63.9,
        0.1,
    )

    cost = price()
    expect('2018 misplaced %', _cell(cost, 2018, 'pct_of_VA'), 0.45, 0.01)
    expect('2024 misplaced %', _cell(cost, 2024, 'pct_of_VA'), 2.32, 0.01)
    expect('2024 misplaced $M', _cell(cost, 2024, 'misplaced_$M'), 678_415.0, 1.0)
    expect('2024 measurable %', _cell(cost, 2024, 'pct_of_measurable'), 3.75, 0.01)

    top = where(2024)
    expect('2024 worst group', float(top['pct_of_group'].iloc[0]), 11.7, 0.1)
    share = 100.0 * float(
        top['misplaced_$M'].head(10).sum() / top['misplaced_$M'].sum()
    )
    expect('top 10 groups share', share, 81.0, 1.0)

    head = residual_headroom()
    expect('published negative V00300', float((head['V00300'] < 0).sum()), 1.0, 0.0)
    expect('worst thin surplus', float(head['pct_of_surplus'].iloc[0]), 121.4, 0.1)
    expect('thin industries', float((head['pct_of_surplus'] > 10).sum()), 22.0, 0.0)
    expect('median vs T005', float(head['pct_of_T005'].median()), 0.50, 0.01)
    expect('median vs surplus', float(head['pct_of_surplus'].median()), 1.55, 0.01)

    for failure in failures:
        print(f'FAIL {failure}')
    if not failures:
        print('OK   every figure in the module docstring reproduces')
    return 1 if failures else 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reconcile', action='store_true')
    parser.add_argument('--topils', action='store_true')
    parser.add_argument('--freedom', action='store_true')
    parser.add_argument('--price', action='store_true')
    parser.add_argument('--where', action='store_true')
    parser.add_argument('--residual', action='store_true')
    parser.add_argument('--check', action='store_true', help='assert every figure')
    args = parser.parse_args()
    if args.check:
        sys.exit(check())

    everything = not any(
        (
            args.reconcile,
            args.topils,
            args.freedom,
            args.price,
            args.where,
            args.residual,
        )
    )
    if everything or args.reconcile:
        print(
            '\nTVA113 against the detail VAPRO panel, rolled to 71 summary industries'
        )
        print('(different release vintages -- so this is measured, not assumed)\n')
        print(reconcile().round(1).to_string())
    if everything or args.topils:
        taxes = topils()
        print(
            f'\nTVA113 V00200 against the SUT\'s T00OTOP + T00TOP - T00SUB, {ANCHOR_YEAR}'
        )
        print(
            f'(max |diff| {taxes["diff"].abs().max():.1f}M across {len(taxes)} '
            f'industries -- the industry axis of product taxes is published)\n'
        )
        print(taxes.head(12).round(0).to_string())
    if everything or args.freedom:
        print('\nHow much of value added is determined once both margins bind\n')
        print(freedom().round(1).to_string())
    if everything or args.price:
        print('\nHolding within-summary composition at 2017, graded on BEA\'s own')
        print('annual leaf lines -- no allocation model in the measurement\n')
        print(price().round(2).to_string())
    if everything or args.where:
        print('\nWhere the 2024 composition shift sits\n')
        print(where(2024).head(15).round(1).to_string())
    if everything or args.residual:
        head = residual_headroom()
        print('\nIf V00300 is the slack row: what a 1% compensation error moves')
        print(
            f'(median {head["pct_of_surplus"].median():.2f}% of gross surplus '
            f'against {head["pct_of_T005"].median():.2f}% of T005 -- T005 is the '
            f'more forgiving\nabsorber in percent, and the one that propagates '
            f'through L)\n'
        )
        columns = ['V00100', 'V00300', 'VAPRO', 'pct_of_surplus', 'pct_of_T005']
        print(head[columns].head(15).round(1).to_string())
        print(
            f'\nindustries where a 1% compensation error moves surplus by >10%: '
            f'{int((head["pct_of_surplus"] > 10).sum())}; '
            f'published negative V00300: {int((head["V00300"] < 0).sum())}'
        )
    print()


if __name__ == '__main__':
    main()
