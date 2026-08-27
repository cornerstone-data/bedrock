"""Step 3 of the nowcast build: the Use table's intermediate block.

Full treatment in ``bedrock/analysis/nowcasting/intermediate_estimation_plan.md``;
this module is `#497 <https://github.com/cornerstone-data/bedrock/issues/497>`_
as scoped there, and the measurements behind every number quoted below live in
``bedrock/analysis/nowcasting/intermediate_structure_drift.py``.

Three moves, in order
---------------------

1. **Seed** from ``Use_SUT_Framework_2017_DET`` -- the published 2017 detail Use
   SUT interior, 402 commodities x 402 industries. Native SUT, native
   **purchaser** value, native **before** redefinitions, all three in one
   object, which is why the seed is the SUT workbook and not
   ``load_2017_Utot_before_redef_usa``. Seed from the **dollar** matrix, not
   from ``A``: going ``A -> U`` via ``U ~ A @ diag(x)`` discards the rounding
   and the negative-clipping baked in when ``A`` was built.
2. **Carry** each column's shares on the commodity price ratio,
   ``(p_c(t) / p_c(2017)) ** theta``, then renormalise the column back to one.
3. **Control** each column to :func:`intermediate_column_control`, so the block
   arrives at the right level rather than at 2017's.

Only the shares are estimated. Steps 2 and 3 are a per-column rescale of a
column-normalised object, so the level and the structure never mix.

⚠️ theta is a parameter, and 1.0 is not obviously right
--------------------------------------------------------

What ``theta`` is mechanically -- a scalar exponent on a commodity deflator, one
per span, and the ``theta = 1 - sigma`` reading of it -- is documented in
``bedrock/analysis/nowcasting/About_the_price_carry.md``.

``theta = 1`` is #497 as written -- a nominal share carried in full on its own
price movement, which assumes zero substitution. ``theta = 0`` is a frozen
``A``. Fitted per year on the published summary panel (``--theta`` on the drift
diagnostic) it is **negative in the target years**:

==== ==== ==== ==== ==== ===== =====
2018 2019 2020 2021 2022 2023  2024
==== ==== ==== ==== ==== ===== =====
0.75 0.75 1.00 0.50 0.25 -0.25 -0.50
==== ==== ==== ==== ==== ===== =====

so at 2023-24 the frozen structure scores *better* when shares are moved
**against** their own price movement. The detail benchmark span 2012->2017 still
fits 1.00, so the disagreement is regime rather than code.

✅ **Both halves of that are now decided**
(`#699 <https://github.com/cornerstone-data/bedrock/issues/699>`_). ``theta``
comes from :func:`default_theta`, a two-regime rule fitted on **78 non-nested
summary spans** rather than on the seven this build runs, and the deflator is
the full purchaser one -- the producer price ratio times
:func:`margin_rate_factor`. #497's ``theta = 1`` survives as
:data:`THETA_497`, and ``margins=False`` still gets the producer-only leg, so
the two can be scored against each other rather than only argued about.

⚠️ **The headline is that the carry barely matters in this regime.** On a
span that crosses the 2021-22 price surge -- which every target year from 2022
on does -- the median gain of the *best* theta over a frozen ``A`` is 0.59% of
the score, against 5.44% off the surge. #497's 1.0 is not merely unfitted
there, it costs 12.6% at 2024; a frozen structure gives that back and the
fitted negative theta adds under one percent on top.

The price index is an *industry* index used on commodity rows
-------------------------------------------------------------

bedrock publishes :func:`~bedrock.transform.iot.derived_price_index
.derive_industry_price_index`, a detail **industry** price index, and #497 asks
for a commodity one. At BEA detail the two code lists are the same 398 codes
plus four each way, and the detail Make table is near-diagonal, so an industry
code's deflator is that commodity's deflator. The four commodity rows with no
industry counterpart -- :data:`UNPRICED_COMMODITIES` -- are held at 1.0 rather
than given a borrowed index; see there for why that is the right answer and not
a gap.

The column control
------------------

``T005[j] = GO_producer[j] - VAPRO[j]``, exact in the published table to $1M on
$34T (measured: 34,468,127 against 34,468,114, and no industry off by more than
1). Both sides are observed annually, from
:mod:`bedrock.transform.iot.derived_intermediate_and_value_added`, which
allocates BEA's ``UGO205-A``/``UVA205-A``/``UII205-A`` underlying-industry
tables down to the 402 detail industries. Its ``VAPRO`` reproduces
``UVA205-A``'s line totals exactly and the published 2017 detail ``VAPRO``
column to 0.89 million USD; ``T005`` is taken there as the residual
``GO - VAPRO``, so the control and :func:`vapro` satisfy T1 by construction.

The control is read off that module rather than differenced here, so the two
agree to the floating-point bit.

✅ **Aggregated to summary and scored against the published summary ``T005``**
(``column_control`` on the drift diagnostic), the control is within
**0.00007% economy-wide** and **0.00023% weighted MAE by industry** in every
year 2018-2024, worst summary industry 0.003%. The superseded frozen-ratio seed
scored 0.2-2.3% and 2.5-8.0% on the same two columns, with ``GSLG`` state and
local government 18.3% low at 2022.

⚠️ **That is a consistency check, not an independent validation.**
``UII205-A``/``UVA205-A`` and the summary Use SUT's ``T005`` are the same BEA
estimate published two ways. What it establishes is that the 191-line to detail
allocation adds back correctly, so the control *is* BEA's published ``T005``
rather than an approximation of it -- not that BEA's number was tested against
a second source.

⚠️ **The remaining ``G*`` defect is
`#578 <https://github.com/cornerstone-data/bedrock/issues/578>`_ and this does
not touch it.** What is fixed here is the government column *total*. #578 is the
commodity *mix* inside the ``G*`` columns, sourced from Census
``govslocalfin``'s function x object split, and sequenced later.

⚠️ **Do not let the control leak into the sourcing argument.** Having the column
total for free is precisely why a candidate source that supplies only a column
total supplies nothing to this step -- ``T31005`` included.

⚠️ Step 5 does NOT overwrite all of this
----------------------------------------

An earlier draft of this docstring said it did. It does not, and the difference
decides how much weight :func:`vapro` carries.

What Step 5 imposes, from :mod:`bedrock.transform.iot.nowcast_targets`:

===== ===================================== ==========================
T1    ``T005 + VAPRO = GO_producer``        **hard, real, 2017-2024**
T18   ``VAPRO`` per industry                **hard, real, 2017-2024**
T4    ``V00100`` by industry group          soft, ``PLACEHOLDER``
T6    ``T00TOP``/``T00SUB`` economy-wide    soft, ``PLACEHOLDER``
T5    ``T00OTOP``, ``V00300``               **deliberately not imposed**
===== ===================================== ==========================

⚠️ **T1 pins the column's sum, not its split.** It says intermediate plus all
five value-added rows equal gross output; it says nothing about where the line
between them falls.

✅ **T18 pins the split, as of 2026-08-26.** It is the value-added half of the
same column, sourced from ``UVA205-A`` -- the sibling of the ``UGO305-A``
behind T1 -- so ``T005`` is now determined per industry rather than left as the
place income-side error lands. This is what the last paragraph of this section
predicted would be needed; it now exists.

⚠️ Within value added the split is still pinned only by T4 and T6 -- both soft,
both still placeholders, and ``va_row_targets`` reads their values off
``published_2017_panel`` after ``del year``, so they carry no annual movement.

⚠️ **T5 is unimposed on purpose, and that is now load-bearing**: ``T00OTOP``
and ``V00300`` "enter the balance as seed only, which is the price of the test
being worth running" -- the income side is held back so GDP stays out-of-sample
evidence. With T18 fixing the column total, leaving ``V00300`` free is also
what makes it the row that absorbs the residual, which is where the model wants
its error: gross operating surplus is **$7.873T** in 2017 and appears in no
``A``, no ``L`` and no emission factor.

So the claim that survives is the narrow one: Step 5 re-solves the
``T00TOP``/``T00SUB`` **wedge**. §The column control's argument assumed
``VABAS`` arrived from Step 2; what arrives instead is an observed ``VAPRO``,
which pins the ``T005``/``VAPRO`` split that T1 alone leaves free.

⚠️ **This supplies ``VAPRO``, not ``VABAS``, and Step 2 is still unbuilt.**
``VAPRO`` is value added at producer prices -- the whole column below ``T005``,
taxes and subsidies on products included. Step 2 owes the *split* of it across
the five value-added rows, and T4/T6 remain soft placeholders reading 2017
values off ``published_2017_panel``. What has changed is that there is now a VA
level for every year 1997-2024 rather than for 2017 alone, so the balance can
run on 2024.

⚠️ **The estimand here is still the column shape**, and the shape is untouched:
it is seeded, carried and renormalised before the control is applied, so the
control rescales a column without moving one share within it.
"""

from __future__ import annotations

import functools

import numpy as np
import pandas as pd

from bedrock.extract.iot.constants import GCS_USA_SUP_DIR
from bedrock.extract.iot.io_2017 import (
    LOCAL_USA_SUP_DIR,
    _load_2017_detail_supply_use_usa,
    _load_benchmark_detail_supply_use_usa,
)
from bedrock.transform.iot.derived_intermediate_and_value_added import (
    derive_detail_intermediate_inputs,
    derive_detail_value_added,
)
from bedrock.transform.iot.derived_price_index import derive_industry_price_index
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.io.gcp import load_from_gcs
from bedrock.utils.taxonomy.bea.matrix_mappings import (
    USA_BENCHMARK_DETAIL_SUT_YEARS,
)
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES
from bedrock.utils.taxonomy.mappings.bea_v2017_commodity__bea_v2017_summary import (
    load_bea_v2017_commodity_to_bea_v2017_summary,
)

#: The benchmark the structure is seeded from. Typed as a benchmark year because
#: the margin leg reads the detail Supply table here, which BEA publishes only
#: for 2007, 2012 and 2017.
SEED_YEAR: USA_BENCHMARK_DETAIL_SUT_YEARS = 2017

#: Years this step can be built for. Bounded by
#: ``BEA_Detail_GrossOutput_IO_<year>``, extracted for 2017-2024; the price
#: index runs to 2025 and is not the binding constraint.
INTERMEDIATE_YEARS = tuple(range(2017, 2025))

#: Commodity rows with no detail industry of the same code, and so no entry in
#: the industry price index. Their carry factor is held at **1.0**.
#:
#: This is not a coverage gap to be filled by borrowing a neighbour's index --
#: none of the four is a produced good with a price:
#:
#: ``S00300``  Noncomparable imports
#: ``S00401``  Scrap
#: ``S00402``  Used and secondhand goods
#: ``S00900``  Rest of the world adjustment
#:
#: ``S00900`` is an accounting bridge outright. ``S00401``/``S00402`` are
#: residual flows whose value is set by what is discarded rather than by a
#: quoted price, and BEA publishes no deflator for either. Holding them at 1.0
#: says the carry has nothing to contribute on these rows, which is true.
UNPRICED_COMMODITIES = ('S00300', 'S00401', 'S00402', 'S00900')

#: #497 as written: the nominal share carried in full on its own price ratio.
#: ⚠️ **Not the default any more** -- kept as the name for what #497 specified,
#: so a caller can ask for it explicitly and the two can be scored against each
#: other. :func:`default_theta` is what the build uses.
THETA_497 = 1.0


def _require_year(year: int) -> None:
    if year not in INTERMEDIATE_YEARS:
        raise ValueError(
            f'no intermediate seed for {year}; gross output is extracted for '
            f'{INTERMEDIATE_YEARS[0]}-{INTERMEDIATE_YEARS[-1]}'
        )


@functools.cache
def _use_sut_detail_2017() -> pd.DataFrame:
    """The published 2017 detail Use SUT workbook sheet, million USD."""
    return _load_2017_detail_supply_use_usa('Use_SUT_detail')


def benchmark_intermediate() -> pd.DataFrame:
    """The 2017 detail Use SUT interior, commodity x industry, USD.

    ⚠️ **The seven negative cells are kept.** They are in the published table
    and they are not errors; clipping them would make the seed disagree with its
    own source on the one property that distinguishes a dollar matrix from a
    reconstructed one.
    """
    workbook = _use_sut_detail_2017()
    missing_rows = [c for c in USA_2017_COMMODITY_CODES if c not in workbook.index]
    missing_columns = [i for i in USA_2017_INDUSTRY_CODES if i not in workbook.columns]
    if missing_rows or missing_columns:
        raise KeyError(
            f'2017 detail Use SUT is missing {len(missing_rows)} commodity rows '
            f'{missing_rows[:5]} and {len(missing_columns)} industry columns '
            f'{missing_columns[:5]}'
        )
    interior = workbook.reindex(
        index=list(USA_2017_COMMODITY_CODES),
        columns=list(USA_2017_INDUSTRY_CODES),
    ).astype(float)
    interior.index.name = 'commodity'
    interior.columns.name = 'industry'
    return interior * MILLION_CURRENCY_TO_CURRENCY


def commodity_price_factor(year: int, base: int = SEED_YEAR) -> pd.Series:
    """``p_c(year) / p_c(base)`` on the 402 detail commodity rows.

    The industry price index read commodity-for-commodity; see the module
    docstring. :data:`UNPRICED_COMMODITIES` come back as exactly 1.0.
    """
    price_index = derive_industry_price_index()
    price_index.index = price_index.index.astype(str)
    for needed in (year, base):
        if needed not in price_index.columns:
            raise ValueError(
                f'no price index for {needed}; available '
                f'{int(price_index.columns.min())}-{int(price_index.columns.max())}'
            )
    now = price_index[year].reindex(list(USA_2017_COMMODITY_CODES))
    then = price_index[base].reindex(list(USA_2017_COMMODITY_CODES))
    factor = now / then.where(then != 0, np.nan)
    factor = factor.replace([np.inf, -np.inf], np.nan).fillna(1.0)
    unexpected = [
        code for code in factor.index[now.isna()] if code not in UNPRICED_COMMODITIES
    ]
    if unexpected:
        raise KeyError(f'no price index for priced commodities: {unexpected}')
    factor.index.name = 'commodity'
    return factor.astype(float)


#: The summary Supply workbook the margin leg reads, for **every** year.
#:
#: ``_load_usa_summary_sut`` pins the vintage by year -- 2017-2022 to the legacy
#: workbook, 2023-2024 to the current one -- so that BEA's revisions do not move
#: published FBAs. That is right for an FBA and wrong for a *ratio* of two years,
#: which would otherwise take its numerator and denominator off different
#: vintages. Measured: the two vintages disagree by a median 0.50pp on 2020's
#: margin rates and 1.20pp on 2022's. ✅ They agree **exactly** on 2017 -- BEA
#: does not revise the benchmark year -- so reading one vintage throughout costs
#: nothing at the base and removes the seam at the target.
SUMMARY_SUPPLY_VINTAGE = 'Supply_Tables_1997-2024_Summary.xlsx'

#: The years :data:`SUMMARY_SUPPLY_VINTAGE` carries a sheet for, and so the years
#: the margin leg of the deflator exists at all. 1997-2024, contiguous.
#:
#: ⚠️ **This is a separate constraint from :data:`INTERMEDIATE_YEARS`**, which is
#: bounded by gross output. They happen to agree at the right-hand end today, so
#: nothing is blocked; a 2025 build (#707) would reach a year with a gross output
#: parquet and no published Supply table, and :func:`margin_rate_factor` refuses
#: rather than carrying a stale or silently-1.0 margin rate into the deflator.
MARGIN_YEARS = tuple(range(1997, 2025))

#: The valuation columns of a Supply table: basic, margins, net product taxes.
#: ``T016 = T013 + T014 + T015``, so ``T014`` is the margins alone rather than a
#: running subtotal.
SUPPLY_VALUATION_COLUMNS = ('T013', 'T014', 'T015')

#: The years the fitted regime splits on: a span that starts at or before 2021
#: and ends at or after 2022 crosses the 2021-22 price surge.
PRICE_SURGE = (2021, 2022)

#: theta on a span that does **not** cross the surge, and on one that does.
#: Fitted on 78 non-nested summary spans (``--regime`` on the drift diagnostic),
#: not on the seven the build runs. See :func:`default_theta`.
THETA_OFF_SURGE = 0.75
THETA_ACROSS_SURGE = 0.0


def default_theta(year: int, base: int = SEED_YEAR) -> float:
    """The fitted exponent for a ``base -> year`` span.

    ⚠️ **theta is not a constant and it is not a function of elapsed time.**
    Fitted on all 78 summary spans with a base of 2012 or later -- non-nested,
    so span length, cumulative inflation and price dispersion are separable
    rather than all moving with the calendar -- the single best predictor is
    whether the span **crosses the 2021-22 price surge**: R^2 0.61 on that
    binary alone, against 0.14 on elapsed years and **0.014 on relative-price
    dispersion**, which was the candidate §Inflation named and which this rules
    out. Adding elapsed years to the regime binary moves its coefficient to
    0.002 and its R^2 not at all.

    Off the surge theta fits 0.755 and here rounds to :data:`THETA_OFF_SURGE`;
    across it 0.141, and here rounds to :data:`THETA_ACROSS_SURGE` -- a frozen
    ``A`` -- rather than to the seven target spans' own fitted values, which run
    to -0.50. Two reasons for rounding up to zero: a negative theta says nominal
    shares move *against* their own price, which is not a mechanism anyone has
    proposed; and it buys almost nothing, because on surge-crossing spans the
    median gain of the best theta over a frozen ``A`` is **0.59%** of the score
    against **5.44%** off the surge. ⚠️ **In the regime this build targets the
    carry is worth well under one percent however theta is set** -- which is the
    real finding, and the reason not to fit it harder.

    ✅ **The one detail span is consistent**: 2012 -> 2017 does not cross the
    surge and fits 1.00 against the rule's 0.75 -- the right side, on a panel
    the rule was not fitted on.
    """
    crosses = base <= PRICE_SURGE[0] and year >= PRICE_SURGE[1]
    return THETA_ACROSS_SURGE if crosses else THETA_OFF_SURGE


@functools.cache
def _summary_supply(year: int) -> pd.DataFrame:
    """One year's sheet of the summary Supply SUT, indexed by commodity code.

    Read off :data:`SUMMARY_SUPPLY_VINTAGE` for every year rather than through
    ``_load_usa_summary_sut``; see there for why.
    """
    supply = load_from_gcs(
        name=SUMMARY_SUPPLY_VINTAGE,
        sub_bucket=GCS_USA_SUP_DIR,
        local_dir=LOCAL_USA_SUP_DIR,
        loader=lambda pth: pd.read_excel(
            pth, sheet_name=str(year), skiprows=5, dtype={'Unnamed: 0': str}
        ),
    )
    supply = supply.set_index(supply.columns[0])
    supply.index = supply.index.astype(str).str.strip()
    supply.columns = supply.columns.astype(str).str.strip()
    return supply


def margin_rate(valuation: pd.DataFrame) -> pd.Series:
    """``mu_c = T014 / (T013 + T015)``: margins over **producer** value.

    ⚠️ **The denominator is producer value, not basic value.** BEA gross output
    is at producers' prices, so the price index this factor multiplies already
    carries the product-tax layer; dividing by ``T013`` alone would double-count
    that wedge -- a median 3.3% overstatement, and worst on exactly the rows
    that matter here (``315AL`` apparel 1.372 against 1.793).
    """
    parts = valuation.reindex(columns=list(SUPPLY_VALUATION_COLUMNS)).apply(
        pd.to_numeric, errors='coerce'
    )
    producer = parts['T013'] + parts['T015']
    rate = parts['T014'] / producer.where(producer != 0, np.nan)
    rate.index.name = 'commodity'
    return rate


def _require_margin_year(year: int) -> None:
    """Refuse a year the Supply vintage does not publish.

    ⚠️ **Silence would be the dangerous answer here.** A missing sheet that fell
    through to a factor of 1.0 would look exactly like "margins did not move",
    and a stale rate carried from the last published year would look like a
    measurement. Both would be invisible in the built block. So this raises, and
    the caller either waits for BEA or passes ``margins=False`` and says in
    writing that the deflator is the producer-price one.
    """
    if year not in MARGIN_YEARS:
        raise ValueError(
            f'no margin rate for {year}: {SUMMARY_SUPPLY_VINTAGE} publishes '
            f'{MARGIN_YEARS[0]}-{MARGIN_YEARS[-1]}. Pass margins=False to carry '
            f'on the producer price ratio alone, which is #497 as written and a '
            f'wrong deflator for a purchaser-valued cell.'
        )


def summary_margin_rate(year: int) -> pd.Series:
    """``mu_c`` on the BEA summary commodities, annually."""
    _require_margin_year(year)
    supply = _summary_supply(year)
    rows = [r for r in supply.index if r != 'IOCode' and not r.startswith('T0')]
    return margin_rate(supply.reindex(rows)).dropna(how='all')


def detail_margin_rate(year: USA_BENCHMARK_DETAIL_SUT_YEARS = SEED_YEAR) -> pd.Series:
    """``mu_c`` on the 402 detail commodities, benchmark years only.

    BEA publishes the Supply table at detail for 2007, 2012 and 2017 and at
    summary every year, which is the whole reason :func:`margin_rate_factor`
    takes its *level* from here and its *movement* from the summary parent.
    """
    supply = _load_benchmark_detail_supply_use_usa('Supply_detail', year)
    supply.columns = supply.columns.astype(str).str.strip()
    return margin_rate(supply.reindex(list(USA_2017_COMMODITY_CODES)))


@functools.cache
def _detail_to_summary_commodity() -> pd.Series:
    """Each detail commodity's summary parent."""
    mapping = {
        str(code): (parents[0] if isinstance(parents, list) else str(parents))
        for code, parents in load_bea_v2017_commodity_to_bea_v2017_summary().items()
    }
    parents = pd.Series(
        {code: mapping.get(code) for code in USA_2017_COMMODITY_CODES}, dtype=object
    )
    missing = list(parents.index[parents.isna()])
    if missing:
        raise KeyError(f'detail commodities with no summary parent: {missing}')
    parents.index.name = 'commodity'
    return parents


def margin_rate_factor(year: int, base: int = SEED_YEAR) -> pd.Series:
    """``(1 + mu_c(year)) / (1 + mu_c(base))`` on the 402 detail commodity rows.

    The margin leg of a *purchaser*-price deflator. A cell of this block is at
    purchaser value, so its price movement is the purchaser one; the industry
    price index supplies the producer leg and this supplies what is left.

    ⚠️ **The rate's level is detail-observed and only its movement is borrowed.**
    ``mu_c(year) = mu_c(base) * mu_parent(year) / mu_parent(base)`` -- the
    benchmark detail Supply table gives every commodity its own rate at
    ``base``, and the summary parent gives the annual movement, because detail
    Supply is published only for benchmark years. ✅ **Scored on the one span
    where both are observed** (2012, through the S0a panel), against the true
    detail factor and weighted by 2017 intermediate dollars: this rule is
    **0.756pp** off, taking the parent's factor down unchanged is **1.010pp**,
    and applying no factor at all is **1.818pp**. So the rule recovers about
    three-fifths of the movement and the simpler one about two-fifths.

    ⚠️ **Applied only to margin-receiving commodities.** For a trade or
    transport commodity ``T014`` is large and negative -- its margin is
    allocated away onto the goods it carries -- so ``mu`` runs to -0.94
    (``42``), -0.99 (``486``), and ``1 + mu`` is a near-zero denominator. Those
    rows are held at exactly 1.0. This costs almost nothing: in the
    purchaser-priced Use table they carry almost no intermediate dollars,
    precisely because their margins are sitting inside the goods rows.

    ⚠️ **This is a correctness fix, not a repair for theta.** It moves the carry
    factor a median 0.35pp on the receiving rows and it does **not** pull theta
    toward the detail panel's 1.00 -- it leaves it unmoved in six years of seven
    (§It was a competing explanation for theta).
    """
    now, then = _detail_margin_rate(year), _detail_margin_rate(base)
    factor = (1.0 + now) / (1.0 + then)
    receiving = (then > 0) & (now > -1)
    factor = factor.where(receiving, 1.0).replace([np.inf, -np.inf], 1.0).fillna(1.0)
    factor.index = pd.Index(list(USA_2017_COMMODITY_CODES), name='commodity')
    return factor.astype(float)


def _detail_margin_rate(year: int) -> pd.Series:
    """``mu_c`` at detail for any year: the benchmark level on the parent's movement.

    Exactly :func:`detail_margin_rate` at :data:`SEED_YEAR`, and elsewhere that
    same per-commodity level scaled by how much its summary parent's rate moved.
    """
    anchor = detail_margin_rate(SEED_YEAR)
    if year == SEED_YEAR:
        return anchor
    parents = _detail_to_summary_commodity()
    now = summary_margin_rate(year).reindex(parents.to_numpy()).to_numpy()
    then = summary_margin_rate(SEED_YEAR).reindex(parents.to_numpy()).to_numpy()
    movement = pd.Series(now / np.where(then == 0, np.nan, then), index=anchor.index)
    return anchor * movement


def commodity_deflator(
    year: int, base: int = SEED_YEAR, margins: bool = True
) -> pd.Series:
    """The purchaser-price ratio the column shares are carried on.

    ``[producer price ratio] x [margin-rate factor]``, per §Margins.2. Passing
    ``margins=False`` returns #497 as written -- the producer leg alone, which
    is the wrong deflator for a purchaser-valued cell and is kept only so the
    two can be scored against each other.
    """
    if margins:
        _require_margin_year(year)
        _require_margin_year(base)
    factor = commodity_price_factor(year, base)
    if margins:
        factor = factor * margin_rate_factor(year, base)
    factor.index.name = 'commodity'
    return factor.astype(float)


def vapro(year: int) -> pd.Series:
    """``VAPRO`` by industry, USD -- observed, from BEA's ``UVA205-A``.

    :mod:`bedrock.transform.iot.derived_intermediate_and_value_added` allocates
    BEA's annual value added by underlying industry down to the 402 detail
    industries. Its line totals reproduce ``UVA205-A`` exactly and its 2017
    column reproduces the published detail ``VAPRO`` to 0.89 million USD, so
    this is a read rather than a seed.
    """
    _require_year(year)
    industries = list(USA_2017_INDUSTRY_CODES)
    observed = derive_detail_value_added(year).reindex(industries)
    if observed.isna().any():
        missing = list(observed.index[observed.isna()])
        raise KeyError(f'no {year} value added for industries: {missing}')
    observed.index.name = 'industry'
    return observed * MILLION_CURRENCY_TO_CURRENCY


def intermediate_column_control(year: int) -> pd.Series:
    """``T005 = GO_producer - VAPRO`` by industry, USD.

    The column total the seeded block is scaled to. Read off
    :func:`~bedrock.transform.iot.derived_intermediate_and_value_added
    .derive_detail_intermediate_inputs` rather than differenced here, so the
    control and :func:`vapro` satisfy T1 to the floating-point bit.
    """
    _require_year(year)
    industries = list(USA_2017_INDUSTRY_CODES)
    control = derive_detail_intermediate_inputs(year).reindex(industries)
    if control.isna().any():
        missing = list(control.index[control.isna()])
        raise KeyError(f'no {year} intermediate inputs for industries: {missing}')
    control.index.name = 'industry'
    return control * MILLION_CURRENCY_TO_CURRENCY


def carry_shares(seed: pd.DataFrame, factor: pd.Series, theta: float) -> pd.DataFrame:
    """A dollar block's column shares moved on ``factor ** theta``, renormalised.

    The whole of what this step estimates, with no data-loading in it, which is
    why the wiring lives in :func:`carried_column_shares` and the arithmetic
    lives here.

    ``share[c, j] * factor[c] ** theta``, each column then divided by its own
    total. ``theta = 0`` returns the seed's shares untouched -- the frozen-``A``
    comparison every measurement in the plan is scored against.

    ⚠️ **Signs survive.** The factor is positive, so a negative seed cell stays
    negative and keeps its magnitude relative to the column.

    ⚠️ **An empty column and a cancelling one are not the same thing.** A column
    of all zeros -- ``4200ID`` customs duties and ``814000`` private households,
    which buy no intermediates -- has no structure to normalise and comes back
    all-zero. A column with real cells that happen to sum to zero *does* have
    structure and cannot be expressed as shares of it, so it raises rather than
    being flattened to the same all-zero answer.
    """
    populated = (seed != 0).any(axis=0)
    totals = seed.sum(axis=0)
    cancelling = list(totals.index[populated & (totals == 0)])
    if cancelling:
        raise ValueError(
            'seed columns have nonzero cells summing to zero, so they cannot be '
            f'renormalised into shares: {cancelling}'
        )
    live = populated
    carried = (seed.loc[:, live] / totals[live]).mul(
        factor.reindex(seed.index) ** theta, axis=0
    )
    renormalised = carried.sum(axis=0)
    degenerate = list(renormalised.index[renormalised.abs() < 1e-12])
    if degenerate:
        raise ValueError(
            'carried shares sum to zero for these industries, so the column '
            f'cannot be renormalised: {degenerate}'
        )
    out = pd.DataFrame(0.0, index=seed.index, columns=seed.columns)
    out.loc[:, live] = carried / renormalised
    out.index.name = 'commodity'
    out.columns.name = 'industry'
    return out


def composed_seed(year: int) -> pd.DataFrame:
    """The 2017 benchmark with each graded survey seed overlaid on its own cells.

    ``commodity x industry`` in USD, the same shape as
    :func:`benchmark_intermediate`, which is what this replaces as the thing
    :func:`carry_shares` starts from.

    **Only the shape is taken.** Every seed returns BEA's own 2017 cells moved
    on a survey *index*, and the block is rescaled again to ``GO - VAPRO`` in
    :func:`apply_column_control`. So the dollar level here is never the estimate
    - :func:`intermediate_column_control` is - and a seed changes *how a column
    divides*, nothing else.

    ==================  =======  =======================================
    block               columns  source
    ==================  =======  =======================================
    manufacturing           232  Economic Census materials, then the
                                 survey index on the non-materials cells
    services/transport      100  ``Census_SAS_Expenses`` / AIES
    agriculture              10  ERS
    ==================  =======  =======================================

    The remaining 60 columns hold their 2017 structure, which says nothing
    observes their movement rather than that none happened.

    ⚠️ **The seeds are overlaid cell-wise, never added.** ``materials_seed``
    returns the *whole* manufacturing column, renormalised to its 2017 total;
    ``nonmaterial_seed`` returns only the **23 non-materials rows**. Adding them
    double-counts those rows - it put ``334111`` 55% above the benchmark at 2017,
    where every seed must be the identity. Non-materials is written second and
    wins on the rows it covers, because there its movement is the survey index
    while ``materials_seed``'s is only the column renormalisation.

    ⚠️ **Rows a seed does not carry keep their benchmark value**, which is why
    each overlay is assigned on ``seed.index`` rather than reindexed to 402.
    Reindexing filled the uncovered rows with ``NaN`` and made the grand total
    ``NaN``.

    ⚠️ **Trade is deliberately absent.** ``trade_expense_supplement.trade_seed``
    exists and is a **no-go**: graded on the benchmark holdout it is a wash, and
    it tracks where ``N`` is not. It is the seed most likely to be wired in by
    reflex, because it is built and it imports.

    ⚠️ **Utilities is absent because no seed builder exists.** EIA 923 graded
    **GO** (+16.0% on ``N``, 3 of 3 columns) but ``utilities_expense_seed`` stops
    at the grading and never produced a ``commodity x industry`` block, so those
    three columns still hold 2017.

    ⚠️ **``ore_seed`` is not applied separately** - ``531ORE`` is one of the 100
    columns ``services_transport_seed`` already moves, and applying both would
    index that column twice.

    ⚠️ **At 2017 every seed is the identity**, verified against the benchmark on
    each seed's own rows, so :func:`reproduction_check` still measures the column
    rescale alone.
    """
    # Deferred: inputs_structure imports this module, so a module-level import
    # here is a cycle. Same reason nowcast_va_taxes defers its analysis imports.
    from bedrock.analysis.nowcasting.agriculture_expense_seed import (  # noqa: PLC0415
        agriculture_seed,
    )
    from bedrock.analysis.nowcasting.inputs_structure import (  # noqa: PLC0415
        materials_seed,
        nonmaterial_seed,
    )
    from bedrock.analysis.nowcasting.services_transport_expense_seed import (  # noqa: PLC0415, E501
        services_transport_seed,
    )

    base = benchmark_intermediate()
    # Ordered: within manufacturing, non-materials is written after materials and
    # so wins on the 23 rows they share.
    overlays: list[tuple[str, pd.DataFrame]] = [
        ('manufacturing', materials_seed(year)),
        ('manufacturing', nonmaterial_seed(year)),
        ('services/transport', services_transport_seed(year)),
        ('agriculture', agriculture_seed(year)),
    ]

    claimed: dict[str, str] = {}
    for block, overlay in overlays:
        columns = [c for c in overlay.columns if c in base.columns]
        clash = sorted(c for c in columns if claimed.get(c, block) != block)
        if clash:
            raise ValueError(
                f'{block} and {claimed[clash[0]]} both seed {clash}, which would '
                f'index those columns twice. The blocks are meant to partition '
                f'the industries they reach.'
            )
        claimed.update({c: block for c in columns})
        rows = [r for r in overlay.index if r in base.index]
        # The seeds are in $M and the benchmark in USD. Only column shares are
        # read downstream so the units would cancel, but a frame carrying two
        # units is a trap for the next reader.
        base.loc[rows, columns] = (
            overlay.loc[rows, columns].astype(float) * MILLION_CURRENCY_TO_CURRENCY
        )
    return base


def carried_column_shares(
    year: int, theta: float | None = None, margins: bool = True
) -> pd.DataFrame:
    """:func:`carry_shares` on the 2017 benchmark and this year's deflator.

    ``theta`` defaults to :func:`default_theta` for the span, and ``margins``
    to the full purchaser deflator; ``theta=THETA_497, margins=False`` is #497
    as written.
    """
    exponent = default_theta(year) if theta is None else theta
    return carry_shares(
        composed_seed(year), commodity_deflator(year, margins=margins), exponent
    )


def apply_column_control(shares: pd.DataFrame, control: pd.Series) -> pd.DataFrame:
    """Scale each column of a share matrix to its control total.

    ⚠️ **An all-zero column cannot absorb a control.** ``4200ID`` and ``814000``
    have no 2017 structure, so a control that puts real dollars on them has
    nowhere to spread them and is refused rather than silently dropped. The
    threshold is the workbook's own $1M grain.
    """
    control = control.reindex(shares.columns)
    if control.isna().any():
        missing = list(control.index[control.isna()])
        raise KeyError(f'column control is missing industries: {missing}')
    empty = shares.abs().sum(axis=0) == 0
    stranded = control[empty & (control.abs() > MILLION_CURRENCY_TO_CURRENCY)]
    if not stranded.empty:
        raise ValueError(
            'column control assigns intermediate dollars to industries with no '
            f'2017 structure to spread them over: {stranded.to_dict()}'
        )
    block = shares.mul(control, axis=1)
    block.index.name = 'commodity'
    block.columns.name = 'industry'
    return block


def derive_intermediate_use(
    year: int,
    theta: float | None = None,
    column_control: pd.Series | None = None,
    margins: bool = True,
) -> pd.DataFrame:
    """The Step 3 intermediate block, commodity x industry, USD, purchaser price.

    Passing ``column_control`` injects the column totals rather than reading
    :func:`intermediate_column_control`, which is what makes this runnable
    without the gross-output parquet -- a pipeline artefact that is not in the
    repository -- and what lets a better-sourced control be swapped in.

    At ``year = 2017`` the carry factors are all 1.0 and the only move left is
    the column rescale, which reproduces the published interior to BEA's own
    publication rounding -- see :func:`reproduction_check` for exactly how much
    that is and why it is not zero.
    """
    _require_year(year)
    control = (
        intermediate_column_control(year) if column_control is None else column_control
    )
    return apply_column_control(
        carried_column_shares(year, theta, margins=margins), control
    )


def reproduction_check(theta: float | None = None) -> pd.Series:
    """How exactly the 2017 build reproduces the published 2017 interior.

    The plumbing test, not a test of the movement: at 2017 every carry factor is
    1.0, so anything left is the column rescale.

    ⚠️ **The residual is BEA's own rounding, and it does not vanish.** Published
    ``T005`` is one rounded number; the interior is 402 separately rounded cells
    summing to a different one. The gap is $350M on $14.9T economy-wide and at
    most $13M on any one column, but a *small* column wears it as a large
    fraction -- ``334610`` is $482M of intermediates and carries $6M of it, so
    the largest **relative** cell error is 1.05% while the largest **absolute**
    one is $6.0M, on a $19.2B cell. Both are reported, because either alone
    reads as the wrong kind of error.
    """
    built = derive_intermediate_use(SEED_YEAR, theta=theta)
    published = benchmark_intermediate()
    error = (built - published).abs()
    rescale = intermediate_column_control(SEED_YEAR) / published.sum(axis=0).replace(
        0, np.nan
    )
    relative = (error / published.abs().where(published.abs() > 0)).replace(
        [np.inf, -np.inf], np.nan
    )
    return pd.Series(
        {
            'max_column_rescale': float((rescale - 1).abs().max()),
            'max_absolute_cell_error_usd': float(error.to_numpy().max()),
            'max_relative_cell_error': float(relative.max().max()),
            'negative_cells': float((built.to_numpy() < 0).sum()),
        }
    )


__all__ = [
    'INTERMEDIATE_YEARS',
    'MARGIN_YEARS',
    'PRICE_SURGE',
    'SEED_YEAR',
    'SUMMARY_SUPPLY_VINTAGE',
    'SUPPLY_VALUATION_COLUMNS',
    'THETA_497',
    'THETA_ACROSS_SURGE',
    'THETA_OFF_SURGE',
    'UNPRICED_COMMODITIES',
    'apply_column_control',
    'benchmark_intermediate',
    'carried_column_shares',
    'carry_shares',
    'commodity_deflator',
    'commodity_price_factor',
    'default_theta',
    'derive_intermediate_use',
    'detail_margin_rate',
    'intermediate_column_control',
    'margin_rate',
    'margin_rate_factor',
    'reproduction_check',
    'summary_margin_rate',
    'vapro',
]
