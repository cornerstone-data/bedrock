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
fits 1.00, so the disagreement is regime rather than code. This module
therefore takes ``theta`` as an argument and defaults to #497's 1.0; choosing it
is `#699 <https://github.com/cornerstone-data/bedrock/issues/699>`_, together
with the margin-rate leg of the deflator, which is **not** applied here.

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

from bedrock.extract.iot.io_2017 import _load_2017_detail_supply_use_usa
from bedrock.transform.iot.derived_intermediate_and_value_added import (
    derive_detail_intermediate_inputs,
    derive_detail_value_added,
)
from bedrock.transform.iot.derived_price_index import derive_industry_price_index
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES

#: The benchmark the structure is seeded from.
SEED_YEAR = 2017

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
#: See the module docstring for why this is a default rather than a finding.
DEFAULT_THETA = 1.0


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


def carry_shares(
    seed: pd.DataFrame, factor: pd.Series, theta: float = DEFAULT_THETA
) -> pd.DataFrame:
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


def carried_column_shares(year: int, theta: float = DEFAULT_THETA) -> pd.DataFrame:
    """:func:`carry_shares` on the 2017 benchmark and this year's price factor."""
    return carry_shares(benchmark_intermediate(), commodity_price_factor(year), theta)


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
    theta: float = DEFAULT_THETA,
    column_control: pd.Series | None = None,
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
    return apply_column_control(carried_column_shares(year, theta), control)


def reproduction_check(theta: float = DEFAULT_THETA) -> pd.Series:
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
    'DEFAULT_THETA',
    'INTERMEDIATE_YEARS',
    'SEED_YEAR',
    'UNPRICED_COMMODITIES',
    'apply_column_control',
    'benchmark_intermediate',
    'carried_column_shares',
    'carry_shares',
    'commodity_price_factor',
    'derive_intermediate_use',
    'intermediate_column_control',
    'reproduction_check',
    'vapro',
]
