"""Nowcast Supply/Use mask: which cells the balance may not move.

The generic machinery is :mod:`bedrock.utils.economic.balance.mask`; this
module is the *sourcing* - which cells of the US detail SUT go in each layer,
and why.

The rule
--------

    **Mask a cell only if the source reports that cell. If the source reports
    the margin, it belongs to the target set instead. Never both.**

A source spent on a cell cannot also be spent on a margin. This is what rules
out the obvious-looking move of freezing the whole final-demand block: ``PCE``
and equipment reproduce their BEA bridges cell for cell, but a bridge is a
**2017 commodity split applied to a current-year NIPA line**. The line total is
observed; the split is an assumption, and it is exactly the assumption the
balance exists to correct. Freeze it and the nowcast can never learn that the
commodity mix moved.

The tiers
---------

**Tier 0 - structural zeros, everywhere.** The 2017 sparsity pattern per block.
Free under either engine, since diagonal scaling cannot make a zero nonzero.

⚠️ **State the assumption out loud.** The Supply block's commodity x industry
core is only **3.2% dense** on the balance's labels, so a structural-zero mask
there asserts *no industry produces a commodity it did not produce in 2017*.
That is a modelling choice, not housekeeping.

**Tier 1 - fixed values, hard.** Cells where one NIPA line lands on one
commodity: :data:`ONE_TO_ONE_FD`. **17 cells, 5.1% of the Use panel's mass**,
costing 5 commodity rows their Use-side freedom and 6 columns all of theirs.
``F06C00`` and ``F07C00`` carry exactly one nonzero commodity row; the other
four carry three or four. For these, masking the cells and targeting the column
total are the same constraint written twice - so **these six columns leave the
target set** when they enter the mask.

**Tier 2 - not masked.** ``F01000``, ``F02E00``, ``F02N00``, ``F02R00``,
``F02S00``, ``F03000``, ``F04000`` and the government equipment/structures
columns. Their totals are targets; their splits stay free.

**Tier 3 - sign locks, not masks.** :data:`SIGN_LOCKED_SUPPLY_COLUMNS` and
:data:`SIGN_LOCKED_USE_ROWS`. These cells need to move; they must not cross
zero. Measured on 2017: ``SUB`` 15/15 negative, ``MADJ`` 6/6 negative, ``TOP``
339/339 non-negative, and the margin give-up side showing 19 negative ``TRADE``
and 5 negative ``TRANS``. Locks are taken from the published sign per cell
rather than asserted per column, so the give-up side is handled without a
special case.

⚠️ ``V00300`` is **not** locked. Gross operating surplus is legitimately
negative for one industry in 2017, and it is the residual the whole system
lands on.

**Tier 4 - held out of the balance, on the commodity axis only.**
:data:`EXCLUDED_COMMODITIES`. ``S00900``'s Use row is 100% final demand
($405,436, all exports) against $3,494 of Supply-side freedom - a joint free
share of 0.9% - and it is already derived from an identity
(``-F010 + Supply T016``), so it is re-derived after the balance rather than
asked to move inside it. ``4200ID`` produces nothing, so its commodity row is
empty.

⚠️ **A code can mean different things on the two axes, and ``4200ID`` does.**
It is empty as a *commodity* but live as an *industry*: customs duties, buying
no intermediates and making no commodity, yet carrying the duty as
``T00TOP`` = ``VAPRO`` = 38,513 in 2017 - the Supply ``MDTY`` column total to
rounding, and exactly its published detail gross output. So it is excluded
from :func:`balance_commodities` and **kept** in :func:`balance_industries`.
Excluding it from both drops a $38.5B hard constraint.

The two axes are not the same set
---------------------------------

**The panel is not square, and nothing may assume it is.** 398 codes appear in
both lists; four are industry-only and four commodity-only:

- **industry-only** - ``331314``, ``S00101``, ``S00201``, ``S00202``. Live
  industries that produce other commodities, carrying 5,100 to 33,922 of
  intermediate purchases each.
- **commodity-only** - ``S00300``, ``S00401``, ``S00402``, ``S00900``.

⚠️ **Two commodities have no domestic make at all**, so their entire Supply
side is bridge columns and a structural-zero mask freezes their make row
completely:

- ``S00300`` noncomparable imports - 260,421, **all** ``MCIF``. Nothing
  domestic produces it; it arrives as an import.
- ``S00402`` used and secondhand goods - 164,495, of which 117,563 is
  ``TRADE``. Used goods reach buyers through margins rather than production,
  which is why its margin rate on basic value looks impossible.

That is not infeasible - both keep ample bridge-column freedom - but it means
leverage on those two rows must be read on the bridge, not on the make.

Sign convention
---------------

The balance stores subsidies **negative**, matching the Supply table. BEA
publishes the Use ``T00SUB`` row positive, so :func:`published_2017_panel`
negates it on the way in and asserts the result. Unnormalised, a
producer-price column margin is wrong by ``2 x T00SUB``.

Years after 2017
----------------

There is no published detail SUT for 2018-2024 - building it is the point - so
every layer here is derived from the 2017 pattern regardless of ``year``. The
``year`` parameter records intent rather than changing the answer today.

⚠️ **Open: does the Tier-1 mask hold for
2018-2024?** The 1:1 line-to-commodity mapping is asserted from the 2017
crosswalks; whether the government consumption and IP columns stay
single-commodity across the window is unconfirmed.
"""

from __future__ import annotations

import functools
from typing import Literal

import numpy as np
import pandas as pd

from bedrock.extract.iot.io_2017 import _load_2017_detail_supply_use_usa
from bedrock.utils.economic.balance.mask import SutMask, assert_subsidies_negative
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_final_demand import SUT_FINAL_DEMAND_CODES
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES

Block = Literal['use', 'supply']

#: Value-added rows of the Use panel. ``T00TOP`` and ``T00SUB`` are not part of
#: the industry-output identity - they are the wedge from basic to producer
#: prices - but the industry column margin is ``T005 + VAPRO``, so all five rows
#: participate.
#:
#: ⚠️ **One row behind the block on purpose** (#784):
#: ``nowcast.USE_VALUE_ADDED_ROWS`` also carries ``T00OSUB`` — subsidies on
#: production, all-zero at 2017 and ~580bn $M in 2020. Wiring it here needs a
#: Tier-0 zero exemption on the row axis (its 2017 zeros are one year's
#: observation of a pandemic-era row, not structure) plus a sign lock, and the
#: mask machinery is about to be reworked as an early Step-5 item — the row
#: joins then, once, rather than twice.
VA_ROWS = ('V00100', 'T00OTOP', 'V00300', 'T00TOP', 'T00SUB')

#: Supply columns between basic (``T013``) and purchaser (``T016``) value. The
#: subtotals ``T007``/``T013``/``T014``/``T015``/``T016`` are derived and are
#: deliberately absent - the balance solves for the components. The trailing
#: space on ``TRADE`` is BEA's, in the published workbook.
SUPPLY_BRIDGE_COLUMNS = ('MCIF', 'MADJ', 'TRADE ', 'TRANS', 'MDTY', 'TOP', 'SUB')

#: Tier 1. Final-demand columns whose NIPA source line lands on one commodity,
#: or close to it. These leave the target set when they enter the mask.
ONE_TO_ONE_FD = ('F06C00', 'F07C00', 'F10C00', 'F06N00', 'F07N00', 'F10N00')

#: Tier 4, **commodity axis only**. ``S00900`` is derived from an identity and
#: has almost no freedom; ``4200ID`` produces nothing, so its commodity row and
#: its Supply row are both empty.
#:
#: ⚠️ ``4200ID`` is **not** excluded as an *industry* - see
#: :func:`balance_industries`. It is a code that means different things on the
#: two axes, and dropping it from both was a bug.
EXCLUDED_COMMODITIES = ('S00900', '4200ID')

#: Tier 0 exemptions. Columns whose 2017 zeros are **not** structural, because
#: what a country trades changes from year to year.
#:
#: ⚠️ A structural zero here would assert that a commodity the US did not
#: import in 2017 can never be imported, and that one it did not export cannot
#: be exported. That is false as a matter of trade rather than arguable as a
#: modelling choice: 2017's zero is one year's observation of a flow that moves.
#: Measured on the 2017-seeded panel, holding these zeros made
#: ``split_fixed_blocks`` raise on 20 Supply cells - ``339116 x MCIF`` at $522M,
#: ``33211A x MCIF`` at $146M and 18 small ``MDTY`` cells - and on
#: ``213111 x F04000`` at $771M on the Use side.
#:
#: Margin and tax columns are deliberately **not** exempt: their zeros mean a
#: commodity bears no margin or no tax, which is a property of the commodity,
#: and both columns have to net to zero for T15/T16. See #749 for the wider
#: review of which Tier 0 zeros are justified.
TRADE_FLOW_SUPPLY_COLUMNS = ('MCIF', 'MADJ', 'MDTY')

#: Tier 0 exemption, Use side. Exports, for the same reason.
TRADE_FLOW_USE_COLUMNS = ('F04000',)

#: Commodities whose ``MCIF`` zero is **structure, not observation** - the
#: wholesale and retail margin commodities, and customs duties.
#:
#: ✅ A wholesaler's or retailer's output *is* a trade margin, and a margin is
#: not a thing that crosses a border: it is earned domestically on a good that
#: may itself be imported, and the import is booked against that good. So these
#: cannot be imported in any year, and the exemption above must not free them.
#: All 20 carry a published 2017 ``MCIF`` of exactly zero, and 19 of them carry
#: a large negative ``TRADE`` - they are the margin-*giving* side of the table.
NEVER_IMPORTED_TRADE_COMMODITIES = (
    '423100',
    '423400',
    '423600',
    '423800',
    '423A00',
    '424200',
    '424400',
    '424700',
    '424A00',
    '425000',
    '441000',
    '444000',
    '445000',
    '446000',
    '447000',
    '448000',
    '452000',
    '454000',
    '4B0000',
    '4200ID',
)

#: Transport commodities BEA publishes at a zero ``MCIF``.
#:
#: ⚠️ **Weaker than the trade set, and a different kind of claim.** Water and
#: rail freight genuinely are traded - IEA publishes them - so this is BEA's
#: routing convention rather than an impossibility, and the tell is that the
#: modes are *not* uniform: ``481000`` air carries 46,393, ``491000`` postal
#: 289 and ``492000`` couriers 44, while these seven are exactly zero. Held
#: because reproducing BEA is the job, but it should be confirmed against BEA's
#: own treatment rather than inferred from one year - see #751.
NEVER_IMPORTED_TRANSPORT_COMMODITIES = (
    '482000',
    '483000',
    '484000',
    '485000',
    '486000',
    '48A000',
    '493000',
)

#: The guard that replaces the structural zero on ``MCIF`` (#749, #751). Freeing
#: the whole column would let import mass land on a commodity that cannot be
#: imported; freeing its complement keeps the observations free and the
#: structure fixed.
#:
#: ✅ **The build is clean on this today** - ``Trade_Imports_2017`` puts nothing
#: on any of the 27. That is worth stating because it is *not* what makes the
#: guard unnecessary: the build is clean because the frozen 2017 ``MCIF``
#: attribution weight is itself zero on these commodities, so the weight has
#: been doing the guard's job by accident. Any change to that weight - which is
#: exactly what #729 and #670 contemplate - removes the accident.
#:
#: ⚠️ The failure mode is already visible in a different form: ``BEA_IEA_imports``
#: maps ``TransportRoadAndOth`` **only** to ``482000``, ``484000`` and ``486000``,
#: all three of them here, so its ~$4B a year is silently dropped rather than
#: misrouted. Fixing that crosswalk without this set in place would convert a
#: drop into a misroute. See #670.
NEVER_IMPORTED_COMMODITIES = (
    NEVER_IMPORTED_TRADE_COMMODITIES + NEVER_IMPORTED_TRANSPORT_COMMODITIES
)

#: Tier 3, Supply side. Locked to their published sign per cell.
SIGN_LOCKED_SUPPLY_COLUMNS = ('MADJ', 'TRADE ', 'TRANS', 'TOP', 'SUB')

#: Tier 3, Use side. ``T00SUB`` only - after normalisation it is non-positive.
SIGN_LOCKED_USE_ROWS = ('T00SUB',)


def balance_commodities() -> tuple[str, ...]:
    """The 400 commodities the balance carries.

    402 less :data:`EXCLUDED_COMMODITIES`.
    """
    excluded = set(EXCLUDED_COMMODITIES)
    return tuple(c for c in USA_2017_COMMODITY_CODES if c not in excluded)


def balance_industries() -> tuple[str, ...]:
    """All 402 industries. Nothing is excluded on this axis.

    ⚠️ **``4200ID`` stays**, even though it is excluded as a commodity.
    ``4200ID`` is customs duties, and on the industry axis it is a
    duties-collecting industry rather than an empty code: it buys no
    intermediates and makes no commodity, but it carries the duty as
    ``T00TOP`` = ``VAPRO`` = **38,513** in 2017, which matches the Supply
    table's ``MDTY`` column total of 38,507 to rounding.

    Published detail gross output carries it at exactly 38,513, so the hard
    ``T005 + VAPRO`` target binds this column.
    Dropping it would delete a $38.5B constraint and, with it, the cleanest
    illustration of the producer-versus-basic wedge in the table -
    ``GO(producer) = T007(basic) + T00TOP - T00SUB`` is ``0 + 38,513 - 0``
    here.

    ⚠️ Its *Supply* column is entirely zero, because duties are
    not output at basic prices. A gross-output target stated at producer prices
    can therefore bind the Use column but **not** the Supply one. The mask
    leaves the column in place so the precheck surfaces that rather than
    hiding it.
    """
    return tuple(USA_2017_INDUSTRY_CODES)


def panel_labels(block: Block) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """``(rows, columns)`` for a block.

    The seed and the mask must agree on these, so they are defined once here
    and imported rather than rebuilt.
    """
    commodities = balance_commodities()
    industries = balance_industries()
    if block == 'use':
        return commodities + VA_ROWS, industries + tuple(SUT_FINAL_DEMAND_CODES)
    if block == 'supply':
        return commodities, industries + SUPPLY_BRIDGE_COLUMNS
    raise ValueError(f'block must be use or supply, got {block!r}')


def _numeric(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.apply(pd.to_numeric, errors='coerce').fillna(0.0).astype(float)


def published_2017_panel(block: Block) -> pd.DataFrame:
    """The published 2017 block, on the balance's labels and sign convention.

    Sourced for the mask, and usable as the 2017 replay seed. The value-added
    by final-demand corner of the Use panel stays zero: it is structurally
    empty and must remain so.

    Returns a **copy**. The build is cached because it costs a GCS read and a
    full re-assembly, but handing out the cached frame itself would let one
    caller's in-place edit reach every later caller - and the seed is exactly
    the kind of object callers modify.
    """
    return _build_published_2017_panel(block).copy()


@functools.cache
def _build_published_2017_panel(block: Block) -> pd.DataFrame:
    """Assemble the block once; :func:`published_2017_panel` hands out copies."""
    rows, columns = panel_labels(block)
    panel = pd.DataFrame(0.0, index=list(rows), columns=list(columns))
    commodities = list(balance_commodities())
    industries = list(balance_industries())

    if block == 'use':
        use = _load_2017_detail_supply_use_usa('Use_SUT_detail')
        final_demand = list(SUT_FINAL_DEMAND_CODES)
        panel.loc[commodities, industries] = _numeric(use.loc[commodities, industries])
        panel.loc[commodities, final_demand] = _numeric(
            use.loc[commodities, final_demand]
        )
        panel.loc[list(VA_ROWS), industries] = _numeric(
            use.loc[list(VA_ROWS), industries]
        )
        # BEA publishes this row positive; the balance stores subsidies
        # negative, matching the Supply table.
        panel.loc['T00SUB'] = -panel.loc['T00SUB']
        assert_subsidies_negative(panel, axis='row', label='T00SUB')
    else:
        supply = _load_2017_detail_supply_use_usa('Supply_detail')
        bridge = list(SUPPLY_BRIDGE_COLUMNS)
        panel.loc[commodities, industries] = _numeric(
            supply.loc[commodities, industries]
        )
        panel.loc[commodities, bridge] = _numeric(supply.loc[commodities, bridge])
        assert_subsidies_negative(panel, axis='column', label='SUB')

    return panel


def _panel_or_default(block: Block, panel: pd.DataFrame | None) -> pd.DataFrame:
    return published_2017_panel(block) if panel is None else panel


def structural_zero_mask(
    block: Block, panel: pd.DataFrame | None = None
) -> pd.DataFrame:
    """Tier 0. Cells that are zero in the pattern and must stay zero.

    On the Supply block this is the assertion that no industry produces a
    commodity it did not produce in 2017 - 3.1% density, so it is a large
    claim cheaply made.

    ⚠️ **The trade-flow columns are exempt** - :data:`TRADE_FLOW_SUPPLY_COLUMNS`
    and :data:`TRADE_FLOW_USE_COLUMNS`. A zero there is one year's observation
    of a flow that moves annually, not a property of the commodity, so freezing
    it asserts that what the US did not trade in 2017 it can never trade.

    ⚠️ **The exemption stops at :data:`NEVER_IMPORTED_COMMODITIES`.** Some zeros
    in ``MCIF`` are exactly the structure Tier 0 exists for - a trade margin
    cannot be imported - so freeing the whole column would trade one error for
    another. The complement is freed; those rows stay frozen.
    """
    values = _panel_or_default(block, panel)
    zeros = values == 0
    exempt = TRADE_FLOW_SUPPLY_COLUMNS if block == 'supply' else TRADE_FLOW_USE_COLUMNS
    present = [column for column in exempt if column in zeros.columns]
    if present:
        freed = pd.DataFrame(True, index=zeros.index, columns=present)
        if block == 'supply' and 'MCIF' in present:
            structural = [c for c in NEVER_IMPORTED_COMMODITIES if c in zeros.index]
            freed.loc[structural, 'MCIF'] = False
        zeros[present] = zeros[present] & ~freed
    return zeros


def never_imported_violations(mcif: pd.Series) -> pd.Series:
    """Imports landing where :data:`NEVER_IMPORTED_COMMODITIES` forbids them.

    The guard the mask cannot give, because a mask constrains the **balance**
    and this has to catch the **build**. Returns the offending values, largest
    first; empty when the vector is clean.

    ✅ **Clean on the 2017 build today**, so this *can* be wired as a hard
    assertion in ``derive_initial_supply_bridge`` rather than only reported.
    Kept as a query for now because the trade crosswalk work in #670 / #729 is
    in flight and will move import mass across these commodities; a gate that
    lands mid-change is a gate that gets disabled.
    """
    codes = [code for code in NEVER_IMPORTED_COMMODITIES if code in mcif.index]
    held = pd.to_numeric(mcif.reindex(codes), errors='coerce').fillna(0.0)
    offending = held[held != 0.0]
    return offending.reindex(offending.abs().sort_values(ascending=False).index)


def fixed_value_mask(
    block: Block, year: int = 2017, panel: pd.DataFrame | None = None
) -> pd.DataFrame:
    """Tier 1. Cells a source reports directly, held at their value.

    The six 1:1 final-demand columns on the Use block, and **nothing on the
    Supply block**: ``MCIF`` has a 2017 candidate and ``MDTY`` a sourced
    method, but whether either is fixed rather than targeted is not yet decided.
    An empty layer is the honest default - a
    cell masked by accident cannot be corrected by the balance.

    Only *nonzero* cells are fixed, so this never collides with Tier 0.
    """
    del year  # the pattern is 2017-derived for every year; see the module note
    values = _panel_or_default(block, panel)
    flags = pd.DataFrame(False, index=values.index, columns=values.columns)
    if block == 'use':
        present = [c for c in ONE_TO_ONE_FD if c in values.columns]
        flags[present] = values[present] != 0
    return flags


def sign_lock_mask(block: Block, panel: pd.DataFrame | None = None) -> pd.DataFrame:
    """Tier 3. Cells that may move but not across zero.

    The lock is the published sign of the cell, so the margin give-up side -
    19 negative ``TRADE`` and 5 negative ``TRANS`` cells in 2017 - is handled
    by the same rule as the rest rather than by a special case.
    """
    values = _panel_or_default(block, panel)
    locks = pd.DataFrame(0, index=values.index, columns=values.columns, dtype=int)
    if block == 'supply':
        for column in SIGN_LOCKED_SUPPLY_COLUMNS:
            if column in values.columns:
                locks[column] = np.sign(values[column]).astype(int)
    else:
        for row in SIGN_LOCKED_USE_ROWS:
            if row in values.index:
                locks.loc[row] = np.sign(values.loc[row]).astype(int)
    return locks


def build_sut_mask(
    block: Block, year: int = 2017, panel: pd.DataFrame | None = None
) -> SutMask:
    """Assemble the three layers for one block.

    Validated against the panel it was built from, so a mask that contradicts
    its own seed fails here rather than inside the balance.
    """
    values = _panel_or_default(block, panel)
    mask = SutMask(
        structural_zero=structural_zero_mask(block, values)
        & ~fixed_value_mask(block, year, values),
        fixed_value=fixed_value_mask(block, year, values),
        sign_lock=sign_lock_mask(block, values),
    )
    mask.validate_against(values)
    return mask


#: Both blocks, in the order a balance walks them.
BLOCKS: tuple[Block, ...] = ('use', 'supply')


def build_sut_masks(year: int = 2017) -> dict[Block, SutMask]:
    """Both blocks, keyed by block name."""
    return {block: build_sut_mask(block, year) for block in BLOCKS}


def mask_summary(year: int = 2017) -> pd.DataFrame:
    """Per block: cell counts per layer, and what the mask costs in dollars.

    The dollar column is the one that matters. Counting cells makes a mask look
    cheap - the 2017 final-demand block is 2.7% of the Use panel's nonzero
    cells and 39.9% of its dollars.
    """
    rows = {}
    for block in BLOCKS:
        panel = published_2017_panel(block)
        mask = build_sut_mask(block, year, panel)
        values = panel.abs().to_numpy()
        summary = mask.summary()
        total = values.sum()
        summary['fixed_%_mass'] = (
            100 * values[mask.fixed_value.to_numpy()].sum() / total if total else 0.0
        )
        rows[block] = summary
    return pd.DataFrame(rows).T
