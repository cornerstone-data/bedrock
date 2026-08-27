"""The sections of the 2017 detail SUT we currently compare a nowcast against.

A Supply or Use table is not one picture.  Its blocks have different shapes,
different row and column spaces and different reconciliation bars, so the
diagnostic is cut into **sections** -- a fixed row axis, a fixed column axis,
one reference loader, one candidate loader and one
:class:`~.table_match.Tolerance` for the whole block.

The reference is always the published **2017 detail SUT** workbooks
(``Use_SUT_Framework_2017_DET.xlsx``, ``Supply_2017_DET.xlsx``).  That is the
benchmark year's answer in the framework the nowcast is being built in, and
holding every section to the same reference vintage is what makes the pictures
comparable to each other.

Everything here is BEA 2017 **detail** schema: 402 commodities, 402 industries,
the published final-demand, value-added and supply-bridge codes.  No summary
rollup -- three of the five blocks are small enough to read at detail
(402 x 19, 3 x 402, 402 x 12).  The two 402 x 402 interiors are not, and are
read through their margins and a severity summary instead -- see below.

Sections defined here
-------------------------

``use_fd_detail_sut``       Step 1.  ``derive_initial_Y_pur`` against the Use
                            table's final-demand columns.  Both sides
                            purchaser price.  Runnable today.
``use_va_detail_sut``       Step 2.  The Use table's value-added rows.
                            Declared, not yet runnable -- see below.
``supply_bridge_detail_sut`` Step 4.  The Supply table's right-hand block --
                            imports, margins, taxes and the subtotals
                            bridging basic to purchaser value.  Runnable;
                            candidate fills MCIF only.
``use_intermediate_detail_sut`` Step 3.  The Use table's 402 x 402 interior.
                            Runnable.
=========================== =================================================

The first three are the whole of what a published 2017 detail reference
supports *outside* the two 402 x 402 interiors.  ``use_intermediate_detail_sut``
is one of those interiors, and it is the exception the docstring above used to
deny: 161,604 cells is too many to read as a picture, but it is not too many to
*score*, and the section machinery reports totals, row and column margins and a
status count without anyone having to look at the grid.  The Supply interior --
the Make table, Step 4a -- is still undeclared.
=======
============================== ==============================================
``use_fd_detail_sut``          Step 1.  ``derive_initial_Y_pur`` against the
                               Use table's final-demand columns.  Both sides
                               purchaser price.  Runnable today.
``use_va_detail_sut``          Step 2.  The Use table's value-added rows.
                               Runnable; all three rows sourced, 2017 only.
``use_intermediate_detail_sut`` Step 3.  The Use table's **interior** --
                               commodity x industry, purchaser value.
                               Runnable.
``supply_output_detail_sut``   Step 4a.  The Supply table's **interior** --
                               the domestic output block, commodity x
                               industry, basic value.  Runnable.
``supply_bridge_detail_sut``   Step 4.  The Supply table's right-hand block --
                               imports, margins, taxes and the subtotals
                               bridging basic to purchaser value.  Runnable;
                               candidate fills MCIF only.
============================== ==============================================

The three small sections are the whole of what a published 2017 detail
reference supports *outside* the two interiors.

The two 402 x 402 sections, and why they are different
------------------------------------------------------

``use_intermediate_detail_sut`` and ``supply_output_detail_sut`` are the two
interiors: 161,604 cells each, against the low thousands for the other three.
They are the exception the docstring above used to deny -- too many cells to
read as a picture, but not too many to *score*, since the section machinery
reports totals, row and column margins and a status count without anyone having
to look at the grid.  Three consequences worth stating rather than discovering.

**The Supply interior is sparse, and the sparsity is the answer.**  Only ~5,000
of its cells are non-zero on either side -- an industry makes a handful of
commodities, not 402.  :class:`~.table_match.Tolerance` already has ``presence``
for this: a zero on both sides is *absent*, not a match, so the 97% of the block
that is structurally empty does not flatter the score.  ⚠️ Never quote a
match *rate* on that section without saying it is over present cells.  The Use
interior is dense by comparison and carries no such caveat.

**Neither can be read cell by cell.**  The other three sections are rendered as
a labelled grid; these two have to be read through their margins -- for Supply,
commodity output ``T007`` by row and industry output by column; for Use,
``T001`` by row and ``T005`` by column -- and through the worst-cell list.  That
is a renderer concern, not a reason to leave a section undeclared: the
reference, the frame and the bar are the same kind of settled argument here as
anywhere else.

⚠️ **The Use interior has no candidate at all until the RAS runs.**  Step 3
seeds a shape whose two margins Step 5 then imposes, so what this section scores
before Step 5 is the seed, not the estimate.

A section can be declared before its candidate exists
-----------------------------------------------------

``use_va_detail_sut`` was declared this way and is now switched on: it carried
the reference loader, the row and column frame and the tolerance -- the three
things that are arguments about the economics rather than about the code -- for
as long as Step 2 had no output, and turning it on was the one line the note
promised (#538).

:attr:`Section.runnable` reports which sections have a candidate, so the
renderer and the tests skip the rest rather than failing on them.  No section
carries ``candidate=None`` at the moment.

Tolerances
----------

``atol`` is half a million dollars throughout, because BEA publishes these
tables rounded to millions -- a difference below that grain is a rounding
artefact of the source, not a defect in the build.  ``rtol`` follows the plan's
stated bar where the plan states one (Step 1's final demand reconciles to
~1.3%, Step 4a's held-out mix test to 0.94%) and is 1% where it does not.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from bedrock.analysis.nowcasting.table_match import (
    TableMatch,
    Tolerance,
    compare_tables,
)
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_final_demand import (
    SUT_FINAL_DEMAND_CODES,
    USA_2017_FINAL_DEMAND_DESC,
)
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES

#: Half of BEA's publication grain.  The detail SUT workbook is published in
#: millions of dollars, so any difference smaller than this is below the
#: resolution of the reference itself.
ROUNDING_ATOL = 0.5 * MILLION_CURRENCY_TO_CURRENCY

#: Value-added rows of the Use SUT.  ``T00OTOP`` is *other* taxes on production
#: less subsidies -- not the MUT's ``V00200``, which is taxes on production
#: **and imports** at producer prices.  Step 2's candidate has to arrive on
#: these codes, in this valuation.
#:
#: ⚠️ The first three are ``VABAS`` and are at **basic** prices; ``T00TOP`` and
#: ``T00SUB`` are the wedge that takes the column to ``VAPRO`` at producer
#: prices.  They are five rows on one frame but not five rows of one kind, and
#: they are not built the same way -- the first three are estimated from NIPA,
#: the last two are *converted* from the Supply table's ``TOP``/``MDTY``/``SUB``
#: columns (:mod:`bedrock.transform.iot.nowcast_va_taxes`).
SUT_VALUE_ADDED_CODES = (
    'V00100',
    'T00OTOP',
    'V00300',
    'T00TOP',
    'T00SUB',
)

#: What those five rows are.  ``USA_2017_VALUE_ADDED_DESC`` describes the MUT's
#: codes, so it does not carry ``T00OTOP``.
SUT_VALUE_ADDED_DESC = {
    'V00100': 'Compensation of employees',
    'T00OTOP': 'Other taxes on production, less subsidies',
    'V00300': 'Gross operating surplus',
    'T00TOP': 'Taxes on products and imports',
    'T00SUB': 'Subsidies (negative, balance convention)',
}

#: The Supply table's right-hand block: everything to the right of the
#: commodity x industry interior, which is the bridge from domestic output at
#: basic value to total supply at purchaser value.  Subtotals are kept in the
#: frame rather than stripped out, because they are the Supply identities and a
#: subtotal that disagrees with its own components is the thing worth seeing::
#:
#:     T013 = T007 + MCIF + MADJ        total supply, basic
#:     T014 = TRADE + TRANS             margins
#:     T015 = MDTY + TOP + SUB          taxes less subsidies
#:     T016 = T013 + T014 + T015        total supply, purchaser
SUPPLY_BRIDGE_CODES = (
    'T007',
    'MCIF',
    'MADJ',
    'T013',
    'TRADE',
    'TRANS',
    'T014',
    'MDTY',
    'TOP',
    'SUB',
    'T015',
    'T016',
)

SUPPLY_BRIDGE_DESC = {
    'T007': 'Total commodity output (domestic, basic value)',
    'MCIF': 'Imports of goods and services, CIF value',
    'MADJ': 'CIF/FOB adjustment on imports',
    'T013': 'Total supply, basic value',
    'TRADE': 'Trade margins',
    'TRANS': 'Transportation costs',
    'T014': 'Total trade and transportation margins',
    'MDTY': 'Import duties',
    'TOP': 'Taxes on products',
    'SUB': 'Subsidies (stored negative)',
    'T015': 'Taxes less subsidies on products',
    'T016': 'Total supply, purchaser value',
}


@dataclass(frozen=True)
class Section:
    """One comparable block of a published table, with all it needs to run."""

    name: str
    #: Human-readable title, used as the plot title and the report label.
    title: str
    #: Which build step this section is the test for.
    step: str
    rows: tuple[str, ...]
    columns: tuple[str, ...]
    row_axis: str
    column_axis: str
    tolerance: Tolerance
    #: ``year -> reference frame``, already in dollars
    reference: Callable[[int], pd.DataFrame]
    #: ``year -> candidate frame``, already in dollars.  ``None`` for a section
    #: whose build step has not happened yet: the reference, the frame and the
    #: bar are all settled, and the step only has to supply this.
    candidate: Callable[[int], pd.DataFrame] | None
    #: Years the reference exists for.  The benchmark SUT is 2017-only.
    years: tuple[int, ...] = (2017,)
    #: ``code -> description`` for the small axis, so the picture is readable
    #: without a code book.  Only used where an axis is short enough to label.
    row_names: Mapping[str, str] = field(default_factory=dict)
    column_names: Mapping[str, str] = field(default_factory=dict)
    row_aliases: Mapping[str, str] = field(default_factory=dict)
    #: Free text carried into the report, for caveats a colour cannot express.
    note: str = ''

    @property
    def runnable(self) -> bool:
        """Whether a candidate exists yet.  See :attr:`candidate`."""
        return self.candidate is not None

    def run(self, year: int = 2017) -> TableMatch:
        """Load both sides and compare them on this section's fixed frame."""
        if year not in self.years:
            raise ValueError(
                f'{self.name} has no reference for {year}; available: {self.years}'
            )
        if self.candidate is None:
            raise NotImplementedError(
                f'{self.name} has no candidate yet - {self.step} has not been '
                'built. The reference, frame and tolerance are defined; point '
                'Section.candidate at the build output to turn this on.'
            )
        return compare_tables(
            self.candidate(year),
            self.reference(year),
            tolerance=self.tolerance,
            rows=pd.Index(self.rows, name=self.row_axis),
            columns=pd.Index(self.columns, name=self.column_axis),
            row_aliases=self.row_aliases,
            label=f'{self.title} ({year})',
        )


# ------------------------------------------------------------------ references


def _use_sut_detail() -> pd.DataFrame:
    from bedrock.extract.iot.io_2017 import (  # noqa: PLC0415
        _load_2017_detail_supply_use_usa,
    )

    return _load_2017_detail_supply_use_usa('Use_SUT_detail')


def _supply_sut_detail() -> pd.DataFrame:
    from bedrock.extract.iot.io_2017 import (  # noqa: PLC0415
        _load_2017_detail_supply_use_usa,
    )

    supply = _load_2017_detail_supply_use_usa('Supply_detail')
    # The workbook ships 'TRADE ' with a trailing space; a code that only
    # matches by accident of whitespace is not a code anyone can look up.
    supply.columns = supply.columns.str.strip()
    return supply


def _block(
    table: pd.DataFrame, rows: Sequence[str], columns: Sequence[str]
) -> pd.DataFrame:
    """Pull a labelled block out of the workbook and put it in dollars."""
    block = table.reindex(index=list(rows), columns=list(columns)).astype(float)
    return block * MILLION_CURRENCY_TO_CURRENCY


def _require_2017(year: int) -> None:
    if year != 2017:
        raise ValueError(
            'Use_SUT_Framework_2017_DET.xlsx is a benchmark-year table published '
            f'once for 2017; no {year} reference exists'
        )


def use_sut_final_demand_reference(year: int = 2017) -> pd.DataFrame:
    """Final-demand block of the published 2017 detail Use SUT table, in USD.

    Purchaser price, which is the basis ``derive_initial_Y_pur`` produces, so
    no PRO<->PUR conversion belongs in this comparison.
    """
    _require_2017(year)
    return _block(_use_sut_detail(), USA_2017_COMMODITY_CODES, SUT_FINAL_DEMAND_CODES)


def use_sut_value_added_reference(year: int = 2017) -> pd.DataFrame:
    """Value-added rows of the published 2017 detail Use SUT table, in USD.

    ⚠️ ``T00SUB`` is flipped to the **balance's** sign convention, negative, as
    ``nowcast_mask.published_2017_panel`` and ``nowcast._USE_VALUE_ADDED_SUBTOTALS``
    store it and as the candidate produces it.  BEA publishes the Use row
    positive and subtracts it.  Comparing an unflipped reference against the
    candidate reports every subsidised industry as a 200% error, which reads
    like a broken row rather than like a sign.
    """
    _require_2017(year)
    block = _block(
        _use_sut_detail(), SUT_VALUE_ADDED_CODES, USA_2017_INDUSTRY_CODES
    ).copy()
    block.loc['T00SUB'] = -block.loc['T00SUB']
    return block


def use_sut_intermediate_reference(year: int = 2017) -> pd.DataFrame:
    """Intermediate interior of the published 2017 detail Use SUT table, in USD.

    402 commodities x 402 industries, purchaser price, before redefinitions.

    ⚠️ **Seven cells are negative and stay negative.**  They are published that
    way, and a candidate that clips them has stopped reproducing its own source.
    """
    _require_2017(year)
    return _block(_use_sut_detail(), USA_2017_COMMODITY_CODES, USA_2017_INDUSTRY_CODES)


def supply_sut_output_reference(year: int = 2017) -> pd.DataFrame:
    """Domestic output block of the published 2017 detail Supply table, in USD.

    The Supply **interior**: commodity x industry, basic value, whose row margin
    is commodity output ``T007`` and whose column margin is industry output.
    Not the ``T007`` column itself -- that is the margin of this block, and
    reproducing a margin says nothing about how the block divides beneath it,
    which is the whole of what Step 4a builds.

    ⚠️ ``_supply_sut_detail`` strips the workbook's trailing space off
    ``'TRADE '``.  Without that the margin column is six characters like a BEA
    detail code and a shape-based column selection swallows it, injecting the
    whole trade margin into the interior.  Selecting by
    :data:`USA_2017_INDUSTRY_CODES` as this does is immune, and is why it is
    done by name.
    """
    _require_2017(year)
    return _block(
        _supply_sut_detail(), USA_2017_COMMODITY_CODES, USA_2017_INDUSTRY_CODES
    )


def supply_sut_bridge_reference(year: int = 2017) -> pd.DataFrame:
    """Right-hand block of the published 2017 detail Supply SUT table, in USD.

    Commodity x :data:`SUPPLY_BRIDGE_CODES` -- imports, margins, taxes and the
    subtotals that carry a commodity from domestic output at basic value to
    total supply at purchaser value.
    """
    _require_2017(year)
    return _block(_supply_sut_detail(), USA_2017_COMMODITY_CODES, SUPPLY_BRIDGE_CODES)


# ------------------------------------------------------------------ candidates


def initial_Y_pur_candidate(year: int) -> pd.DataFrame:
    """Our Step 1 final-demand block, commodity x final-demand code, in USD.

    Runs ``derive_initial_Y_pur``.  This is the authoritative candidate and
    the one the section should use once the FBS runs again; see
    :func:`initial_Y_pur_exported_candidate` for why it currently does not.
    """
    from bedrock.transform.eeio.nowcast import derive_initial_Y_pur  # noqa: PLC0415

    return derive_initial_Y_pur(year)


def initial_value_added_candidate(year: int) -> pd.DataFrame:
    """Our Step 2 value-added block, value-added code x industry, in USD.

    Runs ``derive_initial_value_added``, which stacks the three
    ``NIPA_VA_*_2017`` methods. 2017 only -- the later-year files wait on the
    compensation movement series.
    """
    from bedrock.transform.eeio.nowcast import (  # noqa: PLC0415
        derive_initial_value_added,
    )

    return derive_initial_value_added(year)


def detail_supply_output_candidate(year: int) -> pd.DataFrame:
    """Our Step 4a domestic output block, commodity x industry, in USD.

    Reads the ``Detail_Supply_<year>`` FBS
    (``bedrock/transform/detail/Detail_Supply_<year>.yaml``), which
    disaggregates the published **summary** Supply domestic-output block onto
    the 2017 detail mix.

    ⚠️ **The axes are the reverse of what the column names suggest.**  In this
    FBS the commodity is ``SectorConsumedBy`` and the industry is
    ``SectorProducedBy``, because the Supply table's rows are commodities and
    its columns industries.  Reading them the intuitive way round transposes the
    block, which still balances economy-wide and is therefore not caught by a
    totals check.

    ⚠️ **2017 is close to circular and later years are not evidence at all.**
    The 2017 build reproduces the published detail ``T007`` to rounding
    (33,772,550m against 33,772,566m) because it is disaggregating a summary
    control onto the same detail mix the reference publishes.  Later years close
    on the published *summary* margin exactly by construction.  What the split
    beneath actually rests on is the held-out mix test -- 0.94% economy-wide
    over five years -- and, from 2022, the Economic Census product lines
    (``pxi_mix_test.py``).  ✅ This section's job is to catch a build that has
    broken, not to prove the method.

    ``S00300``, ``S00402`` and ``4200ID`` carry no rows: their published
    ``T007`` is zero by definition -- they are not domestic output and enter the
    Supply table through ``MCIF`` / ``MDTY`` / margins.  They reindex to 0.0,
    which is their correct value and not a gap.
    """
    from bedrock.transform.flowbysector import getFlowBySector  # noqa: PLC0415

    fbs = pd.DataFrame(getFlowBySector(f'Detail_Supply_{year}'))
    return (
        fbs.groupby(['SectorConsumedBy', 'SectorProducedBy'])['FlowAmount']
        .sum()
        .unstack('SectorProducedBy')
        .astype(float)
        .fillna(0.0)
    )


def initial_supply_bridge_candidate(year: int) -> pd.DataFrame:
    """Our Step 4 supply-bridge block, commodity x bridge code, in USD.

    Runs ``derive_initial_supply_bridge``. T007 and MCIF are sourced; other
    columns are unsourced.
    """
    from bedrock.transform.eeio.nowcast import (  # noqa: PLC0415
        derive_initial_supply_bridge,
    )

    return derive_initial_supply_bridge(year)


def initial_U_intermediate_candidate(year: int) -> pd.DataFrame:
    """Our Step 3 intermediate block, commodity x industry, in USD.

    Runs ``derive_initial_U_intermediate`` at the fitted ``theta`` for the span.
    At 2017 the carry is the identity -- both legs of the deflator are 1.0 --
    and only the column control moves, so this section run is a plumbing test;
    the movement is scored on the summary panel by
    ``intermediate_structure_drift``, not here.
    """
    from bedrock.transform.eeio.nowcast import (  # noqa: PLC0415
        derive_initial_U_intermediate,
    )

    return derive_initial_U_intermediate(year)


#: Where ``initial_Y_pur_baseline.export_cellwise_comparison`` writes.
INITIAL_Y_PUR_EXPORT = (
    Path(__file__).parent
    / 'output'
    / 'nowcast_initial_Y_pur_vs_use_sut_framework_2017.csv'
)


def initial_Y_pur_exported_candidate(year: int) -> pd.DataFrame:
    """The Step 1 final-demand block as last exported to CSV, in USD.

    Reads the ``ours_PUR`` column of
    :data:`INITIAL_Y_PUR_EXPORT` and pivots it back to commodity x
    final-demand code.  Only the candidate side is taken from the file -- the
    reference always comes from the published SUT workbook, so a stale
    baseline column in an older export cannot leak into the comparison.

    Not what the section uses -- :func:`initial_Y_pur_candidate` runs the FBS
    live.  Kept because reading a pinned export is the only way to put a past
    run beside a current one, which is what says whether a change moved the
    numbers; and because it is the fallback if the FBS breaks again, as it did
    between ``42f7e59`` and the restoration of ``retain_activity_columns``.
    """
    _require_2017(year)
    if not INITIAL_Y_PUR_EXPORT.exists():
        raise FileNotFoundError(
            f'{INITIAL_Y_PUR_EXPORT} not found; regenerate it with '
            'bedrock.analysis.nowcasting.initial_Y_pur_baseline'
            '.export_cellwise_comparison()'
        )
    long = pd.read_csv(INITIAL_Y_PUR_EXPORT, dtype={'commodity': str})
    wide = long.pivot(
        index='commodity', columns='final_demand_code', values='ours_PUR'
    ).astype(float)
    wide.index.name = 'commodity'
    wide.columns.name = 'final_demand_code'
    return wide


# -------------------------------------------------------------------- sections


USE_FD_DETAIL_SUT = Section(
    name='use_fd_detail_sut',
    title='Use final demand, BEA 2017 detail — ours vs published SUT',
    step='Step 1 - final-demand columns',
    rows=tuple(USA_2017_COMMODITY_CODES),
    columns=SUT_FINAL_DEMAND_CODES,
    row_axis='commodity',
    column_axis='final_demand_code',
    # The plan's Step 1 bar: PCE reconciles to ~1.3%. The ramp runs to 25%,
    # which is where a cell has stopped being a reconciliation difference and
    # started being a different number.
    tolerance=Tolerance(rtol=0.013, atol=ROUNDING_ATOL, ramp=0.25),
    column_names=USA_2017_FINAL_DEMAND_DESC,
    reference=use_sut_final_demand_reference,
    candidate=initial_Y_pur_candidate,
    note=(
        'Candidate is a live run of derive_initial_Y_pur (NIPA_final_dom_uses plus '
        'Trade_Exports F040 for 2017). Reference is always the published SUT '
        'workbook.'
    ),
)

USE_VA_DETAIL_SUT = Section(
    name='use_va_detail_sut',
    title='Use value added, BEA 2017 detail — nowcast vs published SUT',
    step='Step 2 - value-added rows',
    rows=SUT_VALUE_ADDED_CODES,
    columns=tuple(USA_2017_INDUSTRY_CODES),
    row_axis='value_added_code',
    column_axis='industry',
    tolerance=Tolerance(rtol=0.01, atol=ROUNDING_ATOL, ramp=0.25),
    row_names=SUT_VALUE_ADDED_DESC,
    reference=use_sut_value_added_reference,
    candidate=initial_value_added_candidate,
    note=(
        'Candidate is a live run of derive_initial_value_added, 2017-2024. The '
        'five rows are not five claims of one kind. V00100 is an ESTIMATE '
        '(QCEW movement in 69 NIPA groups); T00OTOP is a LEVEL plus two '
        'lookups (43.3% of the row observed); V00300 is a SEED only, and the '
        'residual T18 hands the balance. T00TOP and T00SUB are neither - they '
        'are CONVERTED from the Supply columns by nowcast_va_taxes, so their '
        'levels carry no modelling content at all and only the industry split '
        'is estimated. T00SUB reproduces the published 2017 row exactly; '
        'T00TOP is a seed at r = 0.947, 27.9% off, and stays one. A 2017 run '
        'of the first three tests the plumbing, not the movement series - '
        'near-exact is the floor there, not an achievement.'
    ),
)

SUPPLY_OUTPUT_DETAIL_SUT = Section(
    name='supply_output_detail_sut',
    title='Supply domestic output block, BEA 2017 detail — nowcast vs published SUT',
    step='Step 4a - the commodity x industry domestic output block',
    rows=tuple(USA_2017_COMMODITY_CODES),
    columns=tuple(USA_2017_INDUSTRY_CODES),
    row_axis='commodity',
    column_axis='industry',
    # The plan's Step 4a bar: the held-out test put a carried 2017 mix 0.94%
    # off economy-wide over five years, so 1% is the stated bar rather than a
    # default. ⚠️ presence is left at its default so a cell that is zero on
    # both sides counts as absent -- 97% of this block is structurally empty,
    # and scoring those as matches would report ~97% for any build at all.
    tolerance=Tolerance(rtol=0.01, atol=ROUNDING_ATOL, ramp=0.25),
    reference=supply_sut_output_reference,
    candidate=detail_supply_output_candidate,
    note=(
        'The Supply interior, 402 x 402 and 96.9% structurally empty: 5,059 '
        'cells of 161,604 are present, because an industry makes a handful of '
        'commodities, not 402. Quote a match rate only over present cells. '
        '2017 runs at 100.0% coverage and 99.6% accuracy - 5,038 match, 21 '
        'partial, no misses and no extras. '
        'Candidate is the Detail_Supply_<year> FBS, '
        'which disaggregates the published summary domestic-output block onto '
        'the 2017 detail mix; from 2022 the mix itself moves on Economic '
        'Census product lines (pxi_mix_test.py, 133 of 178 columns). '
        '2017 is close to circular - the same detail mix appears on both sides '
        '- so a green result here means the build has not broken, not that the '
        'method is right. The method rests on the held-out mix test (0.94% '
        'economy-wide over five years) and on the finding that no annual '
        'survey can improve the between-census mix (annual_mix_test.py).'
    ),
)

SUPPLY_BRIDGE_DETAIL_SUT = Section(
    name='supply_bridge_detail_sut',
    title='Supply bridge to purchaser value, BEA 2017 detail — nowcast vs published SUT',
    step='Step 4 - imports, margins and taxes on the Supply table',
    rows=tuple(USA_2017_COMMODITY_CODES),
    columns=SUPPLY_BRIDGE_CODES,
    row_axis='commodity',
    column_axis='supply_bridge_code',
    # Not the exactness bar the Supply *identities* are held to: this compares a
    # built bridge against the published one, which is a reconciliation, not an
    # identity. The identity check is a separate assertion on one table.
    tolerance=Tolerance(rtol=0.01, atol=ROUNDING_ATOL, ramp=0.25),
    column_names=SUPPLY_BRIDGE_DESC,
    reference=supply_sut_bridge_reference,
    candidate=initial_supply_bridge_candidate,
    note=(
        'Candidate is a live run of derive_initial_supply_bridge: MCIF from '
        'mapped Trade_Imports_2017 Detail mass; MDTY from Census duty rate × '
        'goods MCIF leveled to NIPA B235RC; MADJ from Census GEN_CHA_YR '
        'reassigned onto 2017 Supply MADJ destination codes and leveled to '
        'published Supply MADJ; T007 the row margin of the Detail_Supply_2017 '
        'FBS domestic-output block; TRADE/TRANS from step 4c; TOP from step 4d '
        '(NIPA T30500 less customs duties, named product lines annually and the '
        'sales-tax residual on frozen 2017 shares); SUB from step 4d (NIPA '
        'T31300 by type, anchored on 2017 and moved per type, with 2020-21 '
        'pandemic subsidies on BEA PPP-by-industry). All 12 columns are live, '
        'so the four subtotals are evaluable. '
        'T014 nets to ~1 economy-wide, which is why this block needs a '
        'per-commodity picture rather than a totals check.'
    ),
)

USE_INTERMEDIATE_DETAIL_SUT = Section(
    name='use_intermediate_detail_sut',
    title='Use intermediate block, BEA 2017 detail — nowcast vs published SUT',
    step='Step 3 - the intermediate interior',
    rows=tuple(USA_2017_COMMODITY_CODES),
    columns=tuple(USA_2017_INDUSTRY_CODES),
    row_axis='commodity',
    column_axis='industry',
    # 1% because the plan states no bar for Step 3.  ⚠️ At 2017 the candidate is
    # the reference rescaled to a column control that is BEA's own rounded
    # ``T005``, and the interior sums 402 separately rounded cells to a
    # different number -- $350M on $14.9T, at most $13M on a column.  A *small*
    # column wears that as a large fraction, so ``atol`` is what carries those
    # cells, not ``rtol``: ``334610`` is $482M of intermediates and is rescaled
    # by 1.05%.
    tolerance=Tolerance(rtol=0.01, atol=ROUNDING_ATOL, ramp=0.25),
    reference=use_sut_intermediate_reference,
    candidate=initial_U_intermediate_candidate,
    note=(
        'Candidate is derive_initial_U_intermediate: the published 2017 detail '
        'interior column-normalised, carried on the purchaser deflator (the '
        'detail commodity price ratio times the margin-rate factor) at the '
        'fitted theta, and rescaled to GO_producer - VAPRO. Both sides '
        'of that control are observed annually, from '
        'derived_intermediate_and_value_added, which allocates BEA UVA205-A '
        'down to the 402 detail industries; aggregated to summary it matches '
        'the published T005 to 0.0002%. VAPRO is the column total, not Step 2 '
        '- Step 2 owes the split across the five value-added rows, and now '
        'supplies it for 2017-2024 (#538). theta is 0.75 on a span that does not cross the 2021-22 '
        'price surge and 0.0 on one that does (#699), not #497 as written. '
        'At 2017 both legs of the deflator are 1.0, so this run is a plumbing '
        'test. The seven negative cells are preserved.'
    ),
)

#: Every section, by name.  The renderer and the tests both select from here.
SECTIONS: dict[str, Section] = {
    section.name: section
    for section in (
        USE_FD_DETAIL_SUT,
        USE_VA_DETAIL_SUT,
        USE_INTERMEDIATE_DETAIL_SUT,
        SUPPLY_OUTPUT_DETAIL_SUT,
        SUPPLY_BRIDGE_DETAIL_SUT,
    )
}


def get_section(name: str) -> Section:
    if name not in SECTIONS:
        raise KeyError(f'unknown section {name!r}; known: {sorted(SECTIONS)}')
    return SECTIONS[name]
