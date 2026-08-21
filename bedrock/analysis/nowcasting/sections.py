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
rollup -- these three blocks are small enough to read at detail
(402 x 19, 3 x 402, 402 x 12), which is not true of the 402 x 402 interiors.

Sections defined here
-------------------------

=========================== =================================================
``use_fd_detail_sut``       Step 1.  ``derive_initial_Y_pur`` against the Use
                            table's final-demand columns.  Both sides
                            purchaser price.  Runnable today.
``use_va_detail_sut``       Step 2.  The Use table's value-added rows.
                            Declared, not yet runnable -- see below.
``supply_bridge_detail_sut`` Step 4.  The Supply table's right-hand block --
                            imports, margins, taxes and the subtotals
                            bridging basic to purchaser value.  Runnable;
                            candidate fills MCIF only.
=========================== =================================================

These three are the whole of what a published 2017 detail reference supports
outside the two 402 x 402 interiors.

A section can be declared before its candidate exists
-----------------------------------------------------

Step 2 has not been built, so ``use_va_detail_sut`` carries
``candidate=None``.  What it *does* carry is the reference loader, the exact
row and column frame, and the tolerance -- the three things that are arguments
about the economics rather than about the code, and that are therefore worth
settling before the build rather than after it.  Turning the section on is one
line when Step 2 lands: point :attr:`Section.candidate` at its output.

:attr:`Section.runnable` reports which sections have a candidate, so the
renderer and the tests skip the rest rather than failing on them.

Tolerances
----------

``atol`` is half a million dollars throughout, because BEA publishes these
tables rounded to millions -- a difference below that grain is a rounding
artefact of the source, not a defect in the build.  ``rtol`` follows the plan's
stated bar where the plan states one (Step 1's final demand reconciles to
~1.3%) and is 1% where it does not.
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

#: Value-added rows of the Use SUT, basic prices.  ``T00OTOP`` is *other* taxes
#: on production less subsidies -- not the MUT's ``V00200``, which is taxes on
#: production **and imports** at producer prices.  Step 2's candidate has to
#: arrive on these codes, in this valuation.
SUT_VALUE_ADDED_CODES = ('V00100', 'T00OTOP', 'V00300')

#: What those three rows are.  ``USA_2017_VALUE_ADDED_DESC`` describes the MUT's
#: codes, so it does not carry ``T00OTOP``.
SUT_VALUE_ADDED_DESC = {
    'V00100': 'Compensation of employees',
    'T00OTOP': 'Other taxes on production, less subsidies',
    'V00300': 'Gross operating surplus',
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
    """Value-added rows of the published 2017 detail Use SUT table, in USD."""
    _require_2017(year)
    return _block(_use_sut_detail(), SUT_VALUE_ADDED_CODES, USA_2017_INDUSTRY_CODES)


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


def initial_supply_bridge_candidate(year: int) -> pd.DataFrame:
    """Our Step 4 supply-bridge block, commodity x bridge code, in USD.

    Runs ``derive_initial_supply_bridge``. T007 and MCIF are sourced; other
    columns are unsourced.
    """
    from bedrock.transform.eeio.nowcast import (  # noqa: PLC0415
        derive_initial_supply_bridge,
    )

    return derive_initial_supply_bridge(year)


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
    candidate=None,
    note=(
        'Step 2 has not been built, so there is no candidate to compare yet. The '
        'reference, the row/column frame and the bar are settled here so that '
        'Step 2 only has to point Section.candidate at its output.'
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
        'FBS domestic-output block; margins, tax and subtotals unsourced. '
        'T014 nets to ~1 economy-wide, which is why this block needs a '
        'per-commodity picture rather than a totals check.'
    ),
)

#: Every section, by name.  The renderer and the tests both select from here.
SECTIONS: dict[str, Section] = {
    section.name: section
    for section in (USE_FD_DETAIL_SUT, USE_VA_DETAIL_SUT, SUPPLY_BRIDGE_DETAIL_SUT)
}


def get_section(name: str) -> Section:
    if name not in SECTIONS:
        raise KeyError(f'unknown section {name!r}; known: {sorted(SECTIONS)}')
    return SECTIONS[name]
