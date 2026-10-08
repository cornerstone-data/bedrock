"""The NIPA tables Step 2's value-added block is built from (#536).

Two kinds of check, deliberately separated.

The config checks read only ``BEA_NIPA.yaml`` and always run: they pin the
table list and guard against a declared id being a substring of another, which
used to contaminate the parse before table ids were matched exactly.

The reconciliation checks need the flat-file archive and skip without it.  They
are the acceptance test for #536: not "the tables extract" but "the numbers the
value-added block is built from come back correct through the FBA".  Every
expectation is a published 2017 figure from
:mod:`bedrock.analysis.nowcasting.value_added_control_totals` or the Use SUT
itself, so a silent BEA vintage change fails here rather than inside Step 2.
"""

from __future__ import annotations

import os

import pandas as pd
import pytest

from bedrock.extract.bea.BEA_NIPA import extract_table_info, flat_files_local_path
from bedrock.extract.flowbyactivity import getFlowByActivity
from bedrock.extract.generateflowbyactivity import load_fba_config

YEAR = 2017

#: Millions of dollars, the unit NIPA and the SUT workbook both publish in.
#: The FBA carries dollars, so every reading here is divided by this.
MILLION = 1e6

#: BEA's own publication grain: these tables are published to the million, so a
#: reconciliation closer than this is exact as far as the source can say.
ROUNDING = 25.0

#: Tables added for the value-added block, by the row each serves.  ``T30500``
#: (other taxes on production) and ``T31300`` (subsidies) predate #536 and are
#: not repeated here.
VALUE_ADDED_TABLES = {
    'controls': ['T11000', 'T10105', 'T10305', 'T11400'],
    'V00100': ['T60200D', 'T60300D', 'T61000D', 'T61100D', 'T31005', 'T71800'],
    'V00300': [
        'T61200D',
        'T61300D',
        'T61400D',
        'T61500D',
        'T61600D',
        'T61700D',
        'T62200D',
        'T70500',
        'T70700',
        'T70900',
        'T30800',
        'T70305',
        'T70405',
        'T71100',
    ],
    # QCEW covers 93.8% of NIPA wages; these size and shape the other 6.2%
    'gap': [
        'T60400D',
        'T60500D',
        'T60600D',
        'U32500',
        'T70800',
        'RfHhInstComp',
        'T20200B',
    ],
}

#: The declared tables that are *not* current-dollar throughout, and what they
#: carry instead.  Everything else is ``Money``/``USD``.
NON_MONEY_TABLES = {
    'T60400D': ('Employment', 'p'),
    'T60500D': ('Employment', 'p'),
    'T60600D': ('Other', 'Ratio'),
}


def _declared_tables() -> list[str]:
    _, _, config = load_fba_config('BEA_NIPA', YEAR)
    return list(config['tables'])


class TestDeclaredTables:
    def test_every_value_added_table_is_declared(self) -> None:
        declared = set(_declared_tables())
        missing = {
            row: [t for t in tables if t not in declared]
            for row, tables in VALUE_ADDED_TABLES.items()
        }
        assert not any(missing.values()), missing

    def test_no_duplicate_tables(self) -> None:
        declared = _declared_tables()
        assert len(declared) == len(set(declared)), [
            t for t in set(declared) if declared.count(t) > 1
        ]

    def test_no_declared_table_is_a_substring_of_another(self) -> None:
        """``bea_nipa_parse`` matches table ids with ``str.contains``.

        Table ids are now matched exactly, so a substring collision no longer
        contaminates -- ``U70205`` used to drag in ``U70205S``'s 44 physical
        quantity and price index series under their own id. Keep the assertion
        anyway: it is cheap, and it fails loudly if the exact match is ever
        relaxed back to ``str.contains``.
        """
        declared = _declared_tables()
        collisions = [
            (short, long)
            for short in declared
            for long in declared
            if short != long and short in long
        ]
        assert collisions == []


@pytest.fixture(scope='module')
def nipa_2017() -> pd.DataFrame:
    """The BEA_NIPA FBA for 2017, with Table/Code/Line split out."""
    if not os.path.exists(flat_files_local_path()):
        pytest.skip('NIPA FlatFiles.ZIP is not cached locally')
    return extract_table_info(getFlowByActivity('BEA_NIPA', YEAR))


def _code(fba: pd.DataFrame, table: str, code: str) -> float:
    rows = fba.query('Table == @table and Code == @code')['FlowAmount']
    assert len(rows) == 1, f'{table}:{code} matched {len(rows)} rows, expected 1'
    return float(rows.iloc[0]) / MILLION


def _line(fba: pd.DataFrame, table: str, line: int) -> float:
    rows = fba.query('Table == @table and Line == @line')['FlowAmount']
    assert len(rows) == 1, f'{table} line {line} matched {len(rows)} rows'
    return float(rows.iloc[0]) / MILLION


class TestValueAddedTablesReconcile:
    def test_all_tables_present_and_populated(self, nipa_2017: pd.DataFrame) -> None:
        by_table = nipa_2017.groupby('Table')['FlowAmount']
        for tables in VALUE_ADDED_TABLES.values():
            for table in tables:
                assert table in by_table.groups, f'{table} missing from the FBA'
                assert by_table.get_group(table).notna().all(), f'{table} has nulls'

    def test_class_and_unit_come_from_the_series_metric(
        self, nipa_2017: pd.DataFrame
    ) -> None:
        """Not every declared table is money, and the parser must not assume it.

        6.4D and 6.5D are thousands of *persons* and 6.6D is a dollars-per-worker
        ratio.  Under the old flat ``* 1000000`` with Class/Unit hardcoded, 6.4D's
        150,654 thousand employees would have been published as 1.5e11 USD.  The
        scale and the metric both come from ``SeriesRegister`` now.
        """
        for table, (klass, unit) in NON_MONEY_TABLES.items():
            rows = nipa_2017.query('Table == @table')
            assert set(rows['Class']) == {klass}, table
            assert set(rows['Unit']) == {unit}, table

        # employees, not dollars: 150,654 thousand in 2017
        assert _code(nipa_2017, 'T60400D', 'A4201C') == pytest.approx(150.654, abs=0.01)
        # a rate, published as-is rather than scaled: $62,726 per FTE
        assert _line(nipa_2017, 'T60600D', 1) == pytest.approx(0.062726, abs=1e-6)

        # everything not listed above is still Money/USD
        others = nipa_2017[~nipa_2017['Table'].isin([*NON_MONEY_TABLES, 'T11400'])]
        assert set(others['Class']) == {'Money'}
        assert set(others['Unit']) == {'USD'}

    def test_chained_dollars_are_kept_out_of_class_money(
        self, nipa_2017: pd.DataFrame
    ) -> None:
        """1.14 carries three chained-dollar lines among its current-dollar ones.

        Chained dollars are real, not nominal.  Left as ``Class: Money`` they
        would be swept up by any ``Class: Money`` selection and added to
        current-dollar flows, which is silent and wrong; ``Other`` keeps them
        visible without making that possible.
        """
        chained = nipa_2017.query('Unit == "USD_chained"')
        assert set(chained['Table']) == {'T11400'}
        assert len(chained) == 3
        assert set(chained['Class']) == {'Other'}

    def test_the_qcew_gap_reconciles_and_is_not_uniform(
        self, nipa_2017: pd.DataFrame
    ) -> None:
        """7.18 closes the QCEW-to-NIPA wage gap exactly, and shapes it.

        QCEW's national NAICS-6 payroll is 93.8% of NIPA wages paid.  7.18 says
        what the other 6.2% is, and the government/private split of the
        UI-uncovered line already shows it is not spread evenly -- 11.3% of
        government wages against 3.5% of private, a factor of three.  That is
        what makes a per-industry coverage ratio worth computing rather than
        assuming a flat gap.
        """
        bls = _code(nipa_2017, 'T71800', 'BA06RC')
        misreporting = _code(nipa_2017, 'T71800', 'BA07RC')
        uncovered = _code(nipa_2017, 'T71800', 'W873RC')
        timing = _code(nipa_2017, 'T71800', 'Y663RC')
        received = _code(nipa_2017, 'T60300D', 'A034RC')
        assert bls + misreporting + uncovered + timing == pytest.approx(
            received, abs=1.0
        )

        uncovered_government = _code(nipa_2017, 'T71800', 'W787RC')
        uncovered_other = _code(nipa_2017, 'T71800', 'W786RC')
        assert uncovered_government + uncovered_other == pytest.approx(
            uncovered, abs=1.0
        )
        government_rate = uncovered_government / _code(nipa_2017, 'T20200B', 'B202RC')
        private_rate = uncovered_other / _code(nipa_2017, 'T20200B', 'A132RC')
        assert government_rate > 3 * private_rate

    def test_private_households_compensation_is_published_outright(
        self, nipa_2017: pd.DataFrame
    ) -> None:
        """``RfHhInstComp`` W151RC is the SUT's ``814000`` to the dollar.

        The sector QCEW covers worst is the one NIPA states directly, so it is a
        lookup rather than an allocation -- which is the pattern housing and farm
        already follow.
        """
        assert _code(nipa_2017, 'RfHhInstComp', 'W151RC') == pytest.approx(
            18_684, abs=1.0
        )

    def test_general_government_wages_and_supplements_split(
        self, nipa_2017: pd.DataFrame
    ) -> None:
        """3.25U splits what 3.10.5 only totals, and the halves close."""
        wages = _code(nipa_2017, 'U32500', 'B1663C')
        supplements = _code(nipa_2017, 'U32500', 'B1664C')
        assert wages + supplements == pytest.approx(
            _code(nipa_2017, 'U32500', 'A194RC'), abs=1.0
        )

    def test_compensation_is_the_paid_concept_and_its_parts_close_exactly(
        self, nipa_2017: pd.DataFrame
    ) -> None:
        """Wages plus both supplements equal compensation, on the *paid* line.

        6.2D and 6.3D each state their total twice: line 1 is compensation (or
        wages) *received* by residents, line 2 the amount *paid* by domestic
        industries and government.  Value added wants the paid concept, and the
        two differ by the rest-of-world adjustment -- 10,606 in 2017, stated in
        6.2D's own lines 97-99.  The supplements tables have no such split.

        Taking line 1 as the table's root is what leaves an unexplained ~10,600
        against the Use SUT.  On line 2 the identity is exact.
        """
        wages_paid = _line(nipa_2017, 'T60300D', 2)
        social_insurance = _line(nipa_2017, 'T61000D', 1)
        pension_and_insurance = _line(nipa_2017, 'T61100D', 1)
        compensation_paid = _code(nipa_2017, 'T60200D', 'A4002C')

        assert wages_paid + social_insurance + pension_and_insurance == pytest.approx(
            compensation_paid, abs=1.0
        )
        # and the paid concept is what the Use SUT's V00100 carries
        assert compensation_paid == pytest.approx(10_434_981, abs=ROUNDING)

        rest_of_world = _code(nipa_2017, 'T60200D', 'A4187C')
        received = _code(nipa_2017, 'T60200D', 'A033RC')
        assert compensation_paid - received == pytest.approx(-rest_of_world, abs=1.0)

    def test_the_tables_that_restate_a_code_are_the_ones_expected(
        self, nipa_2017: pd.DataFrame
    ) -> None:
        """Four of these tables state one code on more than one line.

        A code is unique within most NIPA tables, so a code lookup reads
        naturally -- but not in these four, where a panel heading repeats the
        code of a line above it.  Selecting by code there returns several rows,
        and a lookup that takes the first silently picks a panel.  Pin the set:
        if a BEA vintage adds a table to it, that table's readers need checking
        rather than the assertion relaxing.
        """
        value_added = [t for tables in VALUE_ADDED_TABLES.values() for t in tables]
        counts = (
            nipa_2017[nipa_2017['Table'].isin(value_added)]
            .groupby(['Table', 'Code'])
            .size()
        )
        restated = sorted({table for table, _ in counts[counts > 1].index})
        assert restated == ['T11400', 'T61100D', 'T61600D', 'T71100']

    def test_6_11d_is_three_panels_and_only_the_first_is_by_industry(
        self, nipa_2017: pd.DataFrame
    ) -> None:
        """6.11D's industry panel is 17 industries, not 36.

        The table states its 1,345,306 total three times, once per panel:
        lines 1-20 by industry, 22-36 by type of fund, 37-45 *benefits paid*.
        Counting all its leaves gives 36 and overstates what it can say about
        industries by more than double; taking the whole table gives 2,370,770
        of benefits paid on top of the contributions, which is a different
        concept and would double-count outright.

        So a method reading 6.11D has to select lines 3-20 explicitly, the same
        hazard the U20405 memorandum block posed for PCE.
        """
        table = nipa_2017.query('Table == "T61100D"')
        total = _line(nipa_2017, 'T61100D', 1)
        assert sorted(table.query('Code == "B040RC"')['Line']) == [1, 2, 22]

        industry_panel = table.query('3 <= Line <= 20')
        private = _line(nipa_2017, 'T61100D', 3)
        government = _line(nipa_2017, 'T61100D', 20)
        assert private + government == pytest.approx(total, abs=1.0)
        # 18 lines, of which manufacturing is a subtotal of its two children
        assert len(industry_panel) == 18

        benefits_paid = _line(nipa_2017, 'T61100D', 37)
        assert benefits_paid > total  # a different concept, not a component

    def test_gross_operating_surplus_assembles_to_v00300(
        self, nipa_2017: pd.DataFrame
    ) -> None:
        """V00300 has no single table; this is the assembly that closes.

        Three lines are the table's *domestic* line rather than its root:
        corporate profits ``A445RC`` (6.16D's root includes rest of the world),
        net interest ``W272RC`` including miscellaneous payments, and
        consumption of fixed capital ``A262RC`` for all sectors (the Section 6
        capital consumption tables are business only).

        The statistical discrepancy is in here because the IO accounts have
        nowhere else to put it.  It is an accounting residual, not a
        measurement of anything, and any industry pattern given to it is
        fiction -- see value_added_control_totals.
        """
        assembly = sum(
            [
                _code(nipa_2017, 'T11000', 'W272RC'),  # net interest and misc
                _code(nipa_2017, 'T70700', 'B029RC'),  # business current transfers
                _code(nipa_2017, 'T11000', 'A041RC'),  # proprietors, IVA + CCAdj
                _code(nipa_2017, 'T70900', 'A048RC'),  # rental income of persons
                _code(nipa_2017, 'T61600D', 'A445RC'),  # corporate profits, domestic
                _code(nipa_2017, 'T11000', 'A108RC'),  # govt enterprise surplus
                _code(nipa_2017, 'T70500', 'A262RC'),  # CFC, all sectors
                _code(nipa_2017, 'T11000', 'A030RC'),  # statistical discrepancy
            ]
        )
        assert assembly == pytest.approx(7_873_013, abs=ROUNDING)

    def test_other_taxes_on_production_is_a_difference_of_two_tables(
        self, nipa_2017: pd.DataFrame
    ) -> None:
        """T00OTOP = taxes on production and imports, less taxes on products."""
        taxes_on_production_and_imports = _code(nipa_2017, 'T11000', 'W056RC')
        taxes_on_products = _code(nipa_2017, 'T30500', 'LA000236') + _code(
            nipa_2017, 'T30500', 'LA000238'
        )
        assert taxes_on_production_and_imports - taxes_on_products == pytest.approx(
            608_542, abs=ROUNDING
        )

    def test_government_compensation_ties_to_the_sut_exactly(
        self, nipa_2017: pd.DataFrame
    ) -> None:
        """3.10.5 is the table that carries it; 3.2 and 3.3 do not.

        S00500 and S00600 are 1:1 lookups with no allocation at all.  Only the
        state-and-local trio needs splitting, inside an exact control.
        """
        assert _code(nipa_2017, 'T31005', 'B237RC') == pytest.approx(246_097, abs=1.0)
        assert _code(nipa_2017, 'T31005', 'W130RC') == pytest.approx(184_220, abs=1.0)
        assert _code(nipa_2017, 'T31005', 'B251RC') == pytest.approx(1_338_917, abs=1.0)
        # government enterprises tie against 6.2D's own lines
        assert _code(nipa_2017, 'T60200D', 'A4081C') == pytest.approx(59_219, abs=1.0)
        assert _code(nipa_2017, 'T60200D', 'B4086C') == pytest.approx(107_032, abs=1.0)

    def test_government_enterprise_surplus_decomposes_by_enterprise(
        self, nipa_2017: pd.DataFrame
    ) -> None:
        """3.8 gives the -4,253 an industry axis it was thought not to have.

        Federal (``B097RC``) and state and local (``B115RC``) sum to the total
        in 1.10 exactly, and the named enterprises beneath them map onto the six
        BEA government-enterprise detail codes.
        """
        federal = _code(nipa_2017, 'T30800', 'B097RC')
        state_and_local = _code(nipa_2017, 'T30800', 'B115RC')
        assert federal + state_and_local == pytest.approx(
            _code(nipa_2017, 'T11000', 'A108RC'), abs=1.0
        )

    def test_housing_and_farm_sector_tables_tie_to_the_sut(
        self, nipa_2017: pd.DataFrame
    ) -> None:
        """The two sectors NIPA states a full value-added account for.

        Compensation is the check that the populations match: housing's 18,921
        against 531HST's 18,920 (531HSO is zero), farm's 30,857 against the ten
        farm detail codes' 30,861.  Both tables' *surplus* components sit
        further from the SUT -- housing by 2.71%, farm gross value added by
        10,118 -- so they are trusted for shares here, not levels.
        """
        assert _code(nipa_2017, 'T70405', 'B1033C') == pytest.approx(18_920, abs=2.0)
        assert _code(nipa_2017, 'T70305', 'A2006C') == pytest.approx(30_861, abs=5.0)

        # the two concentrated V00300 lines, and why housing is a lookup
        housing_interest = _code(nipa_2017, 'T70405', 'B1037C')
        housing_rent = _code(nipa_2017, 'T70405', 'B1035C')
        assert housing_interest / _code(nipa_2017, 'T11000', 'W272RC') > 0.45
        assert housing_rent / _code(nipa_2017, 'T70900', 'A048RC') > 0.95
        # and the owner/tenant split of the interest is published
        assert _code(nipa_2017, 'T71100', 'W318RC') < housing_interest
