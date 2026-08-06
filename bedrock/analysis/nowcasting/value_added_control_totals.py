"""Reconcile the Use SUT's value-added rows against NIPA control totals, 2017.

Step 2 needs to know two things before any FBS is written: which NIPA tables
carry the value-added components, and whether their totals actually agree with
the Use table they have to reproduce.  This answers both for the benchmark
year, where the published answer exists ([#537]).

Every row of the Use SUT's value-added block reconciles to NIPA within BEA's
own rounding.  What does *not* reconcile is the construction of ``V00300``
proposed on the issue -- it is 21.9% short, for six separately identifiable
reasons, and this module names each one rather than reporting a single gap.

The reconciliation
------------------

===========  ======================================================  ==========
``V00100``   T1.10 ``A4002C`` compensation of employees, paid                -3
``T00TOP``   T3.5 taxes on products                                    see note
``T00OTOP``  T1.10 ``W056RC`` less taxes on products                       -22
``T00SUB``   T3.13 / T1.10 ``A107RC`` subsidies                             -1
``V00300``   T1.10 net operating surplus + CFC + statistical              +14
             discrepancy
``VAPRO``    T1.1.5 ``A191RC`` gross domestic product                       -7
===========  ======================================================  ==========

Differences in millions, against totals of 0.6 to 19.6 trillion.

Two of those lines are worth stating out loud, because neither is obvious and
both change what Step 2 has to build:

**GDP equals VAPRO, not VABAS.**  NIPA's GDP is a producer-value measure, so it
lands on the Use table's ``VAPRO`` row (19,612,109 against 19,612,102).
``VABAS`` is 695,567 lower -- that is taxes on products less subsidies, which
sit between the two valuations.  Reconciling GDP against ``VABAS`` would look
like a 3.5% error and be nothing of the kind.

**Gross operating surplus absorbs the statistical discrepancy.**  Net operating
surplus plus consumption of fixed capital is 7,805,125, which is 67,888 short
of ``V00300``.  The statistical discrepancy is 67,902.  The IO accounts have
nowhere else to put it, so it lands in ``V00300``, and any construction that
omits it is short by roughly that amount.

Why the proposed ``V00300`` construction falls short
-----------------------------------------------------

``T61200D + T61400D + T61500D + T61700D + T61300D + T62200D`` sums to
6,145,875 -- 78.1% of ``V00300``.  The gaps, largest first:

- **consumption of fixed capital, -839,833.**  ``T61300D`` and ``T62200D`` are
  the *business* capital consumption allowances.  Households, nonprofits and
  government have consumption of fixed capital too, and it is in value added.
  ``T70500`` carries the whole 3,148,953 and supersedes both.
- **rental income of persons, -642,028.**  No table proposed; it is ``T70900``.
- **proprietors' income, -340,534.**  ``T61200D`` is *nonfarm* proprietors'
  income by its title.  Farm is in ``T71500``.
- **net interest, -212,131.**  ``T61500D`` is net interest; the value-added
  line is net interest *and miscellaneous payments*, and is domestic industries
  only -- the table's root includes rest of the world at -195,341.
- **business current transfer payments, -142,925.**  No table proposed; it is
  ``T70700``.
- **statistical discrepancy, -67,902.**  Not an industry series at all.
- **corporate profits, +513,949.**  ``T61700D`` is profits *before tax without*
  IVA and CCAdj; value added wants them *with*, and domestic industries only.
  ``T61600D`` line ``A445RC`` is the right series.  The proposed pairing
  overshoots, which is why the net shortfall looks smaller than the individual
  gaps.

Granularity is the real constraint, and it is uneven across the block
---------------------------------------------------------------------

The control totals all reconcile.  What varies is how far each row can be
taken *by industry*, and the three rows are in very different positions --
so a single "NIPA does not give value added by industry" would be wrong:

- **``V00100``, 55% of ``VABAS``, is well served.**  ``T60200D`` is compensation
  of employees by industry with 74 leaf rows, which is about BEA summary
  granularity.  This is the row with real industry structure available.
- **``V00300``, 42%, is fragmented.**  Its by-industry pieces are much coarser
  -- corporate profits 23 leaves, net interest 20, nonfarm proprietors 21 --
  and 10.8% of it has no industry axis at any scope.
- **``T00OTOP``, 3%, has none.**  ``T30500`` is organised by level of government
  and kind of tax, which shares no cell correspondence with an industry axis
  at all.

The ceiling everywhere is summary, not detail: **no NIPA table reaches the 402
BEA detail industries**.  Every row therefore needs an allocation source to get
the rest of the way, and *which* source is an open question per row rather than
a settled one -- #538's note about 2017 table ratios is one option, not the
only one, and for compensation it is probably not the best one.

Candidates already in bedrock, worth exploring before defaulting to benchmark
ratios:

- **``BLS_QCEW``** carries ``total_annual_wages`` as ``Class: Money`` in USD at
  NAICS granularity, alongside employment counts.  For ``V00100`` that is a
  *direct dollar* allocator from an annual source, rather than a benchmark-year
  share held fixed.  It is an allocator and not a control total: QCEW covers
  covered employment and wages, not the whole of compensation, so it should
  distribute the NIPA total rather than replace it.
- **``Employment_national_<year>``** already runs QCEW to ``NAICS_6``
  nationally, which is finer than BEA detail needs, and is the obvious fallback
  basis where a dollar measure is not available.

Nothing equivalent has been identified yet for ``V00300`` or ``T00OTOP``; that
is open work, not a settled default.

:func:`value_added_granularity` reports the per-row position and
:func:`by_industry_coverage` the split within ``V00300``.

Usage::

    uv run python -m bedrock.analysis.nowcasting.value_added_control_totals
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from bedrock.analysis.compare_NIPA_to_IOT import nipa_flat_table

YEAR = 2017

#: Rows of the Use SUT value-added block, in publication order.
SUT_VA_ROWS = (
    'V00100',
    'T00OTOP',
    'V00300',
    'VABAS',
    'T00TOP',
    'T00SUB',
    'VAPRO',
)


def sut_value_added_totals(year: int = YEAR) -> pd.Series:
    """Use SUT value-added rows, summed across the 402 detail industries.

    In millions of USD, the workbook's own unit, because everything here is
    compared against NIPA which publishes in the same unit.
    """
    if year != YEAR:
        raise ValueError(f'the detail SUT is a 2017 benchmark table; got {year}')
    from bedrock.extract.iot.io_2017 import (  # noqa: PLC0415
        _load_2017_detail_supply_use_usa,
    )
    from bedrock.utils.taxonomy.bea.v2017_industry import (  # noqa: PLC0415
        USA_2017_INDUSTRY_CODES,
    )

    sut = _load_2017_detail_supply_use_usa('Use_SUT_detail')
    block = sut.loc[list(SUT_VA_ROWS), USA_2017_INDUSTRY_CODES].astype(float)
    return block.sum(axis=1)


def nipa_line(table: str, code: str, year: int = YEAR) -> float:
    """One series out of a NIPA table, by code.

    By code rather than by position, because several of these tables carry the
    same concept twice -- once for all sectors and once for domestic industries
    -- and the root line is not the one value added wants.
    """
    frame = nipa_flat_table(table, year).frame
    match = frame.loc[frame['code'] == code, 'value']
    if match.empty:
        raise KeyError(f'{code} not found in {table}@{year}')
    return float(match.iloc[0])


@dataclass(frozen=True)
class Check:
    """One reconciliation line: a SUT row against a NIPA-derived total."""

    sut_row: str
    label: str
    nipa: float
    source: str

    def compare(self, sut_totals: pd.Series) -> dict[str, object]:
        sut = float(sut_totals[self.sut_row])
        diff = self.nipa - sut
        return {
            'row': self.sut_row,
            'component': self.label,
            'nipa': self.nipa,
            'use_sut': sut,
            'diff': diff,
            'pct': (diff / sut * 100) if sut else float('nan'),
            'source': self.source,
        }


def build_checks(year: int = YEAR) -> list[Check]:
    """The reconciliation lines, with every NIPA value resolved."""
    t110 = {
        code: nipa_line('T11000', code, year)
        for code in (
            'A4002C',  # compensation of employees, paid
            'W056RC',  # taxes on production and imports
            'A107RC',  # subsidies (shown as government)
            'W271RC',  # net operating surplus
            'A262RC',  # consumption of fixed capital
            'A030RC',  # statistical discrepancy
        )
    }
    gdp = nipa_line('T10105', 'A191RC', year)
    sut = sut_value_added_totals(year)
    t00top = float(sut['T00TOP'])

    return [
        Check('V00100', 'Compensation of employees', t110['A4002C'], 'T1.10 A4002C'),
        Check(
            'T00OTOP',
            'Other taxes on production, less subsidies',
            t110['W056RC'] - t00top,
            'T1.10 W056RC less taxes on products',
        ),
        Check(
            'V00300',
            'Gross operating surplus',
            t110['W271RC'] + t110['A262RC'] + t110['A030RC'],
            'T1.10 W271RC + A262RC + A030RC',
        ),
        Check('T00SUB', 'Subsidies', t110['A107RC'], 'T1.10 A107RC'),
        Check('VAPRO', 'Value added, producer value', gdp, 'T1.1.5 A191RC (GDP)'),
        Check(
            'VABAS',
            'Value added, basic value',
            gdp - t00top + float(sut['T00SUB']),
            'GDP less taxes on products plus subsidies',
        ),
    ]


#: The construction proposed on #537, as a list of NIPA tables to sum.
PROPOSED_V00300_TABLES = (
    'T61200D',
    'T61400D',
    'T61500D',
    'T61700D',
    'T61300D',
    'T62200D',
)

#: What ``V00300`` is actually made of, per T1.10, and where each piece has a
#: published NIPA table.  ``by_industry`` records whether that table is
#: organised by industry -- which is what Step 2 needs and mostly cannot get.
V00300_COMPONENTS: tuple[tuple[str, str, str, str], ...] = (
    ('Net interest and miscellaneous payments', 'W272RC', 'T61500D A1850C', 'partial'),
    ('Business current transfer payments (net)', 'B029RC', 'T70700', 'none'),
    ("Proprietors' income with IVA and CCAdj", 'A041RC', 'T61200D + T71500', 'partial'),
    ('Rental income of persons', 'A048RC', 'T70900', 'none'),
    ('Corporate profits with IVA and CCAdj', 'A445RC', 'T61600D A445RC', 'full'),
    ('Current surplus of government enterprises', 'A108RC', '(none)', 'none'),
    ('Consumption of fixed capital', 'A262RC', 'T70500', 'partial'),
    ('Statistical discrepancy', 'A030RC', '(none)', 'none'),
)

#: What ``by_industry`` means, since "does NIPA give this by industry" has three
#: answers and collapsing them to two overstates the position either way.
BY_INDUSTRY_MEANING = {
    'full': 'a by-industry table covers the whole component',
    'partial': 'a by-industry table covers part of it; the rest is by legal '
    'form or type',
    'none': 'no by-industry table at any scope',
}


def v00300_components(year: int = YEAR) -> pd.DataFrame:
    """The pieces of ``V00300``, their size, and how far NIPA gets by industry."""
    rows = []
    for label, code, source, by_industry in V00300_COMPONENTS:
        rows.append(
            {
                'component': label,
                'nipa_code': code,
                'value': nipa_line('T11000', code, year),
                'source': source,
                'by_industry': by_industry,
            }
        )
    frame = pd.DataFrame(rows)
    frame['share'] = frame['value'] / frame['value'].sum() * 100
    return frame


def proposed_v00300(year: int = YEAR) -> pd.DataFrame:
    """Each proposed table's root total, and their sum."""
    rows = []
    for table in PROPOSED_V00300_TABLES:
        frame = nipa_flat_table(table, year).frame
        root = frame.loc[frame['level'] == frame['level'].min()].iloc[0]
        rows.append(
            {
                'table': table,
                'root_line': str(root['name']),
                'value': float(root['value']),
            }
        )
    return pd.DataFrame(rows)


#: Per value-added row: the NIPA table with an industry axis, and what that
#: axis actually is.  ``None`` where no table has an industry dimension.
VA_ROW_SOURCES: tuple[tuple[str, str, str | None, str], ...] = (
    (
        'V00100',
        'Compensation of employees',
        'T60200D',
        'by industry, ~BEA summary granularity',
    ),
    (
        'T00OTOP',
        'Other taxes on production, less subsidies',
        None,
        'T30500 is by level of government and kind of tax, no industry axis',
    ),
    (
        'V00300',
        'Gross operating surplus',
        'several',
        'fragmented; see the V00300 breakdown',
    ),
)


def value_added_granularity(year: int = YEAR) -> pd.DataFrame:
    """How far each value-added row can be taken by industry, and at what grain.

    The three rows are in very different positions, so reporting one number for
    "value added by industry" would misdescribe all of them.  ``leaves`` is the
    count of terminal rows in the source table -- the finest industry split
    NIPA publishes for that row.  None of them reach the 402 BEA detail
    industries.
    """
    sut = sut_value_added_totals(year)
    vabas = float(sut['VABAS'])
    rows = []
    for code, label, table, note in VA_ROW_SOURCES:
        leaves: int | None = None
        if table is not None and table != 'several':
            leaves = len(nipa_flat_table(table, year).leaves().frame)
        value = float(sut[code])
        rows.append(
            {
                'row': code,
                'component': label,
                'value': value,
                'share_of_VABAS': value / vabas * 100,
                'industry_table': table or '(none)',
                'leaves': leaves,
                'granularity': note,
            }
        )
    return pd.DataFrame(rows)


def by_industry_coverage(year: int = YEAR) -> pd.Series:
    """``V00300`` split by how far a by-industry NIPA table reaches.

    Indexed by :data:`BY_INDUSTRY_MEANING` keys, in millions.  Reported as
    three numbers rather than one percentage because ``partial`` is the largest
    bucket, and rounding it either up or down would misstate what Step 2 can
    source directly.
    """
    comps = v00300_components(year)
    totals = comps.groupby('by_industry')['value'].sum()
    return totals.reindex(['full', 'partial', 'none']).fillna(0.0)


#: Where a BEA detail code has to appear before an FBS can target it.
DETAIL_CROSSWALK = (
    'bedrock/utils/mapping/activitytosectormapping/NAICS_Crosswalk_BEA_2017_Detail.csv'
)

#: Final-demand codes that were added to the crosswalk as identity rows, which
#: is the precedent the issue points at.  ``F01000`` came with the original
#: final-demand work; ``S00300`` and ``S00900`` were added by ``7a04a71``.
IDENTITY_ROW_PRECEDENT = ('F01000', 'S00300', 'S00900')


def codes_in_detail_crosswalk(codes: tuple[str, ...]) -> pd.DataFrame:
    """Whether each code appears in the BEA detail crosswalk, and on which side.

    An FBS can only target a sector the crosswalk knows about, so this is the
    gate a value-added FBS has to pass before any of the reconciliation above
    can be reproduced per industry.
    """
    from pathlib import Path  # noqa: PLC0415

    crosswalk = pd.read_csv(Path(DETAIL_CROSSWALK), dtype=str)
    return pd.DataFrame(
        {
            'code': list(codes),
            'as_activity': [int((crosswalk['Activity'] == c).sum()) for c in codes],
            'as_sector': [int((crosswalk['Sector'] == c).sum()) for c in codes],
        }
    )


def report(year: int = YEAR) -> str:
    sut = sut_value_added_totals(year)
    checks = pd.DataFrame(c.compare(sut) for c in build_checks(year))
    comps = v00300_components(year)
    proposed = proposed_v00300(year)
    coverage = by_industry_coverage(year)
    total = float(coverage.sum())

    fmt: dict[str | int, object] = {
        'nipa': '{:,.0f}'.format,
        'use_sut': '{:,.0f}'.format,
        'diff': '{:,.0f}'.format,
        'pct': '{:+.4f}'.format,
        'value': '{:,.0f}'.format,
        'share': '{:.1f}'.format,
    }
    lines = [
        f'Use SUT value added vs NIPA control totals, {year}',
        'millions of USD, the unit both sources publish in',
        '',
        'RECONCILIATION',
        checks.to_string(index=False, formatters=fmt),  # type: ignore[arg-type]
        '',
        '  Every row agrees within BEA rounding. Two things to note:',
        '  - GDP lands on VAPRO, not VABAS. The 695,567 between them is taxes',
        '    on products less subsidies, which is the valuation difference.',
        '  - V00300 needs the statistical discrepancy. Net operating surplus',
        '    plus consumption of fixed capital alone is short by ~67,900.',
        '',
        'HOW FAR EACH ROW CAN BE TAKEN BY INDUSTRY',
        value_added_granularity(year).to_string(
            index=False,
            formatters={
                'value': '{:,.0f}'.format,
                'share_of_VABAS': '{:.1f}'.format,
            },
        ),
        '',
        '  V00100 is the well-served row and it is 55% of VABAS. The ceiling',
        '  everywhere is summary, not detail: no NIPA table reaches the 402 BEA',
        '  detail industries, so every row needs an allocation source to get the',
        '  rest of the way. Which source is open per row, not settled:',
        '    V00100   BLS_QCEW total_annual_wages is Class Money in USD at NAICS',
        '             granularity - a direct dollar allocator from an annual',
        '             source. Employment_national_<year> already runs QCEW to',
        '             NAICS_6 if a headcount basis is wanted instead.',
        '    V00300   nothing identified yet.',
        '    T00OTOP  nothing identified yet.',
        '  2017 benchmark ratios are one option, not the default.',
        '',
        'WHAT V00300 IS MADE OF (T1.10), AND HOW FAR NIPA GETS IT BY INDUSTRY',
        comps.to_string(index=False, formatters=fmt),  # type: ignore[arg-type]
        '',
        '  by-industry reach, which is what Step 2 can source directly:',
        *(
            f'    {key:8s} {coverage[key]:>11,.0f}  {coverage[key] / total * 100:5.1f}%'
            f'   {BY_INDUSTRY_MEANING[key]}'
            for key in ('full', 'partial', 'none')
        ),
        '',
        'THE CONSTRUCTION PROPOSED ON #537',
        proposed.to_string(index=False, formatters=fmt),  # type: ignore[arg-type]
    ]
    proposed_total = float(proposed['value'].sum())
    v00300 = float(sut['V00300'])
    lines += [
        '',
        f'  proposed sum {proposed_total:,.0f} against V00300 {v00300:,.0f}',
        f'  short by {v00300 - proposed_total:,.0f}'
        f'  ({(v00300 - proposed_total) / v00300 * 100:.1f}%)',
        '  See the module docstring for the six reasons, largest first.',
    ]

    va_codes = tuple(SUT_VA_ROWS)
    present = codes_in_detail_crosswalk(va_codes + IDENTITY_ROW_PRECEDENT)
    lines += [
        '',
        'ARE THESE CODES IN THE BEA DETAIL CROSSWALK YET?',
        present.to_string(index=False),
        '',
        '  No value-added code is there, on either side. The final-demand codes',
        '  below the line show the precedent: an identity row, the code mapping',
        '  to itself. Step 2 needs the same for V00100/T00OTOP/V00300 before an',
        '  FBS can target them.',
        '',
        '  Note the crosswalk declares SectorSourceName NAICS_2017_Code only, so',
        '  adding non-NAICS value-added codes as identity rows walks straight',
        '  into the code-space problem #567/#568 exist to fix.',
    ]
    return '\n'.join(lines)


def main() -> None:
    print(report())


if __name__ == '__main__':
    main()
