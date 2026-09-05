# Census_ARTS.py (bedrock)
# !/usr/bin/env python3
# coding=utf-8

"""
Annual Retail Trade Survey (ARTS), US Census Bureau.

The annual **level** of the retail trade margin - Step 4c phase 3 of the nowcast
(#612), the retail half of what ``Census_AWTS`` does for wholesale. As there, it
carries no commodity dimension: #610's 2017-anchored rates do the allocation and
this only sizes the total.

**Three workbooks, one flow each.** Retail publishes gross margin, gross margin
as a percentage of sales, and sales as separate files under one vintage
directory, so the URL helper expands the base into three calls. Each file names
its own measure in its first row rather than in a column, which is where the
flow name is read from.

**Coverage is complete and checkable.** The twelve 3-digit rows 441-454 sum
exactly to the published "Retail gross margin, total" row - 1,458,243 million in
2017 - so a parsed year can be checked against a number in the same file.

**ARTS ends at data year 2022,** after which the annual economic surveys were
consolidated into the Annual Integrated Economic Survey; ``Census_AIES`` carries
2023 onward.

⚠️ **Suppression bites harder here than usual.** 2022 gasoline stations (447) is
published as ``S``, and this is a control total, so zeroing it does not just lose
one industry - it silently shrinks the whole retail margin being allocated. The
published total row is in the same workbook and the suppressed cell is better
recovered by subtracting the other eleven from it than by taking the zero as
data. The flag is preserved on every such cell so that step can find them.
"""

from __future__ import annotations

import io
from typing import Any

import numpy as np
import pandas as pd

from bedrock.transform.flowbyfunctions import assign_fips_location_system
from bedrock.utils.mapping.location import US_FIPS

#: Census's retail wording -> the name ``Census_AWTS`` publishes for the same
#: measure. The three sources behind the trade control total (AWTS, ARTS, AIES)
#: have to be read as one series across 2012-2023, and they cannot be if the
#: same quantity arrives under three spellings.
_FLOW_NAMES = {
    'Retail gross margin': 'Gross margins',
    'Retail gross margin as a percentage of sales': (
        'Gross margins as a percent of sales'
    ),
    'Retail sales': 'Sales',
}

#: See ``Census_AWTS._SUPPRESSION_FLAGS``.
_SUPPRESSION_FLAGS = ('S', 'NA', 'x')

_MILLIONS = 1_000_000.0

_PERCENT_ITEM = 'Gross margins as a percent of sales'


def census_arts_url_helper(
    *, build_url: str, config: dict[str, Any], **_: Any
) -> list[str]:
    """One URL per published measure, from the ``files`` block of the config."""
    return [build_url.replace('__file__', f) for f in config['files'].values()]


def census_arts_call(*, resp: Any, **_: Any) -> list[pd.DataFrame]:
    """Read one measure workbook and tag every row with the measure it holds."""
    df = pd.read_excel(io.BytesIO(resp.content), header=3).dropna(
        subset=['Kind of Business']
    )

    # The measure is named in the first row's label rather than in a column:
    # "Retail gross margin, total ……" - everything before the first comma.
    published = str(df['Kind of Business'].iloc[0]).split(',')[0].strip()
    if published not in _FLOW_NAMES:
        raise ValueError(
            f'ARTS workbook leads with the measure {published!r}, which is not '
            f'one of {sorted(_FLOW_NAMES)}. Census has most likely reworded or '
            f'reordered the tables - reconcile _FLOW_NAMES rather than letting '
            f'the rows be filed under the wrong flow.'
        )

    # the leading row is the all-retail total, and is the check the parsed
    # 3-digit rows are summed against; it has no NAICS code of its own
    df = df.copy()
    df.loc[df.index[0], 'NAICS Code'] = 'Total'
    return [df.assign(FlowName=_FLOW_NAMES[published])]


def census_arts_parse(
    *, df_list: list[pd.DataFrame], source: str, year: int | None, **_: Any
) -> pd.DataFrame:
    """Melt the year columns into rows and complete the FBA columns.

    Rows with no NAICS code are dropped: they are the workbook's alternative
    aggregates - "total (excl. motor vehicle and parts dealers)" and GAFO - which
    overlap the 3-digit rows and would double count.
    """
    df = pd.concat(df_list, sort=False).dropna(subset=['NAICS Code'])

    df = (
        df.drop(columns=['Kind of Business'])
        .melt(
            id_vars=['NAICS Code', 'FlowName'],
            var_name='Year',
            value_name='Amount',
        )
        # the retailer produces the trade margin, so it lands in
        # ActivityProducedBy - matching Census_AWTS and Margins_Transport
        .rename(columns={'NAICS Code': 'ActivityProducedBy'})
        .assign(Year=lambda x: x['Year'].astype(str).str[:4])
    )
    df['ActivityProducedBy'] = df['ActivityProducedBy'].astype(str).str.strip()

    if year is not None:
        df = df.loc[df['Year'] == str(year)].copy()
    if df.empty:
        raise ValueError(f'{source} has no rows for {year}')

    amount = df['Amount'].astype(str).str.strip()
    suppressed = amount.isin(_SUPPRESSION_FLAGS)
    df = df.assign(
        Suppressed=np.where(suppressed, amount, np.nan),
        FlowAmount=pd.to_numeric(amount.where(~suppressed, 0), errors='coerce'),
    ).drop(columns='Amount')

    is_percent = df['FlowName'] == _PERCENT_ITEM
    return (
        df.assign(
            FlowAmount=np.where(
                is_percent, df['FlowAmount'], df['FlowAmount'] * _MILLIONS
            ),
            Unit=np.where(is_percent, 'Percent', 'USD'),
            Class='Money',
            SourceName=source,
            Compartment=None,
            FlowType='TECHNOSPHERE_FLOW',
            Location=US_FIPS,
            # provisional pending a source-specific assessment
            DataReliability=5,
            DataCollection=5,
        )
        .pipe(assign_fips_location_system, 2024)
        .reset_index(drop=True)
    )


#: ⚠️ **ARTS publishes two total rows and they are not the same total.** The
#: first is all of retail; the second excludes motor vehicle and parts dealers,
#: because NAICS 441 inventories are dominated by dealer floor plans that Census
#: reports separately. Both are kept and labelled, because the ``F03000`` trade
#: branch wants the whole of retail while a consumer comparing against the
#: 4411/4413 rows wants the exclusive one. Summing children plus either total
#: double-counts.
_INVENTORY_TOTALS = {
    'Retail inventories, total': 'Total',
    'Retail inventories, total (excl. motor vehicle and parts dealers)': (
        'Total excluding 441'
    ),
}


def census_arts_inventories_call(*, resp: Any, **_: Any) -> list[pd.DataFrame]:
    """Read ``invent.xlsx`` and label its two total rows.

    ⚠️ **The sheet is addressed by position, not by name.** Its name is
    ``'Inventories'`` followed by sixteen spaces, and a lookup by the obvious
    string fails.

    ⚠️ **Kind-of-business labels carry dot leaders** -- the run of periods that
    fills the gap to the number in the printed table -- so they are trimmed
    here rather than in every consumer.
    """
    df = pd.read_excel(io.BytesIO(resp.content), sheet_name=0, header=3)
    df.columns = df.columns.astype(str).str.strip()
    df = df.dropna(subset=['Kind of Business']).copy()

    df['Kind of Business'] = (
        df['Kind of Business']
        .astype(str)
        .str.replace(r'[.…\s]+$', '', regex=True)
        .str.strip()
    )
    code = df['NAICS Code'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()

    labelled = df['Kind of Business'].map(_INVENTORY_TOTALS)
    is_total = labelled.notna()
    if int(is_total.sum()) != len(_INVENTORY_TOTALS):
        raise ValueError(
            f'ARTS invent.xlsx matched {int(is_total.sum())} of '
            f'{len(_INVENTORY_TOTALS)} expected total rows. Census has reworded '
            f'them - reconcile _INVENTORY_TOTALS rather than letting a total be '
            f'read as an industry, which would double-count the column.'
        )
    df['NAICS Code'] = code.where(~is_total, labelled)

    keep = is_total | code.str.fullmatch(r'\d{3,6}')
    return [df.loc[keep].reset_index(drop=True)]


def census_arts_inventories_parse(
    *, df_list: list[pd.DataFrame], source: str, year: int | None, **_: Any
) -> pd.DataFrame:
    """Melt ``invent.xlsx``'s year columns into a long FBA of **stock levels**.

    ``ActivityConsumedBy`` is the holding NAICS industry, matching
    ``Census_ASM_Inventories`` and ``Census_AWTS_Inventories`` so the three
    branches of ``F03000`` join without a transpose.

    ⚠️ **STOCK LEVELS, NOT CHANGES, and end-of-year ones.** Differencing these
    across years imports the holding gains that CIPI excludes through the
    inventory valuation adjustment --
    ``analysis/nowcasting/inventories_estimation_plan.md`` measures that on FIWS,
    where differencing gives -887 against a true -5,679. Structure from here,
    level from NIPA.

    ⚠️ **The NAICS rows nest**, 3-digit parents over 4- and 5-digit children,
    and 441 has children 4411 and 4413 but no 4412 -- the
    ``parent-incompleteChild`` hierarchy ``source_catalog.yaml`` already records
    for ``Census_ARTS``. Summing unfiltered double-counts.
    """
    df = pd.concat(df_list, sort=False)

    df = (
        df.drop(columns=['Kind of Business'])
        .melt(id_vars=['NAICS Code'], var_name='Year', value_name='Amount')
        .assign(Year=lambda x: x['Year'].astype(str).str[:4])
        .rename(columns={'NAICS Code': 'ActivityConsumedBy'})
    )
    df = df[df['Year'].str.fullmatch(r'\d{4}')].copy()
    df['ActivityConsumedBy'] = df['ActivityConsumedBy'].astype(str).str.strip()

    if year is not None:
        df = df.loc[df['Year'] == str(year)].copy()
    if df.empty:
        raise ValueError(f'{source} has no rows for {year}')

    amount = df['Amount'].astype(str).str.strip()
    suppressed = amount.isin(_SUPPRESSION_FLAGS)
    df = df.assign(
        Suppressed=np.where(suppressed, amount, np.nan),
        FlowAmount=pd.to_numeric(amount.where(~suppressed, 0), errors='coerce'),
    ).drop(columns='Amount')
    df = df[df['FlowAmount'].notna()].copy()

    return (
        df.assign(
            FlowAmount=df['FlowAmount'] * _MILLIONS,
            FlowName='Inventories',
            Unit='USD',
            Class='Money',
            SourceName=source,
            ActivityProducedBy=None,
            Description='Inventories, end of year, at cost or market',
            Compartment=None,
            FlowType='TECHNOSPHERE_FLOW',
            Location=US_FIPS,
            # provisional pending a source-specific assessment
            DataReliability=5,
            DataCollection=5,
        )
        .pipe(assign_fips_location_system, 2024)
        .reset_index(drop=True)
    )


if __name__ == '__main__':
    from bedrock.extract.flowbyactivity import getFlowByActivity
    from bedrock.extract.generateflowbyactivity import generateFlowByActivity

    generateFlowByActivity(source='Census_ARTS', year='2012-2022')
    fba = getFlowByActivity('Census_ARTS', 2017)
