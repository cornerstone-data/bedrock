# Census_AWTS.py (bedrock)
# !/usr/bin/env python3
# coding=utf-8

"""
Annual Wholesale Trade Survey (AWTS), US Census Bureau.

Table 4, purchases and gross margins for merchant wholesalers. This is the
annual **level** of the wholesale trade margin - Step 4c phase 3 of the nowcast
(#612). It carries no commodity dimension at all: the allocation across
commodities comes from the 2017-anchored rates built in #610, and this only says
how big the total being allocated is in each year.

**One workbook holds every year.** The 2022 vintage publishes 1992-2022 as
columns, so ``call_all_years`` reads it once and each year is sliced out of the
result - the same shape as ``BTS_FAF``.

**AWTS ends at data year 2022.** From 2023 the annual economic surveys were
consolidated into the Annual Integrated Economic Survey, so ``Census_AIES``
takes over. The two are on the same basis - see ``_TYPE_OF_OPERATION`` below.

⚠️ **"Purchases" is not sales.** The published table has three data items:
``Purchases``, ``Gross margins`` and ``Gross margins as a percent of sales``.
Purchases are the cost of goods bought for resale, so sales are roughly
purchases *plus* the margin, not the purchases figure itself. Measured on 2017,
``Gross margins / Gross margins as a percent of sales`` agrees with
``Purchases + Gross margins`` to within 1% across every kind of business (0.978
to 1.000, with NAICS 42 at 0.997) - the residual being the inventory change
between what was bought and what was sold.

The upstream flowsa method relabelled ``Purchases`` as ``Sales`` when reading
this table. That understates sales by the margin itself and so overstates any
rate computed from it by about a fifth at the NAICS 42 level. The three items
are therefore emitted here under the names Census publishes them under, and
sales are left to be derived where the basis can be stated - see
``Margins_Trade_source.yaml``.
"""

from __future__ import annotations

import io
from typing import Any

import numpy as np
import pandas as pd

from bedrock.transform.flowbyfunctions import assign_fips_location_system
from bedrock.utils.mapping.location import US_FIPS

#: The only type of operation in this table, and the one the margin belongs to.
#:
#: ``nomsbo`` in the filename is "no manufacturers' sales branches and offices":
#: the table covers merchant wholesalers only. That is not a limitation to work
#: around, it is the right cut - an MSBO is a manufacturer's own outlet, and its
#: markup is already inside the manufacturer's commodity rather than being trade
#: margin. It also matches ``Census_AIES``'s ``TYPOP`` code ``1X``, which is what
#: lets the two series splice at 2023.
_TYPE_OF_OPERATION = (
    "Merchant Wholesalers, except manufacturers' sales branches and offices"
)

#: Census marks these instead of a number. Bedrock's convention is to zero the
#: amount and flag it, so ``estimate_suppressed`` can fill it later.
#:
#: ⚠️ A suppressed cell zeroes a *component of a control total*, which is not the
#: usual harmless case: summing the parts then silently under-counts the whole.
#: The published all-wholesale row is the total to trust, and a suppressed
#: sub-industry is better recovered by subtraction from it than by treating the
#: zero as data.
_SUPPRESSION_FLAGS = ('S', 'NA', 'x')

#: Published in millions of dollars; percentages are published as percentages.
_MILLIONS = 1_000_000.0

_PERCENT_ITEM = 'Gross margins as a percent of sales'


def census_awts_call(*, resp: Any, **_: Any) -> list[pd.DataFrame]:
    """Read table 4 and cut the footnotes off the bottom."""
    df = (
        pd.read_excel(io.BytesIO(resp.content), header=3)
        # row 0 of the sheet body is a spacer under the header
        .drop(index=0).reset_index(drop=True)
    )
    df.columns = df.columns.astype(str).str.strip()

    # everything from the "Notes:" row down is prose, not data
    first_column = df.iloc[:, 0].astype(str)
    notes = df.index[first_column.str.contains('Notes', na=False)]
    if len(notes):
        df = df.iloc[: notes[0]]

    return [df]


def census_awts_parse(
    *, df_list: list[pd.DataFrame], source: str, year: int | None, **_: Any
) -> pd.DataFrame:
    """Melt the year columns into rows and complete the FBA columns.

    ``call_all_years: True`` means the generator parses once with ``year=None``
    and slices each year out itself, so the whole frame comes back in that case.
    """
    df = pd.concat(df_list, sort=False)

    operations = set(df['Type of Operation'].dropna().unique())
    if operations != {_TYPE_OF_OPERATION}:
        raise ValueError(
            f'{source} table 4 is expected to cover merchant wholesalers only, '
            f'but publishes types of operation {sorted(operations)}. The margin '
            f'basis has changed - reconcile it against Census_AIES TYPOP 1X '
            f'before using either as a control total.'
        )

    naics, item = df.columns[0], df.columns[1]
    df = (
        df.drop(columns=['Kind of Business', 'Type of Operation'])
        .melt(id_vars=[naics, item], var_name='Year', value_name='Amount')
        # year columns are labelled "2017r" when revised
        .assign(Year=lambda x: x['Year'].astype(str).str[:4])
        .rename(columns={naics: 'ActivityProducedBy', item: 'FlowName'})
    )
    # the wholesaler produces the trade margin, so it lands in
    # ActivityProducedBy - the same placement Margins_Transport gives the
    # freight mode, which is what lets #613 read TRADE and TRANS alike
    df['ActivityProducedBy'] = df['ActivityProducedBy'].astype(str).str.strip()
    df['FlowName'] = df['FlowName'].astype(str).str.strip()

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


if __name__ == '__main__':
    from bedrock.extract.flowbyactivity import getFlowByActivity
    from bedrock.extract.generateflowbyactivity import generateFlowByActivity

    generateFlowByActivity(source='Census_AWTS', year='2012-2022')
    fba = getFlowByActivity('Census_AWTS', 2017)
