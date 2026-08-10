# Census_AIES.py (bedrock)
# !/usr/bin/env python3
# coding=utf-8

"""
Annual Integrated Economic Survey (AIES), US Census Bureau.

From data year 2023 the Census Bureau consolidated its annual economic surveys -
the Annual Wholesale Trade Survey, the Annual Retail Trade Survey, the Annual
Survey of Manufactures and the Service Annual Survey among them - into one
integrated survey. ``Census_AWTS`` and ``Census_ARTS`` therefore stop at 2022 and
this carries the trade margin control total forward (#612).

**What is taken.** ``timeseries/aies/basic`` publishes ``RCPT_GM_DVAL`` (gross
margin) and ``RCPT_TOT_VAL`` (sales, value of shipments, or revenue) by NAICS,
which is the same pair of measures the two predecessor surveys published. Gross
margin as a percent of sales is not published and is not taken: it is the ratio
of the two, and deriving it here would invent a third series that has to agree
with the first two.

⚠️ **Type of operation is not a detail here.** Wholesale gross margin is
published under ``TYPOP`` code ``1X``, merchant wholesalers excluding
manufacturers' sales branches and offices, and is **zero** under the all-types
code ``00``; retail is the other way round, published under ``00``. Reading
either sector at the wrong code returns a well-formed zero rather than an error,
which would quietly delete an entire side of the trade margin. The codes are
therefore pinned per sector in :data:`_TYPE_OF_OPERATION` and a sector that comes
back empty raises.

That ``1X`` is also what makes 2023 splice onto AWTS: the AWTS workbook is the
``nomsbo`` table, merchant wholesalers only, so the two are the same basis rather
than merely similar. Measured on the published NAICS 42 row, wholesale gross
margin runs 1,618.0 $B in 2022 on AWTS against 1,601.2 $B here, a 1.0% fall on
sales down 2.5%, and the margin *rate* moves 20.10% to 20.39% - continuous, so
the consolidation did not move the wholesale basis.

⚠️ **Compare like with like: the published total row, not the sum of the
sub-industries.** Summing four-digit codes gives 1,358.2 $B for AWTS 2022 because
suppression removes 16% of that year, so a 4-digit-to-total comparison across the
splice shows a spurious +17.9% jump where the real move is -1.0%. The retail side
splices less cleanly on the same test - its rate steps 31.3% to 34.2% - which is
open; see the plan's "The splice is the seam that matters".

**2023 only, so far.** Every other year returns 204 No Content - AIES does not
carry the predecessor surveys' back-years, and 2024 is not yet published. The
nowcast window runs to 2024, so the last year of the trade control total has no
observed source and must be extrapolated until the next AIES release.

**NAICS vintage.** AIES 2023 publishes the pre-2022 retail structure - 452
general merchandise stores and 454 nonstore retailers both appear - so it joins
the 2017-anchored rates without recoding.
"""

from __future__ import annotations

import json
from typing import Any

import pandas as pd

from bedrock.transform.flowbyfunctions import assign_fips_location_system
from bedrock.utils.logging.flowsa_log import log
from bedrock.utils.mapping.location import US_FIPS

#: NAICS prefix -> the ``TYPOP`` code the margin is published under. See the
#: module docstring: the wrong code returns zeros rather than an error.
_TYPE_OF_OPERATION = {
    '42': '1X',  # merchant wholesalers, excluding MSBOs
    '44': '00',  # retail, all types of operation
    '45': '00',
}

#: Published variable -> the flow name ``Census_AWTS`` and ``Census_ARTS`` use.
_FLOW_NAMES = {
    'RCPT_GM_DVAL': 'Gross margins',
    'RCPT_TOT_VAL': 'Sales',
}

#: AIES money variables are published in thousands of dollars.
_THOUSANDS = 1_000.0


def census_aies_call(*, resp: Any, **_: Any) -> list[pd.DataFrame]:
    """Convert the API response to a dataframe; 204 means the year is absent."""
    if resp.status_code == 204:
        log.warning(f'No AIES content for {resp.url}')
        return [pd.DataFrame()]
    payload = json.loads(resp.text)
    return [pd.DataFrame(payload[1:], columns=payload[0])]


def census_aies_parse(
    *, df_list: list[pd.DataFrame], source: str, year: int | None, **_: Any
) -> pd.DataFrame:
    """Keep the trade rows at their published type of operation, then melt."""
    df = pd.concat(df_list, sort=False)
    if df.empty:
        raise ValueError(
            f'{source} returned no rows for {year}. AIES currently publishes '
            f'2023 only - the predecessor surveys Census_AWTS and Census_ARTS '
            f'carry 2012-2022, and later years are not released yet.'
        )

    df['NAICS'] = df['NAICS'].astype(str).str.strip()
    df['sector'] = df['NAICS'].str[:2]

    kept = []
    for prefix, typop in _TYPE_OF_OPERATION.items():
        rows = df[(df['sector'] == prefix) & (df['TYPOP'] == typop)]
        if rows.empty:
            raise ValueError(
                f'{source} {year} has no NAICS {prefix} rows at type of '
                f'operation {typop}. That is where the margin for this sector '
                f'is published; the other codes carry zeros, so an empty result '
                f'here would silently drop the sector from the control total.'
            )
        kept.append(rows)
    df = pd.concat(kept, ignore_index=True)

    df = (
        df.melt(
            id_vars=['NAICS', 'TYPOP'],
            value_vars=list(_FLOW_NAMES),
            var_name='Variable',
            value_name='FlowAmount',
        )
        # the trade sector produces the margin, so it lands in
        # ActivityProducedBy - matching Census_AWTS, Census_ARTS and
        # Margins_Transport
        .rename(columns={'NAICS': 'ActivityProducedBy', 'TYPOP': 'Description'})
        .assign(
            FlowName=lambda x: x['Variable'].map(_FLOW_NAMES),
            Year=str(year),
        )
        .drop(columns='Variable')
    )
    df['FlowAmount'] = pd.to_numeric(df['FlowAmount'], errors='coerce') * _THOUSANDS

    return (
        df.assign(
            Unit='USD',
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

    generateFlowByActivity(source='Census_AIES', year='2023')
    fba = getFlowByActivity('Census_AIES', 2023)
