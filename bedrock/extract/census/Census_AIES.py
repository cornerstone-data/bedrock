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
import os
from typing import Any

import pandas as pd

from bedrock.transform.flowbyfunctions import assign_fips_location_system
from bedrock.utils.io.gcp import (
    gcs_extract_input_sub_bucket_from_kwargs,
    load_from_gcs,
)
from bedrock.utils.io.local_extract_input_data import load_local_extract_input_dir
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


def _census_aies_filename(year: str | int) -> str:
    return f'Census_AIES_{year}.csv'


def census_aies_call(*, resp: Any, **kwargs: Any) -> list[pd.DataFrame]:
    """Convert the API response to a dataframe; 204 means the year is absent.

    The raw table is also written under ``extract/input_data/Census_AIES/`` so it
    can be staged to GCS.  AIES needs an API key, and CI has none - without a
    cached copy every AIES-backed test fails there with ``APIError`` rather than
    on anything about the data.  See :func:`census_aies_load_gcs`.
    """
    if resp.status_code == 204:
        log.warning(f'No AIES content for {resp.url}')
        return [pd.DataFrame()]
    payload = json.loads(resp.text)
    df = pd.DataFrame(payload[1:], columns=payload[0])
    out_dir = load_local_extract_input_dir(kwargs)
    df.to_csv(os.path.join(out_dir, _census_aies_filename(kwargs['year'])), index=False)
    return [df]


def census_aies_load_gcs(**kwargs: Any) -> list[pd.DataFrame]:
    """Load the cached AIES table from local ``input_data``, or GCS if missing.

    This is the path CI takes.  ``Census_AWTS`` and ``Census_ARTS`` need no key
    and so regenerate anywhere, but AIES does, which is why the 2023 leg of the
    trade margin has to come from the cache rather than from Census.
    """
    return [
        load_from_gcs(
            name=_census_aies_filename(kwargs['year']),
            sub_bucket=gcs_extract_input_sub_bucket_from_kwargs(kwargs),
            local_dir=load_local_extract_input_dir(kwargs),
            loader=pd.read_csv,
        )
    ]


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


#: AIES expense variables, mapped to the expense each names.  The keys are the
#: successor series to :data:`~Census_ASM.ASM_EXPENSE_FLOWS`, and the two line up
#: one for one on the concepts the materials work needs:
#: ``EXPS_MAT_DVAL`` is ASM's ``CSTMPRT`` and ``EXPS_FUEL_VAL`` its ``CSTFU``.
#:
#: ✅ **Verified additive against the published control.**  ``EXPS_MAT_DVAL +
#: EXPS_FUEL_VAL + EXPS_ELEC_VAL + EXPS_CONTRACT_VAL + EXPS_RESALE_VAL`` sums to
#: ``EXPS_CSTMTOT_DVAL`` at 99.77% in aggregate, median per-industry ratio
#: 1.0000, within 1% for 324 of 360 manufacturing industries -- the same
#: decomposition ASM's ``CSTMTOT`` has, so the splice is a scope match rather
#: than an approximation.
AIES_EXPENSE_FLOWS = {
    'EXPS_MAT_DVAL': 'Cost of materials, parts and containers',
    'EXPS_FUEL_VAL': 'Cost of fuels',
    'EXPS_ELEC_VAL': 'Cost of purchased electricity',
    'EXPS_CONTRACT_VAL': 'Cost of contract work',
    'EXPS_RESALE_VAL': 'Cost of goods purchased for resale',
    'EXPS_ADVERT_VAL': 'Purchased advertising services',
    'EXPS_COMMSVC_VAL': 'Purchased communication services',
    'EXPS_DATAPROC_VAL': 'Purchased data processing and hosting services',
    'EXPS_PROFTECH_VAL': 'Purchased professional and technical services',
    'EXPS_TEMPSTAF_VAL': 'Purchased personnel supply services',
    'EXPS_REFUSE_VAL': 'Purchased refuse removal services',
    'EXPS_MACH_REP_VAL': 'Purchased machinery repair and maintenance',
    'EXPS_BUILD_REP_VAL': 'Purchased building repair and maintenance',
    'EXPS_RENT_BUILD_VAL': 'Rental of buildings',
    'EXPS_RENT_MACH_VAL': 'Rental of machinery and equipment',
    'EXPS_EXSOFT_VAL': 'Purchased software expensed',
    'EXPS_COMPTR_OTHEQ_VAL': 'Purchased computers and peripherals expensed',
    'EXPS_OTHER_VAL': 'All other operating expenses',
}

#: Totals of the cells beside them, kept so a consumer can check additivity.
#: ⚠️ Summing this FBA unfiltered counts the table several times over.
AIES_EXPENSE_CONTROLS = ('EXPS_CSTMTOT_DVAL', 'EXPS_TOT_DVAL', 'RCPT_TOT_VAL')


def census_aies_expenses_parse(
    *, df_list: list[pd.DataFrame], year: int, **_: Any
) -> pd.DataFrame:
    """Format the AIES expense-by-kind table into a long FBA.

    ``ActivityConsumedBy`` is the purchasing NAICS industry and ``FlowName`` the
    expense kind, matching :func:`~Census_ASM.asm_expenses_parse` exactly so the
    two surveys stack into one annual panel.

    ⚠️ **The expense detail is manufacturing only, at every NAICS level.**  This
    is the single most important thing to know before reaching for this source.
    AIES lists 883 six-digit industries, but only sectors 31-33 carry any
    expense cell: 42 publishes ``EXPS_TOT_DVAL`` alone, and 21, 22, 23 and
    51-81 publish **nothing at all**, at 2-, 3-, 4-, 5- and 6-digit alike.  So
    this does *not* source the service industries that drift worst
    (analysis/nowcasting/intermediate_estimation_plan.md, §Where the drift
    sits); it confirms #564's finding rather than overturning it.

    ⚠️ **And it does not cover mining**, which ``Census_EC_MatFuel`` does.  The
    2023 observation is narrower than the census it extends.

    ⚠️ **2023 is the only year.**  2022 predates the consolidated survey and
    2024 is not published; both return ``204 No Content``.  So the annual
    materials panel is census 2017, ASM 2018-2021, census 2022, AIES 2023, and
    **2024-2025 are unobserved** -- which is exactly the span the nowcast has to
    reach and cannot source.

    ⚠️ **Every NAICS level is kept**, including the ``31-33`` rollup, on the same
    reasoning as :func:`~Census_ASM.asm_expenses_parse`: the parents are the only
    available control, and ``'31-33'`` is five characters, so match ``^\\d+$``
    before filtering on length.
    """
    df = pd.concat(df_list, sort=False)

    value_columns = [
        column
        for column in (*AIES_EXPENSE_FLOWS, *AIES_EXPENSE_CONTROLS)
        if column in df.columns
    ]
    long = df.melt(
        id_vars=['NAICS'],
        value_vars=value_columns,
        var_name='FlowName',
        value_name='FlowAmount',
    ).rename(columns={'NAICS': 'ActivityConsumedBy'})
    long['FlowAmount'] = pd.to_numeric(long['FlowAmount'], errors='coerce')
    # ⚠️ A missing cell is withheld; a published zero is a real zero, and AIES
    # publishes a great many of them because the expense block is manufacturing
    # only. Dropping both would delete the evidence for that; only the withheld
    # ones go.
    withheld = int(long['FlowAmount'].isna().sum())
    long = long[long['FlowAmount'].notna()].copy()

    long['Description'] = long['FlowName'].map(
        AIES_EXPENSE_FLOWS
        | {code: f'{code} (control)' for code in AIES_EXPENSE_CONTROLS}
    )
    long['ActivityProducedBy'] = None
    long['Year'] = year
    long['Location'] = US_FIPS
    long['Unit'] = 'Thousand USD'
    long['Class'] = 'Money'
    long['FlowType'] = 'TECHNOSPHERE_FLOW'

    long = assign_fips_location_system(long, year)
    long['SourceName'] = 'Census_AIES_Expenses'
    long['DataReliability'] = 5
    long['DataCollection'] = 5
    long['Compartment'] = None

    log.info(
        'Census_AIES_Expenses %s: %s rows over %s industries and %s expense '
        'kinds; %s cells withheld and dropped.',
        year,
        len(long),
        long['ActivityConsumedBy'].nunique(),
        long['FlowName'].nunique(),
        withheld,
    )
    return long
