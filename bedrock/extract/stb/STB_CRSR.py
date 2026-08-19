"""Commodity Revenue Stratification Report, Surface Transportation Board.

Rail freight **revenue by commodity**, which is the basis BEA allocates the rail
transportation margin on - 16.5% of ``TRANS`` (#611):

    *"For rail, we purchase the Freight Commodity Statistics from the American
    Association of Railroads which gives us very detailed revenue by product
    shipped by rail. The Surface Transportation Board also publishes this
    information along with the Commodity Revenue Stratification Report. We
    receive these data annually."* - W. Nicolls, BEA, 2026-08-11

**AAR is a commercial subscription; this is the public substitute.** The STB is
the federal economic regulator of freight rail, so the Carload Waybill Sample
that Class I railroads must file with it becomes a public record in derived
form. The CRSR is that derivation.

**It reconciles to BEA almost exactly.** 2017 all-data revenue is 68,926 $M
against a published rail give-up of 68,598 $M in the Margins table - 0.48%
apart. Rail gross output is essentially its freight revenue, so the two are
measuring nearly the same object, which is strong evidence this is the same
underlying data BEA buys from AAR.

Granularity
-----------

Two reports are published per year, and this reads the **5-digit** one:

- ``CRSR2`` - 37 two-digit STCC groups. Too coarse to map onto BEA detail:
  "Chemical Products" alone is 15% of rail revenue.
- ``CRSR5`` - **371 five-digit STCC codes** in 2017, one row per
  (STCC5, car type). This is the useful one.

Rows are stratified by revenue-to-variable-cost ratio into three bands
(``R/VC < 100``, ``100 <= R/VC < 180``, ``R/VC >= 180``). The bands are a
regulatory construct - they mark which traffic is potentially rate-regulated -
and carry no commodity meaning, so revenue is summed across all three.

⚠️ Redaction and how to undo it
-------------------------------

Cells thin enough to disclose a shipper are published as the string
``Redacted``: 33.2% of 5-digit cells in 2017, hiding **4.7% of revenue**
(65,667 released against 68,926 all-data). No STCC5 code is *fully* redacted -
0 of 371 - so every commodity retains a released floor.

The file publishes both ``TOTALS (Released Data)`` and ``TOTALS (All Data)``, so
the redacted mass is recoverable in aggregate even though its commodity
attribution is not. Both are emitted as rows with ``ActivityProducedBy`` set to
the total label, and the ``Suppressed`` flag marks every redacted cell, so a
consumer can scale released revenue up to the all-data total rather than
treating a redacted cell as zero. That is the same treatment the trade margin
needs for suppressed retail cells.
"""

from __future__ import annotations

import io
import re
from typing import Any

import numpy as np
import pandas as pd
import requests

from bedrock.transform.flowbyfunctions import assign_fips_location_system
from bedrock.utils.mapping.location import US_FIPS

#: The page listing every year's workbook. URLs are irregular - 2020 onward sit
#: at the site root, earlier years under ``econdata/``, and 2015-2016 carry a
#: random numeric suffix - so the link is resolved from the index rather than
#: constructed, which also means a new year needs no code change.
CRSR_INDEX_URL = (
    'https://www.stb.gov/reports-data/economic-data/'
    'commodity-revenue-stratification-reports/'
)

STB_BASE_URL = 'https://www.stb.gov'

#: Published in thousands of dollars.
_THOUSANDS = 1_000.0

#: The three revenue-to-variable-cost strata. A regulatory split, not a
#: commodity one, so they are summed.
_REVENUE_COLUMNS = ('rev_low', 'rev_mid', 'rev_high')

_COLUMNS = [
    'STCC5',
    'CommodityName',
    'CarType',
    'CarTypeName',
    'cars_low',
    'tons_low',
    'rev_low',
    'vc_low',
    'cars_mid',
    'tons_mid',
    'rev_mid',
    'vc_mid',
    'cars_high',
    'tons_high',
    'rev_high',
    'vc_high',
]

_REDACTED = 'Redacted'

#: Rows at the foot of the sheet that are totals, not commodities.
_TOTAL_ROW_PATTERN = re.compile(r'TOTAL|Percent', re.IGNORECASE)


def stb_crsr_url_helper(*, year: int, **_: Any) -> list[str]:
    """Resolve the 5-digit workbook URL for *year* off the published index.

    ``build_url`` from the generator is deliberately ignored: the workbook URLs
    are irregular enough across years that constructing one is unreliable, so
    the index page is read and the matching link taken from it.
    """
    response = requests.get(
        CRSR_INDEX_URL, timeout=120, headers={'User-Agent': 'Mozilla/5.0'}
    )
    response.raise_for_status()
    links = re.findall(r'href="([^"]+\.xlsx)"', response.text, re.IGNORECASE)
    matches = [link for link in links if 'CRSR5' in link.upper() and str(year) in link]
    if not matches:
        raise ValueError(
            f'No 5-digit CRSR workbook for {year} on {CRSR_INDEX_URL}. The STB '
            f'publishes 2010 onward; a missing recent year usually means it is '
            f'not released yet rather than that the link moved.'
        )
    link = matches[0]
    return [link if link.startswith('http') else f'{STB_BASE_URL}{link}']


def stb_crsr_call(*, resp: Any, **_: Any) -> list[pd.DataFrame]:
    """Read the single stratification sheet, past its four-row banner."""
    workbook = pd.ExcelFile(io.BytesIO(resp.content))
    df = pd.read_excel(
        io.BytesIO(resp.content),
        sheet_name=workbook.sheet_names[0],
        header=None,
        skiprows=6,
    ).iloc[:, : len(_COLUMNS)]
    df.columns = _COLUMNS
    return [df[df['STCC5'].notna()]]


def stb_crsr_parse(
    *, df_list: list[pd.DataFrame], source: str, year: int | None, **_: Any
) -> pd.DataFrame:
    """Sum the three R/VC strata to one revenue per (STCC5, car type)."""
    df = pd.concat(df_list, sort=False)
    key = df['STCC5'].astype(str).str.strip()
    is_total = key.str.contains(_TOTAL_ROW_PATTERN, na=False)

    revenue = pd.DataFrame(
        {
            column: pd.to_numeric(
                df[column].astype(str).str.replace(',', '').str.strip(),
                errors='coerce',
            )
            for column in _REVENUE_COLUMNS
        }
    )
    redacted_cells = (
        df[list(_REVENUE_COLUMNS)]
        .astype(str)
        .apply(lambda s: s.str.strip() == _REDACTED)
    )

    out = pd.DataFrame(
        {
            'ActivityProducedBy': key,
            'FlowName': df['CommodityName'].astype(str).str.strip(),
            'Description': df['CarTypeName'].astype(str).str.strip(),
            # the rail carrier produces the margin, matching Census_AWTS,
            # Census_ARTS, Census_AIES and the pipeline items
            'FlowAmount': revenue.sum(axis=1, min_count=0) * _THOUSANDS,
            'Suppressed': np.where(
                redacted_cells.any(axis=1),
                redacted_cells.sum(axis=1).astype(str) + ' of 3 strata redacted',
                np.nan,
            ),
        }
    )
    # totals carry no commodity, but they are how a consumer recovers the
    # redacted mass - keep them and let the flow name say what they are
    out.loc[is_total, 'FlowName'] = key[is_total]

    if not is_total.any():
        raise ValueError(
            f'{source} {year} has no TOTALS rows. They are the only way to '
            f'recover the redacted revenue, which is 4.7% of the column, so '
            f'their absence means the sheet layout changed.'
        )

    return (
        out.assign(
            Year=str(year),
            Unit='USD',
            Class='Money',
            SourceName=source,
            Compartment=None,
            ActivityConsumedBy=None,
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

    generateFlowByActivity(source='STB_CRSR', year='2017')
    fba = getFlowByActivity('STB_CRSR', 2017)
