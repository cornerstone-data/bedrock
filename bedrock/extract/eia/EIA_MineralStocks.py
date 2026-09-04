"""EIA end-of-year mineral stocks: crude oil, natural gas and coal.

The mining branch of the change-in-private-inventories column (`F03000`,
#529/#660). ``analysis/nowcasting/inventories_estimation_plan.md`` measures why
this branch needs a physical source: NIPA calls it "mining, utilities, and
construction", but **every published cell outside the trade crosswalk's reach is
mining** -- there are no utilities or construction commodities in the column at
all.

⚠️ **STOCK LEVELS IN PHYSICAL UNITS, NOT CHANGES AND NOT DOLLARS.** Two separate
reasons, and both matter:

* Differencing stocks across years imports holding gains, which CIPI excludes
  through the inventory valuation adjustment. The plan measures that error on
  FIWS: differencing gives -887 against a true -5,679, out by roughly six times.
* Converting physical to dollars needs a price, which reintroduces the same
  holding gains from the other direction.

So this ships in ``MBBL`` / ``MMcf`` / ``thousand short tons`` and is used for
**structure**, with the level from NIPA.

✅ **The levels were checked against the benchmark on 2026-08-27** and they are
comparable -- same sign on every cell, coal at 87% of its published cell and oil
and gas at 55%. See the plan's §Levels checked against the benchmark.

⚠️ **Each commodity answers on a different route shape**, which is why this
cannot be one url template:

============  =========================================  ==========================
commodity     route                                      shape
============  =========================================  ==========================
crude oil     ``petroleum/stoc/cu``, ``duoarea=NUS``     annual directly
natural gas   ``natural-gas/stor/sum``, ``process=SAO``  **monthly only** -- annual
                                                         returns zero rows, so
                                                         December is taken
coal          ``total-energy``, *Coal Stocks, Total*     needs an explicit period
                                                         sort or the window
                                                         returns only its last year
============  =========================================  ==========================

❌ **The v2 ``coal/`` routes carry no stocks series** -- shipments, production,
prices and reserves only. Coal stocks are in the Monthly Energy Review, which is
served under ``total-energy``.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from bedrock.transform.flowbyfunctions import assign_fips_location_system
from bedrock.utils.mapping.location import US_FIPS

log = logging.getLogger(__name__)

#: ``process`` code for working gas. ⚠️ **Not ``SAT``.** ``SAT`` is total
#: underground storage and includes **base gas** -- the cushion that is never
#: withdrawn and is a fixed asset rather than an inventory. Base gas is 4.4 Tcf
#: of a 7.4 Tcf total and barely moves, so ``SAT`` dilutes the signal with a
#: constant.
GAS_PROCESS = 'SAO'

#: The Monthly Energy Review series, matched on its description prefix.
#: ⚠️ **"Coal Stocks, Total" is the one to take** -- the electric-power and
#: producers-and-distributors series are its components and summing all three
#: double-counts.
COAL_SERIES_PREFIX = 'Coal Stocks, Total, End of Period'

#: What each commodity is, as a BEA 2017 detail commodity. The mapping is not
#: applied here -- an FBA carries the source's own vocabulary -- but it is
#: recorded because it is the whole reason this source is pulled.
COMMODITY_TO_BEA = {
    'crude oil': '211000',
    'natural gas': '211000',
    'coal': '212100',
}


def eia_mineral_stocks_url_helper(*, build_url: str, year: str, **_: Any) -> list[str]:
    """One url per commodity, because each route answers differently.

    ⚠️ **The key arrives inside ``build_url``, not in ``config``.**
    ``generateflowbyactivity`` substitutes ``__apiKey__`` into the yaml's
    ``base_url`` before calling this, so the key has to be read back out of it --
    ``config['api_key']`` is empty and yields a 403 that looks like a
    permissions problem rather than a missing substitution.
    """
    key = build_url.partition('api_key=')[2].partition('&')[0]
    yr = int(year)
    return [
        (
            f'https://api.eia.gov/v2/petroleum/stoc/cu/data/?api_key={key}'
            f'&frequency=annual&data[0]=value&facets[duoarea][]=NUS'
            f'&start={yr}&end={yr}&length=500'
        ),
        (
            f'https://api.eia.gov/v2/natural-gas/stor/sum/data/?api_key={key}'
            f'&frequency=monthly&data[0]=value&facets[process][]={GAS_PROCESS}'
            f'&start={yr}-12&end={yr}-12&length=500'
        ),
        (
            f'https://api.eia.gov/v2/total-energy/data/?api_key={key}'
            f'&frequency=annual&data[0]=value&start={yr}&end={yr}'
            f'&sort[0][column]=period&sort[0][direction]=asc&length=5000'
        ),
    ]


#: Which commodity each v2 route carries, keyed by a fragment of its path.
#: ⚠️ **The route is the only reliable discriminator.** ``petroleum/stoc/cu`` and
#: ``natural-gas/stor/sum`` return **identical column sets** -- same eleven keys,
#: including ``product`` and ``process`` -- so a parser that recognises a frame
#: by its schema cannot tell crude from gas, and silently labels one as the
#: other. It did: the first version tagged crude oil as natural gas while
#: producing entirely correct numbers.
ROUTE_TO_COMMODITY = {
    'petroleum/stoc/cu': 'crude oil',
    'natural-gas/stor/sum': 'natural gas',
    'total-energy': 'coal',
}


def eia_mineral_stocks_call(*, resp: Any, **_: Any) -> pd.DataFrame:
    """The v2 response's ``response.data`` rows, tagged with their route."""
    payload = resp.json().get('response', {})
    frame = pd.DataFrame(payload.get('data', []))
    url = str(getattr(resp, 'url', ''))
    commodity = next(
        (name for path, name in ROUTE_TO_COMMODITY.items() if path in url), None
    )
    if commodity is None:
        raise ValueError(
            f'EIA_MineralStocks got a response from an unrecognised route: '
            f'{url.split("?")[0]}. Add it to ROUTE_TO_COMMODITY rather than '
            f'letting the parser guess from the schema, which cannot tell '
            f'crude from gas.'
        )
    frame['commodity'] = commodity
    return frame


def eia_mineral_stocks_parse(
    *, df_list: list[pd.DataFrame], year: int, **_: Any
) -> pd.DataFrame:
    """Long FBA: one row per commodity, the end-of-year stock in its own unit.

    ⚠️ **Each frame is tagged with its route by :func:`eia_mineral_stocks_call`,
    and that tag is the only thing that identifies the commodity.** The petroleum
    and natural-gas routes return identical column sets, so neither the schema
    nor the position in ``df_list`` can tell them apart.
    """
    records = []
    for frame in df_list:
        if frame.empty:
            continue
        commodity = str(frame['commodity'].iloc[0])
        rows = frame
        if commodity == 'coal':
            rows = frame[
                frame['seriesDescription']
                .astype(str)
                .str.startswith(COAL_SERIES_PREFIX)
            ]
        elif 'duoarea' in frame.columns:
            rows = frame[frame['duoarea'].astype(str) == 'NUS']
        unit_column = 'units' if 'units' in frame.columns else 'unit'
        for _index, row in rows.iterrows():
            records.append(
                (commodity, row['value'], row.get(unit_column), row['period'])
            )

    long = pd.DataFrame(
        records, columns=['FlowName', 'FlowAmount', 'Unit', 'period']
    ).drop(columns='period')
    long['FlowAmount'] = pd.to_numeric(long['FlowAmount'], errors='coerce')
    missing = int(long['FlowAmount'].isna().sum())
    long = long[long['FlowAmount'].notna()].copy()

    long['Description'] = long['FlowName'].map(
        lambda name: f'End-of-year stocks, {name}, BEA {COMMODITY_TO_BEA[name]}'
    )
    long['ActivityProducedBy'] = long['FlowName'].map(COMMODITY_TO_BEA.get)
    long['ActivityConsumedBy'] = None
    long['Year'] = year
    long['Location'] = US_FIPS
    # ⚠️ Physical, not Money. A `Class: Money` selection must not sweep these up
    # -- converting them needs a price, which reintroduces the holding gains the
    # inventory valuation adjustment exists to strip.
    long['Class'] = 'Other'
    long['FlowType'] = 'TECHNOSPHERE_FLOW'

    long = assign_fips_location_system(long, year)
    long['SourceName'] = 'EIA_MineralStocks'
    long['DataReliability'] = 5
    long['DataCollection'] = 5
    long['Compartment'] = None

    log.info(
        'EIA_MineralStocks %s: %s rows over %s commodities; %s dropped as '
        'non-numeric.',
        year,
        len(long),
        long['FlowName'].nunique(),
        missing,
    )
    return long
