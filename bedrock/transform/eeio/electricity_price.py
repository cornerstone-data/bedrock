"""Annual electricity price by buyer, 2017-2024 (#901).

The price a buyer pays per kWh delivered, which the split into generation and
delivery (#902) divides each bill by. For a manufacturing industry::

    price_i(t) = relative_i(t) x level(t)
    level(t)   = EIA industrial price(t) x MECS-to-EIA ratio(t)

MECS sees the cross-section in its two surveys, and EIA publishes the level each
year:

- **Survey prices** are MECS Table 7.10 dollars over Table 7.7 kWh, both purchases
  from any supplier, so they include delivery. A row with no total falls back to its
  local-utility values, and a row with neither takes its parent's remainder.
- **Relative prices** anchor on MECS 2022. Before 2022 they move geometrically toward
  MECS 2018, reached in 2018 and held to 2017. An industry whose two surveys don't
  publish the same row stays at its 2022 relative, because the two prices would
  measure different things.
- **The level** reproduces the industries' average MECS price in both survey years
  (:func:`survey_average`). The ratio to EIA moves geometrically between them and
  holds outside.

So MECS is reproduced in 2022, and in 2018 wherever both surveys publish the row. Every
year, kWh-weighted relative prices average 1, so the industries' prices average the
level.

Every other buyer pays its EIA class price (:func:`class_price`), because MECS covers
manufacturing only and CBECS has no NAICS codes.

⚠️ These are per kWh **delivered**, as published. The allocator's class kWh are scaled
to eGRID generation and include losses, so convert before comparing the two.
"""

from __future__ import annotations

import functools
import typing as ta
from pathlib import Path

import pandas as pd

SURVEYS: tuple[int, int] = (2018, 2022)
EARLY_SURVEY, ANCHOR_SURVEY = SURVEYS

NATIONAL_ROW = '31-33'

#: Table 7.10 and Table 7.7 spell the total differently.
USD_TOTAL_FLOW = 'Electricity Total'
KWH_TOTAL_FLOW = 'Electricity total'
LOCAL_UTILITY_FLOW = 'Electricity from Local Utility'

#: A parent's remainder below this share of its kWh is mostly rounding.
MIN_REMAINDER_KWH_SHARE = 0.02
#: Years pooled in the generation price, ending in the year itself.
GENERATION_PRICE_YEARS = 3

#: A remainder priced outside this range of its parent is treated as rounding.
REMAINDER_PRICE_RANGE = (0.5, 2.0)

#: MECS 2022 merges these NAICS 2017 codes; MECS 2018 still publishes them.
NAICS_OVERLAY: dict[str, str] = {
    '322121': '322120',
    '322122': '322120',
    '336111': '336110',
    '336112': '336110',
}

_CROSSWALK = (
    Path(__file__).resolve().parents[2]
    / 'utils'
    / 'mapping'
    / 'naics'
    / 'NAICS_to_BEA_Crosswalk_2017.csv'
)


def _published(usd: pd.Series, kwh: pd.Series) -> pd.DataFrame:
    """Rows with positive dollars and kWh."""
    frame = pd.DataFrame({'usd': usd, 'kwh': kwh}).astype(float)
    return frame[(frame['usd'] > 0) & (frame['kwh'] > 0)]


def mecs_table_from_flows(
    usd_total: pd.Series,
    kwh_total: pd.Series,
    usd_local: pd.Series,
    kwh_local: pd.Series,
) -> pd.DataFrame:
    """Published rows by NAICS: ``usd`` ($M), ``kwh`` (million), ``price`` (¢/kWh)."""
    total = _published(usd_total, kwh_total)
    local = _published(usd_local, kwh_local)
    table = pd.concat([total, local.loc[local.index.difference(total.index)]])
    table['price'] = table['usd'] / table['kwh'] * 100.0
    return table.sort_index()


@functools.cache
def mecs_table(survey: int) -> pd.DataFrame:
    """MECS electricity purchases by NAICS, national, for one survey."""
    from bedrock.extract.flowbyactivity import getFlowByActivity  # noqa: PLC0415
    from bedrock.utils.mapping.location import US_FIPS  # noqa: PLC0415

    fba = getFlowByActivity('EIA_MECS_Energy', int(survey))
    fba = fba[fba['Location'].astype(str) == US_FIPS]

    def column(table: str, flow: str) -> pd.Series:
        rows = fba[
            (fba['Description'].astype(str) == table)
            & (fba['FlowName'].astype(str) == flow)
        ]
        return pd.Series(
            pd.to_numeric(rows['FlowAmount'], errors='coerce').to_numpy(dtype=float),
            index=rows['ActivityConsumedBy'].astype(str).str.strip().to_numpy(),
        )

    table = mecs_table_from_flows(
        column('Table 7.10', USD_TOTAL_FLOW),
        column('Table 7.7', KWH_TOTAL_FLOW),
        column('Table 7.10', LOCAL_UTILITY_FLOW),
        column('Table 7.7', LOCAL_UTILITY_FLOW),
    )
    if NATIONAL_ROW not in table.index:
        raise ValueError(f'MECS {survey} has no {NATIONAL_ROW} electricity row')
    return table


@functools.cache
def naics_by_industry() -> dict[str, tuple[str, ...]]:
    """Manufacturing BEA 2017 detail code -> its six-digit NAICS 2017 codes."""
    frame = pd.read_csv(_CROSSWALK, dtype=str)
    naics = frame['NAICS_2017_Code'].astype(str).str.strip()
    industry = frame['BEA_2017_Detail_Code'].astype(str).str.strip()
    keep = naics.str.fullmatch(r'3[123]\d{4}') & industry.str.len().gt(0)
    pairs = pd.DataFrame({'naics': naics[keep], 'industry': industry[keep]})
    pairs = pairs.drop_duplicates()
    if bool(pairs.duplicated('naics').any()):
        raise ValueError('a six-digit manufacturing NAICS maps to several industries')
    return {
        str(code): tuple(sorted(group['naics']))
        for code, group in pairs.groupby('industry')
    }


def deepest_row(naics: str, table: pd.DataFrame) -> str | None:
    """The most detailed published row that contains a six-digit NAICS."""
    overlaid = NAICS_OVERLAY.get(naics, naics)
    if overlaid in table.index:
        return overlaid
    for length in (6, 5, 4, 3):
        if naics[:length] in table.index:
            return naics[:length]
    return None


def _children(row: str, table: pd.DataFrame) -> list[str]:
    """Published rows directly below *row*."""
    below = [r for r in table.index if str(r) != row and str(r).startswith(row)]
    return [
        str(r)
        for r in below
        if not any(
            str(r).startswith(str(other)) and str(r) != str(other) for other in below
        )
    ]


def remainder(row: str, table: pd.DataFrame) -> tuple[float, float]:
    """kWh and ¢/kWh of *row* outside its published children.

    A NAICS whose deepest row is *row* sits in this part of it, not in the children.
    Where the remainder is too small to price, it takes the row's own price.
    """
    row_values = pd.Series(table.loc[row], dtype=float)
    usd, kwh, price = (float(row_values[c]) for c in ('usd', 'kwh', 'price'))
    children = _children(row, table)
    left_usd = usd - float(table.loc[children, 'usd'].sum())
    left_kwh = kwh - float(table.loc[children, 'kwh'].sum())
    if left_kwh <= 0 or left_usd <= 0:
        return 0.0, price
    left_price = left_usd / left_kwh * 100.0
    low, high = REMAINDER_PRICE_RANGE
    if left_kwh / kwh < MIN_REMAINDER_KWH_SHARE or not (
        low * price <= left_price <= high * price
    ):
        return left_kwh, price
    return left_kwh, left_price


def survey_prices(
    table: pd.DataFrame, industries: ta.Mapping[str, tuple[str, ...]]
) -> pd.DataFrame:
    """One survey by industry: ``price``, ``kwh`` and the ``rows`` it rests on.

    An industry's price is its rows' kWh-weighted price. A row shared by several
    industries gives each the row's price, and divides its kWh among them.
    """
    rows_of = {
        code: sorted({r for r in (deepest_row(n, table) for n in naics) if r})
        for code, naics in industries.items()
    }
    owners: dict[str, int] = {}
    for rows in rows_of.values():
        for row in rows:
            owners[row] = owners.get(row, 0) + 1
    records: dict[str, dict[str, object]] = {}
    for code, rows in rows_of.items():
        if not rows:
            continue
        parts = [remainder(row, table) for row in rows]
        weight = sum(kwh for kwh, _ in parts)
        if weight > 0:
            price = sum(kwh * p for kwh, p in parts) / weight
        else:
            price = sum(p for _, p in parts) / len(parts)
        records[code] = {
            'price': price,
            'kwh': sum(
                kwh / owners[row] for row, (kwh, _) in zip(rows, parts, strict=True)
            ),
            'rows': ','.join(rows),
        }
    return pd.DataFrame.from_dict(records, orient='index')


def survey_average(prices: pd.DataFrame) -> float:
    """The kWh-weighted price of :func:`survey_prices`, ¢/kWh.

    This is the national price the relatives and level use, not MECS's ``31-33`` row.
    In MECS 2022 the three-digit rows sum to $2.1bn less than that row, so scaling to
    it would move every industry away from its published price.
    """
    kwh = pd.Series(prices['kwh'], dtype=float)
    return float((pd.Series(prices['price'], dtype=float) * kwh).sum() / kwh.sum())


def relative_path(
    early: pd.DataFrame, anchor: pd.DataFrame, year: int
) -> tuple['pd.Series[float]', 'pd.Series[float]']:
    """Relative prices and kWh weights by industry in *year*, before normalizing.

    *early* and *anchor* are :func:`survey_prices` of the two surveys, with their
    prices already relative to the national price.
    """
    shared = anchor.index.intersection(early.index)
    same_row = pd.Series(False, index=anchor.index)
    same_row[shared] = early.loc[shared, 'rows'].astype(str) == anchor.loc[
        shared, 'rows'
    ].astype(str)
    span = ANCHOR_SURVEY - EARLY_SURVEY
    weight = min(max(ANCHOR_SURVEY - int(year), 0), span) / span
    relative = pd.Series(anchor['price'], dtype=float).copy()
    kwh = pd.Series(anchor['kwh'], dtype=float).copy()
    moved = same_row[same_row].index
    relative[moved] = (
        relative[moved] ** (1.0 - weight)
        * pd.Series(early.loc[moved, 'price'], dtype=float) ** weight
    )
    positive = [
        c
        for c in moved
        if float(kwh[c]) > 0 and float(pd.Series(early['kwh'], dtype=float)[c]) > 0
    ]
    kwh[positive] = (
        kwh[positive] ** (1.0 - weight)
        * pd.Series(early.loc[positive, 'kwh'], dtype=float) ** weight
    )
    return relative, kwh


def price_level(
    year: int,
    eia_price: ta.Callable[[int], float],
    national_price: ta.Mapping[int, float],
) -> float:
    """The national manufacturing price in *year*, ¢/kWh: EIA times a moving ratio."""
    ratios = {s: national_price[s] / eia_price(s) for s in SURVEYS}
    span = ANCHOR_SURVEY - EARLY_SURVEY
    weight = min(max(int(year) - EARLY_SURVEY, 0), span) / span
    ratio = ratios[EARLY_SURVEY] ** (1.0 - weight) * ratios[ANCHOR_SURVEY] ** weight
    return float(eia_price(int(year))) * ratio


@functools.cache
def eia_class_price(year: int, customer_class: str) -> float:
    """EIA average price for one class, ¢/kWh delivered: Table 2.3 over Table 2.2."""
    from bedrock.extract.disaggregation.egrid_generation import (  # noqa: PLC0415
        eia_retail_revenue_usd,
        eia_table_2_2_end_use_mwh,
    )

    mwh = float(eia_table_2_2_end_use_mwh(int(year))[customer_class])
    if mwh <= 0:
        raise ValueError(f'EIA {customer_class} sales are non-positive for {year}')
    return eia_retail_revenue_usd(int(year), customer_class) / mwh / 10.0


def manufacturing_price_from_tables(
    year: int,
    tables: ta.Mapping[int, pd.DataFrame],
    industries: ta.Mapping[str, tuple[str, ...]],
    eia_price: ta.Callable[[int], float],
) -> 'pd.Series[float]':
    """Manufacturing price by industry in *year*, ¢/kWh delivered."""
    national: dict[int, float] = {}
    surveys = {}
    for survey in SURVEYS:
        prices = survey_prices(tables[survey], industries)
        national[survey] = survey_average(prices)
        prices['price'] = pd.Series(prices['price'], dtype=float) / national[survey]
        surveys[survey] = prices
    relative, kwh = relative_path(surveys[EARLY_SURVEY], surveys[ANCHOR_SURVEY], year)
    average = float((relative * kwh).sum() / kwh.sum())
    level = price_level(year, eia_price, national)
    return pd.Series(relative / average * level, dtype=float)


def manufacturing_price(year: int) -> 'pd.Series[float]':
    """Manufacturing price by BEA 2017 detail industry in *year*, ¢/kWh delivered."""
    return manufacturing_price_from_tables(
        year,
        {s: mecs_table(s) for s in SURVEYS},
        naics_by_industry(),
        lambda y: eia_class_price(y, 'Industrial'),
    )


def class_price(year: int, customer_class: str) -> float:
    """The price for buyers outside manufacturing, ¢/kWh delivered."""
    return eia_class_price(year, customer_class)


def resale_price(revenue_usd_m: float, volume_mwh: float) -> float:
    """Sales for resale as ¢/kWh, from revenue in $M and volume in MWh."""
    if volume_mwh <= 0:
        raise ValueError('sales for resale volume is non-positive')
    return revenue_usd_m * 1e6 / volume_mwh / 10.0


def pooled_resale_price(
    year: int, revenue_usd_m: pd.Series, volume_mwh: pd.Series
) -> float:
    """Sales for resale over :data:`GENERATION_PRICE_YEARS` ending in *year*, ¢/kWh."""
    years = range(int(year) - GENERATION_PRICE_YEARS + 1, int(year) + 1)
    missing = [
        y for y in years if y not in revenue_usd_m.index or y not in volume_mwh.index
    ]
    if missing:
        raise ValueError(f'no sales for resale data for {missing}')
    return resale_price(
        float(revenue_usd_m.loc[list(years)].sum()),
        float(volume_mwh.loc[list(years)].sum()),
    )


def generation_price(year: int) -> float:
    """One price per kWh generated in *year*, ¢/kWh (#1215).

    EIA-861 sales for resale, pooled over three years ending in *year*. It is an
    observed wholesale price, not a share of the electricity row's dollars, so it
    doesn't move with the bills it divides. Generation dollars are this price times
    kWh generated: buyers pay it on kWh delivered, and ``42ELEC`` pays it on losses
    (#1217).

    ⚠️ Why three years: large contracts and retail rates lag the wholesale market. The
    annual price puts 6 manufacturers and 16% of manufacturing kWh below it in 2021.
    The trailing average puts none below it in 2021-22, and a few small ones in other
    years, which #902 floors.
    """
    from bedrock.analysis.nowcasting.utilities_expense_seed import (  # noqa: PLC0415
        resale_revenue,
        resale_volume,
    )

    return pooled_resale_price(year, resale_revenue(), resale_volume())


def delivery_price(year: int) -> float:
    """EIA's average retail price less :func:`generation_price`, ¢/kWh delivered (#1217).

    What a kWh's bill pays beyond generation: the price of the delivery sector's
    output, from the same EIA series as the split, not from the tables it feeds.
    """
    from bedrock.extract.disaggregation.egrid_generation import (  # noqa: PLC0415
        eia_retail_revenue_usd,
    )
    from bedrock.transform.iot.eia_utility_go_adjustment import (  # noqa: PLC0415
        retail_sales_mwh,
    )

    # USD per MWh is a tenth of a cent per kWh.
    retail = eia_retail_revenue_usd(int(year)) / retail_sales_mwh(int(year)) / 10.0
    margin = retail - generation_price(year)
    if margin <= 0:
        raise ValueError(f'{year}: retail price is not above the generation price')
    return margin


def annual_generation_price(year: int) -> float:
    """Sales for resale in *year* alone, ¢/kWh. Reported, not used in the split."""
    from bedrock.analysis.nowcasting.utilities_expense_seed import (  # noqa: PLC0415
        resale_revenue,
        resale_volume,
    )

    return resale_price(
        float(resale_revenue().loc[int(year)]), float(resale_volume().loc[int(year)])
    )


def buyers_below(prices: pd.Series, generation: float) -> pd.Series:
    """Buyers paying less per kWh than generation costs, cheapest first, ¢/kWh.

    Their delivery margin would be negative, so #902 has to name and treat them.
    """
    return prices[prices < generation].sort_values()
