"""Split electricity bills into generation and a buyer-specific delivery margin (#902).

For each buyer and year::

    kWh_i      = bill_i / price_i, scaled so each class's kWh equals EIA's sales
    gen_i      = kWh_i x p_gen, and never more than the bill
    delivery_i = bill_i - gen_i

Bills don't change. Generation is one commodity at one price per kWh, and delivery
takes the rest, so delivery differs by buyer and reaches ``42ELEC`` (#1217).

**Prices** come from :mod:`.electricity_price`: MECS and EIA for manufacturing, the EIA
class price for everyone else.

**Generation price.** A three-year trailing average of EIA-861 sales for resale
(:func:`~.electricity_price.generation_price`). A buyer still priced below it has its
generation held at its bill and no delivery margin, and is flagged ``floored``.

**Who isn't scaled to a class.** Exports are power sold at wholesale, so their kWh are
EIA Table 2.14's and they carry no margin beyond what the floor leaves. Electric power's
own purchases aren't end use, so they stay in generation in full.

**Line losses** aren't charged to buyers per kWh. They are an input to ``42ELEC``
(:func:`line_losses_mwh`), so buyers pay for them through the margin.

This doesn't change the older G/T/D allocator in
:mod:`.electricity_gtd_allocation`, which released configs still use.
"""

from __future__ import annotations

import functools
import typing as ta
from dataclasses import dataclass

import pandas as pd

OWN_USE = 'Own use'
EXPORTS = 'Exports'
TRANSPORTATION = 'Transportation'
COMMERCIAL = 'Commercial'
INDUSTRIAL = 'Industrial'

#: Electric columns, which merge into one Cornerstone sector.
ELECTRIC_BUYERS = frozenset({'221100', 'S00101', 'S00202'})

#: EIA's transportation class is electrified rail and transit. The older mapping puts
#: all of NAICS 48-49 in it, which gave $14bn of bills on 7 TWh in 2024: 196 ¢/kWh.
EIA_TRANSPORTATION_BUYERS = frozenset({'482000', '485000', 'S00201'})

MANUFACTURING_PREFIXES = ('31', '32', '33')

#: Sales classes scaled to EIA Table 2.2.
SCALED_CLASSES = ('Residential', COMMERCIAL, INDUSTRIAL, TRANSPORTATION)


@dataclass(frozen=True)
class DeliverySplit:
    """One year's split. ``buyers`` has bill, class, price, kwh, gen, delivery, floored."""

    buyers: pd.DataFrame
    generation_price: float
    losses_mwh: float

    @property
    def losses_usd(self) -> float:
        """What ``42ELEC`` pays ``221100`` for line losses, USD."""
        return self.losses_mwh * 1000.0 * self.generation_price / 100.0


def delivery_classes(codes: ta.Iterable[str], base: ta.Mapping[str, str]) -> pd.Series:
    """Each buyer's class: the end-use map, except own use and transportation."""
    out: dict[str, str] = {}
    for code in map(str, codes):
        cls = base.get(code, COMMERCIAL)
        if code in ELECTRIC_BUYERS:
            cls = OWN_USE
        elif cls == TRANSPORTATION and code not in EIA_TRANSPORTATION_BUYERS:
            cls = COMMERCIAL
        out[code] = cls
    return pd.Series(out, dtype=object)


def buyer_prices(
    classes: pd.Series,
    manufacturing: pd.Series,
    class_price: ta.Callable[[str], float],
) -> 'pd.Series[float]':
    """¢/kWh by buyer: the manufacturing price where there is one, else the class price."""
    prices: dict[str, float] = {}
    for code, cls in classes.items():
        key = str(code)
        if cls in (OWN_USE, EXPORTS):
            continue
        if key.startswith(MANUFACTURING_PREFIXES) and key in manufacturing.index:
            prices[key] = float(manufacturing[key])
        else:
            prices[key] = float(class_price(str(cls)))
    return pd.Series(prices, dtype=float)


def split_bills(
    bills: pd.Series,
    classes: pd.Series,
    prices: pd.Series,
    class_mwh: ta.Mapping[str, float],
    generation_price: float,
    export_mwh: float,
    losses_mwh: float,
) -> DeliverySplit:
    """Split *bills* (USD) at *generation_price* (¢/kWh). See the module docstring."""
    bills = bills[bills > 0].astype(float)
    frame = pd.DataFrame({'bill': bills, 'class': classes.reindex(bills.index)})
    frame['price'] = prices.reindex(bills.index)
    frame['kwh'] = 0.0
    for cls in SCALED_CLASSES:
        members = frame.index[frame['class'] == cls]
        if len(members) == 0:
            continue
        missing = frame.loc[members, 'price'].isna()
        if bool(missing.any()):
            raise ValueError(f'no price for {list(members[missing])[:5]}')
        raw = frame.loc[members, 'bill'] / (frame.loc[members, 'price'] / 100.0)
        frame.loc[members, 'kwh'] = raw * (class_mwh[cls] * 1000.0 / float(raw.sum()))
    exports = frame.index[frame['class'] == EXPORTS]
    if len(exports) > 0:
        share = frame.loc[exports, 'bill'] / float(frame.loc[exports, 'bill'].sum())
        frame.loc[exports, 'kwh'] = share * export_mwh * 1000.0
    full = frame['kwh'] * generation_price / 100.0
    own = frame['class'] == OWN_USE
    frame['gen'] = full.where(~own, frame['bill'])
    frame['floored'] = frame['gen'] > frame['bill']
    frame['gen'] = frame[['gen', 'bill']].min(axis=1)
    frame['delivery'] = frame['bill'] - frame['gen']
    return DeliverySplit(frame, float(generation_price), float(losses_mwh))


def line_losses_mwh(
    generation_mwh: float, end_use_mwh: float, export_mwh: float
) -> float:
    """Generation not reaching end use or exports, MWh. Never negative."""
    return max(float(generation_mwh) - float(end_use_mwh) - float(export_mwh), 0.0)


def delivery_split(year: int, bills: pd.Series) -> DeliverySplit:
    """The split for *year* from a buyer's bills in USD."""
    from bedrock.extract.disaggregation.egrid_generation import (  # noqa: PLC0415
        egrid_mwh_for_io_year,
        eia_table_2_2_end_use_mwh,
        eia_table_2_14_export_mwh,
        eia_table_2_14_year_for_egrid_year,
    )
    from bedrock.transform.eeio.electricity_end_use_mapping import (  # noqa: PLC0415
        build_end_use_map,
    )
    from bedrock.transform.eeio.electricity_price import (  # noqa: PLC0415
        class_price,
        generation_price,
        manufacturing_price,
    )

    sales = eia_table_2_2_end_use_mwh(int(year))
    export_mwh = eia_table_2_14_export_mwh(
        eia_table_2_14_year_for_egrid_year(int(year))
    )
    classes = delivery_classes(bills.index, build_end_use_map())
    prices = buyer_prices(
        classes,
        manufacturing_price(int(year)),
        lambda cls: class_price(int(year), cls),
    )
    return split_bills(
        bills,
        classes,
        prices,
        {cls: float(sales[cls]) for cls in SCALED_CLASSES},
        generation_price(int(year)),
        export_mwh,
        line_losses_mwh(
            egrid_mwh_for_io_year(int(year)),
            float(sales['Total End Use']),
            export_mwh,
        ),
    )


# --- the tables with a delivery sector (#1216, #1217) --------------------------------

#: The Cornerstone slot delivery takes. The sector list stays at 405 and keeps this code
#: on every axis, because released configs and snapshots use it. Outputs label it
#: :data:`DELIVERY_CODE` (:func:`published_label`).
DELIVERY_SLOT = '425000'
DELIVERY_CODE = '42ELEC'
#: Where wholesale agents and brokers go, the host that moves ``N`` least.
BROKERS_HOST = '423A00'
GENERATION_SECTOR = '221100'

#: EIA-923's fuels: all generation.
FUEL_ROWS = ('212100', '211000', '324110')
IMPORTS_COLUMN = 'F05000'

_DELIVERY_EXPENSES = (
    'expenses: Transmission',
    'expenses: Distribution',
    'expenses: Customer Accounts',
    'expenses: Customer Service',
    'expenses: Sales',
)
_GENERATION_NON_FUEL_EXPENSE = 'expenses: Other'

PUBLISHED_LABELS = {
    DELIVERY_SLOT: (DELIVERY_CODE, 'Electricity transmission and distribution'),
    GENERATION_SECTOR: (GENERATION_SECTOR, 'Electricity generation'),
    BROKERS_HOST: (
        BROKERS_HOST,
        'Other durable goods merchant wholesalers, and wholesale agents and brokers',
    ),
}


def delivery_margin_on() -> bool:
    """Whether the active config models delivery as its own sector."""
    from bedrock.utils.config.usa_config import get_usa_config  # noqa: PLC0415

    return bool(get_usa_config().electricity_delivery_margin)


def published_label(code: str, name: str) -> tuple[str, str]:
    """The code and name to show for a sector under the active config."""
    if delivery_margin_on() and str(code) in PUBLISHED_LABELS:
        return PUBLISHED_LABELS[str(code)]
    return str(code), name


def non_fuel_delivery_share(expenses: ta.Mapping[str, float]) -> float:
    """Delivery's share of electric power's non-fuel inputs, from FERC Form 1 lines.

    Transmission, distribution and customer costs against other production costs.
    Administration and maintenance aren't split on the form, so they follow this share.
    """
    delivery = sum(float(expenses[name]) for name in _DELIVERY_EXPENSES)
    generation = float(expenses[_GENERATION_NON_FUEL_EXPENSE])
    return delivery / (delivery + generation)


def merge_brokers(frame: pd.DataFrame, rows: bool, columns: bool) -> pd.DataFrame:
    """*frame* with the slot's row and column added to the host, and zero in the slot."""
    out = frame.astype(float).copy()
    if rows and DELIVERY_SLOT in out.index:
        out.loc[BROKERS_HOST] = out.loc[BROKERS_HOST] + out.loc[DELIVERY_SLOT]
        out.loc[DELIVERY_SLOT] = 0.0
    if columns and DELIVERY_SLOT in out.columns:
        out[BROKERS_HOST] = out[BROKERS_HOST] + out[DELIVERY_SLOT]
        out[DELIVERY_SLOT] = 0.0
    return out


def electricity_bills(udom: pd.DataFrame, y: pd.DataFrame) -> 'pd.Series[float]':
    """Domestic Use and final demand on the electricity row, without imports. USD."""
    final = y.loc[GENERATION_SECTOR].drop(labels=[IMPORTS_COLUMN], errors='ignore')
    return pd.Series(pd.concat([udom.loc[GENERATION_SECTOR], final]), dtype=float)


def _split_column(
    column: pd.Series, share: float, whole_to_generation: ta.Sequence[str]
) -> pd.Series:
    """Delivery's part of an input column: *share* except for the named rows."""
    delivery = column.astype(float) * share
    delivery[[r for r in whole_to_generation if r in delivery.index]] = 0.0
    return delivery


def restructure_tables(
    v: pd.DataFrame,
    udom: pd.DataFrame,
    uimp: pd.DataFrame,
    va: pd.DataFrame,
    y: pd.DataFrame,
    split: DeliverySplit,
    non_fuel_share: float,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """The tables with brokers merged and electric power split in two.

    **Row.** Each buyer's bill divides into generation and delivery.
    **Column.** Fuel and own purchases stay with generation. Other inputs divide by
    *non_fuel_share*. ``42ELEC`` also buys line losses from generation, a new sale.
    Value added is what each column's output leaves.

    Raises rather than returning tables that leave generation or delivery with
    non-positive output or value added.
    """
    v2 = merge_brokers(v, rows=True, columns=True)
    u_dom = merge_brokers(udom, rows=True, columns=True)
    u_imp = merge_brokers(uimp, rows=True, columns=True)
    value = merge_brokers(va, rows=False, columns=True)
    final = merge_brokers(y, rows=True, columns=False)

    frame = split.buyers
    delivery = pd.Series(frame['delivery'], dtype=float)
    in_use = [c for c in delivery.index if c in u_dom.columns]
    in_final = [c for c in delivery.index if c in final.columns]
    missing = delivery.index.difference(pd.Index(in_use + in_final))
    if len(missing) > 0:
        raise ValueError(f'buyers in no table: {list(missing)[:5]}')
    total = float(delivery.sum())
    losses = split.losses_usd

    for table in (u_dom, final):
        amounts = delivery.reindex(table.columns).fillna(0.0)
        table.loc[DELIVERY_SLOT] = amounts
        table.loc[GENERATION_SECTOR] = table.loc[GENERATION_SECTOR] - amounts

    held = [*FUEL_ROWS, GENERATION_SECTOR]
    for table in (u_dom, u_imp):
        part = _split_column(table[GENERATION_SECTOR], non_fuel_share, held)
        table[DELIVERY_SLOT] = part
        table[GENERATION_SECTOR] = table[GENERATION_SECTOR] - part
    u_dom.loc[GENERATION_SECTOR, DELIVERY_SLOT] = losses

    inputs = float(u_dom[DELIVERY_SLOT].sum() + u_imp[DELIVERY_SLOT].sum())
    delivery_value = total - inputs
    electric_value = pd.Series(value[GENERATION_SECTOR], dtype=float)
    value_total = float(electric_value.sum())
    if delivery_value <= 0 or delivery_value >= value_total:
        raise ValueError(
            f'delivery value added of {delivery_value:,.0f} USD does not fit '
            f'electric value added of {value_total:,.0f} USD'
        )
    value[DELIVERY_SLOT] = electric_value * (delivery_value / value_total)
    value[GENERATION_SECTOR] = electric_value - value[DELIVERY_SLOT]

    v2.loc[DELIVERY_SLOT, DELIVERY_SLOT] = total
    generation_output = (
        float(pd.Series(v2.loc[GENERATION_SECTOR], dtype=float)[GENERATION_SECTOR])
        - total
        + losses
    )
    if generation_output <= 0:
        raise ValueError('generation output is not positive after the split')
    v2.loc[GENERATION_SECTOR, GENERATION_SECTOR] = generation_output
    return v2, u_dom, u_imp, value, final


def move_delivery_emissions(e: pd.DataFrame) -> pd.DataFrame:
    """Emissions with brokers merged and SF6 from electric power moved to delivery.

    Generation keeps eGRID. SF6 is what insulates transmission equipment.
    """
    out = merge_brokers(e, rows=False, columns=True)
    if 'SF6' in out.index:
        out.loc['SF6', DELIVERY_SLOT] = out.loc['SF6', GENERATION_SECTOR]
        out.loc['SF6', GENERATION_SECTOR] = 0.0
    return out


def move_delivery_flows(
    flows: pd.DataFrame, sector: str = 'SectorProducedBy', gas: str = 'Flowable'
) -> pd.DataFrame:
    """:func:`move_delivery_emissions` for a long table of flows, one row per flow."""
    codes = flows[sector].astype(str)
    out = flows.copy()
    out[sector] = codes.mask(codes == DELIVERY_SLOT, BROKERS_HOST).mask(
        (codes == GENERATION_SECTOR) & (flows[gas].astype(str) == 'SF6'), DELIVERY_SLOT
    )
    return out


def delivery_margin_rate(split: DeliverySplit) -> float:
    """Delivery dollars per dollar of generation sold to buyers."""
    frame = split.buyers[split.buyers['class'] != OWN_USE]
    return float(frame['delivery'].sum() / frame['gen'].sum())


def restructure_margins(margins: pd.DataFrame, rate: float) -> pd.DataFrame:
    """Margins by commodity with brokers merged and electricity's delivery shown.

    Electricity's producers' value becomes generation, and delivery joins what the
    purchaser pays. Adds the ``Electricity delivery`` column.
    """
    out = merge_brokers(margins, rows=True, columns=False)
    out['Electricity delivery'] = 0.0
    producers = "Producers' Value"
    bill = float(pd.Series(out[producers], dtype=float)[GENERATION_SECTOR])
    out.at[GENERATION_SECTOR, producers] = bill / (1.0 + rate)
    out.at[GENERATION_SECTOR, 'Electricity delivery'] = bill * rate / (1.0 + rate)
    return out


def active_delivery_split() -> DeliverySplit:
    """This model year's split, from the tables before it is applied."""
    return _delivery_state()[0]


def _year_expenses(year: int) -> dict[str, float]:
    from bedrock.extract.disaggregation.egrid_generation import (  # noqa: PLC0415
        eia_table_8_3_line,
    )

    names = (*_DELIVERY_EXPENSES, _GENERATION_NON_FUEL_EXPENSE)
    return {name: eia_table_8_3_line(int(year), name) for name in names}


@functools.cache
def _delivery_state() -> tuple[
    DeliverySplit,
    tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame],
]:
    from bedrock.transform.eeio import sector_disagg_pipeline as pipe  # noqa: PLC0415
    from bedrock.transform.eeio.electricity_disaggregation import (  # noqa: PLC0415
        reallocate_electricity_coproduction,
    )
    from bedrock.utils.config.usa_config import get_usa_config  # noqa: PLC0415

    year = int(get_usa_config().usa_detail_original_year)
    # Electric utilities' other products, $14bn of gas distribution in 2024, would
    # otherwise take generation's factor per dollar.
    v, udom, uimp, va = reallocate_electricity_coproduction(
        pipe.derive_V_after_waste(),
        *pipe.derive_U_after_waste(),
        pipe.derive_VA_after_waste(),
    )
    y = pipe.derive_y_after_waste()
    split = delivery_split(year, electricity_bills(udom, y))
    share = non_fuel_delivery_share(_year_expenses(year))
    return split, restructure_tables(v, udom, uimp, va, y, split, share)


def restructured_tables() -> (
    tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]
):
    """V, Udom, Uimp, VA and Y for this model year with delivery as its own sector."""
    return _delivery_state()[1]
