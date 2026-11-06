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
