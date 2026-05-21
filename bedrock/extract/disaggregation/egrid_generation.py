"""US net electricity generation from EPA eGRID facility inventories (stewi).



Stewi loads plant-level ``Plant annual net generation (MWh)`` as ``FlowName``

``Electricity`` in MJ (``MWh_MJ`` = 3600). Summing facility rows matches stewi's

eGRID national-total validation for that flow.

"""

from __future__ import annotations

from collections.abc import Iterable

import pandas as pd
import stewi
from stewi.globals import MWh_MJ
from stewi.globals import config as stewi_config

EGRID_INVENTORY = "eGRID"

NET_GENERATION_FLOW = "Electricity"


DEFAULT_YEAR_START = 2016

DEFAULT_YEAR_END = 2024


def egrid_inventory_years(year_start: int, year_end: int) -> list[int]:
    """Calendar years with stewi eGRID source config in ``[year_start, year_end]``.



    EPA does not publish eGRID every year (e.g. no 2017).

    """

    keys = stewi_config()["databases"][EGRID_INVENTORY]

    configured = sorted(int(k) for k in keys if str(k).isdigit())

    return [y for y in configured if year_start <= y <= year_end]


def load_egrid_flowbyfacility(
    year: int,
    *,
    download_if_missing: bool = True,
) -> pd.DataFrame:
    """Return stewi eGRID flow-by-facility inventory for *year*."""

    return stewi.getInventory(
        EGRID_INVENTORY,
        year,
        stewiformat="flowbyfacility",
        download_if_missing=download_if_missing,
    )


def _net_generation_mj(flowbyfacility: pd.DataFrame) -> float:
    """Sum ``Electricity`` (net generation) across rows of a stewi eGRID flowbyfacility table, in MJ."""

    gen = flowbyfacility.loc[
        flowbyfacility["FlowName"] == NET_GENERATION_FLOW, "FlowAmount"
    ]

    if gen.empty:

        msg = (
            f"eGRID flow-by-facility has no {NET_GENERATION_FLOW!r} rows "
            "(plant annual net generation)"
        )

        raise ValueError(msg)

    units = flowbyfacility.loc[
        flowbyfacility["FlowName"] == NET_GENERATION_FLOW, "Unit"
    ].unique()

    if len(units) != 1 or units[0] != "MJ":

        msg = f"unexpected units for {NET_GENERATION_FLOW!r}: {units.tolist()}"

        raise ValueError(msg)

    return float(gen.sum())


def us_total_net_generation_mwh(
    year: int,
    *,
    download_if_missing: bool = True,
) -> float:
    """Sum US plant annual net generation (MWh) from stewi eGRID for *year*."""

    inv = load_egrid_flowbyfacility(year, download_if_missing=download_if_missing)

    return _net_generation_mj(inv) / MWh_MJ


def us_total_net_generation_by_year(
    year_start: int = DEFAULT_YEAR_START,
    year_end: int = DEFAULT_YEAR_END,
    *,
    years: Iterable[int] | None = None,
    download_if_missing: bool = True,
) -> pd.Series[float]:
    """US net generation by inventory year (values in MWh, index = year).



    When *years* is omitted, uses ``egrid_inventory_years(year_start, year_end)``.

    """

    if years is None:

        year_list = egrid_inventory_years(year_start, year_end)

    else:

        year_list = sorted(years)

    totals: dict[int, float] = {}

    for year in year_list:

        totals[year] = us_total_net_generation_mwh(
            year, download_if_missing=download_if_missing
        )

    return pd.Series(totals, dtype=float, name="net_generation_mwh")
