"""Supply ``MADJ`` — import c.i.f./f.o.b. adjustment on the basic-supply bridge.

In the BEA Supply table, ``MCIF`` is imports valued c.i.f. and ``MADJ`` is the
commodity-level adjustment that moves that valuation toward the f.o.b. /
customs basis used elsewhere in the accounts. It enters the basic-supply
identity ``T013 = T007 + MCIF + MADJ`` (typically a small negative national
total).

Published ``MADJ`` is booked mainly on transport and insurance Detail codes,
not on the goods NAICS where Census reports import charges. This module maps
Census ``GEN_CHA_YR`` to Detail, then **reassigns** that charge mass onto
commodities with nonzero 2017 Supply ``MADJ`` in proportion to those published
``MADJ`` values (signed shares), and levels the national sum to published
Supply ``MADJ``. The 2017 destination mix is the extendable hold-structure for
later years once an annual level target is chosen.
"""

from __future__ import annotations

import pandas as pd

from bedrock.extract.iot.io_2017 import _load_2017_detail_supply_use_usa
from bedrock.transform.trade.duties import map_census_import_flow_to_detail
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES

_CHARGE_FLOW = 'GEN_CHA_YR'


def _supply_madj_usd() -> pd.Series:
    """2017 Supply ``MADJ`` by Detail commodity, USD."""
    supply = _load_2017_detail_supply_use_usa('Supply_detail')
    supply.columns = supply.columns.str.strip()
    idx = pd.Index(USA_2017_COMMODITY_CODES, name='commodity')
    return (
        pd.to_numeric(supply['MADJ'], errors='coerce').reindex(idx).fillna(0.0)
        * MILLION_CURRENCY_TO_CURRENCY
    )


def supply_madj_national_usd() -> float:
    """Published 2017 Supply ``MADJ`` total over Detail commodities, USD."""
    return float(_supply_madj_usd().sum())


def _madj_destination_shares() -> pd.Series:
    """Signed shares of 2017 Supply ``MADJ`` over its nonzero Detail codes."""
    madj = _supply_madj_usd()
    dest = madj.where(madj != 0.0, 0.0)
    dest_sum = float(dest.sum())
    if dest_sum == 0.0:
        raise ValueError('Published 2017 Supply MADJ is all zero; cannot form shares')
    return dest / dest_sum


def madj_detail_usd(year: int, download_sources_ok: bool = True) -> pd.Series:
    """Detail c.i.f./f.o.b. import adjustment (``MADJ``), USD.

    Maps Census ``GEN_CHA_YR`` to Detail, reassigns the charge total onto
    nonzero 2017 Supply ``MADJ`` codes by signed published ``MADJ`` shares,
    then rescales so the national sum matches published Supply ``MADJ``.
    Does not fill ``T013``. ``download_sources_ok`` is accepted for call-site
    symmetry with other trade helpers; the Census FBA load follows extract
    defaults.
    """
    _ = download_sources_ok  # call-site symmetry; Census FBA load has no flag
    if int(year) != 2017:
        raise NotImplementedError(
            f'madj_detail_usd is only implemented for 2017 (got {year})'
        )
    charges = map_census_import_flow_to_detail(year, _CHARGE_FLOW)
    charge_sum = float(pd.to_numeric(charges, errors='coerce').fillna(0.0).sum())
    if charge_sum == 0.0:
        raise ValueError(
            f'Mapped {_CHARGE_FLOW} mass is zero for {year}; cannot form MADJ'
        )
    shares = _madj_destination_shares()
    provisional = shares * charge_sum
    prov_sum = float(provisional.sum())
    if prov_sum == 0.0:
        raise ValueError(
            f'Provisional MADJ mass is zero for {year}; '
            'cannot level to published Supply MADJ'
        )
    level = supply_madj_national_usd()
    out = provisional * (level / prov_sum)
    out.name = 'MADJ'
    return out
