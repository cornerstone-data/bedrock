"""2017 Supply MADJ from SUT ratios × scaled MCIF (national Supply MADJ level)."""

from __future__ import annotations

import pandas as pd

from bedrock.extract.iot.io_2017 import _load_2017_detail_supply_use_usa
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES


def _supply_madj_mcif_usd() -> tuple[pd.Series, pd.Series]:
    """2017 Supply ``MADJ`` and ``MCIF`` by Detail commodity, USD."""
    supply = _load_2017_detail_supply_use_usa('Supply_detail')
    supply.columns = supply.columns.str.strip()
    idx = pd.Index(USA_2017_COMMODITY_CODES, name='commodity')
    madj = (
        pd.to_numeric(supply['MADJ'], errors='coerce')
        .reindex(idx)
        .fillna(0.0)
        * MILLION_CURRENCY_TO_CURRENCY
    )
    mcif = (
        pd.to_numeric(supply['MCIF'], errors='coerce')
        .reindex(idx)
        .fillna(0.0)
        * MILLION_CURRENCY_TO_CURRENCY
    )
    return madj, mcif


def supply_madj_national_usd() -> float:
    """Published 2017 Supply ``MADJ`` total over Detail commodities, USD."""
    madj, _ = _supply_madj_mcif_usd()
    return float(madj.sum())


def madj_detail_usd(year: int, mcif_usd: pd.Series) -> pd.Series:
    """Detail ``MADJ``: 2017 SUT ``MADJ``/``MCIF`` ratios × ``mcif_usd``, leveled.

    ``ratio[c] = MADJ_SUT[c] / MCIF_SUT[c]``; ``0`` when ``MCIF_SUT == 0``
    (even if SUT ``MADJ`` is nonzero). Negative ratios are kept. Provisional
    mass is ``mcif_usd * ratio``, then rescaled so the national sum matches
    published Supply ``MADJ``. Does not fill ``T013``.
    """
    if int(year) != 2017:
        raise NotImplementedError(
            f'madj_detail_usd is only implemented for 2017 (got {year})'
        )
    madj_sut, mcif_sut = _supply_madj_mcif_usd()
    ratio = (madj_sut / mcif_sut).where(mcif_sut != 0.0, 0.0).fillna(0.0)
    mcif = (
        pd.to_numeric(mcif_usd, errors='coerce')
        .reindex(USA_2017_COMMODITY_CODES)
        .fillna(0.0)
    )
    provisional = mcif * ratio
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
