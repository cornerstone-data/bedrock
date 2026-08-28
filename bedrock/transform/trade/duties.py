"""Census import duty rates → BEA Detail MDTY vector (NIPA B235RC level)."""

from __future__ import annotations

import pandas as pd

from bedrock.extract.flowbyactivity import getFlowByActivity
from bedrock.extract.iot.io_2017 import _load_2017_detail_supply_use_usa
from bedrock.transform.flowbysector import getFlowBySector
from bedrock.transform.trade.utilities import consolidate_vehicle_activities
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.mapping.sectormapping import get_activitytosector_mapping
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES

_CENSUS = 'Census_USATrade'
_DUTY_FLOW = 'CAL_DUT_YR'
_CUSTOMS_FLOW = 'GEN_VAL_YR'


def nipa_customs_duties_usd(year: int) -> float:
    """NIPA T30500 customs duties (``B235RC``) in USD from the ``BEA_NIPA`` FBA."""
    fba = getFlowByActivity('BEA_NIPA', int(year))
    hit = fba.loc[fba['Description'].astype(str).str.contains('B235RC', na=False)]
    if hit.empty:
        raise ValueError(f'BEA_NIPA FBA missing B235RC customs duties for {year}')
    total = float(pd.to_numeric(hit['FlowAmount'], errors='coerce').sum())
    if not pd.notna(total) or total == 0.0:
        raise ValueError(f'BEA_NIPA B235RC is missing or zero for {year}')
    return total


def _census_flow_by_activity(year: int, flow_name: str) -> pd.Series:
    fba = getFlowByActivity(_CENSUS, int(year))
    sub = fba.loc[fba['FlowName'] == flow_name]
    if sub.empty:
        raise ValueError(f'{_CENSUS} FBA missing FlowName {flow_name!r} for {year}')
    # ⚠️ The same vehicle consolidation the Trade FBS applies (#702). The rate
    # below is a ratio of two mapped Census flows and it multiplies the
    # Trade_Imports FBS's own mass, so both sides have to consolidate 336111 /
    # 336112 or the rate is built on one commodity axis and applied to another.
    sub = consolidate_vehicle_activities(sub)
    return (
        pd.to_numeric(sub['FlowAmount'], errors='coerce')
        .groupby(sub['ActivityProducedBy'].astype(str))
        .sum()
    )


def _supply_mcif_weights_usd() -> pd.Series:
    """2017 Supply ``MCIF`` by Detail commodity, USD (attribution weights)."""
    supply = _load_2017_detail_supply_use_usa('Supply_detail')
    supply.columns = supply.columns.str.strip()
    mcif_m = pd.to_numeric(supply['MCIF'], errors='coerce')
    return (
        mcif_m.reindex(USA_2017_COMMODITY_CODES).fillna(0.0)
        * MILLION_CURRENCY_TO_CURRENCY
    )


def map_census_import_flow_to_detail(year: int, flow_name: str) -> pd.Series:
    """Map a Census NAICS-6 import flow to BEA Detail via the Trade Crosswalk.

    1:m rows split in proportion to 2017 Supply ``MCIF`` (same weight family as
    ``Trade_Imports`` goods). Activities whose mapped targets all have zero
    MCIF contribute nothing.
    """
    amounts = _census_flow_by_activity(year, flow_name)
    cw = get_activitytosector_mapping(_CENSUS)
    weights = _supply_mcif_weights_usd()
    out = pd.Series(0.0, index=pd.Index(USA_2017_COMMODITY_CODES, name='commodity'))

    for activity, amount in amounts.items():
        if not pd.notna(amount) or amount == 0.0:
            continue
        sectors = (
            cw.loc[cw['Activity'].astype(str) == str(activity), 'Sector']
            .astype(str)
            .tolist()
        )
        if not sectors:
            continue
        w = weights.reindex(sectors).fillna(0.0)
        w_sum = float(w.sum())
        if w_sum == 0.0:
            continue
        shares = w / w_sum
        for sector, share in shares.items():
            sector_code = str(sector)
            if sector_code in out.index:
                out.loc[sector_code] += float(amount) * float(share)
    return out


def census_goods_mcif_by_detail(
    year: int, download_sources_ok: bool = True
) -> pd.Series:
    """Unscaled Trade Imports FBS mass from Census goods rows, by Detail."""
    fbs = getFlowBySector(
        f'Trade_Imports_{year}',
        download_FBAs_if_missing=download_sources_ok,
        download_FBS_if_missing=download_sources_ok,
    )
    goods = fbs.loc[fbs['MetaSources'].astype(str).str.startswith(_CENSUS)]
    return (
        pd.to_numeric(goods['FlowAmount'], errors='coerce')
        .groupby(goods['SectorProducedBy'].astype(str))
        .sum()
        .reindex(USA_2017_COMMODITY_CODES)
        .fillna(0.0)
    )


def mdty_detail_usd(year: int, download_sources_ok: bool = True) -> pd.Series:
    """Detail ``MDTY``: Census duty rate × goods MCIF, leveled to NIPA ``B235RC``.

    Rate is mapped ``CAL_DUT_YR`` / mapped ``GEN_VAL_YR``. Services stay at
    zero. Works for every year with ``Trade_Imports_<year>`` and ``BEA_NIPA``
    ``B235RC``.
    """
    duty = map_census_import_flow_to_detail(year, _DUTY_FLOW)
    customs = map_census_import_flow_to_detail(year, _CUSTOMS_FLOW)
    rate = (duty / customs).where(customs != 0.0, 0.0).fillna(0.0)
    goods_mcif = census_goods_mcif_by_detail(year, download_sources_ok)
    provisional = rate * goods_mcif
    prov_sum = float(provisional.sum())
    if prov_sum == 0.0:
        raise ValueError(
            f'Provisional MDTY mass is zero for {year}; ' 'cannot level to NIPA B235RC'
        )
    level = nipa_customs_duties_usd(year)
    out = provisional * (level / prov_sum)
    out.name = 'MDTY'
    return out
