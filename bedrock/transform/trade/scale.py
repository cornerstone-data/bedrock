"""Scale Trade mapped mass to BEA ITA goods+services national totals."""

from __future__ import annotations

from typing import Literal

import pandas as pd

from bedrock.extract.bea.BEA_ITA import ita_gs_totals_usd
from bedrock.transform.flowbysector import FlowBySector

TradeDirection = Literal['exports', 'imports']


def trade_direction_from_method(method: str) -> TradeDirection:
    """Infer ITA direction from a Trade FBS method name."""
    if 'Exports' in method:
        return 'exports'
    if 'Imports' in method:
        return 'imports'
    raise ValueError(
        f"Trade method {method!r} must contain 'Exports' or 'Imports' "
        'to choose an ITA G+S total'
    )


def scale_amounts_to_ita(
    amounts: pd.Series,
    year: int,
    direction: TradeDirection,
) -> pd.Series:
    """Multiply a commodity amount series so its sum matches ITA G+S.

    Denominator is the series sum (typically Detail commodities that survive
    reindex onto ``USA_2017_COMMODITY_CODES``). Industry-only Crosswalk rows
    such as ``331314`` are not SUT commodities and must not be in the
    denominator if they are dropped before overlay.
    """
    totals = ita_gs_totals_usd(year)
    target = float(totals[direction])
    mapped = float(pd.to_numeric(amounts, errors='coerce').sum())
    if mapped == 0.0:
        raise ValueError(
            f'Trade mapped mass is zero for {year} {direction}; '
            'cannot scale to ITA G+S'
        )
    scale = target / mapped
    out = pd.to_numeric(amounts, errors='coerce') * scale
    out.name = amounts.name
    return out


def scale_trade_fbs_to_ita(
    fbs: FlowBySector | pd.DataFrame,
    year: int,
    direction: TradeDirection,
) -> FlowBySector | pd.DataFrame:
    """Multiply Trade FBS ``FlowAmount`` so the national sum matches ITA G+S.

    Prefer ``scale_amounts_to_ita`` on the commodity vector used for overlay
    when some FBS ``SectorProducedBy`` values are not SUT commodities.
    """
    scaled = scale_amounts_to_ita(fbs['FlowAmount'], year, direction)
    out = fbs.copy()
    out['FlowAmount'] = scaled.to_numpy()
    return out
