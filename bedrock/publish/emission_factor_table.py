"""Cornerstone supply-chain emission factor tables from N."""

from __future__ import annotations

from typing import cast

import pandas as pd

from bedrock.publish.model_objects import get_M, get_N
from bedrock.publish.placeholders import (
    adjust_publish_matrix,
    placeholder_margin_ef,
)
from bedrock.utils.emissions.characterization import GREENHOUSE_GASES_INDICATOR
from bedrock.utils.taxonomy.cornerstone.commodities import COMMODITY_DESC

COL_CODE = 'Cornerstone Commodity Code'
COL_NAME = 'Cornerstone Commodity Name'
COL_GHG = 'GHG'
COL_UNIT = 'Unit'
COL_WITHOUT = 'Supply Chain Emission Factors without Margins'
COL_MARGINS = 'Margins of Supply Chain Emission Factors'
COL_WITH = 'Supply Chain Emission Factors with Margins'

GHG_LABEL = 'All GHGs'
ELECTRICITY_COMMODITY = '221100'


def _greenhouse_gases_row(n: pd.DataFrame) -> pd.Series[float]:
    if GREENHOUSE_GASES_INDICATOR not in n.index:
        raise KeyError(
            f'N missing {GREENHOUSE_GASES_INDICATOR!r} indicator row; '
            f'got index {list(n.index)!r}'
        )
    row = n.loc[GREENHOUSE_GASES_INDICATOR].astype(float)
    return cast(pd.Series[float], row)


def _unit_label(dollar_year: int) -> str:
    return f'kg CO2e / {dollar_year} USD, purchaser price'


def _commodity_base_code(code: str) -> str:
    return code.split('/', 1)[0]


def _is_excluded_commodity(code: str) -> bool:
    base = _commodity_base_code(code)
    if not base:
        return True
    if base[0] in ('S', 'G'):
        return True
    if base == ELECTRICITY_COMMODITY:
        return True
    return False


def build_emission_factor_table(*, dollar_year: int) -> pd.DataFrame:
    """Long-form CO2e supply-chain factors at purchaser price in ``dollar_year``."""
    n_pur = adjust_publish_matrix(
        get_N(),
        dollar_year=dollar_year,
        purchaser_price=True,
    )
    without = _greenhouse_gases_row(n_pur)
    margins = placeholder_margin_ef(without)
    with_margins = without + margins

    rows: list[dict[str, object]] = []
    for code in without.index.astype(str):
        rows.append(
            {
                COL_CODE: code,
                COL_NAME: COMMODITY_DESC.get(code, ''),
                COL_GHG: GHG_LABEL,
                COL_UNIT: _unit_label(dollar_year),
                COL_WITHOUT: float(without[code]),
                COL_MARGINS: float(margins[code]),
                COL_WITH: float(with_margins[code]),
            }
        )
    return pd.DataFrame(rows)


def finalize_cornerstone_ef_table(table: pd.DataFrame) -> pd.DataFrame:
    """Drop government, electricity, and zero-without-margin rows; add ``/US`` codes."""
    if table.empty:
        return table.copy()

    out = table.copy()
    out[COL_CODE] = out[COL_CODE].map(lambda c: f'{_commodity_base_code(str(c))}/US')
    mask = out[COL_CODE].map(lambda c: not _is_excluded_commodity(str(c)))
    out = out.loc[mask]
    out = out.loc[out[COL_WITHOUT] != 0.0]
    return out.reset_index(drop=True)


def build_purchaser_matrices(*, dollar_year: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return M and N at purchaser price in ``dollar_year`` (raw sector labels)."""
    m_pur = adjust_publish_matrix(
        get_M(),
        dollar_year=dollar_year,
        purchaser_price=True,
    )
    n_pur = adjust_publish_matrix(
        get_N(),
        dollar_year=dollar_year,
        purchaser_price=True,
    )
    return m_pur, n_pur
