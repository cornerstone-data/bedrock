"""Cornerstone supply-chain emission factor tables from N."""

from __future__ import annotations

from typing import cast

import pandas as pd

from bedrock.publish.model_objects import get_M, get_N, get_N_margin
from bedrock.transform.iot.derive_PRO_to_PUR_ratio import phi_for_sectors
from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.economic.inflation_helpers_cornerstone import (
    get_vnorm_adjusted_commodity_price_ratio,
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


def _greenhouse_gases_row(n: pd.DataFrame) -> pd.Series:
    if GREENHOUSE_GASES_INDICATOR not in n.index:
        raise KeyError(
            f'N missing {GREENHOUSE_GASES_INDICATOR!r} indicator row; '
            f'got index {list(n.index)!r}'
        )
    row = n.loc[GREENHOUSE_GASES_INDICATOR].astype(float)
    return cast(pd.Series, row)


def _unit_label(dollar_year: int, *, purchaser_price: bool) -> str:
    price_type = 'purchaser price' if purchaser_price else 'producer price'
    return f'kg CO2e / {dollar_year} USD, {price_type}'


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


def adjust_publish_matrix(
    matrix: pd.DataFrame,
    *,
    dollar_year: int,
    purchaser_price: bool,
) -> pd.DataFrame:
    """Rebase commodity columns to ``dollar_year`` and optionally apply purchaser Phi."""
    cfg = get_usa_config()
    base_year = cfg.model_base_year
    out = matrix.copy()

    if dollar_year != base_year:
        pi = get_vnorm_adjusted_commodity_price_ratio(base_year, dollar_year)
        price_ratio_for_columns = pi.reindex(out.columns, fill_value=1.0)
        # get_vnorm_adjusted_commodity_price_ratio(base, target) is PI_target / PI_base.
        # Divide EF columns so values are expressed per target-year USD denominator.
        out = out.div(price_ratio_for_columns.values, axis=1)

    if purchaser_price:
        phi = phi_for_sectors(out.columns, year=dollar_year)
        out = out.mul(phi.reindex(out.columns, fill_value=1.0).values, axis=1)

    return out


def build_emission_factor_table(
    *, dollar_year: int, purchaser_price: bool = True
) -> pd.DataFrame:
    """Long-form CO2e supply-chain factors in ``dollar_year`` USD."""
    n_pur = adjust_publish_matrix(
        get_N(),
        dollar_year=dollar_year,
        purchaser_price=purchaser_price,
    )
    n_margin_pur = adjust_publish_matrix(
        get_N_margin(),
        dollar_year=dollar_year,
        purchaser_price=purchaser_price,
    )
    without = _greenhouse_gases_row(n_pur)
    margins = _greenhouse_gases_row(n_margin_pur)
    with_margins = without + margins

    rows: list[dict[str, object]] = []
    for code in without.index.astype(str):
        rows.append(
            {
                COL_CODE: code,
                COL_NAME: COMMODITY_DESC.get(code, ''),
                COL_GHG: GHG_LABEL,
                COL_UNIT: _unit_label(dollar_year, purchaser_price=purchaser_price),
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


def build_purchaser_matrices(
    *, dollar_year: int, purchaser_price: bool = True
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return M and N adjusted to ``dollar_year`` (raw sector labels)."""
    m_pur = adjust_publish_matrix(
        get_M(),
        dollar_year=dollar_year,
        purchaser_price=purchaser_price,
    )
    n_pur = adjust_publish_matrix(
        get_N(),
        dollar_year=dollar_year,
        purchaser_price=purchaser_price,
    )
    return m_pur, n_pur
