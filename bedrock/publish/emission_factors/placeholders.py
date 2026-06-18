"""Publish-side valuation placeholders and matrix price adjustment."""

from __future__ import annotations

import pandas as pd

from bedrock.transform.iot.derive_PRO_to_PUR_ratio import phi_for_sectors
from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.economic.inflation_helpers_cornerstone import (
    get_vnorm_adjusted_commodity_price_ratio,
)


def placeholder_margin_ef(without_margins: pd.Series) -> pd.Series[float]:
    """Per-sector margin supply-chain factors; zeros until N_margin is wired."""
    return pd.Series(0.0, index=without_margins.index, dtype=float)


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
