"""Cornerstone industry / FD → EPA Table 2.4 end-use sector mapping.

Re-exports production helpers from ``electricity_end_use_mapping`` (PR4 home).
"""

from __future__ import annotations

import pandas as pd

from bedrock.transform.eeio.electricity_end_use_mapping import (
    _FD_DEFAULTS,
    END_USE_MAPPING_REVIEW_STATUS,
    EPA_END_USES,
    build_end_use_map,
    build_end_use_map_resolved,
    classify_industry_end_use,
    table_2_4_prices_cents_kwh,
)
from bedrock.utils.schemas.cornerstone_schemas import ELECTRICITY_DISAGG_SECTORS

__all__ = [
    'END_USE_MAPPING_REVIEW_STATUS',
    'EPA_END_USES',
    '_FD_DEFAULTS',
    'build_end_use_map',
    'build_end_use_map_resolved',
    'build_price_tilt_weights_by_column',
    'classify_industry_end_use',
    'table_2_4_prices_cents_kwh',
    'write_default_overrides_csv',
]


def build_price_tilt_weights_by_column(
    w_base: pd.Series[float],
    prices: dict[str, float],
    end_use_map: dict[str, str],
    columns: list[str],
) -> pd.DataFrame:
    """Build per-column 221110/121/122 weights from Table 2.4 price tilt."""
    p_ref = prices['Total']
    tilt = {'221110': -1.0, '221121': 0.5, '221122': 0.5}
    w = w_base.reindex(list(ELECTRICITY_DISAGG_SECTORS)).astype(float)
    out = pd.DataFrame(
        index=list(ELECTRICITY_DISAGG_SECTORS), columns=columns, dtype=float
    )
    for col in columns:
        eu = end_use_map.get(str(col), 'Commercial')
        p_e = prices.get(eu, p_ref)
        price_factor = p_e / p_ref - 1.0
        raw = w * pd.Series(
            {k: 1.0 + price_factor * tilt[k] for k in w.index},
            dtype=float,
        )
        total = float(raw.sum())
        out[col] = raw / total if total else w
    return out


def write_default_overrides_csv() -> None:
    """Deprecated: production override CSV lives under transform/eeio/data/."""
    raise NotImplementedError(
        'Use bedrock/transform/eeio/data/cornerstone_to_epa_end_use.csv'
    )
