"""Compare class-priced vs uniform-price mixed-unit EF vectors."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from bedrock.transform.eeio.cornerstone_disagg_pipeline import (
    MixedUnitEfResult,
    compute_mixed_unit_ef_vectors,
    table_2_4_prices_cents_kwh,
)
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_B_non_finetuned,
)
from bedrock.transform.eeio.electricity_disaggregation import GENERATION_SECTOR
from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.schemas.cornerstone_schemas import ELECTRICITY_DISAGG_SECTORS


@dataclass(frozen=True)
class ClassPricesComparison:
    """Class-priced vs uniform mixed EF comparison for electricity sectors."""

    class_result: MixedUnitEfResult
    uniform_result: MixedUnitEfResult
    summary: pd.DataFrame


def _equal_prices_from_table() -> dict[str, float]:
    cfg = get_usa_config()
    prices = table_2_4_prices_cents_kwh(cfg.usa_ghg_data_year)
    total = float(prices['Total'])
    return {k: total for k in prices}


def compare_class_vs_uniform_mixed_efs() -> ClassPricesComparison:
    """Run Track B comparison using non-cached mixed conversion helpers."""
    aq = derive_cornerstone_Aq_scaled()
    b = derive_cornerstone_B_non_finetuned()
    class_result = compute_mixed_unit_ef_vectors(aq, b, prices_by_class=None)
    uniform_result = compute_mixed_unit_ef_vectors(
        aq, b, prices_by_class=_equal_prices_from_table()
    )
    rows: list[dict[str, float | str]] = []
    for sector in ELECTRICITY_DISAGG_SECTORS:
        n_class = float(class_result.N.get(sector, float('nan')))
        n_uniform = float(uniform_result.N.get(sector, float('nan')))
        rows.append(
            {
                'index': sector,
                'N_class': n_class,
                'N_uniform': n_uniform,
                'N_class_minus_N_uniform': n_class - n_uniform,
                'c_col': class_result.c_col,
            }
        )
    gen = GENERATION_SECTOR
    rows.append(
        {
            'index': gen,
            'N_class': float(class_result.N.get(gen, float('nan'))),
            'N_uniform': float(uniform_result.N.get(gen, float('nan'))),
            'N_class_minus_N_uniform': float(class_result.N.get(gen, float('nan')))
            - float(uniform_result.N.get(gen, float('nan'))),
            'c_col': class_result.c_col,
        }
    )
    return ClassPricesComparison(
        class_result=class_result,
        uniform_result=uniform_result,
        summary=pd.DataFrame(rows),
    )
