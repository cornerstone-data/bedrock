"""Compare Make (V) vs Use totals at each cornerstone pipeline stage.

Runs ``compare_output_from_make_and_use`` from
``eeio_diagnostics`` at each stage for Industry and Commodity output.
"""

from __future__ import annotations

import argparse
import logging

import numpy as np
import pandas as pd

from bedrock.extract.iot.io_2017 import (
    load_2017_Uimp_usa,
    load_2017_Utot_usa,
    load_2017_V_usa,
    load_2017_value_added_usa,
    load_2017_Ytot_usa,
)
from bedrock.transform.eeio.cornerstone_expansion import (
    commodity_corresp,
    industry_corresp,
)
from bedrock.transform.eeio.derived_cornerstone import get_waste_disagg_weights
from bedrock.transform.eeio.waste_disaggregation import (
    apply_waste_disagg_to_U,
    apply_waste_disagg_to_V,
    apply_waste_disagg_to_VA,
    apply_waste_disagg_to_Ytot,
)
from bedrock.utils.config.usa_config import reset_usa_config, set_global_usa_config
from bedrock.utils.math.handle_negatives import (
    handle_negative_matrix_values,
    handle_negative_vector_values,
)
from bedrock.utils.schemas.single_region_types import SingleRegionYtotAndTradeVectorSet
from bedrock.utils.taxonomy.bea.v2017_final_demand import (
    USA_2017_FINAL_DEMAND_EXPORT_CODE,
    USA_2017_FINAL_DEMAND_IMPORT_CODE,
)
from bedrock.utils.validation.eeio_diagnostics import (
    DiagnosticResult,
    compare_output_from_make_and_use,
    format_diagnostic_result,
)

log = logging.getLogger(__name__)

DEFAULT_CONFIG = "2025_usa_cornerstone_full_model.yaml"
DEFAULT_TOLERANCE = 0.05


def ytot_matrix_to_set(ytot: pd.DataFrame) -> SingleRegionYtotAndTradeVectorSet:
    """Build ytot / exports / imports vectors from a full Y matrix (scratchbook stages)."""
    return SingleRegionYtotAndTradeVectorSet(
        ytot=handle_negative_vector_values(
            ytot.drop(
                columns=[
                    USA_2017_FINAL_DEMAND_EXPORT_CODE,
                    USA_2017_FINAL_DEMAND_IMPORT_CODE,
                ]
            ).sum(axis=1)
        ),
        exports=ytot[USA_2017_FINAL_DEMAND_EXPORT_CODE],
        imports=(
            -1 * ytot[USA_2017_FINAL_DEMAND_IMPORT_CODE].apply(lambda x: np.min(x, 0))
        ),
    )


def build_pipeline_stages() -> (
    list[tuple[str, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]]
):
    """Return (stage_name, V, U_intermediate, Ytot, VA) for each pipeline stage."""
    com_c = commodity_corresp()
    ind_c = industry_corresp()
    weights = get_waste_disagg_weights()

    v_bea = load_2017_V_usa()
    v_corr = industry_corresp() @ v_bea @ commodity_corresp().T
    v_corr.index.name = "sector"
    v_corr.columns.name = "sector"

    udom_bea = load_2017_Utot_usa() - load_2017_Uimp_usa()
    uimp_bea = load_2017_Uimp_usa()
    udom_corr = com_c @ udom_bea @ ind_c.T
    uimp_corr = com_c @ uimp_bea @ ind_c.T
    for df in (udom_corr, uimp_corr):
        df.index.name = "sector"
        df.columns.name = "sector"

    y_bea = load_2017_Ytot_usa()
    y_corr = commodity_corresp() @ y_bea
    y_corr.index.name = "sector"

    va_bea = load_2017_value_added_usa()
    va_corr = va_bea @ industry_corresp().T
    va_corr.index.name = "sector"
    va_corr.columns.name = "sector"

    stages: list[tuple[str, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]] = [
        ("bea_raw", v_bea, udom_bea + uimp_bea, y_bea, va_bea),
        (
            "after_correspondence",
            v_corr,
            udom_corr + uimp_corr,
            y_corr,
            va_corr,
        ),
    ]

    v_disagg = v_corr
    udom_disagg, uimp_disagg = udom_corr, uimp_corr
    y_disagg = y_corr
    va_disagg = va_corr

    if weights is not None:
        v_disagg = apply_waste_disagg_to_V(v_corr, weights)
        v_disagg.index.name = "sector"
        v_disagg.columns.name = "sector"
        udom_disagg, uimp_disagg = apply_waste_disagg_to_U(
            udom_corr, uimp_corr, weights
        )
        for df in (udom_disagg, uimp_disagg):
            df.index.name = "sector"
            df.columns.name = "sector"
        y_disagg = apply_waste_disagg_to_Ytot(y_corr, weights)
        y_disagg.index.name = "sector"
        va_disagg = apply_waste_disagg_to_VA(va_corr, weights)
        va_disagg.index.name = "sector"
        va_disagg.columns.name = "sector"
        stages.append(
            (
                "after_waste_disagg",
                v_disagg,
                udom_disagg + uimp_disagg,
                y_disagg,
                va_disagg,
            )
        )

    udom_final = handle_negative_matrix_values(udom_disagg)
    uimp_final = handle_negative_matrix_values(uimp_disagg)
    stages.append(
        (
            "final",
            v_disagg,
            udom_final + uimp_final,
            y_disagg,
            va_disagg,
        )
    )
    return stages


def _print_result(output: str, result: DiagnosticResult) -> None:
    status = "PASS" if result.passed else "FAIL"
    print(
        f"{output} (x_V vs Use-side output): max_rel_diff={result.max_rel_diff:.4g}, "
        f"failures>{result.tolerance:.0%}={len(result.failing_sectors)} — {status}"
    )
    if not result.passed:
        for sector in result.failing_sectors:
            print(f"  FAIL {sector}")


def compare_stage(
    stage: str,
    *,
    v: pd.DataFrame,
    u: pd.DataFrame,
    ytot: pd.DataFrame,
    va: pd.DataFrame,
    tolerance: float,
    include_details: bool,
) -> tuple[DiagnosticResult, DiagnosticResult]:
    y_set = ytot_matrix_to_set(ytot)
    industry = compare_output_from_make_and_use(
        output="Industry",
        V=v,
        U=u,
        VA=va,
        y_set=y_set,
        tolerance=tolerance,
        include_details=include_details,
    )
    commodity = compare_output_from_make_and_use(
        output="Commodity",
        V=v,
        U=u,
        VA=va,
        y_set=y_set,
        tolerance=tolerance,
        include_details=include_details,
    )
    print(f"\n=== {stage} ===")
    _print_result("Industry", industry)
    _print_result("Commodity", commodity)
    if include_details and (not industry.passed or not commodity.passed):
        if not industry.passed:
            print(format_diagnostic_result(industry))
        if not commodity.passed:
            print(format_diagnostic_result(commodity))
    return industry, commodity


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare Make vs Use at each cornerstone pipeline stage."
    )
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG,
        help=f"USA config YAML (default: {DEFAULT_CONFIG})",
    )
    parser.add_argument(
        "--tolerance",
        type=float,
        default=DEFAULT_TOLERANCE,
        help=f"Relative tolerance (default: {DEFAULT_TOLERANCE})",
    )
    parser.add_argument(
        "--details",
        action="store_true",
        help="Print formatted diagnostic details on failure",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    reset_usa_config()
    set_global_usa_config(args.config)
    log.info("config=%s tolerance=%s", args.config, args.tolerance)

    for stage_name, v, u, ytot, va in build_pipeline_stages():
        compare_stage(
            stage_name,
            v=v,
            u=u,
            ytot=ytot,
            va=va,
            tolerance=args.tolerance,
            include_details=args.details,
        )


if __name__ == "__main__":
    main()
