"""Compare Make (V) vs Use totals at each cornerstone pipeline stage.

Runs ``compare_output_from_make_and_use`` from
``eeio_diagnostics`` at each stage for Industry and Commodity output.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

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
from bedrock.transform.eeio.cornerstone_disagg_pipeline import get_waste_disagg_weights
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
DEFAULT_CSV_DIR = Path(__file__).resolve().parent / "output"
DEFAULT_CSV_SUMMARY = DEFAULT_CSV_DIR / "compare_make_use_by_stages_summary.csv"
DEFAULT_CSV_FAILURES = DEFAULT_CSV_DIR / "compare_make_use_by_stages_failures.csv"


def ytot_matrix_to_set(ytot: pd.DataFrame) -> SingleRegionYtotAndTradeVectorSet:
    """Build ytot / exports / imports vectors from a full Y matrix."""
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


def _summary_row(
    stage: str, output: str, result: DiagnosticResult
) -> dict[str, object]:
    return {
        "stage": stage,
        "output": output,
        "passed": result.passed,
        "tolerance": result.tolerance,
        "max_rel_diff": result.max_rel_diff,
        "n_failing_sectors": len(result.failing_sectors),
        "failing_sectors": ";".join(result.failing_sectors),
    }


def _failure_rows(
    stage: str, output: str, result: DiagnosticResult
) -> list[dict[str, str]]:
    return [
        {"stage": stage, "output": output, "sector": sector}
        for sector in result.failing_sectors
    ]


def write_results_csv(
    summary_rows: list[dict[str, object]],
    failure_rows: list[dict[str, str]],
    *,
    summary_path: Path,
    failures_path: Path | None = None,
) -> None:
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(summary_rows).to_csv(summary_path, index=False)
    log.info("wrote summary CSV: %s", summary_path)

    if failures_path is not None:
        failures_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(failure_rows).to_csv(failures_path, index=False)
        log.info("wrote failures CSV: %s", failures_path)


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
) -> tuple[
    DiagnosticResult, DiagnosticResult, list[dict[str, object]], list[dict[str, str]]
]:
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
    summary_rows = [
        _summary_row(stage, "Industry", industry),
        _summary_row(stage, "Commodity", commodity),
    ]
    failure_rows = _failure_rows(stage, "Industry", industry) + _failure_rows(
        stage, "Commodity", commodity
    )
    return industry, commodity, summary_rows, failure_rows


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
    parser.add_argument(
        "--csv",
        nargs="?",
        const=str(DEFAULT_CSV_SUMMARY),
        default=None,
        help=(
            "Write summary results to CSV; default path "
            f"{DEFAULT_CSV_SUMMARY} when flag is given without a path"
        ),
    )
    parser.add_argument(
        "--csv-failures",
        nargs="?",
        const=str(DEFAULT_CSV_FAILURES),
        default=None,
        help=(
            "Write one row per failing sector; default path "
            f"{DEFAULT_CSV_FAILURES} when flag is given without a path"
        ),
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    reset_usa_config()
    set_global_usa_config(args.config)
    log.info("config=%s tolerance=%s", args.config, args.tolerance)

    summary_rows: list[dict[str, object]] = []
    failure_rows: list[dict[str, str]] = []
    for stage_name, v, u, ytot, va in build_pipeline_stages():
        _, _, stage_summary, stage_failures = compare_stage(
            stage_name,
            v=v,
            u=u,
            ytot=ytot,
            va=va,
            tolerance=args.tolerance,
            include_details=args.details,
        )
        summary_rows.extend(stage_summary)
        failure_rows.extend(stage_failures)

    if args.csv is not None:
        summary_path = Path(args.csv)
        failures_path = Path(args.csv_failures) if args.csv_failures else None
        if args.csv_failures is None and args.csv == str(DEFAULT_CSV_SUMMARY):
            failures_path = DEFAULT_CSV_FAILURES
        write_results_csv(
            summary_rows,
            failure_rows,
            summary_path=summary_path,
            failures_path=failures_path,
        )


if __name__ == "__main__":
    main()
