"""Export three-stage electricity pipeline matrices for offline PR3 inspection.

Replicates pipeline checkpoint logic read-only (waste → reallocation → disaggregation).
If production orchestration changes, keep this module aligned manually — no production
hooks are added for analysis.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from bedrock.analysis.electricity.reallocation_matrices import (
    _delta_summary,
    _extended_use_tables,
    _totals_summary,
    _with_publish_loc_suffix,
)
from bedrock.extract.disaggregation.disagg_weights import weights_to_csv
from bedrock.extract.iot.io_2017 import load_2017_Ytot_usa
from bedrock.transform.eeio.cornerstone_disagg_pipeline import (
    derive_cornerstone_U_after_waste,
    derive_cornerstone_V_after_waste,
    derive_cornerstone_VA_after_waste,
    derive_disagg_io_bundle,
    derive_disagg_Ytot_with_trade,
    get_waste_disagg_weights,
)
from bedrock.transform.eeio.cornerstone_expansion import commodity_corresp
from bedrock.transform.eeio.electricity_disaggregation import (
    DISAGG_BALANCE_ATOL,
    ELECTRICITY_AGGREGATE,
    build_electricity_disagg_go_weights,
    build_electricity_disagg_weights,
    reallocate_electricity_coproduction,
)
from bedrock.transform.eeio.waste_disaggregation import apply_waste_disagg_to_Ytot
from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.math.formulas import compute_q, compute_x
from bedrock.utils.schemas.cornerstone_schemas import ELECTRICITY_DISAGG_SECTORS
from bedrock.utils.taxonomy.cornerstone.final_demand import FINAL_DEMANDS

logger = logging.getLogger(__name__)

_OUTPUT_DIR = Path(__file__).resolve().parent / "output"
OUTPUT_FILENAME = "electricity_pipeline_stages_V_U_Y.xlsx"
GO_WEIGHTS_FILENAME = "electricity_disagg_go_weights.csv"
WEIGHTS_FILENAME = "electricity_disagg_weights.csv"


@dataclass(frozen=True)
class PipelineStage:
    """One electricity pipeline checkpoint (V/U/VA/Y + extended Use)."""

    label: str
    v: pd.DataFrame
    udom: pd.DataFrame
    uimp: pd.DataFrame
    va: pd.DataFrame
    y: pd.DataFrame
    extended_u: pd.DataFrame
    intermediate: pd.DataFrame


def assert_disaggregation_export_config() -> None:
    """Require waste + reallocation + disaggregation flags (mirrors USAConfig validator)."""
    cfg = get_usa_config()
    missing = [
        name
        for name, enabled in (
            ("implement_waste_disaggregation", cfg.implement_waste_disaggregation),
            (
                "implement_electricity_reallocation",
                cfg.implement_electricity_reallocation,
            ),
            (
                "implement_electricity_disaggregation",
                cfg.implement_electricity_disaggregation,
            ),
        )
        if not enabled
    ]
    if missing:
        raise ValueError(
            "Electricity disaggregation export requires all of: "
            "implement_waste_disaggregation, implement_electricity_reallocation, "
            f"implement_electricity_disaggregation; missing: {', '.join(missing)}"
        )


def _derive_y_after_waste_disagg() -> pd.DataFrame:
    """Y after correspondence + waste only (no electricity row split).

    Mirrors ``derive_disagg_Ytot_with_trade()`` lines 187–193 only.
    """
    ytot_orig = load_2017_Ytot_usa()
    ytot = commodity_corresp() @ ytot_orig
    ytot.index.name = "sector"
    weights = get_waste_disagg_weights()
    if weights is not None:
        ytot = apply_waste_disagg_to_Ytot(ytot, weights)
        ytot.index.name = "sector"
    return ytot


def _extended_use_for_stage(
    *,
    udom: pd.DataFrame,
    uimp: pd.DataFrame,
    va: pd.DataFrame,
    y_fd: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    return _extended_use_tables(udom=udom, uimp=uimp, va=va, y_fd=y_fd)


def _column_total_use_plus_va(
    udom: pd.DataFrame,
    uimp: pd.DataFrame,
    va: pd.DataFrame,
    industry: str,
) -> float:
    return (
        float(udom[industry].sum())
        + float(uimp[industry].sum())
        + float(va[industry].sum())
    )


def _commodity_row_sum(extended_u: pd.DataFrame, commodity: str) -> float:
    if commodity not in extended_u.index:
        return 0.0
    return float(extended_u.loc[commodity].sum())


def _build_stage_tables() -> tuple[PipelineStage, PipelineStage, PipelineStage]:
    """Build waste, reallocation, and disaggregation checkpoints."""
    v1 = derive_cornerstone_V_after_waste()
    udom1, uimp1 = derive_cornerstone_U_after_waste()
    va1 = derive_cornerstone_VA_after_waste()
    y_waste = _derive_y_after_waste_disagg()
    y_fd_stages_12 = y_waste[list(FINAL_DEMANDS)]
    u1, intermediate1 = _extended_use_for_stage(
        udom=udom1, uimp=uimp1, va=va1, y_fd=y_fd_stages_12
    )
    stage1 = PipelineStage(
        label="after_waste_disagg",
        v=v1,
        udom=udom1,
        uimp=uimp1,
        va=va1,
        y=y_waste,
        extended_u=u1,
        intermediate=intermediate1,
    )

    v2, udom2, uimp2, va2 = reallocate_electricity_coproduction(v1, udom1, uimp1, va1)
    u2, intermediate2 = _extended_use_for_stage(
        udom=udom2, uimp=uimp2, va=va2, y_fd=y_fd_stages_12
    )
    stage2 = PipelineStage(
        label="after_electricity_reallocation",
        v=v2,
        udom=udom2,
        uimp=uimp2,
        va=va2,
        y=y_waste,
        extended_u=u2,
        intermediate=intermediate2,
    )

    bundle = derive_disagg_io_bundle()
    y3 = derive_disagg_Ytot_with_trade()
    y_fd3 = y3[list(FINAL_DEMANDS)]
    u3, intermediate3 = _extended_use_for_stage(
        udom=bundle.Udom,
        uimp=bundle.Uimp,
        va=bundle.VA,
        y_fd=y_fd3,
    )
    stage3 = PipelineStage(
        label="after_electricity_disaggregation",
        v=bundle.V,
        udom=bundle.Udom,
        uimp=bundle.Uimp,
        va=bundle.VA,
        y=y3,
        extended_u=u3,
        intermediate=intermediate3,
    )
    return stage1, stage2, stage3


def _build_electricity_balance_summary(
    stage2: PipelineStage,
    stage3: PipelineStage,
) -> pd.DataFrame:
    """Preservation metrics at the reallocation → disaggregation boundary."""
    agg = ELECTRICITY_AGGREGATE
    elec = list(ELECTRICITY_DISAGG_SECTORS)
    atol = DISAGG_BALANCE_ATOL

    x2 = float(compute_x(V=stage2.v)[agg])
    x3 = sum(float(compute_x(V=stage3.v)[s]) for s in elec)
    q2 = float(compute_q(V=stage2.v)[agg])
    q3 = sum(float(compute_q(V=stage3.v)[s]) for s in elec)
    c221100 = _column_total_use_plus_va(stage2.udom, stage2.uimp, stage2.va, agg)
    go_residual = x2 - c221100
    q_use2 = _commodity_row_sum(stage2.extended_u, agg)
    q_use3 = sum(_commodity_row_sum(stage3.extended_u, s) for s in elec)
    va2 = float(stage2.va[agg].sum())
    va3 = float(stage3.va[elec].sum().sum())
    y2 = float(stage2.y.loc[agg].sum()) if agg in stage2.y.index else 0.0
    y3 = float(stage3.y.loc[elec].sum().sum())

    def _row(
        metric: str,
        stage2_val: float | str,
        stage3_val: float | str,
        *,
        check_balance: bool = True,
    ) -> dict[str, object]:
        if not check_balance:
            return {
                "metric": metric,
                "stage2_value": stage2_val,
                "stage3_value": stage3_val,
                "delta": "",
                "passes": "",
            }
        s2 = float(stage2_val)
        s3 = float(stage3_val)
        delta = s3 - s2
        return {
            "metric": metric,
            "stage2_value": s2,
            "stage3_value": s3,
            "delta": delta,
            "passes": abs(delta) <= atol,
        }

    rows = [
        _row("make_row_x", x2, x3),
        _row("make_col_q", q2, q3),
        _row(
            "use_col_total_udom_uimp_va",
            c221100,
            "N/A (221100 removed)",
            check_balance=False,
        ),
        _row("use_commodity_row_q_use", q_use2, q_use3),
        _row("va_column_sum", va2, va3),
        _row("y_row_sum", y2, y3),
        _row(
            "go_identity_residual_stage2",
            go_residual,
            "—",
            check_balance=False,
        ),
    ]
    return pd.DataFrame(rows)


def _write_analysis_weight_csvs(output_dir: Path) -> None:
    """Write GO/disagg weight CSVs to analysis output (not extract)."""
    output_dir.mkdir(parents=True, exist_ok=True)
    w = build_electricity_disagg_go_weights()
    w.to_csv(output_dir / GO_WEIGHTS_FILENAME, header=["weight"])
    weights = build_electricity_disagg_weights(w)
    with (output_dir / WEIGHTS_FILENAME).open(
        "w", encoding="utf-8", newline=""
    ) as handle:
        weights_to_csv(weights, handle)


def write_electricity_disaggregation_intermediate_outputs(
    *,
    output_dir: Path | None = None,
    output_path: Path | None = None,
) -> Path:
    """Write three-stage V/U/Y workbook and analysis weight CSVs."""
    assert_disaggregation_export_config()
    out_dir = output_dir or _OUTPUT_DIR
    dest = output_path or (out_dir / OUTPUT_FILENAME)
    dest.parent.mkdir(parents=True, exist_ok=True)

    stage1, stage2, stage3 = _build_stage_tables()
    _write_analysis_weight_csvs(out_dir)

    summary1 = _totals_summary(
        stage1.label,
        v=stage1.v,
        extended_u=stage1.extended_u,
        va=stage1.va,
        intermediate=stage1.intermediate,
    )
    summary2 = _totals_summary(
        stage2.label,
        v=stage2.v,
        extended_u=stage2.extended_u,
        va=stage2.va,
        intermediate=stage2.intermediate,
    )
    summary3 = _totals_summary(
        stage3.label,
        v=stage3.v,
        extended_u=stage3.extended_u,
        va=stage3.va,
        intermediate=stage3.intermediate,
    )
    delta_realloc = _delta_summary(summary1, summary2)
    delta_disagg = _delta_summary(summary2, summary3)
    balance = _build_electricity_balance_summary(stage2, stage3)

    with pd.ExcelWriter(dest, engine="openpyxl") as writer:
        _with_publish_loc_suffix(stage1.v).to_excel(
            writer, sheet_name="V_after_waste_disagg", index=True
        )
        _with_publish_loc_suffix(stage1.extended_u).to_excel(
            writer, sheet_name="U_after_waste_disagg", index=True
        )
        stage1.y.to_excel(writer, sheet_name="Y_after_waste_disagg", index=True)
        _with_publish_loc_suffix(stage2.v).to_excel(
            writer, sheet_name="V_after_elec_reallocation", index=True
        )
        _with_publish_loc_suffix(stage2.extended_u).to_excel(
            writer, sheet_name="U_after_elec_reallocation", index=True
        )
        stage2.y.to_excel(writer, sheet_name="Y_after_elec_reallocation", index=True)
        _with_publish_loc_suffix(stage3.v).to_excel(
            writer, sheet_name="V_after_elec_disaggregation", index=True
        )
        _with_publish_loc_suffix(stage3.extended_u).to_excel(
            writer, sheet_name="U_after_elec_disaggregation", index=True
        )
        stage3.y.to_excel(writer, sheet_name="Y_after_elec_disaggregation", index=True)
        summary1.reset_index().to_excel(
            writer, sheet_name="totals_after_waste_disagg", index=False
        )
        summary2.reset_index().to_excel(
            writer, sheet_name="totals_after_elec_realloc", index=False
        )
        summary3.reset_index().to_excel(
            writer, sheet_name="totals_after_elec_disagg", index=False
        )
        delta_realloc.reset_index().to_excel(
            writer, sheet_name="totals_delta_realloc", index=False
        )
        delta_disagg.reset_index().to_excel(
            writer, sheet_name="totals_delta_disagg", index=False
        )
        balance.to_excel(writer, sheet_name="electricity_balance", index=False)

    logger.info(
        "Wrote electricity disaggregation intermediate outputs (16 sheets + 2 CSVs) to %s",
        dest.resolve(),
    )
    return dest
