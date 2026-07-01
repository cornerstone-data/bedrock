"""Unit tests for PR3 electricity disaggregation analysis export."""

from __future__ import annotations

from pathlib import Path
from unittest import mock

import pandas as pd
import pytest

from bedrock.analysis.electricity.disaggregation_matrices import (
    GO_WEIGHTS_FILENAME,
    OUTPUT_FILENAME,
    WEIGHTS_FILENAME,
    PipelineStage,
    _build_electricity_balance_summary,
    _derive_y_after_waste_disagg,
    assert_disaggregation_export_config,
    write_electricity_disaggregation_intermediate_outputs,
)
from bedrock.transform.eeio.electricity_disaggregation import (
    DISAGG_BALANCE_ATOL,
    ELECTRICITY_AGGREGATE,
)
from bedrock.utils.schemas.cornerstone_schemas import ELECTRICITY_DISAGG_SECTORS


def _minimal_stage(
    label: str,
    *,
    sectors: list[str],
    diagonal_go: float | dict[str, float] = 100.0,
) -> PipelineStage:
    idx = pd.Index(sectors, name="sector")
    v = pd.DataFrame(0.0, index=idx, columns=idx)
    if isinstance(diagonal_go, dict):
        for sector, value in diagonal_go.items():
            v.at[sector, sector] = value
    else:
        for sector in sectors:
            v.at[sector, sector] = diagonal_go / len(sectors)
    udom = pd.DataFrame(0.0, index=idx, columns=idx)
    uimp = udom.copy()
    va = pd.DataFrame(0.0, index=["V00100"], columns=idx)
    y = pd.DataFrame(1.0, index=idx, columns=["F01000"])
    extended_u = pd.DataFrame(1.0, index=idx, columns=list(idx) + ["F01000"])
    intermediate = udom.copy()
    return PipelineStage(
        label=label,
        v=v,
        udom=udom,
        uimp=uimp,
        va=va,
        y=y,
        extended_u=extended_u,
        intermediate=intermediate,
    )


def test_assert_disaggregation_export_config_requires_all_flags() -> None:
    with mock.patch(
        "bedrock.analysis.electricity.disaggregation_matrices.get_usa_config"
    ) as cfg_mock:
        cfg = mock.Mock(
            implement_waste_disaggregation=True,
            implement_electricity_reallocation=True,
            implement_electricity_disaggregation=False,
        )
        cfg_mock.return_value = cfg
        with pytest.raises(ValueError, match="implement_electricity_disaggregation"):
            assert_disaggregation_export_config()


def test_disaggregation_export_writes_expected_artifacts(tmp_path: Path) -> None:
    sectors405 = ["111000", ELECTRICITY_AGGREGATE, "221200"]
    sectors407 = [
        "111000",
        *list(ELECTRICITY_DISAGG_SECTORS),
        "221200",
    ]
    stage1 = _minimal_stage("after_waste_disagg", sectors=sectors405, diagonal_go=300.0)
    stage2 = _minimal_stage(
        "after_electricity_reallocation", sectors=sectors405, diagonal_go=300.0
    )
    stage3 = _minimal_stage(
        "after_electricity_disaggregation",
        sectors=sectors407,
        diagonal_go={s: 100.0 for s in ELECTRICITY_DISAGG_SECTORS},
    )

    with (
        mock.patch(
            "bedrock.analysis.electricity.disaggregation_matrices._build_stage_tables",
            return_value=(stage1, stage2, stage3),
        ),
        mock.patch(
            "bedrock.analysis.electricity.disaggregation_matrices.build_electricity_disagg_go_weights",
            return_value=pd.Series(
                {"221110": 0.5, "221121": 0.1, "221122": 0.4},
            ),
        ),
        mock.patch(
            "bedrock.analysis.electricity.disaggregation_matrices.build_electricity_disagg_weights",
            return_value=mock.Mock(),
        ),
        mock.patch(
            "bedrock.analysis.electricity.disaggregation_matrices.weights_to_csv",
        ),
        mock.patch(
            "bedrock.analysis.electricity.disaggregation_matrices.assert_disaggregation_export_config",
        ),
    ):
        dest = write_electricity_disaggregation_intermediate_outputs(
            output_dir=tmp_path,
            output_path=tmp_path / OUTPUT_FILENAME,
        )

    assert dest.exists()
    assert (tmp_path / GO_WEIGHTS_FILENAME).exists()
    assert (tmp_path / WEIGHTS_FILENAME).exists()

    workbook = pd.ExcelFile(dest)
    expected_sheets = {
        "V_after_waste_disagg",
        "U_after_waste_disagg",
        "Y_after_waste_disagg",
        "V_after_elec_reallocation",
        "U_after_elec_reallocation",
        "Y_after_elec_reallocation",
        "V_after_elec_disaggregation",
        "U_after_elec_disaggregation",
        "Y_after_elec_disaggregation",
        "totals_after_waste_disagg",
        "totals_after_elec_realloc",
        "totals_after_elec_disagg",
        "totals_delta_realloc",
        "totals_delta_disagg",
        "electricity_balance",
    }
    assert expected_sheets.issubset(set(workbook.sheet_names))

    v3 = pd.read_excel(dest, sheet_name="V_after_elec_disaggregation", index_col=0)
    assert ELECTRICITY_AGGREGATE not in v3.index
    assert ELECTRICITY_AGGREGATE not in v3.columns


def test_electricity_balance_sheet_logic() -> None:
    sectors405 = [ELECTRICITY_AGGREGATE]
    sectors407 = list(ELECTRICITY_DISAGG_SECTORS)
    stage2 = _minimal_stage("stage2", sectors=sectors405, diagonal_go=300.0)
    stage3 = _minimal_stage(
        "stage3",
        sectors=sectors407,
        diagonal_go={s: 100.0 for s in ELECTRICITY_DISAGG_SECTORS},
    )

    balance = _build_electricity_balance_summary(stage2, stage3)
    x_row = balance.loc[balance["metric"] == "make_row_x"].iloc[0]
    assert float(x_row["delta"]) == pytest.approx(0.0, abs=DISAGG_BALANCE_ATOL)
    assert bool(x_row["passes"]) is True


def test_derive_y_after_waste_disagg_skips_electricity_row_split() -> None:
    fake_y = pd.DataFrame({"F01000": [1.0]}, index=pd.Index(["221100"], name="sector"))
    with (
        mock.patch(
            "bedrock.analysis.electricity.disaggregation_matrices.load_2017_Ytot_usa",
            return_value=fake_y,
        ),
        mock.patch(
            "bedrock.analysis.electricity.disaggregation_matrices.commodity_corresp",
            return_value=pd.DataFrame(
                [[1.0]], index=pd.Index(["221100"]), columns=pd.Index(["221100"])
            ),
        ),
        mock.patch(
            "bedrock.analysis.electricity.disaggregation_matrices.get_waste_disagg_weights",
            return_value=None,
        ),
        mock.patch(
            "bedrock.transform.eeio.electricity_disaggregation.disaggregate_electricity_commodity_row_in_y",
        ) as disagg_y_mock,
    ):
        out = _derive_y_after_waste_disagg()
        disagg_y_mock.assert_not_called()
        assert ELECTRICITY_AGGREGATE in out.index
