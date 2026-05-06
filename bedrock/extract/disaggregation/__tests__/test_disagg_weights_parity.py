from __future__ import annotations

import pathlib
from collections.abc import Generator
from typing import ClassVar, TypedDict, cast
from unittest.mock import patch

import pandas as pd
import pytest

from bedrock.extract.disaggregation.disagg_weights import (
    DisaggWeights,
    load_disagg_weights,
)
from bedrock.extract.disaggregation.waste_weights import (
    WasteDisaggWeights,
    load_waste_disagg_weights,
)
from bedrock.transform.eeio import derived_cornerstone as dc
from bedrock.transform.eeio.__tests__.test_waste_disagg_pipeline_integration import (
    _setup_config,
    _teardown,
)
from bedrock.utils.config.usa_config import EEIOWasteDisaggConfig
from bedrock.utils.taxonomy.cornerstone.commodities import WASTE_DISAGG_COMMODITIES

_DATA_DIR = pathlib.Path(__file__).resolve().parents[1]
_USE_PATH = _DATA_DIR / "WasteDisaggregationDetail2017_Use.csv"
_MAKE_PATH = _DATA_DIR / "WasteDisaggregationDetail2017_Make.csv"
_WASTE_CODES = cast(list[str], list(WASTE_DISAGG_COMMODITIES["562000"]))
_VA_ROWS = ["V00100", "V00200", "V00300"]

_cfg = EEIOWasteDisaggConfig(
    use_weights_file=str(_USE_PATH),
    make_weights_file=str(_MAKE_PATH),
    year=2017,
    source_name="WasteDisaggregationDetail2017",
)


@pytest.fixture(scope="module")
def old_weights() -> WasteDisaggWeights:
    return load_waste_disagg_weights(
        _cfg,
        disagg_original_code="562000",
        disagg_new_codes=_WASTE_CODES,
        waste_sectors=_WASTE_CODES,
        va_row_codes=_VA_ROWS,
    )


@pytest.fixture(scope="module")
def new_weights() -> DisaggWeights:
    return load_disagg_weights(
        _cfg,
        original_code="562000",
        new_codes=_WASTE_CODES,
        disagg_sectors=_WASTE_CODES,
        va_row_codes=_VA_ROWS,
    )


def _assert_slices_equal(old: pd.DataFrame, new: pd.DataFrame, name: str) -> None:
    pd.testing.assert_frame_equal(old, new, check_names=True, obj=name)


@pytest.mark.eeio_integration
class TestDisaggWeightsParity:
    def test_use_intersection(
        self, old_weights: WasteDisaggWeights, new_weights: DisaggWeights
    ) -> None:
        _assert_slices_equal(
            old_weights.use_intersection,
            new_weights.use_intersection,
            "use_intersection",
        )

    def test_use_disagg_industry_columns_all_rows(
        self, old_weights: WasteDisaggWeights, new_weights: DisaggWeights
    ) -> None:
        _assert_slices_equal(
            old_weights.use_waste_industry_columns_all_rows,
            new_weights.use_disagg_industry_columns_all_rows,
            "use_disagg_industry_columns_all_rows",
        )

    def test_use_disagg_commodity_rows_all_columns(
        self, old_weights: WasteDisaggWeights, new_weights: DisaggWeights
    ) -> None:
        _assert_slices_equal(
            old_weights.use_waste_commodity_rows_all_columns,
            new_weights.use_disagg_commodity_rows_all_columns,
            "use_disagg_commodity_rows_all_columns",
        )

    def test_use_disagg_rows_specific_columns(
        self, old_weights: WasteDisaggWeights, new_weights: DisaggWeights
    ) -> None:
        _assert_slices_equal(
            old_weights.use_waste_rows_specific_columns,
            new_weights.use_disagg_rows_specific_columns,
            "use_disagg_rows_specific_columns",
        )

    def test_use_va_rows_for_disagg_industry_columns(
        self, old_weights: WasteDisaggWeights, new_weights: DisaggWeights
    ) -> None:
        _assert_slices_equal(
            old_weights.use_va_rows_for_waste_industry_columns,
            new_weights.use_va_rows_for_disagg_industry_columns,
            "use_va_rows_for_disagg_industry_columns",
        )

    def test_use_fd_columns_for_disagg_commodity_rows(
        self, old_weights: WasteDisaggWeights, new_weights: DisaggWeights
    ) -> None:
        _assert_slices_equal(
            old_weights.use_fd_columns_for_waste_commodity_rows,
            new_weights.use_fd_columns_for_disagg_commodity_rows,
            "use_fd_columns_for_disagg_commodity_rows",
        )

    def test_make_intersection(
        self, old_weights: WasteDisaggWeights, new_weights: DisaggWeights
    ) -> None:
        _assert_slices_equal(
            old_weights.make_intersection,
            new_weights.make_intersection,
            "make_intersection",
        )

    def test_make_disagg_commodity_columns_all_rows(
        self, old_weights: WasteDisaggWeights, new_weights: DisaggWeights
    ) -> None:
        _assert_slices_equal(
            old_weights.make_waste_commodity_columns_all_rows,
            new_weights.make_disagg_commodity_columns_all_rows,
            "make_disagg_commodity_columns_all_rows",
        )

    def test_make_disagg_commodity_columns_specific_rows(
        self, old_weights: WasteDisaggWeights, new_weights: DisaggWeights
    ) -> None:
        _assert_slices_equal(
            old_weights.make_waste_commodity_columns_specific_rows,
            new_weights.make_disagg_commodity_columns_specific_rows,
            "make_disagg_commodity_columns_specific_rows",
        )

    def test_make_disagg_industry_rows_specific_columns(
        self, old_weights: WasteDisaggWeights, new_weights: DisaggWeights
    ) -> None:
        _assert_slices_equal(
            old_weights.make_waste_industry_rows_specific_columns,
            new_weights.make_disagg_industry_rows_specific_columns,
            "make_disagg_industry_rows_specific_columns",
        )

    def test_year_and_source_name(
        self, old_weights: WasteDisaggWeights, new_weights: DisaggWeights
    ) -> None:
        assert old_weights.year == new_weights.year
        assert old_weights.source_name == new_weights.source_name


def _build_weights_via_direct_api() -> WasteDisaggWeights:
    cfg = dc.get_usa_config()
    waste_cfg = cfg.eeio_waste_disaggregation
    if waste_cfg is None:
        waste_cfg = dc.EEIOWasteDisaggConfig(
            use_weights_file="extract/disaggregation/WasteDisaggregationDetail2017_Use.csv",
            make_weights_file="extract/disaggregation/WasteDisaggregationDetail2017_Make.csv",
            year=2017,
            source_name="WasteDisaggregationDetail2017",
        )
    resolved_cfg = dc._resolve_waste_cfg_paths(waste_cfg)
    dw = load_disagg_weights(
        resolved_cfg,
        original_code=dc._WASTE_ORIGINAL_CODE,
        new_codes=list(dc._WASTE_NEW_CODES),
        disagg_sectors=list(dc._WASTE_NEW_CODES),
        va_row_codes=list(dc.VALUE_ADDEDS),
    )
    return WasteDisaggWeights.from_disagg_weights(dw)


class PipelineSnapshot(TypedDict):
    V: pd.DataFrame
    Udom: pd.DataFrame
    Uimp: pd.DataFrame
    VA: pd.DataFrame
    Ytot: pd.DataFrame
    Adom: pd.DataFrame
    Aimp: pd.DataFrame
    B: pd.DataFrame
    q_from_aq: pd.Series
    q: pd.Series
    x: pd.Series


def _run_pipeline_snapshot() -> PipelineSnapshot:
    uset = dc.derive_cornerstone_U_with_negatives()
    aq = dc.derive_cornerstone_Aq()
    return {
        "V": dc.derive_cornerstone_V(),
        "Udom": pd.DataFrame(uset.Udom),
        "Uimp": pd.DataFrame(uset.Uimp),
        "VA": dc.derive_cornerstone_VA(),
        "Ytot": dc._derive_cornerstone_Ytot_with_trade(),
        "Adom": pd.DataFrame(aq.Adom),
        "Aimp": pd.DataFrame(aq.Aimp),
        "q_from_aq": aq.scaled_q.copy(),
        "q": dc.derive_cornerstone_q().copy(),
        "B": dc.derive_cornerstone_B_via_vnorm(),
        "x": dc.derive_cornerstone_x().copy(),
    }


@pytest.mark.eeio_integration
class TestFullPipelineParity:
    run_a: ClassVar[PipelineSnapshot]
    run_b: ClassVar[PipelineSnapshot]

    @pytest.fixture(scope="class", autouse=True)
    def pipeline_results(
        self, request: pytest.FixtureRequest
    ) -> Generator[None, None, None]:
        _setup_config("test_usa_config_waste_disagg")
        try:
            run_a = _run_pipeline_snapshot()
            _teardown()
            _setup_config("test_usa_config_waste_disagg")
            direct_weights = _build_weights_via_direct_api()
            with patch.object(
                dc, "get_waste_disagg_weights", return_value=direct_weights
            ):
                run_b = _run_pipeline_snapshot()
            request.cls.run_a = run_a
            request.cls.run_b = run_b
            yield
        finally:
            _teardown()

    def test_pipeline_parity(self) -> None:
        pd.testing.assert_frame_equal(
            self.run_a["V"], self.run_b["V"], check_like=False, obj="V"
        )
        pd.testing.assert_frame_equal(
            self.run_a["Udom"], self.run_b["Udom"], check_like=False, obj="Udom"
        )
        pd.testing.assert_frame_equal(
            self.run_a["Uimp"], self.run_b["Uimp"], check_like=False, obj="Uimp"
        )
        pd.testing.assert_frame_equal(
            self.run_a["VA"], self.run_b["VA"], check_like=False, obj="VA"
        )
        pd.testing.assert_frame_equal(
            self.run_a["Ytot"], self.run_b["Ytot"], check_like=False, obj="Ytot"
        )
        pd.testing.assert_frame_equal(
            self.run_a["Adom"], self.run_b["Adom"], check_like=False, obj="Adom"
        )
        pd.testing.assert_frame_equal(
            self.run_a["Aimp"], self.run_b["Aimp"], check_like=False, obj="Aimp"
        )
        pd.testing.assert_frame_equal(
            self.run_a["B"], self.run_b["B"], check_like=False, obj="B"
        )
        pd.testing.assert_series_equal(
            self.run_a["q_from_aq"], self.run_b["q_from_aq"], obj="q_from_aq"
        )
        pd.testing.assert_series_equal(self.run_a["q"], self.run_b["q"], obj="q")
        pd.testing.assert_series_equal(self.run_a["x"], self.run_b["x"], obj="x")
