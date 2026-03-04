import pathlib
from typing import cast

import pandas as pd
import pytest

from bedrock.extract.disaggregation.waste_weights import (
    WasteDisaggCorrespondenceError,
    WasteDisaggWeightError,
    WasteDisaggWeights,
    WasteWeightTable,
    _apply_correspondence_to_series,
    _empty_weight_table,
    load_waste_disagg_weights,
)
from bedrock.utils.config.usa_config import EEIOWasteDisaggConfig
from bedrock.utils.taxonomy.cornerstone.commodities import WASTE_DISAGG_COMMODITIES


def _make_table(
    index: list[str], columns: list[str], values: list[list[float]]
) -> WasteWeightTable:
    return pd.DataFrame(values, index=index, columns=columns, dtype=float)


def test_waste_disagg_weights_construction() -> None:
    idx = ["562111", "562212"]
    # 2x2 table (industry x commodity), e.g. intersection
    tbl = _make_table(idx, idx, [[0.5, 0.0], [0.0, 0.5]])
    empty_tbl = _empty_weight_table()
    w = WasteDisaggWeights(
        use_intersection=tbl.copy(),
        use_waste_industry_columns_all_rows=tbl.copy(),
        use_waste_commodity_rows_all_columns=tbl.copy(),
        use_waste_rows_specific_columns=empty_tbl,
        use_va_rows_for_waste_industry_columns=tbl.copy(),
        use_fd_columns_for_waste_commodity_rows=empty_tbl,
        make_intersection=tbl.copy(),
        make_waste_commodity_columns_all_rows=tbl.copy(),
        make_waste_commodity_columns_specific_rows=empty_tbl,
        make_waste_industry_rows_specific_columns=empty_tbl,
        year=2017,
        source_name="Test",
    )
    assert w.year == 2017
    assert w.source_name == "Test"
    assert list(w.use_intersection.index) == idx
    assert list(w.use_intersection.columns) == idx
    assert w.use_waste_rows_specific_columns.empty
    assert w.make_waste_industry_rows_specific_columns.empty


def test_waste_disagg_weights_required_fields() -> None:
    idx = ["562111"]
    tbl = _make_table(idx, idx, [[1.0]])
    empty_tbl = _empty_weight_table()
    w = WasteDisaggWeights(
        use_intersection=tbl.copy(),
        use_waste_industry_columns_all_rows=tbl.copy(),
        use_waste_commodity_rows_all_columns=tbl.copy(),
        use_waste_rows_specific_columns=empty_tbl,
        use_va_rows_for_waste_industry_columns=tbl.copy(),
        use_fd_columns_for_waste_commodity_rows=empty_tbl,
        make_intersection=tbl.copy(),
        make_waste_commodity_columns_all_rows=tbl.copy(),
        make_waste_commodity_columns_specific_rows=empty_tbl,
        make_waste_industry_rows_specific_columns=empty_tbl,
        year=2017,
        source_name="WasteDisaggregationDetail2017",
    )
    assert hasattr(w, "use_intersection")
    assert hasattr(w, "make_intersection")
    assert hasattr(w, "year")
    assert hasattr(w, "source_name")


@pytest.mark.eeio_integration
def _write_csv(path: pathlib.Path, rows: list[dict[str, str]]) -> None:
    df = pd.DataFrame(rows)
    df.to_csv(path, index=False)


@pytest.mark.eeio_integration
def test_load_waste_disagg_weights_normalizes_slices(tmp_path: pathlib.Path) -> None:
    use_path = tmp_path / "use.csv"
    make_path = tmp_path / "make.csv"
    _write_csv(
        use_path,
        [
            {
                "IndustryCode": "562111/US",
                "CommodityCode": "562000/US",
                "PercentUsed": "0.6",
            },
            {
                "IndustryCode": "562212/US",
                "CommodityCode": "562000/US",
                "PercentUsed": "0.4",
            },
            {
                "IndustryCode": "562111/US",
                "CommodityCode": "562111/US",
                "PercentUsed": "0.3",
            },
            {
                "IndustryCode": "562212/US",
                "CommodityCode": "562212/US",
                "PercentUsed": "0.7",
            },
        ],
    )
    _write_csv(
        make_path,
        [
            {
                "IndustryCode": "562000/US",
                "CommodityCode": "562111/US",
                "PercentMake": "0.25",
            },
            {
                "IndustryCode": "562000/US",
                "CommodityCode": "562212/US",
                "PercentMake": "0.75",
            },
            {
                "IndustryCode": "562111/US",
                "CommodityCode": "562111/US",
                "PercentMake": "0.5",
            },
            {
                "IndustryCode": "562212/US",
                "CommodityCode": "562212/US",
                "PercentMake": "0.5",
            },
        ],
    )

    cfg = EEIOWasteDisaggConfig(
        use_weights_file=str(use_path),
        make_weights_file=str(make_path),
        year=2017,
        source_name="test",
    )
    waste_sectors = ["562111", "562212"]

    weights = load_waste_disagg_weights(
        cfg,
        disagg_original_code="562000",
        disagg_new_codes=waste_sectors,
        waste_sectors=waste_sectors,
        naics_to_cornerstone=None,
    )

    assert pytest.approx(float(weights.use_intersection.values.sum()), rel=1e-6) == 1.0
    assert pytest.approx(float(weights.make_intersection.values.sum()), rel=1e-6) == 1.0
    assert all(code in weights.use_intersection.index for code in waste_sectors)
    assert all(code in weights.use_intersection.columns for code in waste_sectors)
    assert all(code in weights.make_intersection.index for code in waste_sectors)
    assert all(code in weights.make_intersection.columns for code in waste_sectors)


@pytest.mark.eeio_integration
def test_load_waste_disagg_weights_missing_sectors_get_zero_weight(
    tmp_path: pathlib.Path,
) -> None:
    use_path = tmp_path / "use_missing.csv"
    make_path = tmp_path / "make_missing.csv"
    _write_csv(
        use_path,
        [
            {
                "IndustryCode": "562111",
                "CommodityCode": "562111",
                "PercentUsed": "1.0",
            },
        ],
    )
    _write_csv(
        make_path,
        [
            {
                "IndustryCode": "562000",
                "CommodityCode": "562111",
                "PercentMake": "1.0",
            },
        ],
    )

    cfg = EEIOWasteDisaggConfig(
        use_weights_file=str(use_path),
        make_weights_file=str(make_path),
        year=2017,
        source_name="test",
    )
    waste_sectors = ["562111", "562212"]

    weights = load_waste_disagg_weights(
        cfg,
        disagg_original_code="562000",
        disagg_new_codes=waste_sectors,
        waste_sectors=waste_sectors,
        naics_to_cornerstone=None,
    )

    assert weights.use_intersection.loc["562212", :].sum() == pytest.approx(0.0)
    assert weights.use_intersection.loc[:, "562212"].sum() == pytest.approx(0.0)
    assert pytest.approx(float(weights.use_intersection.values.sum()), rel=1e-6) == 1.0


@pytest.mark.eeio_integration
def test_load_waste_disagg_weights_all_zero_raises(tmp_path: pathlib.Path) -> None:
    use_path = tmp_path / "use_zero.csv"
    make_path = tmp_path / "make_zero.csv"
    _write_csv(
        use_path,
        [
            {
                "IndustryCode": "562111",
                "CommodityCode": "562000",
                "PercentUsed": "0.0",
            },
        ],
    )
    _write_csv(
        make_path,
        [
            {
                "IndustryCode": "562000",
                "CommodityCode": "562111",
                "PercentMake": "0.0",
            },
        ],
    )

    cfg = EEIOWasteDisaggConfig(
        use_weights_file=str(use_path),
        make_weights_file=str(make_path),
        year=2017,
        source_name="test",
    )

    with pytest.raises(WasteDisaggWeightError):
        load_waste_disagg_weights(
            cfg,
            disagg_original_code="562000",
            disagg_new_codes=["562111"],
            waste_sectors=["562111"],
            naics_to_cornerstone=None,
        )


@pytest.mark.eeio_integration
def test_load_waste_disagg_weights_missing_required_column(
    tmp_path: pathlib.Path,
) -> None:
    use_path = tmp_path / "use_missing_column.csv"
    make_path = tmp_path / "make_missing_column.csv"
    _write_csv(
        use_path,
        [
            {
                "IndustryCode": "562111",
                "CommodityCode": "562000",
            },
        ],
    )
    _write_csv(
        make_path,
        [
            {
                "IndustryCode": "562000",
                "CommodityCode": "562111",
                "PercentMake": "1.0",
            },
        ],
    )

    cfg = EEIOWasteDisaggConfig(
        use_weights_file=str(use_path),
        make_weights_file=str(make_path),
        year=2017,
        source_name="test",
    )

    with pytest.raises(WasteDisaggWeightError):
        load_waste_disagg_weights(
            cfg,
            disagg_original_code="562000",
            disagg_new_codes=["562111"],
            waste_sectors=["562111"],
            naics_to_cornerstone=None,
        )


@pytest.mark.eeio_integration
def test_load_waste_disagg_weights_nan_values_raise(tmp_path: pathlib.Path) -> None:
    use_path = tmp_path / "use_nan.csv"
    make_path = tmp_path / "make_nan.csv"
    _write_csv(
        use_path,
        [
            {
                "IndustryCode": "562111",
                "CommodityCode": "562000",
                "PercentUsed": "not-a-number",
            },
        ],
    )
    _write_csv(
        make_path,
        [
            {
                "IndustryCode": "562000",
                "CommodityCode": "562111",
                "PercentMake": "1.0",
            },
        ],
    )

    cfg = EEIOWasteDisaggConfig(
        use_weights_file=str(use_path),
        make_weights_file=str(make_path),
        year=2017,
        source_name="test",
    )

    with pytest.raises(WasteDisaggWeightError):
        load_waste_disagg_weights(
            cfg,
            disagg_original_code="562000",
            disagg_new_codes=["562111"],
            waste_sectors=["562111"],
            naics_to_cornerstone=None,
        )


@pytest.mark.eeio_integration
def test_apply_correspondence_to_series_success() -> None:
    series = pd.Series([0.5, 0.5], index=["N1", "N2"], dtype=float)
    mapping = {"N1": ["C1"], "N2": ["C2"]}
    target_codes = ["C1", "C2"]

    result = _apply_correspondence_to_series(series, mapping, target_codes)

    assert list(result.index) == target_codes
    assert pytest.approx(float(result.sum()), rel=1e-6) == 1.0


@pytest.mark.eeio_integration
def test_apply_correspondence_to_series_incomplete_mapping_raises() -> None:
    series = pd.Series([1.0], index=["N1"], dtype=float)
    mapping = {"N1": ["C1"]}
    target_codes = ["C1", "C2"]

    with pytest.raises(WasteDisaggCorrespondenceError):
        _apply_correspondence_to_series(series, mapping, target_codes)


@pytest.mark.eeio_integration
def test_load_waste_disagg_weights_integration_with_2017_files() -> None:
    data_dir = pathlib.Path(__file__).resolve().parents[1]
    use_path = data_dir / "WasteDisaggregationDetail2017_Use.csv"
    make_path = data_dir / "WasteDisaggregationDetail2017_Make.csv"

    cfg = EEIOWasteDisaggConfig(
        use_weights_file=str(use_path),
        make_weights_file=str(make_path),
        year=2017,
        source_name="WasteDisaggregationDetail2017",
    )
    waste_codes = cast(list[str], list(WASTE_DISAGG_COMMODITIES["562000"]))

    weights = load_waste_disagg_weights(
        cfg,
        disagg_original_code="562000",
        disagg_new_codes=waste_codes,
        waste_sectors=waste_codes,
        naics_to_cornerstone=None,
    )

    assert weights.year == 2017
    assert weights.source_name == "WasteDisaggregationDetail2017"
    assert pytest.approx(float(weights.use_intersection.values.sum()), rel=1e-6) == 1.0
    assert pytest.approx(float(weights.make_intersection.values.sum()), rel=1e-6) == 1.0
    assert all(code in weights.use_intersection.index for code in waste_codes)
    assert all(code in weights.use_intersection.columns for code in waste_codes)
    assert all(code in weights.make_intersection.index for code in waste_codes)
    assert all(code in weights.make_intersection.columns for code in waste_codes)
    assert weights.make_intersection.loc["562111", "562111"] == pytest.approx(
        4.23e-01, rel=1e-3
    )
