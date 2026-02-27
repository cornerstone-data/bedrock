import pandas as pd
import pytest

from bedrock.extract.disaggregation.waste_weights import WasteDisaggWeights, WasteWeightSeries


def _make_series(index: list[str], values: list[float]) -> WasteWeightSeries:
    return pd.Series(values, index=index, dtype=float)


def test_waste_disagg_weights_construction() -> None:
    idx = ["562111", "562212"]
    s = _make_series(idx, [0.5, 0.5])
    empty: dict[str, WasteWeightSeries] = {}
    w = WasteDisaggWeights(
        use_intersection=s,
        use_waste_industry_columns_all_rows=s,
        use_waste_commodity_rows_all_columns=s,
        use_waste_rows_specific_columns=empty,
        use_va_rows_for_waste_industry_columns=s,
        use_fd_columns_for_waste_commodity_rows=empty,
        make_intersection=s,
        make_waste_commodity_columns_all_rows=s,
        make_waste_commodity_columns_specific_rows=empty,
        make_waste_industry_rows_specific_columns=empty,
        year=2017,
        source_name="Test",
    )
    assert w.year == 2017
    assert w.source_name == "Test"
    assert list(w.use_intersection.index) == idx
    assert w.use_waste_rows_specific_columns == {}
    assert w.make_waste_industry_rows_specific_columns == {}


def test_waste_disagg_weights_required_fields() -> None:
    idx = ["562111"]
    s = _make_series(idx, [1.0])
    empty: dict[str, WasteWeightSeries] = {}
    w = WasteDisaggWeights(
        use_intersection=s,
        use_waste_industry_columns_all_rows=s,
        use_waste_commodity_rows_all_columns=s,
        use_waste_rows_specific_columns=empty,
        use_va_rows_for_waste_industry_columns=s,
        use_fd_columns_for_waste_commodity_rows=empty,
        make_intersection=s,
        make_waste_commodity_columns_all_rows=s,
        make_waste_commodity_columns_specific_rows=empty,
        make_waste_industry_rows_specific_columns=empty,
        year=2017,
        source_name="WasteDisaggregationDetail2017",
    )
    assert hasattr(w, "use_intersection")
    assert hasattr(w, "make_intersection")
    assert hasattr(w, "year")
    assert hasattr(w, "source_name")
