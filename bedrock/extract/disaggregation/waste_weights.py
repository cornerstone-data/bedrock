"""Backward-compatibility shim. Prefer disagg_weights.py for new code."""

from __future__ import annotations

import pathlib
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from bedrock.extract.disaggregation.disagg_weights import (
    DisaggCorrespondenceError as WasteDisaggCorrespondenceError,
)
from bedrock.extract.disaggregation.disagg_weights import (
    DisaggWeightError as WasteDisaggWeightError,
)
from bedrock.extract.disaggregation.disagg_weights import (
    DisaggWeights,
    _apply_correspondence_to_series,
    _build_specific_rows_table,
    _empty_weight_table,
    _normalize_code,
    _normalize_table,
    _pivot_and_align,
    load_disagg_weights,
    load_weights_csv,
)
from bedrock.extract.disaggregation.disagg_weights import (
    DisaggWeightSeries as WasteWeightSeries,
)
from bedrock.extract.disaggregation.disagg_weights import (
    DisaggWeightTable as WasteWeightTable,
)
from bedrock.extract.disaggregation.disagg_weights import (
    weights_to_csv as _weights_to_csv,
)

__all__ = [
    "WasteDisaggWeights",
    "WasteDisaggCorrespondenceError",
    "WasteDisaggWeightError",
    "WasteWeightSeries",
    "WasteWeightTable",
    "_apply_correspondence_to_series",
    "_build_specific_rows_table",
    "_empty_weight_table",
    "_normalize_code",
    "_normalize_table",
    "_pivot_and_align",
    "load_disagg_weights",
    "load_weights_csv",
    "load_waste_disagg_weights",
    "weights_to_csv",
]

if TYPE_CHECKING:
    from typing import IO

    from bedrock.utils.config.usa_config import EEIOWasteDisaggConfig


@dataclass
class WasteDisaggWeights:
    use_intersection: WasteWeightTable
    use_waste_industry_columns_all_rows: WasteWeightTable
    use_waste_commodity_rows_all_columns: WasteWeightTable
    use_waste_rows_specific_columns: WasteWeightTable
    use_va_rows_for_waste_industry_columns: WasteWeightTable
    use_fd_columns_for_waste_commodity_rows: WasteWeightTable
    make_intersection: WasteWeightTable
    make_waste_commodity_columns_all_rows: WasteWeightTable
    make_waste_commodity_columns_specific_rows: WasteWeightTable
    make_waste_industry_rows_specific_columns: WasteWeightTable
    year: int
    source_name: str

    @classmethod
    def from_disagg_weights(cls, dw: DisaggWeights) -> WasteDisaggWeights:
        return cls(
            use_intersection=dw.use_intersection,
            use_waste_industry_columns_all_rows=dw.use_disagg_industry_columns_all_rows,
            use_waste_commodity_rows_all_columns=dw.use_disagg_commodity_rows_all_columns,
            use_waste_rows_specific_columns=dw.use_disagg_rows_specific_columns,
            use_va_rows_for_waste_industry_columns=dw.use_va_rows_for_disagg_industry_columns,
            use_fd_columns_for_waste_commodity_rows=dw.use_fd_columns_for_disagg_commodity_rows,
            make_intersection=dw.make_intersection,
            make_waste_commodity_columns_all_rows=dw.make_disagg_commodity_columns_all_rows,
            make_waste_commodity_columns_specific_rows=dw.make_disagg_commodity_columns_specific_rows,
            make_waste_industry_rows_specific_columns=dw.make_disagg_industry_rows_specific_columns,
            year=dw.year,
            source_name=dw.source_name,
        )

    def to_disagg_weights(self) -> DisaggWeights:
        return DisaggWeights(
            use_intersection=self.use_intersection,
            use_disagg_industry_columns_all_rows=self.use_waste_industry_columns_all_rows,
            use_disagg_commodity_rows_all_columns=self.use_waste_commodity_rows_all_columns,
            use_disagg_rows_specific_columns=self.use_waste_rows_specific_columns,
            use_va_rows_for_disagg_industry_columns=self.use_va_rows_for_waste_industry_columns,
            use_fd_columns_for_disagg_commodity_rows=self.use_fd_columns_for_waste_commodity_rows,
            make_intersection=self.make_intersection,
            make_disagg_commodity_columns_all_rows=self.make_waste_commodity_columns_all_rows,
            make_disagg_commodity_columns_specific_rows=self.make_waste_commodity_columns_specific_rows,
            make_disagg_industry_rows_specific_columns=self.make_waste_industry_rows_specific_columns,
            year=self.year,
            source_name=self.source_name,
        )


def load_waste_disagg_weights(
    cfg: EEIOWasteDisaggConfig,
    *,
    disagg_original_code: str,
    disagg_new_codes: list[str],
    waste_sectors: list[str],
    va_row_codes: list[str] | None = None,
    waste_industry_sectors: list[str] | None = None,
) -> WasteDisaggWeights:
    dw = load_disagg_weights(
        cfg,
        original_code=disagg_original_code,
        new_codes=disagg_new_codes,
        disagg_sectors=waste_sectors,
        va_row_codes=va_row_codes,
        industry_subsectors=waste_industry_sectors,
    )
    return WasteDisaggWeights.from_disagg_weights(dw)


def weights_to_csv(weights: WasteDisaggWeights, file: IO[str] | None = None) -> None:
    _weights_to_csv(weights.to_disagg_weights(), file=file)


if __name__ == "__main__":
    from bedrock.utils.config.usa_config import EEIOWasteDisaggConfig
    from bedrock.utils.taxonomy.cornerstone.commodities import WASTE_DISAGG_COMMODITIES

    _data_dir = pathlib.Path(__file__).resolve().parent
    _cfg = EEIOWasteDisaggConfig(
        use_weights_file=str(_data_dir / "WasteDisaggregationDetail2017_Use.csv"),
        make_weights_file=str(_data_dir / "WasteDisaggregationDetail2017_Make.csv"),
        year=2017,
        source_name="WasteDisaggregationDetail2017",
    )
    _weights = load_waste_disagg_weights(
        _cfg,
        disagg_original_code="562000",
        disagg_new_codes=cast(list[str], list(WASTE_DISAGG_COMMODITIES["562000"])),
        waste_sectors=cast(list[str], list(WASTE_DISAGG_COMMODITIES["562000"])),
    )
    # weights_to_csv(_weights, 'weights.csv')

    print("use_intersection")
    print(_weights.use_intersection)
    print("\nuse_waste_industry_columns_all_rows")
    print(_weights.use_waste_industry_columns_all_rows)
    print("\nuse_waste_commodity_rows_all_columns")
    print(_weights.use_waste_commodity_rows_all_columns)
    print("\nuse_waste_rows_specific_columns")
    print(_weights.use_waste_rows_specific_columns)
    print("\nuse_va_rows_for_waste_industry_columns")
    print(_weights.use_va_rows_for_waste_industry_columns)
    print("\nuse_fd_columns_for_waste_commodity_rows")
    print(_weights.use_fd_columns_for_waste_commodity_rows)
    print("\nmake_intersection")
    print(_weights.make_intersection)
    print("\nmake_waste_commodity_columns_all_rows")
    print(_weights.make_waste_commodity_columns_all_rows)
    print("\nmake_waste_commodity_columns_specific_rows")
    print(_weights.make_waste_commodity_columns_specific_rows)
    print("\nmake_waste_industry_rows_specific_columns")
    print(_weights.make_waste_industry_rows_specific_columns)
