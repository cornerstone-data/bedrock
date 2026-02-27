from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    WasteWeightSeries = pd.Series[float]
else:
    WasteWeightSeries = pd.Series


@dataclass
class WasteDisaggWeights:
    use_intersection: WasteWeightSeries
    use_waste_industry_columns_all_rows: WasteWeightSeries
    use_waste_commodity_rows_all_columns: WasteWeightSeries
    use_waste_rows_specific_columns: dict[str, WasteWeightSeries]
    use_va_rows_for_waste_industry_columns: WasteWeightSeries
    use_fd_columns_for_waste_commodity_rows: dict[str, WasteWeightSeries]
    make_intersection: WasteWeightSeries
    make_waste_commodity_columns_all_rows: WasteWeightSeries
    make_waste_commodity_columns_specific_rows: dict[str, WasteWeightSeries]
    make_waste_industry_rows_specific_columns: dict[str, WasteWeightSeries]
    year: int
    source_name: str
