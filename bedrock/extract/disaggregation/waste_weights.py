from __future__ import annotations

from dataclasses import dataclass
from typing import IO, TYPE_CHECKING

import pandas as pd

from bedrock.utils.taxonomy.correspondence import create_correspondence_matrix

if TYPE_CHECKING:
    from bedrock.utils.config.usa_config import EEIOWasteDisaggConfig

    WasteWeightSeries = pd.Series[float]
else:
    WasteWeightSeries = pd.Series


class WasteDisaggWeightError(Exception):
    pass


class WasteDisaggCorrespondenceError(Exception):
    pass


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


def _normalize_code(code: str) -> str:
    code = code.strip()
    if "/" in code:
        code = code.split("/", maxsplit=1)[0]
    return code


def _load_weights_csv(path: str, percent_column: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(path, dtype=str)
    except FileNotFoundError as exc:
        raise WasteDisaggWeightError(f"Weight file not found: {path}") from exc

    required_columns = {"IndustryCode", "CommodityCode", percent_column}
    missing_columns = required_columns.difference(df.columns)
    if missing_columns:
        missing_display = ", ".join(sorted(missing_columns))
        raise WasteDisaggWeightError(
            f"Missing required columns in {path}: {missing_display}"
        )

    df["IndustryCode"] = df["IndustryCode"].map(_normalize_code)
    df["CommodityCode"] = df["CommodityCode"].map(_normalize_code)

    df[percent_column] = pd.to_numeric(df[percent_column], errors="coerce")
    if df[percent_column].isna().any():
        raise WasteDisaggWeightError(
            f"Non-numeric or NaN values in {percent_column} column for file {path}"
        )

    return df


def _apply_correspondence_to_series(
    series: pd.Series,
    mapping: dict[str, list[str]] | None,
    target_codes: list[str],
) -> pd.Series:
    if mapping is None:
        return series

    try:
        correspondence = create_correspondence_matrix(
            mapping,
            domain=list(mapping.keys()),
            range=target_codes,
            is_injective=True,
            is_surjective=True,
            is_complete=True,
        )
    except AssertionError as exc:
        raise WasteDisaggCorrespondenceError(
            "Incomplete or invalid correspondence for waste disaggregation weights"
        ) from exc

    aligned = series.reindex(correspondence.columns).fillna(0.0).astype(float)
    result = correspondence @ aligned
    return result.astype(float)


def _normalize_slice(
    weights: pd.Series,
    waste_sectors: list[str],
    *,
    table: str,
    slice_name: str,
) -> WasteWeightSeries:
    if not isinstance(weights, pd.Series):
        raise WasteDisaggWeightError("weights must be a pandas Series")

    if (weights < 0).any():
        raise WasteDisaggWeightError(
            f"Negative weights encountered for table={table}, slice={slice_name}"
        )

    raw_sum = float(weights.sum())
    if raw_sum <= 0.0:
        raise WasteDisaggWeightError(
            f"All-zero weights for table={table}, slice={slice_name}"
        )

    series = pd.Series(0.0, index=waste_sectors, dtype=float)
    for code, value in weights.items():
        code_str = str(code)
        if code_str in series.index:
            series.loc[code_str] = float(series.loc[code_str]) + float(value)

    series_sum = float(series.sum())
    if series_sum <= 0.0:
        raise WasteDisaggWeightError(
            f"All-zero weights after alignment for table={table}, slice={slice_name}"
        )

    normalized = (series / series_sum).astype(float)
    return normalized


def load_waste_disagg_weights(
    cfg: EEIOWasteDisaggConfig,
    *,
    disagg_original_code: str,
    disagg_new_codes: list[str],
    waste_sectors: list[str],
    naics_to_cornerstone: dict[str, list[str]] | None = None,
) -> WasteDisaggWeights:
    make_df = _load_weights_csv(cfg.make_weights_file, "PercentMake")
    use_df = _load_weights_csv(cfg.use_weights_file, "PercentUsed")

    original = disagg_original_code
    new_codes = set(disagg_new_codes)

    make_intersection_df = make_df[
        make_df["IndustryCode"].isin(new_codes)
        & make_df["CommodityCode"].isin(new_codes)
    ]
    make_col_df = make_df[
        (~make_df["IndustryCode"].isin({original} | new_codes))
        & make_df["CommodityCode"].isin(new_codes)
    ]
    fd_cols: set[str] = set()
    va_rows: set[str] = set()

    use_intersection_df = use_df[
        use_df["IndustryCode"].isin(new_codes) & use_df["CommodityCode"].isin(new_codes)
    ]
    use_col_df = use_df[
        (~use_df["CommodityCode"].isin({original} | new_codes | va_rows))
        & use_df["IndustryCode"].isin(new_codes)
    ]
    use_row_df = use_df[
        use_df["CommodityCode"].isin(new_codes)
        & (~use_df["IndustryCode"].isin(fd_cols | {original} | new_codes))
    ]

    fd_percentages_df = use_df[use_df["IndustryCode"].isin(fd_cols)]
    va_percentages_df = use_df[use_df["CommodityCode"].isin(va_rows)]

    make_intersection_raw = make_intersection_df.groupby("CommodityCode")[
        "PercentMake"
    ].sum()
    make_col_raw = make_col_df.groupby("CommodityCode")["PercentMake"].sum()

    use_intersection_raw = use_intersection_df.groupby("IndustryCode")[
        "PercentUsed"
    ].sum()
    use_col_raw = use_col_df.groupby("IndustryCode")["PercentUsed"].sum()
    use_row_raw = use_row_df.groupby("CommodityCode")["PercentUsed"].sum()

    make_intersection_mapped = _apply_correspondence_to_series(
        make_intersection_raw, naics_to_cornerstone, waste_sectors
    )
    make_col_mapped = _apply_correspondence_to_series(
        make_col_raw, naics_to_cornerstone, waste_sectors
    )
    use_intersection_mapped = _apply_correspondence_to_series(
        use_intersection_raw, naics_to_cornerstone, waste_sectors
    )
    use_col_mapped = _apply_correspondence_to_series(
        use_col_raw, naics_to_cornerstone, waste_sectors
    )
    use_row_mapped = _apply_correspondence_to_series(
        use_row_raw, naics_to_cornerstone, waste_sectors
    )

    if make_intersection_mapped.empty:
        default_series = pd.Series(1.0, index=waste_sectors, dtype=float)
        make_intersection = _normalize_slice(
            default_series,
            waste_sectors,
            table="Make",
            slice_name="intersection_default",
        )
    else:
        make_intersection = _normalize_slice(
            make_intersection_mapped,
            waste_sectors,
            table="Make",
            slice_name="intersection",
        )
    if make_col_mapped.empty:
        make_waste_commodity_columns_all_rows = make_intersection
    else:
        make_waste_commodity_columns_all_rows = _normalize_slice(
            make_col_mapped,
            waste_sectors,
            table="Make",
            slice_name="waste_commodity_columns_all_rows",
        )

    use_intersection = _normalize_slice(
        use_intersection_mapped,
        waste_sectors,
        table="Use",
        slice_name="intersection",
    )
    if use_col_mapped.empty:
        use_waste_industry_columns_all_rows = use_intersection
    else:
        use_waste_industry_columns_all_rows = _normalize_slice(
            use_col_mapped,
            waste_sectors,
            table="Use",
            slice_name="waste_industry_columns_all_rows",
        )
    if use_row_mapped.empty:
        use_waste_commodity_rows_all_columns = use_intersection
    else:
        use_waste_commodity_rows_all_columns = _normalize_slice(
            use_row_mapped,
            waste_sectors,
            table="Use",
            slice_name="waste_commodity_rows_all_columns",
        )

    empty_slice_dict: dict[str, WasteWeightSeries] = {}

    use_fd_columns_for_waste_commodity_rows: dict[str, WasteWeightSeries] = {}
    for fd_col in fd_cols:
        fd_slice = (
            fd_percentages_df[fd_percentages_df["IndustryCode"] == fd_col]
            .groupby("CommodityCode")["PercentUsed"]
            .sum()
        )
        if fd_slice.empty:
            continue
        mapped = _apply_correspondence_to_series(
            fd_slice, naics_to_cornerstone, waste_sectors
        )
        use_fd_columns_for_waste_commodity_rows[fd_col] = _normalize_slice(
            mapped,
            waste_sectors,
            table="Use",
            slice_name=f"fd_column_{fd_col}",
        )

    if not va_percentages_df.empty:
        va_raw = va_percentages_df.groupby("CommodityCode")["PercentUsed"].sum()
        va_mapped = _apply_correspondence_to_series(
            va_raw, naics_to_cornerstone, waste_sectors
        )
        use_va_rows_for_waste_industry_columns = _normalize_slice(
            va_mapped,
            waste_sectors,
            table="Use",
            slice_name="va_rows_for_waste_industry_columns",
        )
    else:
        zero_series = pd.Series(0.0, index=waste_sectors, dtype=float)
        use_va_rows_for_waste_industry_columns = _normalize_slice(
            zero_series + 1.0,
            waste_sectors,
            table="Use",
            slice_name="va_rows_for_waste_industry_columns",
        )

    return WasteDisaggWeights(
        use_intersection=use_intersection,
        use_waste_industry_columns_all_rows=use_waste_industry_columns_all_rows,
        use_waste_commodity_rows_all_columns=use_waste_commodity_rows_all_columns,
        use_waste_rows_specific_columns=empty_slice_dict,
        use_va_rows_for_waste_industry_columns=use_va_rows_for_waste_industry_columns,
        use_fd_columns_for_waste_commodity_rows=use_fd_columns_for_waste_commodity_rows,
        make_intersection=make_intersection,
        make_waste_commodity_columns_all_rows=make_waste_commodity_columns_all_rows,
        make_waste_commodity_columns_specific_rows=empty_slice_dict,
        make_waste_industry_rows_specific_columns=empty_slice_dict,
        year=cfg.year,
        source_name=cfg.source_name,
    )


def weights_to_csv(weights: WasteDisaggWeights, file: IO[str] | None = None) -> None:
    rows: list[dict[str, str | float]] = []

    def add_series(
        series: WasteWeightSeries,
        table: str,
        slice_name: str,
        slice_key: str,
    ) -> None:
        for sector, value in series.items():
            rows.append(
                {
                    "table": table,
                    "slice": slice_name,
                    "slice_key": slice_key,
                    "sector": str(sector),
                    "weight": float(value),
                }
            )

    add_series(weights.use_intersection, "Use", "use_intersection", "")
    add_series(
        weights.use_waste_industry_columns_all_rows,
        "Use",
        "use_waste_industry_columns_all_rows",
        "",
    )
    add_series(
        weights.use_waste_commodity_rows_all_columns,
        "Use",
        "use_waste_commodity_rows_all_columns",
        "",
    )
    for key, series in weights.use_waste_rows_specific_columns.items():
        add_series(series, "Use", "use_waste_rows_specific_columns", key)
    add_series(
        weights.use_va_rows_for_waste_industry_columns,
        "Use",
        "use_va_rows_for_waste_industry_columns",
        "",
    )
    for key, series in weights.use_fd_columns_for_waste_commodity_rows.items():
        add_series(series, "Use", "use_fd_columns_for_waste_commodity_rows", key)

    add_series(weights.make_intersection, "Make", "make_intersection", "")
    add_series(
        weights.make_waste_commodity_columns_all_rows,
        "Make",
        "make_waste_commodity_columns_all_rows",
        "",
    )
    for key, series in weights.make_waste_commodity_columns_specific_rows.items():
        add_series(series, "Make", "make_waste_commodity_columns_specific_rows", key)
    for key, series in weights.make_waste_industry_rows_specific_columns.items():
        add_series(series, "Make", "make_waste_industry_rows_specific_columns", key)

    df = pd.DataFrame(rows, columns=["table", "slice", "slice_key", "sector", "weight"])
    if file is None:
        csv_str = df.to_csv(index=False)
        print(csv_str, end="")
    else:
        df.to_csv(file, index=False)
