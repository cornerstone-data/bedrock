from __future__ import annotations

import pathlib
from dataclasses import dataclass
from typing import IO, TYPE_CHECKING, cast

import pandas as pd

from bedrock.utils.taxonomy.correspondence import create_correspondence_matrix

if TYPE_CHECKING:
    from bedrock.utils.config.usa_config import EEIOWasteDisaggConfig

    WasteWeightSeries = pd.Series[float]
    WasteWeightTable = pd.DataFrame
else:
    WasteWeightSeries = pd.Series
    WasteWeightTable = pd.DataFrame


class WasteDisaggWeightError(Exception):
    pass


class WasteDisaggCorrespondenceError(Exception):
    pass


@dataclass
class WasteDisaggWeights:
    """Weights for waste disaggregation; all slices are 2D (industry × commodity) where applicable.

    Each slice is a WasteWeightTable (pd.DataFrame) with index and columns encoding
    (industry, commodity) or (row_dim, col_dim) so that lookup is by both dimensions.
    - Intersection slices: index=industry_subsectors, columns=commodity_subsectors (table sum = 1).
    - Row/column slices: one dimension = context (industry or commodity), other = subsectors; rows sum to 1.
    """

    use_intersection: (
        WasteWeightTable  # index=industry_subsectors, columns=commodity_subsectors
    )
    use_waste_industry_columns_all_rows: (
        WasteWeightTable  # index=commodity, columns=industry_subsectors
    )
    use_waste_commodity_rows_all_columns: (
        WasteWeightTable  # index=industry, columns=commodity_subsectors
    )
    use_waste_rows_specific_columns: dict[str, WasteWeightTable]
    use_va_rows_for_waste_industry_columns: (
        WasteWeightTable  # index=va_row, columns=industry_subsectors
    )
    use_fd_columns_for_waste_commodity_rows: dict[
        str, WasteWeightTable
    ]  # per fd_col: row=fd_col, cols=commodity_subsectors
    make_intersection: (
        WasteWeightTable  # index=industry_subsectors, columns=commodity_subsectors
    )
    make_waste_commodity_columns_all_rows: (
        WasteWeightTable  # index=industry, columns=commodity_subsectors
    )
    make_waste_commodity_columns_specific_rows: dict[str, WasteWeightTable]
    make_waste_industry_rows_specific_columns: dict[str, WasteWeightTable]
    year: int
    source_name: str


def _normalize_code(code: str) -> str:
    code = code.strip()
    if "/" in code:
        code = code.split("/", maxsplit=1)[0]
    return code


def load_weights_csv(path: str, percent_column: str) -> pd.DataFrame:
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


def _pivot_and_align(
    df: pd.DataFrame,
    value_col: str,
    index_col: str,
    columns_col: str,
    index_codes: list[str],
    column_codes: list[str],
) -> pd.DataFrame:
    """Pivot to (index_col x columns_col), reindex to given codes, fill 0."""
    if df.empty:
        return pd.DataFrame(0.0, index=index_codes, columns=column_codes, dtype=float)
    pivoted = df.pivot_table(
        index=index_col, columns=columns_col, values=value_col, aggfunc="sum"
    )
    return pivoted.reindex(
        index=index_codes, columns=column_codes, fill_value=0.0
    ).astype(float)


def _normalize_table(
    df: pd.DataFrame,
    *,
    table: str,
    slice_name: str,
    index_codes: list[str] | None = None,
    column_codes: list[str] | None = None,
    axis: int | None = 1,
) -> WasteWeightTable:
    """Normalize a weight table. axis=1: each row sums to 1; axis=None: whole table sums to 1.
    If index_codes/column_codes are given, reindex to those; otherwise keep existing index/columns.
    """
    if not isinstance(df, pd.DataFrame):
        raise WasteDisaggWeightError("weights must be a pandas DataFrame")

    if (df < 0).any().any():
        raise WasteDisaggWeightError(
            f"Negative weights encountered for table={table}, slice={slice_name}"
        )

    out = df.copy()
    if index_codes is not None:
        out = out.reindex(index=index_codes, fill_value=0.0)
    if column_codes is not None:
        out = out.reindex(columns=column_codes, fill_value=0.0)
    out = out.astype(float)

    if axis is None:
        total = float(out.sum().sum())
        if total <= 0.0:
            raise WasteDisaggWeightError(
                f"All-zero weights for table={table}, slice={slice_name}"
            )
        return (out / total).astype(float)
    total = out.sum(axis=axis)
    if (total <= 0).any():
        raise WasteDisaggWeightError(
            f"All-zero weights for table={table}, slice={slice_name}"
        )
    if axis == 1:
        return (out.div(total, axis=0)).astype(float)
    return (out.div(total, axis=1)).astype(float)


def load_waste_disagg_weights(
    cfg: EEIOWasteDisaggConfig,
    *,
    disagg_original_code: str,
    disagg_new_codes: list[str],
    waste_sectors: list[str],
    naics_to_cornerstone: dict[str, list[str]] | None = None,
) -> WasteDisaggWeights:
    # naics_to_cornerstone reserved for future index/column correspondence mapping
    _ = naics_to_cornerstone
    make_df = load_weights_csv(cfg.make_weights_file, "PercentMake")
    use_df = load_weights_csv(cfg.use_weights_file, "PercentUsed")

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
    make_row_df = make_df[
        make_df["IndustryCode"].isin(new_codes)
        & (~make_df["CommodityCode"].isin({original} | new_codes))
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

    # --- Make intersection: (industry_subsectors x commodity_subsectors), table sum = 1
    make_intersection_piv = _pivot_and_align(
        make_intersection_df,
        "PercentMake",
        "IndustryCode",
        "CommodityCode",
        waste_sectors,
        waste_sectors,
    )
    if make_intersection_piv.sum().sum() <= 0:
        make_intersection_piv = pd.DataFrame(
            1.0 / (len(waste_sectors) ** 2),
            index=waste_sectors,
            columns=waste_sectors,
            dtype=float,
        )
    make_intersection = _normalize_table(
        make_intersection_piv,
        table="Make",
        slice_name="intersection",
        index_codes=waste_sectors,
        column_codes=waste_sectors,
        axis=None,
    )

    # --- Make commodity columns all rows: (industry x commodity_subsectors), each row sum = 1
    make_col_piv = _pivot_and_align(
        make_col_df,
        "PercentMake",
        "IndustryCode",
        "CommodityCode",
        make_col_df["IndustryCode"].unique().tolist() if not make_col_df.empty else [],
        waste_sectors,
    )
    if make_col_piv.empty or make_col_piv.sum(axis=1).sum() <= 0:
        default_row = make_intersection.sum(axis=0)
        make_waste_commodity_columns_all_rows = pd.DataFrame(
            [default_row.values] * 1,
            index=["__default__"],
            columns=waste_sectors,
            dtype=float,
        )
    else:
        make_waste_commodity_columns_all_rows = _normalize_table(
            make_col_piv,
            table="Make",
            slice_name="waste_commodity_columns_all_rows",
            column_codes=waste_sectors,
            axis=1,
        )

    # --- Make industry rows specific columns: per commodity_col -> (industry_subsectors)
    make_waste_industry_rows_specific_columns: dict[str, WasteWeightTable] = {}
    if not make_row_df.empty:
        for com, grp in make_row_df.groupby("CommodityCode"):
            ser = grp.set_index("IndustryCode")["PercentMake"]
            ser = ser.reindex(waste_sectors, fill_value=0.0).astype(float)
            if ser.sum() <= 0:
                continue
            tbl = (ser / ser.sum()).to_frame().T
            make_waste_industry_rows_specific_columns[str(com)] = tbl

    # --- Use intersection: (industry_subsectors x commodity_subsectors), table sum = 1
    use_intersection_piv = _pivot_and_align(
        use_intersection_df,
        "PercentUsed",
        "IndustryCode",
        "CommodityCode",
        waste_sectors,
        waste_sectors,
    )
    if use_intersection_piv.sum().sum() <= 0:
        raise WasteDisaggWeightError(
            "All-zero weights for table=Use, slice=intersection"
        )
    use_intersection = _normalize_table(
        use_intersection_piv,
        table="Use",
        slice_name="intersection",
        index_codes=waste_sectors,
        column_codes=waste_sectors,
        axis=None,
    )

    # --- Use industry columns all rows: (commodity x industry_subsectors), each row sum = 1
    use_col_piv = _pivot_and_align(
        use_col_df,
        "PercentUsed",
        "CommodityCode",
        "IndustryCode",
        use_col_df["CommodityCode"].unique().tolist() if not use_col_df.empty else [],
        waste_sectors,
    )
    if use_col_piv.empty or use_col_piv.sum(axis=1).sum() <= 0:
        default_row = use_intersection.sum(axis=1)
        use_waste_industry_columns_all_rows = pd.DataFrame(
            [default_row.values] * 1,
            index=["__default__"],
            columns=waste_sectors,
            dtype=float,
        )
    else:
        use_waste_industry_columns_all_rows = _normalize_table(
            use_col_piv,
            table="Use",
            slice_name="waste_industry_columns_all_rows",
            column_codes=waste_sectors,
            axis=1,
        )

    # --- Use commodity rows all columns: (industry x commodity_subsectors), each row sum = 1
    use_row_piv = _pivot_and_align(
        use_row_df,
        "PercentUsed",
        "IndustryCode",
        "CommodityCode",
        use_row_df["IndustryCode"].unique().tolist() if not use_row_df.empty else [],
        waste_sectors,
    )
    if use_row_piv.empty or use_row_piv.sum(axis=1).sum() <= 0:
        default_row = use_intersection.sum(axis=0)
        use_waste_commodity_rows_all_columns = pd.DataFrame(
            [default_row.values] * 1,
            index=["__default__"],
            columns=waste_sectors,
            dtype=float,
        )
    else:
        use_waste_commodity_rows_all_columns = _normalize_table(
            use_row_piv,
            table="Use",
            slice_name="waste_commodity_rows_all_columns",
            column_codes=waste_sectors,
            axis=1,
        )

    use_waste_rows_specific_columns: dict[str, WasteWeightTable] = {}

    # --- FD columns: per fd_col -> (fd_col row x commodity_subsectors)
    use_fd_columns_for_waste_commodity_rows: dict[str, WasteWeightTable] = {}
    for fd_col in fd_cols:
        fd_slice = fd_percentages_df[fd_percentages_df["IndustryCode"] == fd_col]
        if fd_slice.empty:
            continue
        ser = fd_slice.set_index("CommodityCode")["PercentUsed"]
        ser = ser.reindex(waste_sectors, fill_value=0.0).astype(float)
        if ser.sum() <= 0:
            continue
        tbl = (ser / ser.sum()).to_frame().T
        tbl.index = [fd_col]
        use_fd_columns_for_waste_commodity_rows[fd_col] = tbl

    # --- VA rows: (va_row x industry_subsectors), each row sum = 1
    if not va_percentages_df.empty:
        va_piv = _pivot_and_align(
            va_percentages_df,
            "PercentUsed",
            "CommodityCode",
            "IndustryCode",
            va_percentages_df["CommodityCode"].unique().tolist(),
            waste_sectors,
        )
        use_va_rows_for_waste_industry_columns = _normalize_table(
            va_piv,
            table="Use",
            slice_name="va_rows_for_waste_industry_columns",
            column_codes=waste_sectors,
            axis=1,
        )
    else:
        use_va_rows_for_waste_industry_columns = pd.DataFrame(
            [[1.0 / len(waste_sectors)] * len(waste_sectors)] * 1,
            index=["__default__"],
            columns=waste_sectors,
            dtype=float,
        )

    make_waste_commodity_columns_specific_rows: dict[str, WasteWeightTable] = {}

    return WasteDisaggWeights(
        use_intersection=use_intersection,
        use_waste_industry_columns_all_rows=use_waste_industry_columns_all_rows,
        use_waste_commodity_rows_all_columns=use_waste_commodity_rows_all_columns,
        use_waste_rows_specific_columns=use_waste_rows_specific_columns,
        use_va_rows_for_waste_industry_columns=use_va_rows_for_waste_industry_columns,
        use_fd_columns_for_waste_commodity_rows=use_fd_columns_for_waste_commodity_rows,
        make_intersection=make_intersection,
        make_waste_commodity_columns_all_rows=make_waste_commodity_columns_all_rows,
        make_waste_commodity_columns_specific_rows=make_waste_commodity_columns_specific_rows,
        make_waste_industry_rows_specific_columns=make_waste_industry_rows_specific_columns,
        year=cfg.year,
        source_name=cfg.source_name,
    )


def weights_to_csv(weights: WasteDisaggWeights, file: IO[str] | None = None) -> None:
    rows: list[dict[str, str | float]] = []

    def add_table(
        tbl: WasteWeightTable,
        table: str,
        slice_name: str,
        slice_key: str,
    ) -> None:
        for ind in tbl.index:
            for col in tbl.columns:
                val = tbl.loc[ind, col]
                if val != 0.0:
                    rows.append(
                        {
                            "table": table,
                            "slice": slice_name,
                            "slice_key": slice_key,
                            "industry": str(ind),
                            "commodity": str(col),
                            "weight": float(val),
                        }
                    )

    add_table(weights.use_intersection, "Use", "use_intersection", "")
    add_table(
        weights.use_waste_industry_columns_all_rows,
        "Use",
        "use_waste_industry_columns_all_rows",
        "",
    )
    add_table(
        weights.use_waste_commodity_rows_all_columns,
        "Use",
        "use_waste_commodity_rows_all_columns",
        "",
    )
    for key, tbl in weights.use_waste_rows_specific_columns.items():
        add_table(tbl, "Use", "use_waste_rows_specific_columns", key)
    add_table(
        weights.use_va_rows_for_waste_industry_columns,
        "Use",
        "use_va_rows_for_waste_industry_columns",
        "",
    )
    for key, tbl in weights.use_fd_columns_for_waste_commodity_rows.items():
        add_table(tbl, "Use", "use_fd_columns_for_waste_commodity_rows", key)

    add_table(weights.make_intersection, "Make", "make_intersection", "")
    add_table(
        weights.make_waste_commodity_columns_all_rows,
        "Make",
        "make_waste_commodity_columns_all_rows",
        "",
    )
    for key, tbl in weights.make_waste_commodity_columns_specific_rows.items():
        add_table(tbl, "Make", "make_waste_commodity_columns_specific_rows", key)
    for key, tbl in weights.make_waste_industry_rows_specific_columns.items():
        add_table(tbl, "Make", "make_waste_industry_rows_specific_columns", key)

    df = pd.DataFrame(
        rows,
        columns=["table", "slice", "slice_key", "industry", "commodity", "weight"],
    )
    if file is None:
        csv_str = df.to_csv(index=False)
        print(csv_str, end="")
    else:
        df.to_csv(file, index=False)


# %%
if __name__ == "__main__":
    from bedrock.utils.config.usa_config import EEIOWasteDisaggConfig
    from bedrock.utils.taxonomy.cornerstone.commodities import WASTE_DISAGG_COMMODITIES

    data_dir = pathlib.Path(__file__).resolve().parent
    use_path = data_dir / "WasteDisaggregationDetail2017_Use.csv"
    make_path = data_dir / "WasteDisaggregationDetail2017_Make.csv"

    cfg = EEIOWasteDisaggConfig(
        use_weights_file=str(use_path),
        make_weights_file=str(make_path),
        year=2017,
        source_name="WasteDisaggregationDetail2017",
    )
    weights = load_waste_disagg_weights(
        cfg,
        disagg_original_code="562000",
        disagg_new_codes=cast(list[str], list(WASTE_DISAGG_COMMODITIES["562000"])),
        waste_sectors=cast(list[str], list(WASTE_DISAGG_COMMODITIES["562000"])),
        naics_to_cornerstone=None,
    )
    weights_to_csv(weights, 'weight.csv')
