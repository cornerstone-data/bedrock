from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, cast

import pandas as pd

from bedrock.extract.disaggregation.waste_weights import WasteDisaggWeights

VectorToDisagg = Literal[
    "MakeRow",
    "MakeCol",
    "MakeIntersection",
    "UseRow",
    "UseCol",
    "UseIntersection",
    "FinalDemand",
    "ValueAdded",
]


@dataclass
class DisaggSpec:
    original_sector_code: str
    new_sector_codes: list[str]


def get_default_allocation_percentages(
    file_df: pd.DataFrame,
    spec: DisaggSpec,
    num_new_sectors: int,
    output: Literal["Commodity", "Industry"],
) -> pd.DataFrame:
    if output == "Industry":
        default_percentages = file_df[
            file_df["CommodityCode"] == spec.original_sector_code
        ]
        default_percentages = default_percentages.set_index("IndustryCode")
        default_percentages = default_percentages.reindex(spec.new_sector_codes)
    else:
        default_percentages = file_df[
            file_df["IndustryCode"] == spec.original_sector_code
        ]
        default_percentages = default_percentages.set_index("CommodityCode")
        default_percentages = default_percentages.reindex(spec.new_sector_codes)

    if default_percentages.empty or "Percent" not in default_percentages.columns:
        return pd.DataFrame(
            {"Percent": [1.0 / float(num_new_sectors)] * num_new_sectors}
        )

    return default_percentages[["Percent"]]


def create_blank_intersection(new_sector_codes: list[str]) -> pd.DataFrame:
    intersection = pd.DataFrame(
        data=float("nan"),
        index=new_sector_codes,
        columns=new_sector_codes,
    )
    return intersection


def calculate_default_intersection(
    original_intersection: pd.DataFrame,
    default_percentages: pd.DataFrame,
    new_sector_codes: list[str],
) -> pd.DataFrame:
    num_new_sectors = len(new_sector_codes)
    base_raw: Any = original_intersection.iat[0, 0]
    base_value = float(cast(float, base_raw))
    perc = default_percentages["Percent"].to_list()
    if len(perc) != num_new_sectors:
        raise ValueError("default_percentages length does not match new_sector_codes")
    diag_values = [base_value * p for p in perc]
    intersection = pd.DataFrame(
        data=[[0.0] * num_new_sectors for _ in range(num_new_sectors)],
        index=new_sector_codes,
        columns=new_sector_codes,
    )
    for i, value in enumerate(diag_values):
        intersection.iat[i, i] = value
    return intersection


def apply_allocation(
    spec: DisaggSpec,
    alloc_percentages: pd.DataFrame,
    vector_to_disagg: VectorToDisagg,
    original_table: pd.DataFrame,
    file_df: pd.DataFrame,
    weights: WasteDisaggWeights | None = None,
) -> pd.DataFrame:
    new_sector_codes = spec.new_sector_codes
    num_new_sectors = len(new_sector_codes)
    original_sector_code = spec.original_sector_code

    if vector_to_disagg == "MakeRow":
        original_vector = original_table.loc[original_sector_code, :]

        manual_alloc_vector = pd.DataFrame(
            data=float("nan"),
            index=new_sector_codes,
            columns=original_table.columns,
        )

        alloc_row_index = "IndustryCode"
        alloc_col_index = "CommodityCode"

        default_percentages = get_default_allocation_percentages(
            file_df.assign(Percent=file_df.iloc[:, 2]),
            spec,
            num_new_sectors,
            output="Commodity",
        )

        row_values = original_vector.to_numpy(dtype=float).tolist()
        data = [row_values for _ in range(num_new_sectors)]
        default_alloc_vector = pd.DataFrame(
            data=data,
            index=pd.Index(new_sector_codes),
            columns=original_table.columns,
        )
        default_alloc_vector = default_alloc_vector.mul(
            default_percentages["Percent"].to_numpy(dtype=float),
            axis=0,
        )

    elif vector_to_disagg == "MakeCol":
        original_vector = original_table.loc[:, original_sector_code]

        manual_alloc_vector = pd.DataFrame(
            data=float("nan"),
            index=original_table.index,
            columns=new_sector_codes,
        )

        alloc_row_index = "IndustryCode"
        alloc_col_index = "CommodityCode"

        default_percentages = get_default_allocation_percentages(
            file_df.assign(Percent=file_df.iloc[:, 2]),
            spec,
            num_new_sectors,
            output="Commodity",
        )

        col_values = original_vector.to_numpy(dtype=float)
        default_alloc_vector = pd.DataFrame(
            data={code: col_values for code in new_sector_codes},
            index=original_table.index,
        )
        default_alloc_vector = default_alloc_vector.mul(
            default_percentages["Percent"].to_numpy(dtype=float),
            axis=1,
        )

    elif vector_to_disagg == "MakeIntersection":
        intersection = original_table.loc[
            [original_sector_code], [original_sector_code]
        ]

        default_percentages = get_default_allocation_percentages(
            file_df.assign(Percent=file_df.iloc[:, 2]),
            spec,
            num_new_sectors,
            output="Commodity",
        )

        default_alloc_vector = calculate_default_intersection(
            intersection, default_percentages, new_sector_codes
        )
        manual_alloc_vector = create_blank_intersection(new_sector_codes)

        alloc_row_index = "IndustryCode"
        alloc_col_index = "CommodityCode"

    elif vector_to_disagg in ("UseRow", "FinalDemand"):
        original_vector = original_table.loc[original_sector_code, :]

        manual_alloc_vector = pd.DataFrame(
            data=float("nan"),
            index=new_sector_codes,
            columns=original_table.columns,
        )

        alloc_row_index = "CommodityCode"
        alloc_col_index = "IndustryCode"

        default_percentages = get_default_allocation_percentages(
            file_df.assign(Percent=file_df.iloc[:, 2]),
            spec,
            num_new_sectors,
            output="Commodity",
        )

        row_values = original_vector.to_numpy(dtype=float).tolist()
        data = [row_values for _ in range(num_new_sectors)]
        default_alloc_vector = pd.DataFrame(
            data=data,
            index=pd.Index(new_sector_codes),
            columns=original_table.columns,
        )
        default_alloc_vector = default_alloc_vector.mul(
            default_percentages["Percent"].to_numpy(dtype=float),
            axis=0,
        )

    elif vector_to_disagg in ("UseCol", "ValueAdded"):
        original_vector = original_table.loc[:, original_sector_code]

        manual_alloc_vector = pd.DataFrame(
            data=float("nan"),
            index=original_table.index,
            columns=new_sector_codes,
        )

        alloc_row_index = "CommodityCode"
        alloc_col_index = "IndustryCode"

        default_percentages = get_default_allocation_percentages(
            file_df.assign(Percent=file_df.iloc[:, 2]),
            spec,
            num_new_sectors,
            output="Industry",
        )

        col_values = original_vector.to_numpy(dtype=float)
        default_alloc_vector = pd.DataFrame(
            data={code: col_values for code in new_sector_codes},
            index=original_table.index,
        )
        default_alloc_vector = default_alloc_vector.mul(
            default_percentages["Percent"].to_numpy(dtype=float),
            axis=1,
        )

    elif vector_to_disagg == "UseIntersection":
        intersection = original_table.loc[
            [original_sector_code], [original_sector_code]
        ]

        default_percentages = get_default_allocation_percentages(
            file_df.assign(Percent=file_df.iloc[:, 2]),
            spec,
            num_new_sectors,
            output="Industry",
        )

        default_alloc_vector = calculate_default_intersection(
            intersection, default_percentages, new_sector_codes
        )
        manual_alloc_vector = create_blank_intersection(new_sector_codes)

        alloc_row_index = "IndustryCode"
        alloc_col_index = "CommodityCode"

    else:
        raise ValueError(f"Unsupported vectorToDisagg: {vector_to_disagg}")

    if not alloc_percentages.empty:
        for _, row in alloc_percentages.iterrows():
            row_alloc = str(row[alloc_row_index])
            col_alloc = str(row[alloc_col_index])
            raw_alloc: Any = row.iloc[2]
            allocation_value = float(cast(float, raw_alloc))

            if (
                row_alloc not in manual_alloc_vector.index
                or col_alloc not in manual_alloc_vector.columns
            ):
                continue

            if vector_to_disagg in ("MakeRow", "UseRow", "FinalDemand"):
                value = original_vector[col_alloc] * allocation_value
                manual_alloc_vector.loc[row_alloc, col_alloc] = float(value)
            elif vector_to_disagg in ("MakeCol", "UseCol", "ValueAdded"):
                value = original_vector.loc[row_alloc] * allocation_value
                manual_alloc_vector.loc[row_alloc, col_alloc] = float(value)
            elif vector_to_disagg in ("MakeIntersection", "UseIntersection"):
                base_value = float(intersection.iat[0, 0])
                value = base_value * allocation_value
                manual_alloc_vector.loc[row_alloc, col_alloc] = float(value)

    manual_alloc_vector = manual_alloc_vector.fillna(0.0)

    if vector_to_disagg in (
        "MakeRow",
        "MakeIntersection",
        "UseRow",
        "UseIntersection",
        "FinalDemand",
    ):
        manual_indices = manual_alloc_vector.columns[
            manual_alloc_vector.sum(axis=0) != 0.0
        ]
        for col in manual_indices:
            default_alloc_vector.loc[:, col] = manual_alloc_vector.loc[:, col]
    else:
        manual_indices = manual_alloc_vector.index[
            manual_alloc_vector.sum(axis=1) != 0.0
        ]
        for idx in manual_indices:
            default_alloc_vector.loc[idx, :] = manual_alloc_vector.loc[idx, :]

    if weights is not None and vector_to_disagg in (
        "UseRow",
        "UseCol",
        "UseIntersection",
        "FinalDemand",
        "ValueAdded",
        "MakeRow",
        "MakeCol",
        "MakeIntersection",
    ):
        _ = weights  # placeholder; integration with WasteDisaggWeights is caller-defined

    return default_alloc_vector
