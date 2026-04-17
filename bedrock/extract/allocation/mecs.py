import functools

import pandas as pd

from bedrock.utils.io.local_extract_input_data import local_extract_input_dir
from bedrock.utils.io.gcp import load_from_gcs
from bedrock.utils.io.gcp_paths import gcs_extract_input_path

# MECS allocation tables (see EIA_MECS_Energy.yaml); vintage must match xlsx layout below.
_MECS_ALLOCATION_YEAR = 2018

GCS_MECS_DIR = gcs_extract_input_path("EIA_MECS_Energy", _MECS_ALLOCATION_YEAR)
_LOCAL_MECS_DIR = local_extract_input_dir("EIA_MECS_Energy", _MECS_ALLOCATION_YEAR)


@functools.cache
def load_mecs_2_1() -> pd.DataFrame:
    """
    non-fuel consumption by industry in energy unit such as Btu, kWh, etc.
    """

    tbl_2_1 = load_from_gcs(
        name="Table2_1.xlsx",
        sub_bucket=GCS_MECS_DIR,
        local_dir=_LOCAL_MECS_DIR,
        loader=lambda pth: pd.read_excel(
            pth,
            sheet_name="Table 2.1",
            index_col=[0],
            skiprows=13,
            nrows=83,  # import all NAICS rows and Subtotal for allocation purpose
            header=None,
            usecols="A,C:J",
        ),
    )
    tbl_2_1.replace(["*", "W", "Q", "D"], 0, inplace=True)
    tbl_2_1 = tbl_2_1.astype(float)

    idx_lst = tbl_2_1.index.astype(str).to_list()
    idx_lst[-1] = (
        "Total"  # Here we use Subtotal as Total because it is the total of all NAICS codes
    )
    tbl_2_1.index = pd.Index(idx_lst)

    tbl_2_1.columns = pd.Index(
        [
            "Total",
            "Residual Fuel Oil",
            "Distillate Fuel Oil(b)",
            "Natural Gas(c)",
            "HGL (excluding natural gasoline)(d)",
            "Coal",
            "Coke and Breeze",
            "Other(e)",
        ]
    )
    return tbl_2_1


@functools.cache
def load_mecs_3_1() -> pd.DataFrame:
    """
    fuel consumption by industry in energy unit such as Btu, kWh, etc.
    """
    tbl_3_1 = load_from_gcs(
        name="Table3_1.xlsx",
        sub_bucket=GCS_MECS_DIR,
        local_dir=_LOCAL_MECS_DIR,
        loader=lambda pth: pd.read_excel(
            pth,
            sheet_name="Table 3.1",
            index_col=[0],
            skiprows=13,
            nrows=83,
            header=None,
            usecols="A,C:K",
        ),
    )
    tbl_3_1.replace(["*", "W", "Q", "D"], 0.0, inplace=True)
    tbl_3_1 = tbl_3_1.astype(float)

    idx_lst = tbl_3_1.index.astype(str).to_list()
    idx_lst[-1] = "Total"
    tbl_3_1.index = pd.Index(idx_lst)
    tbl_3_1.columns = pd.Index(
        [
            "Total",
            "Net Electricity(b)",
            "Residual Fuel Oil",
            "Distillate Fuel Oil(b)",
            "Natural Gas(d)",
            "HGL (excluding natural gasoline)(e)",
            "Coal",
            "Coke and Breeze",
            "Other(f)",
        ]
    )
    return tbl_3_1
