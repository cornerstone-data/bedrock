import os
import posixpath

import pandas as pd

from ceda_usa.utils.gcp import GCS_CEDA_INPUT_DIR, load_from_gcs

GCS_MECS_DIR = posixpath.join(GCS_CEDA_INPUT_DIR, "EIA_MECS_2018")
IN_DIR = os.path.join(os.path.dirname(__file__), "..", "input_data")


def load_mecs_2_1() -> pd.DataFrame:
    """
    non-fuel consumption by industry in energy unit such as Btu, kWh, etc.
    """

    tbl_2_1 = load_from_gcs(
        posixpath.join(GCS_MECS_DIR, "Table2_1.xlsx"),
        local_dir=IN_DIR,
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


def load_mecs_3_1() -> pd.DataFrame:
    """
    fuel consumption by industry in energy unit such as Btu, kWh, etc.
    """
    tbl_3_1 = load_from_gcs(
        posixpath.join(GCS_MECS_DIR, "Table3_1.xlsx"),
        local_dir=IN_DIR,
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
