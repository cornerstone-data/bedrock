import os
import posixpath

import pandas as pd

from bedrock.extract.iot.constants import GCS_GDP_DETAIL_TABLES, GCS_GDP_DIR
from bedrock.utils.io.gcp import download_gcs_file_if_not_exists

# NOTE: this is the data version used by the BEA Data Archive (https://apps.bea.gov/histdatacore/histChildLevels.html?HMI=8&oldDiv=Industry%20Accounts)
# where "YEAR, Q2" is the major release every year that includes annual update of the Detail tables
BEA_DATA_VERSION = "2025Q2"
SECTOR_NAME_COL = "sector_name"

SUMMARY_LINE_NUMBER_COL = "summary_line_no"
SECTOR_SUMMARY_CODE_COL = "sector_summary_code"


IN_DIR = os.path.join(os.path.dirname(__file__), "input_data")
OUT_DIR = os.path.join(os.path.dirname(__file__), "output_data")


def load_pi_summary_annual() -> pd.DataFrame:
    """
    Download (if needed) and load the annual BEA summary gross output table,
    add 1-indexed summary line numbers, and index rows by those line numbers.
    """

    _download_summary_table()
    df = _load_from_excel(
        fname=os.path.join(IN_DIR, f"{BEA_DATA_VERSION}_SummaryGrossOutput.xlsx"),
        sheet_name="TGO104-A",
    )

    df[SUMMARY_LINE_NUMBER_COL] = range(1, df.shape[0] + 1)  # 1-indexed
    df.index = pd.Index("LINE_NUMBER_" + df[SUMMARY_LINE_NUMBER_COL].astype(str))
    # NOTE: sector name can be not unique, because the original data has hierarchical structure
    return df


def load_pi_summary_quarterly() -> pd.DataFrame:
    """
    Download (if needed) and load the quarterly BEA summary gross output table,
    add 1-indexed summary line numbers, and index rows by those line numbers.
    """
    _download_summary_table()
    df = _load_from_excel(
        fname=os.path.join(IN_DIR, f"{BEA_DATA_VERSION}_SummaryGrossOutput.xlsx"),
        sheet_name="TGO104-Q",
    )

    df[SUMMARY_LINE_NUMBER_COL] = range(1, df.shape[0] + 1)  # 1-indexed
    df.index = pd.Index("LINE_NUMBER_" + df[SUMMARY_LINE_NUMBER_COL].astype(str))
    # NOTE: sector name can be not unique, because the original data has hierarchical structure
    return df


def _download_summary_table() -> None:
    """
    Ensure the summary gross output Excel workbook for the configured BEA
    version exists locally by downloading it from GCS if necessary.
    """
    fname = "GrossOutput.xlsx"
    download_gcs_file_if_not_exists(
        name=fname,
        sub_bucket=posixpath.join(GCS_GDP_DIR, f"GdpByInd_{BEA_DATA_VERSION}"),
        pth=os.path.join(IN_DIR, f"{BEA_DATA_VERSION}_Summary{fname}"),
    )


def load_pi_detail() -> pd.DataFrame:
    """
    Load the detail-level BEA price index table (UGO304-A) for the configured
    BEA data vintage from the local Excel workbook.
    """
    return _load_detail_table("UGO304-A")


def load_go_detail() -> pd.DataFrame:
    """
    Load the detail-level BEA gross output table (UGO305-A) for the configured
    BEA data vintage from the local Excel workbook.
    """
    return _load_detail_table("UGO305-A")


def _load_detail_table(sheet_name: GCS_GDP_DETAIL_TABLES) -> pd.DataFrame:
    """
    Download (if needed) and load a detail-level BEA price index or gross output
    table by sheet name, asserting that sector names remain unique.
    """
    _download_detail_table()
    df = _load_from_excel(
        fname=os.path.join(IN_DIR, f"{BEA_DATA_VERSION}_DetailGrossOutput.xlsx"),
        sheet_name=sheet_name,
    )

    assert df[SECTOR_NAME_COL].is_unique, "expected sector name to be unique"
    return df


def _download_detail_table() -> None:
    """
    Ensure the detail gross output Excel workbook for the configured BEA
    version exists locally by downloading it from GCS if necessary.
    """
    fname = "GrossOutput.xlsx"
    download_gcs_file_if_not_exists(
        name=fname,
        sub_bucket=posixpath.join(GCS_GDP_DIR, f"UGdpByInd_{BEA_DATA_VERSION}"),
        pth=os.path.join(IN_DIR, f"{BEA_DATA_VERSION}_Detail{fname}"),
    )


def _load_from_excel(fname: str, sheet_name: str) -> pd.DataFrame:
    """
    Read a BEA Excel worksheet, skip the header rows, normalize column names,
    drop unused columns, and return a cleaned DataFrame without NA rows.
    """
    return (
        pd.read_excel(
            fname,
            sheet_name=sheet_name,
            skiprows=7,
        )
        .rename(columns={"Unnamed: 1": SECTOR_NAME_COL})
        .drop(columns=["Line", "Unnamed: 2"])
        .dropna()
    )
