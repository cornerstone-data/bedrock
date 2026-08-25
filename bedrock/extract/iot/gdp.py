import os
import posixpath
import typing as ta

import numpy as np
import pandas as pd

from bedrock.extract.iot.constants import GCS_GDP_DETAIL_TABLES, GCS_GDP_DIR
from bedrock.utils.io.gcp import download_gcs_file_if_not_exists
from bedrock.utils.io.gcp_paths import gcs_extract_input_path
from bedrock.utils.io.local_extract_input_data import local_dir_for_gcs_sub_bucket

# NOTE: this is the data version used by the BEA Data Archive (https://apps.bea.gov/histdatacore/histChildLevels.html?HMI=8&oldDiv=Industry%20Accounts)
# where "YEAR, Q2" is the major release every year that includes annual update of the Detail tables
BEA_DATA_VERSION = "2025Q2"
SECTOR_NAME_COL = "sector_name"

SUMMARY_LINE_NUMBER_COL = "summary_line_no"
SECTOR_SUMMARY_CODE_COL = "sector_summary_code"


OUT_DIR = os.path.join(os.path.dirname(__file__), "output_data")

_LOCAL_GDP_SUMMARY_DIR = local_dir_for_gcs_sub_bucket(
    posixpath.join(
        gcs_extract_input_path("BEA_Detail_GrossOutput_IO", BEA_DATA_VERSION),
        f"GdpByInd_{BEA_DATA_VERSION}",
    )
)
_LOCAL_GDP_DETAIL_DIR = local_dir_for_gcs_sub_bucket(
    posixpath.join(
        gcs_extract_input_path("BEA_Detail_GrossOutput_IO", BEA_DATA_VERSION),
        f"UGdpByInd_{BEA_DATA_VERSION}",
    )
)


def load_pi_summary_annual() -> pd.DataFrame:
    """
    Download (if needed) and load the annual BEA summary gross output table,
    add 1-indexed summary line numbers, and index rows by those line numbers.
    """

    _download_summary_table()
    df = _load_from_excel(
        fname=os.path.join(
            _LOCAL_GDP_SUMMARY_DIR, f"{BEA_DATA_VERSION}_SummaryGrossOutput.xlsx"
        ),
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
        fname=os.path.join(
            _LOCAL_GDP_SUMMARY_DIR, f"{BEA_DATA_VERSION}_SummaryGrossOutput.xlsx"
        ),
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
        sub_bucket=posixpath.join(
            GCS_GDP_DIR,
            f"GdpByInd_{BEA_DATA_VERSION}",
        ),
        pth=os.path.join(_LOCAL_GDP_SUMMARY_DIR, f"{BEA_DATA_VERSION}_Summary{fname}"),
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
    Unit is million USD
    """
    return _load_detail_table("UGO305-A")


def _load_detail_table(sheet_name: GCS_GDP_DETAIL_TABLES) -> pd.DataFrame:
    """
    Download (if needed) and load a detail-level BEA price index or gross output
    table by sheet name, asserting that sector names remain unique.
    """
    _download_detail_table()
    df = _load_from_excel(
        fname=os.path.join(
            _LOCAL_GDP_DETAIL_DIR, f"{BEA_DATA_VERSION}_DetailGrossOutput.xlsx"
        ),
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
        sub_bucket=posixpath.join(
            GCS_GDP_DIR,
            f"UGdpByInd_{BEA_DATA_VERSION}",
        ),
        pth=os.path.join(_LOCAL_GDP_DETAIL_DIR, f"{BEA_DATA_VERSION}_Detail{fname}"),
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


# ---------------------------------------------------------------------------
# BEA "underlying" industry detail - UGO205-A / UII205-A / UVA205-A
# ---------------------------------------------------------------------------

#: Sheet holding the 191-row underlying-industry frame in each workbook.
UNDERLYING_SHEETS: ta.Dict[str, ta.Tuple[str, str]] = {
    "gross_output": ("GrossOutput.xlsx", "UGO205-A"),
    "intermediate_inputs": ("IntermediateInputs.xlsx", "UII205-A"),
    "value_added": ("ValueAdded.xlsx", "UVA205-A"),
}

LINE_COL = "line"
INDENT_COL = "indent"

#: Lines 189-191 are addenda (private goods-producing, private
#: services-producing, and ICT-producing industries). They re-aggregate rows
#: that are already counted, so they are dropped rather than mapped.
UNDERLYING_ADDENDA_LINES = (189, 190, 191)

#: Years published in the 205-A tables.
UNDERLYING_YEARS = tuple(range(1997, 2025))


def load_go_underlying() -> pd.DataFrame:
    """Underlying-industry gross output (UGO205-A), million USD."""
    return _load_underlying_table("gross_output")


def load_ii_underlying() -> pd.DataFrame:
    """Underlying-industry intermediate inputs (UII205-A), million USD.

    Intermediate inputs are suppressed for every year on lines 83 (Customs
    duties) and 176 (Private households); those cells read NaN. Both industries
    have a published 2017 detail ``T005`` of zero.
    """
    return _load_underlying_table("intermediate_inputs")


def load_va_underlying() -> pd.DataFrame:
    """Underlying-industry value added (UVA205-A), million USD.

    Value added at producer prices, so ``UGO205-A = UII205-A + UVA205-A``. No
    cells are suppressed.
    """
    return _load_underlying_table("value_added")


def _load_underlying_table(series: str) -> pd.DataFrame:
    """Load one 205-A sheet, indexed by the workbook's own ``Line`` number.

    Returns the 191 data rows with the footnote block dropped, an ``indent``
    column recovered from the leading whitespace of the industry name, and one
    float column per year of :data:`UNDERLYING_YEARS`. Suppressed cells, which
    BEA writes as ``.....``, come back as NaN.
    """
    fname, sheet_name = UNDERLYING_SHEETS[series]
    _download_underlying_workbook(fname)
    df = pd.read_excel(
        os.path.join(_LOCAL_GDP_DETAIL_DIR, f"{BEA_DATA_VERSION}_Detail{fname}"),
        sheet_name=sheet_name,
        skiprows=7,
    )
    df = df[pd.to_numeric(df["Line"], errors="coerce").notna()].copy()
    name = df["Unnamed: 1"].astype(str)
    df[INDENT_COL] = (name.str.len() - name.str.lstrip().str.len()).astype(int)
    df[SECTOR_NAME_COL] = name.str.strip()
    df[LINE_COL] = df["Line"].astype(int)
    years = [str(year) for year in UNDERLYING_YEARS]
    for year in years:
        df[year] = pd.to_numeric(df[year], errors="coerce")
    df = df.set_index(LINE_COL)[[SECTOR_NAME_COL, INDENT_COL] + years]
    if len(df) != 191:
        raise ValueError(f"expected 191 rows in {sheet_name}, got {len(df)}")
    return df


def _download_underlying_workbook(fname: str) -> None:
    """
    Ensure one of the three underlying-detail workbooks exists locally by
    downloading it from GCS if necessary. They sit beside GrossOutput.xlsx in
    the same UGdpByInd folder and use the same naming convention.
    """
    download_gcs_file_if_not_exists(
        name=fname,
        sub_bucket=posixpath.join(
            GCS_GDP_DIR,
            f"UGdpByInd_{BEA_DATA_VERSION}",
        ),
        pth=os.path.join(_LOCAL_GDP_DETAIL_DIR, f"{BEA_DATA_VERSION}_Detail{fname}"),
    )


def underlying_leaf_lines(gross_output: pd.DataFrame | None = None) -> ta.List[int]:
    """The 138 leaf lines of the 191-row hierarchy, in workbook order.

    A row is a leaf when the next row is not indented further. Line 1 ("All
    industries") carries the same indent as the level below it in the workbook,
    so it is forced to the root before the tree is read.
    """
    df = load_go_underlying() if gross_output is None else gross_output
    df = df[~df.index.isin(UNDERLYING_ADDENDA_LINES)]
    indent = df[INDENT_COL].tolist()
    indent[0] = min(indent) - 2
    lines = df.index.tolist()
    return [
        line
        for position, line in enumerate(lines)
        if position == len(lines) - 1 or indent[position + 1] <= indent[position]
    ]


#: Per-year tolerance when matching a run of UGO305-A rows to one UGO205-A
#: leaf: the greater of 5 million USD and 1e-4 of the leaf's own value. BEA
#: rounds the two tables independently and the residual reaches 5 million USD
#: on a 275 billion USD line. The next candidate row is larger than the
#: tolerance by three orders of magnitude in every group, so the match stays
#: unambiguous.
_UNDERLYING_MATCH_ABSOLUTE_TOLERANCE = 5.0
_UNDERLYING_MATCH_RELATIVE_TOLERANCE = 1e-4


def derive_underlying_line_mapping() -> ta.Dict[int, ta.List[str]]:
    """Recover the 191-line to BEA-detail mapping from gross output alone.

    UGO205-A and UGO305-A order industries the same way, so the 138 leaves of
    the 205-A hierarchy partition the 414 rows of 305-A into contiguous runs.
    This walks both in order and closes each run when its cumulative gross
    output matches the leaf's in every year 1997-2024, then reads the BEA codes
    off the 305-A rows the run covers.

    Raises ``ValueError`` if any leaf fails to match or the 305-A rows are not
    consumed exactly, either of which means the two tables no longer align and
    :data:`~bedrock.extract.iot.constants
    .UNDERLYING_LINE_TO_BEA_2017_INDUSTRY_MAPPING` must be regenerated.
    """
    # Imported here rather than at module scope: the transform layer imports
    # this module, and map_detail_table lives above it in the same layer.
    from bedrock.transform.iot.helpers import (  # noqa: PLC0415
        SECTOR_CODE_COL,
        map_detail_table,
    )

    years = [str(year) for year in UNDERLYING_YEARS]
    underlying = load_go_underlying()
    leaves = underlying.loc[underlying_leaf_lines(underlying)]
    detail = map_detail_table(load_go_detail())
    if detail[SECTOR_CODE_COL].isna().any():
        unmapped = detail.loc[detail[SECTOR_CODE_COL].isna(), SECTOR_NAME_COL].tolist()
        raise ValueError(f"UGO305-A rows with no BEA code: {unmapped}")

    targets = leaves[years].to_numpy(float)
    rows = np.nan_to_num(detail[years].to_numpy(float))
    codes = detail[SECTOR_CODE_COL].tolist()

    mapping: ta.Dict[int, ta.List[str]] = {}
    cursor = 0
    for position, line in enumerate(leaves.index):
        target = targets[position]
        tolerance = np.maximum(
            _UNDERLYING_MATCH_ABSOLUTE_TOLERANCE,
            _UNDERLYING_MATCH_RELATIVE_TOLERANCE * np.abs(target),
        )
        start, cumulative, matched = cursor, np.zeros(len(years)), None
        while cursor < len(rows):
            cumulative = cumulative + rows[cursor]
            cursor += 1
            gap = np.abs(cumulative - target)
            if bool(np.all(np.where(np.isnan(gap), True, gap <= tolerance))):
                matched = cursor
                break
        if matched is None:
            raise ValueError(
                f"no run of UGO305-A rows from position {start} sums to line "
                f"{line} ({leaves.loc[line, SECTOR_NAME_COL]})"
            )
        mapping[int(line)] = sorted(set(codes[start:matched]))

    if cursor != len(rows):
        raise ValueError(
            f"matched {cursor} of {len(rows)} UGO305-A rows; the 205-A leaves "
            "do not partition the detail table"
        )
    return mapping
