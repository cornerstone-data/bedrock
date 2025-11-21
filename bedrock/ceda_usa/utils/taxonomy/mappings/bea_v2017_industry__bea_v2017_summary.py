import os
import posixpath
import typing as ta

import pandas as pd

from bedrock.ceda_usa.utils.gcp import GCS_CEDA_USA_DIR
from bedrock.ceda_usa.utils.taxonomy.bea.v2017_commodity_summary import (
    BEA_2017_COMMODITY_SUMMARY_CODE,
    BEA_2017_COMMODITY_SUMMARY_CODES,
)
from bedrock.ceda_usa.utils.taxonomy.bea.v2017_industry import (
    BEA_2017_INDUSTRY_CODE,
    BEA_2017_INDUSTRY_CODES,
)
from bedrock.ceda_usa.utils.taxonomy.utils import validate_mapping
from bedrock.utils.gcp import download_gcs_file_if_not_exists

GCS_CEDA_TAXONOMY_DIR = posixpath.join(GCS_CEDA_USA_DIR, "taxonomy")

IN_DIR = os.path.join(os.path.dirname(__file__), "input_data")


def load_bea_v2017_industry_to_bea_v2017_summary() -> (
    ta.Dict[BEA_2017_INDUSTRY_CODE, ta.List[BEA_2017_COMMODITY_SUMMARY_CODE]]
):
    # This table is originally from https://www.bea.gov/system/files/2023-10/BEA-Industry-and-Commodity-Codes-and-NAICS-Concordance.xlsx
    # but it's no longer available as of June 2, 2025
    # Same content can be found in "NAICS Codes" tab in Use_SUT_Framework_2017_DET dataset downloadable from https://www.bea.gov/industry/input-output-accounts-data
    fname = "BEA-Industry-and-Commodity-Codes-and-NAICS-Concordance.xlsx"
    pth = os.path.join(IN_DIR, fname)
    download_gcs_file_if_not_exists(fname, GCS_CEDA_TAXONOMY_DIR, pth)
    df = (
        pd.read_excel(pth, sheet_name="NAICS Codes", skiprows=6)
        .loc[:, ["Unnamed: 1", "Unnamed: 3"]]
        .rename(
            columns={"Unnamed: 1": "BEA_2017_Summary", "Unnamed: 3": "BEA_2017_Detail"}
        )
    )
    df["BEA_2017_Summary"] = df["BEA_2017_Summary"].ffill().astype(str)

    df = (
        df[df["BEA_2017_Detail"].astype(str).isin(BEA_2017_INDUSTRY_CODES)]
        .drop_duplicates()
        .astype(str)
    )
    mapping = df.groupby("BEA_2017_Detail")["BEA_2017_Summary"].apply(list).to_dict()

    validate_mapping(
        mapping,
        domain=set(BEA_2017_INDUSTRY_CODES),
        codomain=set(BEA_2017_COMMODITY_SUMMARY_CODES),
    )
    return mapping
