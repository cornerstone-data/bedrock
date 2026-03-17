from __future__ import annotations

import functools
import os
import posixpath

import pandas as pd

from bedrock.transform.eeio.derived_2017 import (
    derive_2017_U_set_usa,
    derive_2017_V_usa,
    derive_2017_Y_personal_consumption_expenditure_usa,
)
from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.io.gcp import load_from_gcs
from bedrock.utils.io.gcp_paths import GCS_CEDA_INPUT_DIR
from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS

GCS_BEA_PCE_DIR = posixpath.join(
    GCS_CEDA_INPUT_DIR, "BEA_PersonalConsumptionExpenditure"
)
IN_DIR = os.path.join(os.path.dirname(__file__), "..", "input_data")


@functools.cache
def load_bea_make_table() -> pd.DataFrame:
    """
    This is a wrapper function that loads the latest BEA Supply table.
    WARNING: this table is 2017 Supply and is transposed from the 2012 Make,
    so the rows are the industries and the columns are the commodities
    """

    bea_make = derive_2017_V_usa()
    assert (
        bea_make.index == CEDA_V7_SECTORS
    ).any(), "BEA make table has incorrect index."
    assert (
        bea_make.columns == CEDA_V7_SECTORS
    ).any(), "BEA make table has incorrect columns."
    return bea_make


@functools.cache
def _load_bea_use_table_cached(use_cornerstone: bool) -> pd.DataFrame:
    """Inner loader keyed by schema so both CEDA and Cornerstone can be cached."""
    if use_cornerstone:
        from bedrock.transform.eeio import derived_cornerstone  # noqa: PLC0415

        uset = derived_cornerstone.derive_cornerstone_U_set()
        U_combined = (uset.Udom + uset.Uimp).T
        Y_cs = derived_cornerstone.derive_cornerstone_Y_personal_consumption_expenditure().to_frame().T
        return pd.concat([U_combined, Y_cs])
    U_set = derive_2017_U_set_usa()
    Y_usa = derive_2017_Y_personal_consumption_expenditure_usa().to_frame()
    return pd.concat([(U_set.Udom + U_set.Uimp).T, Y_usa.T])


def load_bea_use_table() -> pd.DataFrame:
    """
    Load BEA Use and Final Demand tables aligned to the model schema.

    When use_cornerstone_2026_model_schema is False, returns CEDA v7 industry rows.
    When True, returns Cornerstone industry rows (from derive_cornerstone_U_set and Y).
    Rows = industries + one PCE row; columns = commodities. Result is cached per schema.
    """
    use_cornerstone = get_usa_config().use_cornerstone_2026_model_schema
    return _load_bea_use_table_cached(use_cornerstone)


@functools.cache
def load_bea_personal_consumption_expenditure() -> pd.Series[float]:
    """
    Latest BEA Personal Consumption Expenditure by Major Type of Product from
    https://apps.bea.gov/iTable/?reqid=19&step=2&isuri=1&categories=survey&_gl=1*1mu0824*_ga*MTkyNDEyMDE5LjE3MTA0NjE1MjE.*_ga_J4698JNNFT*MTcxMDQ2MTUyMC4xLjEuMTcxMDQ2MjIyNS4xNC4wLjA.#eyJhcHBpZCI6MTksInN0ZXBzIjpbMSwyLDMsM10sImRhdGEiOltbImNhdGVnb3JpZXMiLCJTdXJ2ZXkiXSxbIk5JUEFfVGFibGVfTGlzdCIsIjY1Il0sWyJGaXJzdF9ZZWFyIiwiMjAxMiJdLFsiTGFzdF9ZZWFyIiwiMjAyMyJdLFsiU2NhbGUiLCItNiJdLFsiU2VyaWVzIiwiQSJdXX0=
    """
    tbl = load_from_gcs(
        name="BEA Personal Consumption Expenditures by Major Type of Product_June27_2024.csv",
        sub_bucket=GCS_BEA_PCE_DIR,
        local_dir=IN_DIR,
        loader=lambda pth: pd.read_csv(
            pth,
            skiprows=3,
            index_col=1,
        )
        .dropna()
        .drop(columns=["Line"]),
    )
    tbl.index = tbl.index.str.strip()
    tbl.columns = tbl.columns.astype(int)
    return tbl.loc[:, get_usa_config().usa_ghg_data_year]
