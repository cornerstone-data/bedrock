from __future__ import annotations

import logging

import pytest

from bedrock.ceda_usa.transform.allocation.mappings.v7.ceda_mecs import (
    CEDA_INDUSTRY_TO_MECS_2_1_NAICS_MAPPING,
    CEDA_INDUSTRY_TO_MECS_2_1_NAICS_SUBTRACTION_MAPPING,
    CEDA_INDUSTRY_TO_MECS_3_1_NAICS_MAPPING,
    CEDA_INDUSTRY_TO_MECS_3_1_NAICS_SUBTRACTION_MAPPING,
    NON_MECS_INDUSTRIES,
)
from bedrock.ceda_usa.transform.allocation.utils import flatten_items
from bedrock.ceda_usa.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from bedrock.extract.allocation.mecs import load_mecs_3_1

logger = logging.getLogger(__name__)

CEDA_INDUSTRIAL_SECTORS_IN_MECS_2_1_MAPPING = (
    set(flatten_items(CEDA_INDUSTRY_TO_MECS_2_1_NAICS_MAPPING.keys()))
    | set(flatten_items(CEDA_INDUSTRY_TO_MECS_2_1_NAICS_SUBTRACTION_MAPPING.keys()))
    | set(NON_MECS_INDUSTRIES)
)

CEDA_INDUSTRIAL_SECTORS_IN_MECS_3_1_MAPPING = (
    set(flatten_items(CEDA_INDUSTRY_TO_MECS_3_1_NAICS_MAPPING.keys()))
    | set(flatten_items(CEDA_INDUSTRY_TO_MECS_3_1_NAICS_SUBTRACTION_MAPPING.keys()))
    | set(NON_MECS_INDUSTRIES)
)

CEDA_INDUSTRIAL_SECTORS_DEFAULT = {
    # We define industrial sectors to be Ag (1), Mining (21), Construction (23), and Manufacturing (31-33)
    s
    for s in CEDA_V7_SECTORS
    if s.startswith(("1", "21", "23", "3"))
}

NAICS_IN_OUR_MECS_3_1_MAPPING = (
    set(flatten_items(CEDA_INDUSTRY_TO_MECS_3_1_NAICS_MAPPING.values()))
    | set(flatten_items(CEDA_INDUSTRY_TO_MECS_3_1_NAICS_SUBTRACTION_MAPPING.values()))
    | {"321219", "331314", "331315", "331318"}  # parent codes were used
    | {"312", "324", "331", "321", "3315"}  # child codes were used
)


def test_all_industrial_ceda_codes_covered_in_2_1_mapping() -> None:
    """
    Ensure all industrial CEDA codes are included in CEDA to MECS 2.1 mapping
    """
    assert (
        CEDA_INDUSTRIAL_SECTORS_IN_MECS_2_1_MAPPING == CEDA_INDUSTRIAL_SECTORS_DEFAULT
    ), f"{CEDA_INDUSTRIAL_SECTORS_DEFAULT - CEDA_INDUSTRIAL_SECTORS_IN_MECS_2_1_MAPPING} are CEDA industrial codes but not in CEDA <> MECS mapping; {CEDA_INDUSTRIAL_SECTORS_IN_MECS_2_1_MAPPING - CEDA_INDUSTRIAL_SECTORS_DEFAULT} are CEDA codes in CEDA <> MECS mapping but not in CEDA industrial codes."


def test_all_industrial_ceda_codes_covered_in_3_1_mapping() -> None:
    """
    Ensure all industrial CEDA codes are included in CEDA to MECS 3.1 mapping
    """
    assert (
        CEDA_INDUSTRIAL_SECTORS_IN_MECS_3_1_MAPPING == CEDA_INDUSTRIAL_SECTORS_DEFAULT
    ), f"{CEDA_INDUSTRIAL_SECTORS_DEFAULT - CEDA_INDUSTRIAL_SECTORS_IN_MECS_3_1_MAPPING} are CEDA industrial codes but not in CEDA <> MECS mapping; {CEDA_INDUSTRIAL_SECTORS_IN_MECS_3_1_MAPPING - CEDA_INDUSTRIAL_SECTORS_DEFAULT} are CEDA codes in CEDA <> MECS mapping but not in CEDA industrial codes."


@pytest.mark.eeio_integration
def test_all_naics_codes_in_mecs_covered_in_mapping() -> None:
    """
    Ensure all NAICS codes in MECS 3.1 are covered in CEDA to MECS mapping
    """
    naics_in_mecs = set(load_mecs_3_1().index.drop("Total"))
    assert (
        NAICS_IN_OUR_MECS_3_1_MAPPING == naics_in_mecs
    ), f"{naics_in_mecs - NAICS_IN_OUR_MECS_3_1_MAPPING} are NAICS codes in MECS tables but not in CEDA <> MECS mapping; {NAICS_IN_OUR_MECS_3_1_MAPPING - naics_in_mecs} are NAICS codes in CEDA <> MECS mapping but not in MECS tables."
