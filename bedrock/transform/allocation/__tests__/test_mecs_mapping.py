from __future__ import annotations

import pytest

from bedrock.extract.allocation.mecs import load_mecs_3_1
from bedrock.transform.allocation.mappings.cornerstone.mecs import (
    CORNERSTONE_INDUSTRY_TO_MECS_2_1_NAICS_MAPPING,
    CORNERSTONE_INDUSTRY_TO_MECS_2_1_NAICS_SUBTRACTION_MAPPING,
    CORNERSTONE_INDUSTRY_TO_MECS_3_1_NAICS_MAPPING,
    CORNERSTONE_INDUSTRY_TO_MECS_3_1_NAICS_SUBTRACTION_MAPPING,
    NON_MECS_INDUSTRIES,
)
from bedrock.transform.allocation.utils import flatten_items
from bedrock.utils.taxonomy.cornerstone.industries import INDUSTRIES


def _covered(
    mapping: dict[tuple[str, ...], tuple[str, ...]],
    subtraction: dict[tuple[str, ...], tuple[tuple[str, ...], tuple[str, ...]]],
) -> set[str]:
    return (
        set(flatten_items(mapping.keys()))
        | set(flatten_items(subtraction.keys()))
        | set(NON_MECS_INDUSTRIES)
    )


# Ag (1), mining (21), construction (23), and manufacturing (31-33).
CORNERSTONE_INDUSTRIAL_INDUSTRIES = {
    s for s in INDUSTRIES if s.startswith(("1", "21", "23", "3"))
}

NAICS_IN_CORNERSTONE_MECS_3_1_MAPPING = (
    set(flatten_items(CORNERSTONE_INDUSTRY_TO_MECS_3_1_NAICS_MAPPING.values()))
    | set(
        flatten_items(
            CORNERSTONE_INDUSTRY_TO_MECS_3_1_NAICS_SUBTRACTION_MAPPING.values()
        )
    )
    | {"321219"}  # parent code was used
    | {"312", "324", "331", "321", "3315"}  # child codes were used
)


def test_all_industrial_codes_covered_in_2_1_mapping() -> None:
    """Every industrial Cornerstone industry is in the MECS 2.1 mapping."""
    covered = _covered(
        CORNERSTONE_INDUSTRY_TO_MECS_2_1_NAICS_MAPPING,
        CORNERSTONE_INDUSTRY_TO_MECS_2_1_NAICS_SUBTRACTION_MAPPING,
    )
    assert covered == CORNERSTONE_INDUSTRIAL_INDUSTRIES, (
        f"{CORNERSTONE_INDUSTRIAL_INDUSTRIES.difference(covered)} are industrial "
        f"codes missing from the MECS 2.1 mapping; "
        f"{covered.difference(CORNERSTONE_INDUSTRIAL_INDUSTRIES)} are in the "
        f"mapping but are not industrial Cornerstone industries."
    )


def test_all_industrial_codes_covered_in_3_1_mapping() -> None:
    """Every industrial Cornerstone industry is in the MECS 3.1 mapping."""
    covered = _covered(
        CORNERSTONE_INDUSTRY_TO_MECS_3_1_NAICS_MAPPING,
        CORNERSTONE_INDUSTRY_TO_MECS_3_1_NAICS_SUBTRACTION_MAPPING,
    )
    assert covered == CORNERSTONE_INDUSTRIAL_INDUSTRIES, (
        f"{CORNERSTONE_INDUSTRIAL_INDUSTRIES.difference(covered)} are industrial "
        f"codes missing from the MECS 3.1 mapping; "
        f"{covered.difference(CORNERSTONE_INDUSTRIAL_INDUSTRIES)} are in the "
        f"mapping but are not industrial Cornerstone industries."
    )


@pytest.mark.eeio_integration
def test_all_naics_codes_in_mecs_covered_in_mapping() -> None:
    """Every NAICS code in MECS 3.1 is referenced by the Cornerstone 3.1 mapping."""
    naics_in_mecs = set(load_mecs_3_1().index.drop("Total"))
    assert NAICS_IN_CORNERSTONE_MECS_3_1_MAPPING == naics_in_mecs, (
        f"{naics_in_mecs - NAICS_IN_CORNERSTONE_MECS_3_1_MAPPING} are NAICS codes "
        f"in MECS tables but not in the Cornerstone mapping; "
        f"{NAICS_IN_CORNERSTONE_MECS_3_1_MAPPING - naics_in_mecs} are NAICS codes "
        f"in the Cornerstone mapping but not in MECS tables."
    )


@pytest.mark.eeio_integration
def test_cornerstone_mecs_3_1_naics_in_mecs_table() -> None:
    """All MECS NAICS codes referenced in Cornerstone 3.1 mapping values exist in the MECS 3.1 table."""
    mecs_index = set(load_mecs_3_1().index.drop("Total"))
    naics_in_cornerstone_3_1 = set(
        flatten_items(CORNERSTONE_INDUSTRY_TO_MECS_3_1_NAICS_MAPPING.values())
    ) | set(
        flatten_items(
            CORNERSTONE_INDUSTRY_TO_MECS_3_1_NAICS_SUBTRACTION_MAPPING.values()
        )
    )
    missing = naics_in_cornerstone_3_1 - mecs_index
    assert (
        not missing
    ), f"MECS NAICS codes used in Cornerstone 3.1 mapping are not in MECS 3.1 table: {missing}"
