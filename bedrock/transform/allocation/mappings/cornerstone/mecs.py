"""
Cornerstone industry-to-MECS mappings.

Same as CEDA 2.1 and 3.1 NAICS except 331313 uses subtraction: mapped value 3313
minus 331314, 331315, and 331318. Sectors 335221, 335222, 335224, 335228 are
excluded from MECS allocation for Cornerstone.
"""

from bedrock.transform.allocation.mappings.v7.ceda_mecs import (
    CEDA_INDUSTRY_TO_MECS_2_1_NAICS_MAPPING,
    CEDA_INDUSTRY_TO_MECS_2_1_NAICS_SUBTRACTION_MAPPING,
    CEDA_INDUSTRY_TO_MECS_3_1_NAICS_MAPPING,
    CEDA_INDUSTRY_TO_MECS_3_1_NAICS_SUBTRACTION_MAPPING,
)

# Sectors excluded from Cornerstone MECS allocation (no MECS-based allocation).
_CORNERSTONE_MECS_EXCLUDED = (("335221",), ("335222",), ("335224",), ("335228"),)

# CORNERSTONE 2.1 NAICS: same as CEDA except 331313 uses subtraction (3313 minus 331314, 331315, 331318),
# we add 331314 and 33131B (BEA aggregate of 331315+331318), and we drop _CORNERSTONE_MECS_EXCLUDED.
CORNERSTONE_INDUSTRY_TO_MECS_2_1_NAICS_MAPPING: dict[
    tuple[str, ...], tuple[str, ...]
] = {
    k: v
    for k, v in CEDA_INDUSTRY_TO_MECS_2_1_NAICS_MAPPING.items()
    if k != ("331313",) and k not in _CORNERSTONE_MECS_EXCLUDED
}
CORNERSTONE_INDUSTRY_TO_MECS_2_1_NAICS_MAPPING[("331314",)] = ("331314",)
CORNERSTONE_INDUSTRY_TO_MECS_2_1_NAICS_MAPPING[("33131B",)] = ("331315", "331318")

# CORNERSTONE 2.1 NAICS subtraction: same as CEDA plus 331313 -> 3313 minus 331314, 331315, 331318; exclude _CORNERSTONE_MECS_EXCLUDED.
CORNERSTONE_INDUSTRY_TO_MECS_2_1_NAICS_SUBTRACTION_MAPPING: dict[
    tuple[str, ...], tuple[tuple[str, ...], tuple[str, ...]]
] = {
    k: v
    for k, v in CEDA_INDUSTRY_TO_MECS_2_1_NAICS_SUBTRACTION_MAPPING.items()
    if k not in _CORNERSTONE_MECS_EXCLUDED
}
CORNERSTONE_INDUSTRY_TO_MECS_2_1_NAICS_SUBTRACTION_MAPPING[("331313",)] = (
    (("3313",), ("331314", "331315", "331318"))
)

# CORNERSTONE 3.1 NAICS: same as CEDA except 331313 uses subtraction, we add 331314 and 33131B, and we drop _CORNERSTONE_MECS_EXCLUDED.
CORNERSTONE_INDUSTRY_TO_MECS_3_1_NAICS_MAPPING: dict[
    tuple[str, ...], tuple[str, ...]
] = {
    k: v
    for k, v in CEDA_INDUSTRY_TO_MECS_3_1_NAICS_MAPPING.items()
    if k != ("331313",) and k not in _CORNERSTONE_MECS_EXCLUDED
}
CORNERSTONE_INDUSTRY_TO_MECS_3_1_NAICS_MAPPING[("331314",)] = ("331314",)
CORNERSTONE_INDUSTRY_TO_MECS_3_1_NAICS_MAPPING[("33131B",)] = ("331315", "331318")

# CORNERSTONE 3.1 NAICS subtraction: same as CEDA plus 331313; exclude _CORNERSTONE_MECS_EXCLUDED.
CORNERSTONE_INDUSTRY_TO_MECS_3_1_NAICS_SUBTRACTION_MAPPING: dict[
    tuple[str, ...], tuple[tuple[str, ...], tuple[str, ...]]
] = {
    k: v
    for k, v in CEDA_INDUSTRY_TO_MECS_3_1_NAICS_SUBTRACTION_MAPPING.items()
    if k not in _CORNERSTONE_MECS_EXCLUDED
}
CORNERSTONE_INDUSTRY_TO_MECS_3_1_NAICS_SUBTRACTION_MAPPING[("331313",)] = (
    (("3313",), ("331314", "331315", "331318"))
)
