"""
Cornerstone industry-to-MECS mappings.

Same as CEDA 2.1 and 3.1 NAICS except 331313 uses subtraction: mapped value 3313
minus 331314, 331315, and 331318. Cornerstone has aggregate 335220 (major household
appliances) but not 335221, 335222, 335224, 335228; we exclude the four and map
335220 -> MECS 335 so the aggregate receives allocation.
"""

from bedrock.transform.allocation.mappings.v7.ceda_mecs import (
    CEDA_INDUSTRY_TO_MECS_2_1_NAICS_MAPPING,
    CEDA_INDUSTRY_TO_MECS_2_1_NAICS_SUBTRACTION_MAPPING,
    CEDA_INDUSTRY_TO_MECS_3_1_NAICS_MAPPING,
    CEDA_INDUSTRY_TO_MECS_3_1_NAICS_SUBTRACTION_MAPPING,
)

# Single-sector keys to drop (Cornerstone has aggregate 335220 instead).
_CORNERSTONE_MECS_EXCLUDED = (
    ("335221",),
    ("335222",),
    ("335224",),
    ("335228",),
)
# In multi-sector keys, replace these with this aggregate so allocation goes to 335220.
_APPLIANCE_AGGREGATE = "335220"
_APPLIANCE_SUBS = frozenset({"335221", "335222", "335224", "335228"})


def _cornerstone_key(k: tuple[str, ...]) -> tuple[str, ...]:
    """Replace 335221/335222/335224/335228 with aggregate 335220 in key."""
    if not _APPLIANCE_SUBS.intersection(k):
        return k
    return tuple(s for s in k if s not in _APPLIANCE_SUBS) + (_APPLIANCE_AGGREGATE,)


# CORNERSTONE 2.1 NAICS: same as CEDA except 331313 uses subtraction (3313 minus 331314, 331315, 331318),
# we add 331314 and 33131B (BEA aggregate of 331315+331318), exclude _CORNERSTONE_MECS_EXCLUDED,
# and in any key containing the appliance sub-sectors use 335220 instead.
CORNERSTONE_INDUSTRY_TO_MECS_2_1_NAICS_MAPPING: dict[
    tuple[str, ...], tuple[str, ...]
] = {
    _cornerstone_key(k): v
    for k, v in CEDA_INDUSTRY_TO_MECS_2_1_NAICS_MAPPING.items()
    if k != ("331313",) and k not in _CORNERSTONE_MECS_EXCLUDED
}
CORNERSTONE_INDUSTRY_TO_MECS_2_1_NAICS_MAPPING[("331314",)] = ("331314",)
CORNERSTONE_INDUSTRY_TO_MECS_2_1_NAICS_MAPPING[("33131B",)] = ("331315", "331318")

# CORNERSTONE 2.1 NAICS subtraction: same as CEDA plus 331313 -> 3313 minus 331314, 331315, 331318; exclude _CORNERSTONE_MECS_EXCLUDED; use 335220 in keys that had the four.
CORNERSTONE_INDUSTRY_TO_MECS_2_1_NAICS_SUBTRACTION_MAPPING: dict[
    tuple[str, ...], tuple[tuple[str, ...], tuple[str, ...]]
] = {
    _cornerstone_key(k): v
    for k, v in CEDA_INDUSTRY_TO_MECS_2_1_NAICS_SUBTRACTION_MAPPING.items()
    if k not in _CORNERSTONE_MECS_EXCLUDED
}
CORNERSTONE_INDUSTRY_TO_MECS_2_1_NAICS_SUBTRACTION_MAPPING[("331313",)] = (
    ("3313",),
    ("331314", "331315", "331318"),
)

# CORNERSTONE 3.1 NAICS: same as CEDA except 331313 uses subtraction, we add 331314 and 33131B, exclude _CORNERSTONE_MECS_EXCLUDED, and use 335220 in keys that had the four.
CORNERSTONE_INDUSTRY_TO_MECS_3_1_NAICS_MAPPING: dict[
    tuple[str, ...], tuple[str, ...]
] = {
    _cornerstone_key(k): v
    for k, v in CEDA_INDUSTRY_TO_MECS_3_1_NAICS_MAPPING.items()
    if k != ("331313",) and k not in _CORNERSTONE_MECS_EXCLUDED
}
CORNERSTONE_INDUSTRY_TO_MECS_3_1_NAICS_MAPPING[("331314",)] = ("331314",)
CORNERSTONE_INDUSTRY_TO_MECS_3_1_NAICS_MAPPING[("33131B",)] = ("331315", "331318")

# CORNERSTONE 3.1 NAICS subtraction: same as CEDA plus 331313; exclude _CORNERSTONE_MECS_EXCLUDED; use 335220 in keys that had the four.
CORNERSTONE_INDUSTRY_TO_MECS_3_1_NAICS_SUBTRACTION_MAPPING: dict[
    tuple[str, ...], tuple[tuple[str, ...], tuple[str, ...]]
] = {
    _cornerstone_key(k): v
    for k, v in CEDA_INDUSTRY_TO_MECS_3_1_NAICS_SUBTRACTION_MAPPING.items()
    if k not in _CORNERSTONE_MECS_EXCLUDED
}
CORNERSTONE_INDUSTRY_TO_MECS_3_1_NAICS_SUBTRACTION_MAPPING[("331313",)] = (
    ("3313",),
    ("331314", "331315", "331318"),
)
