"""Locked NAICS→Cornerstone map and Make static rules for waste weight derivation."""

from __future__ import annotations

# Provenance id for WeightDerivationProvenance.naics_map_version.
# Ultimate source of the 6-digit rolls is Sector_Crosswalk_Cornerstone_2025
# (Cornerstone Activity ← NAICS_2017_Code)
NAICS_MAP_VERSION = "Sector_Crosswalk_Cornerstone_2025"

# Locked Decision: match Sector_Crosswalk_Cornerstone_2025 waste children.
# Do NOT use preview_sas_rcra_shares.py map (562119→562HAZ was wrong).
SAS_NAICS_TO_CORNERSTONE: dict[str, str] = {
    "562111": "562111",
    "562112": "562HAZ",
    "562211": "562HAZ",
    "562119": "562OTH",
    "562212": "562212",
    "562213": "562213",
    "562219": "562OTH",
    "562910": "562910",
    "56291": "562910",
    "562920": "562920",
    "56292": "562920",
    "562991": "562OTH",
    "562998": "562OTH",
    "562999": "562OTH",
}

WASTE_CHILDREN: list[str] = [
    "562111",
    "562HAZ",
    "562212",
    "562213",
    "562910",
    "562920",
    "562OTH",
]

WASTE_PARENT = "562000"

# RCRA 5-digit residuals (USEEIO Table 6) — only when not already 6-digit-resolved.
RCRA_FIVE_DIGIT_RESIDUAL: dict[str, str] = {
    "56211": "562111",
    "56221": "562212",
    "56299": "562OTH",
}

# Documented Note strings for special Make cells in the bundled long-format CSV.
MAKE_STATIC_RULE_NOTES = (
    "Make table, truck transport to solid waste collection",
    "Make table, landfill gas",
)

# Make-column assignment (non-waste industries × waste commodities): which child
# receives each industry's parent Make mass when balancing waste Make to Use.
# One-hot: that industry's entire parent mass goes to a single waste commodity.
MAKE_COLUMN_ONEHOT_INDUSTRIES: dict[str, str] = {
    "484000": "562111",
    "511110": "562111",
    "561700": "562910",
}
MAKE_COLUMN_GSLGO_INDUSTRIES: frozenset[str] = frozenset({"221300", "GSLGO"})
# Waste children that share the GSLGO / water-sewer Make-column mix (zeros elsewhere).
GSLGO_MEMBERSHIP: frozenset[str] = frozenset({"562111", "562212", "562213", "562920"})

# Make-row assignment (waste industries × non-waste commodities): which waste
# industry produces each commodity when balancing secondary production.
# One-hot: that commodity's entire parent Make mass goes to a single waste industry.
MAKE_ROW_ONEHOT_COMMODITIES: dict[str, str] = {
    "211000": "562212",
    "S00401": "562920",
}
