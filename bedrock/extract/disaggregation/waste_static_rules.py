"""Locked NAICS→Cornerstone map and Make static rules for waste weight derivation."""

from __future__ import annotations

# Map id for WeightDerivationProvenance.naics_map_version
NAICS_MAP_VERSION = "workbook_ec_tab+Sector_Crosswalk_Cornerstone_2025_v1"

# Locked Decision (plan): workbook EC tab + Cornerstone_2025 crosswalk.
# Do NOT use Phase 1 preview_sas_rcra_shares.py map (562119→562HAZ was wrong).
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

# Make static commodity→industry rules (workbook) — time-invariant.
# IndustryCode (row) → CommodityCode → share among waste children for that cell.
# Encoded as long-format Note patterns in bundled Make CSV; kept here for docs.
MAKE_STATIC_RULE_NOTES = (
    "Make table, truck transport to solid waste collection",
    "Make table, landfill gas",
)
