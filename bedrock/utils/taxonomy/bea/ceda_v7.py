"""Descriptions for CEDA v7 codes that are absent from Cornerstone commodities.

Diagnostics labels start from Cornerstone ``COMMODITY_DESC`` and fill only these
baseline-only codes: appliance detail and the pre-disaggregation waste aggregate.
"""

CEDA_V7_SECTOR_DESC: dict[str, str] = {
    "335221": "Household cooking appliance manufacturing",
    "335222": "Household refrigerator and home freezer manufacturing",
    "335224": "Household laundry equipment manufacturing",
    "335228": "Other major household appliance manufacturing",
    "562000": "Waste management and remediation services",
}
