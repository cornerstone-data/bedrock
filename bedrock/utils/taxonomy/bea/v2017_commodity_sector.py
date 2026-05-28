import typing as ta

# Industry and special-product commodities only. Final-demand and value-added
# sector codes (F010, F020, …, V003) live in v2017_final_demand and
# v2017_value_added at detail granularity (F01000, V00100, …).
BEA_2017_SECTOR_COMMODITY_CODE = ta.Literal[
    "11",  # Agriculture, forestry, fishing, and hunting
    "21",  # Mining
    "22",  # Utilities
    "23",  # Construction
    "31G",  # Manufacturing
    "42",  # Wholesale trade
    "44RT",  # Retail trade
    "48TW",  # Transportation and warehousing
    "51",  # Information
    "FIRE",  # Finance, insurance, real estate, rental, and leasing
    "PROF",  # Professional and business services
    "6",  # Educational services, health care, and social assistance
    "7",  # Arts, entertainment, recreation, accommodation, and food services
    "81",  # Other services, except government
    "G",  # Government
    "Used",  # Scrap, used and secondhand goods
    "Other",  # Noncomparable imports and rest-of-the-world adjustment
]

BEA_2017_SECTOR_COMMODITY_CODES: ta.List[BEA_2017_SECTOR_COMMODITY_CODE] = list(
    ta.get_args(BEA_2017_SECTOR_COMMODITY_CODE)
)

BEA_2017_SECTOR_COMMODITY_CODE_DESC: ta.Dict[BEA_2017_SECTOR_COMMODITY_CODE, str] = {
    "11": "Agriculture, forestry, fishing, and hunting",
    "21": "Mining",
    "22": "Utilities",
    "23": "Construction",
    "31G": "Manufacturing",
    "42": "Wholesale trade",
    "44RT": "Retail trade",
    "48TW": "Transportation and warehousing",
    "51": "Information",
    "FIRE": "Finance, insurance, real estate, rental, and leasing",
    "PROF": "Professional and business services",
    "6": "Educational services, health care, and social assistance",
    "7": "Arts, entertainment, recreation, accommodation, and food services",
    "81": "Other services, except government",
    "G": "Government",
    "Used": "Scrap, used and secondhand goods",
    "Other": "Noncomparable imports and rest-of-the-world adjustment",
}
