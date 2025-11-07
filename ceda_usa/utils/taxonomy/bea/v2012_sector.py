import typing as ta

BEA_2012_SECTOR_CODE = ta.Literal[
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

BEA_2012_SECTOR_CODES: ta.List[BEA_2012_SECTOR_CODE] = list(
    ta.get_args(BEA_2012_SECTOR_CODE)
)

BEA_2012_SECTOR_CODE_DESC: ta.Dict[BEA_2012_SECTOR_CODE, str] = {
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
