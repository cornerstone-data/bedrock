import typing as ta

BEA_2017_FINAL_DEMAND_SUMMARY_CODE = ta.Literal[
    "F010",  # Personal consumption expenditures
    "F02E",  # Nonresidential private fixed investment in equipment
    "F02N",  # Nonresidential private fixed investment in intellectual property products
    "F02R",  # Residential private fixed investment
    "F02S",  # Nonresidential private fixed investment in structures
    "F030",  # Change in private inventories
    "F040",  # Exports of goods and services
    "F050",  # Imports of goods and services
    "F06C",  # Federal Government defense: Consumption expenditures
    "F06E",  # Federal national defense: Gross investment in equipment
    "F06N",  # Federal national defense: Gross investment in intellectual property products
    "F06S",  # Federal national defense: Gross investment in structures
    "F07C",  # Federal Government nondefense: Consumption expenditures
    "F07E",  # Federal nondefense: Gross investment in equipment
    "F07N",  # Federal nondefense: Gross investment in intellectual property products
    "F07S",  # Federal nondefense: Gross investment in structures
    "F10C",  # State and local government consumption expenditures
    "F10E",  # State and local: Gross investment in equipment
    "F10N",  # State and local: Gross investment in intellectual property products
    "F10S",  # State and local: Gross investment in structures
]

BEA_2017_FINAL_DEMAND_SUMMARY_CODES: ta.List[BEA_2017_FINAL_DEMAND_SUMMARY_CODE] = list(
    ta.get_args(BEA_2017_FINAL_DEMAND_SUMMARY_CODE)
)

BEA_2017_FINAL_DEMAND_SUMMARY_CODE_DESC: ta.Dict[BEA_2017_FINAL_DEMAND_SUMMARY_CODE, str] = {
    "F010": "Personal consumption expenditures",
    "F02E": "Nonresidential private fixed investment in equipment",
    "F02N": "Nonresidential private fixed investment in intellectual property products",
    "F02R": "Residential private fixed investment",
    "F02S": "Nonresidential private fixed investment in structures",
    "F030": "Change in private inventories",
    "F040": "Exports of goods and services",
    "F050": "Imports of goods and services",
    "F06C": "Federal Government defense: Consumption expenditures",
    "F06E": "Federal national defense: Gross investment in equipment",
    "F06N": "Federal national defense: Gross investment in intellectual property products",
    "F06S": "Federal national defense: Gross investment in structures",
    "F07C": "Federal Government nondefense: Consumption expenditures",
    "F07E": "Federal nondefense: Gross investment in equipment",
    "F07N": "Federal nondefense: Gross investment in intellectual property products",
    "F07S": "Federal nondefense: Gross investment in structures",
    "F10C": "State and local government consumption expenditures",
    "F10E": "State and local: Gross investment in equipment",
    "F10N": "State and local: Gross investment in intellectual property products",
    "F10S": "State and local: Gross investment in structures",
}

BEA_2017_SUMMARY_TOTAL_EXPORTS_CODE = "F040"
BEA_2017_SUMMARY_TOTAL_IMPORTS_CODE = "F050"
