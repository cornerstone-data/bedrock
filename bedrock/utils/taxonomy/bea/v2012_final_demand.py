import typing as ta

USA_2012_FINAL_DEMAND_DESC = {
    # order matters!
    "F01000": "Personal consumption expenditures",
    "F02E00": "Nonresidential private fixed investment in equipment",
    "F02N00": "Nonresidential private fixed investment in intellectual property products",
    "F02R00": "Residential private fixed investment",
    "F02S00": "Nonresidential private fixed investment in structures",
    "F03000": "Change in private inventories",
    "F04000": "Exports of goods and services",
    "F05000": "Imports of goods and services",
    "F06C00": "Federal Government defense: Consumption expenditures",
    "F06E00": "Federal national defense: Gross investment in equipment",
    "F06N00": "Federal national defense: Gross investment in intellectual property products",
    "F06S00": "Federal national defense: Gross investment in structures",
    "F07C00": "Federal Government nondefense: Consumption expenditures",
    "F07E00": "Federal nondefense: Gross investment in equipment",
    "F07N00": "Federal nondefense: Gross investment in intellectual property products",
    "F07S00": "Federal nondefense: Gross investment in structures",
    "F10C00": "State and local government consumption expenditures",
    "F10E00": "State and local: Gross investment in equipment",
    "F10N00": "State and local: Gross investment in intellectual property products",
    "F10S00": "State and local: Gross investment in structures",
}


USA_2012_FINAL_DEMAND_CODES = list(USA_2012_FINAL_DEMAND_DESC.keys())
BEA_2012_FINAL_DEMAND_CODE = ta.Literal[
    "F01000",
    "F02E00",
    "F02N00",
    "F02R00",
    "F02S00",
    "F03000",
    "F04000",
    "F05000",
    "F06C00",
    "F06E00",
    "F06N00",
    "F06S00",
    "F07C00",
    "F07E00",
    "F07N00",
    "F07S00",
    "F10C00",
    "F10E00",
    "F10N00",
    "F10S00",
]
BEA_2012_FINAL_DEMAND_CODES = USA_2012_FINAL_DEMAND_CODES
BEA_2012_FINAL_DEMAND_CODE_DESC: ta.Dict[BEA_2012_FINAL_DEMAND_CODE, str] = USA_2012_FINAL_DEMAND_DESC  # type: ignore
