import typing as ta

USA_2017_SUMMARY_FINAL_DEMAND_DESC = {
    # from GCS_CEDA_INPUT_DIR/USA_AllTablesSUP/Use_Tables_Supply-Use_Framework_2017-2022_Summary.xlsx
    # order matters!
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


USA_2017_SUMMARY_FINAL_DEMAND_CODES = list(USA_2017_SUMMARY_FINAL_DEMAND_DESC.keys())
BEA_2017_FINAL_DEMAND_SUMMARY_CODE = ta.Literal[
    "F010",
    "F02E",
    "F02N",
    "F02R",
    "F02S",
    "F030",
    "F040",
    "F050",
    "F06C",
    "F06E",
    "F06N",
    "F06S",
    "F07C",
    "F07E",
    "F07N",
    "F07S",
    "F10C",
    "F10E",
    "F10N",
    "F10S",
]
BEA_2017_FINAL_DEMAND_SUMMARY_CODES = USA_2017_SUMMARY_FINAL_DEMAND_CODES
BEA_2017_FINAL_DEMAND_SUMMARY_CODE_DESC: ta.Dict[BEA_2017_FINAL_DEMAND_SUMMARY_CODE, str] = USA_2017_SUMMARY_FINAL_DEMAND_DESC  # type: ignore
USA_2017_SUMMARY_TOTAL_EXPORTS_CODE = "F040"
USA_2017_SUMMARY_TOTAL_IMPORTS_CODE = "F050"
