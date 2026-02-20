import typing as ta

USA_2017_VALUE_ADDED_DESC = {
    # from GCS_CEDA_INPUT_DIR/USA_AllTablesSUP/Use_SUT_Framework_2017_DET.xlsx
    # order matters!
    "V00100": "Compensation of employees",
    "V00200": "Taxes on production and imports, less subsidies",
    "V00300": "Gross operating surplus",
}


USA_2017_VALUE_ADDED_CODES = list(USA_2017_VALUE_ADDED_DESC.keys())
BEA_2017_VALUE_ADDED_CODE = ta.Literal[
    "V00100",
    "V00200",
    "V00300",
]
BEA_2017_VALUE_ADDED_CODES = USA_2017_VALUE_ADDED_CODES
BEA_2017_VALUE_ADDED_CODE_DESC: ta.Dict[BEA_2017_VALUE_ADDED_CODE, str] = USA_2017_VALUE_ADDED_DESC  # type: ignore
USA_2017_VALUE_ADDED_COMPENSATION_CODE = "V00100"
USA_2017_VALUE_ADDED_TAXES_CODE = "V00200"
USA_2017_VALUE_ADDED_SURPLUS_CODE = "V00300"
