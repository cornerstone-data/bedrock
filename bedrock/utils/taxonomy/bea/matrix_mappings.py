import typing as ta

USA_2017_DETAIL_IO_MATRIX_NAMES = ta.Literal[
    "Make_detail",
    "Use_detail",
    "Import_detail",
]
USA_2017_DETAIL_IO_MATRIX_MAPPING = {
    "Make_detail": "IOMake After Redefinitions 2017 Detail.xlsx",
    "Use_detail": "IOUse After Redefinitions 2017 Detail.xlsx",
    "Import_detail": "IOImports After Redefinitions 2017 Detail.xlsx",
    "Margins": "Margins Redefinitions 2017 DET.xlsx",
}

USA_2017_DETAIL_IO_BEFORE_REDEF_MATRIX_MAPPING = {
    "Make_detail_before_redef": "IOMake_Before_Redefinitions_2017_Detail.xlsx",
}

USA_2017_DETAIL_IO_SUT_MATRIX_NAMES = ta.Literal[
    "Supply_detail",
    "Use_SUT_detail",
]
USA_2017_DETAIL_IO_SUT_MATRIX_MAPPING = {
    "Supply_detail": "Supply_2017_DET.xlsx",
    "Use_SUT_detail": "Use_SUT_Framework_2017_DET.xlsx",
}

USA_SUMMARY_MUT_NAMES = ta.Literal[
    "Make_summary",
    "Use_summary",
    "Import_summary",
]
USA_SUMMARY_MUT_MAPPING_1997_2022 = {
    "Make_summary": "IOMake_After_Redefinitions_PRO_1997-2022_Summary.xlsx",
    "Use_summary": "IOUse_After_Redefinitions_PRO_1997-2022_Summary.xlsx",
    "Import_summary": "IOImportMatrices_After_Redefinitions_SUM_1997-2022.xlsx",
}
USA_SUMMARY_MUT_MAPPING_1997_2023 = {
    "Make_summary": "IOMake_After_Redefinitions_PRO_1997-2023_Summary.xlsx",
    "Use_summary": "IOUse_After_Redefinitions_PRO_1997-2023_Summary.xlsx",
    "Import_summary": "IOImportMatrices_After_Redefinitions_SUM_1997-2023.xlsx",
}

USA_SUMMARY_SUT_NAMES = ta.Literal[
    "Supply_summary",
    "Use_SUT_summary",
]
USA_SUMMARY_SUT_MAPPING_2017_2022 = {
    "Supply_summary": "Supply_Tables_2017-2022_Summary.xlsx",
    "Use_SUT_summary": "Use_Tables_Supply-Use_Framework_2017-2022_Summary.xlsx",
}

USA_DETAIL_MUT_YEARS = ta.Literal[2007, 2012, 2017]
USA_SUMMARY_MUT_YEARS = ta.Literal[
    2007,
    2008,
    2009,
    2010,
    2012,
    2011,
    2013,
    2014,
    2015,
    2016,
    2017,
    2018,
    2019,
    2020,
    2021,
    2022,
    2023,
    2024,
]

USA_2017_TAX_LESS_SUBSIDIES_CODE = ta.Literal["TOP", "SUB"]
USA_2017_TAX_LESS_SUBSIDIES_CODES: ta.List[USA_2017_TAX_LESS_SUBSIDIES_CODE] = list(
    ta.get_args(USA_2017_TAX_LESS_SUBSIDIES_CODE)
)
