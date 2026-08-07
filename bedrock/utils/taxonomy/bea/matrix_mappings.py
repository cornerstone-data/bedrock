import typing as ta

USA_2017_DETAIL_IO_MATRIX_NAMES = ta.Literal[
    "Make_detail",
    "Use_detail",
    "Import_detail",
    "Margins",
]
USA_2017_DETAIL_IO_MATRIX_MAPPING = {
    "Make_detail": "IOMake After Redefinitions 2017 Detail.xlsx",
    "Use_detail": "IOUse After Redefinitions 2017 Detail.xlsx",
    "Import_detail": "IOImports After Redefinitions 2017 Detail.xlsx",
    "Margins": "Margins Redefinitions 2017 DET.xlsx",
}

USA_2017_DETAIL_IO_BEFORE_REDEF_MATRIX_MAPPING = {
    "Make_detail_before_redef": "IOMake_Before_Redefinitions_2017_Detail.xlsx",
    "Use_detail_before_redef": "IOUse_Before_Redefinitions_PRO_2017_Detail.xlsx",
    "Import_detail_before_redef": "ImportMatrices_Before_Redefinitions_DET_2017.xlsx",
    "Margins": "Margins_Before_Redefinitions_2017_DET.xlsx",
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
USA_SUMMARY_MUT_MAPPING_1997_2024 = {
    "Make_summary": "IOMake_After_Redefinitions_PRO_1997-2024_Summary.xlsx",
    "Use_summary": "IOUse_After_Redefinitions_PRO_1997-2024_Summary.xlsx",
    "Import_summary": "IOImportMatrices_After_Redefinitions_SUM_1997-2024.xlsx",
}

USA_SUMMARY_SUT_NAMES = ta.Literal[
    "Supply_summary",
    "Use_SUT_summary",
]
USA_SUMMARY_SUT_MAPPING_2017_2022 = {
    "Supply_summary": "Supply_Tables_2017-2022_Summary.xlsx",
    "Use_SUT_summary": "Use_Tables_Supply-Use_Framework_2017-2022_Summary.xlsx",
}
# BEA extended the summary supply-use tables back to 1997 with the 2023 vintage, so
# the newer workbooks are named 1997-YYYY rather than 2017-YYYY. The 2024 vintage
# ships in https://apps.bea.gov/industry/release/zip/SUPPLY-USE.zip as
# `Supply_Summary.xlsx` / `Use_Summary.xlsx` - BEA dropped the year span from the file
# names entirely. Renamed on upload to keep the vintage visible.
#
# 2023 and 2024 both read from this one workbook rather than each being pinned to the
# vintage that first published it. Freezing 2023 at the 2023 vintage would be equally
# stable, but it would put a vintage boundary between 2023 and 2024, so their
# year-over-year step would carry a 0.3% revision artifact - and these are the RAS
# control totals for exactly those years.
USA_SUMMARY_SUT_MAPPING_1997_2024 = {
    "Supply_summary": "Supply_Tables_1997-2024_Summary.xlsx",
    "Use_SUT_summary": "Use_Tables_Supply-Use_Framework_1997-2024_Summary.xlsx",
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
USA_GROSS_INDUSTRY_OUTPUT_YEARS = USA_SUMMARY_MUT_YEARS

# Years wired up for summary SUT. It stops at 2017 on the left because that is where
# the pinned 2017-2022 workbook starts, not because of the data: the 1997-YYYY
# workbooks carry 1997-2016 on the same schema, so earlier years can be added here
# once they are given a vintage to pin to in `_load_usa_summary_sut`.
USA_SUMMARY_SUT_YEARS = ta.Literal[
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
