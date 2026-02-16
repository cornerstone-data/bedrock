import typing as ta

FINAL_DEMAND = ta.Literal[
    'F01000',  # Personal consumption expenditures
    'F02E00',  # Nonresidential private fixed investment in equipment
    'F02N00',  # Nonresidential private fixed investment in intellectual property products
    'F02R00',  # Residential private fixed investment
    'F02S00',  # Nonresidential private fixed investment in structures
    'F03000',  # Change in private inventories
    'F04000',  # Exports of goods and services
    'F05000',  # Imports of goods and services
    'F06C00',  # Federal Government defense: Consumption expenditures
    'F06E00',  # Federal national defense: Gross investment in equipment
    'F06N00',  # Federal national defense: Gross investment in intellectual property products
    'F06S00',  # Federal national defense: Gross investment in structures
    'F07C00',  # Federal Government nondefense: Consumption expenditures
    'F07E00',  # Federal nondefense: Gross investment in equipment
    'F07N00',  # Federal nondefense: Gross investment in intellectual property products
    'F07S00',  # Federal nondefense: Gross investment in structures
    'F10C00',  # State and local government consumption expenditures
    'F10E00',  # State and local: Gross investment in equipment
    'F10N00',  # State and local: Gross investment in intellectual property products
    'F10S00',  # State and local: Gross investment in structures
]

FINAL_DEMANDS: ta.List[FINAL_DEMAND] = list(ta.get_args(FINAL_DEMAND))

FINAL_DEMAND_DESC: ta.Dict[FINAL_DEMAND, str] = {
    # order matters
    'F01000': 'Personal consumption expenditures',
    'F02E00': 'Nonresidential private fixed investment in equipment',
    'F02N00': 'Nonresidential private fixed investment in intellectual property products',
    'F02R00': 'Residential private fixed investment',
    'F02S00': 'Nonresidential private fixed investment in structures',
    'F03000': 'Change in private inventories',
    'F04000': 'Exports of goods and services',
    'F05000': 'Imports of goods and services',
    'F06C00': 'Federal Government defense: Consumption expenditures',
    'F06E00': 'Federal national defense: Gross investment in equipment',
    'F06N00': 'Federal national defense: Gross investment in intellectual property products',
    'F06S00': 'Federal national defense: Gross investment in structures',
    'F07C00': 'Federal Government nondefense: Consumption expenditures',
    'F07E00': 'Federal nondefense: Gross investment in equipment',
    'F07N00': 'Federal nondefense: Gross investment in intellectual property products',
    'F07S00': 'Federal nondefense: Gross investment in structures',
    'F10C00': 'State and local government consumption expenditures',
    'F10E00': 'State and local: Gross investment in equipment',
    'F10N00': 'State and local: Gross investment in intellectual property products',
    'F10S00': 'State and local: Gross investment in structures',
}
