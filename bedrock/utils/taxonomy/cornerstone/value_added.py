import typing as ta

VALUE_ADDED = ta.Literal[
    'V00100',  # Compensation of employees
    'V00200',  # Taxes on production and imports, less subsidies
    'V00300',  # Gross operating surplus
]

VALUE_ADDEDS: ta.List[VALUE_ADDED] = list(ta.get_args(VALUE_ADDED))

VALUE_ADDED_DESC: ta.Dict[VALUE_ADDED, str] = {
    # order matters
    'V00100': 'Compensation of employees',
    'V00200': 'Taxes on production and imports, less subsidies',
    'V00300': 'Gross operating surplus',
}
