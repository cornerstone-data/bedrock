import pandas as pd

from bedrock.transform.allocation.derived import _warn_on_sectors_outside_model


def test_reports_unmapped_sector_but_not_final_demand() -> None:
    E = pd.DataFrame(
        {'233411': [1e9], '236115': [44e9], 'F01000': [80e9]}, index=['CO2']
    )
    dropped = _warn_on_sectors_outside_model(E, ['233411'])
    assert dropped.to_dict() == {'236115': 44e9}


def test_empty_when_all_sectors_in_model() -> None:
    E = pd.DataFrame({'233411': [1e9], 'F01000': [80e9]}, index=['CO2'])
    assert _warn_on_sectors_outside_model(E, ['233411']).empty
