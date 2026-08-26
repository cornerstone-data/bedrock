"""Parser units for MECS Table 7.7 (million kWh) vs Table 7.2 (money)."""

from __future__ import annotations

import pandas as pd

from bedrock.extract.eia.EIA_MECS import _eia_clean_mecs_energy


def _raw_frames(title: str, n_value_cols: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    header = [title] + [None] * (n_value_cols + 1)
    data_row = ['311', 'Food'] + [10.0] * n_value_cols
    rse_row = ['311', 'Food'] + [1.0] * n_value_cols
    blank = [None] * (n_value_cols + 2)
    data = pd.DataFrame([blank, blank, header, data_row])
    rse = pd.DataFrame([blank, blank, header, rse_row])
    return data, rse


def test_table_7_7_named_columns_and_million_kwh() -> None:
    data, rse = _raw_frames('Table 7.7', n_value_cols=3)
    config = {
        'table_dict': {
            '2018': {
                'Table 7.7': {
                    'col_names': [
                        'NAICS Code',
                        'Subsector and Industry',
                        'Electricity total',
                        'Electricity from Local Utility',
                        'Electricity from Sources Other than Local Utility',
                    ],
                    'regions': {'Total United States': [4, 4]},
                    'rse_regions': {'Total United States': [4, 4]},
                    'data_type': 'fuel consumption',
                }
            }
        }
    }
    out = _eia_clean_mecs_energy(data, rse, year='2018', config=config)
    assert set(out['FlowName']) == {
        'Electricity total',
        'Electricity from Local Utility',
        'Electricity from Sources Other than Local Utility',
    }
    assert set(out['Unit']) == {'million kWh'}
    assert set(out['Class']) == {'Energy'}


def test_table_7_2_still_money_units() -> None:
    data, rse = _raw_frames('Table 7.2', n_value_cols=1)
    config = {
        'table_dict': {
            '2018': {
                'Table 7.2': {
                    'col_names': [
                        'NAICS Code',
                        'Subsector and Industry',
                        'Net Electricity | USD / million btu',
                    ],
                    'regions': {'Total United States': [4, 4]},
                    'rse_regions': {'Total United States': [4, 4]},
                    'data_type': 'money',
                }
            }
        }
    }
    out = _eia_clean_mecs_energy(data, rse, year='2018', config=config)
    assert set(out['Unit']) == {'USD / million btu'}
    assert set(out['Class']) == {'Money'}
    assert set(out['FlowName']) == {'Net Electricity'}
