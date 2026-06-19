"""Shared fixtures for d_85 analysis tests."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from bedrock.transform.eeio.electricity_disaggregation import ELECTRICITY_AGGREGATE
from bedrock.utils.schemas.cornerstone_schemas import ELECTRICITY_DISAGG_SECTORS
from bedrock.utils.taxonomy.cornerstone.value_added import VALUE_ADDEDS

_DATA_DIR = Path(__file__).resolve().parent.parent / 'data'
_CHECKPOINT_PATH = _DATA_DIR / 'stage2_checkpoint_subset.parquet'


def load_stage2_checkpoint_subset() -> tuple[
    pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame
]:
    """Load frozen stage-2 IO subset from parquet."""
    raw = pd.read_parquet(_CHECKPOINT_PATH)
    out: dict[str, pd.DataFrame] = {}
    for name in ('V', 'Udom', 'Uimp', 'VA', 'Y'):
        sub = raw.loc[raw['matrix'] == name]
        out[name] = sub.pivot(index='row', columns='col', values='value').astype(float)
    return out['V'], out['Udom'], out['Uimp'], out['VA'], out['Y']


def mock_fba_table83_table24() -> pd.DataFrame:
    """Minimal EIA FBA rows for Table 8.3 and 2.4 unit tests."""
    rows = [
        {
            'Year': 2017,
            'Description': (
                'Table 8.3 Revenue and expense statistics for major U.S. '
                'investor-owned electric utilities'
            ),
            'ActivityProducedBy': 'Investor-owned electric utilities',
            'ActivityConsumedBy': '',
            'FlowName': 'expenses: Production',
            'FlowAmount': 98659.0,
        },
        {
            'Year': 2017,
            'Description': (
                'Table 8.3 Revenue and expense statistics for major U.S. '
                'investor-owned electric utilities'
            ),
            'ActivityProducedBy': 'Investor-owned electric utilities',
            'ActivityConsumedBy': '',
            'FlowName': 'expenses: Transmission',
            'FlowAmount': 10804.0,
        },
        {
            'Year': 2017,
            'Description': (
                'Table 8.3 Revenue and expense statistics for major U.S. '
                'investor-owned electric utilities'
            ),
            'ActivityProducedBy': 'Investor-owned electric utilities',
            'ActivityConsumedBy': '',
            'FlowName': 'expenses: Distribution',
            'FlowAmount': 4358.0,
        },
    ]
    for sector, price in (
        ('Residential', 12.89),
        ('Commercial', 10.68),
        ('Industrial', 6.92),
        ('Transportation', 9.49),
        ('Total', 10.54),
    ):
        rows.append(
            {
                'Year': 2017,
                'Description': 'Table 2.4 Average price of electricity to ultimate customers',
                'ActivityProducedBy': 'Total Electric Industry',
                'ActivityConsumedBy': sector,
                'FlowName': '',
                'FlowAmount': price,
            }
        )
    return pd.DataFrame(rows)


def minimal_balanced_checkpoint(
    *,
    T: float = 100.0,
    x_agg: float = 350.0,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Synthetic stage-2 checkpoint for step-wise scenario tests."""
    agg = ELECTRICITY_AGGREGATE
    codes = list(ELECTRICITY_DISAGG_SECTORS)
    extra_rows = ['212100', '541000']
    com_rows = codes + [agg, *extra_rows]
    ind_cols = codes + [agg, *extra_rows, 'F01000']
    V = pd.DataFrame(0.0, index=com_rows, columns=ind_cols)
    V.at[agg, agg] = x_agg
    Udom = pd.DataFrame(0.0, index=com_rows, columns=ind_cols)
    Uimp = Udom.copy()
    Udom.at[agg, agg] = T
    Udom.at['212100', agg] = 50.0
    Udom.at['541000', agg] = 40.0
    VA = pd.DataFrame(0.0, index=list(VALUE_ADDEDS), columns=[agg])
    VA.at['V00100', agg] = 70.0
    VA.at['V00200', agg] = 30.0
    VA.at['V00300', agg] = 60.0
    Y = pd.DataFrame(0.0, index=com_rows, columns=['F01000'])
    Y.at[agg, 'F01000'] = 25.0
    return V, Udom, Uimp, VA, Y


@pytest.fixture
def stage2_checkpoint() -> tuple[
    pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame
]:
    return load_stage2_checkpoint_subset()


@pytest.fixture
def mock_fba() -> pd.DataFrame:
    return mock_fba_table83_table24()
