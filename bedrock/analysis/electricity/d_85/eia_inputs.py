"""EIA Electric Power Annual inputs for d_85 analysis (Tables 8.3 and 2.4).

Table 2.4 loader re-exported from production ``electricity_end_use_mapping`` (PR4).
"""

from __future__ import annotations

import typing as ta

import pandas as pd

from bedrock.transform.eeio.electricity_end_use_mapping import (
    TABLE_2_4_DESCRIPTION,
    TABLE_2_4_PROVIDER,
    EPAEndUse,
    table_2_4_prices_cents_kwh,
)

TABLE_8_3_DESCRIPTION = (
    'Table 8.3 Revenue and expense statistics for major U.S. '
    'investor-owned electric utilities'
)
TABLE_8_3_PRODUCER = 'Investor-owned electric utilities'

TABLE_8_3_GENERATION_FLOWNAMES: dict[str, str] = {
    'Production': 'expenses: Production',
    'PurchasedPower': 'expenses: Purchased Power',
}
TABLE_8_3_SHARED_FLOWNAMES: dict[str, str] = {
    'Transmission': 'expenses: Transmission',
    'Distribution': 'expenses: Distribution',
}
TABLE_8_3_FLOWNAMES: dict[str, str] = {
    **TABLE_8_3_GENERATION_FLOWNAMES,
    **TABLE_8_3_SHARED_FLOWNAMES,
}


def _load_eia_fba(year: int) -> pd.DataFrame:
    from bedrock.extract.flowbyactivity import getFlowByActivity  # noqa: PLC0415

    return getFlowByActivity('EIA_ElectricPowerAnnual', year)


def _table_8_3_gtd_expenses(
    year: int,
    *,
    generation_key: ta.Literal['Production', 'PurchasedPower'],
    fba: pd.DataFrame | None,
) -> dict[str, float]:
    """Return generation + T/D operating expenses from Table 8.3 (IOU utilities)."""
    df = fba if fba is not None else _load_eia_fba(year)
    mask = (
        (df['Year'] == year)
        & (df['Description'].str.startswith(TABLE_8_3_DESCRIPTION, na=False))
        & (df['ActivityProducedBy'] == TABLE_8_3_PRODUCER)
    )
    subset = df.loc[mask]
    flownames = {
        'Generation': TABLE_8_3_GENERATION_FLOWNAMES[generation_key],
        **TABLE_8_3_SHARED_FLOWNAMES,
    }
    out: dict[str, float] = {}
    for label, flow_name in flownames.items():
        rows = subset.loc[subset['FlowName'] == flow_name, 'FlowAmount']
        if rows.empty:
            raise ValueError(
                f'Table 8.3 missing FlowName {flow_name!r} for year {year}'
            )
        out[label] = float(rows.iloc[0])
    return out


def table_8_3_gtd_expenses_musd(
    year: int = 2017,
    *,
    fba: pd.DataFrame | None = None,
) -> dict[str, float]:
    """Return Production + T/D operating expenses from Table 8.3 (IOU utilities)."""
    raw = _table_8_3_gtd_expenses(year, generation_key='Production', fba=fba)
    return {
        'Production': raw['Generation'],
        'Transmission': raw['Transmission'],
        'Distribution': raw['Distribution'],
    }


def table_8_3_purchased_power_gtd_expenses_musd(
    year: int = 2017,
    *,
    fba: pd.DataFrame | None = None,
) -> dict[str, float]:
    """Return Purchased Power + T/D operating expenses from Table 8.3 (IOU utilities)."""
    raw = _table_8_3_gtd_expenses(year, generation_key='PurchasedPower', fba=fba)
    return {
        'PurchasedPower': raw['Generation'],
        'Transmission': raw['Transmission'],
        'Distribution': raw['Distribution'],
    }


__all__ = [
    'EPAEndUse',
    'TABLE_2_4_DESCRIPTION',
    'TABLE_2_4_PROVIDER',
    'TABLE_8_3_DESCRIPTION',
    'TABLE_8_3_FLOWNAMES',
    'TABLE_8_3_GENERATION_FLOWNAMES',
    'TABLE_8_3_PRODUCER',
    'TABLE_8_3_SHARED_FLOWNAMES',
    'table_2_4_prices_cents_kwh',
    'table_8_3_gtd_expenses_musd',
    'table_8_3_purchased_power_gtd_expenses_musd',
]
