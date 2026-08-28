"""Physical electricity trade for Trade FBS (#668).

Dollarizes EIA Electric Power Annual Table 2.14 national MWh with same-year
Census HS 2716 unit values. Quantity is EIA; price is Census merchandise
(``bedrock.extract.census.Census_USATrade`` unit-value CSV / refresh).
"""

from __future__ import annotations

from typing import Any, Literal

import pandas as pd

from bedrock.extract.census.Census_USATrade import (
    HS2716_ELECTRICAL_ENERGY,
    hs2716_unit_value_usd_per_mwh,
)
from bedrock.extract.disaggregation.egrid_generation import (
    eia_table_2_14_export_mwh,
    eia_table_2_14_import_mwh,
)
from bedrock.extract.flowbyactivity import FlowByActivity
from bedrock.utils.mapping.location import US_FIPS

TradeDirection = Literal['exports', 'imports']

#: Crosswalk Activities for national dollarized Table 2.14 rows.
ELECTRICITY_EXPORTS_ACTIVITY = 'Electricity exports'
ELECTRICITY_IMPORTS_ACTIVITY = 'Electricity imports'

_EXPORT_FLOW = 'electricity exports'
_IMPORT_FLOW = 'electricity imports'


def electricity_trade_mwh(year: int, direction: TradeDirection) -> float:
    """National Canada+Mexico EIA Table 2.14 MWh for *year* and *direction*."""
    if direction == 'exports':
        return eia_table_2_14_export_mwh(int(year))
    if direction == 'imports':
        return eia_table_2_14_import_mwh(int(year))
    raise ValueError(f'direction must be exports or imports, got {direction!r}')


def electricity_trade_usd(year: int, direction: TradeDirection) -> float:
    """EIA Table 2.14 MWh × same-year Census HS 2716 unit value, USD."""
    return electricity_trade_mwh(year, direction) * hs2716_unit_value_usd_per_mwh(
        year, direction
    )


def dollarize_electricity_trade_fba(fba: FlowByActivity, **_: Any) -> FlowByActivity:
    """Collapse Table 2.14 CA/MX MWh to national USD rows for Trade FBS.

    ``clean_fba_before_activity_sets`` on ``EIA_ElectricPowerAnnual``: replaces
    Canada/Mexico Table 2.14 import and export rows with one national USD row
    per direction (Activities ``Electricity exports`` / ``Electricity imports``).
    Other tables in the FBA are dropped so activity-set selection only sees
    trade dollars.
    """
    if fba.empty:
        return fba
    year = int(fba['Year'].iloc[0]) if 'Year' in fba.columns else None
    if year is None and 'year' in fba.config:
        year = int(fba.config['year'])
    if year is None:
        raise ValueError('Cannot dollarize electricity trade without Year')

    template = fba.iloc[[0]].copy()
    wanted_flows: set[str] = set()
    for aset in (fba.config.get('activity_sets') or {}).values():
        flow = (aset.get('selection_fields') or {}).get('FlowName')
        if isinstance(flow, str):
            wanted_flows.add(flow)

    rows: list[pd.DataFrame] = []
    for flow_name, direction, produced, consumed in (
        (_EXPORT_FLOW, 'exports', None, ELECTRICITY_EXPORTS_ACTIVITY),
        (_IMPORT_FLOW, 'imports', ELECTRICITY_IMPORTS_ACTIVITY, None),
    ):
        if wanted_flows and flow_name not in wanted_flows:
            continue
        mask = (fba['FlowName'].astype(str) == flow_name) & fba['Description'].astype(
            str
        ).str.contains('Table 2.14', na=False)
        if not mask.any():
            continue
        usd = electricity_trade_usd(year, direction)  # type: ignore[arg-type]
        row = template.copy()
        row['FlowName'] = flow_name
        row['FlowAmount'] = usd
        row['Unit'] = 'USD'
        row['Class'] = 'Money'
        row['Location'] = US_FIPS
        row['ActivityProducedBy'] = produced
        row['ActivityConsumedBy'] = consumed
        row['Description'] = (
            f'Table 2.14 national {direction}: EIA MWh × Census HS '
            f'{HS2716_ELECTRICAL_ENERGY} unit value'
        )
        rows.append(row)

    if not rows:
        raise ValueError(
            f'{fba.full_name}: no Table 2.14 electricity trade rows to dollarize'
        )
    out = pd.concat(rows, ignore_index=True)
    return FlowByActivity(
        out,
        full_name=fba.full_name,
        config=fba.config,
        convert_df_to_flowby=True,
    )


if __name__ == '__main__':
    import pandas as pd

    from bedrock.extract.census.Census_USATrade import (
        refresh_hs2716_electricity_unit_value_csv,
    )

    written = refresh_hs2716_electricity_unit_value_csv()
    print(f'Wrote {written}')
    print(pd.read_csv(written).to_string(index=False))
