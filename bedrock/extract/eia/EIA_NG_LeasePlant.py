"""EIA U.S. natural gas lease and plant fuel consumption (annual MMcf).

Open Data v2 ``natural-gas/cons/sum``, process ``VGL`` (Lease and Plant Fuel
Consumption), ``duoarea=NUS``. Same accounting as Natural Gas Annual / the
end-use consumption table that #980 cites (~1,879 Bcf in 2022).

Volume ships as ``MMCF``; ``unit_conversion.csv`` converts to ``MMT CO2e`` with
the EPA stationary NG factor (54.44 kg CO2/Mcf) so this FBA can enter a GHG
method and interact with UMD table 3-11 after both are sector-standardized.

⚠️ Transmission / pipeline fuel is a different process and is **not** included
— the inventory books that under transportation, not industrial lease/plant.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from bedrock.transform.flowbyfunctions import assign_fips_location_system
from bedrock.utils.logging.flowsa_log import log
from bedrock.utils.mapping.location import US_FIPS


def eia_ng_lease_plant_url_helper(*, build_url: str, year: str, **_: Any) -> list[str]:
    """Annual US lease-and-plant NG consumption for ``year``.

    ⚠️ **The key arrives inside ``build_url``, not in ``config``.**
    ``generateflowbyactivity`` substitutes ``__apiKey__`` into the yaml's
    ``base_url`` before calling this, so the key has to be read back out of it.
    """
    key = build_url.partition('api_key=')[2].partition('&')[0]
    yr = int(year)
    return [
        (
            f'https://api.eia.gov/v2/natural-gas/cons/sum/data/?api_key={key}'
            f'&frequency=annual&data[0]=value'
            f'&facets[duoarea][]=NUS'
            f'&facets[process][]=VGL'
            f'&start={yr}&end={yr}&length=50'
        )
    ]


def eia_ng_lease_plant_call(*, resp: Any, **_: Any) -> pd.DataFrame:
    """The v2 response's ``response.data`` rows."""
    payload = resp.json().get('response', {})
    return pd.DataFrame(payload.get('data', []))


def eia_ng_lease_plant_parse(
    *, df_list: list[pd.DataFrame], year: int, **_: Any
) -> pd.DataFrame:
    """One national row: lease and plant fuel volume (MMCF → MMT CO2e on load)."""
    frame = pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()
    if frame.empty:
        log.warning(f'EIA_NG_LeasePlant {year}: no rows returned for process VGL')
        return frame

    rows = frame[frame['duoarea'].astype(str) == 'NUS'].copy()
    unit_column = 'units' if 'units' in rows.columns else 'unit'
    rows['FlowAmount'] = pd.to_numeric(rows['value'], errors='coerce')
    missing = int(rows['FlowAmount'].isna().sum())
    rows = rows[rows['FlowAmount'].notna()].copy()

    long = pd.DataFrame(
        {
            'FlowName': 'CO2',
            'FlowAmount': rows['FlowAmount'].astype(float),
            'Unit': rows[unit_column].astype(str),
            'Description': 'U.S. natural gas lease and plant fuel consumption',
            'ActivityProducedBy': 'Lease and Plant Fuel',
            'ActivityConsumedBy': None,
            'Year': int(year),
            'Location': US_FIPS,
            'Class': 'Chemicals',
            'FlowType': 'ELEMENTARY_FLOW',
            'SourceName': 'EIA_NG_LeasePlant',
            'DataReliability': 5,
            'DataCollection': 5,
            'Compartment': 'air',
        }
    )
    long = assign_fips_location_system(long, year)

    mmcf = float(long['FlowAmount'].sum()) if not long.empty else 0.0
    log.info(
        f'EIA_NG_LeasePlant {year}: {mmcf:,.0f} MMcf lease and plant fuel '
        f'({len(long)} row(s); {missing} dropped as non-numeric)'
    )
    return long
