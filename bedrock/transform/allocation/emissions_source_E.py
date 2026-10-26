"""Source-resolved USA E for snapshots (ceda EmissionsSource x industries).

Until the FBS → 123-source crosswalk lands (#1135),
:func:`derive_E_usa_emissions_sources` returns a **zero** matrix with a stub
index of length 123 so snapshot wiring and shapes can land independently.
"""

from __future__ import annotations

import logging

import pandas as pd

from bedrock.transform.eeio.sector_disagg_pipeline import (
    electricity_reaggregation_enabled,
)
from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.schemas.cornerstone_schemas import (
    CORNERSTONE_INDUSTRIES,
    CORNERSTONE_INDUSTRIES_ELEC,
)
from bedrock.utils.taxonomy.cornerstone.industries import INDUSTRIES

logger = logging.getLogger(__name__)

# Placeholder row labels until Mo's crosswalk supplies ceda EmissionsSource codes.
# Length matches ceda's EmissionsSource enum (123).
N_CEDA_EMISSIONS_SOURCES = 123
STUB_EMISSIONS_SOURCE_INDEX: tuple[str, ...] = tuple(
    f'stub_emissions_source_{i:03d}' for i in range(N_CEDA_EMISSIONS_SOURCES)
)

E_USA_ES_LONG_COLUMNS: tuple[str, ...] = (
    'emissions_source',
    'gas',
    'sector',
    'kg',
    'co2e_ar5',
    'co2e_ar6',
)


def _published_industry_columns() -> list[str]:
    usa = get_usa_config()
    if (
        usa.implement_electricity_disaggregation
        and not electricity_reaggregation_enabled()
    ):
        return [str(s) for s in CORNERSTONE_INDUSTRIES_ELEC]
    return [str(s) for s in INDUSTRIES]


def derive_E_usa_emissions_sources() -> pd.DataFrame:
    """Industry E by ceda emissions source (sources x industries), AR6 kg CO2e.

    Stub: all zeros on :data:`STUB_EMISSIONS_SOURCE_INDEX` until the crosswalk
    fills real ``EmissionsSource`` rows. Column set matches published gas E
    (405 industries, or 407 when electricity disaggregation is on without
    reaggregation).
    """
    columns = _published_industry_columns()
    if electricity_reaggregation_enabled():
        columns = [str(s) for s in CORNERSTONE_INDUSTRIES]

    E = pd.DataFrame(
        0.0,
        index=pd.Index(STUB_EMISSIONS_SOURCE_INDEX, name='emissions_source'),
        columns=columns,
    )
    logger.info(
        'derive_E_usa_emissions_sources: stub zeros (%s x %s); crosswalk not wired',
        E.shape[0],
        E.shape[1],
    )
    return E


def derive_E_usa_es_long() -> pd.DataFrame:
    """Long source x gas x sector table (kg + AR5/AR6 CO2e) for GWP rebuild.

    Stub: empty frame with the agreed columns until the crosswalk lands.
    """
    return pd.DataFrame(columns=list(E_USA_ES_LONG_COLUMNS))


def derive_E_usa_gas() -> pd.DataFrame:
    """Published industry E by GHG (7 x industries) — former ``E_USA_ES`` content."""
    from bedrock.transform.allocation.derived import derive_E_usa  # noqa: PLC0415

    return derive_E_usa()
