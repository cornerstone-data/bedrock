"""Source-resolved USA E for snapshots (ceda EmissionsSource x industries).

**Draft (#1134).** Crosswalk-only fill-in on retained ``ActivityProducedBy``
plus HFC speciation. Nested-split shares and method YAML breakouts are not
wired: soils / NEU / aviation / res-com stay lumped and are often
``bundled`` / unmatched, so they are omitted from ``E_USA_ES``. Coverage is
intentionally incomplete until
``.cursor/plan/10_21_retained_activity_fbs_pipeline.plan.md`` bake-in lands.

``E_USA_GAS`` is unchanged (7 GHG x industries). Do not publish CEDA-bound
bags until ceda readers switch off gas-shaped ``E_USA_ES``.
"""

from __future__ import annotations

import functools
import logging

import pandas as pd

from bedrock.transform.allocation.emissions_source_crosswalk import (
    add_co2e_columns,
    load_crosswalk,
    load_emissions_sources,
    speciate_unspecified_hfcs,
    tag_fbs_rows,
)
from bedrock.transform.eeio.sector_disagg_pipeline import (
    collapse_electricity_children_columns,
    electricity_reaggregation_enabled,
)
from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.schemas.cornerstone_schemas import (
    CORNERSTONE_INDUSTRIES,
    CORNERSTONE_INDUSTRIES_ELEC,
)
from bedrock.utils.taxonomy.cornerstone.industries import INDUSTRIES

logger = logging.getLogger(__name__)

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
    if electricity_reaggregation_enabled():
        return [str(s) for s in CORNERSTONE_INDUSTRIES]
    return [str(s) for s in INDUSTRIES]


@functools.cache
def _tagged_export_fbs() -> pd.DataFrame:
    """Mapped FBS rows on model-schema sectors with CO2e columns."""
    from bedrock.transform.allocation.derived import (  # noqa: PLC0415
        _load_cornerstone_ghg_fbs_from_gcs,
        _select_cornerstone_ghg_fbs_base_name,
        map_fbs_sectors_to_model_schema,
        resplit_waste_use_emissions,
    )
    from bedrock.utils.emissions.ch4_classification import (  # noqa: PLC0415
        apply_ch4_non_fossil_flowable,
    )

    fbs = _load_cornerstone_ghg_fbs_from_gcs(
        base_name=_select_cornerstone_ghg_fbs_base_name()
    )
    if 'ActivityProducedBy' not in fbs.columns:
        raise ValueError(
            'Cornerstone GHG FBS lacks ActivityProducedBy; retained-activity '
            'pin required for emissions-source export'
        )

    # Draft: no nested-split shares; crosswalk on retained activities as-is.
    fbs = speciate_unspecified_hfcs(fbs)
    fbs = map_fbs_sectors_to_model_schema(fbs)
    fbs = resplit_waste_use_emissions(fbs)
    fbs = tag_fbs_rows(fbs, load_crosswalk())
    apply_ch4_non_fossil_flowable(fbs)
    fbs = add_co2e_columns(fbs)

    by_status = fbs.groupby('status', dropna=False)['co2e_ar6'].sum() / 1e9
    logger.info(
        'emissions-source export CO2e by crosswalk status (Mt AR6):\n%s',
        by_status.sort_values(ascending=False).round(3).to_string(),
    )
    return fbs


def _mapped_rows() -> pd.DataFrame:
    tagged = _tagged_export_fbs()
    mapped = tagged[tagged['status'] == 'mapped'].copy()
    if mapped['ceda_source'].str.contains(';').any():
        raise ValueError('mapped rows must not list multiple ceda_source codes')
    return mapped


@functools.cache
def derive_E_usa_emissions_sources() -> pd.DataFrame:
    """Industry E by ceda emissions source (sources x industries), AR6 kg CO2e.

    Draft: only ``status == mapped`` rows after the analysis crosswalk; no
    nested splits. See module docstring.
    """
    sources = load_emissions_sources()
    columns = _published_industry_columns()
    mapped = _mapped_rows()

    E = mapped.pivot_table(
        index='ceda_source',
        columns='SectorProducedBy',
        values='co2e_ar6',
        aggfunc='sum',
        fill_value=0.0,
    )
    if electricity_reaggregation_enabled():
        E = collapse_electricity_children_columns(E, col_codes=CORNERSTONE_INDUSTRIES)

    E = E.reindex(index=list(sources['code']), columns=columns, fill_value=0.0)
    E.index.name = 'emissions_source'
    E.columns.name = None
    logger.info(
        'derive_E_usa_emissions_sources (draft): %s x %s; total %.1f Mt AR6 '
        'CO2e (crosswalk-only; nested splits not applied)',
        E.shape[0],
        E.shape[1],
        float(E.to_numpy().sum()) / 1e9,
    )
    return E


@functools.cache
def derive_E_usa_es_long() -> pd.DataFrame:
    """Long source x gas x sector table (kg + AR5/AR6 CO2e) for GWP rebuild.

    Draft: same mapped-row set as :func:`derive_E_usa_emissions_sources`.
    """
    mapped = _mapped_rows()
    columns = _published_industry_columns()
    long = (
        mapped.groupby(['ceda_source', 'Flowable', 'SectorProducedBy'], observed=True)
        .agg(
            kg=('kg', 'sum'),
            co2e_ar5=('co2e_ar5', 'sum'),
            co2e_ar6=('co2e_ar6', 'sum'),
        )
        .reset_index()
        .rename(
            columns={
                'ceda_source': 'emissions_source',
                'Flowable': 'gas',
                'SectorProducedBy': 'sector',
            }
        )
    )
    if electricity_reaggregation_enabled():
        child = {'221110', '221120', '221130'}
        mask = long['sector'].isin(child)
        if mask.any():
            collapsed = (
                long[mask]
                .assign(sector='221100')
                .groupby(['emissions_source', 'gas', 'sector'], observed=True)
                .sum(numeric_only=True)
                .reset_index()
            )
            long = pd.concat([long[~mask], collapsed], ignore_index=True)

    long = long[long['sector'].isin(columns)].reset_index(drop=True)
    return long.reindex(columns=list(E_USA_ES_LONG_COLUMNS))


def derive_E_usa_gas() -> pd.DataFrame:
    """Published industry E by GHG (7 x industries) -- former ``E_USA_ES`` content."""
    from bedrock.transform.allocation.derived import derive_E_usa  # noqa: PLC0415

    return derive_E_usa()
