from __future__ import annotations

import logging
import time

import pandas as pd

from bedrock.transform.allocation.constants import EmissionsSource
from bedrock.transform.allocation.registry import ALLOCATED_EMISSIONS_REGISTRY
from bedrock.transform.flowbysector import FlowBySector, getFlowBySector
from bedrock.utils.config.common import load_crosswalk
from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.emissions.ghg import GHG_MAPPING
from bedrock.utils.emissions.gwp import GWP100_AR6_CEDA
from bedrock.utils.mapping.sectormapping import (
    get_activitytosector_mapping,
)
from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from bedrock.utils.taxonomy.cornerstone.industries import INDUSTRIES
from bedrock.utils.taxonomy.correspondence import create_correspondence_matrix
from bedrock.utils.taxonomy.mappings.bea_v2017_industry__bea_v2017_commodity import (
    load_bea_v2017_industry_to_bea_v2017_commodity,
)

logger = logging.getLogger(__name__)


def derive_E_usa() -> pd.DataFrame:
    if get_usa_config().load_E_from_flowsa:
        # Return E_usa (ghg × CEDA v7 sectors). Branches on config load_E_from_flowsa.
        # TODO: update future FBS calls with if else gating here
        return load_E_from_flowsa()
    else:
        # aggregate E from 15 gases to 7 gases
        return create_correspondence_matrix(GHG_MAPPING).T @ derive_E_usa_by_gas()


def derive_E_usa_by_gas() -> pd.DataFrame:
    return (
        derive_E_usa_emissions_sources()
        .groupby(lambda es: EmissionsSource(es).gas, axis=0)  # type: ignore
        .sum()
    )


def derive_E_usa_emissions_sources() -> pd.DataFrame:
    if get_usa_config().use_cornerstone_2026_model_schema:
        target_columns: list[str] = [str(sector) for sector in INDUSTRIES]
    else:
        target_columns = [str(sector) for sector in CEDA_V7_SECTORS]
    E_usa = pd.DataFrame(
        0.0,
        index=[es.value for es in EmissionsSource],
        columns=target_columns,
    )

    total_start = time.time()
    for es, allocator in ALLOCATED_EMISSIONS_REGISTRY.items():
        logger.info(f"Allocating {es}")
        allocated = allocator()
        if allocated.isna().any():
            raise ValueError(f"NaNs found in {es} allocator")
        E_usa.loc[es.value, :] += allocated.reindex(target_columns, fill_value=0.0)

    logger.info(
        f"[TIMING] All {len(ALLOCATED_EMISSIONS_REGISTRY)} allocations completed in {time.time() - total_start:.1f}s"
    )

    return E_usa


def map_to_CEDA(fbs: pd.DataFrame) -> pd.DataFrame:
    """Map FBS sectors from NAICS to CEDA v7 sectors."""
    # Because the schema for the FBS is mixed digit, first need to expand the schema all the way
    # to 6 digits prior to mapping back to the CEDA schema. In doing this mapping we only need
    # to assign a 1:1 mapping (hence drop duplicates, keep = first). When the mapping is reversed
    # back to CEDA we don't want to expand the FBS.

    # Prepare NAICS:BEA mapping file
    cw = load_crosswalk('NAICS_2017_Crosswalk')
    cols_to_stack = ["NAICS_3", "NAICS_4", "NAICS_5"]
    cw_stack = (
        cw.astype({c: "string" for c in cols_to_stack + ["NAICS_6"]})
        .melt(
            id_vars="NAICS_6",
            value_vars=cols_to_stack,
            var_name="level",
            value_name="NAICS",
        )
        .dropna(subset=["NAICS_6", "NAICS"])[["NAICS", "NAICS_6"]]
        .drop_duplicates(subset='NAICS', keep='first')
        .reset_index(drop=True)
    )
    fbs2 = fbs.merge(
        cw_stack,
        how='left',
        left_on='SectorProducedBy',
        right_on='NAICS',
        validate="m:1",
    )
    fbs2['NAICS_6'] = fbs2['NAICS_6'].fillna(fbs2['SectorProducedBy'])

    if get_usa_config().use_cornerstone_2026_model_schema:
        mapping = get_activitytosector_mapping('Cornerstone_2025').drop_duplicates(
            subset='Sector', keep='first'
        )
    else:
        mapping = (
            get_activitytosector_mapping('CEDA_2025')
            # we don't want to map back to the sectors that are aggregated so keep only first
            # this assumes that the first listed mapping is the priority.
            # TODO: update to rely on the reported CEDA schema.
            .drop_duplicates(subset='Sector', keep='first')
        )
    fbs2 = (
        fbs2.merge(
            mapping[['Activity', 'Sector']],
            how='left',
            left_on='NAICS_6',
            right_on=['Sector'],
            validate="m:1",
        )
        .assign(SectorProducedBy=lambda x: x['Activity'].fillna(x['NAICS_6']))
        .drop(columns=['Activity', 'NAICS', 'NAICS_6', 'Sector'])
    )

    ## re assign SPB and aggregate using exisiting functions
    fbs3 = pd.DataFrame(FlowBySector(fbs2).aggregate_flowby())

    # TODO: add test to confirm no data loss

    return fbs3


def _load_cornerstone_ghg_fbs_from_gcs(year: int) -> pd.DataFrame:
    """Download a year-specific Cornerstone GHG FBS parquet from GCS.

    Bypasses ``getFlowBySector`` for the time-series case. The flowsa
    regen path goes through `EPA_GHGI` loaders that are hard-capped at
    `{2022, 2023}` (`bedrock/extract/allocation/epa.py:_get_epa_data_year`),
    so years like 2019–2021 fail there. The pre-built FBS parquets in
    ``gs://cornerstone-default/transform/output_data/`` cover 2019–2023
    already, so we load them directly.

    Picks the most-recently-uploaded parquet whose ``base_name`` matches
    ``GHG_national_Cornerstone_<year>`` so we follow the FBS regeneration
    cadence without pinning the version/hash here.
    """
    from bedrock.utils.io.gcp import (  # noqa: PLC0415
        download_gcs_file_if_not_exists,
        list_bucket_files,
    )

    sub_bucket = "transform/output_data"
    base_name = f"GHG_national_Cornerstone_{year}"
    bucket_df = list_bucket_files(sub_bucket)
    matches = bucket_df[
        (bucket_df["base_name"] == base_name) & (bucket_df["extension"] == ".parquet")
    ].sort_values("created", ascending=False)
    if matches.empty:
        raise FileNotFoundError(
            f"No FBS parquet found at gs://cornerstone-default/{sub_bucket}/ "
            f"matching base_name={base_name!r}"
        )
    filename = matches.iloc[0]["full_path"].rsplit("/", 1)[-1]
    from bedrock.utils.config.settings import FBS_DIR  # noqa: PLC0415

    local_path = str(FBS_DIR / filename)
    download_gcs_file_if_not_exists(filename, sub_bucket, local_path)
    logger.info("Loaded cached FBS for %d from %s", year, filename)
    return pd.read_parquet(local_path)


def load_E_from_flowsa() -> pd.DataFrame:
    """Load E_usa (GHG × CEDA v7 sectors) from the CEDA FBS.

    FBS method is chosen by USA config (first match wins):
    - GHG_national_Cornerstone_2023 when
      new_ghg_method is True
    - GHG_national_Cornerstone_2023_coa_allocation when update_ghg_coa_allocation is True
    - GHG_national_Cornerstone_2023_electricity when
      update_electricity_ghg_method is True
    - GHG_national_Cornerstone_2023_petroleum_natgas when
      update_ghg_attribution_method_for_ng_and_petrol_systems is True
    - GHG_national_Cornerstone_2023_refrigerants_foams when
      update_flowsa_refrigerant_method is True
    - GHG_national_Cornerstone_2023_new_activities when
      add_new_ghg_activities is True
    - GHG_national_Cornerstone_2023_other_gases when
      update_other_gases_ghg_method is True
    - GHG_national_Cornerstone_2023_mobile_combustion when update_transportation_ghg_method is True
    - GHG_national_Cornerstone_2023_ag_livestock when
      update_enteric_fermentation_and_manure_management_ghg_method is True
    - GHG_national_Cornerstone_2023_ag_soils when
      update_liming_and_fertilizer_ghg_method is True
    - GHG_national_CEDA_2023 otherwise

    Only used when load_E_from_flowsa is True in USA config.
    """
    usa = get_usa_config()
    year = usa.usa_ghg_data_year
    # Only the base `new_ghg_method` and CEDA fallback FBS methods exist
    # for years other than 2023. The variant FBSes (`*_coa_allocation`,
    # `*_electricity`, etc.) are 2023-only; raise here rather than failing
    # with an opaque "FBS not found" later.
    needs_2023 = (
        usa.update_ghg_coa_allocation
        or usa.update_electricity_ghg_method
        or usa.update_other_gases_ghg_method
        or usa.update_ghg_attribution_method_for_ng_and_petrol_systems
        or usa.update_flowsa_refrigerant_method
        or usa.update_transportation_ghg_method
        or usa.add_new_ghg_activities
        or usa.update_enteric_fermentation_and_manure_management_ghg_method
        or usa.update_liming_and_fertilizer_ghg_method
    )
    if needs_2023 and year != 2023:
        raise ValueError(
            f'usa_ghg_data_year={year} is incompatible with the active '
            'update_*_ghg_method flag — variant FBS methods only exist '
            'for 2023. Either set usa_ghg_data_year=2023 or disable the '
            'update_*_ghg_method flag.'
        )
    if usa.new_ghg_method:
        # Bypass flowsa regen for non-2023 years: the EPA loader behind
        # `getFlowBySector` is hard-capped at {2022, 2023}, but the
        # already-built FBS parquets exist on GCS at
        # `transform/output_data/` for 2019–2023. Load the cached parquet
        # directly so the year-Y diagnostics get year-Y GHG data.
        fbs = _load_cornerstone_ghg_fbs_from_gcs(year)
    else:
        if usa.update_ghg_coa_allocation:
            methodname = 'GHG_national_Cornerstone_2023_coa_allocation'
        elif usa.update_electricity_ghg_method:
            methodname = 'GHG_national_Cornerstone_2023_electricity'
        elif usa.update_other_gases_ghg_method:
            methodname = 'GHG_national_Cornerstone_2023_other_gases'
        elif usa.update_ghg_attribution_method_for_ng_and_petrol_systems:
            methodname = 'GHG_national_Cornerstone_2023_petroleum_natgas'
        elif usa.update_flowsa_refrigerant_method:
            methodname = "GHG_national_Cornerstone_2023_refrigerants_foams"
        elif usa.update_transportation_ghg_method:
            methodname = 'GHG_national_Cornerstone_2023_mobile_combustion'
        elif usa.add_new_ghg_activities:
            methodname = 'GHG_national_Cornerstone_2023_new_activities'
        elif usa.update_enteric_fermentation_and_manure_management_ghg_method:
            methodname = 'GHG_national_Cornerstone_2023_ag_livestock'
        elif usa.update_liming_and_fertilizer_ghg_method:
            methodname = 'GHG_national_Cornerstone_2023_ag_soils'
        else:
            methodname = f'GHG_national_CEDA_{year}'
        fbs = getFlowBySector(methodname=methodname)

    fbs = map_to_CEDA(fbs)

    # Align flow names with temporary mapping
    gas_map = {
        # CO2
        'Carbon dioxide': 'CO2',
        # CH4
        'Methane': 'CH4_fossil',
        # N2O
        'Nitrous oxide': 'N2O',
        # NF3
        'Nitrogen trifluoride': 'NF3',
        # SF6
        'Sulfur hexafluoride': 'SF6',
        # HFCs (all beginning with HFC- or explicitly HFC)
        'HFC, PFC and SF6 F-HTFs': 'HFCs',  # mixed basket → assign to HFCs?
        # 'HFC-125': 'HFCs',
        # 'HFC-134a': 'HFCs',
        # 'HFC-143a': 'HFCs',
        # 'HFC-227ea': 'HFCs',
        # 'HFC-23': 'HFCs',
        # 'HFC-236fa': 'HFCs',
        # 'HFC-32': 'HFCs',
        'HFCs and PFCs, unspecified': 'HFCs',  # ambiguous → can also map to 'PFCs'
        # PFCs
        'Carbon tetrafluoride': 'CF4',
        'Hexafluoroethane': 'C2F6',
        'PFC': 'PFCs',
        'Perfluorocyclobutane': 'c-C4F8',
        'Perfluoropropane': 'C3F8',
    }
    fbs['Flowable'] = fbs['Flowable'].map(gas_map).fillna(fbs['Flowable'])

    # CH4: use CH4_non_fossil when meta source is table 5_* or when in 2_1 and sector starts with 1 or 562 or 2213
    # to align with CH4_NON_FOSSIL defined in extract/allocation/epa.py
    meta = fbs['MetaSources'].astype(str)
    sector = fbs['SectorProducedBy'].astype(str)
    ch4_non_fossil_mask = meta.str.contains('_5_', regex=False, na=False) | (
        meta.str.contains('2_1', regex=False, na=False)
        & sector.str.match(r'^(1|562|2213)', na=False)
    )
    fbs.loc[ch4_non_fossil_mask & (fbs['Flowable'] == 'CH4_fossil'), 'Flowable'] = (
        'CH4_non_fossil'
    )

    # Convert values to CO2e
    ghg_mapping: dict[str, float] = {k: v for k, v in GWP100_AR6_CEDA.items()}
    ghg_mapping['HFCs'] = 1  # should already be in CO2e
    ghg_mapping['PFCs'] = 1  # should already be in CO2e
    fbs['CO2e'] = fbs['FlowAmount'] * fbs['Flowable'].map(ghg_mapping)

    # fbs.to_csv('GHG_CEDA_fbs_bea.csv')

    # aggregate and set FlowName as index, sectors as columns
    E_usa = fbs.pivot_table(
        index='Flowable',
        columns='SectorProducedBy',
        values='CO2e',
        aggfunc='sum',
        fill_value=0,
    )

    # Collapse across flows
    reverse = {m: g for g, members in GHG_MAPPING.items() for m in members}
    # some flows are not in GHG_MAPPING for some reason
    reverse['HFC-227ea'] = 'HFCs'
    reverse['c-C4F8'] = 'PFCs'
    reverse['CH4_fossil'] = 'CH4'
    reverse['CH4_non_fossil'] = 'CH4'
    new_index = E_usa.index.map(lambda x: reverse.get(x, x))
    E_usa = E_usa.groupby(new_index).agg('sum')

    # Collapse across sectors (when CEDA: group BEA→CEDA; when Cornerstone: already in schema)
    if get_usa_config().use_cornerstone_2026_model_schema:
        target_columns = [str(sector) for sector in INDUSTRIES]
        # E_usa already has Cornerstone columns from derive_E_usa_emissions_sources
        E_usa = E_usa.reindex(columns=target_columns, fill_value=0)
    else:
        mapping = load_bea_v2017_industry_to_bea_v2017_commodity()
        target_columns = [str(sector) for sector in CEDA_V7_SECTORS]
        col_to_target = {k: v[0] for k, v in mapping.items()}
        for c in E_usa.columns:
            if c not in col_to_target and c in target_columns:
                col_to_target[c] = c  # type: ignore
        dropped_by_groupby = sorted(set(E_usa.columns) - set(col_to_target.keys()))
        if dropped_by_groupby:
            logger.warning(
                "E_usa columns with no mapping (dropped by groupby): %s",
                dropped_by_groupby,
            )
        E_usa = E_usa.groupby(col_to_target, axis=1).sum()  # type: ignore
        target_set = set(target_columns)
        extra = sorted(set(E_usa.columns) - target_set)
        missing = sorted(target_set - set(E_usa.columns))
        if extra:
            logger.warning(
                "E_usa columns not in target schema (will be dropped by reindex): %s",
                extra,
            )
        if missing:
            logger.debug(
                "Target schema columns missing from E_usa (will be filled with 0): %s",
                missing,
            )
        E_usa = E_usa.reindex(columns=target_columns, fill_value=0)

    return E_usa


if __name__ == "__main__":
    from bedrock.utils.config.usa_config import set_global_usa_config

    set_global_usa_config("2025_usa_cornerstone_taxonomy_and_waste_disagg.yaml")
    df1 = load_E_from_flowsa()
    # df2 = derive_E_usa()
    # row_diff = df1.sum(axis=1) - df2.sum(axis=1)
    # row_rel_diff = df1.sum(axis=1) / df2.sum(axis=1)
