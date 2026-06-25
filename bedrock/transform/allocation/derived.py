from __future__ import annotations

import logging
import time

import pandas as pd

from bedrock.transform.allocation.constants import EmissionsSource
from bedrock.transform.allocation.registry import ALLOCATED_EMISSIONS_REGISTRY
from bedrock.transform.flowbysector import FlowBySector, getFlowBySector
from bedrock.transform.iot.derived_gross_industry_output import derive_gross_output
from bedrock.utils.config.common import load_crosswalk
from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.emissions.ghg import GHG_MAPPING
from bedrock.utils.emissions.gwp import GWP100_AR6_CEDA
from bedrock.utils.mapping.sectormapping import (
    get_activitytosector_mapping,
)
from bedrock.utils.schemas.cornerstone_schemas import CORNERSTONE_INDUSTRIES_ELEC
from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from bedrock.utils.taxonomy.cornerstone.industries import (
    INDUSTRIES,
    WASTE_DISAGG_INDUSTRIES,
)
from bedrock.utils.taxonomy.correspondence import create_correspondence_matrix
from bedrock.utils.taxonomy.mappings.bea_v2017_industry__bea_v2017_commodity import (
    load_bea_v2017_industry_to_bea_v2017_commodity,
)

logger = logging.getLogger(__name__)

# USEEIO does not distinguish fossil from non-fossil CH4
_USEEIO_WORKBOOK_CH4_GWP = 27.9


def _select_flowsa_ghg_method() -> str:
    """Select FBS methodname from USA config (first match wins).

    The base `new_ghg_method` and CEDA fallback methods are parameterized on
    `usa_ghg_data_year`. The 2023-only variants (`*_ghgi_mecs`, `*_umd_ghgia`)
    raise here if set with a non-2023 year rather than failing later with an
    opaque "FBS not found".
    """
    usa = get_usa_config()
    year = usa.usa_ghg_data_year
    needs_2023 = usa.update_mecs_method or usa.v0_3_umd_2023_ghgia
    if needs_2023 and year != 2023:
        raise ValueError(
            f'usa_ghg_data_year={year} is incompatible with the active '
            'update_*_ghg_method flag — variant FBS methods only exist '
            'for 2023. Either set usa_ghg_data_year=2023 or disable the '
            'update_*_ghg_method flag.'
        )
    if usa.v0_3_umd_2024_ghgia and year != 2024:
        raise ValueError(
            f'usa_ghg_data_year={year} is incompatible with v0_3_umd_2024_ghgia '
            '— the 2024 UMD GHGIA FBS only exists for 2024. Set '
            'usa_ghg_data_year=2024 or use v0_3_umd_2023_ghgia.'
        )
    if usa.new_ghg_method:
        return f'GHG_national_Cornerstone_{year}'
    if usa.use_ghg_national_2023_m2:
        return 'GHG_national_2023_m2'
    if usa.update_mecs_method:
        return 'GHG_national_Cornerstone_2023_ghgi_mecs'
    if usa.v0_3_umd_2023_ghgia:
        return 'GHG_national_Cornerstone_2023_umd_ghgia'
    if usa.v0_3_umd_2024_ghgia:
        return 'GHG_national_Cornerstone_2024'
    return f'GHG_national_CEDA_{year}'


def _build_mapping_with_allocations(
    mapping: pd.DataFrame, *, use_output_weights: bool
) -> pd.DataFrame:
    """Return Sector->Activity mapping with an Allocation column.

    When ``use_output_weights`` is True, one-to-many Sector mappings are split
    using gross industry output shares for ``usa_ghg_data_year``.
    """
    mapping2 = mapping[['Activity', 'Sector']].dropna().copy()
    if not use_output_weights:
        return (
            mapping2.drop_duplicates(subset='Sector', keep='first')
            .assign(Allocation=1.0)
            .reset_index(drop=True)
        )
    cfg = get_usa_config()
    go = derive_gross_output(
        target_year=cfg.usa_ghg_data_year,
        iot_before_or_after_redefinition=cfg.iot_before_or_after_redefinition,
    )
    mapping2['Output'] = mapping2['Activity'].map(go)
    mapping2['Output'] = mapping2['Output'].fillna(0.0)

    group_sum = mapping2.groupby('Sector')['Output'].transform('sum')
    group_size = mapping2.groupby('Sector')['Sector'].transform('size')
    bad_one_to_many = (group_size > 1) & (group_sum <= 0)
    if bad_one_to_many.any():
        bad_sectors = sorted(mapping2.loc[bad_one_to_many, 'Sector'].dropna().unique())
        raise ValueError(
            'Missing/zero gross output for one-to-many weighted mapping sectors: '
            f'{bad_sectors[:20]}'
        )

    mapping2['Allocation'] = 0.0
    valid_weight = group_sum > 0
    mapping2.loc[valid_weight, 'Allocation'] = (
        mapping2.loc[valid_weight, 'Output'] / group_sum.loc[valid_weight]
    )
    mapping2.loc[~valid_weight, 'Allocation'] = 1.0 / group_size.loc[~valid_weight]

    return mapping2[['Activity', 'Sector', 'Allocation']].reset_index(drop=True)


def _should_use_output_weighted_mapping() -> bool:
    return bool(get_usa_config().use_ghg_national_2023_m2)


def _apply_electricity_disagg_cornerstone_mapping(
    mapping: pd.DataFrame,
) -> pd.DataFrame:
    """Retarget electric-power NAICS to disaggregated Cornerstone sectors."""
    mapping = mapping.copy()
    gen_naics = {f'22111{i}' for i in range(1, 9)}
    mapping.loc[mapping['Sector'].isin(gen_naics), 'Activity'] = '221110'
    mapping.loc[mapping['Sector'] == '221121', 'Activity'] = '221121'
    mapping.loc[mapping['Sector'] == '221122', 'Activity'] = '221122'
    return mapping


def _apply_cornerstone_waste_overrides(mapping: pd.DataFrame) -> pd.DataFrame:
    """Override waste NAICS mappings with Cornerstone waste-disaggregated targets."""
    waste_targets = set(WASTE_DISAGG_INDUSTRIES['562000'])
    cs_mapping = _build_mapping_with_allocations(
        get_activitytosector_mapping('Cornerstone_2025'),
        use_output_weights=False,
    )[['Sector', 'Activity']].dropna()
    waste_override = cs_mapping[
        cs_mapping['Sector'].str.startswith('562')
        & cs_mapping['Activity'].isin(waste_targets)
    ].drop_duplicates()
    waste_naics = set(waste_override['Sector'])
    return pd.concat(
        [mapping[~mapping['Sector'].isin(waste_naics)], waste_override],
        ignore_index=True,
    ).drop_duplicates()


def _build_naics_to_bea_weighted_mapping() -> pd.DataFrame:
    """Build NAICS->BEA mapping weighted by gross output for GHG year.

    When waste disaggregation is enabled, start from the NAICS->BEA crosswalk and
    override waste NAICS rows with the Cornerstone waste disaggregation.
    """
    cw = load_crosswalk('NAICS_to_BEA_Crosswalk_2017')
    mapping = cw.rename(
        columns={
            'NAICS_2017_Code': 'Sector',
            'BEA_2017_Detail_Code': 'Activity',
        }
    )[['Sector', 'Activity']]
    mapping = mapping.dropna().drop_duplicates().astype('string')
    if get_usa_config().implement_waste_disaggregation:
        mapping = _apply_cornerstone_waste_overrides(mapping)

    cfg = get_usa_config()
    go = derive_gross_output(
        target_year=cfg.usa_ghg_data_year,
        iot_before_or_after_redefinition=cfg.iot_before_or_after_redefinition,
    )
    mapping['Output'] = mapping['Activity'].map(go).fillna(0.0)

    group_sum = mapping.groupby('Sector')['Output'].transform('sum')
    group_size = mapping.groupby('Sector')['Sector'].transform('size')
    bad_one_to_many = (group_size > 1) & (group_sum <= 0)
    if bad_one_to_many.any():
        bad_sectors = sorted(mapping.loc[bad_one_to_many, 'Sector'].dropna().unique())
        raise ValueError(
            'Missing/zero gross output for one-to-many weighted NAICS->BEA sectors: '
            f'{bad_sectors[:20]}'
        )

    mapping['Allocation'] = 0.0
    valid_weight = group_sum > 0
    mapping.loc[valid_weight, 'Allocation'] = (
        mapping.loc[valid_weight, 'Output'] / group_sum.loc[valid_weight]
    )
    mapping.loc[~valid_weight, 'Allocation'] = 1.0 / group_size.loc[~valid_weight]

    return mapping[['Sector', 'Activity', 'Allocation']].reset_index(drop=True)


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
        logger.info(f'Allocating {es}')
        allocated = allocator()
        if allocated.isna().any():
            raise ValueError(f'NaNs found in {es} allocator')
        E_usa.loc[es.value, :] += allocated.reindex(target_columns, fill_value=0.0)

    logger.info(
        f'[TIMING] All {len(ALLOCATED_EMISSIONS_REGISTRY)} allocations completed in {time.time() - total_start:.1f}s'
    )

    return E_usa


def map_fbs_sectors_to_model_schema(fbs: pd.DataFrame) -> pd.DataFrame:
    """Map FBS NAICS sectors into the active model schema.

    Behavior differs by path:
    - Weighted path (`use_ghg_national_2023_m2`): preserve original NAICS code
      levels (e.g., `53`, `531`, `531110`) and apply weighted NAICS->BEA mapping.
      This avoids pre-collapsing aggregates to a single NAICS_6 code.
    - Non-weighted paths: expand mixed-digit NAICS to NAICS_6 with a 1:1
      first-match helper mapping, then map into Cornerstone/CEDA activities.
    """

    use_output_weights = _should_use_output_weighted_mapping()
    # For weighted NAICS->BEA mapping (m2 path), preserve the original NAICS
    # code level (e.g., 531) so allocation uses all matching crosswalk rows.
    # Pre-collapsing to one NAICS_6 (keep='first') can bias allocations.
    if use_output_weights:
        fbs2 = fbs.copy()
        fbs2['NAICS_6'] = fbs2['SectorProducedBy']
        mapping = _build_naics_to_bea_weighted_mapping()
    else:
        # Prepare NAICS:NAICS_6 expansion used for non-weighted mapping flows.
        cw = load_crosswalk('NAICS_2017_Crosswalk')
        cols_to_stack = ['NAICS_3', 'NAICS_4', 'NAICS_5']
        cw_stack = (
            cw.astype({c: 'string' for c in cols_to_stack + ['NAICS_6']})
            .melt(
                id_vars='NAICS_6',
                value_vars=cols_to_stack,
                var_name='level',
                value_name='NAICS',
            )
            .dropna(subset=['NAICS_6', 'NAICS'])[['NAICS', 'NAICS_6']]
            .drop_duplicates(subset='NAICS', keep='first')
            .reset_index(drop=True)
        )
        fbs2 = fbs.merge(
            cw_stack,
            how='left',
            left_on='SectorProducedBy',
            right_on='NAICS',
            validate='m:1',
        )
        fbs2['NAICS_6'] = fbs2['NAICS_6'].fillna(fbs2['SectorProducedBy'])

        if get_usa_config().use_cornerstone_2026_model_schema:
            mapping = _build_mapping_with_allocations(
                get_activitytosector_mapping('Cornerstone_2025'),
                use_output_weights=False,
            )
            if get_usa_config().implement_electricity_disaggregation:
                mapping = _apply_electricity_disagg_cornerstone_mapping(mapping)
        else:
            mapping = _build_mapping_with_allocations(
                get_activitytosector_mapping('CEDA_2025'),
                use_output_weights=False,
            )

    pre_total = float(fbs2['FlowAmount'].sum())
    fbs2 = (
        fbs2.merge(
            mapping[['Activity', 'Sector', 'Allocation']],
            how='left',
            left_on='NAICS_6',
            right_on=['Sector'],
            validate='m:m',
        )
        .assign(Allocation=lambda x: x['Allocation'].fillna(1.0))
        .assign(FlowAmount=lambda x: x['FlowAmount'] * x['Allocation'])
        .assign(SectorProducedBy=lambda x: x['Activity'].fillna(x['NAICS_6']))
        .drop(
            columns=['Activity', 'NAICS', 'NAICS_6', 'Sector', 'Allocation'],
            errors='ignore',
        )
    )

    # Re-assign SectorProducedBy and aggregate using existing functions.
    fbs3 = pd.DataFrame(FlowBySector(fbs2).aggregate_flowby())

    if use_output_weights:
        post_total = float(fbs3['FlowAmount'].sum())
        rel_diff = abs(post_total - pre_total) / abs(pre_total) if pre_total else 0.0
        if rel_diff > 0.005:
            raise ValueError(
                'FlowAmount conservation failed in weighted NAICS->BEA mapping '
                f'(pre={pre_total}, post={post_total}, rel_diff={rel_diff:.6f})'
            )

    return fbs3


def _load_cornerstone_ghg_fbs_from_gcs(year: int) -> pd.DataFrame:
    """Download a pre-built, year-specific Cornerstone GHG FBS parquet from GCS.

    Bypasses ``getFlowBySector`` for the time-series case. The flowsa
    regen path goes through `EPA_GHGI` loaders that are hard-capped at
    `{2022, 2023}` (`bedrock/extract/allocation/epa.py:_get_epa_data_year`),
    so years like 2019–2021 (and the 2024 UMD FBS) fail there. The pre-built
    FBS parquets in ``gs://cornerstone-default/transform/output_data/`` whose
    ``base_name`` is ``GHG_national_Cornerstone_<year>`` are loaded directly
    instead (used by new_ghg_method and v0_3_umd_2024_ghgia).

    Picks the most-recently-uploaded parquet whose ``base_name`` matches so we
    follow the FBS regeneration cadence without pinning the version/hash here.
    """
    import os  # noqa: PLC0415

    from bedrock.utils.config.settings import FBS_DIR  # noqa: PLC0415
    from bedrock.utils.io.gcp import (  # noqa: PLC0415
        download_gcs_file,
        list_bucket_files,
    )

    sub_bucket = 'transform/output_data'
    base_name = f'GHG_national_Cornerstone_{year}'
    bucket_df = list_bucket_files(sub_bucket)
    matches = bucket_df[
        (bucket_df['base_name'] == base_name) & (bucket_df['extension'] == '.parquet')
    ].sort_values('created', ascending=False)
    if matches.empty:
        raise FileNotFoundError(
            f'No FBS parquet found at gs://cornerstone-default/{sub_bucket}/ '
            f'matching base_name={base_name!r}'
        )
    filename = matches.iloc[0]['full_path'].rsplit('/', 1)[-1]
    local_path = str(FBS_DIR / filename)
    # Use `download_gcs_file` rather than `_if_not_exists`: the latter
    # downloads ALL files matching the parsed (base, version, hash) into
    # the same `pth`, so the metadata JSON overwrites the parquet.
    if not os.path.exists(local_path):
        download_gcs_file(filename, sub_bucket, local_path)
    logger.info('Loaded cached FBS from %s', filename)
    return pd.read_parquet(local_path)


def load_E_from_flowsa() -> pd.DataFrame:
    """Load E_usa (GHG × CEDA v7 sectors) from the CEDA FBS.

    FBS method is chosen by USA config (first match wins):
    - GHG_national_Cornerstone_{year} when new_ghg_method is True
      (loaded from GCS parquet)
    - GHG_national_2023_m2 when use_ghg_national_2023_m2 is True
    - GHG_national_Cornerstone_2023_ghgi_mecs when update_mecs_method is True
    - GHG_national_Cornerstone_2023_umd_ghgia when v0_3_umd_2023_ghgia is True
    - GHG_national_Cornerstone_2024 when v0_3_umd_2024_ghgia is True
      (loaded from GCS parquet)
    - GHG_national_CEDA_{year} otherwise

    Only used when load_E_from_flowsa is True in USA config.
    """
    usa = get_usa_config()
    year = usa.usa_ghg_data_year
    # Only the base `new_ghg_method` and CEDA fallback FBS methods exist
    # for years other than 2023. The 2023-only variants raise here rather
    # than failing later with an opaque "FBS not found".
    needs_2023 = usa.update_mecs_method or usa.v0_3_umd_2023_ghgia
    if needs_2023 and year != 2023:
        raise ValueError(
            f'usa_ghg_data_year={year} is incompatible with the active '
            'update_*_ghg_method flag — variant FBS methods only exist '
            'for 2023. Either set usa_ghg_data_year=2023 or disable the '
            'update_*_ghg_method flag.'
        )
    if usa.v0_3_umd_2024_ghgia and year != 2024:
        raise ValueError(
            f'usa_ghg_data_year={year} is incompatible with v0_3_umd_2024_ghgia '
            '— the 2024 UMD GHGIA FBS only exists for 2024. Set '
            'usa_ghg_data_year=2024 or use v0_3_umd_2023_ghgia.'
        )
    if usa.new_ghg_method or usa.v0_3_umd_2024_ghgia:
        if usa.new_ghg_method and usa.implement_electricity_disaggregation:
            from flowsa import getFlowBySector as get_flowsa_fbs  # noqa: PLC0415

            fbs = get_flowsa_fbs(
                methodname='GHG_national_Cornerstone_2023_egrid',
                download_FBS_if_missing=True,
            )
        else:
            # Bypass flowsa regen: the EPA loader behind `getFlowBySector` is
            # hard-capped at {2022, 2023}, so other years (incl. the 2024 UMD
            # FBS) fail there. Load the pre-built FBS parquet from GCS at
            # `transform/output_data/` (GHG_national_Cornerstone_<year>) directly
            # so the year-Y diagnostics get year-Y GHG data.
            fbs = _load_cornerstone_ghg_fbs_from_gcs(year)
    else:
        methodname = _select_flowsa_ghg_method()
        if methodname == 'GHG_national_2023_m2':
            # For m2, explicitly attempt remote FBS download before generation.
            fbs = getFlowBySector(methodname=methodname, download_FBS_if_missing=True)
        else:
            fbs = getFlowBySector(methodname=methodname)

    fbs = map_fbs_sectors_to_model_schema(fbs)

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
    if usa.use_ghg_national_2023_m2:
        # Keep m2 diagnostics aligned with USEEIO workbook characterization.
        ghg_mapping['CH4_fossil'] = _USEEIO_WORKBOOK_CH4_GWP
        ghg_mapping['CH4_non_fossil'] = _USEEIO_WORKBOOK_CH4_GWP
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
        if get_usa_config().implement_electricity_disaggregation:
            target_columns = [str(sector) for sector in CORNERSTONE_INDUSTRIES_ELEC]
        else:
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
                'E_usa columns with no mapping (dropped by groupby): %s',
                dropped_by_groupby,
            )
        E_usa = E_usa.groupby(col_to_target, axis=1).sum()  # type: ignore
        target_set = set(target_columns)
        extra = sorted(set(E_usa.columns) - target_set)
        missing = sorted(target_set - set(E_usa.columns))
        if extra:
            logger.warning(
                'E_usa columns not in target schema (will be dropped by reindex): %s',
                extra,
            )
        if missing:
            logger.debug(
                'Target schema columns missing from E_usa (will be filled with 0): %s',
                missing,
            )
        E_usa = E_usa.reindex(columns=target_columns, fill_value=0)

    return E_usa


if __name__ == '__main__':
    from bedrock.utils.config.usa_config import set_global_usa_config

    set_global_usa_config('2025_usa_cornerstone_taxonomy_and_waste_disagg.yaml')
    df1 = load_E_from_flowsa()
    # df2 = derive_E_usa()
    # row_diff = df1.sum(axis=1) - df2.sum(axis=1)
    # row_rel_diff = df1.sum(axis=1) / df2.sum(axis=1)
