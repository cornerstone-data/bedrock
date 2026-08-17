from __future__ import annotations

import logging

import pandas as pd

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
from bedrock.utils.taxonomy.cornerstone.industries import (
    INDUSTRIES,
    WASTE_DISAGG_INDUSTRIES,
)

logger = logging.getLogger(__name__)


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


def derive_E_usa() -> pd.DataFrame:
    return load_E_from_flowsa()


def map_fbs_sectors_to_model_schema(fbs: pd.DataFrame) -> pd.DataFrame:
    """Map FBS NAICS sectors into the active model schema.

    Expands mixed-digit NAICS to NAICS_6 with a 1:1 first-match helper
    mapping, then maps into Cornerstone/CEDA activities.
    """

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

    mapping = _build_mapping_with_allocations(
        get_activitytosector_mapping('Cornerstone_2025'),
        use_output_weights=False,
    )
    if get_usa_config().implement_electricity_disaggregation:
        mapping = _apply_electricity_disagg_cornerstone_mapping(mapping)

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
    return pd.DataFrame(FlowBySector(fbs2).aggregate_flowby())


_EGRID_FBS_METHOD_BY_YEAR: dict[int, str] = {
    2023: 'GHG_national_Cornerstone_2023_egrid',
    2024: 'GHG_national_Cornerstone_2024_egrid',
}


def egrid_fbs_method_for_year(year: int) -> str:
    """Return the eGRID-backed Cornerstone GHG FBS method name for *year*."""
    try:
        return _EGRID_FBS_METHOD_BY_YEAR[year]
    except KeyError as exc:
        supported = ', '.join(str(y) for y in sorted(_EGRID_FBS_METHOD_BY_YEAR))
        raise ValueError(
            f'usa_ghg_data_year={year} is unsupported for the electricity-'
            f'disaggregation eGRID FBS; supported years: {supported}'
        ) from exc


def _load_egrid_fbs_for_electricity_disagg() -> pd.DataFrame:
    """Load the eGRID-based national GHG FBS for electricity disaggregation.

    Selects ``GHG_national_Cornerstone_<year>_egrid`` from
    ``usa_ghg_data_year`` so v0.2 (2023) and v0.3 (2024) electricity configs
    stay year-matched.
    """
    method = egrid_fbs_method_for_year(get_usa_config().usa_ghg_data_year)
    try:
        return _load_cornerstone_ghg_fbs_from_gcs(base_name=method)
    except FileNotFoundError:
        logger.info(
            'eGRID FBS parquet not in transform/output_data; '
            'loading via getFlowBySector (%s)',
            method,
        )
        return getFlowBySector(
            methodname=method,
            download_FBS_if_missing=True,
        )


def _load_cornerstone_ghg_fbs_from_gcs(
    year: int | None = None,
    *,
    base_name: str | None = None,
) -> pd.DataFrame:
    """Download a pre-built Cornerstone GHG FBS parquet from GCS.

    Bypasses ``getFlowBySector`` for the time-series case. The flowsa
    regen path goes through `EPA_GHGI` loaders that are hard-capped at
    `{2022, 2023}` (`bedrock/extract/allocation/epa.py:_get_epa_data_year`),
    so years like 2019–2021 (and the 2024 UMD FBS) fail there. The pre-built
    FBS parquets in ``gs://cornerstone-default/transform/output_data/`` whose
    ``base_name`` is ``GHG_national_Cornerstone_<year>`` (or a method-specific
    name such as ``GHG_national_Cornerstone_2023_egrid``) are loaded directly
    instead (used by use_cornerstone_ghg_model).

    Picks the most-recently-uploaded parquet whose ``base_name`` matches so we
    follow the FBS regeneration cadence without pinning the version/hash here.
    """
    import os  # noqa: PLC0415

    from bedrock.utils.config.settings import FBS_DIR  # noqa: PLC0415
    from bedrock.utils.io.gcp import (  # noqa: PLC0415
        download_gcs_file,
        list_bucket_files,
    )

    if base_name is None:
        if year is None:
            raise ValueError('Either year or base_name must be provided')
        resolved_base_name = f'GHG_national_Cornerstone_{year}'
    else:
        resolved_base_name = base_name

    sub_bucket = 'transform/output_data'
    bucket_df = list_bucket_files(sub_bucket)
    matches = bucket_df[
        (bucket_df['base_name'] == resolved_base_name)
        & (bucket_df['extension'] == '.parquet')
    ].sort_values('created', ascending=False)
    if matches.empty:
        raise FileNotFoundError(
            f'No FBS parquet found at gs://cornerstone-default/{sub_bucket}/ '
            f'matching base_name={resolved_base_name!r}'
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
    """Load E_usa (GHG × model-schema sectors) from a flowsa FBS.

    FBS selection ("GHG model allocation" bucket + data-year knob):
    - use_cornerstone_ghg_model → the pre-built GHG_national_Cornerstone_{year}
      FBS parquet from GCS. Which inventory/attribution vintages that carries
      (EPA GHGI vs UMD GHGIA, MECS survey year) is defined per year by the
      method files in ``bedrock/transform/ghg/``.
    - otherwise → GHG_national_CEDA_{year}, the flowsa implementation of the
      legacy CEDA allocation methodology (method files exist for 2023 only).
    """
    usa = get_usa_config()
    year = usa.usa_ghg_data_year
    if usa.use_cornerstone_ghg_model:
        if usa.implement_electricity_disaggregation:
            fbs = _load_egrid_fbs_for_electricity_disagg()
        else:
            # Bypass flowsa regen: the EPA loader behind `getFlowBySector` is
            # hard-capped at {2022, 2023}, so other years (incl. the 2024 UMD
            # FBS) fail there. Load the pre-built FBS parquet from GCS at
            # `transform/output_data/` (GHG_national_Cornerstone_<year>) directly
            # so the year-Y diagnostics get year-Y GHG data.
            fbs = _load_cornerstone_ghg_fbs_from_gcs(year)
    else:
        if year != 2023:
            raise ValueError(
                f'usa_ghg_data_year={year} is incompatible with '
                'use_cornerstone_ghg_model=False — the CEDA-methodology FBS '
                '(GHG_national_CEDA_{year}) only exists for 2023.'
            )
        fbs = getFlowBySector(methodname=f'GHG_national_CEDA_{year}')

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

    # Collapse across sectors (already in Cornerstone schema from
    # map_fbs_sectors_to_model_schema).
    if usa.implement_electricity_disaggregation:
        target_columns = [str(sector) for sector in CORNERSTONE_INDUSTRIES_ELEC]
    else:
        target_columns = [str(sector) for sector in INDUSTRIES]
    E_usa = E_usa.reindex(columns=target_columns, fill_value=0)

    return E_usa
