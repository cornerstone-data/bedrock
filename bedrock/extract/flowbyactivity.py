"""
FlowByActivity (FBA) data are attributed to a class, allowing the configuration
file and other attributes to be attached to the FBA object. The functions
defined in this file are specific to FBA data and.

Generation of FBA datasets calls on the functions defined in
gerneateflowbyactivity.py

"""

# necessary so 'FlowBySector'/'FlowByActivity' can be used in fxn
# annotations without importing the class to the py script which would lead
# to circular reasoning
from __future__ import annotations

from functools import partial, reduce
from typing import TYPE_CHECKING, Any, Literal, cast

import fedelemflowlist
import numpy as np
import pandas as pd

from bedrock.extract.generateflowbyactivity import generateFlowByActivity
from bedrock.transform.flowby import _FlowBy, flowby_config
from bedrock.transform.flowbyfunctions import filter_by_geoscale
from bedrock.utils.config import settings
from bedrock.utils.config.settings import (
    DEFAULT_DOWNLOAD_IF_MISSING,
    FBA_DIR,
    NAME_SEP_CHAR,
)
from bedrock.utils.logging.flowsa_log import log
from bedrock.utils.mapping import sectormapping
from bedrock.utils.mapping.geo import filtered_fips, scale
from bedrock.utils.mapping.sector import (
    _schema_block_year,
    convert_naics_year,
    industry_spec_key,
    load_schema_conversion_crosswalk,
    parse_sector_source_name,
    return_schema_crosswalk,
    sector_hierarchy_from_config,
    sector_source_name,
    subset_sector_key,
)
from bedrock.utils.metadata.metadata import set_fb_meta
from bedrock.utils.validation.exceptions import FBANotAvailableError
from bedrock.utils.validation.validation import compare_geographic_totals

if TYPE_CHECKING:
    from bedrock.transform.flowbysector import FlowBySector


class FlowByActivity(_FlowBy):
    _metadata = [*_FlowBy()._metadata]

    def __init__(
        self,
        data: pd.DataFrame | _FlowBy | None = None,
        *args: Any,
        mapped: bool = False,
        w_sector: bool = False,
        **kwargs: Any,
    ) -> None:
        if isinstance(data, pd.DataFrame):
            mapped = mapped or any(
                [c in data.columns for c in flowby_config['_mapped_fields']]
            )
            w_sector = w_sector or any(
                [c in data.columns for c in flowby_config['_sector_fields']]
            )

            if mapped and w_sector:
                fields = flowby_config['fba_mapped_w_sector_fields']
            elif mapped:
                fields = flowby_config['fba_mapped_fields']
            elif w_sector:
                fields = flowby_config['fba_w_sector_fields']
            else:
                fields = flowby_config['fba_fields']

            column_order = flowby_config['fba_column_order']
        else:
            fields = None
            column_order = None

        super().__init__(
            data, fields=fields, column_order=column_order, *args, **kwargs
        )

    @property
    def _constructor(self) -> type[FlowByActivity]:
        return FlowByActivity

    @property
    def _constructor_sliced(self) -> type[_FBASeries]:  # type: ignore[override]
        return _FBASeries

    @classmethod
    def return_FBA(
        cls,
        full_name: str,
        year: int | None = None,
        git_version: str | None = None,
        config: dict[str, Any] | None = None,
        download_ok: bool = settings.DEFAULT_DOWNLOAD_IF_MISSING,
        **kwargs: Any,
    ) -> FlowByActivity:
        """
        Loads stored data in the FlowByActivity format. If it is not
        available, tries to download it from EPA's remote server (if
        download_ok is True), or generate it.
        :param datasource: str, the code of the datasource.
        :param year: int, a year, e.g. 2012
        :param download_ok: bool, if True will attempt to load from
            EPA remote server prior to generating
        :kwargs: keyword arguments to pass to _getFlowBy(). Possible kwargs
            include config.
        :return: a FlowByActivity dataframe
        """
        if year is None and isinstance(config, dict):
            year = config.get('year')

        # set metaname
        if year is None:
            meta_name = full_name
        else:
            meta_name = f'{full_name}_{year}'
        if git_version is not None:
            meta_name = f'{meta_name}_{git_version}'

        file_metadata = set_fb_meta(meta_name, 'FlowByActivity')
        flowby_generator = partial(generateFlowByActivity, source=full_name, year=year)
        return super()._getFlowBy(  # type: ignore[return-value]
            file_metadata=file_metadata,
            download_ok=download_ok,
            flowby_generator=flowby_generator,
            output_path=str(FBA_DIR),
            full_name=full_name,
            config=config or {},
            **kwargs,
        )

    # TODO: probably only slight modification is needed to allow for material
    # flow list mapping using this function as well.
    def map_to_fedefl_list(
        self: 'FlowByActivity', drop_unmapped_rows: bool = False
    ) -> 'FlowByActivity':
        fba_merge_keys = ['Flowable', 'Unit', 'Context']
        mapping_fields = [
            'SourceListName',
            'SourceFlowName',
            'SourceFlowContext',
            'SourceUnit',
            'ConversionFactor',
            'TargetFlowName',
            'TargetFlowContext',
            'TargetUnit',
            'TargetFlowUUID',
        ]
        mapping_merge_keys = ['SourceFlowName', 'SourceUnit', 'SourceFlowContext']
        merge_type: Literal['inner', 'left'] = 'inner' if drop_unmapped_rows else 'left'

        mapping_subset = self.config.get('fedefl_mapping')

        log.info(
            f'Mapping flows in {self.full_name} to '
            f'{mapping_subset} in federal elementary flow list'
        )

        # Check for use of multiple mapping files
        if isinstance(mapping_subset, list):
            fba_merge_keys.append('SourceName')
            mapping_merge_keys.append('SourceListName')

        fba = self.assign(
            Flowable=self.FlowName,
            Context=self.Compartment,
        ).drop(columns=['FlowName', 'Compartment'])

        # first check for a flow mapping file stored locally. If it does not exist, then use the fedelemflowlist
        # mapping file
        mapping_load: pd.DataFrame | None = None
        if mapping_subset is not None:
            if isinstance(mapping_subset, str):
                local_csv = (
                    settings.mappingpath / 'flowmapping' / f'{mapping_subset}.csv'
                )
                if local_csv.is_file():
                    log.info(f'Loading flow mapping from {local_csv}')
                    mapping_load = pd.read_csv(local_csv)

        if mapping_load is None:
            mapping_load = fedelemflowlist.get_flowmapping(mapping_subset)

        mapping = mapping_load[mapping_fields].assign(
            ConversionFactor=lambda x: x.ConversionFactor.fillna(1)
        )
        if mapping.empty:
            log.error(
                f'Elementary flow list entries for {mapping_subset} not ' f'found'
            )
            return FlowByActivity(self, mapped=True)

        mapped_fba = fba.merge(
            mapping,
            how=merge_type,
            left_on=fba_merge_keys,
            right_on=mapping_merge_keys,
            indicator='mapped',
        )

        is_mappable = mapped_fba.TargetFlowName.notnull()
        mapped_fba = mapped_fba.assign(
            Flowable=mapped_fba.Flowable.mask(is_mappable, mapped_fba.TargetFlowName),
            Context=mapped_fba.Context.mask(is_mappable, mapped_fba.TargetFlowContext),
            Unit=mapped_fba.Unit.mask(is_mappable, mapped_fba.TargetUnit),
            FlowAmount=mapped_fba.FlowAmount.mask(
                is_mappable, mapped_fba.FlowAmount * mapped_fba.ConversionFactor
            ),
            FlowUUID=mapped_fba.TargetFlowUUID,
        ).drop(columns=mapping_fields)

        if any(mapped_fba.mapped == 'both'):
            log.info(
                f'Units standardized to '
                f'{list(mapping.TargetUnit.unique())} by mapping to '
                f'federal elementary flow list'
            )
        if any(mapped_fba.mapped == 'left_only'):
            unstandardized_units = list(
                mapped_fba.query('mapped == "left_only"').Unit.unique()
            )
            log.warning(
                f'Some units not standardized by mapping to federal '
                f'elementary flows list: {unstandardized_units}'
            )

        return mapped_fba.drop(columns='mapped')

    # TODO: Can this be generalized to a _FlowBy method?
    def convert_to_geoscale(
        self: FlowByActivity,
        target_geoscale: Literal['national', 'state', 'county'] | scale | None = None,
    ) -> FlowByActivity:
        '''
        Converts, by filtering or aggregating (or both), the given dataset to
        the target geoscale.

        Rows from the calling FlowBy that correspond to a higher level (more
        aggregated) geoscale than the target are dropped. Then, for each
        combination of 'ActivityProducedBy' and 'ActivityConsumedBy', and for
        each level at or below (less aggregated than) the target geoscale,
        determine the highest level at which data is reported for each unit at
        that scale (so if the level is 'state', find the highest level at which
        data is reported for each state, for each activity combination).
        Finally, use this information to identify the correct source scale
        for each activity combination and regional unit (details below), then
        filter or aggregate (or both) to convert the dataset so all rows are
        at the target geoscale.

        For any region and activity combination, the correct source geoscale
        is the highest (most aggregated) geoscale at or below the target
        geoscale, for which data covering that region and activity combination
        is reported. For example, if the target geoscale is 'national',
        national level data should be used if available. If not, state level
        data should be aggregated up if available. However, if some states
        report county level data AND NOT state level data, then for those
        states (and only those states) county level data should be aggregated
        up. County level data from states that also report state level data
        should, in this example, be ignored.

        :param target_geoscale: str or geo.scale constant, the geoscale to
            convert the calling FlowBy data set to. Currently, this needs to be
            one which corresponds to a FIPS level (that is, one of national,
            state, or county)
        :return: FlowBy data set, with rows filtered or aggregated to the
            target geoscale.
        '''
        if self.LocationSystem.eq('Census_Region').all():
            return self
        geoscale_input = target_geoscale or self.config.get('geoscale')
        if isinstance(geoscale_input, str):
            target_scale: scale = scale.from_string(
                cast(
                    Literal[
                        'national',
                        'census_region',
                        'census_division',
                        'state',
                        'county',
                    ],
                    geoscale_input,
                )
            )
        else:
            assert isinstance(
                geoscale_input, scale
            ), 'target_geoscale must be provided or set in config'
            target_scale = geoscale_input

        geoscale_by_fips = pd.concat(
            [
                (
                    filtered_fips(
                        cast(
                            Literal[scale.NATIONAL, scale.STATE, scale.COUNTY],
                            s,
                        )
                    ).assign(geoscale=s.name, National='USA')
                    # ^^^ Need to have a column for each relevant scale
                    .rename(columns={'FIPS': 'Location'})
                )
                # ^^^ (only FIPS for now)
                for s in [sc for sc in scale if sc.has_fips_level]
            ]
        )

        geoscale_name_columns = [s.name.title() for s in scale if s.has_fips_level]

        log.info(
            f'Determining appropriate source geoscale for '
            f'{self.full_name}; target geoscale is '
            f'{target_scale.name.lower()}'
        )

        highest_reporting_level_by_geoscale = [
            (
                self.merge(geoscale_by_fips, how='inner')
                .loc[lambda df: df['geoscale'].str.lower().map(scale.from_string) <= sc]
                .assign(
                    geoscale_scale=lambda df: df['geoscale']
                    .str.lower()
                    .map(scale.from_string)
                )
                .groupby(
                    ['ActivityProducedBy', 'ActivityConsumedBy']
                    + [s.name.title() for s in scale if s.has_fips_level and s >= sc],
                    dropna=False,
                )
                .agg({'geoscale_scale': lambda x: max(x)})
                .reset_index()
                .rename(
                    columns={
                        'geoscale_scale': f'highest_reporting_level_by_{sc.name.title()}'
                    }
                )
            )
            for sc in scale
            if sc.has_fips_level and sc <= target_scale
        ]

        # if an activity column is a mix of string and np.nan values but
        # after subsetting, the column is all np.nan, then the column dtype is
        # converted to float which causes an error when merging float col back
        # with the original object dtype. So convert float cols back to object
        for df in highest_reporting_level_by_geoscale:
            for c in ['ActivityProducedBy', 'ActivityConsumedBy']:
                if df[c].dtype == float:
                    df[c] = df[c].astype(object)

        fba_with_reporting_levels: FlowByActivity = cast(
            FlowByActivity,
            reduce(
                lambda x, y: x.merge(y, how='left'),
                [self, geoscale_by_fips, *highest_reporting_level_by_geoscale],
            ),
        )

        reporting_level_columns = [
            f'highest_reporting_level_by_{s.name.title()}'
            for s in scale
            if s.has_fips_level and s <= target_scale
        ]

        fba_at_source_geoscale = (
            fba_with_reporting_levels.assign(
                source_geoscale=fba_with_reporting_levels[
                    reporting_level_columns
                ].apply(
                    lambda row: max(
                        (v for v in row if isinstance(v, scale)), default=np.nan
                    ),
                    axis=1,
                )
            )
            .loc[
                lambda df: df['geoscale']
                == df['source_geoscale'].map(
                    lambda s: s.name if isinstance(s, scale) else np.nan
                )
            ]
            .drop(
                columns=(['geoscale', *geoscale_name_columns, *reporting_level_columns])
            )
        ).reset_index(drop=True)

        if len(fba_at_source_geoscale.source_geoscale.unique()) > 1:
            log.warning(
                f"{fba_at_source_geoscale.full_name} has multiple "
                f"source geoscales: "
                f"{', '.join([s.name.lower() for s in fba_at_source_geoscale.source_geoscale.unique()])}"
            )
        else:
            log.info(
                f'{fba_at_source_geoscale.full_name} source geoscale is '
                f'{fba_at_source_geoscale.source_geoscale.unique()[0].name.lower()}'
            )

        fba_at_target_scale = (
            fba_at_source_geoscale.drop(columns='source_geoscale')
            .convert_fips_to_geoscale(
                cast(
                    Literal[scale.NATIONAL, scale.STATE, scale.COUNTY],
                    target_scale,
                )
            )
            .aggregate_flowby()
            .astype(
                {
                    activity: flowby_config['fba_fields'][activity]
                    for activity in ['ActivityProducedBy', 'ActivityConsumedBy']
                }
            )
        )

        if target_scale != scale.NATIONAL:
            # TODO: This block of code can be simplified a great deal once
            #       validation.py is rewritten to use the FB config dictionary
            activities = list(
                self.add_primary_secondary_columns('Activity').PrimaryActivity.unique()
            )

            compare_geographic_totals(
                fba_at_target_scale,
                self,
                self.source_name,
                self.config,
                self.full_name.split('.')[-1],
                activities,
                df_type='FBS',
                subnational_geoscale=target_scale.name.lower(),
                # ^^^ TODO: Rewrite validation to use fb metadata
            )

        return fba_at_target_scale.reset_index(drop=True)

    def map_to_sectors(
        self: FlowByActivity,
        target_year: Literal[2002, 2007, 2012, 2017, 2022],
        external_config_path: str | None = None,
    ) -> FlowByActivity:
        """
        Maps the activities in the calling dataframe to industries/sectors, but
        does not perform any attribution. Columns for SectorProducedBy and
        SectorConsumedBy are added to the FBA. Each activity may be matched
        with many industries/sectors, and each industry/sector may have many
        activities matched to it.

        Mapping builds one Activity to target key covering all ``industry_spec``
        schemas (the industry sectors the method targets — NAICS and/or BEA),
        then merges once per direction.

        - Text activities: CW rows map onto matching ``industry_spec`` targets.
        - Sector-like: expand onto targets in the activity's schema via that
          schema's hierarchy; convert to the other schema's targets when that
          schema is also in ``industry_spec`` and the activity is not already
          covered by the native keep-set.

        :param target_year: int, target schema year (``target_schema_year``).
        :param external_config_path: str, an external path to search for a
            crosswalk.
        """
        from bedrock.transform.flowbyclean import (  # noqa: PLC0415
            define_parentincompletechild_descendants,
            drop_parentincompletechild_descendants,
        )

        activity_schema = self.config.get('activity_schema')
        if isinstance(activity_schema, str) or (
            isinstance(activity_schema, dict)
            and activity_schema
            and all(isinstance(k, (int, str)) and str(k).isdigit() for k in activity_schema)
            and any(
                isinstance(v, str) and v.endswith('_Code')
                for v in activity_schema.values()
            )
        ):
            raise ValueError(
                'Legacy string/year-keyed activity_schema is no longer supported; '
                'use a dict keyed by schema (naics:/bea:/...) in source_catalog.yaml.'
            )
        if isinstance(activity_schema, dict) and (
            'ProducedBy' in activity_schema or 'ConsumedBy' in activity_schema
        ):
            raise ValueError(
                'Per-direction activity_schema (ProducedBy/ConsumedBy) is not '
                'supported yet; use one shared activity_schema block per source.'
            )

        def _source_years_from_dict(
            schema_dict: dict[str, Any],
        ) -> dict[str, int]:
            years: dict[str, int] = {}
            for schema, sch_dict in schema_dict.items():
                if not isinstance(sch_dict, dict):
                    continue
                if 'by_year' in sch_dict:
                    by_year = sch_dict['by_year']
                    data_year = self.config.get('year')
                    years[schema] = int(
                        by_year.get(data_year, by_year.get(str(data_year), target_year))
                    )
                else:
                    years[schema] = int(sch_dict.get('year', target_year))
            return years

        def _missing_codes_for_dict(
            schema_dict: dict[str, Any],
            activities: pd.Series,
        ) -> dict[str, list[str]]:
            """Activity values not found in declared schema code lists."""
            present = {
                str(v)
                for v in activities.dropna().unique()
                if str(v) != ''
            }
            if not present:
                return {}
            years = _source_years_from_dict(schema_dict)
            missing: dict[str, list[str]] = {}
            for schema, sch_dict in schema_dict.items():
                if not isinstance(sch_dict, dict):
                    continue
                year = int(years.get(schema, target_year))
                cw = return_schema_crosswalk(schema, year)
                valid = {
                    str(v)
                    for v in cw.to_numpy().ravel()
                    if pd.notna(v) and str(v) != ''
                }
                not_found = sorted(present - valid)
                if not_found:
                    missing[schema] = not_found
            return missing

        # null / None → text activities; mapping CW supplies SectorSourceName
        sector_like = (
            isinstance(activity_schema, dict) and len(activity_schema) > 0
        )
        hierarchy = sector_hierarchy_from_config(self.config)
        industry_key = industry_spec_key(
            self.config['industry_spec'],
            int(self.config['target_schema_year']),
        )
        industry_key = sectormapping.assign_technological_correlation(industry_key)

        activity_to_source_sector_crosswalk: pd.DataFrame | None = None
        source_years: dict[str, int] = {}
        activity_schemas: set[str] = set()
        # Sector-like single schema: default SectorSourceName when merge left it null
        default_sec_source_name: str | None = None

        if sector_like:
            assert isinstance(activity_schema, dict)
            activity_schemas = {
                k for k, v in activity_schema.items() if isinstance(v, dict)
            }
            log.info(
                f'Activities in {self.full_name} are sector-like '
                f'({", ".join(sorted(activity_schemas))}).'
            )
            source_years = _source_years_from_dict(activity_schema)
            if len(activity_schemas) == 1:
                only = next(iter(activity_schemas))
                default_sec_source_name = sector_source_name(
                    only, int(source_years.get(only, target_year))
                )
            if hierarchy is None:
                for sch_dict in activity_schema.values():
                    if isinstance(sch_dict, dict) and sch_dict.get('hierarchy'):
                        hierarchy = sch_dict.get('hierarchy')
                        break
            missing_by_schema = _missing_codes_for_dict(
                activity_schema,
                pd.concat(
                    [
                        self['ActivityProducedBy'],
                        self['ActivityConsumedBy'],
                    ],
                    ignore_index=True,
                ),
            )
            if missing_by_schema:
                detail = '; '.join(
                    f'{schema.upper()}: {", ".join(codes)}'
                    for schema, codes in sorted(missing_by_schema.items())
                )
                log.warning(
                    f'{self.full_name}: sector-like activity values not found '
                    f'in declared schema code lists - {detail}'
                )
        else:
            log.info(
                f'Getting crosswalk between activities in {self.full_name} '
                f'and sectors.'
            )
            activity_to_source_sector_crosswalk = sectormapping.get_activitytosector_mapping(
                self.config.get('activity_to_sector_mapping') or self.source_name,
                fbsconfigpath=external_config_path,
            ).astype('object')[
                ['Activity', 'Sector', 'SectorType', 'SectorSourceName']
            ]

        def _key_for_schema(schema: str) -> pd.DataFrame:
            return industry_key[
                industry_key['SectorSourceName'].map(
                    lambda s: parse_sector_source_name(s)[0]
                )
                == schema
            ].copy()

        target_schemas = sorted(
            {
                parse_sector_source_name(sec_source_name)[0]
                for sec_source_name in industry_key['SectorSourceName']
                .dropna()
                .unique()
            }
        )

        def _mapping_jobs_for_dict(
            schema_dict: dict[str, Any] | None,
            *,
            activity_to_source_sector_crosswalk: pd.DataFrame | None,
        ) -> list[tuple[pd.DataFrame, pd.DataFrame | None, str]]:
            """Build (primary, secondary, source_year) jobs for one schema dict."""
            jobs: list[tuple[pd.DataFrame, pd.DataFrame | None, str]] = []
            if schema_dict is not None:
                schemas = {
                    k for k, v in schema_dict.items() if isinstance(v, dict)
                }
                years = _source_years_from_dict(schema_dict)
                keep_by_schema: dict[str, set[str]] = {}
                # Keep: native schema identity parent→child
                for schema in schemas:
                    if schema not in target_schemas:
                        continue
                    schema_key = _key_for_schema(schema)
                    if schema_key.empty:
                        continue
                    jobs.append(
                        (schema_key, None, str(years.get(schema, target_year)))
                    )
                    keep_by_schema[schema] = set(
                        schema_key['source_sector'].dropna().astype(str).tolist()
                    )

                # Convert leftovers to default_schema only. Extra industry_spec
                # schemas are keep-lists (identity), not convert destinations.
                spec = self.config.get('industry_spec') or {}
                default_schema = spec.get('default_schema')
                if default_schema:
                    other = {default_schema} - schemas
                else:
                    other = set(target_schemas) - schemas
                method_year = int(self.config.get('target_schema_year', target_year))
                for t_schema in sorted(other):
                    target_key = _key_for_schema(t_schema)
                    if target_key.empty:
                        continue
                    t_block = spec.get(t_schema)
                    t_year = (
                        _schema_block_year(t_block, method_year)
                        if isinstance(t_block, dict)
                        else method_year
                    )
                    for s_schema in sorted(schemas):
                        s_year = int(years.get(s_schema, target_year))
                        conversion_cw = load_schema_conversion_crosswalk(
                            s_schema,
                            t_schema,
                            s_year,
                            t_year,
                            fbsconfigpath=external_config_path,
                        )
                        if conversion_cw.empty:
                            continue
                        keep = keep_by_schema.get(s_schema, set())
                        if keep:
                            conversion_cw = conversion_cw[
                                ~conversion_cw['Activity'].astype(str).isin(keep)
                            ]
                        if not conversion_cw.empty:
                            log.info(
                                f'Mapping {self.full_name} {s_schema} '
                                f'activities to {t_schema} (convert).'
                            )
                            jobs.append((conversion_cw, target_key, str(s_year)))
                return jobs

            # Text activities: CW to industry only
            assert activity_to_source_sector_crosswalk is not None
            if not activity_to_source_sector_crosswalk.empty and not industry_key.empty:
                jobs.append((activity_to_source_sector_crosswalk, industry_key, str(target_year)))
            return jobs

        fba_w_sectors = self.copy()
        for direction in ['ProducedBy', 'ConsumedBy']:
            if fba_w_sectors[f'Activity{direction}'].isna().all():
                fba_w_sectors = fba_w_sectors.assign(
                    **{
                        f'Sector{direction}': np.nan,
                        f'{direction}SectorType': np.nan,
                    }
                )
            else:
                if hierarchy == 'parent-incompleteChild':
                    fba_w_sectors = define_parentincompletechild_descendants(
                        fba_w_sectors, activity_col=f'Activity{direction}'
                    )

                if sector_like:
                    assert isinstance(activity_schema, dict)
                    mapping_jobs = _mapping_jobs_for_dict(
                        activity_schema, activity_to_source_sector_crosswalk=None
                    )
                else:
                    mapping_jobs = _mapping_jobs_for_dict(
                        None,
                        activity_to_source_sector_crosswalk=(
                            activity_to_source_sector_crosswalk
                        ),
                    )

                if not mapping_jobs:
                    raise ValueError(
                        f'No sector-mapping jobs for {self.full_name} '
                        f'({direction}): activity_schema='
                        f'{sorted(activity_schema) if sector_like and activity_schema else None}, '
                        f'industry_spec schemas={target_schemas}. Text activities '
                        f'need CW rows tagged with a target schema; sector-like '
                        f'sources need a matching industry_spec block (or '
                        f'a conversion crosswalk to a target schema).'
                    )

                mapping_parts: list[pd.DataFrame] = []
                for primary_key, secondary_key, _source_year in mapping_jobs:
                    mapping_parts.append(
                        subset_sector_key(
                            fba_w_sectors,
                            f'Activity{direction}',
                            primary_sector_key=primary_key,
                            secondary_sector_key=secondary_key,
                        )
                    )
                activity_to_target_sector_crosswalk = (
                    pd.concat(mapping_parts, ignore_index=True).drop_duplicates()
                    if mapping_parts
                    else pd.DataFrame()
                )

                fba_w_sectors = (
                    fba_w_sectors.merge(
                        activity_to_target_sector_crosswalk,
                        how='left',
                        on=[
                            'Class',
                            'Flowable',
                            'Context',
                            'ActivityProducedBy',
                            'ActivityConsumedBy',
                        ],
                    )
                    .rename(
                        columns={
                            'target_sector': f'Sector{direction}',
                            'Sector': f'Sector{direction}',
                            'SectorType': f'{direction}SectorType',
                        }
                    )
                    .drop(
                        columns=[
                            'ActivitySourceName',
                            'source_sector',
                            'Activity',
                        ],
                        errors='ignore',
                    )
                )
                # Second direction merge may suffix SectorSourceName; keep one column.
                if (
                    'SectorSourceName_x' in fba_w_sectors.columns
                    or 'SectorSourceName_y' in fba_w_sectors.columns
                ):
                    sec_source_name_x = fba_w_sectors.get(
                        'SectorSourceName_x', fba_w_sectors.get('SectorSourceName')
                    )
                    sec_source_name_y = fba_w_sectors.get('SectorSourceName_y')
                    if sec_source_name_x is not None and sec_source_name_y is not None:
                        fba_w_sectors['SectorSourceName'] = sec_source_name_x.fillna(
                            sec_source_name_y
                        )
                    elif sec_source_name_x is not None:
                        fba_w_sectors['SectorSourceName'] = sec_source_name_x
                    elif sec_source_name_y is not None:
                        fba_w_sectors['SectorSourceName'] = sec_source_name_y
                    fba_w_sectors = fba_w_sectors.drop(
                        columns=['SectorSourceName_x', 'SectorSourceName_y'],
                        errors='ignore',
                    )
                # SectorSourceName is authoritative for this FBA
                dq_cols = ['DataReliability', 'DataCollection']
                for c in dq_cols:
                    if f'{c}_y' in fba_w_sectors.columns:
                        fba_w_sectors.loc[
                            fba_w_sectors[f'{c}_y'].notnull(), f'{c}_x'
                        ] = fba_w_sectors[f'{c}_y']
                        fba_w_sectors = fba_w_sectors.drop(columns=[f'{c}_y']).rename(
                            columns={f'{c}_x': c}
                        )
                if hierarchy == 'parent-incompleteChild':
                    fba_w_sectors = drop_parentincompletechild_descendants(
                        fba_w_sectors, sector_col=f'Sector{direction}'
                    )

        for dq in ['DataReliability', 'DataCollection', 'TechnologicalCorrelation']:
            if f'{dq}_x' in fba_w_sectors.columns:
                fba_w_sectors = fba_w_sectors.assign(
                    **{
                        f'{dq}': fba_w_sectors[[f'{dq}_x', f'{dq}_y']].apply(
                            np.nanmax, axis=1
                        )
                    }
                )

        # NAICS vintage conversion only when sector-like NAICS years differ
        naics_year = source_years.get('naics')
        if (
            sector_like
            and naics_year is not None
            and naics_year != self.config['target_schema_year']
        ):
            fba_w_sectors = cast(
                FlowByActivity,
                convert_naics_year(
                    fba_w_sectors,
                    sector_source_name('naics', int(self.config['target_schema_year'])),
                    sector_source_name('naics', int(naics_year)),
                    self.full_name,
                ),
            )

        if not sector_like:
            not_mapped = fba_w_sectors[
                fba_w_sectors[['SectorProducedBy', 'SectorConsumedBy']].isna().all(1)
            ]
            if len(not_mapped) > 0:
                not_mapped = not_mapped[
                    ['ActivityProducedBy', 'ActivityConsumedBy']
                ].drop_duplicates()
                unmapped_activities = sorted(
                    set(not_mapped.ActivityProducedBy.dropna()).union(
                        set(not_mapped.ActivityConsumedBy.dropna())
                    )
                )
                log.warning(
                    f'Activities in {not_mapped.full_name} are not mapped to '
                    f'sectors: {unmapped_activities}'
                )

        fba_w_sectors = fba_w_sectors[
            ~(
                fba_w_sectors['SectorProducedBy'].isna()
                & fba_w_sectors['SectorConsumedBy'].isna()
            )
        ]

        # Ensure SectorSourceName exists; sector-like fills from activity_schema when null
        if 'SectorSourceName' not in fba_w_sectors.columns:
            fba_w_sectors = fba_w_sectors.assign(SectorSourceName=np.nan)
        if default_sec_source_name is not None:
            fba_w_sectors['SectorSourceName'] = fba_w_sectors[
                'SectorSourceName'
            ].fillna(default_sec_source_name)

        sector_bearing = (
            fba_w_sectors['SectorProducedBy'].notna()
            | fba_w_sectors['SectorConsumedBy'].notna()
        )
        missing_sec_source_name = fba_w_sectors['SectorSourceName'].isna()
        bad_sec_source_name = sector_bearing & missing_sec_source_name
        if bad_sec_source_name.any():
            n_bad = int(bad_sec_source_name.sum())
            log.warning(
                f'{self.full_name}: dropping {n_bad} sector-bearing rows '
                f'with null SectorSourceName'
            )
            fba_w_sectors = fba_w_sectors.loc[~bad_sec_source_name]

        return (
            fba_w_sectors.drop(
                columns=[
                    'TechnologicalCorrelation_x',
                    'TechnologicalCorrelation_y',
                    'DataReliability_x',
                    'DataReliability_y',
                    'DataCollection_x',
                    'DataCollection_y',
                ],
                errors='ignore',
            )
            .reset_index(drop=True)
        )

    def prepare_fbs(
        self: FlowByActivity,
        external_config_path: str | None = None,
        download_sources_ok: bool = True,
        skip_select_by: bool = False,
        retain_activity_columns: bool = False,
        fbs_method_name: str | None = None,
    ) -> FlowBySector:

        from bedrock.transform.flowbysector import FlowBySector  # noqa: PLC0415

        # drop the activity columns in the FBS unless method yaml specifies
        # to keep them
        drop_cols = [
            'ActivityProducedBy',
            'ActivityConsumedBy',
        ]
        if retain_activity_columns:
            drop_cols = []

        if 'activity_sets' in self.config:
            try:
                return FlowBySector(
                    pd.concat(
                        [
                            fba.prepare_fbs(
                                external_config_path=external_config_path,
                                download_sources_ok=download_sources_ok,
                                skip_select_by=True,
                                retain_activity_columns=retain_activity_columns,
                                fbs_method_name=fbs_method_name,
                            )
                            for fba in (
                                self.select_by_fields()
                                .function_socket('clean_fba_before_activity_sets')
                                .activity_sets()
                            )
                        ]
                    ).reset_index(drop=True),
                    convert_df_to_flowby=True,
                )
            except ValueError:
                # This discards every activity_set, not just the one that
                # failed, so a fault in one silently zeroes the whole method.
                # Log it - the silence is what makes this class of bug expensive
                # to find.
                log.exception(
                    f'Discarding ALL activity_sets for {self.full_name}: one '
                    f'of them raised while being prepared. The method will '
                    f'return no rows.'
                )
                return FlowBySector(pd.DataFrame(), convert_df_to_flowby=True)
        log.info(f'Processing FlowBySector for {self.full_name}')
        # Primary FlowBySector generation approach:
        return FlowBySector(
            self.function_socket('clean_fba_before_mapping')
            .select_by_fields(skip_select_by=skip_select_by)
            .function_socket('estimate_suppressed')
            .select_by_fields(
                skip_select_by=skip_select_by,
                selection_fields=self.config.get(
                    'selection_fields_after_data_suppression_estimation', 'null'
                ),
            )
            .convert_units_and_flows()  # and also map to flow lists
            .function_socket('clean_fba')
            .assign_geographic_correlation(fbs_method_name=fbs_method_name)
            .convert_to_geoscale()
            .attribute_flows_to_sectors(
                external_config_path=external_config_path,
                download_sources_ok=download_sources_ok,
            )  # recursive call to prepare_fbs
            .drop(columns=drop_cols)
            .aggregate_flowby()
            .function_socket('clean_fbs_after_aggregation'),
            convert_df_to_flowby=True,
        )

    def activity_sets(self) -> list[FlowByActivity]:  # type: ignore[override]
        '''
        This function breaks up an FBA dataset into its activity sets, if its
        config dictionary specifies activity sets, and returns a list of the
        resulting FBAs. Otherwise, it returns a list containing the calling
        FBA.

        Activity sets are determined by the selection_field key under each
        activity set name. An error will be logged if any rows from the calling
        FBA are assigned to multiple activity sets.
        '''
        if 'activity_sets' not in self.config:
            return [self]

        log.info(f'Splitting {self.full_name} into activity sets')
        activities = self.config['activity_sets']
        parent_config = {
            k: v
            for k, v in self.config.items()
            if k not in ['activity_sets', 'clean_fba_before_activity_sets']
            and not k.startswith('_')
        }
        parent_fba = self.reset_index(names='row')

        child_fba_list: list[FlowByActivity] = []
        assigned_rows: set[Any] = set()
        for activity_set, activity_config in activities.items():
            log.info(f'Creating FlowByActivity for {activity_set}')

            child_fba = parent_fba.add_full_name(
                f'{parent_fba.full_name}{NAME_SEP_CHAR}{activity_set}'
            ).select_by_fields(
                selection_fields=activity_config.get('selection_fields'),
                exclusion_fields=activity_config.get('exclusion_fields'),
                assign_fields=activity_config.get('assign_fields'),
            )

            child_fba.config = {**parent_config, **activity_config}
            child_fba = child_fba.assign(SourceName=child_fba.full_name)

            if set(child_fba.row) & assigned_rows:
                double_counted = child_fba.query(
                    f'row in {list(set(child_fba.row) & assigned_rows)}'
                )
                log.critical(
                    f'Some rows from {parent_fba.full_name} assigned to '
                    f'multiple activity sets. This will lead to '
                    f'double-counting:\n{double_counted}'
                )
                # raise ValueError('Some rows in multiple activity sets')

            assigned_rows.update(child_fba.row)
            if (not child_fba.empty) and len(child_fba.query('FlowAmount != 0')) > 0:
                child_fba_list.append(child_fba.drop(columns='row'))
            else:
                log.error(
                    f'Activity set {child_fba.full_name} is empty. '
                    f'Check activity set definition!'
                )

        if set(parent_fba.row) - assigned_rows:
            log.warning(
                f'Some rows from {parent_fba.full_name} not assigned to an '
                f'activity set. Is this intentional?'
            )
            unassigned = parent_fba.query('row not in @assigned_rows')  # noqa: F841

        return child_fba_list

    def convert_units_and_flows(self: 'FlowByActivity') -> 'FlowByActivity':
        if 'emissions_factors' in self.config:
            self = self.convert_activity_to_emissions()
        if 'adjustment_factor' in self.config:
            # ^^^ TODO: There has to be a better way to do this.
            self = self.assign(
                FlowAmount=self.FlowAmount * self.config['adjustment_factor']
            )

        self = self.convert_daily_to_annual()
        if self.config.get('fedefl_mapping'):
            mapped = self.map_to_fedefl_list(
                drop_unmapped_rows=self.config.get('drop_unmapped_rows', False)
            )
        else:
            mapped = self.rename(
                columns={'FlowName': 'Flowable', 'Compartment': 'Context'}
            )
        if self.config.get('standardize_units', True):
            mapped = mapped.standardize_units()

        return mapped

    def convert_activity_to_emissions(self: 'FlowByActivity') -> 'FlowByActivity':
        '''
        This method converts flows of an activity (most commonly a measure of
        fuel burned) into flows of one or more pollutants. This is a first
        draft, so it may need some refinement.

        Emissions factors may be specified in a .csv file, with whatever
        columns need to be matched on for accurate conversion from activity to
        emissions.
        '''
        emissions_factors = pd.read_csv(
            settings.datapath / f'{self.config["emissions_factors"]}.csv'
        ).drop(columns='source')

        emissions_fba = (
            self.merge(emissions_factors, how='left')
            .assign(
                FlowName=lambda x: x.pollutant,
                FlowAmount=lambda x: x.FlowAmount * x.emissions_factor,
                Unit=lambda x: x.target_unit,
                Class='Chemicals',
                FlowType='ELEMENTARY_FLOW',
            )
            .drop(columns=['pollutant', 'target_unit', 'emissions_factor'])
            .add_primary_secondary_columns('Activity')
            .assign(
                ActivityProducedBy=lambda x: x.PrimaryActivity,
                ActivityConsumedBy=lambda x: x.SecondaryActivity,
            )
            # ^^^ TODO: This is a line I'm quite skeptical of. There's got to
            #     be a better way to do this. Maybe specify in the config?
            .drop(columns=['PrimaryActivity', 'SecondaryActivity'])
        )
        return emissions_fba


"""
The three classes extending pd.Series, together with the _constructor...
methods of each class, are required for allowing pandas methods called on
objects of these classes to return objects of these classes, as desired.

For more information, see
https://pandas.pydata.org/docs/development/extending.html
"""


class _FBASeries(pd.Series):
    _metadata = [*FlowByActivity()._metadata]

    @property
    def _constructor(self) -> type[_FBASeries]:
        return _FBASeries

    @property
    def _constructor_expanddim(self) -> type[FlowByActivity]:
        return FlowByActivity


def getFlowByActivity(
    datasource: str,
    year: int,
    git_version: str | None = None,
    flowclass: str | list[str] | None = None,
    geographic_level: str | None = None,
    download_FBA_if_missing: bool = DEFAULT_DOWNLOAD_IF_MISSING,
) -> pd.DataFrame:
    """
    Retrieves stored data in the FlowByActivity format
    :param datasource: str, the code of the datasource.
    :param year: int, a year, e.g. 2012
    :param flowclass: str or list, a 'Class' of the flow. Optional. E.g.
    'Water' or ['Employment', 'Chemicals']
    :param geographic_level: str, a geographic level of the data.
                             Optional. E.g. 'national', 'state', 'county'.
    :param download_FBA_if_missing: bool, if True will attempt to load from
        remote server prior to generating if file not found locally
    :return: a pandas DataFrame in FlowByActivity format
    """
    fba = FlowByActivity.return_FBA(
        full_name=datasource,
        config={},
        year=int(year),
        git_version=git_version,
        download_ok=download_FBA_if_missing,
    )

    if len(fba) == 0:
        raise FBANotAvailableError(
            message=f"Error generating {datasource} for {str(year)}"
        )
    if flowclass is not None:
        fba = fba.query('Class == @flowclass')
    # if geographic level specified, only load rows in geo level
    if geographic_level is not None:
        fba = filter_by_geoscale(fba, geographic_level)  # type: ignore[assignment]
    return pd.DataFrame(fba.reset_index(drop=True))
