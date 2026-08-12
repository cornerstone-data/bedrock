import re
from typing import Any, Literal

import numpy as np
import pandas as pd

from bedrock.transform.flowbyfunctions import aggregator
from bedrock.utils.config import common, settings
from bedrock.utils.logging.flowsa_log import log, vlog
from bedrock.utils.mapping.dqi import adjust_dqi_reliability_collection_scores

# Level names per schema, coarsest → finest. New hierarchical schemas
# should be added here and require a `{SCHEMA}_{year}_Crosswalk` with those columns.
SECTOR_HIERARCHY_ORDER: dict[str, tuple[str, ...]] = {
    # NAICS_7 = unofficial extensions (e.g. waste 5629201–3) kept in NAICS_{year}_Crosswalk
    "naics": ("NAICS_2", "NAICS_3", "NAICS_4", "NAICS_5", "NAICS_6", "NAICS_7"),
    "bea": ("Sector", "Summary", "Detail"),
}

_SECTOR_SOURCE_NAME_RE = re.compile(r'^([A-Za-z]+)_(\d{4})_Code$')


def parse_sector_source_name(name: str) -> tuple[str, int]:
    """Parse `{SCHEMA}_{year}_Code` → (schema_lower, year)."""
    m = _SECTOR_SOURCE_NAME_RE.fullmatch(str(name))
    if not m:
        raise ValueError(
            f'Cannot parse SectorSourceName {name!r}; expected {{SCHEMA}}_{{year}}_Code'
        )
    return m.group(1).lower(), int(m.group(2))


def sector_source_name(schema: str, year: int) -> str:
    """Build honest SectorSourceName for a schema/year."""
    return f'{schema.upper()}_{year}_Code'


def _schema_block_year(block: dict[str, Any], default_year: int) -> int:
    """Resolve classification year for one industry_spec schema block.

    Top-level ``target_schema_year`` is the default; a block may override with
    the same key name::

        target_schema_year: 2017
        industry_spec:
          default_schema: naics
          naics:
            target_schema_year: 2022
            default_level: NAICS_3
          bea:
            default_level: Detail
            Detail: ['F01000']
    """
    if 'year' in block:
        raise ValueError(
            "industry_spec schema blocks use 'target_schema_year' (not 'year') "
            "to override the method-level target_schema_year"
        )
    if 'target_schema_year' in block:
        return int(block['target_schema_year'])
    return int(default_year)


_INDUSTRY_SPEC_TOP_META_KEYS = frozenset({'default_schema'})
_INDUSTRY_SPEC_META_KEYS = frozenset({'default_level', 'target_schema_year', 'codes'})


def return_schema_crosswalk(schema: str, year: int) -> pd.DataFrame:
    """Load hierarchy columns for a registered schema.

    - ``naics`` (and other file-backed schemas): ``{SCHEMA}_{year}_Crosswalk.csv``
    - ``bea``: derived from ``NAICS_to_BEA_Crosswalk_{year}`` as
      ``Sector`` / ``Summary`` / ``Detail`` — do **not** ship a separate
      ``BEA_{year}_Crosswalk.csv``.
    """
    if schema == 'bea':
        n2b = common.load_crosswalk(f'NAICS_to_BEA_Crosswalk_{year}')
        rename = {
            f'BEA_{year}_Sector_Code': 'Sector',
            f'BEA_{year}_Summary_Code': 'Summary',
            f'BEA_{year}_Detail_Code': 'Detail',
        }
        missing = [c for c in rename if c not in n2b.columns]
        if missing:
            raise ValueError(
                f'NAICS_to_BEA_Crosswalk_{year} missing columns {missing}'
            )
        return (
            n2b[list(rename)]
            .rename(columns=rename)
            .drop_duplicates()
            .reset_index(drop=True)
        )
    return common.load_crosswalk(f'{schema.upper()}_{year}_Crosswalk')


def _industry_spec_key_hierarchical(
    schema: str,
    block: dict[str, Any],
    year: int,
    *,
    full_tree: bool,
) -> pd.DataFrame:
    """Build source→target key for one hierarchical schema block.

    ``default_level`` is the target hierarchy column. Other level keys refine
    listed industries to that column (same mask semantics for every schema).

    When ``full_tree`` is False (non-default schema), level lists are required
    and the key only includes industries matching those lists.
    """
    block_year = _schema_block_year(block, year)
    cw = return_schema_crosswalk(schema, block_year)
    default_col = block['default_level']
    assert isinstance(default_col, str), "'default_level' must be a string column name"
    level_items = {
        k: v for k, v in block.items() if k not in _INDUSTRY_SPEC_META_KEYS
    }
    if not full_tree:
        if not level_items:
            raise ValueError(
                f"industry_spec[{schema!r}] is not default_schema and must list "
                f"industries under hierarchy level keys (e.g. Detail: [...])"
            )
        listed: list[Any] = []
        for industries in level_items.values():
            if isinstance(industries, str):
                listed.append(industries)
            else:
                listed.extend(industries)
        cw = cw.loc[cw.isin(listed).any(axis=1)].copy()
        if cw.empty:
            raise ValueError(
                f"industry_spec[{schema!r}] level lists matched no hierarchy rows: "
                f"{listed}"
            )
    cw = cw.assign(target_sector=cw[default_col])
    for level, industries in level_items.items():
        if isinstance(industries, str):
            industries = [industries]
        cw['target_sector'] = cw['target_sector'].mask(
            cw.drop(columns='target_sector').isin(industries).any(axis='columns'),
            cw[level],
        )
    key = cw.melt(id_vars='target_sector', value_name='source_sector')
    key = (
        key[['source_sector', 'target_sector']]
        .dropna()
        .drop_duplicates()
        .assign(SectorSourceName=sector_source_name(schema, block_year))
    )
    return key


def industry_spec_key(
    industry_spec: dict[str, Any],
    year: int,
) -> pd.DataFrame:
    """
    Provides a key for mapping sector codes to a target industry breakdown.

    industry_spec must be nested by schema (hard cut — no flat legacy shape)::

        industry_spec = {
            'default_schema': 'naics',
            'naics': {
                'default_level': 'NAICS_3',
                'NAICS_4': ['112', '113'],
                'NAICS_6': ['1129'],
            },
            'bea': {
                'default_level': 'Detail',
                'Detail': ['F01000', 'S00102'],
            },
        }

    ``default_schema`` is the primary schema (full hierarchy + refinements).
    Other schema blocks must list industries under level keys (keep set).

    Method-level ``target_schema_year`` is passed as ``year`` and applies to every
    block unless a block sets its own ``target_schema_year`` override.

    Returns columns ``source_sector``, ``target_sector``, ``SectorSourceName``.
    Flat / unversioned schemas use a ``codes`` list for identity mapping.
    """
    if 'default' in industry_spec:
        raise ValueError(
            'Flat industry_spec is no longer supported; nest under schema keys '
            '(naics:, bea:, ...) with default_level / default_schema.'
        )

    default_schema = industry_spec.get('default_schema')
    schema_blocks = {
        k: v
        for k, v in industry_spec.items()
        if k not in _INDUSTRY_SPEC_TOP_META_KEYS
    }

    hierarchy_schemas = [
        s
        for s, b in schema_blocks.items()
        if isinstance(b, dict) and s in SECTOR_HIERARCHY_ORDER
    ]
    if len(hierarchy_schemas) > 1 and default_schema is None:
        raise ValueError(
            'industry_spec with multiple schemas requires default_schema '
            f'(got {sorted(hierarchy_schemas)})'
        )
    if default_schema is not None and default_schema not in schema_blocks:
        raise ValueError(
            f'default_schema {default_schema!r} has no matching industry_spec block'
        )

    parts: list[pd.DataFrame] = []
    for schema, block in schema_blocks.items():
        if not isinstance(block, dict):
            raise ValueError(
                f'industry_spec[{schema!r}] must be a dict, got {type(block).__name__}'
            )
        if schema in SECTOR_HIERARCHY_ORDER:
            full_tree = default_schema is None or schema == default_schema
            parts.append(
                _industry_spec_key_hierarchical(
                    schema, block, year, full_tree=full_tree
                )
            )
        elif 'codes' in block:
            codes = block['codes']
            if isinstance(codes, str):
                codes = [codes]
            block_year = _schema_block_year(block, year)
            parts.append(
                pd.DataFrame(
                    {
                        'source_sector': codes,
                        'target_sector': codes,
                        'SectorSourceName': sector_source_name(schema, block_year),
                    }
                )
            )
        else:
            raise ValueError(
                f'Unknown schema {schema!r}; register in SECTOR_HIERARCHY_ORDER '
                f'or provide a flat block with a codes list'
            )

    if not parts:
        raise ValueError('industry_spec is empty')

    return (
        pd.concat(parts, ignore_index=True)
        .drop_duplicates()
        .sort_values(by=['SectorSourceName', 'source_sector', 'target_sector'])
        .reset_index(drop=True)
    )


def sector_hierarchy_from_config(config: Any) -> str | None:
    """Resolve activity-row nesting from nested activity_schema (or legacy key)."""
    if not isinstance(config, dict):
        config = dict(config) if config is not None else {}
    if config.get('sector_hierarchy') is not None:
        return config.get('sector_hierarchy')
    raw = config.get('activity_schema')
    if isinstance(raw, dict):
        for block in raw.values():
            if isinstance(block, dict) and block.get('hierarchy') is not None:
                return block.get('hierarchy')
    return None


def subset_sector_key(
    flowbyactivity: pd.DataFrame,
    activitycol: str,
    sector_source_year: str,
    primary_sector_key: pd.DataFrame,
    secondary_sector_key: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Subset the sector key to return an industry that most closely maps source sectors to target
    sectors by matching on sector length, based on the sectors that are in the FBA

    @param flowbyactivity: FBA (if activities are sector like) or df of activity to sector mapping
    (if activities are text based) that contains activity data
    @param activitycol:
    @param primary_sector_key:
    @param secondary_sector_key:
    @return:
    """

    # if the primary sector key is the activity to sector crosswalk, which is the case for FBAs with non-sector-like
    # activities, merge with the secondary sector key (the naics industry key) to pull in target sectors and tech
    # corr scoring
    group_cols = ["target_sector", "Class", "Flowable", "Context"]
    merge_col = "source_sector"
    drop_col = activitycol
    if "Activity" in primary_sector_key.columns:
        group_cols = group_cols + ["Activity"]
        merge_col = "Activity"
        drop_col = "source_sector"

        assert (
            secondary_sector_key is not None
        ), "secondary_sector_key required when Activity column present"
        # Match activity CW to industry key on schema (naics/bea), not vintage —
        # activity SectorSourceName year may differ from the target on the
        # industry key.
        if (
            'SectorSourceName' in primary_sector_key.columns
            and 'SectorSourceName' in secondary_sector_key.columns
        ):
            # Incomplete CW rows (null SectorSourceName) cannot join by schema.
            null_sec_source_name = primary_sector_key['SectorSourceName'].isna() | (
                primary_sector_key['SectorSourceName'].astype(str).str.strip() == ''
            )
            if null_sec_source_name.any():
                dropped = primary_sector_key.loc[
                    null_sec_source_name, ['Activity', 'Sector']
                ].drop_duplicates()
                log.warning(
                    f'Dropping {len(dropped)} activity-to-sector CW rows with '
                    f'null SectorSourceName: '
                    f'{set(zip(dropped["Activity"], dropped["Sector"]))}'
                )
                primary_sector_key = primary_sector_key.loc[
                    ~null_sec_source_name
                ].copy()
            primary_sector_key = primary_sector_key.assign(
                _schema=primary_sector_key['SectorSourceName'].map(
                    lambda s: parse_sector_source_name(s)[0]
                )
            ).drop(columns=['SectorSourceName'])
            secondary_sector_key = secondary_sector_key.assign(
                _schema=secondary_sector_key['SectorSourceName'].map(
                    lambda s: parse_sector_source_name(s)[0]
                )
            )
            primary_sector_key = primary_sector_key.merge(
                secondary_sector_key,
                how='left',
                left_on=['Sector', '_schema'],
                right_on=['source_sector', '_schema'],
            ).drop(columns=['_schema'], errors='ignore')
        else:
            primary_sector_key = primary_sector_key.merge(
                secondary_sector_key,
                how='left',
                left_on='Sector',
                right_on='source_sector',
            )
        # print where values are not mapped
        unmapped = primary_sector_key.query('source_sector.isnull()')
        if len(unmapped) > 0:
            log.warning(
                f'Activities are unmapped for '
                f'{set(zip(unmapped["Activity"], unmapped["Sector"]))}'
            )
        # drop null values and sector col
        primary_sector_key = primary_sector_key.dropna(subset=['source_sector']).drop(
            columns=['Sector']
        )
    # else, if activities are sector-like
    else:
        # if activities end in ".0", strip characters from activity (noted in some data pulled from stewi)
        flowbyactivity[activitycol] = flowbyactivity[activitycol].str.replace(".0", "")
        # if activities are sector-like, drop all sectors that are not in sector crosswalk, due to datasets such as
        # BLS QCEW which often has non-traditional NAICS6, but the parent NAICS5 do map correctly to sectors
        flowbyactivity = flowbyactivity[
            flowbyactivity[activitycol].isin(primary_sector_key["source_sector"].values)
        ]

    # drop rows of data where activitycol value is null - no mapping required
    flowbyactivity = flowbyactivity.query(f'~{activitycol}.isnull()')
    # want to best match class/flowable/context/activities combos with target sectors, retain both activity columns
    # for situations where an activity can be listed in both columns for different circumstances
    subset_cols = [
        'Class',
        'Flowable',
        'Context',
        'ActivityProducedBy',
        'ActivityConsumedBy',
        'DataReliability',
        'DataCollection',
    ]
    # list DQI columns in df
    dqi = [
        col
        for col in ['DataReliability', 'DataCollection']
        if col in flowbyactivity.columns
    ]
    # Drop missing DQI columns from subset list
    subset_cols = [
        col
        for col in subset_cols
        if col not in ['DataReliability', 'DataCollection'] or col in dqi
    ]
    # ensure dq column decimals do not cause errors with dropping duplicates, without this statement, rows often
    # duplicated
    if dqi:
        flowbyactivity.loc[:, dqi] = flowbyactivity.loc[:, dqi].round(decimals=5)
    flowbyactivity = flowbyactivity[subset_cols].drop_duplicates()

    primary_sector_key_2 = (
        pd.DataFrame(
            flowbyactivity.merge(
                primary_sector_key,
                how='left',
                left_on=activitycol,
                right_on=merge_col,
            )
        )
        .dropna(subset=[merge_col])
        .drop(columns=activitycol)
    )

    # drop parent sectors if parent-completechild
    if sector_hierarchy_from_config(flowbyactivity.config) == 'parent-completeChild':

        def drop_parent_sectors(sector_key: pd.DataFrame) -> pd.DataFrame:
            sector_list = sector_key['source_sector'].astype(str).tolist()

            def is_parent(x: str) -> bool:
                return any(
                    sector != x and sector.startswith(x) for sector in sector_list
                )

            return sector_key[~sector_key['source_sector'].astype(str).apply(is_parent)]

        # todo: check futurewarning dataframegroupby.apply fix working as expected
        primary_sector_key_2 = primary_sector_key_2.groupby(
            ['Class', 'Flowable', 'Context'], group_keys=False, dropna=False
        )[primary_sector_key_2.columns.tolist()].apply(drop_parent_sectors)

    # modify dqi scores for data reliability and collection based on mapping
    if "DataReliability" in flowbyactivity.columns:
        primary_sector_key_2 = adjust_dqi_reliability_collection_scores(
            primary_sector_key_2, sector_source_year
        )

    # Keep rows where source = target
    df_keep = primary_sector_key_2[
        primary_sector_key_2["source_sector"] == primary_sector_key_2["target_sector"]
    ].reset_index(drop=True)

    # subset df to all remaining target sectors and Activity if present by dropping the one to one matches
    df_remaining = primary_sector_key_2[
        primary_sector_key_2["source_sector"] != primary_sector_key_2["target_sector"]
    ].reset_index(drop=True)

    # function to identify which source naics most closely match to the target naics
    def subset_target_sectors_by_source_sectors(group: pd.DataFrame) -> pd.DataFrame:
        target = group["target_sector"].iloc[0]
        target_length = len(target)

        # first check for length source > length target
        group_filtered_greater = group[group["source_sector"].apply(len) > target_length]
        if not group_filtered_greater.empty:
            # keep rows where source length is smallest greater length
            min_source_length = min(group_filtered_greater["source_sector"].apply(len))
            result_greater = group_filtered_greater[
                group_filtered_greater["source_sector"].apply(len) == min_source_length
            ]
            # drop the greater data from the remainder df before looking for shorter lengths
            if "Activity" in group.columns:
                group = group[
                    ~(
                        (group["target_sector"].isin(result_greater["target_sector"]))
                        & (group["Activity"].isin(result_greater["Activity"]))
                    )
                ]
            else:
                group = group[
                    ~group["target_sector"].isin(result_greater["target_sector"])
                ]
        else:
            result_greater = pd.DataFrame()

        # if there are no source length greater than target, check for source values shorter
        group_filtered_shorter = group[group["source_sector"].apply(len) < target_length]
        if not group_filtered_shorter.empty:
            # keep rows where source length is smallest smaller length
            max_source_length = max(group_filtered_shorter["source_sector"].apply(len))
            result_shorter = group_filtered_shorter[
                group_filtered_shorter["source_sector"].apply(len) == max_source_length
            ]
        else:
            result_shorter = pd.DataFrame()
        return pd.concat([result_greater, result_shorter], ignore_index=True)

    if sector_hierarchy_from_config(flowbyactivity.config) == 'parent-incompleteChild':
        df_remaining_mapped = df_remaining.copy()
    else:
        # todo: check impact of changing code to remove future warning
        df_remaining_mapped = (
            df_remaining.groupby(group_cols, dropna=False, group_keys=False)[
                df_remaining.columns.tolist()
            ].apply(subset_target_sectors_by_source_sectors)
            # .reset_index(drop=True)
        )

    mapping = pd.concat([df_keep, df_remaining_mapped], ignore_index=True)

    # depending on if activities are naics-like or not determines which column to drop,
    # if text based activities, drop duplicates.
    # Necessary when source activities initially map to a finer resolution NAICS level
    mapping = (
        mapping.drop(
            columns=drop_col, errors='ignore'
        )  # if activities naics-like, already dropped col
        .drop_duplicates()
        .reset_index(drop=True)
        # rename activity column back to original name
        .rename(
            columns={'Activity': f'{activitycol}', 'source_sector': f'{activitycol}'},
            errors='ignore',
        )
    )

    return mapping


def map_target_sectors_to_less_aggregated_sectors(
    industry_spec: dict[str, Any],
    year: int,
) -> pd.DataFrame:
    """
    Map each target sector to coarser hierarchy parents for equal attribution.

    Returns ``target_sector``, ``SectorSourceName``, and ``_hier_{i}`` columns where
    ``i`` is the 0-based index into ``SECTOR_HIERARCHY_ORDER[schema]`` (coarsest
    first). Levels finer than the target are set to NaN.
    """
    if 'default' in industry_spec:
        raise ValueError(
            'Flat industry_spec is no longer supported; nest under schema keys '
            '(naics:, bea:, ...) with default_level / default_schema.'
        )

    default_schema = industry_spec.get('default_schema')
    parts: list[pd.DataFrame] = []
    max_depth = 0
    for schema, block in industry_spec.items():
        if schema in _INDUSTRY_SPEC_TOP_META_KEYS:
            continue
        if not isinstance(block, dict):
            continue
        if schema not in SECTOR_HIERARCHY_ORDER:
            # Flat schemas: identity only (no hierarchical equal-attr peers)
            if 'codes' in block:
                codes = block['codes']
                if isinstance(codes, str):
                    codes = [codes]
                block_year = _schema_block_year(block, year)
                parts.append(
                    pd.DataFrame(
                        {
                            'target_sector': codes,
                            'SectorSourceName': sector_source_name(schema, block_year),
                            '_hier_0': codes,
                        }
                    )
                )
                max_depth = max(max_depth, 1)
            continue

        order = SECTOR_HIERARCHY_ORDER[schema]
        max_depth = max(max_depth, len(order))
        block_year = _schema_block_year(block, year)
        cw = return_schema_crosswalk(schema, block_year)
        default_col = block['default_level']
        assert isinstance(
            default_col, str
        ), "'default_level' must be a string column name"
        level_items = {
            k: v for k, v in block.items() if k not in _INDUSTRY_SPEC_META_KEYS
        }
        full_tree = default_schema is None or schema == default_schema
        if not full_tree:
            if not level_items:
                raise ValueError(
                    f"industry_spec[{schema!r}] is not default_schema and must "
                    f"list industries under hierarchy level keys"
                )
            listed: list[Any] = []
            for industries in level_items.values():
                if isinstance(industries, str):
                    listed.append(industries)
                else:
                    listed.extend(industries)
            cw = cw.loc[cw.isin(listed).any(axis=1)].copy()
        cw = cw.assign(target_sector=cw[default_col])
        for level, industries in level_items.items():
            if isinstance(industries, str):
                industries = [industries]
            cw['target_sector'] = cw['target_sector'].mask(
                cw.drop(columns='target_sector').isin(industries).any(axis='columns'),
                cw[level],
            )

        # Index of each hierarchy column; null levels finer than the target
        level_to_idx = {lvl: i for i, lvl in enumerate(order)}
        # Resolve target level index per row (which hierarchy col equals target)
        target_idx = pd.Series(np.nan, index=cw.index, dtype=float)
        for lvl, i in level_to_idx.items():
            if lvl in cw.columns:
                target_idx = target_idx.mask(cw[lvl] == cw['target_sector'], float(i))
        # Fallback: if target not found in hierarchy cols, keep finest present
        target_idx = target_idx.fillna(float(len(order) - 1))

        out = cw[['target_sector']].copy()
        out['SectorSourceName'] = sector_source_name(schema, block_year)
        for i, lvl in enumerate(order):
            col = f'_hier_{i}'
            if lvl in cw.columns:
                out[col] = np.where(i <= target_idx, cw[lvl], np.nan)
            else:
                out[col] = np.nan
        parts.append(out.drop_duplicates())

    if not parts:
        raise ValueError('industry_spec is empty')

    result = pd.concat(parts, ignore_index=True)
    for i in range(max_depth):
        col = f'_hier_{i}'
        if col not in result.columns:
            result[col] = np.nan
    return result.drop_duplicates().reset_index(drop=True)


def map_source_sectors_to_more_aggregated_sectors(
    year: Literal[2002, 2007, 2012, 2017],
) -> pd.DataFrame:
    """
    Map source NAICS to all possible other sector lengths
    parent-childhierarchy
    """
    naics_crosswalk = return_schema_crosswalk('naics', year)

    naics = []
    for n in naics_crosswalk.columns.values.tolist():
        naics_sub = naics_crosswalk.assign(source_sector=naics_crosswalk[n])
        naics.append(naics_sub)

    # concat data into single dataframe
    naics_key = pd.concat(naics, sort=False)
    naics_key = naics_key.dropna(subset=['source_sector'])

    # drop source_sector that are more aggregated than target_sector, reorder
    for n in range(2, 8):
        naics_key[f'NAICS_{n}'] = np.where(
            naics_key[f'NAICS_{n}'].str.len() > naics_key['source_sector'].str.len(),
            np.nan,
            naics_key[f'NAICS_{n}'],
        )

    # rename columns to align with previous code
    naics_key = naics_key.rename(
        columns={
            'NAICS_2': 'n2',
            'NAICS_3': 'n3',
            'NAICS_4': 'n4',
            'NAICS_5': 'n5',
            'NAICS_6': 'n6',
            'NAICS_7': 'n7',
        }
    )

    return naics_key.drop_duplicates()


def map_source_sectors_to_less_aggregated_sectors(
    year: Literal[2002, 2007, 2012, 2017],
) -> pd.DataFrame:
    """
    Map source NAICS to all possible other sector lengths
    parent-childhierarchy
    """
    naics_crosswalk = return_schema_crosswalk('naics', year)

    naics = []
    for n in naics_crosswalk.columns.values.tolist():
        naics_sub = naics_crosswalk.assign(source_sector=naics_crosswalk[n])
        naics.append(naics_sub)

    # concat data into single dataframe
    naics_key = pd.concat(naics, sort=False)
    naics_key = naics_key.dropna(subset=['source_sector'])

    # drop source_sector that are more aggregated than target_sector, reorder
    for n in range(2, 8):
        naics_key[f'NAICS_{n}'] = np.where(
            naics_key[f'NAICS_{n}'].str.len() < naics_key['source_sector'].str.len(),
            np.nan,
            naics_key[f'NAICS_{n}'],
        )

    cw_melt = (
        naics_key.melt(
            id_vars="source_sector", var_name="SectorLength", value_name='Sector'
        )
        .drop_duplicates()
        .reset_index(drop=True)
    )

    cw_melt = (
        (cw_melt.query("source_sector != Sector").query("~Sector.isna()"))
        .drop_duplicates()
        .reset_index(drop=True)
    )

    return cw_melt


def year_crosswalk(
    source_year: Literal[2002, 2007, 2012, 2017, 2022],
    target_year: Literal[2002, 2007, 2012, 2017, 2022],
) -> pd.DataFrame:
    '''
    Provides a key for switching between years of the NAICS specification.

    :param source_year: int, one of 2002, 2007, 2012, or 2017.
    :param target_year: int, one of 2002, 2007, 2012, or 2017.
    :return: pd.DataFrame with columns 'source_sector' and 'target_sector',
        corresponding to NAICS codes for the source and target specifications.
    '''
    return (
        pd.read_csv(
            settings.datapath / 'NAICS_Crosswalk_TimeSeries.csv', dtype='object'
        )
        .assign(
            source_sector=lambda x: x[f'NAICS_{source_year}_Code'],
            target_sector=lambda x: x[f'NAICS_{target_year}_Code'],
        )[['source_sector', 'target_sector']]
        .drop_duplicates()
        .reset_index(drop=True)
    )


def check_if_sectors_are_naics(
    df_load: pd.DataFrame, crosswalk_list: list[str], column_headers: list[str]
) -> list[str]:
    """
    Check if activity-like sectors are in fact sectors.
    Also works for the Sector column
    :param df_load: df with activity or sector columns
    :param crosswalk_list: list, sectors found in crosswalk
    :param column_headers: list, headers to check for sectors
    :return: list, values that are not sectors
    """

    # create a df of non-sectors to export
    non_sectors_df: list[pd.DataFrame] = []
    # create a df of just the non-sectors column
    non_sectors_list: list[pd.DataFrame] = []
    # loop through the df headers and determine if value
    # is not in crosswalk list
    for c in column_headers:
        # create df where sectors do not exist in master crosswalk
        non_sectors_filtered = df_load[~df_load[c].isin(crosswalk_list)]
        # drop rows where c is empty
        non_sectors_filtered = non_sectors_filtered[~non_sectors_filtered[c].isna()]
        # subset to just the sector column
        if len(non_sectors_filtered) != 0:
            sectors = non_sectors_filtered[[c]].rename(columns={c: 'NonSectors'})
            non_sectors_df.append(non_sectors_filtered)
            non_sectors_list.append(sectors)

    non_sectors_result: list[str] = []
    if len(non_sectors_df) != 0:
        # concat the df and the df of sectors
        ns_list = pd.concat(non_sectors_list, sort=False, ignore_index=True)
        # print the NonSectors
        non_sectors_result = ns_list['NonSectors'].drop_duplicates().tolist()
        vlog.debug('There are sectors that are not target NAICS Codes')
        vlog.debug(non_sectors_result)

    return non_sectors_result


def generate_naics_crosswalk_conversion_ratios(
    sectorsourcename: str, targetsectorsourcename: str
) -> pd.DataFrame:
    """
    Create a melt version of the source naics source years crosswalk to map
    naics to naics target year
    :param sectorsourcename: str, the source sector year
    :param targetsectorsourcename: str, the target sector year, such as
    "NAICS_2012_Code"
    :return: df, naics crosswalk melted
    """

    # load the mastercroswalk and subset by sectorsourcename,
    # save values to list
    df = common.load_crosswalk('NAICS_Year_Concordance')[
        [sectorsourcename, targetsectorsourcename]
    ].drop_duplicates()

    all_ratios = []

    # Calculate allocation ratios for each length
    for length in range(6, 1, -1):
        # Truncate both NAICS and NAICS_2017_Code to the current string length
        df['source'] = df[f'{sectorsourcename}'].str[:length]
        df['target'] = df[f'{targetsectorsourcename}'].str[:length]

        # Group by the truncated NAICS codes
        df_grouped = (
            df.groupby(['source', 'target']).size().reset_index(name='naics_count')
        )

        # Calculate the allocation ratios
        df_grouped['allocation_ratio'] = df_grouped.groupby('source')[
            'naics_count'
        ].transform(lambda x: x / x.sum())

        # Add the length to the results
        df_grouped['length'] = length

        # Collect the results
        all_ratios.append(df_grouped)

    # Combine all ratios into a single DataFrame
    ratios_df = pd.concat(all_ratios, ignore_index=True)

    # TODO: modify how unofficial sectors are added - ensure correct mapping between years
    # append the unofficial sector codes
    year_match = re.search(r'\d+', sectorsourcename)
    year_df = common.load_sector_length_cw_melt(
        year_match.group() if year_match else '2012'
    )
    year_df = year_df[year_df['SectorLength'] > 6]
    year_df = year_df.rename(columns={'Sector': 'source', 'SectorLength': 'length'})
    year_df['target'] = year_df['source']
    year_df['naics_count'] = 1
    year_df['allocation_ratio'] = 1

    # add unofficial sectors
    ratios_df = pd.concat([ratios_df, year_df], ignore_index=True)

    # rename cols
    ratios_df = ratios_df.rename(
        columns={'source': 'NAICS', 'target': f'{targetsectorsourcename}'}
    )

    # drop gov and household codes by length
    ratios_df = ratios_df[
        ~(
            (ratios_df['NAICS'].str.startswith('F0'))
            & (ratios_df['NAICS'].str.len() < 4)
        )
    ]
    ratios_df = ratios_df[
        ~(
            (ratios_df['NAICS'].str.startswith('S0'))
            & (ratios_df['NAICS'].str.len() < 6)
        )
    ]

    return ratios_df


def return_closest_naics_year(
    nonsectors: list[str], mapping: pd.DataFrame, targetsectorsourcename: str
) -> dict[str, str]:
    """
    Match sectors to the closest NAICS year to target naics using naics timeseries.
    """
    naics_years = mapping.columns
    sector_year_match = {}
    for sector in nonsectors:
        closest_year = ''
        min_difference = 100
        for year in naics_years:
            if year in mapping.columns and sector in mapping[year].values:
                difference = abs(
                    int(year.split('_')[1]) - int(targetsectorsourcename.split('_')[1])
                )
                if difference < min_difference:
                    closest_year = year
                    min_difference = difference

        sector_year_match[sector] = closest_year

    return sector_year_match


def replace_sectors_with_targetsectors(
    df: pd.DataFrame,
    non_naics: list[str],
    cw_melt: pd.DataFrame,
    column_headers: list[str],
    targetsectorsourcename: str,
) -> pd.DataFrame:
    """
    Replacing sectors with those of the target yeear
    :param df:
    :param non_naics:
    :param cw_melt:
    :param column_headers:
    :param targetsectorsourcename:
    :return:
    """
    for c in column_headers:
        if df[c].isna().all():
            continue
        # merge df with the melted sector crosswalk
        df = df.merge(cw_melt, left_on=c, right_on='NAICS', how='left')
        # if there is a value in the sectorsourcename column,
        # use that value to replace sector in column c if value in
        # column c is in the non_naics list
        df[c] = np.where(
            (df[c] == df['NAICS']) & (df[c].isin(non_naics)),
            df[targetsectorsourcename],
            df[c],
        )
        # multiply the FlowAmount col by allocation_ratio
        df.loc[df[c] == df[targetsectorsourcename], 'FlowAmount'] = (
            df['FlowAmount'] * df['allocation_ratio']
        )
        # drop columns
        df = df.drop(columns=[targetsectorsourcename, 'NAICS', 'allocation_ratio'])
    # replace the sector year in the SectorSourceName column
    if 'SectorSourceName' in df.columns:
        df['SectorSourceName'] = targetsectorsourcename

    return df


def convert_naics_year(
    df_load: pd.DataFrame,
    targetsectorsourcename: str,
    sectorsourcename: str,
    dfname: str,
) -> pd.DataFrame:
    """
    Convert sector year
    :param df_load: df with sector columns or sector-like activities
    :param sectorsourcename: str, sector source name (ex. NAICS_2012_Code)
    :param dfname: str, name of data source
    :return: df, with sectors replaced with new sector year
    """
    # todo: update this function to work better with recursive method

    # todo: ensure non-naics (7-digits, etc are converted)

    # determine which headers are in the df
    column_headers = ['ActivityProducedBy', 'ActivityConsumedBy']
    if 'SectorConsumedBy' in df_load:
        column_headers = ['SectorProducedBy', 'SectorConsumedBy']
    if 'Sector' in df_load:
        column_headers = ['Sector']

    # if activities are naics-like, also update the NAICS in the activity cols. necessary for aggregation and
    # resetting the group totals - otherwise "direct" allocation will be forced to "equal" allocation and the FBS
    # results will be incorrect
    try:
        if df_load.config['data_format'] in ['FBS']:
            activity_schema = "NAICS"
        else:
            raw = df_load.config.get('activity_schema')
            if isinstance(raw, dict) and raw is not None:
                # Nested catalog shape: convert only when naics is present
                activity_schema = "NAICS" if 'naics' in raw else "None"
            elif isinstance(raw, str):
                activity_schema = raw
            else:
                activity_schema = (
                    df_load.config.get('activity_schema', {}).get(
                        df_load.config['year']
                    )
                    if isinstance(df_load.config.get('activity_schema'), dict)
                    else "None"
                )
    except AttributeError:
        # The only non FBA/FBS run via FLOWSA are data pulled from stewi, which are naics-based, however, stewi data
        # contains APB and ACB cols and does not have the group_id/group_totals and goes through separate allocation
        # methods, so assigning schema as None
        activity_schema = "None"

        # however, need to ensure that these NAICS are formatted correctly - stewi data are at times imported
        # with some NAICS values including decimals that do not get mapped correctly (ex. '311712.0')
        for col in column_headers:
            if col in df_load.columns:
                df_load[col] = df_load[col].apply(
                    lambda x: x.split(".")[0] if isinstance(x, str) else x
                )

    if "NAICS" in activity_schema and "ActivityProducedBy" in df_load.columns:
        column_headers += ['ActivityProducedBy', 'ActivityConsumedBy']

    # load the mastercrosswalk and subset by sectorsourcename,
    # save values to list
    if targetsectorsourcename == sectorsourcename:
        df = df_load.copy()
        cw_list = (
            common.load_crosswalk("NAICS_Crosswalk_TimeSeries")[targetsectorsourcename]
            .drop_duplicates()
            .tolist()
        )
    else:
        log.info(
            f"Converting {sectorsourcename} to " f"{targetsectorsourcename} in {dfname}"
        )

        # load conversion crosswalk
        cw_melt = generate_naics_crosswalk_conversion_ratios(
            sectorsourcename, targetsectorsourcename
        )
        # drop the count column
        cw_melt = cw_melt.drop(columns=['naics_count', 'length'])
        cw_list = cw_melt[targetsectorsourcename].drop_duplicates().tolist()

        # check if there are any sectors that are not in the naics annual crosswalk
        non_naics = check_if_sectors_are_naics(df_load, cw_list, column_headers)

        # loop through the df headers and determine if value is
        # not in crosswalk list
        df = df_load.copy()
        if len(non_naics) != 0:
            df = replace_sectors_with_targetsectors(
                df, non_naics, cw_melt, column_headers, targetsectorsourcename
            )

    # regardless of if sector year = target sector year, check if there are any non naics that are naics in another
    # naics year. Checking for other years as data out of stewi not always assigned correctly
    nonsectors = check_if_sectors_are_naics(df, cw_list, column_headers)
    # Determine closest NAICS year for non_naics sectors
    if len(nonsectors) > 0:
        log.info(
            'Checking if sectors represent a different '
            f'NAICS year, if so, replace with {targetsectorsourcename}'
        )
        # load entire naics year crosswalk to determine if nonsectors belong to another naics year
        mapping = common.load_crosswalk("NAICS_Crosswalk_TimeSeries")
        sector_year_mapping = return_closest_naics_year(
            nonsectors, mapping, targetsectorsourcename
        )

        # Generate multiple crosswalks based on found NAICS years
        cw_melt_list = []
        for sector, year in sector_year_mapping.items():
            if year:
                cw_melt = generate_naics_crosswalk_conversion_ratios(
                    year, targetsectorsourcename
                )
                cw_melt = cw_melt[cw_melt['NAICS'] == sector].drop(
                    columns=['naics_count', 'length']
                )
                cw_melt_list.append(cw_melt)

        # if sectors were found to represent a different naics year, use those values to map to target naics
        if len(cw_melt_list) > 0:
            # Merge generated crosswalks
            cw_melt = pd.concat(cw_melt_list, ignore_index=True)

            df = replace_sectors_with_targetsectors(
                df, nonsectors, cw_melt, column_headers, targetsectorsourcename
            )
        # check if there are any sectors that are not in
        # the target sector crosswalk and if so, drop those sectors
        log.info(
            'Checking for unconverted NAICS - determine if rows should ' 'be dropped.'
        )
        nonsectors = check_if_sectors_are_naics(df, cw_list, column_headers)

    if len(nonsectors) != 0:
        log.info(f'Dropping non {targetsectorsourcename}s from dataframe: {nonsectors}')
        for c in column_headers:
            if df[c].isna().all():
                continue
            # drop rows where column value is in the nonnaics list
            df = df[~df[c].isin(nonsectors)]

    # if activities are naics-like, must reset group_id and group_total by grouping through the reset APB and ACB
    # columns. This is necessary for cases like QCEW data, where we would be left with duplicate group_id rows when
    # there is a one:many mapping upon converting NAICS years. This function already correctly allocated the
    # FlowAmount to the new sector values. Do not want duplicated group_id rows because later steps in flowsa will
    # further, incorrectly, allocate FlowAmounts (such as through equal allocation)

    # aggregate data
    if hasattr(df, 'aggregate_flowby'):
        if "NAICS" in activity_schema:
            df2 = (
                df.drop(columns=['group_id', 'group_total']).aggregate_flowby(
                    columns_to_group_by=df.groupby_cols  # type: ignore[operator]
                )
            ).reset_index(drop=True)
            df2 = df2.assign(group_id=df2.index, group_total=df2['FlowAmount'])
        else:
            df2 = df.aggregate_flowby(
                columns_to_group_by=df.groupby_cols + ['group_id']  # type: ignore[operator]
            )
    # stewi data are imported as DF, not as FBA/FBS Class Objects
    else:
        possible_column_headers = (
            'FlowAmount',
            'Spread',
            'Min',
            'Max',
            'DataReliability',
            'TemporalCorrelation',
            'GeographicalCorrelation',
            'TechnologicalCorrelation',
            'DataCollection',
            'Description',
        )
        # list of column headers to group aggregation by
        groupby_cols = [
            e for e in df.columns.values.tolist() if e not in possible_column_headers
        ]
        df2 = aggregator(df, groupby_cols)

    return df2


def return_max_sector_level(
    industry_spec: dict[str, str | list[str]],
) -> int:
    """
    Return max sector length/level based on industry spec.

    The industry_spec is a (possibly nested) dictionary formatted as in this
    example:

    industry_spec = {'default': 'NAICS_3',
                     'NAICS_4': ['112', '113'],
                     'NAICS_6': ['1129']
                     }
    """
    # list of keys in industry spec
    level_list = list(industry_spec.keys())
    # append default sector level
    default_val = industry_spec['default']
    if isinstance(default_val, str):
        level_list.append(default_val)

    n: list[int] = []
    for string in level_list:
        # Convert each found number to an integer and extend the result into the list n
        n.extend(map(int, re.findall(r'\d+', string)))

    max_level = max(n)

    return max_level
