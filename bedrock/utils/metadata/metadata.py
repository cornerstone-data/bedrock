# metadata.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Functions for creating and loading metadata files for
FlowByActivity (FBA) and FlowBySector (FBS) datasets
"""

import json
import os
from pathlib import Path
from typing import Any

import pandas as pd
from esupy.processed_data_mgmt import FileMeta, Paths

from bedrock.publish.bibliography import load_source_dict
from bedrock.utils.config.common import (
    get_catalog_info,
    return_true_source_catalog_name,
)
from bedrock.utils.config.settings import (
    FBA_DIR,
    FBS_DIR,
    GIT_HASH,
    GIT_HASH_LONG,
    PKG,
    PKG_VERSION_NUMBER,
    WRITE_FORMAT,
    extractpath,
    return_folder_path,
    transformpath,
)
from bedrock.utils.logging.flowsa_log import log


def set_fb_meta(name_data: str, category: str) -> FileMeta:
    """
    Create meta data for a parquet
    :param name_data: string, name of df
    :param category: string, 'FlowBySector' or 'FlowByActivity'
    :return: object, metadata for parquet
    """
    fb_meta = FileMeta()
    fb_meta.tool = PKG
    fb_meta.category = category
    fb_meta.name_data = name_data
    fb_meta.tool_version = PKG_VERSION_NUMBER
    fb_meta.git_hash = GIT_HASH
    fb_meta.ext = WRITE_FORMAT
    fb_meta.date_created = pd.to_datetime('today').strftime('%Y-%m-%d %H:%M:%S')
    return fb_meta


def write_metadata(
    source_name: str,
    config: dict[str, Any],
    fb_meta: FileMeta,
    pth: str,
    **kwargs: Any,
) -> None:
    """
    Write the metadata and output as a JSON in a local directory
    :param source_name: string, source name for either a FBA or FBS dataset
    :param config: dictionary, configuration file
    :param fb_meta: object, metadata
    :param kwargs: additional parameters, if running for FBA, define
        "year" of data
    :return: object, metadata that includes methodology for FBAs
    """

    fb_meta.tool_meta = return_fb_meta_data(
        source_name, config, fb_meta.category, **kwargs
    )
    fname = f'{fb_meta.name_data}_v{fb_meta.tool_version}'
    if fb_meta.git_hash is not None:
        fname = f'{fname}_{fb_meta.git_hash}'
    fname = f'{pth}/{fname}_metadata.json'
    with open(fname, 'w') as fi:
        fi.write(json.dumps(fb_meta.__dict__, indent=4))


def return_fb_meta_data(
    source_name: str, config: dict[str, Any], category: str, **kwargs: dict[str, Any]
) -> dict[str, Any]:
    """
    Generate the metadata specific to a Flow-by-Activity or
    Flow-By-Sector method
    :param source_name: string, FBA or FBS method name
    :param config: dictionary, FBA or FBS method
    :param category: string, "FlowByActivity" or "FlowBySector"
    :param kwargs: additional parameters, if running for FBA, define "year"
    :return: object, metadata for FBA or FBS method
    """

    if category == 'FlowBySector':
        method_data = return_fbs_method_data(source_name, config)
        folder_path = return_folder_path(transformpath, source_name)
        subset_path = "/".join(folder_path.parts[-2:])

    elif category == 'FlowByActivity':
        # when FBA meta created, kwargs exist for year
        year = kwargs['year']
        method_data = return_fba_method_meta(source_name, year=year)
        # return the catalog source name to ensure the method urls are correct
        # for FBAs
        source_name = return_true_source_catalog_name(source_name)
        folder_path = return_folder_path(extractpath, source_name)
        subset_path = "/".join(folder_path.parts[-2:])

    # create empty dictionary
    fb_dict = {}
    # add url of FlowBy method at time of commit
    fb_dict['method_url'] = (
        f'https://github.com/cornerstone-data/bedrock/blob/{GIT_HASH_LONG}/bedrock/'
        f'{subset_path}/{source_name}.yaml'
    )

    fb_dict.update(method_data)

    return fb_dict


def get_source_metadata(
    source: str,
    nested_attr: dict[str, Any] | None,
    attr_source_meta: dict[str, Any],
    primary_source_meta: dict[str, Any],
    config: dict[str, Any],
    v: dict[str, Any],
) -> None:
    if source not in attr_source_meta:
        try:
            sources_to_cache = config.get('sources_to_cache', {})
            if sources_to_cache and source in sources_to_cache:
                nested_attr = sources_to_cache[source]
            year = nested_attr.get('year', v.get('year')) if nested_attr else None
        except AttributeError:
            year = None

        cat = (
            get_catalog_info(source)
            .get('data_format', 'FBS')
            .replace('FBS', 'FlowBySector')
            .replace('FBA', 'FlowByActivity')
        )

        attr_source_meta[source] = getMetadata(source, year=year, category=cat)

        if nested_attr is not None:
            if 'activity_sets' in nested_attr:
                activities = nested_attr['activity_sets']
                recursive_attribution(
                    activities, attr_source_meta, primary_source_meta, config
                )
            else:
                attr_dict = (
                    nested_attr.get('attribution_source')
                    or nested_attr.get('attribute')
                    or nested_attr.get('clean_source')
                )
                process_nested_sources(
                    attr_dict,
                    attr_source_meta,
                    primary_source_meta,
                    config,
                    v,
                    nested_attr,
                )


def process_nested_sources(
    attr_dict: dict[str, Any] | list[Any] | str | None,
    attr_source_meta: dict[str, Any],
    primary_source_meta: dict[str, Any],
    config: dict[str, Any],
    v: dict[str, Any],
    nested_attr: dict[str, Any] | None = None,
) -> None:
    if isinstance(attr_dict, list):
        for item in attr_dict:
            nested_attr_dict = (
                item.get('attribution_source')
                or item.get('attribute')
                or item.get('clean_source')
            )
            process_nested_sources(
                nested_attr_dict, attr_source_meta, primary_source_meta, config, v
            )
    elif isinstance(attr_dict, dict):
        for source, nested_attr_item in attr_dict.items():
            get_source_metadata(
                source,
                nested_attr_item,
                attr_source_meta,
                primary_source_meta,
                config,
                v,
            )
    elif isinstance(attr_dict, str):
        # Handle the case where attr_dict is a string
        get_source_metadata(
            attr_dict,
            nested_attr,
            attr_source_meta,
            primary_source_meta,
            config,
            v,
        )


def recursive_attribution(
    activities: dict[str, Any],
    attr_source_meta: dict[str, Any],
    primary_source_meta: dict[str, Any],
    config: dict[str, Any],
) -> None:
    for aset, attr in activities.items():
        attr_dict = (
            attr.get('attribution_source')
            or attr.get('attribute')
            or attr.get('clean_source')
        )
        process_nested_sources(
            attr_dict, attr_source_meta, primary_source_meta, config, attr
        )

        if 'literature_sources' in attr:
            lit = attr['literature_sources']
            for s, y in lit.items():
                lit_meta = return_fba_method_meta(s, year=y)
                attr_source_meta[s] = lit_meta

        if 'attribution_source_meta' in primary_source_meta:
            primary_source_meta['attribution_source_meta'].update(attr_source_meta)
        else:
            primary_source_meta['attribution_source_meta'] = attr_source_meta


def return_fbs_method_data(
    source_name: str, config: dict[str, Any]  # noqa: ARG001
) -> dict[str, Any]:

    from bedrock.extract.stewifbs.stewiFBS import (  # noqa: PLC0415
        add_stewi_metadata,
        add_stewicombo_metadata,
    )

    def process_primary_source(k: str, v: dict[str, Any], meta: dict[str, Any]) -> bool:
        if k == 'stewiFBS':
            if v.get('local_inventory_name'):
                meta['primary_source_meta'][k] = add_stewicombo_metadata(
                    v.get('local_inventory_name')
                )
            else:
                meta['primary_source_meta'][k] = add_stewi_metadata(v['inventory_dict'])
            return True
        return False

    # Create empty dictionary for storing meta data
    meta: dict[str, Any] = {'primary_source_meta': {}}

    # subset the FBS dictionary into a dictionary of source names
    fb = config['source_names']

    for k, v in fb.items():
        if process_primary_source(k, v, meta):
            continue

        cat = (
            get_catalog_info(k)
            .get('data_format', v.get('data_format', ''))
            .replace('FBS', 'FlowBySector')
            .replace('FBA', 'FlowByActivity')
        )

        # append source and year
        year = v.get('year', config.get('year'))
        meta['primary_source_meta'][k] = getMetadata(k, year=year, category=cat)

        # create dictionary of allocation datasets for different activities
        activities = v.get('activity_sets')
        if activities is None:
            if 'attribution_source' in v:
                # for single attribution with no activity sets
                activities = {'activity': v.copy()}
            else:
                continue

        # initiate nested dictionary
        attr_source_meta: dict[str, Any] = {}
        # subset activity data and allocate to sector
        recursive_attribution(
            activities, attr_source_meta, meta['primary_source_meta'][k], config
        )

    return meta


def return_fba_method_meta(sourcename: str, **kwargs: Any) -> dict[str, Any]:
    """
    Return meta for a FlowByActivity method
    :param sourcename: string, the FlowByActivity sourcename
    :param kwargs: requires "year" defined
    :return: meta object
    """
    # load info from either a FBA method yaml or the literature yaml
    fba = load_source_dict(sourcename)
    # initiate empty dictionary
    fba_dict = {}

    # add year if creating an FBA metafile
    if 'year' in kwargs:
        fba_dict['data_year'] = kwargs['year']

    try:
        # loop through the FBA yaml and add info
        for k, v in fba.items():
            # include bib_id because this info pulled
            # when generating a method bib
            if k in (
                'author',
                'source_name',
                'source_url',
                'original_data_download_date',
                'date_accessed',
                'source_publication_date',
                'bib_id',
            ):
                if k == 'source_publication_date':
                    if isinstance(v, dict):
                        try:
                            fba_dict[k] = str(v[kwargs['year']])
                        except KeyError:
                            log.info(
                                "There is no information on the "
                                "publication date for kwargs['year'] in "
                                "the FBA method yaml."
                            )
                    else:
                        fba_dict[k] = str(v)
                else:
                    fba_dict[k] = str(v)
    except:  # noqa: E722
        log.warning('No metadata found for %s', sourcename)
        fba_dict['meta_data'] = f'No metadata found for {sourcename}'

    return fba_dict


def find_file(meta: FileMeta, paths: Paths) -> Path | None:
    """
    Searches for file within path.local_path based on file metadata; if
    metadata matches, returns most recently created file path object
    :param meta: populated instance of class FileMeta
    :param paths: populated instance of class Paths
    :return: str with the file path if found, otherwise an empty string
    """
    if paths.exists():
        with os.scandir(str(paths)) as files:
            # List all file satisfying the criteria in the passed metadata
            matches = [
                f
                for f in files
                if f.name.startswith(meta.name_data)
                and meta.ext.lower() in f.name.lower()
            ]
            # Sort files in reverse order by ctime (creation time on Windows,
            # last metadata modification time on Unix)
            sorted_matches = sorted(
                matches, key=lambda f: f.stat().st_ctime, reverse=True
            )
        # Return the path to the most recent matching file, or '' if no
        # match exists.
        if sorted_matches:
            return paths / sorted_matches[0].name
    return None


def read_source_metadata(
    paths: Paths, meta: FileMeta, force_JSON: bool = False
) -> dict[str, Any] | None:
    """
    return the locally saved metadata dictionary from JSON,
    meta should reflect the outputfile for which the metadata is associated

    :param meta: object of class FileMeta used to load the outputfile
    :param paths: object of class Paths
    :param force_JSON: bool, searches based on named JSON instead of outputfile
    :return: metadata dictionary
    """
    if force_JSON:
        meta.ext = "json"
        path = find_file(meta, paths)
    else:
        p = find_file(meta, paths)
        path = p.parent / f"{p.stem}_metadata.json" if p else None

    if path is None:
        log.warning(f"metadata not found for {meta.name_data}")
        return None

    try:
        return json.loads(path.read_text())
    except FileNotFoundError:
        log.warning(f"metadata not found for {meta.name_data}")
        return None


def getMetadata(
    source: str, year: int | str | None = None, category: str | None = None
) -> dict[str, Any]:
    """
    Use the esupy package functions to return the metadata for
    a FBA or FBS used to generate a FBS
    :param source: string, FBA or FBA source name
    :param year: string, year of FBA data, for FBS use None
    :param category: string, 'FlowBySector' or 'FlowByActivity'
    :return: meta object, previously generated FBA or FBS meta
    """
    from bedrock.extract.generateflowbyactivity import set_fba_name  # noqa: PLC0415

    if category == "FlowByActivity":
        file_path = FBA_DIR
    if category is None:
        log.error('Category required, specify "FlowByActivity" or ' '"FlowBySector"')
        category = 'FlowBySector'  # Default to avoid type errors
        file_path = FBS_DIR
    # if category is FBS ensure year is not added to source name when
    # looking for metadata
    if category == 'FlowBySector':
        year = None
        file_path = FBS_DIR

    year_str = str(year) if isinstance(year, int) else year
    name = set_fba_name(source, year_str)
    meta = read_source_metadata(file_path, set_fb_meta(name, category))
    if meta is None:
        log.warning('No metadata found for %s', source)
        meta = {'source_meta': f'No metadata found for {name}'}

    return meta
