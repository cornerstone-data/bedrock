# Census_AIES.py (bedrock)
# !/usr/bin/env python3
# coding=utf-8

"""
Annual Integrated Economic Survey (AIES), US Census Bureau.

From data year 2023 the Census Bureau consolidated its annual economic surveys -
the Annual Wholesale Trade Survey, the Annual Retail Trade Survey, the Annual
Survey of Manufactures and the Service Annual Survey among them - into one
integrated survey. ``Census_AWTS`` and ``Census_ARTS`` therefore stop at 2022 and
this carries the trade margin control total forward (#612).

**What is taken.** ``RCPT_GM_DVAL`` (gross margin) and ``RCPT_TOT_VAL`` (sales,
value of shipments, or revenue) by NAICS, which is the same pair of measures the
two predecessor surveys published. Gross margin as a percent of sales is not
published and is not taken: it is the ratio of the two, and deriving it here
would invent a third series that has to agree with the first two.

⚠️ **The two measures come from two different datasets, so this source issues
two requests.** AIES is published as per-year datasets - ``data/<year>/aiesbasic``
and ``data/<year>/aiesmiscsector`` - and the ``timeseries/aies/*`` path used until
2026-09 now returns **404 for every year, 2023 included**. ``RCPT_GM_DVAL`` is
*declared* on ``aiesbasic`` but is an orphan there (group ``N/A``, no concept) and
returns null for every row; the populated copy is on ``aiesmiscsector``. Yet
``aiesmiscsector`` carries no NAICS 486 detail, and those pipeline items are what
the transport margin reads from this source. Neither dataset contains the other,
so :func:`census_aies_url_helper` issues one request each and
:func:`_join_aies_legs` joins them on NAICS/TYPOP/TAXSTAT back into the single
wide table this source has always produced.

⚠️ **The per-year datasets publish less gross-margin detail than the retired
timeseries path did.** Regenerating 2023 through the new endpoints reproduces
sales **exactly** on the published NAICS 42 and 44-45 rows, but nine wholesale
detail cells that used to carry a margin - automobiles, computers, appliances and
confectionery among them - are now withheld, and Census restated the rest: the
NAICS 42 margin moves +0.20% and 44-45 +0.55%. Those are revisions and
suppression, not a migration defect.

⚠️ **Type of operation is not a detail here.** Wholesale gross margin is
published under ``TYPOP`` code ``1X``, merchant wholesalers excluding
manufacturers' sales branches and offices, and is **zero** under the all-types
code ``00``; retail is the other way round, published under ``00``. Reading
either sector at the wrong code returns a well-formed zero rather than an error,
which would quietly delete an entire side of the trade margin. The codes are
therefore pinned per sector in :data:`_TYPE_OF_OPERATION` and a sector that comes
back empty raises.

That ``1X`` is also what makes 2023 splice onto AWTS: the AWTS workbook is the
``nomsbo`` table, merchant wholesalers only, so the two are the same basis rather
than merely similar. Measured on the published NAICS 42 row, wholesale gross
margin runs 1,618.0 $B in 2022 on AWTS against 1,601.2 $B here, a 1.0% fall on
sales down 2.5%, and the margin *rate* moves 20.10% to 20.39% - continuous, so
the consolidation did not move the wholesale basis.

⚠️ **Compare like with like: the published total row, not the sum of the
sub-industries.** Summing four-digit codes gives 1,358.2 $B for AWTS 2022 because
suppression removes 16% of that year, so a 4-digit-to-total comparison across the
splice shows a spurious +17.9% jump where the real move is -1.0%. The retail side
splices less cleanly on the same test - its rate steps 31.3% to 34.2% - which is
open; see the plan's "The splice is the seam that matters".

**Published years.** AIES does not carry the predecessor surveys' back-years, so
years before 2023 are absent. An unpublished year now arrives as a **404** from a
path that is otherwise correct, rather than as the ``204 No Content`` the retired
timeseries path returned; :func:`_absent_year` treats the two the same so that
"not published yet" does not surface as an ``APIError``.

**NAICS vintage.** AIES 2023 publishes the pre-2022 retail structure - 452
general merchandise stores and 454 nonstore retailers both appear - so it joins
the 2017-anchored rates without recoding.
"""

from __future__ import annotations

import json
import os
import re
from typing import Any

import pandas as pd

from bedrock.transform.flowbyfunctions import assign_fips_location_system
from bedrock.utils.io.gcp import (
    gcs_extract_input_sub_bucket_from_kwargs,
    load_from_gcs,
)
from bedrock.utils.io.local_extract_input_data import load_local_extract_input_dir
from bedrock.utils.logging.flowsa_log import log
from bedrock.utils.mapping.location import US_FIPS

#: NAICS prefix -> the ``TYPOP`` code the margin is published under. See the
#: module docstring: the wrong code returns zeros rather than an error.
_TYPE_OF_OPERATION = {
    '42': '1X',  # merchant wholesalers, excluding MSBOs
    '44': '00',  # retail, all types of operation
    '45': '00',
    # Transportation, carried for the *transport* margin rather than the trade
    # one (#611): NAICS 486's four detailed items are the pipeline margin items,
    # and they continue Census_SAS Table 2 from 2023. They are inert for the
    # trade margin, which filters to FlowName 'Gross margins' and then reads
    # NAICS 42 or 44-45 by name, so no transport row can reach it.
    '48': '00',
}

#: Published variable -> the flow name ``Census_AWTS`` and ``Census_ARTS`` use.
_FLOW_NAMES = {
    'RCPT_GM_DVAL': 'Gross margins',
    'RCPT_TOT_VAL': 'Sales',
}

#: AIES money variables are published in thousands of dollars.
_THOUSANDS = 1_000.0

#: The dimensions the per-year AIES datasets share, and so the key the two
#: legs of the pull are joined on.
_AIES_JOIN_KEYS = ('NAICS', 'TYPOP', 'TAXSTAT')


#: ``data/<year>/<dataset>`` -> the dataset name, for naming the cache file and
#: for reporting which leg of the two-request pull a response belongs to.
_DATASET_IN_URL = re.compile(r'/data/\d{4}/([a-z0-9_]+)\?')

#: Vintage-endpoint column -> the name this source has always emitted. The
#: retired ``timeseries/aies`` path called the industry column ``NAICS``; the
#: per-year datasets call it ``NAICS2017``. Renaming here keeps the parse
#: functions, the cached CSVs and every downstream consumer unchanged.
_VINTAGE_COLUMN_RENAMES = {
    'NAICS2017': 'NAICS',
    'NAICS2017_LABEL': 'NAICS_LABEL',
}


def _aies_dataset_from_url(url: str) -> str:
    """The AIES dataset a response came from, e.g. ``aiesbasic``."""
    found = _DATASET_IN_URL.search(url or '')
    if found is None:
        raise ValueError(
            f'could not read an AIES dataset name out of {url!r}. The url is '
            f'expected to look like https://api.census.gov/data/<year>/'
            f'<dataset>?... - see Census_AIES.yaml.'
        )
    return found.group(1)


def _normalise_aies_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Put a vintage-endpoint response on this source's historical columns."""
    return df.rename(columns=_VINTAGE_COLUMN_RENAMES)


def _absent_year(resp: Any, what: str) -> bool:
    """Whether the response means "this year is not published".

    ⚠️ **404 has to be treated alongside 204.** The retired ``timeseries``
    path answered an unpublished year with ``204 No Content``; the per-year
    datasets simply do not exist until the year is released, so the same
    condition now arrives as a 404 from a path that is otherwise correct.
    Letting it raise would turn "not published yet" into an ``APIError``.
    """
    if resp.status_code == 204:
        log.warning(f'No {what} content for {_redacted(resp.url)}')
        return True
    if resp.status_code == 404:
        log.warning(
            f'No {what} dataset for {_redacted(resp.url)} - the per-year '
            f'dataset does not exist, i.e. the year is not published.'
        )
        return True
    return False


def _redacted(url: str | None) -> str:
    """A url with its API key removed, so a log line cannot leak the key."""
    return re.sub(r'(?i)((?:key|api_key|apikey|token)=)[^&\s]+', r'***', url or '')


def census_aies_url_helper(*, build_url: str, config: dict, **_: Any) -> list[str]:
    """One url per AIES dataset this source draws on.

    ⚠️ **Two requests, not one.** Gross margin and the NAICS 486 pipeline items
    live in different per-year datasets and neither is a superset of the other -
    see the header of ``Census_AIES.yaml``. :func:`census_aies_parse` joins the
    responses back into the single wide table this source has always produced.
    """
    return [
        build_url.replace('__dataset__', dataset).replace('__get__', variables)
        for dataset, variables in config['datasets'].items()
    ]


#: Expense variables the per-year datasets stopped publishing after 2023.
#: ⚠️ **Census rejects an unknown variable outright** - the response is
#: ``error: unknown variable '...'``, not a null column - so a 2024 request that
#: still names these fails the whole pull rather than returning them empty.
#: 2024 replaces the two rental lines with a single combined ``EXPS_RENT_VAL``;
#: taking it needs a rule for splitting one number across the building and
#: machinery commodities, which is deliberately not done here.
_EXPENSE_VARIABLES_RETIRED_AFTER = {
    'EXPS_COMMSVC_VAL': 2023,
    'EXPS_EXSOFT_VAL': 2023,
    'EXPS_RENT_BUILD_VAL': 2023,
    'EXPS_RENT_MACH_VAL': 2023,
}


def census_aies_expenses_url_helper(
    *, build_url: str, year: str | int, **_: Any
) -> list[str]:
    """Drop the expense variables the requested year no longer publishes."""
    year = int(year)
    retired = [
        name for name, last in _EXPENSE_VARIABLES_RETIRED_AFTER.items() if year > last
    ]
    if not retired:
        return [build_url]
    requested = _get_variables(build_url)
    # ⚠️ urlencode percent-encodes the separator, so the list arrives as
    # ``A%2CB`` rather than ``A,B``. Splitting on a bare comma silently matches
    # nothing and every variable survives the filter.
    separator = '%2C' if '%2C' in requested else ','
    kept = [
        variable for variable in re.split('%2C|,', requested) if variable not in retired
    ]
    log.warning(
        f'AIES {year} does not publish {sorted(retired)}; requesting the '
        f'remaining {len(kept)} variables'
    )
    return [_with_variables(build_url, separator.join(kept))]


def _get_variables(url: str) -> str:
    """The comma-separated variable list out of a built Census url."""
    found = re.search(r'[?&]get=([^&]*)', url)
    if found is None:
        raise ValueError(f'no get= parameter in {url!r}')
    return found.group(1)


def _with_variables(url: str, variables: str) -> str:
    """The same url carrying a different variable list."""
    return re.sub(r'([?&]get=)[^&]*', lambda m: m.group(1) + variables, url, count=1)


def _census_aies_filename(year: str | int, dataset: str | None = None) -> str:
    """Cache file for one leg of the pull.

    ``dataset`` is None only for the pre-2026-09 single-request layout, which
    :func:`census_aies_load_gcs` still falls back to so an already-cached year
    keeps loading without being re-pulled.
    """
    if dataset is None:
        return f'Census_AIES_{year}.csv'
    return f'Census_AIES_{year}_{dataset}.csv'


def census_aies_call(*, resp: Any, **kwargs: Any) -> list[pd.DataFrame]:
    """Convert one leg of the AIES pull to a dataframe.

    The raw table is also written under ``extract/input_data/Census_AIES/`` so it
    can be staged to GCS.  AIES needs an API key, and CI has none - without a
    cached copy every AIES-backed test fails there with ``APIError`` rather than
    on anything about the data.  See :func:`census_aies_load_gcs`.

    ⚠️ **This runs once per dataset**, so the cache file has to carry the
    dataset name.  Writing both legs to one filename would leave whichever
    response arrived last, silently dropping the other half of the table.
    """
    if _absent_year(resp, 'AIES'):
        return [pd.DataFrame()]
    payload = json.loads(resp.text)
    df = _normalise_aies_columns(pd.DataFrame(payload[1:], columns=payload[0]))
    dataset = _aies_dataset_from_url(kwargs.get('url') or resp.url)
    out_dir = load_local_extract_input_dir(kwargs)
    df.to_csv(
        os.path.join(out_dir, _census_aies_filename(kwargs['year'], dataset)),
        index=False,
    )
    return [df]


def census_aies_load_gcs(**kwargs: Any) -> list[pd.DataFrame]:
    """Load one cached leg of the AIES pull from ``input_data``, or GCS.

    This is the path CI takes.  ``Census_AWTS`` and ``Census_ARTS`` need no key
    and so regenerate anywhere, but AIES does, which is why the 2023 leg of the
    trade margin has to come from the cache rather than from Census.

    ⚠️ **This runs once per url, so it must return only that url's leg.**
    Returning every leg on each call would hand :func:`census_aies_parse` two
    copies of the table.

    ⚠️ **Falls back to the single-file layout** used before the source moved to
    the per-year datasets, so a year already cached as ``Census_AIES_<year>.csv``
    keeps loading instead of forcing a re-pull.  Only the first dataset takes the
    fallback, for the same no-duplicates reason.
    """
    sub_bucket = gcs_extract_input_sub_bucket_from_kwargs(kwargs)
    local_dir = load_local_extract_input_dir(kwargs)
    dataset = _aies_dataset_from_url(kwargs['url'])
    try:
        return [
            load_from_gcs(
                name=_census_aies_filename(kwargs['year'], dataset),
                sub_bucket=sub_bucket,
                local_dir=local_dir,
                loader=pd.read_csv,
            )
        ]
    except FileNotFoundError:
        datasets = list(kwargs.get('config', {}).get('datasets', ()))
        if datasets and dataset != datasets[0]:
            log.warning(
                f'no cached {dataset} leg for {kwargs["year"]}; the legacy '
                f'single-file cache is being read on the {datasets[0]} leg'
            )
            return [pd.DataFrame()]
        log.warning(
            f'no per-dataset cache for {kwargs["year"]}; falling back to the '
            f'pre-2026-09 single-file layout'
        )
        return [
            load_from_gcs(
                name=_census_aies_filename(kwargs['year']),
                sub_bucket=sub_bucket,
                local_dir=local_dir,
                loader=pd.read_csv,
            )
        ]


def _join_aies_legs(df_list: list[pd.DataFrame]) -> pd.DataFrame:
    """Rebuild the single wide AIES table from the per-dataset responses.

    ⚠️ **A join, not a concat.**  The legs carry different *measures* for the
    same rows - sales from ``aiesbasic``, gross margin from ``aiesmiscsector`` -
    so stacking them would double every row and leave each copy missing half its
    columns.  They are joined on the dimensions the two datasets share.

    One leg (the legacy single-file cache, or a year where only one dataset
    answered) is returned unchanged.
    """
    legs = [df for df in df_list if df is not None and not df.empty]
    if not legs:
        return pd.DataFrame()
    joined = legs[0]
    for leg in legs[1:]:
        keys = [c for c in _AIES_JOIN_KEYS if c in joined.columns and c in leg.columns]
        if not keys:
            raise ValueError(
                f'AIES legs share none of {_AIES_JOIN_KEYS}, so they cannot be '
                f'joined: {sorted(joined.columns)} vs {sorted(leg.columns)}.'
            )
        overlap = (set(joined.columns) & set(leg.columns)) - set(keys)
        joined = joined.merge(leg.drop(columns=list(overlap)), on=keys, how='left')
    return joined


def census_aies_parse(
    *, df_list: list[pd.DataFrame], source: str, year: int | None, **_: Any
) -> pd.DataFrame:
    """Keep the trade rows at their published type of operation, then melt."""
    df = _join_aies_legs(df_list)
    if df.empty:
        raise ValueError(
            f'{source} returned no rows for {year}. AIES currently publishes '
            f'2023 only - the predecessor surveys Census_AWTS and Census_ARTS '
            f'carry 2012-2022, and later years are not released yet.'
        )

    df['NAICS'] = df['NAICS'].astype(str).str.strip()
    df['sector'] = df['NAICS'].str[:2]

    kept = []
    for prefix, typop in _TYPE_OF_OPERATION.items():
        rows = df[(df['sector'] == prefix) & (df['TYPOP'] == typop)]
        if rows.empty:
            raise ValueError(
                f'{source} {year} has no NAICS {prefix} rows at type of '
                f'operation {typop}. That is where the margin for this sector '
                f'is published; the other codes carry zeros, so an empty result '
                f'here would silently drop the sector from the control total.'
            )
        kept.append(rows)
    df = pd.concat(kept, ignore_index=True)

    df = (
        df.melt(
            id_vars=['NAICS', 'TYPOP'],
            value_vars=list(_FLOW_NAMES),
            var_name='Variable',
            value_name='FlowAmount',
        )
        # the trade sector produces the margin, so it lands in
        # ActivityProducedBy - matching Census_AWTS, Census_ARTS and
        # Margins_Transport
        .rename(columns={'NAICS': 'ActivityProducedBy', 'TYPOP': 'Description'})
        .assign(
            FlowName=lambda x: x['Variable'].map(_FLOW_NAMES),
            Year=str(year),
        )
        .drop(columns='Variable')
    )
    df['FlowAmount'] = pd.to_numeric(df['FlowAmount'], errors='coerce') * _THOUSANDS

    return (
        df.assign(
            Unit='USD',
            Class='Money',
            SourceName=source,
            Compartment=None,
            FlowType='TECHNOSPHERE_FLOW',
            Location=US_FIPS,
            # provisional pending a source-specific assessment
            DataReliability=5,
            DataCollection=5,
        )
        .pipe(assign_fips_location_system, 2024)
        .reset_index(drop=True)
    )


# --- transport margins (#611) ----------------------------------------------

#: ``RCPT_MOTR_<group>_DVAL`` -> the Table 8 group name it continues.
#:
#: These strings are **not cosmetic**: they are the join key into
#: ``Crosswalk_SAS_Group_to_BEA_2017.csv``, and all ten identified groups match
#: AIES's own published labels exactly, so the crosswalk carries across the
#: survey consolidation unchanged. Verified against
#: ``timeseries/aies/miscsector/variables.json``.
#:
#: ⚠️ ``RCPT_MOTR_HAZRD_DVAL`` is deliberately absent. Hazardous materials is a
#: cross-cut of the same revenue rather than a twelfth group; including it would
#: double-count and break the partition check downstream.
_MOTOR_CARRIER_GROUPS = {
    'RCPT_MOTR_AGR_DVAL': 'Agricultural products',
    'RCPT_MOTR_ELECT_DVAL': (
        'Electronic and precision instruments and motorized vehicles'
    ),
    'RCPT_MOTR_FUEL_DVAL': 'Coal and petroleum products',
    'RCPT_MOTR_GRAIN_DVAL': 'Grains, alcohol, and tobacco products',
    'RCPT_MOTR_METAL_DVAL': 'Base metal and machinery',
    'RCPT_MOTR_NEWFRN_DVAL': ('New furniture and miscellaneous manufactured products'),
    'RCPT_MOTR_OTH_DVAL': 'Other goods',
    'RCPT_MOTR_PHARM_DVAL': 'Pharmaceutical and chemical products',
    'RCPT_MOTR_STONE_DVAL': 'Stone, nonmetallic minerals, and metallic ores',
    'RCPT_MOTR_USEDGD_DVAL': 'Used household and office goods',
    'RCPT_MOTR_WOOD_DVAL': 'Wood products, textiles, and leathers',
}

#: The row the eleven groups partition.
_MOTOR_CARRIER_TOTAL = 'Total Motor Carrier Revenue'

#: SAS Table 8's row prefix, reproduced here so the AIES rows land in the same
#: vocabulary as the years they continue - ``load_truck_group_revenue`` then
#: switches source rather than carrying a second parallel implementation.
_MOTOR_CARRIER_PREFIX = 'Estimated Revenue by Commodities Handled: '

#: NAICS 484 is the only industry carrying commodity detail, exactly as in SAS
#: Table 8.
_MOTOR_CARRIER_NAICS = '484'


def _census_aies_miscsector_filename(year: str | int) -> str:
    return f'Census_AIES_MiscSector_{year}.csv'


def census_aies_miscsector_call(*, resp: Any, **kwargs: Any) -> list[pd.DataFrame]:
    """Convert the API response to a dataframe; 204 means the year is absent.

    Mirrors :func:`census_aies_call`, including writing the raw table under
    ``extract/input_data/`` so it can be staged to GCS for keyless CI.
    """
    if _absent_year(resp, 'AIES miscsector'):
        return [pd.DataFrame()]
    payload = json.loads(resp.text)
    df = _normalise_aies_columns(pd.DataFrame(payload[1:], columns=payload[0]))
    out_dir = load_local_extract_input_dir(kwargs)
    df.to_csv(
        os.path.join(out_dir, _census_aies_miscsector_filename(kwargs['year'])),
        index=False,
    )
    return [df]


def census_aies_miscsector_load_gcs(**kwargs: Any) -> list[pd.DataFrame]:
    """Load the cached miscsector table from local ``input_data``, or GCS."""
    return [
        load_from_gcs(
            name=_census_aies_miscsector_filename(kwargs['year']),
            sub_bucket=gcs_extract_input_sub_bucket_from_kwargs(kwargs),
            local_dir=load_local_extract_input_dir(kwargs),
            loader=pd.read_csv,
        )
    ]


def census_aies_miscsector_parse(
    *, df_list: list[pd.DataFrame], source: str, year: int | None, **_: Any
) -> pd.DataFrame:
    """Motor carrier revenue by commodity group, in SAS Table 8's vocabulary.

    The eleven groups and the total are emitted with the same ``FlowName``
    strings ``Census_SAS`` Table 8 uses, so the truck allocator reads one row
    vocabulary across the 2022/2023 survey seam.
    """
    df = pd.concat(df_list, sort=False)
    if df.empty:
        raise ValueError(
            f'{source} returned no rows for {year}. AIES currently publishes '
            f'2023 only - Census_SAS Table 8 carries 2015-2022, and later years '
            f'are not released yet.'
        )

    df['NAICS'] = df['NAICS'].astype(str).str.strip()
    rows = df[df['NAICS'] == _MOTOR_CARRIER_NAICS]
    if rows.empty:
        raise ValueError(
            f'{source} {year} has no NAICS {_MOTOR_CARRIER_NAICS} rows. That is '
            f'the only industry carrying commodity detail, so without it there '
            f'is no truck allocator.'
        )

    flow_names = {
        **{k: f'{_MOTOR_CARRIER_PREFIX}{v}' for k, v in _MOTOR_CARRIER_GROUPS.items()},
        'RCPT_MOTR_VAL': _MOTOR_CARRIER_TOTAL,
    }
    present = [c for c in flow_names if c in rows.columns]
    missing = sorted(set(flow_names) - set(present))
    if missing:
        raise ValueError(
            f'{source} {year} is missing {missing}. The eleven commodity groups '
            f'are meant to partition {_MOTOR_CARRIER_TOTAL} exactly; a dropped '
            f'variable would silently shrink every share once they renormalise.'
        )

    df = (
        rows.melt(
            id_vars=['NAICS'],
            value_vars=present,
            var_name='Variable',
            value_name='FlowAmount',
        )
        .rename(columns={'NAICS': 'ActivityProducedBy'})
        .assign(
            FlowName=lambda x: x['Variable'].map(flow_names),
            # Census_SAS carries the sheet in Description and the truck loader
            # filters on it; keep that contract.
            Description='Table 8',
            Year=str(year),
        )
        .drop(columns='Variable')
    )
    df['FlowAmount'] = pd.to_numeric(df['FlowAmount'], errors='coerce') * _THOUSANDS

    # AIES publishes no suppression flag on these items, but the truck loader
    # reads the column to recover a single suppressed group by subtraction.
    df['Suppressed'] = None

    return (
        df.assign(
            Unit='USD',
            Class='Money',
            SourceName=source,
            Compartment=None,
            FlowType='TECHNOSPHERE_FLOW',
            Location=US_FIPS,
            # provisional pending a source-specific assessment
            DataReliability=5,
            DataCollection=5,
        )
        .pipe(assign_fips_location_system, 2024)
        .reset_index(drop=True)
    )


if __name__ == '__main__':
    from bedrock.extract.flowbyactivity import getFlowByActivity
    from bedrock.extract.generateflowbyactivity import generateFlowByActivity

    generateFlowByActivity(source='Census_AIES', year='2023')
    fba = getFlowByActivity('Census_AIES', 2023)


#: AIES expense variables, mapped to the expense each names.  The keys are the
#: successor series to :data:`~Census_ASM.ASM_EXPENSE_FLOWS`, and the two line up
#: one for one on the concepts the materials work needs:
#: ``EXPS_MAT_DVAL`` is ASM's ``CSTMPRT`` and ``EXPS_FUEL_VAL`` its ``CSTFU``.
#:
#: ✅ **Verified additive against the published control.**  ``EXPS_MAT_DVAL +
#: EXPS_FUEL_VAL + EXPS_ELEC_VAL + EXPS_CONTRACT_VAL + EXPS_RESALE_VAL`` sums to
#: ``EXPS_CSTMTOT_DVAL`` at 99.77% in aggregate, median per-industry ratio
#: 1.0000, within 1% for 324 of 360 manufacturing industries -- the same
#: decomposition ASM's ``CSTMTOT`` has, so the splice is a scope match rather
#: than an approximation.
AIES_EXPENSE_FLOWS = {
    'EXPS_MAT_DVAL': 'Cost of materials, parts and containers',
    'EXPS_FUEL_VAL': 'Cost of fuels',
    'EXPS_ELEC_VAL': 'Cost of purchased electricity',
    'EXPS_CONTRACT_VAL': 'Cost of contract work',
    'EXPS_RESALE_VAL': 'Cost of goods purchased for resale',
    'EXPS_ADVERT_VAL': 'Purchased advertising services',
    'EXPS_COMMSVC_VAL': 'Purchased communication services',
    'EXPS_DATAPROC_VAL': 'Purchased data processing and hosting services',
    'EXPS_PROFTECH_VAL': 'Purchased professional and technical services',
    'EXPS_TEMPSTAF_VAL': 'Purchased personnel supply services',
    'EXPS_REFUSE_VAL': 'Purchased refuse removal services',
    'EXPS_MACH_REP_VAL': 'Purchased machinery repair and maintenance',
    'EXPS_BUILD_REP_VAL': 'Purchased building repair and maintenance',
    'EXPS_RENT_BUILD_VAL': 'Rental of buildings',
    'EXPS_RENT_MACH_VAL': 'Rental of machinery and equipment',
    'EXPS_EXSOFT_VAL': 'Purchased software expensed',
    'EXPS_COMPTR_OTHEQ_VAL': 'Purchased computers and peripherals expensed',
    'EXPS_OTHER_VAL': 'All other operating expenses',
    # ⚠️ Sector-specific cells, published for a handful of industries each
    # and zero elsewhere.  They exist only in ``exp02`` and are named to
    # match SAS Table 5's item wording so the two panels stack.
    'EXPS_FUEL_TRANSP_VAL': 'Purchased fuels for transportation equipment',
    'EXPS_TRANSP_REP_VAL': (
        'Purchased repairs and maintenance to transportation equipment'
    ),
    'EXPS_TRANSP_VAL': 'Purchased freight transportation',
    'EXPS_SUPPLY_MED_VAL': 'Medical supplies',
    'EXPS_INS_PREM_VAL': 'Cost of insurance',
    'EXPS_PROFLIAB_VAL': 'Professional liability insurance',
    'EXPS_PRINT_VAL': 'Purchased printing services',
}

#: Totals of the cells beside them, kept so a consumer can check additivity.
#: ⚠️ Summing this FBA unfiltered counts the table several times over.
AIES_EXPENSE_CONTROLS = ('EXPS_CSTMTOT_DVAL', 'EXPS_TOT_DVAL', 'RCPT_TOT_VAL')


def census_aies_expenses_parse(
    *, df_list: list[pd.DataFrame], year: int, **_: Any
) -> pd.DataFrame:
    """Format the AIES expense-by-kind table into a long FBA.

    ``ActivityConsumedBy`` is the purchasing NAICS industry and ``FlowName`` the
    expense kind, matching :func:`~Census_ASM.asm_expenses_parse` exactly so the
    two surveys stack into one annual panel.

    ⚠️ **The expense detail in this pull is manufacturing only** -- because it
    reads ``timeseries/aies/basic``, where every service row comes back a
    **well-formed zero**, the same trap :data:`_TYPE_OF_OPERATION` documents.
    ❌ **An earlier draft read that as "21, 22, 23 and 51-81 publish nothing at
    all" and generalised it to the survey.  That is wrong.**
    ``timeseries/aies/exp02`` (group ``AIES00EXP02``) publishes all 41 expense
    variables for **13 service sectors** in 2023.  See
    ``analysis/nowcasting/services_transport_expense_resource.py``.

    ⚠️ **Wholesale and retail are genuinely absent** -- no 42, 44 or 45 rows in
    ``exp02``, and ``ecnbasic`` carries the expense variables for sectors 21, 23
    and 31-33 only in both 2017 and 2022.

    ⚠️ **And it does not cover mining**, which ``Census_EC_MatFuel`` does.  The
    2023 observation is narrower than the census it extends.

    ⚠️ **2023 is the only year.**  2022 predates the consolidated survey and
    2024 is not published; both return ``204 No Content``.  So the annual
    materials panel is census 2017, ASM 2018-2021, census 2022, AIES 2023, and
    the estimation span ends there.  Adding 2024 once it publishes is #707,
    Phase 2 work tied to a 2024 table; 2025 is out of scope.

    ⚠️ **Every NAICS level is kept**, including the ``31-33`` rollup, on the same
    reasoning as :func:`~Census_ASM.asm_expenses_parse`: the parents are the only
    available control, and ``'31-33'`` is five characters, so match ``^\\d+$``
    before filtering on length.
    """
    df = pd.concat(df_list, sort=False)

    value_columns = [
        column
        for column in (*AIES_EXPENSE_FLOWS, *AIES_EXPENSE_CONTROLS)
        if column in df.columns
    ]
    long = df.melt(
        id_vars=['NAICS'],
        value_vars=value_columns,
        var_name='FlowName',
        value_name='FlowAmount',
    ).rename(columns={'NAICS': 'ActivityConsumedBy'})
    long['FlowAmount'] = pd.to_numeric(long['FlowAmount'], errors='coerce')
    # ⚠️ A missing cell is withheld; a published zero is a real zero, and AIES
    # publishes a great many of them because the expense block is manufacturing
    # only. Dropping both would delete the evidence for that; only the withheld
    # ones go.
    withheld = int(long['FlowAmount'].isna().sum())
    long = long[long['FlowAmount'].notna()].copy()

    long['Description'] = long['FlowName'].map(
        AIES_EXPENSE_FLOWS
        | {code: f'{code} (control)' for code in AIES_EXPENSE_CONTROLS}
    )
    long['ActivityProducedBy'] = None
    long['Year'] = year
    long['Location'] = US_FIPS
    long['Unit'] = 'Thousand USD'
    long['Class'] = 'Money'
    long['FlowType'] = 'TECHNOSPHERE_FLOW'

    long = assign_fips_location_system(long, year)
    long['SourceName'] = 'Census_AIES_Expenses'
    long['DataReliability'] = 5
    long['DataCollection'] = 5
    long['Compartment'] = None

    log.info(
        'Census_AIES_Expenses %s: %s rows over %s industries and %s expense '
        'kinds; %s cells withheld and dropped.',
        year,
        len(long),
        long['ActivityConsumedBy'].nunique(),
        long['FlowName'].nunique(),
        withheld,
    )
    return long
