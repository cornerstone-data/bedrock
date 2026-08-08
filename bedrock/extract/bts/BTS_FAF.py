# BTS_FAF.py (bedrock)
# !/usr/bin/env python3
# coding=utf-8

"""
Freight Analysis Framework (FAF), ORNL / US Bureau of Transportation Statistics.

FAF models freight movement between origin-destination pairs, by commodity
(SCTG) and mode. This extractor keeps only what the transport margin needs -
national totals per (commodity, mode) - and discards the origin-destination
detail, which is the bulk of the file.

**Why this source.** Transport is the one margin with an external source that
gives both the level and the commodity allocation. BEA's own method distributes
transport cost across commodities on the share of transport receipts from moving
each one, taken from the Commodity Flow Survey; the manual assumes revenue per
ton is constant across commodities because CFS reports tons and value but not
revenue. FAF is modelled from the same CFS but published annually rather than
every five years, and it adds **ton-miles**, which is the better freight-cost
proxy since cost scales with distance. Both measures are emitted here so the
choice can be settled on the 2017 fit rather than by assertion.

**Version.** FAF5.7.1, whose base year is 2017 - the same CFS vintage the BEA
method rests on, and the benchmark year the margin rates are anchored to. FAF6
exists with a 2022 base year, but a 2022 base cannot be checked against a
published 2017 ``TRANS`` column. One file carries 2017-2024 annually (plus
2030-2050 forecasts, which are dropped), so the whole nowcast window and the
benchmark come from a single download.

**Sizes.** The archive is 145 MB and the CSV inside it 529 MB over 1.2 million
rows, so it is cached under ``extract/input_data/BTS_FAF/`` and read with
``usecols`` rather than re-downloaded or read whole.

⚠️ **Two traps this deliberately avoids.**

*No PDF scraping.* The SCTG and mode code tables are published inside the same
archive as ``FAF5_metadata.xlsx``. The upstream flowsa version reads them out of
the FAF User Guide PDF with ``tabula``, which needs a Java runtime to recover
tables that ship in the download already.

*Names are validated, not assumed.* FAF publishes numeric codes; the
descriptions come from the metadata workbook and are what the activity-to-sector
crosswalk joins on. A renamed description would otherwise drop a commodity
silently - allocating transport margin across fewer commodities while every
total still balances - so every activity name is checked against the crosswalk
at extract time and a mismatch raises.
"""

from __future__ import annotations

import os
import zipfile
from typing import Any

import pandas as pd

from bedrock.transform.flowbyfunctions import assign_fips_location_system
from bedrock.utils.config.settings import crosswalkpath
from bedrock.utils.io.gcp import download_extract_input_from_gcs_if_not_exists
from bedrock.utils.io.local_extract_input_data import local_extract_input_dir
from bedrock.utils.mapping.location import US_FIPS

#: One archive holds every year, so there is one file to cache rather than one
#: per year - the same shape as BEA's NIPA flat files.
FAF_ZIP = 'FAF5.7.1_State.zip'

#: Sheets of ``FAF5_metadata.xlsx`` that name the numeric codes.
_SCTG_SHEET = 'Commodity (SCTG2)'
_MODE_SHEET = 'Mode'

#: **All** trade types are kept, which is a deliberate departure from the flowsa
#: version's ``trade_type == 1`` filter.
#:
#: The concern that filter answers is real - the foreign-port-to-domestic-port
#: leg of an import is already inside the Supply table's ``MCIF``, and counting
#: it again as transport margin would double it. But FAF does not report that
#: leg here: on an import record the ``dms_mode``, ``dms_orig``/``dms_dest`` and
#: ton-mile fields all describe the **domestic** portion of the move, from the
#: entry region onward. That leg is exactly the domestic-port-to-user movement
#: BEA counts in ``TRANS``, so dropping it discards margin-bearing freight
#: rather than avoiding a double count.
#:
#: It is 20% of ton-miles, and it is not spread evenly: excluding it is what
#: made the 2017 fit worst on precisely the import-heavy commodities. Keeping it
#: lifts the ton-mile fit against published ``TRANS`` from 0.274 to 0.370 and
#: cuts the share error from 75.7 to 68.2 percentage points.
_KEEP_ALL_TRADE_TYPES = True

#: Published unit -> (bedrock unit, factor). FAF reports tons in thousands, and
#: dollars and ton-miles in millions.
_MEASURES: dict[str, tuple[str, float]] = {
    'tons': ('tons', 1_000.0),
    'tmiles': ('ton-miles', 1_000_000.0),
    'current_value': ('USD', 1_000_000.0),
}

#: The crosswalk both activity dimensions join on.
_CROSSWALK = 'NAICS_Crosswalk_FAF_Mode_and_SCTG.csv'


def faf_local_path(source: str = 'BTS_FAF') -> str:
    """Where the archive is cached: ``extract/input_data/BTS_FAF/``."""
    return os.path.join(local_extract_input_dir(source, year=None), FAF_ZIP)


def _value_column(year: int) -> str:
    """The nominal-dollar column for *year*.

    FAF publishes ``value_*`` in constant base-year dollars for every year and
    ``current_value_*`` in nominal dollars from 2018 on. 2017 is the base year,
    so it has no ``current_value_2017`` - the two are the same number, and
    ``value_2017`` is it.
    """
    return 'value_2017' if year == 2017 else f'current_value_{year}'


def _measure_columns(years: list[int]) -> dict[str, tuple[str, int, float]]:
    """Published column -> (bedrock unit, year, scaling factor)."""
    columns = {}
    for year in years:
        for published, (unit, factor) in _MEASURES.items():
            name = _value_column(year) if published == 'current_value' else (
                f'{published}_{year}'
            )
            columns[name] = (unit, year, factor)
    return columns


def _code_names(zip_file: zipfile.ZipFile) -> tuple[dict[int, str], dict[int, str]]:
    """SCTG and mode code -> description, from the metadata workbook."""
    metadata = next(n for n in zip_file.namelist() if n.lower().endswith('.xlsx'))
    with zip_file.open(metadata) as handle:
        book = pd.read_excel(handle, sheet_name=[_SCTG_SHEET, _MODE_SHEET], header=0)

    def as_map(sheet: pd.DataFrame) -> dict[int, str]:
        frame = sheet.rename(
            columns={'Numeric Label': 'code', 'Description': 'description'}
        ).dropna(subset=['code', 'description'])
        return {
            int(code): str(description).strip()
            for code, description in zip(frame['code'], frame['description'])
        }

    return as_map(book[_SCTG_SHEET]), as_map(book[_MODE_SHEET])


def _validate_against_crosswalk(commodities: set[str], modes: set[str]) -> None:
    """Raise if FAF names something the activity-to-sector crosswalk cannot join.

    The failure this prevents is quiet: an unjoined commodity carries no
    transport margin, the remaining commodities still absorb the whole control
    total, and every total balances while the allocation is wrong.
    """
    crosswalk = pd.read_csv(crosswalkpath / _CROSSWALK, dtype=str).fillna('')
    known = {
        source: set(rows['Activity'])
        for source, rows in crosswalk.groupby('ActivitySourceName')
    }
    missing = {
        'commodity': sorted(commodities - known.get('FAF_SCTG', set())),
        'mode': sorted(modes - known.get('FAF_Mode', set())),
    }
    unmatched = {kind: names for kind, names in missing.items() if names}
    if unmatched:
        raise ValueError(
            f'FAF publishes activity names that {_CROSSWALK} cannot join, so '
            f'they would be dropped silently: {unmatched}. FAF has most likely '
            f'renamed them - reconcile the crosswalk Activity column against '
            f'the metadata workbook rather than letting the rows fall out.'
        )


def _read_faf_zip(zip_path: str, config: dict[str, Any]) -> list[pd.DataFrame]:
    """National (commodity, mode) totals per year and measure, long format."""
    years = [int(year) for year in config['years']]
    measures = _measure_columns(years)
    keys = ['sctg2', 'dms_mode']

    with zipfile.ZipFile(zip_path) as zip_file:
        sctg_names, mode_names = _code_names(zip_file)
        data_file = next(n for n in zip_file.namelist() if n.lower().endswith('.csv'))
        with zip_file.open(data_file) as handle:
            raw = pd.read_csv(
                handle,
                usecols=[*keys, *measures],
                dtype={'sctg2': int, 'dms_mode': int},
            )

    # every trade type, summed over origin-destination pairs - see
    # _KEEP_ALL_TRADE_TYPES on why the import and export legs belong here
    totals = raw.groupby(keys, as_index=False).sum()

    long = totals.melt(id_vars=keys, var_name='measure', value_name='FlowAmount')
    long['Unit'] = long['measure'].map(lambda m: measures[m][0])
    long['Year'] = long['measure'].map(lambda m: str(measures[m][1]))
    long['FlowAmount'] = long['FlowAmount'] * long['measure'].map(
        lambda m: measures[m][2]
    )
    long['FlowName'] = long['sctg2'].map(sctg_names)
    long['ActivityProducedBy'] = long['dms_mode'].map(mode_names)

    unnamed = long['FlowName'].isna() | long['ActivityProducedBy'].isna()
    if unnamed.any():
        codes = long.loc[unnamed, keys].drop_duplicates()
        raise ValueError(
            f'FAF data carries codes the metadata workbook does not name: '
            f'{codes.to_dict("records")}'
        )
    _validate_against_crosswalk(
        set(long['FlowName']), set(long['ActivityProducedBy'])
    )

    return [long.drop(columns=['measure', *keys])]


def faf_call(
    *, resp: Any, source: str, config: dict[str, Any], **_: Any
) -> list[pd.DataFrame]:
    """Cache the downloaded archive under extract-input, then read it.

    Only reached with ``extract_data_from_raw_sources: True``; writing it down
    rather than parsing it in memory is what lets every later run go through
    :func:`faf_load_gcs` instead of pulling 145 MB from ORNL again.
    """
    local_path = faf_local_path(source)
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open(local_path, 'wb') as f:
        f.write(resp.content)
    return _read_faf_zip(local_path, config)


def faf_load_gcs(**kwargs: Any) -> list[pd.DataFrame]:
    """Load the archive from the local cache, or GCS extract-input if missing."""
    source = str(kwargs['source'])
    local_path = faf_local_path(source)
    if not os.path.exists(local_path):
        download_extract_input_from_gcs_if_not_exists(
            # the archive spans every year, so it sits directly under
            # extract/input-data/BTS_FAF/ rather than in a year subfolder
            {**kwargs, 'year': None},
            local_dir=os.path.dirname(local_path),
            object_name=FAF_ZIP,
        )
    if not os.path.exists(local_path):
        raise FileNotFoundError(
            f'{FAF_ZIP} is neither cached at {local_path} nor available from '
            f'gs://cornerstone-default/extract/input-data/{source}/. Set '
            f'extract_data_from_raw_sources: True in {source}.yaml to fetch it '
            f'from ORNL and cache it, then upload it so others need not.'
        )
    return _read_faf_zip(local_path, kwargs['config'])


def faf_parse(
    *, df_list: list[pd.DataFrame], source: str, year: int, **_: Any
) -> pd.DataFrame:
    """Select *year* out of the archive-wide frame and complete the FBA columns."""
    df = pd.concat(df_list, ignore_index=True)
    df = df.loc[df['Year'] == str(year)].copy()
    if df.empty:
        raise ValueError(f'{source} has no rows for {year}')
    return (
        df.assign(
            Class='Other',
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


def move_flow_to_ACB(fba: pd.DataFrame, **_: Any) -> pd.DataFrame:
    """``clean_fba_before_activity_sets``: the commodity moved is the consumer.

    FAF's two dimensions are the mode (which produces the transport service) and
    the commodity moved. Only the mode is an activity in the published data, so
    the commodity rides in ``FlowName`` out of the extractor and is promoted to
    ``ActivityConsumedBy`` here, which is what lets the crosswalk map it to the
    commodity that receives the margin.
    """
    return fba.assign(ActivityConsumedBy=fba['FlowName'])


def move_SCB_to_flow(fbs: pd.DataFrame, **_: Any) -> pd.DataFrame:
    """``clean_fbs``: replace the SCTG flow name with the mapped commodity."""
    return fbs.assign(Flowable=fbs['SectorConsumedBy'], SectorConsumedBy=None)
