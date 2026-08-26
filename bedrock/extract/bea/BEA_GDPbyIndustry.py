# BEA_GDPbyIndustry.py (bedrock)
# !/usr/bin/env python3
# coding=utf-8

"""
Value added by industry, and its three components, from BEA's GDP-by-Industry
accounts (#538).

This is the one BEA product that states **gross operating surplus by industry**
directly, annually, rather than leaving it to be assembled from eight NIPA lines
on four incompatible partitions -- see
:mod:`bedrock.analysis.nowcasting.compensation_allocation`. Table ``TVA113``,
*Components of Value Added by Industry*, gives for each industry:

===========  ==============================================  ================
code         ``TVA113`` component                            SUT counterpart
===========  ==============================================  ================
``VAPRO``    the industry row itself, value added            ``VAPRO``
``V00100``   Compensation of employees                       ``V00100``
``V00200``   Taxes on production and imports less subsidies  ``T00OTOP`` + ``T00TOP`` + ``T00SUB``
``V00300``   Gross operating surplus                         ``V00300``
===========  ==============================================  ================

⚠️ **The component codes are bedrock's, not BEA's.** ``TVA113`` labels its rows
in words and gives them no codes. The four above are assigned here so the FBA
speaks the same code space as the tables it will be compared against -- and
``V00200`` deliberately takes the *MUT* code, because the SUT splits that
concept three ways and this table does not.

⚠️ **No API key, and that is why the flat file is used.** BEA's ``GDPbyIndustry``
API dataset carries the same tables but needs a ``UserID``; the release archive
at ``apps.bea.gov/industry/Release/ZIP/GDPbyInd.zip`` does not, so this follows
``BEA_NIPA``'s pattern of caching one archive under ``extract/input_data`` rather
than calling an API per year.

Reading the table
-----------------

⚠️ **``TVA113`` is hierarchical and summing it double-counts**, the same trap the
NIPA tables set: 101 industry rows of which about 71 are the leaves that
correspond to BEA summary industries, with ``Private industries``,
``Manufacturing``, ``Durable goods`` and so on stated above them. Methods must
select lines explicitly.

⚠️ **Lines 390, 394 and 398 are an ``Addenda:`` block** -- private
goods-producing, private services-producing, and ICT-producing industries -- and
they restate industries already counted above. The U20405 memorandum hazard
again.

⚠️ **Two industry names appear twice**: ``General government`` and ``Government
enterprises``, once under Federal and once under State and local. Select by
line, not by name, or alias them as ``NIPA_VA_compensation_2017.yaml`` does.

⚠️ **``Line`` in ``Description`` is the INDUSTRY's line, not the component
row's.** ``Farms`` is line 13 and its compensation, taxes and surplus are lines
14-16; all four rows carry 13. That is deliberate: selections are made on the
industry axis, so ``Line: [13, 17, 25, ...]`` picks a set of industries and
``Code`` picks which component of them. Cite the industry line, not the value's
own row.
"""

import os
import re
import zipfile
from typing import Any

import pandas as pd

from bedrock.extract.bea.BEA_NIPA import extract_table_info  # noqa: F401
from bedrock.transform.flowbyfunctions import assign_fips_location_system
from bedrock.utils.io.gcp import download_extract_input_from_gcs_if_not_exists
from bedrock.utils.io.local_extract_input_data import local_extract_input_dir
from bedrock.utils.mapping.location import US_FIPS

#: One archive holds every table and every year, so there is one file to cache
#: rather than one per year -- as with ``BEA_NIPA``'s ``FlatFiles.ZIP``.
GDP_BY_INDUSTRY_ZIP = 'GDPbyInd.zip'

#: ``TVA113`` publishes in millions of dollars; the FBA is in dollars, matching
#: ``BEA_NIPA``.
MILLIONS_TO_DOLLARS = 1_000_000.0

#: Row 7 of every ``-A`` sheet is the header: ``Line``, the label, a blank, then
#: one column per year. Rows 0-6 are the title block.
_HEADER_ROW = 7

#: Component labels, and the code each is given. Anything else in the label
#: column starts a new industry.
_COMPONENTS: dict[str, str] = {
    'Compensation of employees': 'V00100',
    'Taxes on production and imports less subsidies': 'V00200',
    'Gross operating surplus': 'V00300',
}

#: The code for an industry's own row, which is its value added at producer
#: prices.
_VALUE_ADDED = 'VAPRO'

#: What each code means, for the ``FlowName`` column.
_FLOW_NAMES: dict[str, str] = {
    _VALUE_ADDED: 'Value added',
    'V00100': 'Compensation of employees',
    'V00200': 'Taxes on production and imports less subsidies',
    'V00300': 'Gross operating surplus',
}


def gdp_by_industry_local_path(source: str = 'BEA_GDPbyIndustry') -> str:
    """Where the release archive is cached under ``extract/input_data``."""
    return os.path.join(local_extract_input_dir(source, year=None), GDP_BY_INDUSTRY_ZIP)


def _read_sheets_from_zip(zip_path: str, config: dict[str, Any]) -> list[pd.DataFrame]:
    """Read each ``sheets`` entry out of each ``files`` entry, unparsed.

    The frame is returned header-less and un-typed; :func:`gdp_by_industry_parse`
    does the shaping, so that a sheet whose title block changes height fails
    there with a readable message rather than here with a column-count error.
    """
    frames = []
    with zipfile.ZipFile(zip_path) as archive:
        for filename in config['files']:
            if filename not in archive.namelist():
                raise FileNotFoundError(
                    f'{filename} is not in {GDP_BY_INDUSTRY_ZIP}; it holds '
                    f'{sorted(archive.namelist())}'
                )
            with archive.open(filename) as handle:
                workbook = pd.ExcelFile(handle)
                for sheet in config['sheets']:
                    if sheet not in workbook.sheet_names:
                        raise ValueError(
                            f'{filename} has no sheet {sheet}; it holds '
                            f'{workbook.sheet_names}'
                        )
                    frame = workbook.parse(sheet, header=None)
                    frame.attrs['sheet'] = sheet
                    frames.append(frame)
    return frames


def gdp_by_industry_call(
    *, resp: Any, source: str, config: dict[str, Any], **_: Any
) -> list[pd.DataFrame]:
    """Cache the downloaded archive, then read it.

    Only reached with ``extract_data_from_raw_sources: True``. Writing the
    archive down rather than parsing it from memory is what lets every later run
    go through :func:`gdp_by_industry_load_gcs`.
    """
    local_path = gdp_by_industry_local_path(source)
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open(local_path, 'wb') as file:
        file.write(resp.content)
    return _read_sheets_from_zip(local_path, config)


def gdp_by_industry_load_gcs(**kwargs: Any) -> list[pd.DataFrame]:
    """Load the archive from the local cache, or GCS extract-input if missing."""
    source = str(kwargs['source'])
    local_path = gdp_by_industry_local_path(source)
    if not os.path.exists(local_path):
        download_extract_input_from_gcs_if_not_exists(
            # the archive covers every year, so it sits directly under
            # extract/input-data/BEA_GDPbyIndustry/ rather than in a year folder
            {**kwargs, 'year': None},
            local_dir=os.path.dirname(local_path),
            object_name=GDP_BY_INDUSTRY_ZIP,
        )
    if not os.path.exists(local_path):
        raise FileNotFoundError(
            f'{GDP_BY_INDUSTRY_ZIP} is neither cached at {local_path} nor '
            f'available from gs://cornerstone-default/extract/input-data/'
            f'{source}/. Set extract_data_from_raw_sources: True in '
            f'{source}.yaml to fetch it from BEA and cache it, then upload it '
            f'so others need not.'
        )
    return _read_sheets_from_zip(local_path, kwargs['config'])


def _clean_industry_name(label: str) -> str:
    """Strip BEA's footnote markers and surrounding whitespace.

    The ``Addenda:`` rows carry ``\\1\\``-style references that are unrelated to
    the industry name and have no counterpart in any other BEA table.
    """
    return re.sub(r'\s*\\\d+\\\s*$', '', label).strip()


def _parse_sheet(frame: pd.DataFrame, sheet: str) -> pd.DataFrame:
    """One ``-A`` sheet as long rows: industry x component x year."""
    header = [str(value).strip() for value in frame.iloc[_HEADER_ROW].tolist()]
    if header[0] != 'Line':
        raise ValueError(
            f'{sheet} row {_HEADER_ROW} starts with {header[0]!r}, not "Line"; '
            f'the title block has changed height and the parser needs updating'
        )
    years = {
        index: int(value)
        for index, value in enumerate(header)
        if re.fullmatch(r'(19|20)\d{2}', value)
    }
    if not years:
        raise ValueError(f'{sheet} header carries no year columns: {header}')

    records = []
    industry: str | None = None
    industry_line: int | None = None
    for _, row in frame.iloc[_HEADER_ROW + 1 :].iterrows():
        label = str(row.iloc[1])
        if label == 'nan':
            continue
        stripped = label.strip()
        code = _COMPONENTS.get(stripped)
        if code is None:
            # Not a component label, so it opens a new industry. Its own row is
            # that industry's value added.
            industry = _clean_industry_name(label)
            industry_line = int(row.iloc[0])
            code = _VALUE_ADDED
        if industry is None or industry_line is None:
            raise ValueError(
                f'{sheet} states a component before any industry, at line '
                f'{row.iloc[0]}'
            )
        for column, year in years.items():
            amount = pd.to_numeric(row.iloc[column], errors='coerce')
            if pd.isna(amount):
                continue
            records.append(
                {
                    'ActivityProducedBy': industry,
                    'Code': code,
                    'Line': industry_line,
                    'Table': sheet,
                    'Year': str(year),
                    'FlowAmount': float(amount) * MILLIONS_TO_DOLLARS,
                }
            )
    return pd.DataFrame(records)


def gdp_by_industry_parse(
    *, df_list: list[pd.DataFrame], source: str, config: dict[str, Any], **_: Any
) -> pd.DataFrame:
    """Shape every requested sheet into one FBA frame, all years.

    The framework slices the result by ``Year``, so every year the sheet carries
    is emitted here rather than filtered.
    """
    sheets = config['sheets']
    parsed = [
        _parse_sheet(frame, frame.attrs.get('sheet', sheets[index]))
        for index, frame in enumerate(df_list)
    ]
    df = pd.concat(parsed, ignore_index=True)

    df['Description'] = df['Table'] + ': ' + df['Code'] + ' - ' + df['Line'].astype(str)
    df['FlowName'] = df['Code'].map(_FLOW_NAMES)
    df = df.drop(columns=['Table', 'Code', 'Line'])

    df['Class'] = 'Money'
    df['Unit'] = 'USD'
    df['SourceName'] = source
    df['ActivityConsumedBy'] = ''
    df['Compartment'] = ''
    df['FlowType'] = 'TECHNOSPHERE_FLOW'
    df['Location'] = US_FIPS
    df = assign_fips_location_system(df, 2024)
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    return df
