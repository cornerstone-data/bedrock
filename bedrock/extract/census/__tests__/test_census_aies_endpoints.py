"""AIES on the per-year Census datasets (#707).

AIES moved from ``timeseries/aies/*`` to ``data/<year>/aies<dataset>`` in
2026-09, and the old path now returns 404 for **every** year, 2023 included.
The migration is not a one-line url swap, and these tests pin the three things
that were wrong on the first attempt:

* the pull is now **two requests joined**, because gross margin and the NAICS
  486 pipeline items live in different datasets and neither contains the other;
* the loaders run **once per url**, so returning every leg per call duplicates
  the table;
* ``urlencode`` percent-encodes the variable separator, so a filter that splits
  the ``get`` list on a bare comma silently matches nothing.

These are config- and pure-function checks: none of them calls Census.
"""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.extract.census.Census_AIES import (
    _aies_dataset_from_url,
    _join_aies_legs,
    _normalise_aies_columns,
    census_aies_expenses_url_helper,
    census_aies_url_helper,
)
from bedrock.extract.generateflowbyactivity import load_fba_config

AIES_SOURCES = (
    'Census_AIES',
    'Census_AIES_MiscSector',
    'Census_AIES_Expenses',
    'Census_AIES_Service_Expenses',
)


@pytest.mark.parametrize('source', AIES_SOURCES)
def test_no_source_still_points_at_the_retired_timeseries_path(source: str) -> None:
    """``timeseries/aies`` 404s for every year, so no source may still use it."""
    _, _, config = load_fba_config(source, 2023)
    api_path = config['url']['api_path']
    assert 'timeseries' not in api_path, f'{source} still on the retired path'
    assert '__year__/' in api_path, f'{source} must select the per-year dataset'


@pytest.mark.parametrize('source', AIES_SOURCES)
def test_no_source_sends_a_time_predicate(source: str) -> None:
    """The per-year datasets carry no ``time`` variable; the year is the path."""
    _, _, config = load_fba_config(source, 2023)
    assert 'time' not in config['url']['url_params']


@pytest.mark.parametrize('source', AIES_SOURCES)
def test_industry_column_is_requested_by_its_vintage_name(source: str) -> None:
    """The per-year datasets call the industry column ``NAICS2017``."""
    _, _, config = load_fba_config(source, 2023)
    requested = config['url']['url_params'].get('get', '')
    if requested == '__get__':  # per-dataset, see Census_AIES.yaml
        requested = ','.join(config['datasets'].values())
    assert 'NAICS2017' in requested
    assert not requested.startswith('NAICS,')


def test_census_aies_issues_one_request_per_dataset() -> None:
    """Gross margin and the 486 pipeline items need both datasets."""
    _, _, config = load_fba_config('Census_AIES', 2023)
    urls = census_aies_url_helper(
        build_url='https://x/data/2023/__dataset__?get=__get__&for=us:*',
        config=config,
    )
    assert len(urls) == 2
    assert {_aies_dataset_from_url(u) for u in urls} == {
        'aiesbasic',
        'aiesmiscsector',
    }
    # each url must carry its own variable list, not the placeholder
    assert not any('__get__' in u for u in urls)


def test_join_is_a_join_not_a_concat() -> None:
    """Stacking the legs would double the rows and blank half the columns."""
    basic = pd.DataFrame(
        {
            'NAICS': ['42', '486'],
            'TYPOP': ['1X', '00'],
            'TAXSTAT': ['00', '00'],
            'RCPT_TOT_VAL': [100.0, 7.0],
        }
    )
    margin = pd.DataFrame(
        {
            'NAICS': ['42'],
            'TYPOP': ['1X'],
            'TAXSTAT': ['00'],
            'RCPT_GM_DVAL': [20.0],
        }
    )
    joined = _join_aies_legs([basic, margin])
    assert len(joined) == 2, 'the join must not add rows'
    row = joined[joined['NAICS'] == '42'].iloc[0]
    assert row['RCPT_TOT_VAL'] == 100.0 and row['RCPT_GM_DVAL'] == 20.0
    # a row the margin leg does not carry keeps its sales and gets no margin
    assert pd.isna(joined[joined['NAICS'] == '486'].iloc[0]['RCPT_GM_DVAL'])


def test_join_passes_a_single_leg_through() -> None:
    """The pre-2026-09 single-file cache is one already-wide frame."""
    wide = pd.DataFrame(
        {
            'NAICS': ['42'],
            'TYPOP': ['1X'],
            'TAXSTAT': ['00'],
            'RCPT_TOT_VAL': [100.0],
            'RCPT_GM_DVAL': [20.0],
        }
    )
    assert _join_aies_legs([wide, pd.DataFrame()]).equals(wide)


def test_normalise_renames_the_vintage_columns() -> None:
    df = pd.DataFrame({'NAICS2017': ['42'], 'NAICS2017_LABEL': ['Wholesale']})
    out = _normalise_aies_columns(df)
    assert list(out.columns) == ['NAICS', 'NAICS_LABEL']


@pytest.mark.parametrize('separator', [',', '%2C'])
def test_retired_expense_variables_are_dropped_for_2024(separator: str) -> None:
    """⚠️ urlencode emits ``%2C``; splitting on a bare comma drops nothing."""
    variables = separator.join(
        ['NAICS2017', 'EXPS_TOT_DVAL', 'EXPS_RENT_BUILD_VAL', 'EXPS_COMMSVC_VAL']
    )
    url = f'https://x/data/2024/aiesexp02?get={variables}&for=us:*'
    (out,) = census_aies_expenses_url_helper(build_url=url, year=2024)
    assert 'EXPS_RENT_BUILD_VAL' not in out
    assert 'EXPS_COMMSVC_VAL' not in out
    assert 'EXPS_TOT_DVAL' in out and 'NAICS2017' in out
    assert 'for=us:*' in out, 'the rest of the query must survive'


def test_2023_keeps_every_expense_variable() -> None:
    """2023 still publishes all four, so the year must not be filtered."""
    variables = 'NAICS2017%2CEXPS_RENT_BUILD_VAL%2CEXPS_COMMSVC_VAL'
    url = f'https://x/data/2023/aiesexp02?get={variables}&for=us:*'
    (out,) = census_aies_expenses_url_helper(build_url=url, year=2023)
    assert out == url
