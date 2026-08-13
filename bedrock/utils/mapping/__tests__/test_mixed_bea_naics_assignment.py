"""Contracts for mixed BEA/NAICS sector assignment.

``industry_spec_key`` must keep NAICS manufacturing targets identical when a
BEA keep-set is added, and tag keep-set codes as ``BEA_2017_Code``.

``equally_attribute`` must reproduce the NAICS-only 3/3/6 split in its
docstring, and must split (not double-count) when NAICS_2 ``11`` and BEA
Sector ``11`` share a group.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal, assert_series_equal

from bedrock.extract.flowbyactivity import FlowByActivity
from bedrock.utils.mapping.sector import industry_spec_key

_NAICS_BLOCK = {
    'default_level': 'NAICS_3',
    'NAICS_4': ['311'],
    'NAICS_5': ['3112'],
    'NAICS_6': ['31111'],
}

_BEA_KEEP = ['531HSO', '531HST', '531ORE', '230301', 'F01000', 'GSLGE']


def _naics_only_spec() -> dict[str, Any]:
    return {'default_schema': 'naics', 'naics': dict(_NAICS_BLOCK)}


def _mixed_spec() -> dict[str, Any]:
    return {
        'default_schema': 'naics',
        'naics': dict(_NAICS_BLOCK),
        'bea': {'default_level': 'Detail', 'Detail': list(_BEA_KEEP)},
    }


def _naics_rows(key: pd.DataFrame, prefix: str) -> pd.DataFrame:
    out = key.loc[
        (key['SectorSourceName'] == 'NAICS_2017_Code')
        & key['source_sector'].astype(str).str.startswith(prefix),
        ['source_sector', 'target_sector', 'SectorSourceName'],
    ]
    return out.sort_values(['source_sector', 'target_sector']).reset_index(drop=True)


def test_flat_industry_spec_is_rejected() -> None:
    """``industry_spec_key`` rejects the un-nested YAML shape.

    ``{default: NAICS_3, NAICS_4: [...]}`` has no ``default_schema`` and no
    nested ``naics:`` / ``bea:`` blocks. ``industry_spec_key`` raises rather
    than mapping those codes as NAICS-only.
    """
    with pytest.raises(ValueError, match='Flat industry_spec'):
        industry_spec_key({'default': 'NAICS_3', 'NAICS_4': ['311']}, 2017)


def test_manufacturing_targets_unchanged_when_bea_keepset_added() -> None:
    """Same ``naics:`` block, with and without a ``bea:`` Detail keep-set.

    Every 311* ``source_sector`` → ``target_sector`` row stays identical and
    tagged ``NAICS_2017_Code``. Housing / construction / gov / FD keep-set
    codes (531HSO, 230301, F01000, GSLGE, …) appear as ``BEA_2017_Code``
    targets. Failure: the keep-set retargets manufacturing, or those BEA
    codes never enter the key.
    """
    naics_only = industry_spec_key(_naics_only_spec(), 2017)
    mixed = industry_spec_key(_mixed_spec(), 2017)
    assert_frame_equal(_naics_rows(naics_only, '311'), _naics_rows(mixed, '311'))
    assert (naics_only['SectorSourceName'] == 'NAICS_2017_Code').all()
    bea_targets = set(
        mixed.loc[mixed['SectorSourceName'] == 'BEA_2017_Code', 'target_sector']
    )
    assert bea_targets >= set(_BEA_KEEP)


def _plain_sum(out: pd.DataFrame, col: str) -> pd.Series:
    summed = out.groupby(col, dropna=False)['FlowAmount'].sum().sort_index()
    return pd.Series(summed.to_numpy(), index=list(summed.index), name='FlowAmount')


def _flow_by_sector(out: pd.DataFrame) -> pd.Series:
    return _plain_sum(out, 'SectorProducedBy')


def _flow_by_schema(out: pd.DataFrame) -> pd.Series:
    return _plain_sum(out, 'SectorSourceName')


def _mapped_fba(rows: list[dict[str, Any]], industry_spec: dict[str, Any]) -> FlowByActivity:
    base = {
        'Class': 'Chemicals',
        'SourceName': 'test',
        'Flowable': 'CO2',
        'Unit': 'kg',
        'FlowType': 'ELEMENTARY_FLOW',
        'Context': 'air',
        'Location': '00000',
        'LocationSystem': 'FIPS',
        'Year': 2017,
        'DataReliability': 1.0,
        'DataCollection': 1.0,
        'ActivityProducedBy': 'A',
        'SectorConsumedBy': pd.NA,
        'group_id': 0,
    }
    df = pd.DataFrame([{**base, **row} for row in rows])
    return FlowByActivity(
        df,
        convert_df_to_flowby=True,
        mapped=True,
        w_sector=True,
        config={
            'industry_spec': industry_spec,
            'target_schema_year': 2017,
            'data_format': 'FBA',
        },
        full_name='test.equal_attr',
    )


def test_equal_attribute_naics_only_matches_documented_split() -> None:
    """NAICS-only equal split: 111110=3, 111120=3, 213111=6 (sum 12).

    One group, three mapped rows, each FlowAmount 12, target NAICS_6.
    NAICS_2 parents are ``11`` (111110, 111120) and ``21`` (213111), so each
    family gets 6; the two 11* children split that 6. These are 2017 NAICS_6
    codes in ``NAICS_2017_Crosswalk`` — the same worked example as
    ``equally_attribute``. Failure: the hierarchy walk is not dividing by
    unique parents at each NAICS level (amounts stay 12).
    """
    spec = {'default_schema': 'naics', 'naics': {'default_level': 'NAICS_6'}}
    fba = _mapped_fba(
        [
            {
                'SectorProducedBy': s,
                'SectorSourceName': 'NAICS_2017_Code',
                'FlowAmount': 12.0,
            }
            for s in ('111110', '111120', '213111')
        ],
        spec,
    )
    out = fba.equally_attribute()
    got = _flow_by_sector(out)
    expected = pd.Series(
        {'111110': 3.0, '111120': 3.0, '213111': 6.0}, name='FlowAmount'
    )
    assert_series_equal(got, expected)
    assert got.sum() == pytest.approx(12.0)


def test_equal_attribute_same_code_two_schemas_splits_not_double_counts() -> None:
    """NAICS ``11`` and BEA ``11`` in one group split 12 → 6 + 6 (sum 12).

    Two mapped rows, same ``SectorProducedBy`` string ``11``, different
    ``SectorSourceName`` (``NAICS_2017_Code`` vs ``BEA_2017_Code``), each
    FlowAmount 12. Schema-qualified coarsest peers are distinct, so each
    schema gets half. Failure: the two ``11``s collapse to one peer and each
    row keeps 12 (sum 24).
    """
    spec = {
        'default_schema': 'naics',
        'naics': {'default_level': 'NAICS_2'},
        'bea': {'default_level': 'Sector', 'Sector': ['11']},
    }
    fba = _mapped_fba(
        [
            {
                'SectorProducedBy': '11',
                'SectorSourceName': 'NAICS_2017_Code',
                'FlowAmount': 12.0,
            },
            {
                'SectorProducedBy': '11',
                'SectorSourceName': 'BEA_2017_Code',
                'FlowAmount': 12.0,
            },
        ],
        spec,
    )
    out = fba.equally_attribute()
    by_schema = _flow_by_schema(out)
    expected = pd.Series(
        {'BEA_2017_Code': 6.0, 'NAICS_2017_Code': 6.0}, name='FlowAmount'
    )
    assert_series_equal(by_schema, expected)
    assert float(out['FlowAmount'].sum()) == pytest.approx(12.0)
