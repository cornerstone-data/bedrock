"""Unit tests for the 191-line to BEA-detail allocation.

The arithmetic takes frames, so none of this needs GCS or the gross output
parquet. The year-by-year reconciliation against BEA's published tables is a
CLI flag on ``bedrock/analysis/nowcasting/underlying_industry_coverage.py``,
not a test.
"""

import numpy as np
import pandas as pd
import pytest

from bedrock.transform.iot.derived_intermediate_and_value_added import (
    ANCHOR_YEAR,
    allocate_underlying_to_detail,
)

MAPPING = {10: ['aaa', 'bbb'], 20: ['ccc']}
YEARS = [2017, 2018]


def _gross_output() -> pd.DataFrame:
    return pd.DataFrame(
        {2017: [100.0, 300.0, 500.0], 2018: [120.0, 300.0, 400.0]},
        index=pd.Index(['aaa', 'bbb', 'ccc'], name='industry'),
    )


def _anchor() -> pd.Series:
    # aaa is half value added, bbb a quarter, ccc a fifth.
    return pd.Series(
        {'aaa': 50.0, 'bbb': 75.0, 'ccc': 100.0}, name=ANCHOR_YEAR
    ).rename_axis('industry')


def _group_values() -> pd.DataFrame:
    return pd.DataFrame(
        {2017: [125.0, 100.0], 2018: [150.0, 90.0]},
        index=pd.Index([10, 20], name='line'),
    )


def test_group_totals_are_preserved_exactly() -> None:
    out = allocate_underlying_to_detail(
        _group_values(), _anchor(), _gross_output(), MAPPING
    )
    for line, children in MAPPING.items():
        for year in YEARS:
            assert out.loc[children, year].sum() == pytest.approx(
                _group_values().loc[line, year]
            )


def test_anchor_year_reproduces_the_anchor() -> None:
    """At the anchor year the rescale is the identity."""
    out = allocate_underlying_to_detail(
        _group_values(), _anchor(), _gross_output(), MAPPING
    )
    pd.testing.assert_series_equal(
        out[ANCHOR_YEAR],
        _anchor().rename(ANCHOR_YEAR),
        check_names=False,
    )


def test_weights_move_on_each_industrys_own_gross_output() -> None:
    """aaa grows 20% and bbb is flat, so aaa takes a larger share in 2018."""
    out = allocate_underlying_to_detail(
        _group_values(), _anchor(), _gross_output(), MAPPING
    )
    share_2017 = out.loc['aaa', 2017] / out.loc[['aaa', 'bbb'], 2017].sum()
    share_2018 = out.loc['aaa', 2018] / out.loc[['aaa', 'bbb'], 2018].sum()
    assert share_2017 == pytest.approx(50.0 / 125.0)
    # 0.5 * 120 / (0.5 * 120 + 0.25 * 300)
    assert share_2018 == pytest.approx(60.0 / 135.0)
    assert share_2018 > share_2017


def test_singleton_line_takes_the_whole_value() -> None:
    out = allocate_underlying_to_detail(
        _group_values(), _anchor(), _gross_output(), MAPPING
    )
    assert out.loc['ccc', 2018] == pytest.approx(90.0)


def test_a_zero_anchor_group_falls_back_to_gross_output_share() -> None:
    anchor = _anchor().copy()
    anchor[['aaa', 'bbb']] = 0.0
    group_values = _group_values().copy()
    group_values.loc[10] = [0.0, 90.0]
    out = allocate_underlying_to_detail(group_values, anchor, _gross_output(), MAPPING)
    # 2018 gross output is 120 / 300, so aaa takes 120/420 of 90.
    assert out.loc['aaa', 2018] == pytest.approx(90.0 * 120.0 / 420.0)
    assert out.loc[['aaa', 'bbb'], 2018].sum() == pytest.approx(90.0)


def test_negative_anchor_is_carried_not_clipped() -> None:
    """A published negative VAPRO stays negative; S00201 is a real case."""
    anchor = _anchor().copy()
    anchor['aaa'] = -50.0
    group_values = _group_values().copy()
    group_values.loc[10] = [25.0, 25.0]
    out = allocate_underlying_to_detail(group_values, anchor, _gross_output(), MAPPING)
    assert out.loc['aaa', ANCHOR_YEAR] == pytest.approx(-50.0)
    assert out.loc['bbb', ANCHOR_YEAR] == pytest.approx(75.0)


def test_suppressed_group_values_raise() -> None:
    group_values = _group_values().copy()
    group_values.loc[10, 2018] = np.nan
    with pytest.raises(ValueError, match='suppressed'):
        allocate_underlying_to_detail(group_values, _anchor(), _gross_output(), MAPPING)


def test_missing_anchor_year_raises() -> None:
    gross_output = _gross_output().drop(columns=[ANCHOR_YEAR])
    with pytest.raises(ValueError, match='anchor year'):
        allocate_underlying_to_detail(_group_values(), _anchor(), gross_output, MAPPING)


def test_missing_year_raises() -> None:
    gross_output = _gross_output().drop(columns=[2018])
    with pytest.raises(ValueError, match='missing years'):
        allocate_underlying_to_detail(_group_values(), _anchor(), gross_output, MAPPING)


def test_missing_industry_in_anchor_raises() -> None:
    anchor = _anchor().drop('bbb')
    with pytest.raises(KeyError, match='bbb'):
        allocate_underlying_to_detail(_group_values(), anchor, _gross_output(), MAPPING)


def test_zero_anchor_year_gross_output_raises() -> None:
    gross_output = _gross_output().copy()
    gross_output.loc['bbb', ANCHOR_YEAR] = 0.0
    with pytest.raises(ValueError, match='gross output'):
        allocate_underlying_to_detail(_group_values(), _anchor(), gross_output, MAPPING)


def test_output_is_in_mapping_code_order() -> None:
    out = allocate_underlying_to_detail(
        _group_values(), _anchor(), _gross_output(), MAPPING
    )
    assert list(out.index) == ['aaa', 'bbb', 'ccc']
    assert list(out.columns) == YEARS


def test_a_code_under_two_lines_raises() -> None:
    mapping = {10: ['aaa', 'bbb'], 20: ['bbb', 'ccc']}
    with pytest.raises(ValueError, match='more than one line'):
        allocate_underlying_to_detail(
            _group_values(), _anchor(), _gross_output(), mapping
        )


def test_duplicated_line_raises() -> None:
    group_values = pd.concat([_group_values(), _group_values().loc[[10]]])
    with pytest.raises(ValueError, match='duplicated lines'):
        allocate_underlying_to_detail(group_values, _anchor(), _gross_output(), MAPPING)
