"""Unit tests for detail→summary MUT rollup helpers."""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.analysis.nowcasting.after_redef_MUTs.redefinitions_ratio_implementation.summary_rollup import (
    DETAIL_VA_TO_SUMMARY,
    compare_rollup_block,
    first_parent_map,
    rollup_make_to_summary,
    rollup_use_intermediate_to_summary,
    rollup_va_to_summary,
)
from bedrock.transform.iot.nowcast_redefinition_ratios import ATOL
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY as M


def test_first_parent_map_takes_parents0() -> None:
    mapping = {'1111A0': ['111CA', 'OTHER'], '1111B0': ['111CA']}
    assert first_parent_map(mapping) == {
        '1111A0': '111CA',
        '1111B0': '111CA',
    }


def test_rollup_make_toy_2x2_to_1x1(monkeypatch: pytest.MonkeyPatch) -> None:
    industry_map = {'1111A0': '111CA', '1111B0': '111CA'}
    commodity_map = {'1111A0': '111CA', '1111B0': '111CA'}
    monkeypatch.setattr(
        'bedrock.analysis.nowcasting.after_redef_MUTs.redefinitions_ratio_implementation.summary_rollup.industry_first_parent_map',
        lambda: industry_map,
    )
    monkeypatch.setattr(
        'bedrock.analysis.nowcasting.after_redef_MUTs.redefinitions_ratio_implementation.summary_rollup.commodity_first_parent_map',
        lambda: commodity_map,
    )
    # Bypass full summary reindex for the toy by patching codes used in reindex.
    monkeypatch.setattr(
        'bedrock.analysis.nowcasting.after_redef_MUTs.redefinitions_ratio_implementation.summary_rollup.USA_2017_SUMMARY_INDUSTRY_CODES',
        ['111CA'],
    )
    monkeypatch.setattr(
        'bedrock.analysis.nowcasting.after_redef_MUTs.redefinitions_ratio_implementation.summary_rollup.USA_2017_SUMMARY_COMMODITY_CODES',
        ['111CA'],
    )
    monkeypatch.setattr(
        'bedrock.analysis.nowcasting.after_redef_MUTs.redefinitions_ratio_implementation.summary_rollup.USA_2017_SUMMARY_INDUSTRY_INDEX',
        pd.Index(['111CA']),
    )
    monkeypatch.setattr(
        'bedrock.analysis.nowcasting.after_redef_MUTs.redefinitions_ratio_implementation.summary_rollup.USA_2017_SUMMARY_COMMODITY_INDEX',
        pd.Index(['111CA']),
    )
    V = pd.DataFrame(
        [[10.0, 20.0], [30.0, 40.0]],
        index=['1111A0', '1111B0'],
        columns=['1111A0', '1111B0'],
    )
    rolled = rollup_make_to_summary(V)
    assert float(rolled.loc['111CA', '111CA']) == pytest.approx(100.0)


def test_rollup_va_remaps_detail_codes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        'bedrock.analysis.nowcasting.after_redef_MUTs.redefinitions_ratio_implementation.summary_rollup.industry_first_parent_map',
        lambda: {'1111A0': '111CA'},
    )
    monkeypatch.setattr(
        'bedrock.analysis.nowcasting.after_redef_MUTs.redefinitions_ratio_implementation.summary_rollup.USA_2017_SUMMARY_INDUSTRY_CODES',
        ['111CA'],
    )
    monkeypatch.setattr(
        'bedrock.analysis.nowcasting.after_redef_MUTs.redefinitions_ratio_implementation.summary_rollup.USA_2017_SUMMARY_INDUSTRY_INDEX',
        pd.Index(['111CA']),
    )
    monkeypatch.setattr(
        'bedrock.analysis.nowcasting.after_redef_MUTs.redefinitions_ratio_implementation.summary_rollup.USA_2017_INDUSTRY_CODES',
        ['1111A0'],
    )
    VA = pd.DataFrame(
        [[5.0], [7.0], [9.0]],
        index=['V00100', 'V00200', 'V00300'],
        columns=['1111A0'],
    )
    rolled = rollup_va_to_summary(VA)
    assert list(rolled.index) == ['V001', 'V002', 'V003']
    assert float(rolled.loc['V001', '111CA']) == pytest.approx(5.0)
    assert DETAIL_VA_TO_SUMMARY['V00100'] == 'V001'


def test_rollup_unmapped_code_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        'bedrock.analysis.nowcasting.after_redef_MUTs.redefinitions_ratio_implementation.summary_rollup.industry_first_parent_map',
        lambda: {'1111A0': '111CA'},
    )
    monkeypatch.setattr(
        'bedrock.analysis.nowcasting.after_redef_MUTs.redefinitions_ratio_implementation.summary_rollup.commodity_first_parent_map',
        lambda: {'1111A0': '111CA'},
    )
    monkeypatch.setattr(
        'bedrock.analysis.nowcasting.after_redef_MUTs.redefinitions_ratio_implementation.summary_rollup.USA_2017_INDUSTRY_CODES',
        ['1111A0', '999999'],
    )
    U = pd.DataFrame(
        [[1.0, 2.0]],
        index=['1111A0'],
        columns=['1111A0', '999999'],
    )
    with pytest.raises(ValueError, match='unmapped column codes'):
        rollup_use_intermediate_to_summary(U)


def test_compare_rollup_block_pass_and_fail() -> None:
    ref = pd.DataFrame([[10.0 * M]], index=['111CA'], columns=['111CA'])
    same = ref.copy()
    ok = compare_rollup_block(same, ref, label='Make')
    assert ok.ok
    assert ok.n_partial == 0
    bad = pd.DataFrame([[10.0 * M + 2 * ATOL]], index=['111CA'], columns=['111CA'])
    fail = compare_rollup_block(bad, ref, label='Make')
    assert not fail.ok
    assert fail.n_partial >= 1
