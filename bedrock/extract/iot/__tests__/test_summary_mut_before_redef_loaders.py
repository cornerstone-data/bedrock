"""Tests for before-redef and 2024-vintage summary MUT loaders."""

from __future__ import annotations

import pytest

from bedrock.extract.iot import io_2017
from bedrock.utils.taxonomy.bea.matrix_mappings import (
    USA_SUMMARY_MUT_MAPPING_1997_2022,
    USA_SUMMARY_MUT_MAPPING_1997_2024,
)
from bedrock.utils.taxonomy.bea.v2017_value_added import SUMMARY_VA_CODES


def test_span_year_guard_rejects_pre_2017() -> None:
    with pytest.raises(ValueError, match='out of span domain'):
        io_2017.load_summary_V_before_redef_usa(2016)
    with pytest.raises(ValueError, match='out of span domain'):
        io_2017.load_summary_V_usa_2024_vintage(2016)


def test_2024_vintage_mapping_filenames() -> None:
    """Span after loaders always use the 1997–2024-named workbooks."""
    assert USA_SUMMARY_MUT_MAPPING_1997_2024['Make_summary'].endswith(
        '1997-2024_Summary.xlsx'
    )
    # Production pin for 2017/2022 stays on the older workbook.
    assert '1997-2022' in USA_SUMMARY_MUT_MAPPING_1997_2022['Make_summary']


def test_require_summary_span_year_bounds() -> None:
    assert io_2017._require_summary_span_year(2017) == 2017
    assert io_2017._require_summary_span_year(2024) == 2024
    with pytest.raises(ValueError, match='out of span domain'):
        io_2017._require_summary_span_year(2025)


def test_summary_va_codes_constant() -> None:
    assert SUMMARY_VA_CODES == ('V001', 'V002', 'V003')


@pytest.mark.realdata
def test_before_and_2024_vintage_loaders_2017_shapes() -> None:
    V_b = io_2017.load_summary_V_before_redef_usa(2017)
    U_b = io_2017.load_summary_Utot_before_redef_usa(2017)
    Uimp_b = io_2017.load_summary_Uimp_before_redef_usa(2017)
    VA_b = io_2017.load_summary_value_added_before_redef_usa(2017)
    assert V_b.shape == (71, 73)
    assert U_b.shape == (73, 71)
    assert Uimp_b.shape == (73, 71)
    assert VA_b.shape == (3, 71)
    assert list(VA_b.index) == list(SUMMARY_VA_CODES)

    V_a = io_2017.load_summary_V_usa_2024_vintage(2017)
    U_a = io_2017.load_summary_Utot_usa_2024_vintage(2017)
    Uimp_a = io_2017.load_summary_Uimp_usa_2024_vintage(2017)
    VA_a = io_2017.load_summary_value_added_usa_2024_vintage(2017)
    assert V_a.shape == (71, 73)
    assert U_a.shape == (73, 71)
    assert Uimp_a.shape == (73, 71)
    assert VA_a.shape == (3, 71)
    assert list(VA_a.index) == list(SUMMARY_VA_CODES)


@pytest.mark.realdata
def test_before_and_2024_vintage_loaders_2024() -> None:
    V_b = io_2017.load_summary_V_before_redef_usa(2024)
    U_b = io_2017.load_summary_Utot_before_redef_usa(2024)
    Uimp_b = io_2017.load_summary_Uimp_before_redef_usa(2024)
    VA_b = io_2017.load_summary_value_added_before_redef_usa(2024)
    assert V_b.shape == (71, 73)
    assert U_b.shape == (73, 71)
    assert Uimp_b.shape == (73, 71)
    assert VA_b.shape == (3, 71)

    V_a = io_2017.load_summary_V_usa_2024_vintage(2024)
    assert V_a.shape == (71, 73)
    VA_a = io_2017.load_summary_value_added_usa_2024_vintage(2024)
    assert list(VA_a.index) == list(SUMMARY_VA_CODES)
