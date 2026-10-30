"""BEA annual update check: gross output and summary supply-use on one update."""

from __future__ import annotations

import logging

import pytest

from bedrock.extract.iot.bea_annual_update import (
    NEWEST_BEA_ANNUAL_UPDATE,
    annual_update_from_last_year,
    assert_same_bea_annual_update,
)

SUPPLY = 'Supply_Tables_1997-2024_Summary.xlsx'
USE = 'Use_Tables_Supply-Use_Framework_1997-2024_Summary.xlsx'


def test_update_is_the_year_after_the_last_year_published() -> None:
    # The 2025 update (September 2025) publishes through 2024; the old
    # 2017-2022 workbook ends in 2022, the 2023 update.
    assert annual_update_from_last_year(2024) == 2025
    assert annual_update_from_last_year(2022) == 2023


def test_same_update_passes_and_returns_it() -> None:
    update = assert_same_bea_annual_update(
        gross_output=NEWEST_BEA_ANNUAL_UPDATE,
        summary_sut={SUPPLY: NEWEST_BEA_ANNUAL_UPDATE, USE: NEWEST_BEA_ANNUAL_UPDATE},
    )
    assert update == NEWEST_BEA_ANNUAL_UPDATE


def test_mismatch_raises_and_names_the_workbook() -> None:
    with pytest.raises(ValueError, match='Supply_Tables_2017-2022_Summary.xlsx'):
        assert_same_bea_annual_update(
            gross_output=2025,
            summary_sut={'Supply_Tables_2017-2022_Summary.xlsx': 2023, USE: 2025},
        )


def test_older_than_newest_update_warns(caplog: pytest.LogCaptureFixture) -> None:
    older = NEWEST_BEA_ANNUAL_UPDATE - 1
    with caplog.at_level(logging.WARNING):
        assert_same_bea_annual_update(
            gross_output=older, summary_sut={SUPPLY: older, USE: older}
        )
    assert f'newest is {NEWEST_BEA_ANNUAL_UPDATE}' in caplog.text
