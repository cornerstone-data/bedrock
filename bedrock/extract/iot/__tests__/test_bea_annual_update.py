"""BEA annual update check: every industry-accounts input on one update."""

from __future__ import annotations

import logging

import pytest

from bedrock.extract.iot.bea_annual_update import (
    NEWEST_BEA_ANNUAL_UPDATE,
    annual_update_from_last_year,
    assert_same_bea_annual_update,
)

GROSS_OUTPUT = 'gross output UGO305-A (2026Q2)'
SUPPLY = 'Supply_Tables_1997-2025_Summary.xlsx'
USE = 'Use_Tables_Supply-Use_Framework_1997-2025_Summary.xlsx'
VALUE_ADDED = 'TVA113-A (GDPbyInd_2026Q2.zip)'


def _all_on(update: int) -> dict[str, int]:
    return {GROSS_OUTPUT: update, SUPPLY: update, USE: update, VALUE_ADDED: update}


def test_update_is_the_year_after_the_last_year_published() -> None:
    # The 2026 update (September 2026) publishes through 2025; the old
    # 2017-2022 workbook ends in 2022, the 2023 update.
    assert annual_update_from_last_year(2025) == 2026
    assert annual_update_from_last_year(2022) == 2023


def test_same_update_passes_and_returns_it() -> None:
    update = assert_same_bea_annual_update(_all_on(NEWEST_BEA_ANNUAL_UPDATE))
    assert update == NEWEST_BEA_ANNUAL_UPDATE


def test_mismatch_raises_and_names_the_workbook() -> None:
    updates = {
        **_all_on(2026),
        'Supply_Tables_1997-2024_Summary.xlsx': 2025,
    }
    del updates[SUPPLY]
    with pytest.raises(ValueError, match='Supply_Tables_1997-2024_Summary.xlsx'):
        assert_same_bea_annual_update(updates)


def test_value_added_components_on_an_older_update_raises() -> None:
    # The June 2026 quarterly archive ends in 2024: the 2025 update by content.
    updates = {**_all_on(2026), VALUE_ADDED: 2025}
    with pytest.raises(ValueError, match='TVA113-A'):
        assert_same_bea_annual_update(updates)


def test_older_than_newest_update_warns(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The bedrock logger does not propagate to the root logger caplog listens on.
    monkeypatch.setattr(logging.getLogger('bedrock'), 'propagate', True)
    older = NEWEST_BEA_ANNUAL_UPDATE - 1
    with caplog.at_level(logging.WARNING):
        assert_same_bea_annual_update(_all_on(older))
    assert f'newest is {NEWEST_BEA_ANNUAL_UPDATE}' in caplog.text
