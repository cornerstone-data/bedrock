"""Integration: 2017 ratio apply matches published after-redef MUT."""

from __future__ import annotations

from pathlib import Path

import pytest

from bedrock.analysis.nowcasting.sections import (
    MAKE_AFTER_REDEF_DETAIL_MUT,
    UIMP_AFTER_REDEF_DETAIL_MUT,
    USE_AFTER_REDEF_DETAIL_MUT,
    VA_AFTER_REDEF_DETAIL_MUT,
    compare_redef_margins_2017,
)
from bedrock.transform.iot.nowcast_redefinition_ratios import (
    RATIOS_MARGINS_PATH,
    RATIOS_U_PATH,
    RATIOS_UIMP_PATH,
    RATIOS_V_PATH,
    RATIOS_VA_PATH,
)

_RATIO_PATHS = (
    RATIOS_V_PATH,
    RATIOS_U_PATH,
    RATIOS_VA_PATH,
    RATIOS_UIMP_PATH,
    RATIOS_MARGINS_PATH,
)


@pytest.mark.eeio_integration
def test_2017_ratio_full_matches_published_after() -> None:
    for path in _RATIO_PATHS:
        assert Path(path).exists(), f'missing ratio artifact: {path}'

    MAKE_AFTER_REDEF_DETAIL_MUT.run(2017).assert_ok(
        max_partial=0, max_miss=0, max_extra=0, max_margin_partial=0
    )
    USE_AFTER_REDEF_DETAIL_MUT.run(2017).assert_ok(
        max_partial=0, max_miss=0, max_extra=0, max_margin_partial=0
    )
    VA_AFTER_REDEF_DETAIL_MUT.run(2017).assert_ok(
        max_partial=0, max_miss=0, max_extra=0, max_margin_partial=0
    )
    UIMP_AFTER_REDEF_DETAIL_MUT.run(2017).assert_ok(
        max_partial=0, max_miss=0, max_extra=0, max_margin_partial=0
    )
    compare_redef_margins_2017(2017).assert_ok(
        max_partial=0, max_miss=0, max_extra=0, max_margin_partial=0
    )
