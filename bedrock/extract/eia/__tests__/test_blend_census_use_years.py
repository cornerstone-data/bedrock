"""#1210: the Use that splits MECS parents moves from 2017 to 2022 along a line."""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from bedrock.extract.eia.EIA_MECS import blend_census_use_years
from bedrock.extract.flowbyactivity import FlowByActivity

# Row 221200 (gas): 2017 splits 80/20 between 325110 and 325190, 2022 splits
# 40/60. 2022 dollars are twice 2017's, which the blend must ignore.
USE = {
    2017: [('221200', '325110', 80.0), ('221200', '325190', 20.0)],
    2022: [('221200', '325110', 80.0), ('221200', '325190', 120.0)],
}


def _use(year: int, rows: list[tuple[str, str, float]]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                'ActivityProducedBy': apb,
                'ActivityConsumedBy': acb,
                'FlowAmount': amount,
                'Year': year,
                'Location': '00000',
                'Unit': 'USD',
            }
            for apb, acb, amount in rows
        ]
    )


def _late(
    inventory_year: int, use: dict[int, list[tuple[str, str, float]]] = USE
) -> FlowByActivity:
    return FlowByActivity(
        _use(2022, use[2022]),
        full_name='Nowcast_Detail_Use_AfterRedef',
        config={
            'year': 2022,
            'use_blend': {
                'inventory_year': inventory_year,
                'early_year': 2017,
                'late_year': 2022,
            },
        },
    )


def _patch_early(
    monkeypatch: pytest.MonkeyPatch, use: dict[int, list[tuple[str, str, float]]] = USE
) -> None:
    def _return_fba(
        cls: Any, full_name: str, year: int, config: dict[str, Any], **_k: Any
    ) -> FlowByActivity:
        return FlowByActivity(_use(year, use[year]), full_name=full_name, config=config)

    monkeypatch.setattr(FlowByActivity, 'return_FBA', classmethod(_return_fba))


def _amounts(fba: pd.DataFrame) -> dict[str, float]:
    return dict(fba.groupby('ActivityConsumedBy')['FlowAmount'].sum().astype(float))


@pytest.mark.parametrize(
    ('inventory_year', 'share_325110'),
    [(2017, 0.8), (2018, 0.72), (2020, 0.56), (2021, 0.48), (2022, 0.4), (2024, 0.4)],
)
def test_split_moves_linearly_between_census_years(
    monkeypatch: pytest.MonkeyPatch, inventory_year: int, share_325110: float
) -> None:
    _patch_early(monkeypatch)
    out = _amounts(blend_census_use_years(_late(inventory_year)))
    assert out['325110'] / sum(out.values()) == pytest.approx(share_325110)
    # Row total stays on the late-year Use.
    assert sum(out.values()) == pytest.approx(200.0)


def test_cell_in_one_year_only_counts_as_zero_in_the_other(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    use = {
        2017: [('221200', '325110', 50.0), ('221200', '325120', 50.0)],
        2022: [('221200', '325110', 100.0)],
    }
    _patch_early(monkeypatch, use)
    out = _amounts(blend_census_use_years(_late(2019, use)))
    # v = 0.6: 325120 = 0.6 x 0.5 = 0.3 of the row; 325110 = 0.6 x 0.5 + 0.4.
    assert out['325120'] == pytest.approx(30.0)
    assert out['325110'] == pytest.approx(70.0)


def test_late_year_and_after_return_the_late_use(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _fail(*_a: Any, **_k: Any) -> None:
        raise AssertionError('2017 Use should not be loaded from 2022 on')

    monkeypatch.setattr(FlowByActivity, 'return_FBA', classmethod(_fail))
    late = _late(2023)
    pd.testing.assert_frame_equal(blend_census_use_years(late), late)
