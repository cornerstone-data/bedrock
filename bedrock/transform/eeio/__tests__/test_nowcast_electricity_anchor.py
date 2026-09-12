"""Unit tests for nowcast EIA purchaser / A/q reanchor helpers."""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest

import bedrock.transform.eeio.electricity_gtd_allocation as gtd
from bedrock.transform.eeio.electricity_gtd_allocation import (
    ELECTRICITY_AGGREGATE,
    IMPORT_FD_CODE,
    apply_purchaser_allocation_to_y,
    purchaser_electricity_purchases_at_io_year,
    reanchor_electricity_aq_at_year,
)
from bedrock.utils.config.usa_config import USAConfig
from bedrock.utils.schemas.single_region_types import SingleRegionAqMatrixSet


def test_apply_purchaser_allocation_to_y_uses_usa_base_io_data_year(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen_years: list[int] = []
    empty = pd.Series(dtype=float)
    alloc = gtd.EIAPurchaserAllocation(
        electricity_purchases=empty,
        end_use_class=empty,
        mwh=empty,
        gen_dollars=empty,
        t_dollars=empty,
        d_dollars=empty,
        clipped=pd.Series(dtype=bool),
        p=1.0,
        egrid_mwh=1.0,
        td_share=0.06,
    )

    def fake_get(eia_year: int) -> gtd.EIAPurchaserAllocation:
        seen_years.append(eia_year)
        return alloc

    cfg = USAConfig(
        usa_detail_io_source='nowcast',
        usa_base_io_data_year=2024,
        model_base_year=2024,
        usa_ghg_data_year=2024,
        apply_io_year_adjustments=False,
        use_cornerstone_ghg_model=True,
        implement_waste_disaggregation=True,
        implement_electricity_reallocation=True,
        implement_electricity_disaggregation=True,
    )
    monkeypatch.setattr(gtd, 'get_eia_purchaser_allocation', fake_get)
    monkeypatch.setattr(
        'bedrock.utils.config.usa_config.get_usa_config',
        lambda: cfg,
    )

    Y = pd.DataFrame(
        0.0,
        index=[ELECTRICITY_AGGREGATE, '1111A0'],
        columns=['F01000'],
    )
    Y.at[ELECTRICITY_AGGREGATE, 'F01000'] = 100.0
    apply_purchaser_allocation_to_y(Y)
    assert seen_years == [2024]


def test_purchaser_electricity_purchases_at_io_year_combines_use_and_y(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    udom = pd.DataFrame(
        {
            '1111A0': [10.0],
            '452000': [20.0],
        },
        index=[ELECTRICITY_AGGREGATE],
    )
    y = pd.DataFrame(
        {
            'F01000': [5.0],
            IMPORT_FD_CODE: [99.0],
        },
        index=[ELECTRICITY_AGGREGATE],
    )
    monkeypatch.setattr(
        'bedrock.transform.eeio.electricity_disaggregation'
        '._derive_post_reallocation_checkpoint_for_disagg',
        lambda: (None, udom, None, None),
    )
    monkeypatch.setattr(
        'bedrock.transform.eeio.electricity_disaggregation'
        '._derive_y_before_electricity_disagg_lazy',
        lambda: y,
    )
    purchases = purchaser_electricity_purchases_at_io_year()
    assert IMPORT_FD_CODE not in purchases.index
    assert float(purchases['1111A0']) == pytest.approx(10.0)
    assert float(purchases['452000']) == pytest.approx(20.0)
    assert float(purchases['F01000']) == pytest.approx(5.0)


def test_reanchor_electricity_aq_at_year_skips_published_helpers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    purchases = pd.Series({'1111A0': 100.0}, dtype=float)
    empty = pd.Series(dtype=float)
    alloc = gtd.EIAPurchaserAllocation(
        electricity_purchases=purchases,
        end_use_class=empty,
        mwh=empty,
        gen_dollars=purchases * 0.7,
        t_dollars=purchases * 0.1,
        d_dollars=purchases * 0.2,
        clipped=pd.Series(False, index=purchases.index),
        p=1.0,
        egrid_mwh=1.0,
        td_share=0.06,
    )

    forbidden = {
        '_purchaser_electricity_purchases_from_aq': MagicMock(
            side_effect=AssertionError('forbidden')
        ),
        '_scaled_export_fd_electricity_purchases': MagicMock(
            side_effect=AssertionError('forbidden')
        ),
        'reanchor_electricity_aq_after_year_scaling': MagicMock(
            side_effect=AssertionError('forbidden')
        ),
    }
    for name, mock in forbidden.items():
        monkeypatch.setattr(gtd, name, mock)

    summary_scaled = MagicMock(side_effect=AssertionError('forbidden'))
    monkeypatch.setattr(
        'bedrock.transform.eeio.cornerstone_year_scaling.get_summary_year_scaled_aq',
        summary_scaled,
        raising=False,
    )

    monkeypatch.setattr(
        gtd, 'purchaser_electricity_purchases_at_io_year', lambda: purchases
    )
    monkeypatch.setattr(gtd, '_go_p_and_td_shares', lambda: (0.7, 0.3))
    allocate = MagicMock(return_value=alloc)
    monkeypatch.setattr(gtd, 'allocate_purchaser_gtd', allocate)

    codes = ['221110', '221121', '221122', '1111A0']
    adom = pd.DataFrame(0.01, index=codes, columns=codes)
    aimp = pd.DataFrame(0.0, index=codes, columns=codes)
    q = pd.Series({c: 10.0 for c in codes}, dtype=float)
    aq = SingleRegionAqMatrixSet(
        Adom=adom,  # type: ignore[arg-type]
        Aimp=aimp,  # type: ignore[arg-type]
        scaled_q=q,
    )

    sentinel = SingleRegionAqMatrixSet(
        Adom=adom,  # type: ignore[arg-type]
        Aimp=aimp,  # type: ignore[arg-type]
        scaled_q=q,
    )
    apply_mock = MagicMock(return_value=sentinel)
    monkeypatch.setattr(gtd, '_apply_eia_purchaser_allocation_to_aq', apply_mock)

    out = reanchor_electricity_aq_at_year(aq, year=2024)
    assert out is sentinel
    allocate.assert_called_once()
    assert allocate.call_args.kwargs['eia_year'] == 2024
    apply_mock.assert_called_once_with(aq, alloc, 0.3)
    for mock in forbidden.values():
        mock.assert_not_called()
    summary_scaled.assert_not_called()
