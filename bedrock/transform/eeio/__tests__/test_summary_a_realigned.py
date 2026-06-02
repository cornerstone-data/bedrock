"""Tests for the summary-tables A branch with dollar-year-aligned scaling.

``scale_a_matrix_with_summary_tables=True`` now always:
  1. deflates the target-year summary A to 2017 USD before forming the ratio
     against the 2017 summary A (so the ratio is in matched dollar years),
  2. applies the structural ratio to the 2017 detail A,
  3. inflates the result 2017 → model_year.
"""

from __future__ import annotations

import pytest

from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_Aq,
    derive_cornerstone_Aq_scaled,
)
from bedrock.utils.config.usa_config import get_usa_config


def _set_summary_tables(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = get_usa_config()
    monkeypatch.setattr(cfg, 'use_cornerstone_2026_model_schema', True)
    monkeypatch.setattr(cfg, 'scale_a_matrix_with_summary_tables', True)
    monkeypatch.setattr(cfg, 'adjust_summary_A_and_q_dollar_year', True)
    monkeypatch.setattr(cfg, 'scale_a_matrix_with_useeio_method', False)
    monkeypatch.setattr(cfg, 'scale_a_matrix_with_commodity_price_index', False)
    derive_cornerstone_Aq_scaled.cache_clear()


def test_summary_tables_branch_is_noop_at_model_year_equals_detail_year(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When ``model_year == detail_year == 2017``, the cross-year ratio is
    identically 1, the summary-A deflation is a no-op, and the final inflate
    is a no-op — so the summary-tables branch must return the base 2017 A
    unchanged.
    """
    cfg = get_usa_config()
    _set_summary_tables(monkeypatch)
    monkeypatch.setattr(cfg, 'model_base_year', 2017)

    base = derive_cornerstone_Aq()
    result = derive_cornerstone_Aq_scaled()

    max_dev_dom = (result.Adom.to_numpy() - base.Adom.to_numpy()).max()
    max_dev_imp = (result.Aimp.to_numpy() - base.Aimp.to_numpy()).max()
    assert (
        abs(max_dev_dom) < 1e-9
    ), f"Adom drifted at model_year=2017 (max |Δ| = {max_dev_dom:.2e})"
    assert (
        abs(max_dev_imp) < 1e-9
    ), f"Aimp drifted at model_year=2017 (max |Δ| = {max_dev_imp:.2e})"


def test_summary_tables_branch_no_nan_no_negatives(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Summary-tables A and q should have no NaN and no negative entries —
    the deflate step preserves sign, ``scale_cornerstone_A``'s downstream
    cap enforces non-negativity on the per-matrix output, and the final
    inflate is sign-preserving.

    Column-sum ≤ 1 is not asserted: the final price-index inflation
    (``diag(p) @ A @ diag(1/p)``) can push some columns above 1, matching
    the default CEDA branch's behavior — the codebase does not re-cap after
    inflation.
    """
    cfg = get_usa_config()
    _set_summary_tables(monkeypatch)
    monkeypatch.setattr(cfg, 'model_base_year', 2023)

    result = derive_cornerstone_Aq_scaled()
    assert not result.Adom.isna().to_numpy().any(), 'Adom has NaN'
    assert not result.Aimp.isna().to_numpy().any(), 'Aimp has NaN'
    assert (result.Adom.to_numpy() >= 0).all(), 'Adom has negatives'
    assert (result.Aimp.to_numpy() >= 0).all(), 'Aimp has negatives'
    assert not result.scaled_q.isna().to_numpy().any(), 'scaled_q has NaN'
