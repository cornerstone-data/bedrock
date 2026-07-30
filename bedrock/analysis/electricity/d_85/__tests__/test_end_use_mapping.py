"""Tests for cornerstone → EPA end-use mapping."""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.analysis.electricity.d_85.end_use_mapping import (
    build_end_use_map,
    build_price_tilt_weights_by_column,
    classify_industry_end_use,
)
from bedrock.utils.schemas.cornerstone_schemas import CORNERSTONE_COMMODITIES_ELEC
from bedrock.utils.taxonomy.cornerstone.final_demand import FINAL_DEMANDS


def test_classify_industry_end_use_rules() -> None:
    assert classify_industry_end_use('331110') == ('Industrial', 'naics_31-33')
    assert classify_industry_end_use('481000') == ('Transportation', 'naics_48-49')
    assert classify_industry_end_use('541000') == ('Commercial', 'naics_52-81')
    assert classify_industry_end_use('221110') == ('Industrial', 'electricity_child')


def test_build_end_use_map_covers_industries_and_fd() -> None:
    mapping = build_end_use_map()
    for code in CORNERSTONE_COMMODITIES_ELEC:
        assert code in mapping
    for code in ('221110', '221121', '221122'):
        assert mapping[code] == 'Industrial'
    for fd in FINAL_DEMANDS:
        assert fd in mapping


def test_price_tilt_weights_sum_to_one() -> None:
    w_base = pd.Series({'221110': 0.34, '221121': 0.04, '221122': 0.62})
    prices = {
        'Residential': 12.0,
        'Commercial': 10.0,
        'Industrial': 7.0,
        'Transportation': 9.0,
        'Total': 10.0,
    }
    end_use_map = {'541000': 'Commercial', 'F01000': 'Residential'}
    w_by_col = build_price_tilt_weights_by_column(
        w_base, prices, end_use_map, ['541000', 'F01000']
    )
    for col in w_by_col.columns:
        assert pytest.approx(float(w_by_col[col].sum())) == 1.0
