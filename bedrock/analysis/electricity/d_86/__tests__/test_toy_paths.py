"""Tests for the three Methods #86 toy paths."""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.analysis.electricity.d_86.toy_paths import (
    assert_section2_matches_section3,
    run_section1_production,
    run_section2_flow_mixed,
    run_section3_direct_mixed,
)
from bedrock.transform.eeio.electricity_disaggregation import GENERATION_SECTOR


def test_section1_production_identities() -> None:
    result = run_section1_production()
    ef = result.ef
    assert ef.commodity_identity is not None
    assert ef.leontief_identity is not None
    assert ef.commodity_identity.passed
    assert ef.leontief_identity.passed


def test_section2_flow_mixed_matches_section3() -> None:
    s2 = run_section2_flow_mixed()
    s3 = run_section3_direct_mixed()
    assert_section2_matches_section3(s2, s3)


def test_section3_generation_q_in_mwh() -> None:
    s3 = run_section3_direct_mixed()
    gen = GENERATION_SECTOR
    assert float(s3.q[gen]) == pytest.approx(s3.mwh_221110)
    q_usd = float(s3.scaled.q_target[gen])
    assert s3.c_col == pytest.approx(s3.mwh_221110 / q_usd)


def test_section1_all_monetary_usd() -> None:
    result = run_section1_production()
    gen = GENERATION_SECTOR
    assert float(result.scaled.q_target[gen]) > float(result.monetary.q[gen])
    assert float(result.ef.d[gen]) > 0.0


def test_atot_equals_adom_plus_aimp_all_sections() -> None:
    s1 = run_section1_production()
    s2 = run_section2_flow_mixed()
    s3 = run_section3_direct_mixed()
    for label, adom, aimp, atot in (
        ('s1', s1.scaled.adom.a_target, s1.scaled.aimp.a_target, s1.scaled.atot_target),
        ('s2', s2.adom, s2.aimp, s2.atot),
        ('s3', s3.adom, s3.aimp, s3.atot),
    ):
        pd.testing.assert_frame_equal(
            atot,
            adom + aimp,
            atol=1e-9,
            rtol=0.0,
            obj=label,
        )


def test_section2_domestic_row_mwh_anchor() -> None:
    s2 = run_section2_flow_mixed()
    s3 = run_section3_direct_mixed()
    assert float(s2.q[GENERATION_SECTOR]) == pytest.approx(s3.mwh_221110)
