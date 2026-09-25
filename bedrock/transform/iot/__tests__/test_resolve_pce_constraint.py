"""Hermetic tests for #1008 ``resolve_pce_constraint`` and dual-arm grade checks."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
import pytest

from bedrock.analysis.electricity.current.eia_gtd.pce_electricity_pin import (
    ARM_SKIP_CANDIDATE,
    CANDIDATES,
    PceCandidateSummaryRow,
    PcePinEiaBandRow,
    PcePinT11Row,
    _skip_summary_row,
    check_grade,
)
from bedrock.transform.iot.nowcast_sut_assembly import resolve_pce_constraint


def _cfg(*, flag: bool, mode: str) -> SimpleNamespace:
    return SimpleNamespace(
        constrain_electricity_pce_cell=flag,
        electricity_pce_constraint_mode=mode,
    )


def test_resolve_explicit_mode_wins_even_if_constrain_false() -> None:
    assert (
        resolve_pce_constraint(
            pce_constraint='tier1_fixed',
            constrain_electricity_pce_cell=False,
        )
        == 'tier1_fixed'
    )


def test_resolve_explicit_force_off() -> None:
    with patch(
        'bedrock.transform.iot.nowcast_sut_assembly.get_usa_config',
        return_value=_cfg(flag=True, mode='row_side_target'),
    ):
        assert (
            resolve_pce_constraint(
                pce_constraint=None,
                constrain_electricity_pce_cell=False,
            )
            == 'none'
        )


def test_resolve_production_flag_off() -> None:
    with patch(
        'bedrock.transform.iot.nowcast_sut_assembly.get_usa_config',
        return_value=_cfg(flag=False, mode='row_side_target'),
    ):
        assert resolve_pce_constraint(None, None) == 'none'


def test_resolve_ship_hermetic_flag_on_mode_winner() -> None:
    with patch(
        'bedrock.transform.iot.nowcast_sut_assembly.get_usa_config',
        return_value=_cfg(flag=True, mode='row_side_target'),
    ):
        assert resolve_pce_constraint(None, None) == 'row_side_target'


def test_resolve_flag_on_mode_none_raises() -> None:
    with patch(
        'bedrock.transform.iot.nowcast_sut_assembly.get_usa_config',
        return_value=_cfg(flag=True, mode='none'),
    ):
        with pytest.raises(ValueError, match="mode != 'none'"):
            resolve_pce_constraint(None, None)


def test_resolve_constrain_true_kwarg_uses_config_mode() -> None:
    with patch(
        'bedrock.transform.iot.nowcast_sut_assembly.get_usa_config',
        return_value=_cfg(flag=False, mode='eia_band'),
    ):
        assert (
            resolve_pce_constraint(
                pce_constraint=None,
                constrain_electricity_pce_cell=True,
            )
            == 'eia_band'
        )


def _ok_summary(
    candidate: str,
    *,
    rebase_eia: bool,
    selected: bool = False,
    eligible: bool = True,
    eia_ok: bool = True,
) -> PceCandidateSummaryRow:
    return PceCandidateSummaryRow(
        candidate=candidate,
        t11_all_years_ok=True,
        eia_all_spans_ok=eia_ok,
        displacement_note='test',
        eligible=eligible,
        selected=selected,
        rebase_eia=rebase_eia,
        baseline_vintage='f709829',
        arm_status='ok',
    )


def _seed_frame() -> pd.DataFrame:
    return pd.DataFrame({'F01000': [1.0]}, index=pd.Index(['221100']))


def test_check_grade_both_soft_skip_cardinality() -> None:
    years = [2022, 2023]
    summary = [
        _ok_summary('tier1_fixed', rebase_eia=False, selected=False, eligible=True),
        _ok_summary('row_side_target', rebase_eia=False, selected=True, eligible=True),
        _ok_summary('eia_band', rebase_eia=False, selected=False, eligible=True),
        _skip_summary_row('f709829'),
    ]
    t11 = [
        PcePinT11Row(
            candidate=c,
            year=y,
            t11_max_abs_residual=0.0,
            skipped='-',
            ok=True,
            rebase_eia=False,
            baseline_vintage='f709829',
        )
        for c in CANDIDATES
        for y in years
    ]
    eia = [
        PcePinEiaBandRow(
            candidate=c,
            year_a=2022,
            year_b=2023,
            shipped_yoy_usd=1.0,
            eia_residential_yoy_usd=1.0,
            band_ok=True,
            source_note='mut_from_balanced',
            artifact_vintage='test',
            rebase_eia=False,
            baseline_vintage='f709829',
        )
        for c in CANDIDATES
    ]
    with patch(
        'bedrock.analysis.electricity.current.eia_gtd.pce_electricity_pin.'
        'derive_initial_Y_pur',
        return_value=_seed_frame(),
    ):
        failures = check_grade(
            years=years,
            t11_rows=t11,
            eia_rows=eia,
            disp_rows=[],
            sink_rows=[],
            summary=summary,
            rebase_eia_mode='both',
            require_rebase_on=False,
        )
    assert failures == 0


def test_check_grade_rejects_selected_on_true_arm() -> None:
    years = [2022, 2023]
    summary = [
        _ok_summary('tier1_fixed', rebase_eia=False, selected=True),
        _ok_summary('row_side_target', rebase_eia=False),
        _ok_summary('eia_band', rebase_eia=False),
        _ok_summary('tier1_fixed', rebase_eia=True, selected=True, eligible=False),
        _ok_summary('row_side_target', rebase_eia=True, selected=False, eligible=False),
        _ok_summary('eia_band', rebase_eia=True, selected=False, eligible=False),
    ]
    with (
        patch(
            'bedrock.analysis.electricity.current.eia_gtd.pce_electricity_pin.'
            '_rebase_eia_supported',
            return_value=True,
        ),
        patch(
            'bedrock.analysis.electricity.current.eia_gtd.pce_electricity_pin.'
            'derive_initial_Y_pur',
            return_value=_seed_frame(),
        ),
    ):
        failures = check_grade(
            years=years,
            t11_rows=[],
            eia_rows=[],
            disp_rows=[],
            sink_rows=[],
            summary=summary,
            rebase_eia_mode='both',
        )
    assert failures >= 1


def test_skip_sentinel_fields() -> None:
    row = _skip_summary_row('f709829')
    assert row.candidate == ARM_SKIP_CANDIDATE
    assert row.selected is False
    assert row.eligible is False
    assert row.rebase_eia is True
    assert row.arm_status == 'skipped_flag_absent'
    assert row.t11_all_years_ok is False
    assert row.eia_all_spans_ok is False
