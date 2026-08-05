# ruff: noqa: PLC0415
"""Unit tests for the EEIO diagnostics module."""

from __future__ import annotations

import dataclasses as dc

import numpy as np
import pandas as pd
import pytest

import bedrock.utils.math.formulas as formulas
from bedrock.transform.eeio.derived_2017 import (
    derive_2017_Aq_usa,
    derive_2017_q_usa,
    derive_2017_U_with_negatives,
    derive_2017_x_usa,
    derive_2017_Ytot_usa_matrix_set,
    derive_detail_y_imp_usa,
)
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_q,
    derive_cornerstone_U_set,
    derive_cornerstone_U_with_negatives,
    derive_cornerstone_x,
    derive_cornerstone_Y_and_trade_scaled,
    derive_cornerstone_y_nab,
    derive_cornerstone_Ytot_matrix_set,
)
from bedrock.utils.config.usa_config import (
    USAConfig,
    reset_usa_config,
    set_global_usa_config,
)
from bedrock.utils.math.formulas import compute_y_imp
from bedrock.utils.validation.eeio_diagnostics import (
    DiagnosticResult,
    assert_eeio_year_alignment_precondition,
    compare_commodity_output_to_domestics_use_plus_exports,
    compare_E_and_LCI_result,
    compare_output_vs_leontief_x_demand,
    eeio_year_alignment_precondition_ok,
    format_diagnostic_result,
    run_all_diagnostics,
    validate_result,
)


class TestDiagnosticResult:
    """Tests for the DiagnosticResult dataclass (field storage and validation).

    ``max_rel_diff`` values in these fixtures are arbitrary; they are not
    required to satisfy ``validate_result``'s pass rule (normalized residual
    <= 1.0). See ``TestValidateResult`` for that semantics.
    """

    def test_basic_passing_result(self) -> None:
        """Test basic instantiation with a passing result."""
        result = DiagnosticResult(
            name="Row sum check",
            passed=True,
            tolerance=0.01,
            max_rel_diff=0.005,
            failing_sectors=[],
        )

        assert result.name == "Row sum check"
        assert result.passed is True
        assert result.tolerance == 0.01
        assert result.max_rel_diff == 0.005
        assert result.failing_sectors == []
        assert result.details is None  # Optional field defaults to None

    def test_failed_result_with_failing_sectors(self) -> None:
        """Test a failed result with sectors that failed the check."""
        result = DiagnosticResult(
            name="Column sum check",
            passed=False,
            tolerance=0.01,
            max_rel_diff=0.05,
            failing_sectors=["11", "21", "31"],
        )

        assert result.passed is False
        assert len(result.failing_sectors) == 3
        assert "11" in result.failing_sectors
        assert "21" in result.failing_sectors
        assert "31" in result.failing_sectors
        assert result.max_rel_diff == 0.05

    def test_result_with_details_dataframe(self) -> None:
        """Test a result with a details DataFrame."""
        details_df = pd.DataFrame(
            {
                "sector": ["11", "21"],
                "expected": [100.0, 200.0],
                "actual": [105.0, 195.0],
                "rel_diff": [0.05, 0.025],
            }
        )

        result = DiagnosticResult(
            name="Detailed check",
            passed=False,
            tolerance=0.01,
            max_rel_diff=0.05,
            failing_sectors=["11"],
            details=details_df,
        )

        assert result.details is not None
        assert isinstance(result.details, pd.DataFrame)
        assert len(result.details) == 2
        assert "sector" in result.details.columns
        assert "expected" in result.details.columns
        assert "actual" in result.details.columns
        assert "rel_diff" in result.details.columns

    def test_negative_tolerance_raises_error(self) -> None:
        """Test that negative tolerance raises ValueError."""
        with pytest.raises(ValueError, match="Tolerance must be non-negative"):
            DiagnosticResult(
                name="Invalid check",
                passed=True,
                tolerance=-0.01,
                max_rel_diff=0.005,
                failing_sectors=[],
            )

    def test_negative_max_rel_diff_raises_error(self) -> None:
        """Test that negative max_rel_diff raises ValueError."""
        with pytest.raises(ValueError, match="max_rel_diff must be non-negative"):
            DiagnosticResult(
                name="Invalid check",
                passed=True,
                tolerance=0.01,
                max_rel_diff=-0.005,
                failing_sectors=[],
            )

    def test_zero_tolerance_is_valid(self) -> None:
        """Test that zero tolerance is accepted (edge case)."""
        result = DiagnosticResult(
            name="Exact match check",
            passed=True,
            tolerance=0.0,
            max_rel_diff=0.0,
            failing_sectors=[],
        )

        assert result.tolerance == 0.0
        assert result.max_rel_diff == 0.0


class TestValidateResult:
    """Tests for ``validate_result`` pass/fail semantics (normalized residual)."""

    def test_zero_value_tiny_residual_passes(self) -> None:
        """Sectors with q=0 compare absolute residual against atol, not rel_diff."""
        value = pd.Series({"S00402": 0.0, "1111A0": 100.0})
        value_check = pd.Series({"S00402": 7.6e-6, "1111A0": 100.5})

        result = validate_result("zero q", value, value_check, tolerance=0.01)

        assert result.passed is True
        assert result.failing_sectors == []
        assert result.max_rel_diff <= 1.0

    def test_zero_value_large_residual_fails(self) -> None:
        value = pd.Series({"S00402": 0.0})
        value_check = pd.Series({"S00402": 1.0})

        result = validate_result("zero q", value, value_check, tolerance=0.01)

        assert result.passed is False
        assert result.failing_sectors == ["S00402"]

    def test_nonzero_value_uses_relative_tolerance(self) -> None:
        value = pd.Series({"1111A0": 100.0})
        value_check = pd.Series({"1111A0": 102.0})

        result = validate_result("rel", value, value_check, tolerance=0.01)

        assert result.passed is False
        assert result.failing_sectors == ["1111A0"]


class TestFormatDiagnosticResult:
    """Tests for the format_diagnostic_result function."""

    def test_format_passing_result(self) -> None:
        """Test formatting a passing diagnostic result."""
        result = DiagnosticResult(
            name="Row sum check",
            passed=True,
            tolerance=0.01,
            max_rel_diff=0.005,
            failing_sectors=[],
        )

        formatted = format_diagnostic_result(result)

        assert "Diagnostic: Row sum check" in formatted
        assert "Status: PASSED" in formatted
        assert "Tolerance (rtol): 0.0100" in formatted
        assert "Max normalized residual: 0.0050 (pass if <= 1.0)" in formatted
        assert "Failing sectors: None" in formatted

    def test_format_failed_result_with_sectors(self) -> None:
        """Test formatting a failed result with failing sectors."""
        result = DiagnosticResult(
            name="Column sum check",
            passed=False,
            tolerance=0.01,
            max_rel_diff=0.05,
            failing_sectors=["11", "21"],
        )

        formatted = format_diagnostic_result(result)

        assert "Diagnostic: Column sum check" in formatted
        assert "Status: FAILED" in formatted
        assert "Failing sectors (2): 11, 21" in formatted

    def test_format_result_with_many_failing_sectors(self) -> None:
        """Test that formatting truncates when many sectors fail."""
        many_sectors = [str(i) for i in range(15)]
        result = DiagnosticResult(
            name="Many failures",
            passed=False,
            tolerance=0.01,
            max_rel_diff=0.05,
            failing_sectors=many_sectors,
        )

        formatted = format_diagnostic_result(result)

        assert "Failing sectors (15):" in formatted
        assert "+5 more" in formatted


class TestRunAllDiagnostics:
    """Tests for the run_all_diagnostics function."""

    def test_run_single_passing_diagnostic(self) -> None:
        """Test running a single passing diagnostic."""

        def passing_check() -> DiagnosticResult:
            return DiagnosticResult(
                name="Passing check",
                passed=True,
                tolerance=0.01,
                max_rel_diff=0.005,
                failing_sectors=[],
            )

        results = run_all_diagnostics([passing_check], log_results=False)

        assert len(results) == 1
        assert results[0].passed is True

    def test_run_multiple_diagnostics(self) -> None:
        """Test running multiple diagnostics."""

        def check_a() -> DiagnosticResult:
            return DiagnosticResult(
                name="Check A",
                passed=True,
                tolerance=0.01,
                max_rel_diff=0.005,
                failing_sectors=[],
            )

        def check_b() -> DiagnosticResult:
            return DiagnosticResult(
                name="Check B",
                passed=False,
                tolerance=0.01,
                max_rel_diff=0.05,
                failing_sectors=["11"],
            )

        results = run_all_diagnostics([check_a, check_b], log_results=False)

        assert len(results) == 2
        assert results[0].passed is True
        assert results[1].passed is False

    def test_stop_on_failure(self) -> None:
        """Test that stop_on_failure raises RuntimeError on first failure."""

        def failing_check() -> DiagnosticResult:
            return DiagnosticResult(
                name="Failing check",
                passed=False,
                tolerance=0.01,
                max_rel_diff=0.05,
                failing_sectors=["11"],
            )

        with pytest.raises(RuntimeError, match="Diagnostic 'Failing check' failed"):
            run_all_diagnostics(
                [failing_check],
                log_results=False,
                stop_on_failure=True,
            )

    def test_continues_after_failure_by_default(self) -> None:
        """Test that diagnostics continue after failure when stop_on_failure is False."""
        call_order: list[str] = []

        def check_a() -> DiagnosticResult:
            call_order.append("a")
            return DiagnosticResult(
                name="Check A",
                passed=False,
                tolerance=0.01,
                max_rel_diff=0.05,
                failing_sectors=["11"],
            )

        def check_b() -> DiagnosticResult:
            call_order.append("b")
            return DiagnosticResult(
                name="Check B",
                passed=True,
                tolerance=0.01,
                max_rel_diff=0.005,
                failing_sectors=[],
            )

        results = run_all_diagnostics(
            [check_a, check_b],
            log_results=False,
            stop_on_failure=False,
        )

        assert len(results) == 2
        assert call_order == ["a", "b"]


@pytest.mark.eeio_integration
@pytest.mark.parametrize(
    "pipeline",
    [
        pytest.param(
            "ceda",
            marks=pytest.mark.xfail(
                reason="CEDA: q≠U_dom+y_d for 13 sectors after schema-alignment changes to 2017 detail trade/U.",
            ),
        ),
        pytest.param(
            "cornerstone",
            marks=pytest.mark.xfail(
                reason="Cornerstone: q≠U_dom+y_d for 13 sectors; BEA→CS remap and waste disagg break NAB identity.",
            ),
        ),
    ],
)
def test_compare_Uset_y_dom_and_q_usa(
    pipeline: str,
) -> None:

    if pipeline != "cornerstone":
        U_set = derive_2017_U_with_negatives()
        y_set = derive_2017_Ytot_usa_matrix_set()
        # CEDA has derive_detail_y_imp_usa(); it uses derive_2017_U_set_usa().Uimp
        # (negatives handled), not U_with_negatives().Uimp.
        y_imp = derive_detail_y_imp_usa()
        q = derive_2017_q_usa()

        U_d = U_set.Udom
        y_d = y_set.ytot - y_imp + y_set.exports
    else:
        # Cornerstone checks q (from V / Make) against U_dom row sums plus domestic
        # final demand y_d = y_tot − y_imp + exports, all in 2017-detail nominal
        # units mapped to CS commodities. Thirteen sectors fail at 1% rtol,
        # concentrated in mining/petroleum and waste codes (562*, S00402): the
        # BEA→Cornerstone correspondence and waste disaggregation split parent GO
        # across children without preserving the national-accounts identity
        # sector-by-sector. Near-zero q on special codes (e.g. S00402) is a
        # separate issue from the L·y atol fix in validate_result.
        U_set = derive_cornerstone_U_with_negatives()
        y_set = derive_cornerstone_Ytot_matrix_set()
        # No derive_cornerstone_y_imp wrapper; inline compute_y_imp as in
        # derive_cornerstone_y_nab(). Uimp from derive_cornerstone_U_set() (negatives
        # handled), not U_with_negatives().Uimp.
        y_imp = compute_y_imp(
            imports=y_set.imports,
            Uimp=derive_cornerstone_U_set().Uimp,
        )
        # q from V (Make), same role as derive_2017_q_usa() on the CEDA branch.
        q = derive_cornerstone_q()
        U_d = U_set.Udom
        y_d = y_set.ytot - y_imp + y_set.exports

    r_q_with_U_d_and_y_d_validation = (
        compare_commodity_output_to_domestics_use_plus_exports(
            q=q, U_d=U_d, y_d=y_d, tolerance=0.01, include_details=True
        )
    )

    assert len(r_q_with_U_d_and_y_d_validation.failing_sectors) == 0


@pytest.mark.eeio_integration
@pytest.mark.parametrize(
    "modelType, use_domestic, pipeline",
    [
        ("Commodity", True, "cornerstone"),
        pytest.param(
            "Commodity",
            False,
            "cornerstone",
            marks=pytest.mark.xfail(
                reason="Cornerstone total L·y still uses ytot/trade, not y_nab.",
            ),
        ),
        pytest.param(
            "Commodity",
            False,
            "ceda",
            marks=pytest.mark.xfail(
                reason="CEDA: scaled q≠L_total·y_total for ~298 commodity sectors (total Leontief identity).",
            ),
        ),
    ],
)  # TODO: add industry parameters when Industry models become available
def test_compare_output_and_L_y(
    modelType: str,
    use_domestic: bool,
    pipeline: str,
) -> None:

    if pipeline != "cornerstone":
        # CEDA: unscaled 2017-detail A and q; y built from 2017 Ytot/trade in IO year.
        Aq = derive_2017_Aq_usa()
        y_set = derive_2017_Ytot_usa_matrix_set()
        y_imp = derive_detail_y_imp_usa()
        output = (
            derive_2017_q_usa() if modelType == "Commodity" else derive_2017_x_usa()
        )
        if use_domestic:
            y = y_set.ytot - y_imp + y_set.exports
            L = formulas.compute_L_matrix(A=Aq.Adom)
        else:
            y = y_set.ytot + y_set.exports - y_set.imports
            L = formulas.compute_L_matrix(A=Aq.Adom + Aq.Aimp)
    else:
        # Cornerstone scales A and q to model year; CEDA branch stays in 2017 detail.
        Aq = derive_cornerstone_Aq_scaled()
        # Output must match Aq scaling (scaled_q), not derive_cornerstone_q() from V.
        output = Aq.scaled_q if modelType == "Commodity" else derive_cornerstone_x()
        if use_domestic:
            # y_nab from backcompute_y_from_A_and_q(Adom, scaled_q); unclipped.
            y = derive_cornerstone_y_nab()
            L = formulas.compute_L_matrix(A=Aq.Adom)
        else:
            # Total L·y still uses y from derive_cornerstone_Y_and_trade_scaled
            # (summary-disaggregated BEA Y/trade), not IO-balanced y_nab.
            y_trade = derive_cornerstone_Y_and_trade_scaled()
            y = y_trade.ytot + y_trade.exports - y_trade.imports
            L = formulas.compute_L_matrix(A=Aq.Adom + Aq.Aimp)

    r_output_L_y_validation = compare_output_vs_leontief_x_demand(
        output=output, L=L, y=y, tolerance=0.01, include_details=True
    )
    assert len(r_output_L_y_validation.failing_sectors) == 0


# ---------------------------------------------------------------------------
# useeior-parity LCI ≈ E + year-alignment precondition
# ---------------------------------------------------------------------------


@dc.dataclass(frozen=True)
class _ToyLciMatrices:
    B: pd.DataFrame
    L: pd.DataFrame
    y: pd.Series[float]
    E_ind: pd.DataFrame
    V: pd.DataFrame
    x: pd.Series[float]
    q: pd.Series[float]


def _toy_lci_matrices() -> _ToyLciMatrices:
    """Minimal commodity LCI≈E system with C_m = I and L = I."""
    industries = ['i1', 'i2']
    commodities = ['c1', 'c2']
    flows = ['CO2']

    V = pd.DataFrame(
        [[10.0, 0.0], [0.0, 20.0]],
        index=industries,
        columns=commodities,
    )
    x = V.sum(axis=1)
    q = pd.Series([10.0, 20.0], index=commodities)
    E_ind = pd.DataFrame([[100.0, 200.0]], index=flows, columns=industries)
    # With C_m = I, E_c = E_ind; B = E_c / q
    B = pd.DataFrame([[10.0, 10.0]], index=flows, columns=commodities)
    L = pd.DataFrame(
        np.eye(2),
        index=commodities,
        columns=commodities,
    )
    y = q.copy()
    return _ToyLciMatrices(B=B, L=L, y=y, E_ind=E_ind, V=V, x=x, q=q)


def test_eeio_year_alignment_precondition_ok() -> None:
    cfg = USAConfig.model_validate(
        {
            'model_base_year': 2024,
            'usa_ghg_data_year': 2024,
            'apply_io_year_adjustments': True,
        },
        strict=True,
    )
    assert cfg.use_ghg_year_x_in_B
    assert eeio_year_alignment_precondition_ok(cfg)
    assert_eeio_year_alignment_precondition(cfg)


def test_eeio_year_alignment_precondition_fails_loud() -> None:
    cfg = USAConfig.model_validate(
        {
            'model_base_year': 2024,
            'usa_ghg_data_year': 2023,
            'apply_io_year_adjustments': True,
        },
        strict=True,
    )
    assert not eeio_year_alignment_precondition_ok(cfg)
    with pytest.raises(ValueError, match='precondition failed'):
        assert_eeio_year_alignment_precondition(cfg)


def test_toy_lci_passes_when_LCI_equals_E_c() -> None:
    m = _toy_lci_matrices()
    result = compare_E_and_LCI_result(
        B=m.B,
        L=m.L,
        y=m.y,
        E_ind=m.E_ind,
        V=m.V,
        x=m.x,
        tolerance=0.01,
        check_precondition=False,
    )
    assert result.passed
    assert result.failing_sectors == []


def test_toy_lci_fails_when_E_perturbed() -> None:
    m = _toy_lci_matrices()
    E_ind = m.E_ind.copy()
    E_ind.iloc[0, 0] = 1000.0
    result = compare_E_and_LCI_result(
        B=m.B,
        L=m.L,
        y=m.y,
        E_ind=E_ind,
        V=m.V,
        x=m.x,
        tolerance=0.01,
        check_precondition=False,
    )
    assert not result.passed
    assert len(result.failing_sectors) > 0


@pytest.mark.eeio_integration
def test_v0_3_domestic_lci_equals_e() -> None:
    """Live domestic LCI≈E under aligned v0.3 (Vnorm path, χ=1 precondition)."""
    reset_usa_config(should_reset_env_var=True)
    set_global_usa_config('2025_usa_cornerstone_v0_3.yaml')
    try:
        from bedrock.transform.allocation.derived import derive_E_usa
        from bedrock.transform.eeio.derived_cornerstone import (
            derive_cornerstone_Aq_scaled,
            derive_cornerstone_B_non_finetuned,
            derive_cornerstone_Vnorm_scrap_corrected,
            derive_cornerstone_x_after_redefinition,
            derive_cornerstone_y_nab,
        )
        from bedrock.utils.math.formulas import compute_L_matrix

        assert_eeio_year_alignment_precondition()

        Aq = derive_cornerstone_Aq_scaled()
        L_d = compute_L_matrix(A=Aq.Adom)
        y_d = derive_cornerstone_y_nab()
        result = compare_E_and_LCI_result(
            B=derive_cornerstone_B_non_finetuned(),
            L=L_d,
            y=y_d,
            E_ind=derive_E_usa(),
            x=derive_cornerstone_x_after_redefinition(),
            Vnorm=derive_cornerstone_Vnorm_scrap_corrected(),
            q=Aq.scaled_q,
            tolerance=0.01,
            include_details=True,
            check_precondition=False,
        )
        assert result.passed, (
            f'domestic LCI≈E failed ({len(result.failing_sectors)} cells): '
            f'{result.failing_sectors[:20]}'
        )
    finally:
        reset_usa_config(should_reset_env_var=True)


@pytest.mark.eeio_integration
@pytest.mark.xfail(
    reason=(
        'Cornerstone total LCI≈E uses y_trade (ytot/trade), not IO-balanced '
        'throughput; same imbalance as total Ly≈q (~1749 cells fail).'
    ),
)
def test_v0_3_total_lci_equals_e() -> None:
    """Live total LCI≈E under aligned v0.3 (Vnorm path; L_tot / y_trade pairing)."""
    reset_usa_config(should_reset_env_var=True)
    set_global_usa_config('2025_usa_cornerstone_v0_3.yaml')
    try:
        from bedrock.transform.allocation.derived import derive_E_usa
        from bedrock.transform.eeio.derived_cornerstone import (
            derive_cornerstone_Aq_scaled,
            derive_cornerstone_B_non_finetuned,
            derive_cornerstone_Vnorm_scrap_corrected,
            derive_cornerstone_x_after_redefinition,
            derive_cornerstone_Y_and_trade_scaled,
        )
        from bedrock.utils.math.formulas import compute_L_matrix

        assert_eeio_year_alignment_precondition()

        Aq = derive_cornerstone_Aq_scaled()
        L_tot = compute_L_matrix(A=Aq.Adom + Aq.Aimp)
        y_trade = derive_cornerstone_Y_and_trade_scaled()
        y_tot = y_trade.ytot + y_trade.exports - y_trade.imports
        result = compare_E_and_LCI_result(
            B=derive_cornerstone_B_non_finetuned(),
            L=L_tot,
            y=y_tot,
            E_ind=derive_E_usa(),
            x=derive_cornerstone_x_after_redefinition(),
            Vnorm=derive_cornerstone_Vnorm_scrap_corrected(),
            q=Aq.scaled_q,
            tolerance=0.01,
            include_details=True,
            check_precondition=False,
        )
        assert result.passed, (
            f'total LCI≈E failed ({len(result.failing_sectors)} cells): '
            f'{result.failing_sectors[:20]}'
        )
    finally:
        reset_usa_config(should_reset_env_var=True)
