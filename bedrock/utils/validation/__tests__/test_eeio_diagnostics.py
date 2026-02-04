# ruff: noqa: PLC0415
"""Unit tests for the EEIO diagnostics module."""

import pandas as pd
import pytest

import bedrock.utils.math.formulas as formulas
from bedrock.transform.eeio.derived import derive_Aq_usa
from bedrock.transform.eeio.derived_2017 import (
    derive_2017_q_usa,
    derive_2017_U_with_negatives,
    derive_2017_Ytot_usa_matrix_set,
    derive_detail_y_imp_usa,
)
from bedrock.utils.validation.eeio_diagnostics import (
    DiagnosticResult,
    compare_commodity_output_to_domestics_use_plus_exports,
    compare_output_vs_leontief_x_demand,
    format_diagnostic_result,
    run_all_diagnostics,
)


class TestDiagnosticResult:
    """Tests for the DiagnosticResult dataclass."""

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
        assert result.max_rel_diff > result.tolerance

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
        assert "Tolerance: 0.0100" in formatted
        assert "Max relative difference: 0.0050" in formatted
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
@pytest.mark.xfail(
    reason="This test is failing because of data manipulation for aligning with the CEDA schema. Need to resolve during method reconciliation."
)
def test_compare_Uset_y_dom_and_q_usa() -> None:
    U_set = derive_2017_U_with_negatives()
    y_set = derive_2017_Ytot_usa_matrix_set()
    y_imp = derive_detail_y_imp_usa()
    q = derive_2017_q_usa()

    U_d = U_set.Udom
    y_d = y_set.ytot - y_imp + y_set.exports

    r_q_with_U_d_and_y_d_validation = (
        compare_commodity_output_to_domestics_use_plus_exports(
            q=q, U_d=U_d, y_d=y_d, tolerance=0.01, include_details=True
        )
    )

    assert len(r_q_with_U_d_and_y_d_validation.failing_sectors) == 0


@pytest.mark.eeio_integration
def test_compare_output_and_L_y(
    modelType: str = "Commodity",
    use_domestic: bool = False,
) -> None:

    # Load Aq model objects
    Aq = derive_Aq_usa()
    A_d = Aq.Adom
    A_imp = Aq.Aimp

    # Load y vectors
    y_set = derive_2017_Ytot_usa_matrix_set()
    y_imp = derive_detail_y_imp_usa()

    # Load output value
    if modelType == "Commodity":
        output = derive_2017_q_usa()
    else:
        # TODO: For industry models need to add g = output via derive_2017_g_usa(). Using q = output for now.
        output = derive_2017_q_usa()

    # Compute appropriate L and y
    if use_domestic:
        y = y_set.ytot - y_imp + y_set.exports  # y_d
        L = formulas.compute_L_matrix(A=A_d)  # L_d
    else:
        y = y_set.ytot  # total y (non-domestic)
        L = formulas.compute_L_matrix(
            A=(A_d + A_imp)
        )  # Is this correct? total L (non domestic)

    r_output_L_y_validation = compare_output_vs_leontief_x_demand(
        output=output, L=L, y=y, tolerance=0.01, include_details=True
    )
    assert len(r_output_L_y_validation.failing_sectors) == 0
