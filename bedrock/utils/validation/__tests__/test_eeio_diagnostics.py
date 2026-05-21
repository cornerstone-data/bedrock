# ruff: noqa: PLC0415
"""Unit tests for the EEIO diagnostics module."""

from typing import Callable

import pandas as pd
import pytest

import bedrock.utils.config.common as common
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
    _derive_cornerstone_Ytot_with_trade,
    derive_cornerstone_Aq,
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_B_non_finetuned,
    derive_cornerstone_q,
    derive_cornerstone_U_set,
    derive_cornerstone_U_with_negatives,
    derive_cornerstone_V,
    derive_cornerstone_VA,
    derive_cornerstone_Vnorm_scrap_corrected,
    derive_cornerstone_x,
    derive_cornerstone_Y_and_trade_scaled,
    derive_cornerstone_y_nab,
    derive_cornerstone_Ytot_matrix_set,
    get_waste_disagg_weights,
)
from bedrock.utils.config.usa_config import (
    USAConfig,
    reset_usa_config,
    set_global_usa_config,
)
from bedrock.utils.economic.inflation_helpers_cornerstone import (
    _cornerstone_to_ceda_v7_parent,
    get_cornerstone_industry_price_ratio,
    get_vnorm_adjusted_commodity_price_ratio,
)
from bedrock.utils.math.formulas import compute_y_imp
from bedrock.utils.validation.eeio_diagnostics import (
    DiagnosticResult,
    assert_diagnostic_passed,
    compare_commodity_output_to_domestics_use_plus_exports,
    compare_output_vs_leontief_x_demand,
    format_diagnostic_result,
    run_all_diagnostics,
)

CORNERSTONE_FULL_MODEL_CONFIG = '2025_usa_cornerstone_full_model.yaml'

# keep in sync with test_waste_disagg_pipeline_integration._CACHED_FUNCTIONS + inflation helpers
_CORNERSTONE_CACHED_FUNCTIONS: list[Callable[..., object]] = [
    get_waste_disagg_weights,
    derive_cornerstone_V,
    derive_cornerstone_Vnorm_scrap_corrected,
    derive_cornerstone_x,
    derive_cornerstone_q,
    derive_cornerstone_U_with_negatives,
    derive_cornerstone_U_set,
    _derive_cornerstone_Ytot_with_trade,
    derive_cornerstone_Ytot_matrix_set,
    derive_cornerstone_VA,
    derive_cornerstone_Aq,
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_B_non_finetuned,
    derive_cornerstone_y_nab,
    get_cornerstone_industry_price_ratio,
    get_vnorm_adjusted_commodity_price_ratio,
    _cornerstone_to_ceda_v7_parent,
]


def _clear_cornerstone_caches() -> None:
    for fn in _CORNERSTONE_CACHED_FUNCTIONS:
        if hasattr(fn, 'cache_clear'):
            fn.cache_clear()


def _setup_cornerstone_config() -> USAConfig:
    from bedrock.utils.config.usa_config import get_usa_config

    _clear_cornerstone_caches()
    reset_usa_config(should_reset_env_var=True)
    set_global_usa_config(CORNERSTONE_FULL_MODEL_CONFIG)
    common.download_fba_on_api_error = True
    cfg = get_usa_config()
    assert cfg.use_cornerstone_2026_model_schema
    assert cfg.implement_waste_disaggregation
    assert cfg.load_E_from_flowsa
    assert cfg.new_ghg_method
    assert cfg.use_E_data_year_for_x_in_B
    assert cfg.model_base_year == 2023
    assert cfg.usa_io_data_year == 2022
    assert not cfg.scale_a_matrix_with_useeio_method
    return cfg


def _teardown_cornerstone_config() -> None:
    _clear_cornerstone_caches()
    reset_usa_config(should_reset_env_var=True)
    common.download_fba_on_api_error = False


@pytest.fixture
def cornerstone_full_model_config(request: pytest.FixtureRequest) -> USAConfig:
    cfg = _setup_cornerstone_config()
    request.addfinalizer(_teardown_cornerstone_config)
    return cfg


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


@pytest.mark.xfail(
    reason="Data manipulation for aligning with the CEDA schema. Need to resolve during method reconciliation."
)
@pytest.mark.eeio_integration
@pytest.mark.parametrize(
    "modelType, use_domestic",
    [
        ("Commodity", False),
    ],
)  # TODO: add ("Commodity", True) test and industry parameters when Industry models become available [("Industry", False), ("Industry", True)]
def test_compare_output_and_L_y(
    modelType: str,
    use_domestic: bool,
) -> None:

    # Load Aq model objects
    Aq = derive_2017_Aq_usa()
    Adom = Aq.Adom
    Aimp = Aq.Aimp

    # Load y vectors
    y_set = derive_2017_Ytot_usa_matrix_set()
    y_imp = derive_detail_y_imp_usa()

    # Load output value
    if modelType == "Commodity":
        output = derive_2017_q_usa()
    else:
        # TODO: For industry models need to add x = output via derive_2017_x_usa(). Using q = output for now.
        output = derive_2017_x_usa()

    # Compute appropriate L and y
    if use_domestic:
        y = y_set.ytot - y_imp + y_set.exports  # y_d
        L = formulas.compute_L_matrix(A=Adom)  # L_d
    else:
        y = y_set.ytot + y_set.exports - y_set.imports  # total y (non-domestic)
        L = formulas.compute_L_matrix(
            A=(Adom + Aimp)
        )  # Is this correct? total L (non domestic)

    r_output_L_y_validation = compare_output_vs_leontief_x_demand(
        output=output, L=L, y=y, tolerance=0.01, include_details=True
    )
    print(len(r_output_L_y_validation.failing_sectors))
    assert len(r_output_L_y_validation.failing_sectors) == 0


@pytest.mark.xfail(
    reason=(
        '13 sectors fail q = U_dom + y_dom (S00402 q=0 vs Use FD; 325414 ~15% '
        'import-heavy gap; 562* waste disagg overlaps Make/Use failures).'
    ),
)
@pytest.mark.eeio_integration
def test_cornerstone_compare_Uset_y_dom_and_q(
    cornerstone_full_model_config: USAConfig,
) -> None:
    """Cornerstone q = U_dom + y_dom under full model config."""
    # Domestic commodity output identity should hold sector-by-sector when q comes from
    # Make and U_dom + y_dom from Use/trade. Failures largely mirror commodity Make/Use
    # imbalance: S00402 (zero Make q, nonzero Use FD), waste 562* (~4–11%), plus 325414
    # (~15%) where domestic final demand + intermediate use exceeds Make-side q—likely
    # import content not fully netted in y_dom relative to scaled U_dom.
    del cornerstone_full_model_config
    y_set = derive_cornerstone_Ytot_matrix_set()
    y_imp = compute_y_imp(
        imports=y_set.imports,
        Uimp=derive_cornerstone_U_set().Uimp,
    )
    y_d = y_set.ytot - y_imp + y_set.exports
    U_d = derive_cornerstone_U_with_negatives().Udom
    q = derive_cornerstone_q()

    result = compare_commodity_output_to_domestics_use_plus_exports(
        q=q,
        U_d=U_d,
        y_d=y_d,
        tolerance=0.01,
        include_details=True,
    )

    assert_diagnostic_passed(result)


@pytest.mark.xfail(
    reason=(
        '~298 sectors fail scaled_q = L_dom @ y_nab at 1%; wiring is correct but '
        'A/q/y_nab scaling and S00402/waste misalignment break the Leontief identity.'
    ),
)
@pytest.mark.eeio_integration
def test_cornerstone_compare_output_and_L_y_domestic(
    cornerstone_full_model_config: USAConfig,
) -> None:
    """Cornerstone scaled_q = L_dom @ y_nab (useeior domestic / NAB check)."""
    # Test uses the matching L_dom + y_nab pair per backcompute_q_from_L_and_y, but
    # scaled_q from the pipeline still disagrees with L @ y in ~298/405 sectors
    # (median rel_diff ~2.8%). S00402 is inf (q=0). Many import-heavy
    # sectors (33641A, 325414, 333991) show 20–40% gaps, indicating
    # scaled Adom, scaled_q, and disaggregated y_nab are not on a single consistent
    # national-accounts scale—not a test wiring bug.
    del cornerstone_full_model_config
    aq = derive_cornerstone_Aq_scaled()
    y = derive_cornerstone_y_nab()
    L = formulas.compute_L_matrix(A=aq.Adom)

    result = compare_output_vs_leontief_x_demand(
        output=aq.scaled_q,
        L=L,
        y=y,
        tolerance=0.01,
        include_details=True,
    )

    assert_diagnostic_passed(result)


@pytest.mark.xfail(
    reason=(
        '~323 sectors fail scaled_q = L_total @ y_complete; total y is negative for '
        'many codes and S00402/4200ID are pathological under Production_Complete y.'
    ),
)
@pytest.mark.eeio_integration
def test_cornerstone_compare_output_and_L_y_total(
    cornerstone_full_model_config: USAConfig,
) -> None:
    """Cornerstone scaled_q = L_total @ y_complete (useeior Production_Complete check).

    Production_Complete analog: ytot + exports - imports from scaled Y trade set,
    matching the CEDA eeio_integration test and useeior ``prepareProductionDemand``
    structure (consumption + exports + signed imports).
    """
    # useeior Production_Complete check: L_total @ (ytot + exports - imports). Worse
    # than the domestic NAB check (~323 vs ~298 failures). y_complete is negative for
    # many sectors after subtracting imports; 4200ID shows rel_diff=1.0. S00402 remains
    # inf. Suggests scaled trade/Y disaggregation and total A matrix are not yet aligned
    # with scaled_q for the full open-economy Leontief identity.
    del cornerstone_full_model_config
    aq = derive_cornerstone_Aq_scaled()
    y_set = derive_cornerstone_Y_and_trade_scaled()
    y = y_set.ytot + y_set.exports - y_set.imports
    L = formulas.compute_L_matrix(A=aq.Adom + aq.Aimp)

    result = compare_output_vs_leontief_x_demand(
        output=aq.scaled_q,
        L=L,
        y=y,
        tolerance=0.01,
        include_details=True,
    )

    assert_diagnostic_passed(result)
