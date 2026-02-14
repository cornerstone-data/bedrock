"""
Diagnostics module for EEIO validation checks.

Provides utilities for runtime validation of EEIO matrices and data structures,
including standardized result reporting and batch diagnostic execution.
"""

from __future__ import annotations

import dataclasses as dc
import logging
import typing as ta

import numpy as np
import pandas as pd

from bedrock.transform.eeio.derived_2017 import (
    derive_2017_Ytot_usa_matrix_set,
    derive_detail_VA_usa,
)
from bedrock.utils.economic.inflation import (
    obtain_inflation_factors_from_reference_data,
)
from bedrock.utils.math.formulas import (
    backcompute_q_from_L_and_y,
    compute_commodity_mix_matrix,
    compute_Vnorm_matrix,
)

logger = logging.getLogger(__name__)


@dc.dataclass
class DiagnosticResult:
    """
    Standardized result container for diagnostic checks.

    Attributes:
        name: Descriptive name of the diagnostic check.
        passed: Whether the diagnostic check passed.
        tolerance: The tolerance threshold used for the check.
        max_rel_diff: Maximum relative difference observed.
        failing_sectors: List of sector identifiers that failed the check.
        details: Optional DataFrame with detailed diagnostic information.
    """

    name: str
    passed: bool
    tolerance: float
    max_rel_diff: float
    failing_sectors: ta.List[str]
    details: ta.Optional[pd.DataFrame] = None

    def __post_init__(self) -> None:
        """Validate the diagnostic result after initialization."""
        if self.tolerance < 0:
            raise ValueError("Tolerance must be non-negative")
        if self.max_rel_diff < 0:
            raise ValueError("max_rel_diff must be non-negative")


def format_diagnostic_result(result: DiagnosticResult) -> str:
    """
    Format a DiagnosticResult for logging output.

    Creates a human-readable string representation of the diagnostic result,
    suitable for logging output.

    Args:
        result: The DiagnosticResult to format.

    Returns:
        Formatted string representation of the diagnostic result.

    Example:
        >>> result = DiagnosticResult(
        ...     name="Row sum check",
        ...     passed=False,
        ...     tolerance=0.01,
        ...     max_rel_diff=0.05,
        ...     failing_sectors=["11", "21"]
        ... )
        >>> print(format_diagnostic_result(result))
        Diagnostic: Row sum check
        Status: FAILED
        Tolerance: 0.0100
        Max relative difference: 0.0500
        Failing sectors (2): 11, 21
    """
    status = "PASSED" if result.passed else "FAILED"

    lines = [
        f"Diagnostic: {result.name}",
        f"Status: {status}",
        f"Tolerance: {result.tolerance:.4f}",
        f"Max relative difference: {result.max_rel_diff:.4f}",
    ]

    if result.failing_sectors:
        sector_count = len(result.failing_sectors)
        # Limit display to first 10 sectors if many are failing
        if sector_count > 10:
            displayed_sectors = ", ".join(result.failing_sectors[:10])
            lines.append(
                f"Failing sectors ({sector_count}): {displayed_sectors}, ... "
                f"(+{sector_count - 10} more)"
            )
        else:
            displayed_sectors = ", ".join(result.failing_sectors)
            lines.append(f"Failing sectors ({sector_count}): {displayed_sectors}")
    else:
        lines.append("Failing sectors: None")

    return "\n".join(lines)


DiagnosticCallable = ta.Callable[[], DiagnosticResult]


def run_all_diagnostics(
    diagnostics: ta.List[DiagnosticCallable],
    *,
    log_results: bool = True,
    stop_on_failure: bool = False,
) -> ta.List[DiagnosticResult]:
    """
    Execute a list of diagnostic functions and collect results.

    Runs each diagnostic callable, optionally logging results and handling
    failures according to the specified behavior.

    Args:
        diagnostics: List of callable functions that each return a DiagnosticResult.
        log_results: If True, log each result using logger. Defaults to True.
        stop_on_failure: If True, stop execution on first failure. Defaults to False.

    Returns:
        List of DiagnosticResult objects from all executed diagnostics.

    Raises:
        RuntimeError: If stop_on_failure is True and a diagnostic fails.

    Example:
        >>> def check_row_sums() -> DiagnosticResult:
        ...     # Perform check...
        ...     return DiagnosticResult(
        ...         name="Row sum check",
        ...         passed=True,
        ...         tolerance=0.01,
        ...         max_rel_diff=0.001,
        ...         failing_sectors=[]
        ...     )
        >>> results = run_all_diagnostics([check_row_sums])
    """
    results: ta.List[DiagnosticResult] = []

    for diagnostic in diagnostics:
        try:
            result = diagnostic()
            results.append(result)

            if log_results:
                formatted = format_diagnostic_result(result)
                if result.passed:
                    logger.info(formatted)
                else:
                    logger.warning(formatted)

            if stop_on_failure and not result.passed:
                raise RuntimeError(
                    f"Diagnostic '{result.name}' failed. "
                    f"Max relative difference: {result.max_rel_diff:.4f} "
                    f"(tolerance: {result.tolerance:.4f})"
                )

        except Exception as e:
            if isinstance(e, RuntimeError) and stop_on_failure:
                raise
            # Log unexpected errors but continue with other diagnostics
            logger.error(f"Error running diagnostic: {e}")
            # Create a failed result for the error case
            error_result = DiagnosticResult(
                name=f"Error in {diagnostic.__name__ if hasattr(diagnostic, '__name__') else 'unknown'}",
                passed=False,
                tolerance=0.0,
                max_rel_diff=float("inf"),
                failing_sectors=[],
                details=None,
            )
            results.append(error_result)

    # Log summary
    if log_results and results:
        passed_count = sum(1 for r in results if r.passed)
        total_count = len(results)
        summary = f"Diagnostics complete: {passed_count}/{total_count} passed"
        if passed_count == total_count:
            logger.info(summary)
        else:
            logger.warning(summary)

    return results


def validate_result(
    name: str,
    value: pd.Series[float],
    value_check: pd.Series[float],
    *,
    tolerance: float = 0.01,
    include_details: bool = False,
) -> DiagnosticResult:
    """
    Helper function to compare and format validation results
    Pass/fail: |(c - x) / x| <= tolerance for all sectors. Where x = 0, rel_diff
    is treated as 0.

    Parameters
    ----------
    name - string value identifying the diagnostic being run
    value - original value to check
        Float series from e.g. ``derive_2017_q_usa``
    value_check - computed value to compare against original
        Float series obtained from calcualtion
    tolerance
        Tolerance for |rel_diff|; default 0.05.
    include_details
        If True, attach a details DataFrame (sector, expected, actual, rel_diff).

    Returns
    -------
    DiagnosticResult
        Standardized result with pass/fail, max_rel_diff, failing_sectors, optional details.

    """
    rel_diff = (value - value_check) / value
    rel_diff = rel_diff.fillna(0.0)  # convert NaN (e.g., div by 0) to 0s
    rel_diff_abs = np.abs(rel_diff)

    failing_sectors = rel_diff_abs[rel_diff_abs > tolerance]
    passing_sectors = rel_diff_abs[rel_diff_abs <= tolerance]
    max_rd = float(np.max(rel_diff_abs))

    details = None
    if include_details:
        data = {
            "failing sectors": list(getattr(failing_sectors, "index", [])),
            "passing sectors": list(getattr(passing_sectors, "index", [])),
            "failing values": np.array(failing_sectors).tolist(),
            "max_rel_diff": max_rd,
        }

        details = pd.DataFrame({key: pd.Series(value) for key, value in data.items()})

    passed = len(value) == len(passing_sectors)
    return DiagnosticResult(
        name=name,
        passed=passed,
        tolerance=tolerance,
        max_rel_diff=max_rd,
        failing_sectors=list(getattr(failing_sectors, "index", [])),
        details=details,
    )


def compare_commodity_output_to_domestics_use_plus_exports(
    q: pd.Series[float],
    U_d: pd.DataFrame,
    y_d: pd.Series[float],
    *,
    tolerance: float = 0.01,
    include_details: bool = False,
) -> DiagnosticResult:
    """
    Compares the total commodity output against the summation of model domestic Use (U_D) and production demand (y_d, including exports)

    Pass/fail: |(c - x) / x| <= tolerance for all sectors. Where x = 0, rel_diff
    is treated as 0.

    Parameters
    ----------
    q
        Float series from e.g. ``derive_2017_q_usa``
    U_d
        Dataframe from e.g. ``derive_2017_U_set_usa().Udom
    y_d
        Float series from e.g. ``derive_ydom_and_yimp_usa().ydom``
    tolerance
        Tolerance for |rel_diff|; default 0.05.
    include_details
        If True, attach a details DataFrame (sector, expected, actual, rel_diff).

    Returns
    -------
    DiagnosticResult
        Standardized result with pass/fail, max_rel_diff, failing_sectors, optional details.
    """

    # Make sure all elements have common sectors
    sectors = q.index.intersection(U_d.index).intersection(y_d.index)
    if len(sectors) != len(q.index):
        return DiagnosticResult(
            name="Unequal number of sectors in arguments of compare_commodity_output_to_domestics_use_plus_exports",
            passed=False,
            tolerance=tolerance,
            max_rel_diff=float("inf"),
            failing_sectors=[],
            details=None,
        )

    q_check = U_d.sum(axis=1) + y_d
    name = "commodity output and domestics use plus exports"

    d_result = validate_result(
        name, q, q_check, tolerance=tolerance, include_details=include_details
    )

    return d_result


def compare_output_vs_leontief_x_demand(
    output: pd.Series[float],
    L: pd.DataFrame,
    y: pd.Series[float],
    *,
    tolerance: float = 0.01,
    include_details: bool = False,
) -> DiagnosticResult:
    """
    Compares the total sector output (commodity or industry) against
    the model result calculation of L @ y.
    Pass/fail: |(c - x) / x| <= tolerance for all sectors. Where x = 0, rel_diff
    is treated as 0.

    Parameters
    ----------
    output
        Float series. If commodity model, output = q from ``derive_2017_q_usa``; if industry model, output = g  from ``derive_2017_g_usa``
    L
        Dataframe. Leontief inverse (total or domestic)
    y
        Float series. National accounting balance final demand (y_nab)
    use_domestic
        If True, use the domestic Leontief inverse and final demand. Default is False.
    tolerance
        Tolerance for |rel_diff|; default 0.01.
    include_details
        If True, attach a details DataFrame (sector, expected, actual, rel_diff).

    Returns
    -------
    DiagnosticResult
        Standardized result with pass/fail, max_rel_diff, failing_sectors, optional details.
    """

    # Make sure all elements have common sectors: TODO: make this a new function as it is called in several validation functions
    sectors = output.index.intersection(L.index).intersection(y.index)
    if len(sectors) != len(output.index):
        return DiagnosticResult(
            name="Unequal number of sectors in arguments of compare_commodity_output_to_domestics_use_plus_exports",
            passed=False,
            tolerance=tolerance,
            max_rel_diff=float("inf"),
            failing_sectors=[],
            details=None,
        )

    # calculate scaling factor
    output_check = backcompute_q_from_L_and_y(L=L, y=y)
    name = "compare output and L * y"

    d_result = validate_result(
        name, output, output_check, tolerance=tolerance, include_details=include_details
    )

    return d_result


def commodity_industry_output_cpi_consistency(
    V: pd.DataFrame,
    q: pd.Series[float],
    x: pd.Series[float],
    base_year: int,
    target_year: int,
    tolerance: float,
    include_details: bool = False,
) -> DiagnosticResult:
    """Test that commodity output adjusted by CPI equals market share matrix times CPI-adjusted industry output."""

    # Commodity mix matrix C_m (commodity x industry) (Marketshares transposed)
    # This is equivalent to generateCommodityMixMatrix in useeior which also uses t(V) and x
    C_m = compute_commodity_mix_matrix(V=V, x=x)

    # Market share matrix M_s (industry x commodity)
    # This is equivalent to generateMarketSharesfromMake in useeior which also uses V and q
    M_s = compute_Vnorm_matrix(V=V, q=q)

    # CPI vectors from bedrock's inflation utilities
    # This is equivalent to Detail_CPI_IO_17sch.rda which in turn is the same as model$MultiYearIndustryCPI
    industry_CPI = obtain_inflation_factors_from_reference_data()

    # Create commodity CPI by multiplying an I x 1 matrix @ a I x C matrix which yields a C x 1 matrix
    # for each column of industry_CPI, which are the various years
    commodity_CPI = pd.DataFrame().reindex_like(industry_CPI)
    for i in range(len(industry_CPI.columns)):
        commodity_CPI.iloc[:, i] = industry_CPI.iloc[:, i] @ M_s

    # Calculate CPI ratios
    industry_CPI_ratio = industry_CPI[target_year] / industry_CPI[base_year]
    commodity_CPI_ratio = commodity_CPI[target_year] / commodity_CPI[base_year]

    # Calculate q_check and x_check
    q_check = q * commodity_CPI_ratio
    x_check = C_m @ (x * industry_CPI_ratio)

    name = "commodity_industry_output_cpi_consistency"

    d_result = validate_result(
        name, q_check, x_check, tolerance=tolerance, include_details=include_details
    )

    return d_result


def compare_output_from_make_and_use(
    output: str,
    V: pd.DataFrame,
    U: pd.DataFrame,
    tolerance: float,
    include_details: bool = False,
) -> DiagnosticResult:
    """Check that industry output from Use and Make tables are the same"""

    if output == "Industry":
        VA = derive_detail_VA_usa()
        x_make = V.sum(axis=1)
        x_use = U.sum(axis=0) + VA.sum(axis=0)

        name = "compare_industry_output_from_make_and_use"
        d_result = validate_result(
            name, x_make, x_use, tolerance=tolerance, include_details=include_details
        )
    elif output == "Commodity":
        y_set = derive_2017_Ytot_usa_matrix_set()
        q_make = V.sum(axis=0)
        q_use = U.sum(axis=1) + (y_set.ytot + y_set.exports - y_set.imports)

        name = "compare_commodity_output_from_make_and_use"
        d_result = validate_result(
            name, q_make, q_use, tolerance=tolerance, include_details=include_details
        )
    else:
        d_result = DiagnosticResult(
            name="invalid output parameter requested for comparison between make and use, select commodity or industry",
            passed=False,
            tolerance=tolerance,
            max_rel_diff=0.005,
            failing_sectors=[],
        )

    return d_result
