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


def compareCommodityOutputandDomesticUseplusProductionDemand(
    q: pd.Series[float],
    U_d: pd.DataFrame,
    y_d: pd.Series[float],
    *,
    tolerance: float = 0.01,
    include_details: bool = False,
) -> DiagnosticResult:
    """
    Compares the total commodity output against the summation of model domestic Use and production demand


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
            name="Unequal number of sectors in arguments of compareCommodityOutputandDomesticUseplusProductionDemand",
            passed=False,
            tolerance=tolerance,
            max_rel_diff=float("inf"),
            failing_sectors=[],
            details=None,
        )

    q_check = U_d.sum(axis=1) + y_d
    rel_diff = (q - q_check) / q
    rel_diff = rel_diff.fillna(0.0)  # convert NaN (e.g., div by 0) to 0s
    rel_diff_abs = np.abs(rel_diff)

    failing_sectors = rel_diff_abs[rel_diff_abs > tolerance]
    passing_sectors = rel_diff_abs[rel_diff_abs <= tolerance]
    max_rd = float(np.max(rel_diff_abs))

    # failing_sectors = sectors[rel_diff_abs > tolerance].tolist()
    name = "CommodityOutputandDomesticUseplusProductionDemand"

    details = None
    if include_details:
        data = {
            "failing sectors": failing_sectors.index.tolist(),
            "passing sectors": passing_sectors.index.tolist(),
            "failing values": failing_sectors.values,
            "max_rel_diff": max_rd,
        }

        details = pd.DataFrame({key: pd.Series(value) for key, value in data.items()})

    return DiagnosticResult(
        name=name,
        passed=passing_sectors,
        tolerance=tolerance,
        max_rel_diff=max_rd,
        failing_sectors=failing_sectors,
        details=details,
    )


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
