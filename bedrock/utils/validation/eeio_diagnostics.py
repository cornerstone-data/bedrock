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

from bedrock.utils.config.usa_config import USAConfig, get_usa_config
from bedrock.utils.math.formulas import (
    backcompute_q_from_L_and_y,
    compute_commodity_mix_matrix,
    compute_E_from_BLy,
)
from bedrock.utils.schemas.single_region_types import SingleRegionYtotAndTradeVectorSet

logger = logging.getLogger(__name__)

# Absolute floor for |value| in relative-error diagnostics. Sectors with q ≈ 0
# (e.g. S00402 used goods) compare |diff| against atol instead of (diff / q).
_VALIDATE_RESULT_ATOL = 1e-4


@dc.dataclass
class DiagnosticResult:
    """
    Standardized result container for diagnostic checks.

    Attributes:
        name: Descriptive name of the diagnostic check.
        passed: Whether the diagnostic check passed.
        tolerance: Relative tolerance (rtol) passed to ``validate_result``;
            scales the allowed residual as ``tolerance * |value| + atol``.
        max_rel_diff: Worst-case normalized residual from ``validate_result``:
            ``max(|diff| / (tolerance * |value| + atol))`` over sectors.
            Values <= 1.0 are within tolerance; > 1.0 fail. Not directly
            comparable to ``tolerance``. Structural errors may set this to inf.
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
        ...     max_rel_diff=1.5,
        ...     failing_sectors=["11", "21"]
        ... )
        >>> print(format_diagnostic_result(result))
        Diagnostic: Row sum check
        Status: FAILED
        Tolerance (rtol): 0.0100
        Max normalized residual: 1.5000 (pass if <= 1.0)
        Failing sectors (2): 11, 21
    """
    status = "PASSED" if result.passed else "FAILED"

    lines = [
        f"Diagnostic: {result.name}",
        f"Status: {status}",
        f"Tolerance (rtol): {result.tolerance:.4f}",
        f"Max normalized residual: {result.max_rel_diff:.4f} (pass if <= 1.0)",
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
                    f"Max normalized residual: {result.max_rel_diff:.4f} "
                    f"(pass if <= 1.0; rtol: {result.tolerance:.4f})"
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
    atol: float = _VALIDATE_RESULT_ATOL,
    include_details: bool = False,
) -> DiagnosticResult:
    """
    Helper function to compare and format validation results.

    Pass/fail uses ``|diff| <= tolerance * |value| + atol`` (same form as
    ``numpy.isclose``). Where ``|value|`` is near zero, ``atol`` bounds the
    absolute residual instead of dividing by zero.

    Parameters
    ----------
    name - string value identifying the diagnostic being run
    value - original value to check
        Float series from e.g. ``derive_2017_q_usa``
    value_check - computed value to compare against original
        Float series obtained from calcualtion
    tolerance
        Relative tolerance (rtol) in ``|diff| <= tolerance * |value| + atol``;
        default 0.01.
    atol
        Absolute tolerance added to the allowed residual; default 1e-4.
    include_details
        If True, attach a details DataFrame with per-sector normalized
        residuals in the ``failing values`` column.

    Returns
    -------
    DiagnosticResult
        ``passed`` is True when all normalized residuals are <= 1.0.
        ``max_rel_diff`` is the worst normalized residual (see
        ``DiagnosticResult``).

    """
    abs_diff = (value - value_check).abs()
    value_abs = value.abs()
    allowed = tolerance * value_abs + atol

    with np.errstate(divide="ignore", invalid="ignore"):
        normalized = abs_diff / allowed
    normalized = normalized.replace([np.inf, -np.inf], np.nan).fillna(0.0)

    failing_sectors = normalized.index[normalized > 1.0]
    passing_sectors = normalized.index[normalized <= 1.0]
    max_rd = float(normalized.max()) if len(normalized) else 0.0

    details = None
    if include_details:
        data = {
            "failing sectors": list(getattr(failing_sectors, "index", failing_sectors)),
            "passing sectors": list(getattr(passing_sectors, "index", passing_sectors)),
            "failing values": normalized.loc[failing_sectors].tolist(),
            "max_rel_diff": max_rd,
        }

        details = pd.DataFrame({key: pd.Series(value) for key, value in data.items()})

    passed = len(failing_sectors) == 0
    return DiagnosticResult(
        name=name,
        passed=passed,
        tolerance=tolerance,
        max_rel_diff=max_rd,
        failing_sectors=list(failing_sectors.astype(str)),
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

    Pass/fail: ``|diff| <= tolerance * |value| + atol`` for all sectors (see
    ``validate_result``).

    Parameters
    ----------
    q
        Float series from e.g. ``derive_2017_q_usa``
    U_d
        Dataframe from e.g. ``derive_2017_U_set_usa().Udom
    y_d
        Float series from e.g. ``derive_ydom_and_yimp_usa().ydom``
    tolerance
        Relative tolerance (rtol) forwarded to ``validate_result``; default 0.01.
    include_details
        If True, attach per-sector normalized residuals via ``validate_result``.

    Returns
    -------
    DiagnosticResult
        Pass/fail and ``max_rel_diff`` follow ``validate_result`` (normalized
        residual <= 1.0).
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
    Pass/fail: ``|diff| <= tolerance * |value| + atol`` for all sectors (see
    ``validate_result``).

    Parameters
    ----------
    output
        Float series. If commodity model, output = q from ``derive_2017_q_usa``; if industry model, output = x  from ``derive_2017_x_usa``
    L
        Dataframe. Leontief inverse (total or domestic)
    y
        Float series. National accounting balance final demand (y_nab)
    use_domestic
        If True, use the domestic Leontief inverse and final demand. Default is False.
    tolerance
        Relative tolerance (rtol) forwarded to ``validate_result``; default 0.01.
    include_details
        If True, attach per-sector normalized residuals via ``validate_result``.

    Returns
    -------
    DiagnosticResult
        Pass/fail and ``max_rel_diff`` follow ``validate_result`` (normalized
        residual <= 1.0).
    """

    # Make sure all elements have common sectors:
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
    industry_CPI_ratio: pd.Series[float],
    commodity_CPI_ratio: pd.Series[float],
    tolerance: float,
    include_details: bool = False,
) -> DiagnosticResult:
    """Test that CPI-adjusted commodity output matches the market-share mix of CPI-adjusted industry output.

    Pass/fail and ``max_rel_diff`` are computed by ``validate_result``.
    """

    # Commodity mix matrix C_m (commodity x industry) (Marketshares transposed)
    # This is equivalent to generateCommodityMixMatrix in useeior which also uses t(V) and x
    C_m = compute_commodity_mix_matrix(V=V, x=x)

    q_check = q * commodity_CPI_ratio
    x_check = C_m @ (x * industry_CPI_ratio)

    name = "commodity_industry_output_cpi_consistency"

    d_result = validate_result(
        name, q_check, x_check, tolerance=tolerance, include_details=include_details
    )

    return d_result


def compare_output_from_make_and_use(
    output: ta.Literal['Industry', 'Commodity'],
    V: pd.DataFrame,
    U: pd.DataFrame,
    VA: pd.DataFrame,
    y_set: SingleRegionYtotAndTradeVectorSet,
    tolerance: float,
    include_details: bool = False,
) -> DiagnosticResult:
    """Check that Make-table and Use-table output agree for industry or commodity.

    Pass/fail and ``max_rel_diff`` are computed by ``validate_result``.
    """

    if output == "Industry":
        x_make = V.sum(axis=1)
        x_use = U.sum(axis=0) + VA.sum(axis=0)

        name = "compare_industry_output_from_make_and_use"
        d_result = validate_result(
            name,
            x_make,
            x_use,
            tolerance=tolerance,
            include_details=include_details,
        )
    elif output == "Commodity":
        q_make = V.sum(axis=0)
        q_use = U.sum(axis=1) + (y_set.ytot + y_set.exports - y_set.imports)

        name = "compare_commodity_output_from_make_and_use"
        d_result = validate_result(
            name,
            q_make,
            q_use,
            tolerance=tolerance,
            include_details=include_details,
        )
    else:
        raise ValueError(
            'invalid output parameter requested for comparison between make and use, select commodity or industry'
        )
    return d_result


def assert_eeio_year_alignment_precondition(
    cfg: ta.Optional[USAConfig] = None,
) -> None:
    """Raise if the active (or given) config is not aligned for χ=1 LCI≈E checks.

    Replaces useeior ``generateChiMatrix``: when this passes, ``compare_E_and_LCI_result``
    may use χ=1. Failures are loud (``ValueError``), never silent skips.

    Requires matching model/GHG years, GHG-year ``x`` in B
    (``use_ghg_year_x_in_B``), and no deflated-B path that introduces an
    intermediate dollar year.

    Parameters
    ----------
    cfg
        Config to check; defaults to ``get_usa_config()``.
    """
    if cfg is None:
        cfg = get_usa_config()

    reasons: list[str] = []
    if cfg.model_base_year != cfg.usa_ghg_data_year:
        reasons.append(
            f'model_base_year ({cfg.model_base_year}) != '
            f'usa_ghg_data_year ({cfg.usa_ghg_data_year})'
        )
    if not cfg.use_ghg_year_x_in_B:
        reasons.append(
            'use_ghg_year_x_in_B is False '
            '(need apply_io_year_adjustments or use_E_data_year_for_x_in_B)'
        )
    if cfg.deflate_x_to_detail_io_year_for_B:
        reasons.append(
            'deflate_x_to_detail_io_year_for_B is True '
            '(intermediate dollar years break χ=1)'
        )
    if reasons:
        raise ValueError(
            'EEIO year-alignment precondition failed for χ=1 LCI≈E validation: '
            + '; '.join(reasons)
        )


def _flatten_matrix_for_validate(df: pd.DataFrame) -> pd.Series[float]:
    """Stack a flow×sector matrix to a Series with MultiIndex labels ``flow|sector``."""
    stacked = df.stack()
    stacked.index = stacked.index.map(
        lambda idx: f'{idx[0]}|{idx[1]}' if isinstance(idx, tuple) else str(idx)
    )
    return ta.cast('pd.Series[float]', stacked.astype(float))


def compare_E_and_LCI_result(
    *,
    B: pd.DataFrame,
    L: pd.DataFrame,
    y: pd.Series[float],
    E_ind: pd.DataFrame,
    V: ta.Optional[pd.DataFrame] = None,
    x: ta.Optional[pd.Series[float]] = None,
    Vnorm: ta.Optional[pd.DataFrame] = None,
    q: ta.Optional[pd.Series[float]] = None,
    tolerance: float = 0.01,
    include_details: bool = False,
    check_precondition: bool = True,
    cfg: ta.Optional[USAConfig] = None,
) -> DiagnosticResult:
    """Compare direct-perspective LCI to commodity-transformed satellite totals.

    Port of useeior ``compareEandLCIResult`` with χ=1 (no Chi matrix). Requires
    :func:`assert_eeio_year_alignment_precondition` unless
    ``check_precondition=False`` (unit tests with synthetic matrices).

    Commodity ``E_c`` (pick one path)::

        # A) Vnorm path (Cornerstone B = (E/x) @ Vnorm_scrap): preferred when
        #    scrap-corrected Vnorm is used — C_m(V, x_Make) will not match.
        E_c = (E_ind / x @ Vnorm) · diag(q)

        # B) useeior C_m path (no scrap adjustment on market shares):
        E_c = (C_m @ E_ind.T).T     # C_m from Make V, x

        c   = L @ y
        LCI = B · diag(c)           # B already commodity — do not @ V_n again
        compare LCI to E_c cell-wise

    Provide either (``Vnorm``, ``x``, ``q``) or (``V``, ``x``). Distinct from
    NAB national ``sum(diag(D) @ L @ y) ≈ sum(E)``.
    """
    if check_precondition:
        assert_eeio_year_alignment_precondition(cfg)

    use_vnorm_path = Vnorm is not None and q is not None and x is not None
    use_cm_path = V is not None and x is not None and not use_vnorm_path
    if not use_vnorm_path and not use_cm_path:
        raise ValueError(
            'compare_E_and_LCI_result requires either (Vnorm, x, q) or (V, x)'
        )

    flows = B.index.intersection(E_ind.index)
    if len(flows) == 0:
        return DiagnosticResult(
            name='compare_E_and_LCI_result: empty flow intersection',
            passed=False,
            tolerance=tolerance,
            max_rel_diff=float('inf'),
            failing_sectors=[],
            details=None,
        )

    if use_vnorm_path:
        assert Vnorm is not None and q is not None and x is not None
        industries = Vnorm.index.intersection(E_ind.columns).intersection(x.index)
        commodities = (
            B.columns.intersection(Vnorm.columns)
            .intersection(q.index)
            .intersection(L.index)
            .intersection(y.index)
        )
        if len(industries) == 0 or len(commodities) == 0:
            return DiagnosticResult(
                name='compare_E_and_LCI_result: empty industry/commodity intersection',
                passed=False,
                tolerance=tolerance,
                max_rel_diff=float('inf'),
                failing_sectors=[],
                details=None,
            )
        Bi = (
            E_ind.loc[flows, industries]
            .divide(x.reindex(industries).fillna(0.0), axis=1)
            .fillna(0.0)
        )
        E_c = Bi @ Vnorm.loc[industries, commodities]
        E_c = E_c.multiply(q.reindex(commodities).fillna(0.0), axis=1)
    else:
        assert V is not None and x is not None
        C_m = compute_commodity_mix_matrix(V=V, x=x)
        industries = C_m.columns.intersection(E_ind.columns)
        if len(industries) == 0:
            return DiagnosticResult(
                name='compare_E_and_LCI_result: empty industry intersection',
                passed=False,
                tolerance=tolerance,
                max_rel_diff=float('inf'),
                failing_sectors=[],
                details=None,
            )
        E_aligned = E_ind.loc[flows, industries]
        E_c = (C_m.loc[:, industries] @ E_aligned.T).T
        commodities = (
            B.columns.intersection(E_c.columns)
            .intersection(L.index)
            .intersection(y.index)
        )
        if len(commodities) == 0:
            return DiagnosticResult(
                name='compare_E_and_LCI_result: empty commodity intersection',
                passed=False,
                tolerance=tolerance,
                max_rel_diff=float('inf'),
                failing_sectors=[],
                details=None,
            )
        E_c = E_c.loc[flows, commodities]

    B_c = B.loc[flows, commodities]
    L_a = L.loc[commodities, commodities]
    y_a = y.reindex(commodities).fillna(0.0)

    LCI = compute_E_from_BLy(B=B_c, L=L_a, y=y_a)
    LCI = LCI.reindex(index=E_c.index, columns=E_c.columns).fillna(0.0)

    # useeior: (LCI - E) / E  →  value=E_c, value_check=LCI
    return validate_result(
        'compare_E_and_LCI_result',
        _flatten_matrix_for_validate(E_c),
        _flatten_matrix_for_validate(LCI),
        tolerance=tolerance,
        include_details=include_details,
    )
