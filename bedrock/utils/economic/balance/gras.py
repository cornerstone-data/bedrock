"""Ndarray GRAS kernel for a single signed matrix.

Originally from the ceda repo (``ras_balancing.py``). Note that
non-negativity clamps, the sparse path, ``_neutralize_infeasible_targets``,
and RAS ``_margin_scale_factors`` are not copied from that repo. Scale is GRAS
(Lenzen, Wood and Gallego 2007; Temurshoev, Miller and Bouwmeester 2013
all-negative margins). Junius and Oosterhaven (2003) is the name only.

Callers pass one matrix, row/col vectors, ``free_mask``, and
``sign_flex``. Mapping from ``SutMask``: ``free_mask = mask.free.to_numpy()``,
``sign_flex = (mask.sign_lock.to_numpy() == 0)``. Kernel
``sign_flex is None`` is all-False (stricter than a default ``SutMask``).
Nonzero holds are the caller's offset, not this kernel.
``engine`` in ``orchestrate.py`` calls this kernel per block.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import numpy.typing as npt

logger = logging.getLogger('bedrock.utils.economic.balance.gras')

# Stall projection: a globally consistent margin problem can still be locally
# infeasible — a set of rows S whose combined targets exceed what their
# reachable columns can absorb. See ceda ras_balancing.py. Formulas stay
# non-negative (policy A): project_infeasible raises on signed seed/targets.
STALL_CHECK_WINDOW = 25
STALL_IMPROVEMENT_FRACTION = 0.01
STALL_MULTIPLIER_LOG_GROWTH = 0.1
STALL_MIN_RELATIVE_GAP_FLOOR = 0.002
STALL_MIN_GAP_RTOL_MULTIPLE = 10.0
STALL_GAP_DECAY = 0.5
STALL_MIN_LOG_GROWTH_FLOOR = 1e-3


def _stall_min_relative_gap(rtol: float) -> float:
    return max(STALL_MIN_RELATIVE_GAP_FLOOR, STALL_MIN_GAP_RTOL_MULTIPLE * rtol)


def _clamp_deficient_margin(
    *,
    sums: npt.NDArray[np.float64],
    targets: npt.NDArray[np.float64],
    log_multiplier_growth: npt.NDArray[np.float64],
    min_relative_gap: float,
    min_log_growth: float,
) -> tuple[npt.NDArray[np.float64], npt.NDArray[np.float64], npt.NDArray[np.bool_]]:
    """Clamp targets of the deficient set down to deliverable capacity."""
    gap = targets - sums
    with np.errstate(divide='ignore', invalid='ignore'):
        relative_gap = np.where(targets > 0.0, gap / targets, 0.0)
    deficient = (
        (log_multiplier_growth > min_log_growth)
        & (gap > 0.0)
        & (relative_gap > min_relative_gap)
    )
    clamped_amounts = np.zeros_like(targets)
    if not deficient.any():
        return targets, clamped_amounts, deficient
    clamped_amounts[deficient] = gap[deficient]
    targets = targets.copy()
    targets[deficient] = sums[deficient]
    return targets, clamped_amounts, deficient


class _StallDetector:
    """Tracks per-margin multiplier growth and projects locally infeasible
    targets. Non-negative formulas only (stall policy A)."""

    def __init__(self, n_rows: int, n_cols: int, *, rtol: float) -> None:
        self._cum_log_r = np.zeros(n_rows, dtype=np.float64)
        self._cum_log_c = np.zeros(n_cols, dtype=np.float64)
        self._log_r_at_last_check = np.zeros(n_rows, dtype=np.float64)
        self._log_c_at_last_check = np.zeros(n_cols, dtype=np.float64)
        self._err_at_last_check = np.inf
        self._min_relative_gap = _stall_min_relative_gap(rtol)
        self._min_gap_floor = rtol
        self._min_log_growth = STALL_MULTIPLIER_LOG_GROWTH
        self.rounds = 0
        self.projected_target_mass = 0.0
        self.projected_row_mask = np.zeros(n_rows, dtype=bool)
        self.projected_col_mask = np.zeros(n_cols, dtype=bool)

    def record_scales(
        self,
        row_scale: npt.NDArray[np.float64],
        col_scale: npt.NDArray[np.float64],
    ) -> None:
        positive_r = row_scale > 0.0
        self._cum_log_r[positive_r] += np.log(row_scale[positive_r])
        positive_c = col_scale > 0.0
        self._cum_log_c[positive_c] += np.log(col_scale[positive_c])

    def maybe_project(
        self,
        *,
        iteration: int,
        residual: float,
        row_sums: npt.NDArray[np.float64],
        col_sums: npt.NDArray[np.float64],
        row_targets: npt.NDArray[np.float64],
        col_targets: npt.NDArray[np.float64],
    ) -> tuple[npt.NDArray[np.float64], npt.NDArray[np.float64]]:
        if iteration % STALL_CHECK_WINDOW != 0:
            return row_targets, col_targets
        err = residual
        stalled = (
            np.isfinite(self._err_at_last_check)
            and self._err_at_last_check > 0.0
            and (self._err_at_last_check - err)
            < STALL_IMPROVEMENT_FRACTION * self._err_at_last_check
        )
        row_growth = self._cum_log_r - self._log_r_at_last_check
        col_growth = self._cum_log_c - self._log_c_at_last_check
        self._err_at_last_check = err
        self._log_r_at_last_check = self._cum_log_r.copy()
        self._log_c_at_last_check = self._cum_log_c.copy()
        if not stalled:
            return row_targets, col_targets

        row_targets, row_clamped, deficient_rows = _clamp_deficient_margin(
            sums=row_sums,
            targets=row_targets,
            log_multiplier_growth=row_growth,
            min_relative_gap=self._min_relative_gap,
            min_log_growth=self._min_log_growth,
        )
        col_targets, col_clamped, deficient_cols = _clamp_deficient_margin(
            sums=col_sums,
            targets=col_targets,
            log_multiplier_growth=col_growth,
            min_relative_gap=self._min_relative_gap,
            min_log_growth=self._min_log_growth,
        )
        row_mass = float(row_clamped.sum())
        col_mass = float(col_clamped.sum())
        if not (deficient_rows.any() or deficient_cols.any()):
            relaxable = (
                self._min_relative_gap > self._min_gap_floor
                or self._min_log_growth > STALL_MIN_LOG_GROWTH_FLOOR
            )
            if relaxable:
                self._min_relative_gap = max(
                    self._min_gap_floor, self._min_relative_gap * STALL_GAP_DECAY
                )
                self._min_log_growth = max(
                    STALL_MIN_LOG_GROWTH_FLOOR,
                    self._min_log_growth * STALL_GAP_DECAY,
                )
                logger.info(
                    'GRAS: stall at iteration %d with no margin above the '
                    'projection thresholds; relaxing to gap>%.4g growth>%.4g',
                    iteration,
                    self._min_relative_gap,
                    self._min_log_growth,
                )
            return row_targets, col_targets

        self.rounds += 1
        self.projected_target_mass += row_mass + col_mass
        self.projected_row_mask |= deficient_rows
        self.projected_col_mask |= deficient_cols
        col_total = float(col_targets.sum())
        if col_total > 0.0:
            col_targets = col_targets * (float(row_targets.sum()) / col_total)
        logger.warning(
            'GRAS: stall at iteration %d (residual pinned at %.4f); projecting '
            '%d row targets (-%.4e) and %d col targets (-%.4e) down to '
            'deliverable capacity (locally infeasible cut)',
            iteration,
            err,
            int(deficient_rows.sum()),
            row_mass,
            int(deficient_cols.sum()),
            col_mass,
        )
        return row_targets, col_targets


@dataclass(frozen=True)
class GrasBalanceResult:
    matrix: npt.NDArray[np.float64]
    converged: bool
    iterations: int
    max_row_err: float
    max_col_err: float
    max_row_rel_err: float
    max_col_rel_err: float
    col_rel_err_p50: float
    col_rel_err_p99: float
    projection_rounds: int
    projected_target_mass: float
    projected_rows: npt.NDArray[np.bool_]
    projected_cols: npt.NDArray[np.bool_]


def _log_progress(
    iteration: int,
    max_row_err: float,
    max_col_err: float,
    row_residual: float,
    col_residual: float,
) -> None:
    if iteration % 10 != 0:
        return
    worst = max(row_residual, col_residual)
    logger.info(
        'GRAS iter %d: %s (worst residual %.3f, need <= 1.0) '
        'max_row_err=%.4e max_col_err=%.4e residual_row=%.3f residual_col=%.3f',
        iteration,
        'CONVERGED' if worst <= 1.0 else 'not converged',
        worst,
        max_row_err,
        max_col_err,
        row_residual,
        col_residual,
    )


def _normalized_residual(
    sums: npt.NDArray[np.float64],
    targets: npt.NDArray[np.float64],
    rtol: float,
    atol: float,
) -> float:
    """Worst margin violation as a multiple of its own allowance."""
    allowed = atol + rtol * np.abs(targets)
    err = np.abs(sums - targets)
    ratio = np.zeros_like(err, dtype=np.float64)
    has_allowance = allowed > 0.0
    ratio[has_allowance] = err[has_allowance] / allowed[has_allowance]
    ratio[~has_allowance] = np.where(err[~has_allowance] > 0.0, np.inf, 0.0)
    return float(np.max(ratio, initial=0.0))


def _relative_errors(
    sums: npt.NDArray[np.float64],
    targets: npt.NDArray[np.float64],
    atol: float,
) -> npt.NDArray[np.float64]:
    denom = np.maximum(np.abs(targets), atol)
    err = np.abs(sums - targets)
    out = np.zeros_like(err, dtype=np.float64)
    positive = denom > 0.0
    out[positive] = err[positive] / denom[positive]
    out[~positive] = np.where(err[~positive] > 0.0, np.inf, 0.0)
    return out


def _max_relative_error(
    sums: npt.NDArray[np.float64],
    targets: npt.NDArray[np.float64],
    atol: float,
) -> float:
    return float(np.max(_relative_errors(sums, targets, atol), initial=0.0))


def _margin_errors(
    row_sums: npt.NDArray[np.float64],
    col_sums: npt.NDArray[np.float64],
    row_targets: npt.NDArray[np.float64],
    col_targets: npt.NDArray[np.float64],
    rtol: float,
    atol: float,
) -> tuple[float, float, float, float]:
    return (
        float(np.max(np.abs(row_sums - row_targets), initial=0.0)),
        float(np.max(np.abs(col_sums - col_targets), initial=0.0)),
        _normalized_residual(row_sums, row_targets, rtol, atol),
        _normalized_residual(col_sums, col_targets, rtol, atol),
    )


def _fit_quality(
    row_sums: npt.NDArray[np.float64],
    col_sums: npt.NDArray[np.float64],
    row_targets: npt.NDArray[np.float64],
    col_targets: npt.NDArray[np.float64],
    atol: float,
) -> tuple[float, float, float, float]:
    col_rel = _relative_errors(col_sums, col_targets, atol)
    if col_rel.size == 0:
        return _max_relative_error(row_sums, row_targets, atol), 0.0, 0.0, 0.0
    return (
        _max_relative_error(row_sums, row_targets, atol),
        float(np.max(col_rel)),
        float(np.percentile(col_rel, 50)),
        float(np.percentile(col_rel, 99)),
    )


def _gras_scale_factors(
    p: npt.NDArray[np.float64],
    n: npt.NDArray[np.float64],
    t: npt.NDArray[np.float64],
) -> npt.NDArray[np.float64]:
    """GRAS multipliers. Later ``where`` wins, matching ``gras_internal``."""
    with np.errstate(divide='ignore', invalid='ignore'):
        root = np.sqrt(np.maximum(0.0, t**2 + 4.0 * p * n))
        s = np.where(p > 0.0, (t + root) / (2.0 * p), n / (-t))
        s = np.where((p <= 0.0) & (n == 0.0), 1.0, s)
        s = np.where((t == 0.0) & (p == 0.0), 0.0, s)
    return s


def _gras_scale(
    x: npt.NDArray[np.float64],
    targets: npt.NDArray[np.float64],
    *,
    axis: int,
    original: npt.NDArray[np.float64],
    sign_flex: npt.NDArray[np.bool_],
    free_mask: npt.NDArray[np.bool_],
) -> tuple[npt.NDArray[np.float64], npt.NDArray[np.float64]]:
    """Apply GRAS along rows (axis=1) or columns (axis=0). Clamp vs input."""
    if axis == 1:
        pos = np.where(x > 0.0, x, 0.0).sum(axis=1)
        neg = np.where(x < 0.0, -x, 0.0).sum(axis=1)
        s = _gras_scale_factors(pos, neg, targets)
        s_b = s[:, np.newaxis]
    else:
        pos = np.where(x > 0.0, x, 0.0).sum(axis=0)
        neg = np.where(x < 0.0, -x, 0.0).sum(axis=0)
        s = _gras_scale_factors(pos, neg, targets)
        s_b = s[np.newaxis, :]
    with np.errstate(divide='ignore', invalid='ignore'):
        scaled = np.where(x > 0.0, x * s_b, np.where(x < 0.0, x / s_b, 0.0))
        scaled = np.where(s_b == 0.0, 0.0, scaled)
    flipped = np.sign(scaled) != np.sign(original)
    scaled = np.where((~sign_flex) & flipped, 0.0, scaled)
    scaled = np.where(free_mask, scaled, 0.0)
    return scaled, s


def _as_float_copy(
    values: npt.NDArray[np.float64], *, name: str
) -> npt.NDArray[np.float64]:
    copied = np.array(values, dtype=np.float64, copy=True)
    if not np.isfinite(copied).all():
        raise ValueError(f'{name} contains non-finite values')
    return copied


def _as_bool_copy(values: npt.NDArray[np.bool_]) -> npt.NDArray[np.bool_]:
    return np.array(values, dtype=bool, copy=True)


def _raise_empty_free_margins(
    free_mask: npt.NDArray[np.bool_],
    row_targets: npt.NDArray[np.float64],
    col_targets: npt.NDArray[np.float64],
    atol: float,
) -> None:
    empty_rows = ~free_mask.any(axis=1)
    empty_cols = ~free_mask.any(axis=0)
    bad_rows = empty_rows & (np.abs(row_targets) > atol)
    bad_cols = empty_cols & (np.abs(col_targets) > atol)
    if not (bad_rows.any() or bad_cols.any()):
        return
    row_idx = int(np.flatnonzero(bad_rows)[0]) if bad_rows.any() else None
    col_idx = int(np.flatnonzero(bad_cols)[0]) if bad_cols.any() else None
    raise ValueError(
        'nonzero target facing an empty free margin'
        + (f', first at row {row_idx}' if row_idx is not None else '')
        + (f', first at col {col_idx}' if col_idx is not None else '')
    )


def gras_balance(
    *,
    matrix: npt.NDArray[np.float64],
    row_targets: npt.NDArray[np.float64],
    col_targets: npt.NDArray[np.float64],
    free_mask: npt.NDArray[np.bool_] | None = None,
    sign_flex: npt.NDArray[np.bool_] | None = None,
    max_iter: int = 100,
    rtol: float = 1e-6,
    atol: float = 0.0,
    project_infeasible: bool = False,
    close_rows_exactly: bool = False,
) -> GrasBalanceResult:
    """GRAS-balance one signed matrix to row and column targets.

    ``free_mask is None`` participates every cell (all-True).
    ``sign_flex is None`` forbids sign changes (all-False). A later SUT
    wrapper must pass ``sign_flex=(mask.sign_lock.to_numpy() == 0)``;
    omitting it sign-locks the whole table.
    """
    x_in = _as_float_copy(matrix, name='matrix')
    if x_in.ndim != 2:
        raise ValueError(f'matrix must be 2-D, got shape {x_in.shape}')
    n_rows, n_cols = x_in.shape
    row_t = _as_float_copy(row_targets, name='row_targets').ravel()
    col_t = _as_float_copy(col_targets, name='col_targets').ravel()
    if row_t.shape != (n_rows,) or col_t.shape != (n_cols,):
        raise ValueError(
            f'Target shapes ({row_t.shape[0]}, {col_t.shape[0]}) '
            f'do not match matrix shape ({n_rows}, {n_cols})'
        )
    if free_mask is None:
        mask = np.ones((n_rows, n_cols), dtype=bool)
    else:
        mask = _as_bool_copy(free_mask)
        if mask.shape != x_in.shape:
            raise ValueError(
                f'free_mask shape {mask.shape} does not match matrix {x_in.shape}'
            )
    if sign_flex is None:
        flex = np.zeros((n_rows, n_cols), dtype=bool)
    else:
        flex = _as_bool_copy(sign_flex)
        if flex.shape != x_in.shape:
            raise ValueError(
                f'sign_flex shape {flex.shape} does not match matrix {x_in.shape}'
            )

    original = x_in.copy()
    x = np.where(mask, x_in, 0.0)
    _raise_empty_free_margins(mask, row_t, col_t, atol)

    if project_infeasible:
        participating = x[mask]
        if (participating < 0.0).any() or (row_t < 0.0).any() or (col_t < 0.0).any():
            raise ValueError(
                'project_infeasible=True is undefined on signed problems; '
                'found a negative participating seed cell or a negative target'
            )

    stall = _StallDetector(n_rows, n_cols, rtol=rtol)
    converged = False
    iterations = 0
    for iterations in range(1, max_iter + 1):
        x, row_scale = _gras_scale(
            x, row_t, axis=1, original=original, sign_flex=flex, free_mask=mask
        )
        pre_col_sums = x.sum(axis=0)
        x, col_scale = _gras_scale(
            x, col_t, axis=0, original=original, sign_flex=flex, free_mask=mask
        )
        stall.record_scales(row_scale, col_scale)
        row_sums = x.sum(axis=1)
        col_sums = x.sum(axis=0)
        max_row_err, max_col_err, row_residual, col_residual = _margin_errors(
            row_sums, col_sums, row_t, col_t, rtol, atol
        )
        _log_progress(iterations, max_row_err, max_col_err, row_residual, col_residual)
        if max(row_residual, col_residual) <= 1.0:
            converged = True
            break
        if project_infeasible:
            row_t, col_t = stall.maybe_project(
                iteration=iterations,
                residual=max(row_residual, col_residual),
                row_sums=row_sums,
                col_sums=pre_col_sums,
                row_targets=row_t,
                col_targets=col_t,
            )

    if close_rows_exactly:
        x, _ = _gras_scale(
            x, row_t, axis=1, original=original, sign_flex=flex, free_mask=mask
        )

    final_row_sums = x.sum(axis=1)
    final_col_sums = x.sum(axis=0)
    max_row_err, max_col_err, _, _ = _margin_errors(
        final_row_sums, final_col_sums, row_t, col_t, rtol, atol
    )
    max_row_rel, max_col_rel, col_p50, col_p99 = _fit_quality(
        final_row_sums, final_col_sums, row_t, col_t, atol
    )
    if project_infeasible:
        projected_rows = stall.projected_row_mask
        projected_cols = stall.projected_col_mask
    else:
        projected_rows = np.zeros(n_rows, dtype=bool)
        projected_cols = np.zeros(n_cols, dtype=bool)
    return GrasBalanceResult(
        matrix=x,
        converged=converged,
        iterations=iterations,
        max_row_err=max_row_err,
        max_col_err=max_col_err,
        max_row_rel_err=max_row_rel,
        max_col_rel_err=max_col_rel,
        col_rel_err_p50=col_p50,
        col_rel_err_p99=col_p99,
        projection_rounds=stall.rounds,
        projected_target_mass=stall.projected_target_mass,
        projected_rows=projected_rows,
        projected_cols=projected_cols,
    )
