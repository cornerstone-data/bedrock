"""SUT orchestration: ``engine(free, residual, masks)`` around ``gras_balance``.

PR2 of Step 5. Named protocol for hard T1 and T11–T17 only. Soft targets
(T2, T4, T6–T9) are skipped: their ``.values`` are not read; unconstrained
slots hold at the current Z row/col sum. KRAS is a later PR.

Do not import ``nowcast_targets`` or ``nowcast_mask``. Bridge / tax literals
match ``nowcast_mask.SUPPLY_BRIDGE_COLUMNS`` and Use VA rows (BEA trailing
space on ``TRADE ``).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

import numpy as np
import pandas as pd

from bedrock.utils.economic.balance.gras import GrasBalanceResult, gras_balance
from bedrock.utils.economic.balance.mask import SutMask
from bedrock.utils.economic.balance.offset import assert_free_seed
from bedrock.utils.economic.balance.targets import Target, TargetSet

# Match nowcast_mask.SUPPLY_BRIDGE_COLUMNS / Use VA rows; do not import that
# module. Trailing space on TRADE is BEA's label.
TRADE = 'TRADE '
TRANS = 'TRANS'
MDTY = 'MDTY'
TOP = 'TOP'
SUB = 'SUB'
T00TOP = 'T00TOP'
T00SUB = 'T00SUB'
CUSTOMS = '4200ID'

NAMED = ('T1', 'T11', 'T12', 'T13', 'T14', 'T15', 'T16', 'T17')
KNOWN_HARD = frozenset(NAMED)
REQUIRED = ('T1', 'T11')


@dataclass(frozen=True)
class SutBalanceResult:
    blocks: dict[str, pd.DataFrame]
    outer_iterations: int
    t11_max_abs_residual: float
    skipped: tuple[str, ...]
    last: dict[str, GrasBalanceResult]


def _require_subset(have: pd.Index, need: pd.Index, what: str) -> None:
    missing = [label for label in need if label not in have]
    if missing:
        raise KeyError(f'{what} names labels not on the panel: {missing}')


def _require_label(have: pd.Index, label: str, what: str) -> None:
    if label not in have:
        raise KeyError(f'{what} names {label!r}, which is not on the panel')


def _lookup(residual: TargetSet) -> tuple[dict[str, Target], tuple[str, ...]]:
    """First match by name among T1/T11–T17. Duplicates of those names raise."""
    seen: dict[str, Target] = {}
    skipped: list[str] = []
    for target in residual:
        if target.name in NAMED:
            if target.name in seen:
                raise ValueError(f'duplicate target name {target.name!r}')
            seen[target.name] = target
        if not target.hard:
            skipped.append(target.name)
            continue
        if target.name not in KNOWN_HARD:
            raise ValueError(f'unknown hard target name {target.name!r}')
    for name in REQUIRED:
        found = seen.get(name)
        if found is None or not found.hard:
            raise ValueError(f'{name} must be present with hard=True; missing or soft')
    t13 = seen.get('T13')
    t14 = seen.get('T14')
    t13_on = t13 is not None and t13.hard
    t14_on = t14 is not None and t14.hard
    if t13_on and not t14_on:
        raise ValueError('hard T13 requires hard T14 (cannot split TOP vs MDTY)')
    return seen, tuple(skipped)


def _imposed(seen: Mapping[str, Target], name: str) -> Target | None:
    target = seen.get(name)
    if target is None or not target.hard:
        return None
    return target


def _check_labels(
    z_use: pd.DataFrame,
    z_supply: pd.DataFrame,
    seen: Mapping[str, Target],
) -> None:
    t1 = seen['T1']
    t11 = seen['T11']
    _require_subset(z_use.columns, t1.values.index, 'T1')
    _require_subset(z_use.index, t11.values.index, 'T11')
    _require_subset(z_supply.index, t11.values.index, 'T11')
    t17 = _imposed(seen, 'T17')
    if t17 is not None:
        _require_subset(z_use.columns, t17.values.index, 'T17')
        _require_subset(z_supply.columns, t17.values.index, 'T17')
        _require_label(z_use.index, T00TOP, 'T17')
        _require_label(z_use.index, T00SUB, 'T17')
    if _imposed(seen, 'T12') is not None:
        _require_label(z_use.index, T00SUB, 'T12')
        _require_label(z_supply.columns, SUB, 'T12')
    if _imposed(seen, 'T13') is not None:
        _require_label(z_use.index, T00TOP, 'T13')
        _require_label(z_supply.columns, TOP, 'T13')
        _require_label(z_supply.columns, MDTY, 'T13')
    if _imposed(seen, 'T14') is not None:
        _require_label(z_use.index, T00TOP, 'T14')
        _require_label(z_use.columns, CUSTOMS, 'T14')
        _require_label(z_supply.columns, MDTY, 'T14')
    if _imposed(seen, 'T15') is not None:
        _require_label(z_supply.columns, TRADE, 'T15')
    if _imposed(seen, 'T16') is not None:
        _require_label(z_supply.columns, TRANS, 'T16')


def _cell(frame: pd.DataFrame, row: str, col: str) -> float:
    return float(np.asarray(frame.loc[row, col], dtype=np.float64))


def _t11_max_abs(t11: Target, z_use: pd.DataFrame, z_supply: pd.DataFrame) -> float:
    residual = (t11.evaluate({'use': z_use, 'supply': z_supply}) - t11.values).abs()
    if residual.empty:
        return 0.0
    return float(residual.max())


def _t11_live(t11_index: pd.Index, free: pd.DataFrame) -> pd.Index:
    """T11 labels whose scaled-block row has at least one free cell.

    A frozen or structurally empty Use row is absorbed on Supply (T11's
    job). Writing ``Z_supply.row - T11.values`` onto that Use row sends a
    nonzero target at an empty free margin; kernel ``atol`` is 0.0, so
    even BEA's ±$1M publication rounding raises. Hold those rows at the
    current Z sum instead. Same-block empties (T1, T12–T17) still raise.
    """
    return t11_index[free.loc[t11_index].any(axis=1).to_numpy()]


def _use_vectors(
    z_use: pd.DataFrame,
    z_supply: pd.DataFrame,
    seen: Mapping[str, Target],
    use_free: pd.DataFrame,
) -> tuple[pd.Series, pd.Series]:
    t1 = seen['T1']
    t11 = seen['T11']
    row_t = z_use.sum(axis=1).astype(float)
    col_t = z_use.sum(axis=0).astype(float)
    col_t.loc[t1.values.index] = t1.values.astype(float)
    live = _t11_live(t11.values.index, use_free)
    row_t.loc[live] = z_supply.sum(axis=1).loc[live] - t11.values.astype(float).loc[live]
    t12 = _imposed(seen, 'T12')
    if t12 is not None:
        row_t.loc[T00SUB] = float(t12.values.item()) + float(z_supply[SUB].sum())
    t13 = _imposed(seen, 'T13')
    if t13 is not None:
        row_t.loc[T00TOP] = (
            float(t13.values.item())
            + float(z_supply[TOP].sum())
            + float(z_supply[MDTY].sum())
        )
    return row_t, col_t


def _supply_vectors(
    z_use: pd.DataFrame,
    z_supply: pd.DataFrame,
    seen: Mapping[str, Target],
    supply_free: pd.DataFrame,
) -> tuple[pd.Series, pd.Series]:
    t11 = seen['T11']
    row_t = z_supply.sum(axis=1).astype(float)
    col_t = z_supply.sum(axis=0).astype(float)
    live = _t11_live(t11.values.index, supply_free)
    row_t.loc[live] = z_use.sum(axis=1).loc[live] + t11.values.astype(float).loc[live]
    t17 = _imposed(seen, 'T17')
    if t17 is not None:
        wedge = z_use.loc[[T00TOP, T00SUB]].sum(axis=0)
        combined = t17.values.astype(float) + z_use.sum(axis=0) - wedge
        col_t.loc[t17.values.index] = combined.loc[t17.values.index]
    t15 = _imposed(seen, 'T15')
    if t15 is not None:
        col_t.loc[TRADE] = float(t15.values.item())
    t16 = _imposed(seen, 'T16')
    if t16 is not None:
        col_t.loc[TRANS] = float(t16.values.item())
    t14 = _imposed(seen, 'T14')
    if t14 is not None:
        col_t.loc[MDTY] = _cell(z_use, T00TOP, CUSTOMS) - float(t14.values.item())
    t12 = _imposed(seen, 'T12')
    if t12 is not None:
        col_t.loc[SUB] = float(z_use.loc[T00SUB].sum()) - float(t12.values.item())
    t13 = _imposed(seen, 'T13')
    if t13 is not None:
        col_t.loc[TOP] = (
            float(z_use.loc[T00TOP].sum()) - float(t13.values.item())
        ) - float(np.asarray(col_t.loc[MDTY], dtype=np.float64))
    return row_t, col_t


def _balance_block(
    z: pd.DataFrame,
    mask: SutMask,
    row_t: pd.Series,
    col_t: pd.Series,
    *,
    rtol: float,
    close_rows_exactly: bool,
) -> tuple[pd.DataFrame, GrasBalanceResult]:
    result = gras_balance(
        matrix=z.to_numpy(dtype=np.float64),
        row_targets=row_t.to_numpy(dtype=np.float64),
        col_targets=col_t.to_numpy(dtype=np.float64),
        free_mask=mask.free.to_numpy(),
        sign_flex=mask.sign_lock.to_numpy() == 0,
        rtol=rtol,
        project_infeasible=False,
        close_rows_exactly=close_rows_exactly,
    )
    balanced = pd.DataFrame(result.matrix, index=z.index, columns=z.columns)
    return balanced, result


def engine(
    free: Mapping[str, pd.DataFrame],
    residual: TargetSet,
    masks: Mapping[str, SutMask],
    *,
    max_outer: int = 20,
    rtol: float = 1e-6,
    atol: float = 100.0,
    close_rows_on_last: bool = True,
) -> SutBalanceResult:
    """Balance Use then Supply against a residual TargetSet.

    ``atol`` is the T11 outer stop (BEA million-dollar units). It is not
    passed to ``gras_balance``; kernel ``atol`` stays 0.0.
    """
    if max_outer < 1:
        raise ValueError(f'max_outer must be >= 1, got {max_outer}')
    if 'use' not in free or 'supply' not in free:
        raise KeyError(
            f"engine requires blocks 'use' and 'supply' in free; got {sorted(free)}"
        )
    if 'use' not in masks or 'supply' not in masks:
        raise KeyError(
            f"engine requires blocks 'use' and 'supply' in masks; got {sorted(masks)}"
        )

    seen, skipped = _lookup(residual)
    blocks = {name: frame.copy() for name, frame in free.items()}
    z_use = blocks['use']
    z_supply = blocks['supply']
    _check_labels(z_use, z_supply, seen)
    assert_free_seed(z_use, masks['use'])
    assert_free_seed(z_supply, masks['supply'])

    last: dict[str, GrasBalanceResult] = {}
    outer_iterations = 0
    t11 = seen['T11']

    def run_pair(close_rows_exactly: bool) -> None:
        nonlocal z_use, z_supply, outer_iterations
        row_t, col_t = _use_vectors(z_use, z_supply, seen, masks['use'].free)
        z_use, last['use'] = _balance_block(
            z_use,
            masks['use'],
            row_t,
            col_t,
            rtol=rtol,
            close_rows_exactly=close_rows_exactly,
        )
        row_t, col_t = _supply_vectors(z_use, z_supply, seen, masks['supply'].free)
        z_supply, last['supply'] = _balance_block(
            z_supply,
            masks['supply'],
            row_t,
            col_t,
            rtol=rtol,
            close_rows_exactly=close_rows_exactly,
        )
        blocks['use'] = z_use
        blocks['supply'] = z_supply
        outer_iterations += 1

    t11_err = 0.0
    for i in range(max_outer):
        closer = close_rows_on_last and i == max_outer - 1
        run_pair(closer)
        t11_err = _t11_max_abs(t11, z_use, z_supply)
        if t11_err <= atol:
            if close_rows_on_last and not closer:
                run_pair(True)
                t11_err = _t11_max_abs(t11, z_use, z_supply)
            break

    return SutBalanceResult(
        blocks=blocks,
        outer_iterations=outer_iterations,
        t11_max_abs_residual=t11_err,
        skipped=skipped,
        last=last,
    )
