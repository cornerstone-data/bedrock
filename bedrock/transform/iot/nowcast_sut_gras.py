"""Nowcast Supply/Use GRAS: ``engine(free, residual, masks)`` around ``gras_balance``.

Hard targets this adapter knows by name:

* T1  — industry gross output (Use columns)
* T11 — commodity identity (Supply row − Use row)
* T12 — subsidies (Use T00SUB = Supply SUB)
* T13 — product taxes (Use T00TOP = Supply TOP + MDTY)
* T14 — customs duties (Use T00TOP[4200ID] = Supply MDTY)
* T15 — trade margin column sums to 0 (``TRADE ``)
* T16 — transport margin column sums to 0 (TRANS)
* T17 — basic-to-producer wedge (Supply industry col vs Use, via T00TOP/T00SUB)

Soft targets when ``impose_soft`` (default True): T2/T7 are a weighted blend
from the entry Z; T4 is a column-neutral closer after each Use pass.
T6/T8/T9 whole-name defer when T12–T14 occupy any of their slots.
``impose_soft=False`` skips soft targets: their ``.values`` are not read;
unconstrained slots hold at the current Z row/col sum. The engine reads
``target.weight``; it does not import ``WEIGHTS``.

Does not import ``nowcast_targets`` or ``nowcast_mask``. Bridge / tax
literals match ``nowcast_mask.SUPPLY_BRIDGE_COLUMNS`` and Use VA rows
(BEA trailing space on ``TRADE ``).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

import numpy as np
import pandas as pd

from bedrock.utils.economic.balance.gras import GrasBalanceResult, gras_balance
from bedrock.utils.economic.balance.mask import SutMask
from bedrock.utils.economic.balance.offset import assert_free_seed
from bedrock.utils.economic.balance.targets import Target, TargetSet, TargetTerm

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
MCIF = 'MCIF'

NAMED = ('T1', 'T11', 'T12', 'T13', 'T14', 'T15', 'T16', 'T17')
KNOWN_HARD = frozenset(NAMED)
KNOWN_SOFT = frozenset({'T2', 'T4', 'T6', 'T7', 'T8', 'T9'})
KNOWN_NAMES = KNOWN_HARD | KNOWN_SOFT
REQUIRED = ('T1', 'T11')


@dataclass(frozen=True)
class SutBalanceResult:
    blocks: dict[str, pd.DataFrame]
    outer_iterations: int
    t11_max_abs_residual: float
    skipped: tuple[str, ...]
    last: dict[str, GrasBalanceResult]
    soft_deferred: tuple[str, ...]


def _require_subset(have: pd.Index, need: pd.Index, what: str) -> None:
    missing = [label for label in need if label not in have]
    if missing:
        raise KeyError(f'{what} names labels not on the panel: {missing}')


def _require_label(have: pd.Index, label: str, what: str) -> None:
    if label not in have:
        raise KeyError(f'{what} names {label!r}, which is not on the panel')


def _t4_term(t4: Target) -> tuple[TargetTerm, str]:
    if len(t4.terms) != 1:
        raise ValueError('T4 must have exactly one term')
    term = t4.terms[0]
    if term.aggregator is None:
        raise ValueError('T4 requires an aggregator')
    restrict = term.restrict_to
    if restrict is None or len(restrict) != 1:
        raise ValueError('T4 restrict_to must be exactly one Use row')
    return term, restrict[0]


def _defer_soft(name: str, seen: Mapping[str, Target]) -> bool:
    if name in ('T6', 'T9'):
        return _imposed(seen, 'T12') is not None or _imposed(seen, 'T13') is not None
    if name == 'T8':
        return _imposed(seen, 'T14') is not None
    return False


def _lookup(
    residual: TargetSet, *, impose_soft: bool
) -> tuple[dict[str, Target], tuple[str, ...], tuple[str, ...]]:
    """First match by name among hard T1/T11–T17 and known soft T2/T4/T6–T9."""
    seen: dict[str, Target] = {}
    for target in residual:
        if target.name in KNOWN_NAMES:
            if target.name in seen:
                raise ValueError(f'duplicate target name {target.name!r}')
            seen[target.name] = target
        if target.hard and target.name not in KNOWN_HARD:
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

    skipped: list[str] = []
    deferred: list[str] = []
    for target in residual:
        if target.hard:
            continue
        name = target.name
        if not impose_soft or name not in KNOWN_SOFT:
            skipped.append(name)
            continue
        if _defer_soft(name, seen):
            deferred.append(name)
            continue
    return seen, tuple(skipped), tuple(deferred)


def _imposed(seen: Mapping[str, Target], name: str) -> Target | None:
    target = seen.get(name)
    if target is None or not target.hard:
        return None
    return target


def _imposed_soft(
    seen: Mapping[str, Target],
    skipped: tuple[str, ...],
    deferred: tuple[str, ...],
) -> dict[str, Target]:
    held_back = set(skipped) | set(deferred)
    imposed: dict[str, Target] = {}
    for name in KNOWN_SOFT:
        target = seen.get(name)
        if target is None or target.hard or name in held_back:
            continue
        imposed[name] = target
    return imposed


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


def _check_soft_labels(
    z_use: pd.DataFrame,
    z_supply: pd.DataFrame,
    imposed: Mapping[str, Target],
) -> None:
    t2 = imposed.get('T2')
    if t2 is not None:
        _require_subset(z_use.columns, t2.values.index, 'T2')
    t4 = imposed.get('T4')
    if t4 is not None:
        _term, row_label = _t4_term(t4)
        _require_label(z_use.index, row_label, 'T4')
    t6 = imposed.get('T6')
    if t6 is not None:
        _require_subset(z_use.index, t6.values.index, 'T6')
    t7 = imposed.get('T7')
    if t7 is not None:
        _require_label(z_supply.columns, MCIF, 'T7')
        _require_subset(z_supply.columns, t7.values.index, 'T7')
    t8 = imposed.get('T8')
    if t8 is not None:
        _require_label(z_supply.columns, MDTY, 'T8')
        _require_subset(z_supply.columns, t8.values.index, 'T8')
    t9 = imposed.get('T9')
    if t9 is not None:
        _require_subset(z_supply.columns, t9.values.index, 'T9')


def _cell(frame: pd.DataFrame, row: str, col: str) -> float:
    return float(np.asarray(frame.loc[row, col], dtype=np.float64))


def _blend(target: Target, blocks: Mapping[str, pd.DataFrame]) -> pd.Series:
    current0 = target.evaluate(blocks)
    values = target.values.astype(float)
    return current0 + float(target.weight) * (values - current0)


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
    blends: Mapping[str, pd.Series],
) -> tuple[pd.Series, pd.Series]:
    t1 = seen['T1']
    t11 = seen['T11']
    row_t = z_use.sum(axis=1).astype(float)
    col_t = z_use.sum(axis=0).astype(float)
    col_t.loc[t1.values.index] = t1.values.astype(float)
    live = _t11_live(t11.values.index, use_free)
    row_t.loc[live] = (
        z_supply.sum(axis=1).loc[live] - t11.values.astype(float).loc[live]
    )
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
    t2 = blends.get('T2')
    if t2 is not None:
        slots = t2.index.difference(t1.values.index)
        col_t.loc[slots] = t2.loc[slots].astype(float)
    t6 = blends.get('T6')
    if t6 is not None:
        row_t.loc[t6.index] = t6.astype(float)
    return row_t, col_t


def _supply_vectors(
    z_use: pd.DataFrame,
    z_supply: pd.DataFrame,
    seen: Mapping[str, Target],
    supply_free: pd.DataFrame,
    blends: Mapping[str, pd.Series],
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
    t7 = blends.get('T7')
    if t7 is not None:
        col_t.loc[MCIF] = float(t7.astype(float).loc[MCIF])
    t8 = blends.get('T8')
    if t8 is not None:
        col_t.loc[MDTY] = float(t8.astype(float).loc[MDTY])
    t9 = blends.get('T9')
    if t9 is not None:
        col_t.loc[t9.index] = t9.astype(float)
    return row_t, col_t


def _apply_t4_closer(
    z_use: pd.DataFrame,
    mask: SutMask,
    t4: Target,
    desired: pd.Series,
) -> pd.DataFrame:
    """Scale free V00100 cells in a group; put −d on sign-flex compensators.

    Copy first: ``_balance_block`` wraps ``result.matrix`` with no copy.
    Write only compensable columns. The group may miss ``desired``.
    """
    z = z_use.copy()
    term, row_label = _t4_term(t4)
    aggregator = term.aggregator
    assert aggregator is not None
    group_pos = {name: i for i, name in enumerate(aggregator.groups)}
    free = mask.free
    sign_flex = mask.sign_lock.eq(0)

    for g_name in desired.index:
        gi = group_pos[g_name]
        members = [
            detail_j
            for ji, detail_j in enumerate(aggregator.detail)
            if aggregator.matrix[gi, ji] == 1.0 and detail_j in z.columns
        ]
        free_sum = 0.0
        frozen_sum = 0.0
        for j in members:
            val = float(np.asarray(z.loc[row_label, j], dtype=np.float64))
            if bool(free.loc[row_label, j]):
                free_sum += val
            else:
                frozen_sum += val
        if free_sum == 0.0:
            continue
        factor = (float(desired.loc[g_name]) - frozen_sum) / free_sum
        for j in members:
            if not bool(free.loc[row_label, j]):
                continue
            compensators: list[tuple[str, float]] = []
            abs_sum = 0.0
            for i in z.index:
                row_i = str(i)
                if row_i == row_label:
                    continue
                if bool(free.loc[row_i, j]) and bool(sign_flex.loc[row_i, j]):
                    cell = float(np.asarray(z.loc[row_i, j], dtype=np.float64))
                    compensators.append((row_i, cell))
                    abs_sum += abs(cell)
            if abs_sum == 0.0:
                continue
            old = float(np.asarray(z.loc[row_label, j], dtype=np.float64))
            new = factor * old
            d = new - old
            z.loc[row_label, j] = new
            for i, cell in compensators:
                z.loc[i, j] = cell + (-d * abs(cell) / abs_sum)
    return z


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
    impose_soft: bool = True,
) -> SutBalanceResult:
    """Balance Use then Supply against a residual TargetSet.

    ``atol`` is the T11 outer stop (BEA million-dollar units). It is not
    passed to ``gras_balance``; kernel ``atol`` stays 0.0.

    Soft targets blend once from the entry ``Z``. ``impose_soft=False`` is
    the hard-only protocol (T2/T4/T6–T9 skipped).
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

    seen, skipped, soft_deferred = _lookup(residual, impose_soft=impose_soft)
    blocks = {name: frame.copy() for name, frame in free.items()}
    z_use = blocks['use']
    z_supply = blocks['supply']
    _check_labels(z_use, z_supply, seen)
    imposed_soft = _imposed_soft(seen, skipped, soft_deferred)
    _check_soft_labels(z_use, z_supply, imposed_soft)
    assert_free_seed(z_use, masks['use'])
    assert_free_seed(z_supply, masks['supply'])

    entry = {'use': z_use, 'supply': z_supply}
    blends = {name: _blend(target, entry) for name, target in imposed_soft.items()}

    last: dict[str, GrasBalanceResult] = {}
    outer_iterations = 0
    t11 = seen['T11']

    def run_pair(close_rows_exactly: bool) -> None:
        nonlocal z_use, z_supply, outer_iterations
        row_t, col_t = _use_vectors(z_use, z_supply, seen, masks['use'].free, blends)
        z_use, last['use'] = _balance_block(
            z_use,
            masks['use'],
            row_t,
            col_t,
            rtol=rtol,
            close_rows_exactly=close_rows_exactly,
        )
        if 'T4' in blends:
            z_use = _apply_t4_closer(z_use, masks['use'], seen['T4'], blends['T4'])
        row_t, col_t = _supply_vectors(
            z_use, z_supply, seen, masks['supply'].free, blends
        )
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
        soft_deferred=soft_deferred,
    )
