"""Load and normalize D/N/q/class-MWh vectors for the comparison deck."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import TypeGuard

import pandas as pd

from bedrock.analysis.electricity.current.diagnostics.deck.pairs import (
    CHILD_SECTORS,
    CLASS_ORDER,
    GENERATION_SECTOR,
    STAR_SECTOR,
    ImplId,
    StepId,
)

MISSING = '—'
NA = 'N/A'
SAME = 'same'

# Display rounding used for "same" (matches the sample's three-decimal EFs).
EF_DECIMALS = 3
SAME_ATOL = 5e-4
# electricity_conversion_factors(mixed Aq) uses q already in MWh, so c_col ≈ 1.
_IDENTITY_C_COL = 1.0
_IDENTITY_C_COL_ATOL = 1e-6
# Dollar generation q is orders of magnitude above eGRID MWh (~4e9).
_Q_USD_OVER_MWH_MIN = 10.0


@dataclass
class StepSnapshot:
    """One YAML step's vectors. Missing pieces stay ``None``."""

    d: pd.Series | None = None
    n: pd.Series | None = None
    q: pd.Series | None = None
    x: pd.Series | None = None
    mixed: bool = False
    c_col: float | None = None
    class_mwh: pd.Series | None = None
    class_mwh_target: pd.Series | None = None


@dataclass
class ImplBundle:
    impl_id: ImplId
    steps: dict[StepId, StepSnapshot] = field(default_factory=dict)


def series_from_parquet(path: Path, value_col: str | None = None) -> pd.Series:
    """Accept original (1 × sectors) or pre-MECS (sectors × 1) freeze layouts."""
    frame = pd.read_parquet(path)
    if value_col is not None and value_col in frame.columns:
        out = frame[value_col].astype(float)
        out.index = out.index.map(str)
        return out
    if frame.shape[0] == 1 and frame.shape[1] > 1:
        out = frame.iloc[0].astype(float)
        out.index = out.index.map(str)
        out.index.name = 'sector'
        return out
    if frame.shape[1] == 1:
        out = frame.iloc[:, 0].astype(float)
        out.index = out.index.map(str)
        return out
    squeezed = frame.squeeze()
    if not isinstance(squeezed, pd.Series):
        raise TypeError(f'expected a Series from {path}, got {type(squeezed)}')
    out = squeezed.astype(float)
    out.index = out.index.map(str)
    return out


def class_mwh_from_parquet(path: Path) -> tuple[pd.Series, pd.Series | None]:
    frame = pd.read_parquet(path)
    if 'class' not in frame.columns or 'value' not in frame.columns:
        raise ValueError(f'{path} needs class/value columns')
    model = pd.Series(
        frame['value'].astype(float).to_numpy(),
        index=frame['class'].astype(str),
        dtype=float,
    )
    model = model[~model.index.isin(['HH'])]
    target: pd.Series | None = None
    if 'target' in frame.columns:
        target = pd.Series(
            frame['target'].astype(float).to_numpy(),
            index=frame['class'].astype(str),
            dtype=float,
        )
        target = target[~target.index.isin(['HH'])]
    return model, target


def c_col_is_monetary(c_col: float | None) -> TypeGuard[float]:
    """True when ``c_col`` is MWh/$ from dollar generation ``q``, not ~1 from mixed ``q``."""
    if c_col is None or not math.isfinite(c_col) or c_col <= 0:
        return False
    return abs(float(c_col) - _IDENTITY_C_COL) > _IDENTITY_C_COL_ATOL


def c_col_from_q_usd_and_mwh(q_usd: float, mwh: float) -> float:
    from bedrock.transform.eeio.electricity_disaggregation import (  # noqa: PLC0415
        electricity_output_factor,
    )

    return float(electricity_output_factor(q_usd, mwh))


def fill_mixed_c_col(bundle: ImplBundle) -> None:
    """Recover monetary ``c_col`` for mixed units from 3-way dollar ``q`` / mixed MWh ``q``.

    Needed when ``c_col`` was computed after generation ``q`` was already
    converted to MWh (identity ~1).
    """
    mixed = bundle.steps.get('mixed_units')
    if mixed is None or not mixed.mixed:
        return
    if c_col_is_monetary(mixed.c_col):
        return
    three = bundle.steps.get('three_way')
    if (
        three is None
        or three.q is None
        or mixed.q is None
        or GENERATION_SECTOR not in three.q.index
        or GENERATION_SECTOR not in mixed.q.index
    ):
        return
    q_usd = float(three.q.loc[GENERATION_SECTOR])
    mwh = float(mixed.q.loc[GENERATION_SECTOR])
    if q_usd <= 0 or mwh <= 0 or q_usd < mwh * _Q_USD_OVER_MWH_MIN:
        return
    mixed.c_col = c_col_from_q_usd_and_mwh(q_usd, mwh)


def ef_kg_per_usd(
    ef: pd.Series,
    *,
    mixed: bool,
    c_col: float | None,
) -> pd.Series:
    """Native D/N → kg/USD. Mixed-units generation is kg/MWh × monetary ``c_col``."""
    out = ef.astype(float).copy()
    out.index = out.index.map(str)
    if mixed and c_col_is_monetary(c_col) and GENERATION_SECTOR in out.index:
        out.loc[GENERATION_SECTOR] = float(out.loc[GENERATION_SECTOR]) * float(c_col)
    return out


def _usd_weights(step: StepSnapshot) -> pd.Series | None:
    raw = step.x if step.x is not None else step.q
    if raw is None:
        return None
    w = raw.astype(float).copy()
    w.index = w.index.map(str)
    if step.mixed and c_col_is_monetary(step.c_col) and GENERATION_SECTOR in w.index:
        w.loc[GENERATION_SECTOR] = float(w.loc[GENERATION_SECTOR]) / float(step.c_col)
    return w


def star_aggregate(step: StepSnapshot, kind: str) -> float | None:
    """``221100*``: x-weighted (else q-weighted) mean of child kg/USD EFs."""
    src = step.d if kind == 'D' else step.n
    if src is None:
        return None
    usd = ef_kg_per_usd(src, mixed=step.mixed, c_col=step.c_col)
    weights = _usd_weights(step)
    if weights is None:
        present = [s for s in CHILD_SECTORS if s in usd.index]
        if not present:
            return None
        return float(usd.reindex(present).astype(float).mean())
    num = 0.0
    den = 0.0
    for sector in CHILD_SECTORS:
        if sector not in usd.index:
            continue
        w = float(weights.get(sector, 0.0) or 0.0)
        num += float(usd.loc[sector]) * w
        den += w
    if den == 0.0:
        return None
    return num / den


def sector_ef_usd(step: StepSnapshot, kind: str, sector: str) -> float | None:
    if sector == STAR_SECTOR:
        return star_aggregate(step, kind)
    src = step.d if kind == 'D' else step.n
    if src is None:
        return None
    usd = ef_kg_per_usd(src, mixed=step.mixed, c_col=step.c_col)
    if sector not in usd.index:
        return None
    return float(usd.loc[sector])


def values_match(left: float | None, right: float | None) -> bool:
    if left is None or right is None:
        return False
    return abs(left - right) <= SAME_ATOL


def format_ef(value: float | None) -> str:
    if value is None:
        return MISSING
    if abs(value) < 1e-12:
        return '0'
    return f'{value:.{EF_DECIMALS}f}'


def format_twh(mwh: float) -> str:
    return f'{mwh / 1e6:,.1f} TWh'


def format_usd(value: float | None) -> str:
    if value is None or not math.isfinite(value):
        return MISSING
    return f'{value / 1e9:,.2f} $B'


def format_ratio(model: float, target: float) -> str:
    if target == 0.0:
        return MISSING
    return f'{model / target:.3f}'


def grouped_mwh(class_mwh: pd.Series, members: tuple[str, ...]) -> float:
    total = 0.0
    for name in members:
        if name in class_mwh.index:
            total += float(class_mwh.loc[name])
    return total


def class_total_mwh(class_mwh: pd.Series) -> float:
    return grouped_mwh(class_mwh, CLASS_ORDER)
