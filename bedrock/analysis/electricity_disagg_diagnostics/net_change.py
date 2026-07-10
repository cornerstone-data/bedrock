"""Chained net change in total U.S. BLy across electricity disagg steps."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import pandas as pd

from bedrock.analysis.electricity_disagg_diagnostics.bly import align_bly_pair
from bedrock.analysis.electricity_disagg_diagnostics.dispersion import DISPERSION_TOL

BarKind = Literal['level', 'delta']


@dataclass(frozen=True)
class NetChangeBar:
    """One column in the net-change waterfall."""

    label: str
    kind: BarKind
    value: float
    bottom: float = 0.0
    signed_delta: float | None = None


@dataclass(frozen=True)
class ChainedNetChange:
    footing_label: str
    step_labels: list[str]
    stage_totals_mmt: list[float]
    step_deltas_mmt: list[float]
    footing_total_mmt: float

    @property
    def final_total_mmt(self) -> float:
        return self.stage_totals_mmt[-1]

    @property
    def combined_delta_mmt(self) -> float:
        return self.final_total_mmt - self.stage_totals_mmt[0]

    @property
    def combined_delta_pct(self) -> float:
        if self.footing_total_mmt == 0:
            return 0.0
        return 100 * self.combined_delta_mmt / self.footing_total_mmt

    def _scale(self, mmt: float) -> float:
        if self.footing_total_mmt == 0:
            return 0.0
        return 100 * mmt / self.footing_total_mmt

    def bar_value(self, bar: NetChangeBar, *, use_pct: bool) -> float:
        return self._scale(bar.value) if use_pct else bar.value

    def bar_bottom(self, bar: NetChangeBar, *, use_pct: bool) -> float:
        return self._scale(bar.bottom) if use_pct else bar.bottom

    def build_bars(self) -> list[NetChangeBar]:
        """Level bar per stage; delta bar only when total BLy moves between steps."""
        bars: list[NetChangeBar] = [
            NetChangeBar(
                label=self._level_label(self.footing_label),
                kind='level',
                value=self.stage_totals_mmt[0],
            )
        ]
        running = self.stage_totals_mmt[0]
        for step_label, delta, total in zip(
            self.step_labels,
            self.step_deltas_mmt,
            self.stage_totals_mmt[1:],
            strict=True,
        ):
            prev = running
            bars.append(
                NetChangeBar(
                    label=self._level_label(step_label),
                    kind='level',
                    value=total,
                )
            )
            if abs(delta) > DISPERSION_TOL:
                short = self._short_step_label(step_label)
                bars.append(
                    NetChangeBar(
                        label=f'BLy change due to\n{short}',
                        kind='delta',
                        value=abs(delta),
                        bottom=min(prev, total),
                        signed_delta=delta,
                    )
                )
            running = total
        return bars

    @staticmethod
    def _short_step_label(label: str) -> str:
        return ' '.join(label.split())

    @staticmethod
    def _level_label(label: str) -> str:
        return ' '.join(label.split())


def pairwise_net_change(older: pd.Series[float], newer: pd.Series[float]) -> float:
    a, b = align_bly_pair(older, newer)
    return float(b.sum() - a.sum())


def compute_chained_net_change(
    footing: pd.Series[float],
    step_series: list[pd.Series[float]],
    step_labels: list[str],
    *,
    footing_label: str,
) -> ChainedNetChange:
    if len(step_series) != len(step_labels):
        raise ValueError('step_series and step_labels must have the same length')
    chain = [footing, *step_series]
    stage_totals = [float(s.sum()) for s in chain]
    step_deltas = [
        pairwise_net_change(chain[i], chain[i + 1]) for i in range(len(step_series))
    ]
    return ChainedNetChange(
        footing_label=footing_label,
        step_labels=step_labels,
        stage_totals_mmt=stage_totals,
        step_deltas_mmt=step_deltas,
        footing_total_mmt=float(footing.sum()),
    )
