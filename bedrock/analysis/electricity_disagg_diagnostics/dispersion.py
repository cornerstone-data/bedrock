"""Chained incremental BLy dispersion metrics."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from bedrock.analysis.electricity_disagg_diagnostics.bly import align_bly_pair

DISPERSION_TOL = 1e-4  # MMT CO2e


@dataclass(frozen=True)
class ChainedDispersion:
    step_labels: list[str]
    step_values_mmt: list[float]
    combined_mmt: float
    offset_mmt: float
    footing_total_mmt: float

    @property
    def step_values_pct(self) -> list[float]:
        if self.footing_total_mmt == 0:
            return [0.0] * len(self.step_values_mmt)
        return [100 * v / self.footing_total_mmt for v in self.step_values_mmt]

    @property
    def offset_pct(self) -> float:
        if self.footing_total_mmt == 0:
            return 0.0
        return 100 * self.offset_mmt / self.footing_total_mmt

    @property
    def combined_pct(self) -> float:
        if self.footing_total_mmt == 0:
            return 0.0
        return 100 * self.combined_mmt / self.footing_total_mmt

    @property
    def show_offsetting_bar(self) -> bool:
        return self.offset_mmt > DISPERSION_TOL


def pairwise_dispersion(older: pd.Series[float], newer: pd.Series[float]) -> float:
    a, b = align_bly_pair(older, newer)
    return float((b - a).abs().sum())


def compute_chained_dispersion(
    footing: pd.Series[float],
    step_series: list[pd.Series[float]],
    step_labels: list[str],
) -> ChainedDispersion:
    if len(step_series) != len(step_labels):
        raise ValueError('step_series and step_labels must have the same length')
    chain = [footing, *step_series]
    step_values = [
        pairwise_dispersion(chain[i], chain[i + 1]) for i in range(len(step_series))
    ]
    combined = pairwise_dispersion(footing, step_series[-1])
    offset = sum(step_values) - combined
    footing_total = float(footing.sum())
    if combined > sum(step_values) + DISPERSION_TOL:
        raise ValueError(
            f'combined dispersion {combined} exceeds sum(steps) {sum(step_values)}'
        )
    return ChainedDispersion(
        step_labels=step_labels,
        step_values_mmt=step_values,
        combined_mmt=combined,
        offset_mmt=offset,
        footing_total_mmt=footing_total,
    )
