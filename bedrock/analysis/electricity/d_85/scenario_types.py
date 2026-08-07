"""Typed containers for d_85 disaggregation scenarios."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass
class ScenarioWeights:
    """Per-step weight bundle for step-wise PR3 orchestration."""

    w_make_intersection: pd.Series[float]
    w_use_intersection: pd.Series[float] | None
    w_column_steps: pd.Series[float]
    w_row_uniform: pd.Series[float] | None
    w_row_by_column: pd.DataFrame | None
    intersection_3x3: pd.DataFrame | None


@dataclass
class DisaggScenarioResult:
    """407-sector IO checkpoint after scenario-specific disaggregation."""

    name: str
    weights: ScenarioWeights
    V: pd.DataFrame
    Udom: pd.DataFrame
    Uimp: pd.DataFrame
    VA: pd.DataFrame
    Y: pd.DataFrame
    q: pd.Series[float]
    x: pd.Series[float]
    metrics_only: bool = False
