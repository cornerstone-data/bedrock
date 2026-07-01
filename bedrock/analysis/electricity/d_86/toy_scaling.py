"""Toy year-scaling and inflation mirroring ``derive_cornerstone_Aq_scaled``."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from bedrock.transform.eeio.electricity_disaggregation import GENERATION_SECTOR
from bedrock.utils.math.formulas import compute_total_industry_inputs

TOY_DETAIL_YEAR = 2017
TOY_MODEL_YEAR = 2023


@dataclass(frozen=True)
class ToyScaledInflatedAq:
    """Intermediate A/q after summary scaling and commodity-PI inflation."""

    detail_year: int
    model_year: int
    a_detail: pd.DataFrame
    q_detail: pd.Series[float]
    a_scale_ratio: pd.DataFrame
    q_scale_ratio: pd.Series[float]
    a_scaled: pd.DataFrame
    q_scaled: pd.Series[float]
    commodity_pi: pd.Series[float]
    a_target: pd.DataFrame
    q_target: pd.Series[float]


def toy_summary_q_ratio() -> pd.Series[float]:
    """Toy summary q_target / q_base (2017 USD, dollar-year-adjusted)."""
    return pd.Series(
        {GENERATION_SECTOR: 1.12, 'C1': 1.05, 'C2': 1.03},
        dtype=float,
    )


def toy_summary_a_dom_ratio() -> pd.DataFrame:
    """Toy element-wise summary Adom_target / Adom_base ratios (2017 USD)."""
    codes = [GENERATION_SECTOR, 'C1', 'C2']
    ratios = pd.DataFrame(1.0, index=codes, columns=codes)
    ratios.loc[GENERATION_SECTOR, GENERATION_SECTOR] = 1.08
    ratios.loc[GENERATION_SECTOR, 'C1'] = 1.10
    ratios.loc[GENERATION_SECTOR, 'C2'] = 1.06
    ratios.loc['C1', GENERATION_SECTOR] = 1.04
    ratios.loc['C2', GENERATION_SECTOR] = 1.03
    return ratios


def toy_summary_a_imp_ratio() -> pd.DataFrame:
    """Toy summary Aimp ratios (imports grow faster than domestic on gen row)."""
    ratios = toy_summary_a_dom_ratio().copy()
    ratios.loc[GENERATION_SECTOR, 'C1'] = 1.15
    ratios.loc[GENERATION_SECTOR, 'C2'] = 1.12
    return ratios


def toy_commodity_price_ratio() -> pd.Series[float]:
    """Toy V-norm-style commodity PI: detail_year → model_year."""
    return pd.Series(
        {GENERATION_SECTOR: 1.25, 'C1': 1.10, 'C2': 1.08},
        dtype=float,
    )


def scale_toy_a(
    a: pd.DataFrame,
    ratio: pd.DataFrame,
) -> pd.DataFrame:
    """Element-wise scale as ``scale_cornerstone_A`` on a 1:1 detail↔summary toy."""
    aligned = ratio.reindex(index=a.index, columns=a.columns, fill_value=1.0)
    scaled = a * aligned
    total_inputs = compute_total_industry_inputs(A=scaled)
    for col in total_inputs[total_inputs > 1].index:
        scaled[col] *= 0.98 / float(total_inputs[col])
    return scaled


def scale_toy_q(q: pd.Series[float], ratio: pd.Series[float]) -> pd.Series[float]:
    """Element-wise scale as ``scale_cornerstone_q`` on the toy axis."""
    return q * ratio.reindex(q.index, fill_value=1.0)


def inflate_toy_a_commodity_pi(
    a: pd.DataFrame,
    commodity_pi: pd.Series[float],
) -> pd.DataFrame:
    """``diag(p) @ A @ diag(1/p)`` as in ``inflate_cornerstone_A_matrix_with_commodity_pi``."""
    p = commodity_pi.reindex(a.index, fill_value=1.0).astype(float)
    p_vals = p.to_numpy()
    out = np.diag(p_vals) @ a.to_numpy() @ np.diag(1.0 / p_vals)
    return pd.DataFrame(out, index=a.index, columns=a.columns)


def inflate_toy_q(
    q: pd.Series[float],
    commodity_pi: pd.Series[float],
) -> pd.Series[float]:
    """``q * p`` as in ``inflate_cornerstone_q_or_y_with_commodity_pi``."""
    return q * commodity_pi.reindex(q.index, fill_value=1.0)


def toy_scale_and_inflate_aq(
    a_detail: pd.DataFrame,
    q_detail: pd.Series[float],
    *,
    a_scale_ratio: pd.DataFrame | None = None,
    detail_year: int = TOY_DETAIL_YEAR,
    model_year: int = TOY_MODEL_YEAR,
) -> ToyScaledInflatedAq:
    """Apply toy summary scaling then commodity-PI inflation (CEDA-style path)."""
    a_ratio = (
        toy_summary_a_dom_ratio()
        if a_scale_ratio is None
        else a_scale_ratio.reindex(
            index=a_detail.index, columns=a_detail.columns, fill_value=1.0
        )
    )
    q_ratio = toy_summary_q_ratio()
    pi = toy_commodity_price_ratio()

    a_scaled = scale_toy_a(a_detail, a_ratio)
    q_scaled = scale_toy_q(q_detail, q_ratio)
    a_target = inflate_toy_a_commodity_pi(a_scaled, pi)
    q_target = inflate_toy_q(q_scaled, pi)

    return ToyScaledInflatedAq(
        detail_year=detail_year,
        model_year=model_year,
        a_detail=a_detail,
        q_detail=q_detail,
        a_scale_ratio=a_ratio,
        q_scale_ratio=q_ratio,
        a_scaled=a_scaled,
        q_scaled=q_scaled,
        commodity_pi=pi,
        a_target=a_target,
        q_target=q_target,
    )


def rebuild_diagonal_flows(
    a: pd.DataFrame,
    q: pd.Series[float],
    y: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Rebuild diagonal-Make ``V``, ``U``, ``Y`` consistent with ``A`` and ``q``."""
    codes = list(a.index)
    v = pd.DataFrame(0.0, index=codes, columns=codes)
    for code in codes:
        v.loc[code, code] = float(q[code])
    u = a.multiply(q, axis=1)
    y_out = y.reindex(index=codes).copy()
    return v, u, y_out
