"""Mixed-unit conversion on toy flow tables (same rules as production ``A`` transform)."""

from __future__ import annotations

import pandas as pd

from bedrock.transform.eeio.electricity_disaggregation import (
    GENERATION_SECTOR,
    apply_electricity_unit_conversion_to_A,
    apply_electricity_unit_conversion_to_B,
    apply_electricity_unit_conversion_to_q,
    electricity_class_row_factors,
    electricity_output_factor,
)


def compute_toy_conversion_factors(
    *,
    a: pd.DataFrame,
    q: pd.Series[float],
    y_row: pd.Series[float],
    mwh_221110: float,
    prices_by_class: dict[str, float],
    end_use_map: dict[str, str],
    generation_sector: str = GENERATION_SECTOR,
) -> tuple[float, pd.Series[float]]:
    """Return ``(c_col, c_row)`` using production conversion helpers."""
    q_usd = float(q[generation_sector])
    c_col = electricity_output_factor(q_usd, mwh_221110)
    adom_row = a.loc[generation_sector]
    c_row = electricity_class_row_factors(
        adom_row,
        q,
        y_row,
        prices_by_class,
        end_use_map,
        mwh_221110,
    )
    return c_col, c_row


def apply_mixed_conversion_to_flows(
    *,
    v: pd.DataFrame,
    u: pd.DataFrame,
    y: pd.DataFrame,
    c_col: float,
    c_row: pd.Series[float],
    generation_sector: str = GENERATION_SECTOR,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Scale generation-sector flows so that re-derived ``A`` matches the
    production ``apply_electricity_unit_conversion_to_A`` transform.

    Rules (diagonal Make, domestic Use):
      - ``V[gen, gen] *= c_col``
      - ``U[gen, j] *= c_j`` for every industry column ``j``
      - ``U[i, gen]`` unchanged for ``i != gen`` (input purchases stay in $)
      - ``Y[gen, fd] *= c_j`` where ``c_j`` is keyed by final-demand column
    """
    v_m = v.copy()
    u_m = u.copy()
    y_m = y.copy()
    gen = generation_sector

    v_m.loc[gen, gen] = float(v_m.loc[gen, gen]) * c_col

    for col in u_m.columns:
        c_j = float(c_row.get(col, c_col))
        u_m.loc[gen, col] = float(u_m.loc[gen, col]) * c_j

    for col in y_m.columns:
        c_j = float(c_row.get(col, c_col))
        y_m.loc[gen, col] = float(y_m.loc[gen, col]) * c_j

    return v_m, u_m, y_m


def apply_direct_mixed_transform(
    *,
    a: pd.DataFrame,
    q: pd.Series[float],
    b: pd.DataFrame,
    c_col: float,
    c_row: pd.Series[float],
    generation_sector: str = GENERATION_SECTOR,
) -> tuple[pd.DataFrame, pd.Series[float], pd.DataFrame]:
    """Production path: transform ``A``, ``q``, and ``B`` directly."""
    a_m = apply_electricity_unit_conversion_to_A(
        a,
        c_col=c_col,
        c_row=c_row,
        generation_sector=generation_sector,
    )
    q_m = apply_electricity_unit_conversion_to_q(
        q,
        c_col,
        generation_sector=generation_sector,
    )
    b_m = apply_electricity_unit_conversion_to_B(
        b,
        c_col,
        generation_sector=generation_sector,
    )
    return a_m, q_m, b_m


def default_toy_mwh(q_gen_usd: float) -> float:
    """Anchor generation output at 10 MWh per $1 (toy scale)."""
    return q_gen_usd * 0.01
