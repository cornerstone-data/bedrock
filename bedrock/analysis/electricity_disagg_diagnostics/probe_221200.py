"""Explain the 221200 (natural gas distribution) direct-EF drop.

The gas-distribution *industry*'s satellite emissions barely change; what
falls is the emissions *attributed to the 221200 commodity*. Commodity direct
factors are built as ``B_commodity = (E_industry / x_industry) @ Vnorm`` — a
Make-share-weighted average of the producing industries' intensities. Before
reallocation the electricity industry co-produces ~10% of the gas-distribution
commodity, so the commodity inherits a large slug of power-sector intensity.
Co-production reallocation zeroes that Make off-diagonal, so the commodity
stops inheriting electricity intensity and its ``D`` collapses.

Reports (a) the Make transfers, (b) the footing commodity-intensity
decomposition by producing industry, and (c) ``D``, commodity output ``q``,
and attributed ``E = D·q`` across footing / reallocation / 3-way split.

Run:
    python -m bedrock.analysis.electricity_disagg_diagnostics.probe_221200
"""

from __future__ import annotations

import pandas as pd

from bedrock.analysis.electricity_disagg_diagnostics.full_trace import (
    _clear_model_caches,
)
from bedrock.publish.model_objects import get_D, get_V, get_x
from bedrock.transform.eeio.cornerstone_disagg_pipeline import (
    derive_cornerstone_U_after_waste,
    derive_cornerstone_V_after_waste,
    derive_cornerstone_VA_after_waste,
)
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_Vnorm_scrap_corrected,
    derive_E_usa,
)
from bedrock.transform.eeio.electricity_disaggregation import (
    build_coproduction_transfer_schedule,
)
from bedrock.utils.config.usa_config import reset_usa_config, set_global_usa_config
from bedrock.utils.math.formulas import compute_q

GAS = "221200"
ELEC = "221100"
CONFIGS = [
    ("footing", "2025_usa_cornerstone_v0_2"),
    ("reallocation", "2025_usa_cornerstone_v0_2_electricity_reallocation"),
    ("3-way split", "2025_usa_cornerstone_v0_2_electricity_disaggregation"),
]


def _load(cfg: str) -> None:
    reset_usa_config()
    _clear_model_caches()
    set_global_usa_config(cfg)


def _d_series() -> pd.Series:
    s = get_D().sum(axis=0)
    s.index = s.index.astype(str)
    return s.astype(float)


def _q_series() -> pd.Series:
    q = compute_q(V=get_V())
    q.index = q.index.astype(str)
    return q.astype(float)


def report_make_and_intensity() -> None:
    """Footing Make transfers and commodity-intensity decomposition."""
    _load(CONFIGS[1][1])  # reallocation config exposes the pre-realloc checkpoint
    v0 = derive_cornerstone_V_after_waste()
    _ = derive_cornerstone_U_after_waste()
    _ = derive_cornerstone_VA_after_waste()
    schedule = build_coproduction_transfer_schedule(v0)
    out = sum(t.amount for t in schedule if t.source == ELEC and t.target == GAS)
    inb = sum(t.amount for t in schedule if t.source == GAS and t.target == ELEC)
    print("=== Co-production transfers touching 221200 ===")
    print(f"  outbound 221100 -> 221200 : ${out / 1e9:.3f} B (secondary gas output)")
    print(f"  inbound  221200 -> 221100 : ${inb / 1e9:.3f} B (secondary electricity)")

    _load(CONFIGS[0][1])  # footing
    E = derive_E_usa()
    E.columns = E.columns.astype(str)
    x = get_x()
    x.index = x.index.astype(str)
    vnorm = derive_cornerstone_Vnorm_scrap_corrected()
    vnorm.index = vnorm.index.astype(str)
    vnorm.columns = vnorm.columns.astype(str)

    producers = vnorm[GAS][vnorm[GAS].abs() > 1e-6].sort_values(ascending=False)
    bi_gas = float(E[GAS].sum()) / float(x[GAS])
    print("\n=== Footing: who produces the 221200 commodity, and its intensity ===")
    print(
        f"{'industry':<10}{'Make share':>12}{'Bi (kg/USD)':>14}"
        f"{'Bi rel. gas':>14}{'contrib':>10}{'% of D':>9}"
    )
    contribs: dict[str, float] = {}
    for ind in producers.index:
        share = float(producers[ind])
        bi = float(E[ind].sum()) / float(x[ind]) if ind in x.index else 0.0
        contribs[ind] = bi * share
    total = sum(contribs.values())
    for ind in producers.index:
        share = float(producers[ind])
        bi = float(E[ind].sum()) / float(x[ind]) if ind in x.index else 0.0
        print(
            f"{ind:<10}{share:>12.2%}{bi:>14.4f}{bi / bi_gas:>13.1f}x"
            f"{contribs[ind]:>10.4f}{contribs[ind] / total:>9.1%}"
        )


def report_d_table() -> None:
    print("\n=== 221200 commodity D, output q, attributed E = D*q ===")
    print(f"{'step':<14}{'D (kg/USD)':>14}{'q (USD)':>18}{'E=D*q (kg)':>18}{'%dD':>10}")
    base_d = None
    for label, cfg in CONFIGS:
        _load(cfg)
        d = _d_series()
        q = _q_series()
        d_s = float(d[GAS])
        q_s = float(q[GAS])
        if base_d is None:
            base_d = d_s
        print(
            f"{label:<14}{d_s:>14.4f}{q_s:>18,.0f}"
            f"{d_s * q_s:>18,.0f}{(d_s / base_d - 1):>10.2%}"
        )


def main() -> None:
    report_make_and_intensity()
    report_d_table()


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.WARNING, format="%(message)s")
    main()
