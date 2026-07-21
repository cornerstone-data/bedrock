"""Decompose D/N change between reallocation and 3-way split."""

from __future__ import annotations

from typing import Any, cast

import pandas as pd

from bedrock.analysis.electricity_disagg_diagnostics.full_trace import (
    _clear_model_caches,
    _scalar_float,
    _sector_q_usd,
    _weighted_ef,
)
from bedrock.publish.model_objects import get_B, get_D, get_L, get_N, get_q
from bedrock.transform.eeio.cornerstone_disagg_pipeline import (
    electricity_conversion_factors,
    electricity_mixed_units_enabled,
)
from bedrock.transform.eeio.derived import derive_E_usa
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_Aq_mixed_units,
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_Vnorm_scrap_corrected,
    derive_cornerstone_x_after_redefinition,
)
from bedrock.transform.eeio.electricity_disaggregation import (
    GENERATION_SECTOR,
    get_electricity_commodity_row_weights,
)
from bedrock.utils.config.usa_config import (
    get_usa_config,
    reset_usa_config,
    set_global_usa_config,
)
from bedrock.utils.math.formulas import (
    backcompute_y_from_A_and_q,
    compute_d,
    compute_L_matrix,
    compute_M_matrix,
    compute_n,
)
from bedrock.utils.schemas.cornerstone_schemas import (
    ELECTRICITY_AGGREGATE_SECTOR,
    ELECTRICITY_DISAGG_SECTORS,
)
from bedrock.utils.validation.calculate_national_accounting_balance_diagnostics import (
    _compute_bly_series,
)


def _analyze(config: str, label: str, sectors: list[str]) -> dict[str, Any]:
    reset_usa_config()
    _clear_model_caches()
    set_global_usa_config(config)

    E = derive_E_usa()
    x = derive_cornerstone_x_after_redefinition()
    Vnorm = derive_cornerstone_Vnorm_scrap_corrected()
    B = get_B()
    L = get_L()
    D_vec = compute_d(B=B)
    M = compute_M_matrix(B=B, L=L)
    N_vec = compute_n(M=M)
    D_pub = get_D()
    N_pub = get_N()
    aq_monetary = derive_cornerstone_Aq_scaled()
    monetary_q = aq_monetary.scaled_q
    mixed = electricity_mixed_units_enabled()
    c_col: float | None = None
    if mixed:
        c_col, _ = electricity_conversion_factors(aq_monetary)
        aq = derive_cornerstone_Aq_mixed_units()
    else:
        aq = aq_monetary
    q = get_q()
    y = backcompute_y_from_A_and_q(A=aq.Adom, q=q)
    L_dom = compute_L_matrix(A=aq.Adom)
    ly = L_dom @ y
    bly_vec = _compute_bly_series(B=B, Adom=aq.Adom, y=y)

    rows = []
    q_usd_sum = 0.0
    d_num = 0.0
    n_num = 0.0
    for s in sectors:
        if s not in E.columns:
            continue
        e_s = _scalar_float(E[s].sum())
        x_s = _scalar_float(x[s])
        q_usd = _sector_q_usd(s, q, monetary_q)
        q_rep = _scalar_float(q[s]) if s in q.index else 0.0
        bi = e_s / x_s if x_s else 0.0
        vnorm_diag = (
            _scalar_float(Vnorm.at[s, s])
            if s in Vnorm.index and s in Vnorm.columns
            else float("nan")
        )
        b_sum = _scalar_float(B[s].sum()) if s in B.columns else float("nan")
        d_s = (
            _scalar_float(D_pub[s].squeeze())
            if s in D_pub.columns
            else _scalar_float(D_vec[s])
        )
        n_s = (
            _scalar_float(N_pub[s].squeeze())
            if s in N_pub.columns
            else _scalar_float(N_vec[s])
        )
        # USD-comparable intensities for mixed generation (kg/MWh → kg/USD).
        d_usd = (
            d_s * float(c_col) if mixed and s == GENERATION_SECTOR and c_col else d_s
        )
        n_usd = (
            n_s * float(c_col) if mixed and s == GENERATION_SECTOR and c_col else n_s
        )
        l_diag = _scalar_float(L.at[s, s]) if s in L.index else float("nan")
        ldom_diag = _scalar_float(L_dom.at[s, s]) if s in L_dom.index else float("nan")
        y_s = _scalar_float(y[s]) if s in y.index else 0.0
        ly_s = _scalar_float(ly[s]) if s in ly.index else 0.0
        bly_s = _scalar_float(bly_vec[s]) if s in bly_vec.index else 0.0
        bly_check = d_s * ly_s
        q_usd_sum += q_usd
        d_num += d_usd * q_usd
        n_num += n_usd * q_usd
        rows.append(
            {
                "sector": s,
                "E_Mt": e_s / 1e9,
                "x_B": x_s / 1e9,
                "q_usd_B": q_usd / 1e9,
                "q_rep_B": q_rep / 1e9,
                "E_over_x": bi,
                "Vnorm_diag": vnorm_diag,
                "sum_B_D": b_sum,
                "D": d_s,
                "D_usd": d_usd,
                "L_diag": l_diag,
                "Ldom_diag": ldom_diag,
                "N": n_s,
                "N_usd": n_usd,
                "N_over_D": n_s / d_s if d_s else float("nan"),
                "y_nab_B": y_s / 1e9,
                "Ldom_y_B": ly_s / 1e9,
                "BLy_Mt": bly_s / 1e9,
                "D_times_Ldom_y_Mt": bly_check / 1e9,
                "mixed_gen": mixed and s == GENERATION_SECTOR,
            }
        )

    return {
        "label": label,
        "config": config,
        "sectors": sectors,
        "mixed": mixed,
        "c_col": c_col,
        "rows": rows,
        "E_total_Mt": sum(r["E_Mt"] for r in rows),
        "q_usd_total_B": q_usd_sum / 1e9,
        "y_nab_total_B": sum(r["y_nab_B"] for r in rows),
        "BLy_total_Mt": sum(r["BLy_Mt"] for r in rows),
        "D_weighted": d_num / q_usd_sum if q_usd_sum else 0.0,
        "N_weighted": n_num / q_usd_sum if q_usd_sum else 0.0,
        "D_weighted_fn": _weighted_ef(
            D_pub, q, monetary_q, sectors, c_col=c_col, mixed=mixed
        ),
        "N_weighted_fn": _weighted_ef(
            N_pub, q, monetary_q, sectors, c_col=c_col, mixed=mixed
        ),
    }


def _analyze_y_nab_block(
    config: str,
    sectors: list[str],
    *,
    mixed: bool = False,
) -> dict[str, Any]:
    """Row-balance components for y_nab = q - rowsum(Adom * q) over electricity block."""
    reset_usa_config()
    _clear_model_caches()
    set_global_usa_config(config)

    aq = (
        derive_cornerstone_Aq_mixed_units() if mixed else derive_cornerstone_Aq_scaled()
    )
    q = aq.scaled_q
    adom = aq.Adom
    adom_q = adom.multiply(q, axis=1).sum(axis=1)
    y = backcompute_y_from_A_and_q(A=adom, q=q)

    rows = []
    for s in sectors:
        if s not in q.index:
            continue
        rows.append(
            {
                "sector": s,
                "q_B": float(q[s]) / 1e9,
                "adom_q_B": float(adom_q[s]) / 1e9,
                "y_nab_B": float(y[s]) / 1e9,
                "mixed": mixed and s == ELECTRICITY_DISAGG_SECTORS[0],
            }
        )

    return {
        "config": config,
        "mixed": mixed,
        "sectors": sectors,
        "rows": rows,
        "q_total_B": sum(r["q_B"] for r in rows),
        "adom_q_total_B": sum(r["adom_q_B"] for r in rows),
        "y_nab_total_B": sum(r["y_nab_B"] for r in rows),
    }


def _fmt_flow_b(value: float, *, mixed: bool) -> str:
    if mixed:
        return f"{value:.2f} B (mixed units)"
    return f"${value:.2f} B"


def render_y_nab_section_md(
    realloc: dict[str, Any],
    split_block: dict[str, Any],
    mixed_block: dict[str, Any],
) -> str:
    y_agg = realloc["y_nab_total_B"]
    w_row = get_electricity_commodity_row_weights()
    naive_rows = [
        {
            "sector": s,
            "w_row": float(w_row[s]),
            "naive_B": y_agg * float(w_row[s]),
            "actual_B": next(
                r["y_nab_B"] for r in split_block["rows"] if r["sector"] == s
            ),
        }
        for s in ELECTRICITY_DISAGG_SECTORS
    ]

    realloc_adom_q = realloc["q_usd_total_B"] - realloc["y_nab_total_B"]
    delta_q = split_block["q_total_B"] - realloc["q_usd_total_B"]
    delta_adom_q = split_block["adom_q_total_B"] - realloc_adom_q
    delta_y = split_block["y_nab_total_B"] - realloc["y_nab_total_B"]

    mixed_110 = next(
        r for r in mixed_block["rows"] if r["sector"] == ELECTRICITY_DISAGG_SECTORS[0]
    )
    split_110 = next(
        r for r in split_block["rows"] if r["sector"] == ELECTRICITY_DISAGG_SECTORS[0]
    )

    lines = [
        "",
        "---",
        "",
        "## Walkthrough: y_nab block changes (reallocation → unit conversion)",
        "",
        "Block **y_nab** in the summary table is the sum of `y_nab` over electricity sector(s): "
        "**221100** before PR3, **221110 + 221121 + 221122** after. Values are **not** obtained by "
        "splitting aggregate `y_nab[221100]`; each step recomputes from the domestic IO balance:",
        "",
        "```",
        "y_nab_i = q_i - sum_j(Adom_ij * q_j)   # backcompute_y_from_A_and_q(Adom, q)",
        "```",
        "",
        f"### Reallocation → 3-way split: ${realloc['y_nab_total_B']:.2f}B → "
        f"${split_block['y_nab_total_B']:.2f}B (−${-delta_y:.2f}B)",
        "",
        "| | sum(q) | sum(Adom·q) | sum(y_nab) |",
        "|---|---:|---:|---:|",
        f"| reallocation (221100) | {_fmt_flow_b(realloc['q_usd_total_B'], mixed=False)} | "
        f"{_fmt_flow_b(realloc_adom_q, mixed=False)} | "
        f"**{_fmt_flow_b(realloc['y_nab_total_B'], mixed=False)}** |",
        f"| 3-way split (3 sectors) | {_fmt_flow_b(split_block['q_total_B'], mixed=False)} | "
        f"{_fmt_flow_b(split_block['adom_q_total_B'], mixed=False)} | "
        f"**{_fmt_flow_b(split_block['y_nab_total_B'], mixed=False)}** |",
        "",
        "The drop decomposes as:",
        "",
        "```",
        f"Δy_nab = Δq - Δ(Adom·q) = ({delta_q:+.2f}B) - ({delta_adom_q:+.2f}B) = {delta_y:+.2f}B",
        "```",
        "",
        "PR3 disaggregates V/U/VA (not a proportional carve of aggregate `y_nab`). That raises "
        "domestic intermediate flows in `Adom` (block mean A diagonal ~0.071 → ~0.154) and "
        "slightly lowers electricity `q` ($595.09B → $593.81B). More of each child's `q` is "
        "explained by domestic purchases (`Adom·q`), so less remains as `y_nab`.",
        "",
        "Per-sector backcompute at 3-way split:",
        "",
        "| Sector | q | Adom·q (row) | y_nab |",
        "|--------|---:|---:|---:|",
    ]
    for r in split_block["rows"]:
        lines.append(
            f"| {r['sector']} | {_fmt_flow_b(r['q_B'], mixed=False)} | "
            f"{_fmt_flow_b(r['adom_q_B'], mixed=False)} | **{_fmt_flow_b(r['y_nab_B'], mixed=False)}** |"
        )
    lines.append(
        f"| **Sum** | **{_fmt_flow_b(split_block['q_total_B'], mixed=False)}** | "
        f"**{_fmt_flow_b(split_block['adom_q_total_B'], mixed=False)}** | "
        f"**{_fmt_flow_b(split_block['y_nab_total_B'], mixed=False)}** |"
    )
    mixed_delta = mixed_block["y_nab_total_B"] - split_block["y_nab_total_B"]
    lines.extend(
        [
            "",
            f"### 3-way split → unit conversion: ${split_block['y_nab_total_B']:.2f}B → "
            f"{mixed_block['y_nab_total_B']:.2f} B mixed ({mixed_delta:+.2f} B)",
            "",
            "PR4 only changes **221110** (generation → MWh). 221121 and 221122 are unchanged in USD:",
            "",
            "| Sector | y_nab (3-way, USD) | y_nab (mixed) | Δ |",
            "|--------|-------------------:|--------------:|--:|",
        ]
    )
    for r in split_block["rows"]:
        mixed_r = next(mr for mr in mixed_block["rows"] if mr["sector"] == r["sector"])
        delta = mixed_r["y_nab_B"] - r["y_nab_B"]
        if mixed_r["mixed"]:
            delta_str = f"{delta:+.2f} B (mixed units)"
        elif abs(delta) < 0.005:
            delta_str = "$0.00 B"
        else:
            delta_str = f"${delta:+.2f} B"
        lines.append(
            f"| {r['sector']} | {_fmt_flow_b(r['y_nab_B'], mixed=False)} | "
            f"{_fmt_flow_b(mixed_r['y_nab_B'], mixed=mixed_r['mixed'])} | {delta_str} |"
        )
    lines.extend(
        [
            "",
            "Under mixed units, `q_221110` goes from **$230.42B (USD)** to **4.19 B (mixed units, MWh)** "
            "and `Adom` is rescaled for the generation row/column. `y_nab` is backcomputed again on that "
            "mixed `A`/`q`. The summary-table block total **144.12 B (mixed units)** is **not comparable USD**; "
            "only 221121 and 221122 remain monetary. The ~60.5B drop is almost entirely **221110's `y_nab` "
            f"collapsing under MWh units** ({_fmt_flow_b(split_110['y_nab_B'], mixed=False)} → "
            f"{_fmt_flow_b(mixed_110['y_nab_B'], mixed=True)}).",
            "",
            "| | sum(q) | sum(Adom·q) | sum(y_nab) |",
            "|---|---:|---:|---:|",
            f"| 3-way split (USD) | {_fmt_flow_b(split_block['q_total_B'], mixed=False)} | "
            f"{_fmt_flow_b(split_block['adom_q_total_B'], mixed=False)} | "
            f"**{_fmt_flow_b(split_block['y_nab_total_B'], mixed=False)}** |",
            f"| unit conversion (mixed) | {_fmt_flow_b(mixed_block['q_total_B'], mixed=True)} | "
            f"{_fmt_flow_b(mixed_block['adom_q_total_B'], mixed=True)} | "
            f"**{_fmt_flow_b(mixed_block['y_nab_total_B'], mixed=True)}** |",
            "",
            "### y_nab is not split by w_row",
            "",
            "The **Y / use-commodity-row** split uses compensating weights **`w_row`** (from GO shares "
            "and Table 8.3 intersection). That splits commodity rows in **Y** and **U** — not `y_nab`. "
            "`y_nab` follows **`q − Adom·q`** on the disaggregated IO.",
            "",
            f"| Sector | w_row | naive (w_row × ${y_agg:.2f}B) | actual backcompute |",
            "|--------|------:|-------------------------:|-------------------:|",
        ]
    )
    for row in naive_rows:
        lines.append(
            f"| {row['sector']} | {row['w_row']:.4f} | ${row['naive_B']:.2f} B | "
            f"**${row['actual_B']:.2f} B** |"
        )
    lines.extend(
        [
            "",
            "**w_row** shares: 221110 **33.11%**, 221121 **3.58%**, 221122 **63.31%**.",
            "",
        ]
    )
    return "\n".join(lines)


def render_walkthrough_md(realloc: dict[str, Any], split: dict[str, Any]) -> str:
    lines = [
        "",
        "---",
        "",
        "## Walkthrough: reallocation to 3-way split (D, N, BLy)",
        "",
        "This section explains why **D** rises from "
        f"**{realloc['D_weighted']:.3f}** to **{split['D_weighted']:.3f} kg/USD** and **N** from "
        f"**{realloc['N_weighted']:.3f}** to **{split['N_weighted']:.3f} kg/USD**, and why **BLy** rises from "
        f"**{realloc['BLy_total_Mt']:,.0f}** to **{split['BLy_total_Mt']:,.0f} MtCO₂e**.",
        "",
        "### Formulas (production diagnostics path)",
        "",
        "| Step | Formula |",
        "|------|---------|",
        "| B_ind | `E / x` (industry gross output at GHG year) |",
        "| B | `B_ind @ Vnorm` |",
        "| D | `sum_g B[g, sector]` (kg CO₂e / USD commodity) |",
        "| A | `Adom + Aimp` (year-scaled) |",
        "| L | `(I - A)^-1` (total); `L_dom = (I - Adom)^-1` for BLy |",
        "| M | `B @ L` |",
        "| N | `sum_g M[g, sector]` |",
        "| y_nab | `backcompute_y_from_A_and_q(Adom, q)` |",
        "| **BLy** | **`diag(D) @ L_dom @ y_nab`** (per sector: `BLy_j = D_j * (L_dom @ y_nab)_j`) |",
        "",
        "Block **D** and **N** in the summary tables are q-weighted across electricity sectors:",
        "`sum(D_s * q_s) / sum(q_s)` and the same for N.",
        "",
        "### Reallocation — aggregate 221100",
        "",
        "| Quantity | Value |",
        "|----------|------:|",
    ]
    r0 = realloc["rows"][0]
    lines.extend(
        [
            f"| E | {r0['E_Mt']:.2f} MtCO₂e |",
            f"| x (B denominator) | ${r0['x_B']:.2f} B |",
            f"| q (scaled USD) | ${r0['q_usd_B']:.2f} B |",
            f"| D = E/x (Vnorm=1) | {r0['D']:.4f} kg/USD |",
            f"| L_dom diagonal | {r0['Ldom_diag']:.4f} |",
            f"| L_total diagonal | {r0['L_diag']:.4f} |",
            f"| N | {r0['N']:.4f} kg/USD |",
            f"| y_nab | ${r0['y_nab_B']:.2f} B |",
            f"| (L_dom @ y_nab) | ${r0['Ldom_y_B']:.2f} B |",
            f"| **BLy** | **{r0['BLy_Mt']:.2f} MtCO₂e** |",
            "",
            "```",
            f"D[221100] = E/x = {r0['E_Mt']:.2f}e12 / {r0['x_B']:.2f}e9 = {r0['D']:.4f}",
            f"N[221100] = sum(B @ L) = {r0['N']:.4f}",
            "BLy[221100] = D * (L_dom @ y_nab)_221100",
            f"          = {r0['D']:.4f} * ${r0['Ldom_y_B']:.2f}B",
            f"          = {r0['BLy_Mt']:.2f} MtCO2e",
            "```",
            "",
            "### 3-way split — 221110 + 221121 + 221122",
            "",
            "PR3 reloads **E** from `GHG_national_Cornerstone_2023_egrid` and splits IO. "
            "Almost all inventory lands on **221110** with a much smaller **x**.",
            "",
            "| Sector | E (Mt) | x (B) | q (B) | D | L_dom,ii | N | y_nab (B) | L_dom @ y_nab (B) | BLy (Mt) |",
            "|--------|-------:|------:|------:|--:|---------:|--:|----------:|------------------:|---------:|",
        ]
    )
    for r in split["rows"]:
        lines.append(
            f"| {r['sector']} | {r['E_Mt']:.2f} | {r['x_B']:.2f} | {r['q_usd_B']:.2f} | "
            f"{r['D']:.4f} | {r['Ldom_diag']:.4f} | {r['N']:.4f} | {r['y_nab_B']:.2f} | "
            f"{r['Ldom_y_B']:.2f} | **{r['BLy_Mt']:.2f}** |"
        )
    lines.extend(
        [
            f"| **Sum** | **{split['E_total_Mt']:.2f}** | — | **{split['q_usd_total_B']:.2f}** | — | — | — | "
            f"**{split['y_nab_total_B']:.2f}** | — | **{split['BLy_total_Mt']:.2f}** |",
            "",
            "#### Block D (q-weighted)",
            "",
            "```",
            "D_block = (D_110*q_110 + D_121*q_121 + D_122*q_122) / (q_110 + q_121 + q_122)",
        ]
    )
    s = split["rows"]
    d_num = sum(r["D"] * r["q_usd_B"] for r in s)
    lines.append(
        f"        = ({s[0]['D']:.4f}*{s[0]['q_usd_B']:.2f} + {s[1]['D']:.4f}*{s[1]['q_usd_B']:.2f} "
        f"+ {s[2]['D']:.4f}*{s[2]['q_usd_B']:.2f}) / {split['q_usd_total_B']:.2f}"
    )
    lines.append(
        f"        = {d_num:.2f} / {split['q_usd_total_B']:.2f} = {split['D_weighted']:.6f} kg/USD"
    )
    lines.append("```")
    lines.extend(
        [
            "",
            "Generation dominates (~99.7% of block D) because `D_110` is large and "
            "`q_110` is a substantial share of block q.",
            "",
            "#### Block N (q-weighted)",
            "",
            "```",
            "N_block = (N_110*q_110 + N_121*q_121 + N_122*q_122) / sum(q)",
        ]
    )
    n_num = sum(r["N"] * r["q_usd_B"] for r in s)
    lines.append(
        f"        = ({s[0]['N']:.4f}*{s[0]['q_usd_B']:.2f} + {s[1]['N']:.4f}*{s[1]['q_usd_B']:.2f} "
        f"+ {s[2]['N']:.4f}*{s[2]['q_usd_B']:.2f}) / {split['q_usd_total_B']:.2f}"
    )
    lines.append(
        f"        = {n_num:.2f} / {split['q_usd_total_B']:.2f} = {split['N_weighted']:.6f} kg/USD"
    )
    lines.append("```")
    lines.extend(
        [
            "",
            "#### Block BLy (sum over electricity sectors)",
            "",
            "```",
            "BLy_block = BLy_110 + BLy_121 + BLy_122",
            "BLy_j     = D_j * (L_dom @ y_nab)_j",
            "",
        ]
    )
    for r in s:
        lines.append(
            f"BLy[{r['sector']}] = {r['D']:.4f} * ${r['Ldom_y_B']:.2f}B = {r['BLy_Mt']:.2f} MtCO2e"
        )
    bly_parts = " + ".join(f"{r['BLy_Mt']:.2f}" for r in s)
    lines.append(f"BLy_block = {bly_parts} = {split['BLy_total_Mt']:.2f} MtCO2e")
    lines.append("```")
    lines.extend(
        [
            "",
            "### Delta summary (reallocation → 3-way split)",
            "",
            "| Metric | Reallocation | 3-way split | Change | Primary driver |",
            "|--------|-------------:|------------:|-------:|----------------|",
            f"| D_block (kg/USD) | {realloc['D_weighted']:.4f} | {split['D_weighted']:.4f} | "
            f"{split['D_weighted']-realloc['D_weighted']:+.4f} | eGRID E on 221110 / small x_gen |",
            f"| N_block (kg/USD) | {realloc['N_weighted']:.4f} | {split['N_weighted']:.4f} | "
            f"{split['N_weighted']-realloc['N_weighted']:+.4f} | Higher D_gen + higher L_gen |",
            f"| BLy_block (Mt) | {realloc['BLy_total_Mt']:.2f} | {split['BLy_total_Mt']:.2f} | "
            f"{split['BLy_total_Mt']-realloc['BLy_total_Mt']:+.2f} | BLy_110 jumps with D_110 and L_dom @ y_nab |",
            f"| E_block (Mt) | {realloc['E_total_Mt']:.2f} | {split['E_total_Mt']:.2f} | "
            f"{split['E_total_Mt']-realloc['E_total_Mt']:+.2f} | eGRID FBS vs aggregate FBS |",
            f"| y_nab block (B) | {realloc['y_nab_total_B']:.2f} | {split['y_nab_total_B']:.2f} | "
            f"{split['y_nab_total_B']-realloc['y_nab_total_B']:+.2f} | IO split reallocates domestic demand |",
            "",
            f"**Why BLy rises more than E ({split['E_total_Mt']-realloc['E_total_Mt']:+.0f} Mt):** "
            "BLy is not E. It is **attributed production** through the IO identity. Generation BLy uses "
            f"`D_110 = {s[0]['D']:.2f}` (not the aggregate {realloc['D_weighted']:.2f}) "
            f"times `(L_dom @ y_nab)_110` (${s[0]['Ldom_y_B']:.0f}B). Transmission adds "
            f"{s[1]['BLy_Mt']:.1f} MtCO₂e; distribution has D≈0 so BLy≈0. The block sum "
            f"**{split['BLy_total_Mt']:,.0f} Mt** exceeds inventory **{split['E_total_Mt']:,.0f} Mt** "
            "because BLy counts attributed production through domestic final demand, not raw FBS totals.",
            "",
        ]
    )
    return "\n".join(lines)


def _conversion_factor_detail(config: str) -> dict[str, Any]:
    """Live ``c_col`` / ``c_row`` inputs and a few example purchasers."""
    from bedrock.extract.disaggregation.egrid_generation import (  # noqa: PLC0415
        us_total_net_generation_mwh,
    )
    from bedrock.transform.eeio.cornerstone_disagg_pipeline import (  # noqa: PLC0415
        _model_year_y_row_221110,
    )
    from bedrock.transform.eeio.electricity_disaggregation import (  # noqa: PLC0415
        _class_price,
        electricity_class_row_factors,
        electricity_output_factor,
    )
    from bedrock.transform.eeio.electricity_end_use_mapping import (  # noqa: PLC0415
        build_end_use_map,
        table_2_4_prices_cents_kwh,
    )

    reset_usa_config()
    _clear_model_caches()
    set_global_usa_config(config)
    aq = derive_cornerstone_Aq_scaled()
    cfg = get_usa_config()
    q_usd = float(aq.scaled_q[GENERATION_SECTOR])
    mwh = float(us_total_net_generation_mwh(cfg.model_base_year))
    c_col = electricity_output_factor(q_usd, mwh)
    prices = cast(dict[str, float], table_2_4_prices_cents_kwh(cfg.usa_ghg_data_year))
    end_use_map = build_end_use_map()
    y_row = _model_year_y_row_221110(aq)
    adom_row = cast(pd.Series, aq.Adom.loc[GENERATION_SECTOR])
    c_row = electricity_class_row_factors(
        adom_row, aq.scaled_q, y_row, prices, end_use_map, mwh
    )

    denom = 0.0
    inter_examples: list[dict[str, Any]] = []
    for col in adom_row.index:
        coef = float(adom_row[col])
        if coef == 0.0:
            continue
        p_j = _class_price(str(col), prices, end_use_map)
        flow_usd = coef * float(aq.scaled_q[col])
        denom += flow_usd / p_j
        if abs(flow_usd) >= 5e9 and len(inter_examples) < 3:
            inter_examples.append(
                {
                    "col": str(col),
                    "end_use": end_use_map[str(col)],
                    "p": p_j,
                    "a": coef,
                    "q_B": float(aq.scaled_q[col]) / 1e9,
                    "flow_B": flow_usd / 1e9,
                    "c_j": float(c_row[col]),
                }
            )
    for col in y_row.index:
        y_val = float(y_row[col])
        if y_val == 0.0:
            continue
        denom += y_val / _class_price(str(col), prices, end_use_map)

    lam = float(mwh / denom)
    fd_examples: list[dict[str, Any]] = []
    for col in y_row.index:
        y_val = float(y_row[col])
        if abs(y_val) < 5e9:
            continue
        p_f = _class_price(str(col), prices, end_use_map)
        fd_examples.append(
            {
                "col": str(col),
                "end_use": end_use_map[str(col)],
                "p": p_f,
                "y_B": y_val / 1e9,
                "c_j": float(c_row[col]),
            }
        )
        if len(fd_examples) >= 2:
            break

    return {
        "model_base_year": int(cfg.model_base_year),
        "ghg_year": int(cfg.usa_ghg_data_year),
        "q_usd": q_usd,
        "mwh": mwh,
        "c_col": c_col,
        "prices": prices,
        "lam": lam,
        "denom": denom,
        "inter_examples": inter_examples,
        "fd_examples": fd_examples,
        "c_row_min": float(c_row.min()),
        "c_row_median": float(c_row.median()),
        "c_row_max": float(c_row.max()),
        "n_c_row": int(c_row.notna().sum()),
    }


def _render_conversion_factors_subsection(detail: dict[str, Any]) -> list[str]:
    q_b = detail["q_usd"] / 1e9
    mwh_b = detail["mwh"] / 1e9
    prices = detail["prices"]
    lines = [
        "### How `c_col` and `c_row` are calculated",
        "",
        "Mixed units need two kinds of conversion factors for generation (**221110**):",
        "",
        "| Factor | Role | Units |",
        "|--------|------|-------|",
        "| **`c_col`** | Converts the **generation column** "
        "(output `q_110`, inputs into gen, `B[:,110]`) from USD to MWh | MWh / USD |",
        "| **`c_row`** | Converts the **generation sales row** "
        "(`Adom[110, ·]`, `Aimp[110, ·]`, and FD purchases of gen) from USD to MWh, "
        "**by purchaser** (end-use class) | MWh / USD (per column) |",
        "",
        "`c_col` is a single national average intensity. `c_row` varies by purchaser "
        "because residential, commercial, industrial, and transportation buyers face "
        "different retail electricity prices (EIA EPA Table 2.4).",
        "",
        "#### `c_col` — output / column factor",
        "",
        "```",
        "c_col = MWh_eGRID / q_USD_221110",
        "```",
        "",
        f"- **MWh_eGRID** = U.S. total net generation from eGRID for "
        f"model_base_year **{detail['model_base_year']}** "
        f"= **{detail['mwh']:,.0f} MWh** ({mwh_b:.4f} × 10⁹).",
        f"- **q_USD_221110** = scaled commodity output of generation "
        f"= **${q_b:.4f} B**.",
        "",
        "```",
        f"c_col = {detail['mwh']:,.0f} / {detail['q_usd']:,.2f}",
        f"      = {detail['c_col']:.6f} MWh/USD",
        "```",
        "",
        "Interpretation: each dollar of generation output corresponds to "
        f"**{detail['c_col']:.4f} MWh** on average. Applying `q_MWh = q_USD × c_col` "
        "and `B_MWh = B_USD / c_col` keeps `B·q` (kg CO₂e) unchanged.",
        "",
        "#### `c_row` — sales-row factors by purchaser class",
        "",
        "Purchaser column `j` is mapped to an EPA end-use class "
        "(Residential / Commercial / Industrial / Transportation) via "
        "`build_end_use_map()`, then priced with Table 2.4 retail rates "
        f"(cents/kWh, GHG year **{detail['ghg_year']}**):",
        "",
        "| End-use class | Table 2.4 price (¢/kWh) |",
        "|---------------|------------------------:|",
        f"| Residential | {prices['Residential']:.2f} |",
        f"| Commercial | {prices['Commercial']:.2f} |",
        f"| Industrial | {prices['Industrial']:.2f} |",
        f"| Transportation | {prices['Transportation']:.2f} |",
        "",
        "Domestic generation-row USD flows are intermediate sales "
        "`A_110,j · q_j` plus model-year final-demand purchases `y_110,f`. "
        "Define a price-weighted denominator and a scalar **λ** that forces "
        "total converted MWh to equal eGRID generation:",
        "",
        "```",
        "denom = Σ_j (A_110,j · q_j) / p_j  +  Σ_f y_110,f / p_f",
        "λ     = MWh_eGRID / denom",
        "c_j   = λ / p_j     # for every purchaser column j (and FD category f)",
        "```",
        "",
        "Here `p_j` is the Table 2.4 price for `j`'s end-use class. **λ** absorbs "
        "unit consistency between USD flows and ¢/kWh prices so that",
        "",
        "```",
        "Σ_j (A_110,j · q_j · c_j) + Σ_f (y_110,f · c_f) = MWh_eGRID",
        "```",
        "",
        "exactly (row MWh identity). Numerically for this run:",
        "",
        "```",
        f"denom = {detail['denom']:.4e}",
        f"λ     = {detail['mwh']:,.0f} / denom = {detail['lam']:.6f}",
        f"c_row ranges [{detail['c_row_min']:.6f}, {detail['c_row_max']:.6f}] "
        f"MWh/USD across {detail['n_c_row']} columns "
        f"(median {detail['c_row_median']:.6f})",
        "```",
        "",
    ]
    if detail["inter_examples"]:
        ex0 = detail["inter_examples"][0]
        lines.extend(
            [
                f"**Example — intermediate purchaser** ({ex0['col']}, {ex0['end_use']}):",
                "",
                "```",
                f"p_{ex0['col']} = {ex0['p']:.2f} ¢/kWh  ({ex0['end_use']})",
                f"A_110,{ex0['col']} = {ex0['a']:.6f}",
                f"q_{ex0['col']} = ${ex0['q_B']:.2f} B",
                f"flow_USD = A · q = ${ex0['flow_B']:.2f} B",
                f"c_{ex0['col']} = λ / p = {detail['lam']:.6f} / {ex0['p']:.2f} "
                f"= {ex0['c_j']:.6f} MWh/USD",
                "```",
                "",
            ]
        )
        if len(detail["inter_examples"]) > 1:
            ex1 = detail["inter_examples"][1]
            lines.extend(
                [
                    f"**Another intermediate example** ({ex1['col']}, {ex1['end_use']}): "
                    f"`p = {ex1['p']:.2f}` ¢/kWh → "
                    f"`c = {ex1['c_j']:.6f}` MWh/USD "
                    f"(flow ${ex1['flow_B']:.2f} B).",
                    "",
                ]
            )
    if detail["fd_examples"]:
        fd0 = detail["fd_examples"][0]
        lines.extend(
            [
                f"**Example — final demand** ({fd0['col']}, {fd0['end_use']}):",
                "",
                "```",
                f"y_110,{fd0['col']} = ${fd0['y_B']:.2f} B",
                f"p = {fd0['p']:.2f} ¢/kWh ({fd0['end_use']})",
                f"c = λ / p = {detail['lam']:.6f} / {fd0['p']:.2f} "
                f"= {fd0['c_j']:.6f} MWh/USD",
                "```",
                "",
            ]
        )
    lines.extend(
        [
            "In `A`, the generation **row** is multiplied by `c_j` (USD sales → MWh sales) "
            "and the generation **column** is divided by `c_col` (inputs per $ → inputs per MWh). "
            "Cheaper industrial power gets a **larger** `c_j` than residential for the same λ, "
            "so a dollar of industrial purchases maps to more MWh.",
            "",
        ]
    )
    return lines


def render_unit_conversion_walkthrough_md(
    split: dict[str, Any],
    mixed: dict[str, Any],
    *,
    conversion_detail: dict[str, Any] | None = None,
) -> str:
    """Explain why electricity-block BLy is unchanged under PR4 mixed units."""
    c_col = float(mixed["c_col"] or 0.0)
    s_rows = {r["sector"]: r for r in split["rows"]}
    m_rows = {r["sector"]: r for r in mixed["rows"]}
    gen = GENERATION_SECTOR
    s110, m110 = s_rows[gen], m_rows[gen]
    d_bly = mixed["BLy_total_Mt"] - split["BLy_total_Mt"]
    d_e = mixed["E_total_Mt"] - split["E_total_Mt"]
    d_d = mixed["D_weighted"] - split["D_weighted"]
    d_n = mixed["N_weighted"] - split["N_weighted"]

    lines = [
        "",
        "---",
        "",
        "## Walkthrough: 3-way split to unit conversion (D, N, BLy)",
        "",
        "This section explains what PR4 (mixed units) changes — and why **electricity-block "
        f"BLy stays at {split['BLy_total_Mt']:,.2f} MtCO₂e** (Δ = {d_bly:+.2e} Mt).",
        "",
        "### What PR4 does",
        "",
        "PR4 converts **only generation (221110)** from USD to physical MWh via "
        f"`c_col = {c_col:.6f} MWh/USD` (eGRID net generation ÷ monetary `q_110`):",
        "",
        "| Object | Conversion |",
        "|--------|------------|",
        "| `q_110` | `q_MWh = q_USD × c_col` |",
        "| `B[:, 110]` (and thus `D_110`) | `B_MWh = B_USD / c_col` → `D` in kg/MWh |",
        "| `Adom`/`Aimp` gen row & column | Rescaled with `c_col` / `c_row` so IO balance holds in mixed units |",
        "| `E`, `x`, T/D sectors (221121/221122) | **Unchanged** |",
        "",
    ]
    if conversion_detail is not None:
        lines.extend(_render_conversion_factors_subsection(conversion_detail))
    lines.extend(
        [
            "### Side-by-side sector table",
            "",
            "3-way (all USD) vs unit conversion (221110 in MWh; T/D still USD). "
            "`(L_dom @ y)` equals reported `q` under row balance.",
            "",
            "| Sector | E (Mt) | D (3-way) | D (mixed) | q (3-way, USD B) | q (mixed) | "
            "L@y (3-way) | L@y (mixed) | BLy 3-way (Mt) | BLy mixed (Mt) |",
            "|--------|-------:|----------:|----------:|-----------------:|----------:|"
            "-----------:|------------:|---------------:|---------------:|",
        ]
    )
    for s in ELECTRICITY_DISAGG_SECTORS:
        sr, mr = s_rows[s], m_rows[s]
        if mr["mixed_gen"]:
            d_mix = f"{mr['D']:.4f} kg/MWh"
            q_mix = f"{mr['q_rep_B']:.4f} B MWh"
            ly_mix = f"{mr['Ldom_y_B']:.4f} B MWh"
        else:
            d_mix = f"{mr['D']:.4f} kg/USD"
            q_mix = f"${mr['q_rep_B']:.2f} B"
            ly_mix = f"${mr['Ldom_y_B']:.2f} B"
        lines.append(
            f"| {s} | {sr['E_Mt']:.2f} | {sr['D']:.4f} kg/USD | {d_mix} | "
            f"${sr['q_usd_B']:.2f} | {q_mix} | ${sr['Ldom_y_B']:.2f} | {ly_mix} | "
            f"**{sr['BLy_Mt']:.2f}** | **{mr['BLy_Mt']:.2f}** |"
        )
    lines.extend(
        [
            f"| **Sum** | **{split['E_total_Mt']:.2f}** | — | — | "
            f"**${split['q_usd_total_B']:.2f}** | — | — | — | "
            f"**{split['BLy_total_Mt']:.2f}** | **{mixed['BLy_total_Mt']:.2f}** |",
            "",
            "### Why BLy is unchanged (the key identity)",
            "",
            "Per sector, attributed emissions are",
            "",
            "```",
            "BLy_j = D_j * (L_dom @ y_nab)_j",
            "```",
            "",
            "With a balanced domestic IO, `L_dom @ y_nab = q`, so **`BLy_j = D_j * q_j`**.",
            "",
            "For generation, PR4 multiplies `q` by `c_col` and divides `D` by the **same** `c_col`:",
            "",
            "```",
            f"c_col = {c_col:.6f} MWh/USD",
            f"D_110_USD  = {s110['D']:.6f} kg/USD",
            f"q_110_USD  = ${s110['q_usd_B']:.4f} B",
            f"BLy_110    = {s110['D']:.6f} * ${s110['q_usd_B']:.4f}B = {s110['BLy_Mt']:.2f} Mt",
            "",
            f"D_110_MWh  = D_110_USD / c_col = {m110['D']:.6f} kg/MWh",
            f"q_110_MWh  = q_110_USD * c_col = {m110['q_rep_B']:.6f} B MWh",
            f"BLy_110    = {m110['D']:.6f} * {m110['q_rep_B']:.6f}B = {m110['BLy_Mt']:.2f} Mt",
            "```",
            "",
            "The `c_col` factors cancel: `(D/c_col) * (q·c_col) = D·q`. Transmission and "
            "distribution never change units, so their `BLy` is identical. Therefore the "
            "**block sum is identical** at Mt precision — not an accident of rounding, but "
            "the design of the mixed-units transform.",
            "",
            "National total U.S. BLy is likewise unchanged for the same reason: only the "
            "generation column's intensity and activity units flip together; inventory `E` "
            "and all other sectors' `(D, q)` pairs are untouched.",
            "",
            "### What *does* change (and what does not)",
            "",
            "| Metric | 3-way split | Unit conversion | Change | Why |",
            "|--------|------------:|----------------:|-------:|-----|",
            f"| E_block (Mt) | {split['E_total_Mt']:.2f} | {mixed['E_total_Mt']:.2f} | "
            f"{d_e:+.2f} | FBS inventory not recomputed |",
            f"| D_block (kg/USD-equiv) | {split['D_weighted']:.4f} | {mixed['D_weighted']:.4f} | "
            f"{d_d:+.4f} | USD-equivalent D uses `D_MWh × c_col` for gen; stable |",
            f"| N_block (kg/USD-equiv) | {split['N_weighted']:.4f} | {mixed['N_weighted']:.4f} | "
            f"{d_n:+.4f} | `L` changes with mixed `A`; total EF intensities move |",
            f"| BLy_block (Mt) | {split['BLy_total_Mt']:.2f} | {mixed['BLy_total_Mt']:.2f} | "
            f"{d_bly:+.2e} | `D·q` invariant under `c_col` |",
            "",
            "**Takeaway:** Mixed units re-express generation on a physical activity basis "
            "(`kg/MWh` × MWh). Absolute attributed emissions (`BLy`) are invariant; "
            "Leontief total intensities (`N`) need not be, because `A`/`L` are rewritten.",
            "",
        ]
    )
    return "\n".join(lines)


def _strip_supplemental_sections(content: str) -> str:
    marker = "\n---\n\n## Walkthrough:"
    idx = content.find(marker)
    if idx == -1:
        return content.rstrip() + "\n"
    return content[:idx].rstrip() + "\n"


def append_walkthrough_to_report(out_path: str) -> None:
    realloc = _analyze(
        "2025_usa_cornerstone_v0_2_electricity_reallocation",
        "reallocation",
        [ELECTRICITY_AGGREGATE_SECTOR],
    )
    split = _analyze(
        "2025_usa_cornerstone_v0_2_electricity_disaggregation",
        "3-way split",
        list(ELECTRICITY_DISAGG_SECTORS),
    )
    mixed = _analyze(
        "2025_usa_cornerstone_v0_2_electricity_mixed_units",
        "unit conversion",
        list(ELECTRICITY_DISAGG_SECTORS),
    )
    split_block = _analyze_y_nab_block(
        "2025_usa_cornerstone_v0_2_electricity_disaggregation",
        list(ELECTRICITY_DISAGG_SECTORS),
    )
    mixed_block = _analyze_y_nab_block(
        "2025_usa_cornerstone_v0_2_electricity_mixed_units",
        list(ELECTRICITY_DISAGG_SECTORS),
        mixed=True,
    )
    conversion_detail = _conversion_factor_detail(
        "2025_usa_cornerstone_v0_2_electricity_mixed_units"
    )
    with open(out_path, encoding="utf-8") as f:
        base = _strip_supplemental_sections(f.read())
    supplemental = (
        render_walkthrough_md(realloc, split)
        + render_unit_conversion_walkthrough_md(
            split, mixed, conversion_detail=conversion_detail
        )
        + render_y_nab_section_md(realloc, split_block, mixed_block)
    )
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(base + supplemental)


def main() -> None:
    realloc = _analyze(
        "2025_usa_cornerstone_v0_2_electricity_reallocation",
        "reallocation",
        [ELECTRICITY_AGGREGATE_SECTOR],
    )
    split = _analyze(
        "2025_usa_cornerstone_v0_2_electricity_disaggregation",
        "3-way split",
        list(ELECTRICITY_DISAGG_SECTORS),
    )

    print("=" * 72)
    print("REALLOCATION (221100 aggregate)")
    print("=" * 72)
    for r in realloc["rows"]:
        print(
            f"  {r['sector']}: E={r['E_Mt']:.2f} Mt | x={r['x_B']:.2f}B | q={r['q_usd_B']:.2f}B | "
            f"E/x={r['E_over_x']:.4f} | Vnorm={r['Vnorm_diag']:.4f} | D={r['D']:.4f} | "
            f"L_ii={r['L_diag']:.4f} | N={r['N']:.4f}"
        )
    print(f"  Block E total: {realloc['E_total_Mt']:.2f} MtCO2e")
    print(f"  Block q_usd:   ${realloc['q_usd_total_B']:.2f} B")
    print(f"  D_weighted = sum(D_s*q_s)/sum(q_s) = {realloc['D_weighted']:.6f} kg/USD")
    print(f"  N_weighted = sum(N_s*q_s)/sum(q_s) = {realloc['N_weighted']:.6f} kg/USD")

    print()
    print("=" * 72)
    print("3-WAY SPLIT (221110 + 221121 + 221122)")
    print("=" * 72)
    for r in split["rows"]:
        print(
            f"  {r['sector']}: E={r['E_Mt']:.2f} Mt | x={r['x_B']:.2f}B | q={r['q_usd_B']:.2f}B | "
            f"E/x={r['E_over_x']:.4f} | Vnorm={r['Vnorm_diag']:.4f} | D={r['D']:.4f} | "
            f"L_ii={r['L_diag']:.4f} | N={r['N']:.4f}"
        )
    print(f"  Block E total: {split['E_total_Mt']:.2f} MtCO2e")
    print(f"  Block q_usd:   ${split['q_usd_total_B']:.2f} B")
    print(f"  D_weighted = sum(D_s*q_s)/sum(q_s) = {split['D_weighted']:.6f} kg/USD")
    print(f"  N_weighted = sum(N_s*q_s)/sum(q_s) = {split['N_weighted']:.6f} kg/USD")

    print()
    print("=" * 72)
    print("DELTA DECOMPOSITION")
    print("=" * 72)
    d0, d1 = realloc["D_weighted"], split["D_weighted"]
    n0, n1 = realloc["N_weighted"], split["N_weighted"]
    print(f"  dD_total = {d1 - d0:+.6f} kg/USD ({100*(d1/d0-1):+.1f}%)")
    print(f"  dN_total = {n1 - n0:+.6f} kg/USD ({100*(n1/n0-1):+.1f}%)")
    d_cf = (
        sum(split["rows"][i]["D"] * split["rows"][i]["q_usd_B"] for i in range(3))
        / split["q_usd_total_B"]
    )
    print(f"    = {d_cf:.6f} (actual split D_weighted)")
    print(
        f"  BLy_block realloc: {realloc['BLy_total_Mt']:.2f} Mt | split: {split['BLy_total_Mt']:.2f} Mt"
    )


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--append":
        from bedrock.analysis.electricity_disagg_diagnostics.paths import OUT_DIR

        append_walkthrough_to_report(str(OUT_DIR / "electricity_full_trace.md"))
        print(f"Appended walkthrough to {OUT_DIR / 'electricity_full_trace.md'}")
    else:
        main()
