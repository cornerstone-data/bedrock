"""Decompose D/N change between reallocation and 3-way split."""

from __future__ import annotations

from typing import Any

from bedrock.analysis.electricity_disagg_diagnostics.full_trace import (
    _clear_model_caches,
    _scalar_float,
    _sector_q_usd,
    _weighted_ef,
)
from bedrock.publish.model_objects import get_B, get_D, get_L, get_N, get_q
from bedrock.transform.eeio.derived import derive_E_usa
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_Aq_mixed_units,
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_Vnorm_scrap_corrected,
    derive_cornerstone_x_after_redefinition,
)
from bedrock.transform.eeio.electricity_disaggregation import (
    get_electricity_commodity_row_weights,
)
from bedrock.utils.config.usa_config import reset_usa_config, set_global_usa_config
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
    aq = derive_cornerstone_Aq_scaled()
    monetary_q = aq.scaled_q
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
        q_s = _sector_q_usd(s, q, monetary_q)
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
        l_diag = _scalar_float(L.at[s, s]) if s in L.index else float("nan")
        ldom_diag = _scalar_float(L_dom.at[s, s]) if s in L_dom.index else float("nan")
        y_s = _scalar_float(y[s]) if s in y.index else 0.0
        ly_s = _scalar_float(ly[s]) if s in ly.index else 0.0
        bly_s = _scalar_float(bly_vec[s]) if s in bly_vec.index else 0.0
        bly_check = d_s * ly_s
        q_usd_sum += q_s
        d_num += d_s * q_s
        n_num += n_s * q_s
        rows.append(
            {
                "sector": s,
                "E_Mt": e_s / 1e9,
                "x_B": x_s / 1e9,
                "q_usd_B": q_s / 1e9,
                "E_over_x": bi,
                "Vnorm_diag": vnorm_diag,
                "sum_B_D": b_sum,
                "D": d_s,
                "L_diag": l_diag,
                "Ldom_diag": ldom_diag,
                "N": n_s,
                "N_over_D": n_s / d_s if d_s else float("nan"),
                "y_nab_B": y_s / 1e9,
                "Ldom_y_B": ly_s / 1e9,
                "BLy_Mt": bly_s / 1e9,
                "D_times_Ldom_y_Mt": bly_check / 1e9,
            }
        )

    return {
        "label": label,
        "config": config,
        "sectors": sectors,
        "rows": rows,
        "E_total_Mt": sum(r["E_Mt"] for r in rows),
        "q_usd_total_B": q_usd_sum / 1e9,
        "y_nab_total_B": sum(r["y_nab_B"] for r in rows),
        "BLy_total_Mt": sum(r["BLy_Mt"] for r in rows),
        "D_weighted": d_num / q_usd_sum if q_usd_sum else 0.0,
        "N_weighted": n_num / q_usd_sum if q_usd_sum else 0.0,
        "D_weighted_fn": _weighted_ef(D_pub, q, monetary_q, sectors),
        "N_weighted_fn": _weighted_ef(N_pub, q, monetary_q, sectors),
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
        "This section explains why **D** rises from **2.883** to **3.347 kg/USD** and **N** from "
        "**3.316** to **4.338 kg/USD**, and why **BLy** rises from **1,716** to **1,987 MtCO₂e**.",
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
            "Generation dominates (~99.7% of block D) because `D_110 = 8.60` and `q_110` is 39% of block q.",
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
            "**Why BLy rises more than E (+33 Mt):** BLy is not E. It is **attributed production** "
            "through the IO identity. Generation BLy uses `D_110 = 8.60` (not the aggregate 2.88) "
            "times `(L_dom @ y_nab)_110` ($230B). Transmission adds $5.7 MtCO₂e; distribution has "
            "D≈0 so BLy≈0. The block sum **1,987 Mt** exceeds inventory **1,471 Mt** because BLy "
            "counts attributed production through domestic final demand, not raw FBS totals.",
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
    split_block = _analyze_y_nab_block(
        "2025_usa_cornerstone_v0_2_electricity_disaggregation",
        list(ELECTRICITY_DISAGG_SECTORS),
    )
    mixed_block = _analyze_y_nab_block(
        "2025_usa_cornerstone_v0_2_electricity_mixed_units",
        list(ELECTRICITY_DISAGG_SECTORS),
        mixed=True,
    )
    with open(out_path, encoding="utf-8") as f:
        base = _strip_supplemental_sections(f.read())
    supplemental = render_walkthrough_md(realloc, split) + render_y_nab_section_md(
        realloc, split_block, mixed_block
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
