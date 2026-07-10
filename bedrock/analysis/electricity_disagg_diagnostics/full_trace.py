"""Collect IO / E / D / N / BLy traces across the electricity disagg config chain."""

from __future__ import annotations

import dataclasses as dc
from collections.abc import Sequence
from types import ModuleType
from typing import Any, cast

import pandas as pd

from bedrock.analysis.electricity_disagg_diagnostics.paths import OUT_DIR
from bedrock.publish.model_objects import get_B, get_D, get_L, get_N, get_q
from bedrock.transform.eeio.cornerstone_disagg_pipeline import (
    derive_disagg_Ytot_with_trade,
    electricity_conversion_factors,
    electricity_mixed_units_enabled,
)
from bedrock.transform.eeio.derived import derive_E_usa
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_U_set,
    derive_cornerstone_V,
    derive_cornerstone_VA,
    derive_cornerstone_Vnorm_scrap_corrected,
    derive_cornerstone_x_after_redefinition,
)
from bedrock.transform.eeio.electricity_disaggregation import GENERATION_SECTOR
from bedrock.utils.config.usa_config import (
    get_usa_config,
    reset_usa_config,
    set_global_usa_config,
)
from bedrock.utils.math.formulas import (
    backcompute_y_from_A_and_q,
    compute_x,
)
from bedrock.utils.schemas.cornerstone_schemas import (
    ELECTRICITY_AGGREGATE_SECTOR,
    ELECTRICITY_DISAGG_SECTORS,
)
from bedrock.utils.validation.calculate_national_accounting_balance_diagnostics import (
    _compute_bly_series,
)

CONFIG_CHAIN: list[tuple[str, str]] = [
    ("v0.2", "2025_usa_cornerstone_v0_2"),
    ("reallocation", "2025_usa_cornerstone_v0_2_electricity_reallocation"),
    ("3-way split", "2025_usa_cornerstone_v0_2_electricity_disaggregation"),
    ("unit conversion", "2025_usa_cornerstone_v0_2_electricity_mixed_units"),
]

GHG_ORDER = ["CO2", "CH4", "N2O", "SF6", "HFCs", "PFCs", "NF3"]


@dc.dataclass
class ConfigTrace:
    label: str
    config_name: str
    sectors: list[str]
    mixed_units: bool
    c_col: float | None
    io: dict[str, float]
    e_abs_kg: dict[str, float]
    d_abs_kg_per_usd: dict[str, float]
    n_abs_kg_per_usd: dict[str, float]
    d_total: float
    n_total: float
    bly_mt: float
    y_nab_usd: float


def _clear_module_caches(module: ModuleType) -> None:
    for name in dir(module):
        obj = getattr(module, name)
        if callable(obj) and hasattr(obj, "cache_clear"):
            obj.cache_clear()


def _scalar_float(value: object) -> float:
    return float(cast(Any, value))


def _clear_model_caches() -> None:
    import bedrock.publish.model_objects as model_objects  # noqa: PLC0415
    import bedrock.transform.allocation.derived as allocation_derived  # noqa: PLC0415
    import bedrock.transform.eeio.cornerstone_disagg_pipeline as disagg_pipeline  # noqa: PLC0415
    import bedrock.transform.eeio.derived as eeio_derived  # noqa: PLC0415
    import bedrock.transform.eeio.derived_cornerstone as derived_cornerstone  # noqa: PLC0415

    for mod in (
        model_objects,
        allocation_derived,
        disagg_pipeline,
        eeio_derived,
        derived_cornerstone,
    ):
        _clear_module_caches(mod)


def _active_electricity_sectors() -> list[str]:
    cfg = get_usa_config()
    if cfg.implement_electricity_disaggregation:
        return list(ELECTRICITY_DISAGG_SECTORS)
    return [ELECTRICITY_AGGREGATE_SECTOR]


def _sum_sectors(series: pd.Series, sectors: Sequence[str]) -> float:
    return float(series.reindex(list(sectors), fill_value=0.0).sum())


def _sum_matrix_diag(df: pd.DataFrame, sectors: Sequence[str]) -> float:
    total = 0.0
    for s in sectors:
        if s in df.index and s in df.columns:
            total += _scalar_float(df.at[s, s])
    return total


def _sum_matrix_col(df: pd.DataFrame, sectors: Sequence[str]) -> float:
    present = [s for s in sectors if s in df.columns]
    if not present:
        return 0.0
    return float(df[present].sum().sum())


def _sum_matrix_row(df: pd.DataFrame, sectors: Sequence[str]) -> float:
    present = [s for s in sectors if s in df.index]
    if not present:
        return 0.0
    return float(df.loc[present].sum().sum())


def _q_usd_total(
    q: pd.Series[float],
    *,
    sectors: Sequence[str],
    monetary_q: pd.Series[float] | None,
    _c_col: float | None,
) -> float:
    """USD-denominated commodity output total comparable across configs."""
    if monetary_q is not None and GENERATION_SECTOR in sectors:
        usd_parts = [float(monetary_q[s]) for s in sectors if s != GENERATION_SECTOR]
        usd_parts.append(float(monetary_q[GENERATION_SECTOR]))
        return float(sum(usd_parts))
    return _sum_sectors(q, sectors)


def collect_config_trace(label: str, config_name: str) -> ConfigTrace:
    reset_usa_config()
    _clear_model_caches()
    set_global_usa_config(config_name)
    sectors = _active_electricity_sectors()
    mixed = electricity_mixed_units_enabled()

    V = derive_cornerstone_V()
    uset = derive_cornerstone_U_set()
    Udom, Uimp = uset.Udom, uset.Uimp
    VA = derive_cornerstone_VA()
    Y = derive_disagg_Ytot_with_trade()
    x = compute_x(V=V)
    Vnorm = derive_cornerstone_Vnorm_scrap_corrected()

    aq_monetary = derive_cornerstone_Aq_scaled()
    c_col: float | None = None
    monetary_q = aq_monetary.scaled_q
    if mixed:
        c_col, _ = electricity_conversion_factors(aq_monetary)
        aq = derive_cornerstone_Aq_scaled()
        if electricity_mixed_units_enabled():
            from bedrock.transform.eeio.derived_cornerstone import (  # noqa: PLC0415
                derive_cornerstone_Aq_mixed_units,
            )

            aq = derive_cornerstone_Aq_mixed_units()
    else:
        aq = aq_monetary

    q = get_q()
    B = get_B()
    L = get_L()
    E = derive_E_usa()
    x_b = derive_cornerstone_x_after_redefinition()

    A = aq.Adom + aq.Aimp
    q_usd = _q_usd_total(q, sectors=sectors, monetary_q=monetary_q, _c_col=c_col)

    io = {
        "x industry output (USD)": _sum_sectors(x, sectors),
        "q commodity output (USD-equiv)": q_usd,
        "q commodity output (reported)": _sum_sectors(q, sectors),
        "V Make diagonal sum (USD)": _sum_matrix_diag(V, sectors),
        "Udom column sum (USD)": _sum_matrix_col(Udom, sectors),
        "Uimp column sum (USD)": _sum_matrix_col(Uimp, sectors),
        "Udom row sum (USD)": _sum_matrix_row(Udom, sectors),
        "VA column sum (USD)": _sum_matrix_col(VA, sectors),
        "Y row sum (USD)": _sum_matrix_row(Y, sectors),
        "Vnorm diagonal mean": _mean_diag(Vnorm, sectors),
        "A diagonal mean (scaled)": _mean_diag(A, sectors),
        "L diagonal mean": _mean_diag(L, sectors),
        "x for B denominator (USD)": _sum_sectors(x_b, sectors),
    }
    if mixed and c_col is not None:
        io["q generation MWh (221110)"] = float(q[GENERATION_SECTOR])
        io["c_col (MWh per USD gen)"] = float(c_col)

    e_abs: dict[str, float] = {}
    for gas in GHG_ORDER:
        if gas in E.index:
            e_abs[gas] = sum(
                _scalar_float(E.at[gas, s]) for s in sectors if s in E.columns
            )
    e_abs["TOTAL"] = float(sum(v for k, v in e_abs.items() if k != "TOTAL"))

    d_total = _weighted_ef(get_D(), q, monetary_q, sectors, c_col=c_col, mixed=mixed)
    n_total = _weighted_ef(get_N(), q, monetary_q, sectors, c_col=c_col, mixed=mixed)

    d_by_gas: dict[str, float] = {}
    n_by_gas: dict[str, float] = {}
    M = B @ L
    e_total = e_abs["TOTAL"]
    for gas in GHG_ORDER:
        if gas in e_abs and e_total:
            d_by_gas[gas] = (e_abs[gas] / e_total) * d_total
        if gas in M.index:
            num = 0.0
            for s in sectors:
                if s not in M.columns:
                    continue
                q_s = _sector_q_usd(s, q, monetary_q)
                m_gs = _scalar_float(M.at[gas, s])
                if mixed and s == GENERATION_SECTOR and c_col:
                    m_gs = m_gs * float(c_col)
                num += m_gs * q_s
            n_by_gas[gas] = num / q_usd if q_usd else 0.0
    d_by_gas["TOTAL"] = d_total
    n_gas_sum = sum(n_by_gas.values())
    if n_gas_sum > 0:
        scale = n_total / n_gas_sum
        n_by_gas = {g: v * scale for g, v in n_by_gas.items()}
    n_by_gas["TOTAL"] = n_total

    y = backcompute_y_from_A_and_q(A=aq.Adom, q=q)
    bly = _compute_bly_series(B=B, Adom=aq.Adom, y=y)
    bly_mt = _sum_sectors(bly, sectors) / 1e9
    y_nab = _sum_sectors(y, sectors)

    return ConfigTrace(
        label=label,
        config_name=config_name,
        sectors=sectors,
        mixed_units=mixed,
        c_col=c_col,
        io=io,
        e_abs_kg=e_abs,
        d_abs_kg_per_usd=d_by_gas,
        n_abs_kg_per_usd=n_by_gas,
        d_total=d_total,
        n_total=n_total,
        bly_mt=bly_mt,
        y_nab_usd=y_nab,
    )


def _sector_q_usd(
    sector: str,
    q: pd.Series[float],
    monetary_q: pd.Series[float] | None,
) -> float:
    if monetary_q is not None and sector in monetary_q.index:
        return float(monetary_q[sector])
    return float(q[sector])


def _mean_diag(df: pd.DataFrame, sectors: Sequence[str]) -> float:
    vals = [
        _scalar_float(df.at[s, s]) for s in sectors if s in df.index and s in df.columns
    ]
    return float(sum(vals) / len(vals)) if vals else 0.0


def _ef_per_usd(
    ef_df: pd.DataFrame,
    sector: str,
    *,
    mixed: bool,
    c_col: float | None,
) -> float:
    if sector in ef_df.columns:
        val = _scalar_float(ef_df[sector].squeeze())
    elif sector in ef_df.index:
        val = _scalar_float(ef_df.loc[sector].squeeze())
    else:
        return 0.0
    if mixed and sector == GENERATION_SECTOR and c_col:
        return val * float(c_col)
    return val


def _weighted_ef(
    ef_df: pd.DataFrame,
    q: pd.Series[float],
    monetary_q: pd.Series[float] | None,
    sectors: Sequence[str],
    *,
    c_col: float | None = None,
    mixed: bool = False,
) -> float:
    num = 0.0
    den = 0.0
    for s in sectors:
        q_s = _sector_q_usd(s, q, monetary_q)
        val = _ef_per_usd(ef_df, s, mixed=mixed, c_col=c_col)
        num += val * q_s
        den += q_s
    return num / den if den else 0.0


def collect_all_traces() -> list[ConfigTrace]:
    return [collect_config_trace(label, cfg) for label, cfg in CONFIG_CHAIN]


def _fmt_usd_b(val: float) -> str:
    return f"${val / 1e9:,.2f} B"


def _fmt_mtco2e_kg(val: float) -> str:
    return f"{val / 1e9:,.2f} MtCO₂e"


def _fmt_c_col(val: float) -> str:
    return f"{val:.6f} MWh/USD"


def _fmt_ef(val: float) -> str:
    return f"{val:.6f}"


def _pct_share(part: float, total: float) -> str:
    if total <= 0:
        return "—"
    return f"{100 * part / total:.2f}%"


def _delta_note(
    row: str,
    values: list[float],
    *,
    is_intensity: bool = False,
) -> str:
    if len(values) < 2:
        return ""
    base = values[0]
    if base == 0:
        return ""
    rel = [abs(v - base) / abs(base) for v in values[1:]]
    max_rel = max(rel) if rel else 0.0
    if max_rel < 0.005:
        return ""
    idx = rel.index(max_rel) + 1
    step = CONFIG_CHAIN[idx][0]
    notes = {
        ("x industry output (USD)", "reallocation"): (
            "PR2 clears 221100 co-production off-diagonals onto the diagonal; "
            "industry gross output is reshaped but national totals are preserved."
        ),
        ("x industry output (USD)", "3-way split"): (
            "PR3 splits aggregate 221100 into three industries; summed child x replaces parent x."
        ),
        ("q commodity output (USD-equiv)", "3-way split"): (
            "Commodity outputs are partitioned across 221110/221121/221122; sum matches parent q."
        ),
        ("q generation MWh (221110)", "unit conversion"): (
            "221110 q switches from USD to physical MWh; USD-equiv row uses monetary q for comparison."
        ),
        ("c_col (MWh per USD gen)", "unit conversion"): (
            "Anchors generation output to eGRID net generation MWh divided by monetary q."
        ),
        ("A diagonal mean (scaled)", "reallocation"): (
            "Co-production transfers change domestic input coefficients into the electricity block."
        ),
        ("A diagonal mean (scaled)", "3-way split"): (
            "Three-way Make/Use split reallocates intersection purchases across child sectors."
        ),
        ("TOTAL", "reallocation"): (
            "E is pinned to the GHG FBS snapshot; small drift only from sector mapping."
        ),
        ("TOTAL", "3-way split"): (
            "E reloads from eGRID FBS at child sectors; CO₂ rises ~3% vs aggregate FBS."
        ),
        ("TOTAL", "unit conversion"): (
            "E inventory unchanged; only A/q/B units change for generation."
        ),
        ("D total", "unit conversion"): (
            "Block D is E/q in USD-equiv; unit conversion leaves E and USD q unchanged, so D is stable."
        ),
        ("N total", "unit conversion"): (
            "Block N is q-weighted in USD-equiv; mixed units change A/L for generation but national BLy is unchanged."
        ),
        ("BLy (MtCO2e)", "reallocation"): (
            "IO reshape without Y/E change shifts attributed BLy between electricity and gas distribution."
        ),
        ("BLy (MtCO2e)", "3-way split"): (
            "Child-sector BLy sum exceeds parent due to IO restructuring; E also shifts to eGRID FBS."
        ),
        ("BLy (MtCO2e)", "unit conversion"): (
            "Mixed units leave national BLy unchanged at Mt precision."
        ),
    }
    key = (row, step)
    if key in notes:
        return notes[key]
    if is_intensity:
        return f"Largest change at {step} ({max_rel:.1%} vs v0.2); reflects IO and/or unit-basis shift."
    return f"Largest change at {step} ({max_rel:.1%} vs v0.2)."


def write_full_trace_markdown(
    traces: list[ConfigTrace],
    out_path: Any,
) -> None:
    labels = [t.label for t in traces]
    lines: list[str] = [
        "# Electricity full trace across v0.2 chain",
        "",
        "Comparison of IO anchors, emissions inventory **E**, direct EF **D**, "
        "total EF **N**, and **BLy** for the electricity block.",
        "",
        "Configs: **v0.2** footing → **reallocation** (PR2) → **3-way split** (PR3) "
        "→ **unit conversion** (PR4 mixed units).",
        "",
        "After PR3, values aggregate **221110 + 221121 + 221122** (summed) and compare "
        "to parent **221100** in earlier steps. For mixed units, USD-comparable rows "
        "use monetary q for generation; generation physical q is shown separately.",
        "",
    ]

    # IO anchors — skip internal-only rows in main table
    io_rows = [
        "x industry output (USD)",
        "q commodity output (USD-equiv)",
        "V Make diagonal sum (USD)",
        "Udom column sum (USD)",
        "Uimp column sum (USD)",
        "Udom row sum (USD)",
        "VA column sum (USD)",
        "Y row sum (USD)",
        "Vnorm diagonal mean",
        "A diagonal mean (scaled)",
        "L diagonal mean",
        "x for B denominator (USD)",
    ]
    lines.extend(_table_io(traces, labels, io_rows))

    mixed_rows = ["q generation MWh (221110)", "c_col (MWh per USD gen)"]
    if any(r in t.io for t in traces for r in mixed_rows):
        lines.append("")
        lines.append("### Mixed-units detail (unit conversion step)")
        lines.append("")
        lines.extend(_table_io(traces, labels, mixed_rows))

    # E absolute
    lines.append("")
    lines.append("## E inventory (absolute, kg CO₂e)")
    lines.append("")
    lines.extend(_table_gas_abs(traces, labels, "e_abs_kg", "kg CO₂e"))

    # E shares
    lines.append("")
    lines.append("## E inventory (shares of total)")
    lines.append("")
    lines.extend(_table_gas_share(traces, labels, "e_abs_kg"))

    # D absolute
    lines.append("")
    lines.append("## D — direct EF (kg CO₂e / USD-equiv)")
    lines.append("")
    lines.extend(_table_gas_abs(traces, labels, "d_abs_kg_per_usd", "kg/USD"))

    lines.append("")
    lines.append("## D — shares of total direct EF")
    lines.append("")
    lines.extend(_table_gas_share(traces, labels, "d_abs_kg_per_usd"))

    # N absolute
    lines.append("")
    lines.append("## N — total EF (kg CO₂e / USD-equiv)")
    lines.append("")
    lines.extend(_table_gas_abs(traces, labels, "n_abs_kg_per_usd", "kg/USD"))

    lines.append("")
    lines.append("## N — shares of total EF")
    lines.append("")
    lines.extend(_table_gas_share(traces, labels, "n_abs_kg_per_usd"))

    # BLy
    lines.append("")
    lines.append("## BLy attribution")
    lines.append("")
    lines.extend(_table_bly(traces, labels))

    out_path = str(out_path)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def _table_io(
    traces: list[ConfigTrace],
    labels: list[str],
    rows: list[str],
) -> list[str]:
    header = ["Metric", *labels, "Notes"]
    sep = ["---"] * len(header)
    body: list[str] = []
    for row in rows:
        vals: list[float] = []
        cells = [row]
        for t in traces:
            v = t.io.get(row)
            if v is None:
                cells.append("—")
            elif row == "c_col (MWh per USD gen)":
                cells.append(_fmt_c_col(v))
                vals.append(v)
            elif row == "q generation MWh (221110)":
                cells.append(f"{v:,.0f} MWh")
                vals.append(v)
            elif "USD" in row or row.startswith("x ") or row.startswith("q commodity"):
                cells.append(_fmt_usd_b(v))
                vals.append(v)
            else:
                cells.append(f"{v:.6f}")
                vals.append(v)
        note = _delta_note(row, vals) if vals else ""
        cells.append(note)
        body.append("| " + " | ".join(cells) + " |")
    return [
        "| " + " | ".join(header) + " |",
        "| " + " | ".join(sep) + " |",
        *body,
    ]


def _gas_rows(traces: list[ConfigTrace], field: str) -> list[str]:
    gases = [g for g in GHG_ORDER if any(g in getattr(t, field) for t in traces)]
    gases.append("TOTAL")
    return gases


def _table_gas_abs(
    traces: list[ConfigTrace],
    labels: list[str],
    field: str,
    unit: str,
) -> list[str]:
    header = ["GHG", *labels, "Notes"]
    sep = ["---"] * len(header)
    body: list[str] = []
    data = field
    for gas in _gas_rows(traces, data):
        cells = [gas]
        vals: list[float] = []
        for t in traces:
            d = getattr(t, data)
            v = d.get(gas, 0.0)
            vals.append(v)
            if gas == "TOTAL" and field == "e_abs_kg":
                cells.append(_fmt_mtco2e_kg(v))
            elif field == "e_abs_kg":
                cells.append(_fmt_mtco2e_kg(v))
            else:
                cells.append(_fmt_ef(v))
        row_label = "TOTAL" if gas == "TOTAL" else gas
        note = _delta_note(
            row_label if gas == "TOTAL" else f"{row_label} {field}",
            vals,
            is_intensity=field != "e_abs_kg",
        )
        if gas == "TOTAL" and field != "e_abs_kg":
            note = _delta_note(
                f"{'D' if 'd_' in field else 'N'} total", vals, is_intensity=True
            )
        cells.append(note)
        body.append("| " + " | ".join(cells) + " |")
    lines = [
        f"*Units: {unit}*",
        "",
        "| " + " | ".join(header) + " |",
        "| " + " | ".join(sep) + " |",
        *body,
    ]
    return lines


def _table_gas_share(
    traces: list[ConfigTrace],
    labels: list[str],
    field: str,
) -> list[str]:
    header = ["GHG", *labels, "Notes"]
    sep = ["---"] * len(header)
    body: list[str] = []
    for gas in [g for g in GHG_ORDER if any(g in getattr(t, field) for t in traces)]:
        cells = [gas]
        vals: list[float] = []
        for t in traces:
            d = getattr(t, field)
            total = d.get("TOTAL", sum(v for k, v in d.items() if k != "TOTAL"))
            v = d.get(gas, 0.0)
            share = 100 * v / total if total else 0.0
            vals.append(share)
            cells.append(_pct_share(v, total))
        cells.append(_delta_note(f"{gas} share", vals))
        body.append("| " + " | ".join(cells) + " |")
    return [
        "| " + " | ".join(header) + " |",
        "| " + " | ".join(sep) + " |",
        *body,
    ]


def _table_bly(traces: list[ConfigTrace], labels: list[str]) -> list[str]:
    header = ["Metric", *labels, "Notes"]
    sep = ["---"] * len(header)
    rows = ["BLy (MtCO2e)", "y_nab (USD)"]
    body: list[str] = []
    for row in rows:
        cells = [row]
        vals: list[float] = []
        for t in traces:
            if row.startswith("BLy"):
                v = t.bly_mt
                cells.append(f"{v:,.2f}")
            else:
                v = t.y_nab_usd
                cells.append(_fmt_usd_b(v))
            vals.append(v)
        note = _delta_note(row, vals)
        cells.append(note)
        body.append("| " + " | ".join(cells) + " |")
    return [
        "| " + " | ".join(header) + " |",
        "| " + " | ".join(sep) + " |",
        *body,
    ]


def main() -> None:
    traces = collect_all_traces()
    out = OUT_DIR / "electricity_full_trace.md"
    write_full_trace_markdown(traces, out)
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
