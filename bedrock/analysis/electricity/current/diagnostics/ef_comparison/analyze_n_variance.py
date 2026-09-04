"""Analyze why total-EF (N) rises for downstream sectors at the 3-way split
and why the mixed-units (physical generation) panel spreads further.

Decomposes ``N_j = sum_i D_i * L_ij`` for the v0.3.1 electricity footing, the
3-way electricity split, and mixed units, isolating the electricity-supply-chain
contribution so we can test whether the per-sector N change is driven by each
sector's electricity share of N (user hypothesis), why the 3-way change is
almost always positive, and why unit conversion raises N further for many
sectors (via rewritten A/L, flat ``c_row = 1/p``, and P5 D0 re-anchor).

Run:
    python -m bedrock.analysis.electricity.current.diagnostics.ef_comparison.analyze_n_variance

Outputs (under ``output/ef/panel/``):
    - n_variance_analysis.csv          footing ↔ 3-way
    - n_variance_mixed_analysis.csv    footing / 3-way / mixed + end-use class
    - n_variance_explained.md          refreshes marked sections in place
      (high/low ``N`` walkthrough + mixed-units panel write-up)
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import cast

import numpy as np
import pandas as pd

from bedrock.analysis.electricity.current.diagnostics.full_trace.full_trace import (
    _clear_model_caches,
)
from bedrock.analysis.electricity.current.diagnostics.paths import OUT_DIR
from bedrock.publish.model_objects import get_D, get_L, get_N
from bedrock.transform.eeio.electricity_disaggregation import GENERATION_SECTOR
from bedrock.transform.eeio.electricity_end_use_mapping import (
    build_end_use_map,
    classify_industry_end_use,
)
from bedrock.utils.config.usa_config import reset_usa_config, set_global_usa_config
from bedrock.utils.schemas.cornerstone_schemas import (
    ELECTRICITY_AGGREGATE_SECTOR,
    ELECTRICITY_DISAGG_SECTORS,
)

FOOTING_CONFIG = "2025_usa_cornerstone_v0_3_electricity_footing"
SPLIT_CONFIG = "2025_usa_cornerstone_v0_3_electricity_disaggregation"
MIXED_CONFIG = "2025_usa_cornerstone_v0_3_electricity_mixed_units"

FOOTING_ELEC = [ELECTRICITY_AGGREGATE_SECTOR]
SPLIT_ELEC = list(ELECTRICITY_DISAGG_SECTORS)

PANEL_DIR = OUT_DIR / "ef" / "panel"
EXPLAINED_MD = PANEL_DIR / "n_variance_explained.md"
MIXED_SECTION_BEGIN = "<!-- BEGIN mixed-units-n-variance -->"
MIXED_SECTION_END = "<!-- END mixed-units-n-variance -->"
WALKTHROUGH_SECTION_BEGIN = "<!-- BEGIN high-low-n-walkthrough -->"
WALKTHROUGH_SECTION_END = "<!-- END high-low-n-walkthrough -->"

# Contrast examples used in the mixed-units write-up / slides.
CONTRAST_SECTORS: list[tuple[str, str]] = [
    ("33641A", "Aerospace products"),
    ("452000", "General merchandise retail"),
    ("1121A0", "Cattle ranching"),
]

# High vs low electricity-share walkthrough (3-way vs footing).
WALKTHROUGH_SECTORS: list[tuple[str, str]] = [
    ("517110", "Wired telecom carriers"),
    ("562212", "Solid-waste landfill"),
]
# Closed-form slope from ``%ΔN ≈ elec_share × k`` (Point 1 fit).
WALKTHROUGH_SHARE_SLOPE = 0.28


@dataclass
class ModelVectors:
    d: pd.Series  # characterized direct EF (kg CO2e / USD or kg/MWh for gen mixed)
    n: pd.Series  # characterized total EF
    ell: pd.DataFrame  # Leontief inverse L (index=input i, cols=output j)


def _series_over_sectors(df: pd.DataFrame) -> pd.Series:
    """Collapse an (impact x sector) frame to a per-sector CO2e Series."""
    s = df.sum(axis=0)
    s.index = s.index.astype(str)
    return s.astype(float)


def load_model(config: str) -> ModelVectors:
    reset_usa_config()
    _clear_model_caches()
    set_global_usa_config(config)
    d = _series_over_sectors(get_D())
    n = _series_over_sectors(get_N())
    ell = get_L().copy()
    ell.index = ell.index.astype(str)
    ell.columns = ell.columns.astype(str)
    return ModelVectors(d=d, n=n, ell=ell)


def elec_contribution(mv: ModelVectors, elec_sectors: list[str]) -> pd.Series:
    """C_elec_j = sum_{i in elec} D_i * L_ij (kg CO2e / USD of j)."""
    present = [s for s in elec_sectors if s in mv.ell.index and s in mv.d.index]
    d_sub = mv.d.reindex(present).to_numpy()
    l_sub = mv.ell.loc[present]  # (elec x j)
    contrib = pd.Series(d_sub @ l_sub.to_numpy(), index=l_sub.columns, dtype=float)
    return contrib


def elec_dollars_embodied(mv: ModelVectors, elec_sectors: list[str]) -> pd.Series:
    """L_elec_j = sum_{i in elec} L_ij (electricity activity embodied per $ of j)."""
    present = [s for s in elec_sectors if s in mv.ell.index]
    return mv.ell.loc[present].sum(axis=0).astype(float)


def build_analysis() -> tuple[pd.DataFrame, ModelVectors, ModelVectors]:
    foot = load_model(FOOTING_CONFIG)
    split = load_model(SPLIT_CONFIG)

    # Verify the decomposition identity N_j == sum_i D_i L_ij on the footing.
    n_check = pd.Series(
        foot.d.reindex(foot.ell.index).to_numpy() @ foot.ell.to_numpy(),
        index=foot.ell.columns,
    )
    max_resid = float((n_check - foot.n.reindex(n_check.index)).abs().max())
    print(f"footing N decomposition max |resid| = {max_resid:.3e} (should be ~0)")

    celec_foot = elec_contribution(foot, FOOTING_ELEC)
    celec_split = elec_contribution(split, SPLIT_ELEC)
    ldollars_foot = elec_dollars_embodied(foot, FOOTING_ELEC)
    ldollars_split = elec_dollars_embodied(split, SPLIT_ELEC)

    # Restrict to non-electricity sectors present in both models.
    drop = set(FOOTING_ELEC) | set(SPLIT_ELEC)
    common = [s for s in foot.n.index if s in split.n.index and s not in drop]

    df = pd.DataFrame(index=pd.Index(common, name="sector"))
    df["N_foot"] = foot.n.reindex(common)
    df["N_split"] = split.n.reindex(common)
    df["D_foot"] = foot.d.reindex(common)
    df["D_split"] = split.d.reindex(common)
    df["Celec_foot"] = celec_foot.reindex(common)
    df["Celec_split"] = celec_split.reindex(common)
    df["Lelec_foot"] = ldollars_foot.reindex(common)
    df["Lelec_split"] = ldollars_split.reindex(common)
    df = df.dropna(subset=["N_foot", "N_split"])
    df = df[df["N_foot"] > 0]

    df["dN"] = df["N_split"] - df["N_foot"]
    df["dN_pct"] = df["dN"] / df["N_foot"]
    df["dD"] = df["D_split"] - df["D_foot"]
    df["dD_pct"] = df["dD"] / df["D_foot"].where(df["D_foot"] != 0, np.nan)
    df["elec_share_N"] = df["Celec_foot"] / df["N_foot"]
    df["Crest_foot"] = df["N_foot"] - df["Celec_foot"]
    df["Crest_split"] = df["N_split"] - df["Celec_split"]
    df["dCelec"] = df["Celec_split"] - df["Celec_foot"]
    df["dCrest"] = df["Crest_split"] - df["Crest_foot"]
    # Effective embodied electricity intensity (kg CO2e per $ of embodied elec).
    df["eff_int_foot"] = df["Celec_foot"] / df["Lelec_foot"].where(
        df["Lelec_foot"] != 0, np.nan
    )
    df["eff_int_split"] = df["Celec_split"] / df["Lelec_split"].where(
        df["Lelec_split"] != 0, np.nan
    )
    return df, foot, split


def build_mixed_analysis(
    split_df: pd.DataFrame,
    _split: ModelVectors,
    mixed: ModelVectors,
) -> pd.DataFrame:
    """Extend the 3-way analysis with mixed-units N/D/C_elec and EPA end-use class."""
    end_use_map = build_end_use_map()
    celec_mixed = elec_contribution(mixed, SPLIT_ELEC)
    common = [s for s in split_df.index if s in mixed.n.index]

    df = split_df.loc[common].copy()
    df["N_mixed"] = mixed.n.reindex(common)
    df["D_mixed"] = mixed.d.reindex(common)
    df["Celec_mixed"] = celec_mixed.reindex(common)
    df = df.dropna(subset=["N_mixed", "N_split", "N_foot"])
    df = df[df["N_split"] > 0]

    df["dN_mixed_vs_foot"] = df["N_mixed"] - df["N_foot"]
    df["dN_pct_mixed_vs_foot"] = df["dN_mixed_vs_foot"] / df["N_foot"]
    df["dN_mixed_vs_split"] = df["N_mixed"] - df["N_split"]
    df["dN_pct_mixed_vs_split"] = df["dN_mixed_vs_split"] / df["N_split"]
    df["Celec_ratio_mixed_split"] = df["Celec_mixed"] / df["Celec_split"].where(
        df["Celec_split"] != 0, np.nan
    )
    df["dD_pct_mixed_vs_foot"] = (df["D_mixed"] - df["D_foot"]) / df["D_foot"].where(
        df["D_foot"] != 0, np.nan
    )

    end_uses: list[str] = []
    rules: list[str] = []
    for s in df.index.astype(str):
        eu = end_use_map.get(s)
        _eu2, rule = classify_industry_end_use(s)
        if eu is None:
            eu = _eu2
        end_uses.append(str(eu))
        rules.append(str(rule))
    df["end_use"] = end_uses
    df["end_use_rule"] = rules
    return df


def _pct_fmt(x: float) -> str:
    return f"{x:+.1%}"


def _fmt_kg(x: float) -> str:
    """Format kg CO2e / USD intensities used in worked examples."""
    if abs(x) >= 1.0:
        return f"{x:.3f}"
    if abs(x) >= 0.01:
        return f"{x:.4f}"
    return f"{x:.6f}"


def _walkthrough_sector_block(
    code: str,
    name: str,
    foot: ModelVectors,
    split: ModelVectors,
    row: pd.Series,
) -> list[str]:
    """Markdown lines for one high/low walkthrough sector."""
    agg = ELECTRICITY_AGGREGATE_SECTOR
    n_foot = float(row["N_foot"])
    n_split = float(row["N_split"])
    d_foot = float(row["D_foot"])
    d_split = float(row["D_split"])
    celec_foot = float(row["Celec_foot"])
    celec_split = float(row["Celec_split"])
    crest_foot = float(row["Crest_foot"])
    crest_split = float(row["Crest_split"])
    lelec_foot = float(row["Lelec_foot"])
    lelec_split = float(row["Lelec_split"])
    d_n = float(row["dN"])
    d_celec = float(row["dCelec"])
    d_crest = float(row["dCrest"])
    d_n_pct = float(row["dN_pct"])
    share = float(row["elec_share_N"])
    d_agg = float(foot.d[agg])
    l_agg = float(cast(float, foot.ell.at[agg, code]))

    approx_pct = share * WALKTHROUGH_SHARE_SLOPE
    lelec_pct = (lelec_split / lelec_foot - 1.0) if lelec_foot else float("nan")
    celec_pct = (celec_split / celec_foot - 1.0) if celec_foot else float("nan")
    celec_frac_of_dn = (d_celec / d_n) if d_n else float("nan")

    lines: list[str] = [
        f"### {code} — {name}",
        "",
        "| | Footing | 3-way split | Δ |",
        "|---|---:|---:|---:|",
        f"| Own `D` | {_fmt_kg(d_foot)} | {_fmt_kg(d_split)} | "
        f"**{_pct_fmt(float(row['dD_pct']))}** |",
        f"| `N` | {_fmt_kg(n_foot)} | {_fmt_kg(n_split)} | "
        f"**{_pct_fmt(d_n_pct)}** |",
        f"| Electricity share of `N` (footing) | **{share:.1%}** | — | — |",
        "",
        "#### Footing — one blended electricity commodity",
        "",
        "```",
        f"C_elec = L[{agg}→{code}] · D_{agg}",
        f"       = {l_agg:.6f} · {d_agg:.4f}",
        f"       = {celec_foot:.5f}",
        "",
        f"C_rest = N − C_elec = {n_foot:.5f} − {celec_foot:.5f} = {crest_foot:.5f}",
        f"N      = {celec_foot:.5f} + {crest_foot:.5f} = {n_foot:.5f}",
        "```",
        "",
        "#### After 3-way — generation / transmission / distribution",
        "",
        f"Electricity **dollars** embodied (`L_elec`): "
        f"{lelec_foot:.6f} → {lelec_split:.6f} ({_pct_fmt(lelec_pct)}). "
        "Emissions change because generation carries undiluted intensity:",
        "",
        "```",
        f"C_elec = L[221110→{code}]·D_110",
        f"       + L[221121→{code}]·D_121",
        f"       + L[221122→{code}]·D_122",
    ]

    for kid in SPLIT_ELEC:
        lk = (
            float(cast(float, split.ell.at[kid, code]))
            if kid in split.ell.index
            else 0.0
        )
        dk = float(split.d[kid]) if kid in split.d.index else 0.0
        contrib = lk * dk
        lines.append(f"  {kid}: {lk:.6f} · {dk:.4f} = {contrib:.5f}")

    lines.extend(
        [
            f"       = {celec_split:.5f}",
            "",
            f"C_rest ≈ {crest_split:.5f}",
            f"N      = {celec_split:.5f} + {crest_split:.5f} = {n_split:.5f}",
            "```",
            "",
            "| Piece | Footing | Split |",
            "|---|---:|---:|",
        ]
    )
    for kid in SPLIT_ELEC:
        lk = (
            float(cast(float, split.ell.at[kid, code]))
            if kid in split.ell.index
            else 0.0
        )
        dk = float(split.d[kid]) if kid in split.d.index else 0.0
        lines.append(f"| `{kid}` contribution | — | {lk * dk:.5f} |")
    lines.extend(
        [
            f"| Non-electricity (`C_rest`) | {crest_foot:.5f} | {crest_split:.5f} |",
            f"| **Total `N`** | **{n_foot:.5f}** | **{n_split:.5f}** |",
            "",
            "```",
            f"dN      = {d_n:.5f}",
            f"dC_elec = {d_celec:.5f}   ← {celec_frac_of_dn:.0%} of the move",
            f"dC_rest = {d_crest:.5f}",
            f"%ΔN     = {_pct_fmt(d_n_pct)}",
            f"C_elec change = {_pct_fmt(celec_pct)} on electricity channel",
            "```",
            "",
            "Closed-form check from Point 1: "
            f"`%ΔN ≈ elec_share × {WALKTHROUGH_SHARE_SLOPE:.2f}` "
            f"= {share:.3f} × {WALKTHROUGH_SHARE_SLOPE:.2f} "
            f"= {_pct_fmt(approx_pct)} "
            f"(observed {_pct_fmt(d_n_pct)}).",
            "",
        ]
    )
    return lines


def render_high_low_walkthrough_section(
    df: pd.DataFrame,
    foot: ModelVectors,
    split: ModelVectors,
) -> str:
    """Markdown: high vs low electricity-share ``N`` walkthrough (3-way vs footing)."""
    high_code, high_name = WALKTHROUGH_SECTORS[0]
    low_code, low_name = WALKTHROUGH_SECTORS[1]
    missing = [c for c, _ in WALKTHROUGH_SECTORS if c not in df.index]
    if missing:
        raise KeyError(f"Walkthrough sectors missing from analysis frame: {missing}")

    high = cast(pd.Series, df.loc[high_code])
    low = cast(pd.Series, df.loc[low_code])
    d_agg = float(foot.d[ELECTRICITY_AGGREGATE_SECTOR])
    d_gen = float(split.d[GENERATION_SECTOR])

    lines: list[str] = [
        WALKTHROUGH_SECTION_BEGIN,
        "",
        "## Worked examples — high vs low electricity share "
        f"({high_code} vs {low_code})",
        "",
        "Scope: **3-way monetary split vs v0.3.1 electricity footing** for two "
        "non-electricity "
        "sectors — one electricity-dominated footprint and one process-emissions "
        "dominated footprint. Own `D` is unchanged; `y` is not used "
        "(`N_j = Σ_i D_i L_ij`).",
        "",
        "Identity:",
        "",
        "```",
        "N_j = C_elec_j + C_rest_j",
        "C_elec_j = Σ_{i ∈ electricity} D_i · L_ij",
        "C_rest_j = N_j − C_elec_j",
        "```",
        "",
        f"Undilution reference intensities: footing `D_{ELECTRICITY_AGGREGATE_SECTOR}` "
        f"= {d_agg:.4f} kg/USD; split `D_{GENERATION_SECTOR}` = {d_gen:.4f} kg/USD "
        f"(~{d_gen / d_agg:.1f}×).",
        "",
        *(_walkthrough_sector_block(high_code, high_name, foot, split, high)),
        f"**Why `{high_code}` moves a lot:** electricity is **{float(high['elec_share_N']):.0%}** "
        "of footing `N`. Effective embodied-electricity intensity rises from "
        f"{float(high['eff_int_foot']):.2f} → {float(high['eff_int_split']):.2f} kg per "
        "electricity $; that ~25% intensity bump on a large share of `N` produces a "
        f"double-digit `%ΔN`.",
        "",
        *(_walkthrough_sector_block(low_code, low_name, foot, split, low)),
        f"**Why `{low_code}` barely moves:** the same undilution physics raises "
        f"`C_elec` by ~{float(low['dCelec']):.3f} kg/$, but footing `N` is dominated by "
        f"`C_rest ≈ {float(low['Crest_foot']):.2f}`. Electricity is only "
        f"**{float(low['elec_share_N']):.2%}** of `N`, so `%ΔN` is tiny.",
        "",
        "### Side-by-side",
        "",
        f"| | {high_code} ({high_name}) | {low_code} ({low_name}) |",
        "|---|---:|---:|",
        f"| `L_elec` (footing) | {float(high['Lelec_foot']):.4f} | "
        f"{float(low['Lelec_foot']):.4f} |",
        f"| `C_elec` footing → split | "
        f"{float(high['Celec_foot']):.4f} → {float(high['Celec_split']):.4f} | "
        f"{float(low['Celec_foot']):.4f} → {float(low['Celec_split']):.4f} |",
        f"| `dC_elec` | {float(high['dCelec']):+.4f} | {float(low['dCelec']):+.4f} |",
        f"| `C_rest` (footing) | {float(high['Crest_foot']):.4f} | "
        f"{float(low['Crest_foot']):.4f} |",
        f"| Elec share of `N` | **{float(high['elec_share_N']):.1%}** | "
        f"**{float(low['elec_share_N']):.2%}** |",
        f"| `%ΔN` | **{_pct_fmt(float(high['dN_pct']))}** | "
        f"**{_pct_fmt(float(low['dN_pct']))}** |",
        f"| Own `%ΔD` | {_pct_fmt(float(high['dD_pct']))} | "
        f"{_pct_fmt(float(low['dD_pct']))} |",
        "",
        "**One-liner:** same electricity-channel undilution in both sectors; "
        "`%ΔN` scales with electricity's share of that sector's total EF.",
        "",
        "### Reproduce",
        "",
        "```",
        "python -m bedrock.analysis.electricity.current.diagnostics."
        "ef_comparison.analyze_n_variance",
        "```",
        "",
        "That regenerates `n_variance_analysis.csv` and refreshes this section "
        "(between the HTML markers) from live footing / 3-way models. "
        f"Sectors are configured as `WALKTHROUGH_SECTORS` in "
        "`ef_comparison/analyze_n_variance.py` "
        f"(currently `{high_code}`, `{low_code}`).",
        "",
        WALKTHROUGH_SECTION_END,
        "",
    ]
    return "\n".join(lines)


def _end_use_summary_table(df: pd.DataFrame) -> list[str]:
    lines = [
        "| End-use | n | Median `%ΔN` (3-way→mixed) | Mean | Min | Max | Share with `%ΔN` > 0 |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for eu in ("Industrial", "Commercial", "Transportation", "Residential"):
        g = df[df["end_use"] == eu]
        if g.empty:
            continue
        lines.append(
            f"| **{eu}** | {len(g)} | {_pct_fmt(float(g['dN_pct_mixed_vs_split'].median()))} | "
            f"{_pct_fmt(float(g['dN_pct_mixed_vs_split'].mean()))} | "
            f"{_pct_fmt(float(g['dN_pct_mixed_vs_split'].min()))} | "
            f"{_pct_fmt(float(g['dN_pct_mixed_vs_split'].max()))} | "
            f"{(g['dN_pct_mixed_vs_split'] > 0).mean():.1%} |"
        )
    return lines


def _contrast_table(df: pd.DataFrame) -> list[str]:
    lines = [
        "| Sector | Name | End-use | N footing | N 3-way | N mixed | "
        "`%ΔN` vs footing (3-way) | `%ΔN` vs footing (mixed) | Note |",
        "|---|---|---|---:|---:|---:|---:|---:|---|",
    ]
    notes = {
        "33641A": "High elec share of `N`; mixed units use uniform `1/p` plus P5 re-anchor",
        "452000": "Commercial; extra mixed bump is uniform `c_row`, not class prices",
        "1121A0": "Industrial but low elec share of `N`; small move either step",
    }
    for code, name in CONTRAST_SECTORS:
        if code not in df.index:
            continue
        r = df.loc[code]
        lines.append(
            f"| {code} | {name} | {r['end_use']} | "
            f"{r['N_foot']:.3f} | {r['N_split']:.3f} | {r['N_mixed']:.3f} | "
            f"{_pct_fmt(float(r['dN_pct']))} | "
            f"{_pct_fmt(float(r['dN_pct_mixed_vs_foot']))} | "
            f"{notes.get(code, '')} |"
        )
    return lines


def render_mixed_units_section(
    df: pd.DataFrame,
    split: ModelVectors,
    mixed: ModelVectors,
) -> str:
    """Markdown for why the mixed-units N panel spreads more than the 3-way panel."""
    gen = GENERATION_SECTOR
    d_gen_split = float(split.d[gen])
    d_gen_mixed = float(mixed.d[gen])
    c_col = d_gen_split / d_gen_mixed if d_gen_mixed else float("nan")

    med_3way = float(df["dN_pct"].median())
    med_mixed = float(df["dN_pct_mixed_vs_foot"].median())
    max_3way = float(df["dN_pct"].max())
    max_mixed = float(df["dN_pct_mixed_vs_foot"].max())
    gt10_3way = int((df["dN_pct"] > 0.10).sum())
    gt10_mixed = int((df["dN_pct_mixed_vs_foot"] > 0.10).sum())
    gt15_3way = int((df["dN_pct"] > 0.15).sum())
    gt15_mixed = int((df["dN_pct_mixed_vs_foot"] > 0.15).sum())
    n_sec = len(df)
    celec_ratio_med = float(df["Celec_ratio_mixed_split"].median())

    ind = df[df["end_use"] == "Industrial"]
    com = df[df["end_use"] == "Commercial"]
    trn = df[df["end_use"] == "Transportation"]
    other = pd.concat([com, trn]) if len(trn) else com
    min_ind = float(ind["dN_pct_mixed_vs_split"].min()) if len(ind) else float("nan")
    max_other = (
        float(other["dN_pct_mixed_vs_split"].max()) if len(other) else float("nan")
    )
    n_ind_below_com_med = (
        int(
            (ind["dN_pct_mixed_vs_split"] < com["dN_pct_mixed_vs_split"].median()).sum()
        )
        if len(ind) and len(com)
        else 0
    )

    eff_foot = float(df["eff_int_foot"].median())
    eff_split = float(df["eff_int_split"].median())

    lines: list[str] = [
        MIXED_SECTION_BEGIN,
        "",
        "## Why the mixed-units (physical generation) panel shows higher `N` than "
        "the 3-way split",
        "",
        "Scope: the **right panel** (\"Conversion to physical units\") of "
        "`v0.3_eia_gtd_pre_mecs_N.png` vs the **middle panel** (\"3-way monetary split\"). "
        "Both panels are percent differences against the **same v0.3.1 electricity "
        "footing**, so the mixed panel **stacks** the 3-way undilution, the "
        "mixed-units `A`/`L` rewrite (flat `c_row = 1/p`), and P5 D0 re-anchor.",
        "",
        "### Panel facts (non-electricity sectors vs footing)",
        "",
        "| Step | Median `%ΔN` | Max `%ΔN` | Sectors with `%ΔN` > 10% | "
        "Sectors with `%ΔN` > 15% | n |",
        "|---|---:|---:|---:|---:|---:|",
        f"| 3-way monetary split | {_pct_fmt(med_3way)} | {_pct_fmt(max_3way)} | "
        f"{gt10_3way} | {gt15_3way} | {n_sec} |",
        f"| Conversion to physical units | {_pct_fmt(med_mixed)} | {_pct_fmt(max_mixed)} | "
        f"{gt10_mixed} | {gt15_mixed} | {n_sec} |",
        "",
        "### Suggested slide framing",
        "",
        "#### What causes the extra `N` move at unit conversion",
        "",
        "1. **This is not another jump in direct EF.** USD-equivalent **`D` is unchanged** "
        f"for generation under the mixed-units transform: "
        f"`D_110` = {d_gen_split:.4f} kg/USD ↔ {d_gen_mixed:.4f} kg/MWh via "
        f"`c_col = D_USD/D_MWh ≈ {c_col:.6f}` MWh/USD. T/D direct EFs and inventory `E` "
        "are untouched; block USD-equiv `D` is stable.",
        "2. **What changes is `L` (and thus `N = Σᵢ Dᵢ Lᵢⱼ`).** Mixed units rewrite "
        "**A**: the generation **sales row** is multiplied by a **uniform** "
        "`c_row = 1/p` (USD→MWh), and the generation **column** is divided by "
        "`c_col`. Leontief requirements for generation become **MWh per $** of "
        "sector `j`. P5 (`reanchor_electricity_aq_after_year_scaling`) then "
        "re-applies D0 at `model_base_year`.",
        "3. **Physical MWh per electricity dollar is uniform across purchasers.** "
        "Production `c_row` is **not** `λ / p_j` from Table 2.4. Every A column "
        "gets the same `1/p`. Remaining end-use differences in `%ΔN` come from "
        "electricity's share of `N` and supply-chain structure, not class prices.",
        "4. **The panel is vs footing, so effects stack:** 3-way undilution "
        f"(embodied-electricity intensity median ~{eff_foot:.2f} → ~{eff_split:.2f} "
        "kg/$ elec) **+** mixed-units physical `L` rewrite **+** P5 re-anchor → "
        "wider / higher `%ΔN` cloud than the middle panel alone. Empirically, the "
        "median electricity-channel contribution "
        f"**`C_elec` rises {celec_ratio_med:.2f}×** from 3-way → mixed.",
        "",
        "**One-liner:** The 3-way split raises the *price* of electricity emissions "
        "(`D_gen`); mixed units re-express generation as physical MWh via uniform "
        "`1/p` and re-anchor A/q to D0 (`L[gen→j]` in MWh).",
        "",
        "#### Same share logic, bigger multiplier",
        "",
        "| Idea | 3-way vs footing | Mixed vs footing |",
        "|---|---|---|",
        "| Own `%ΔD` | ~0 (non-elec) | ~0 (non-elec; gen reported USD-equiv) |",
        "| Driver | Higher elec intensity (undiluted `D_110`) | Same + uniform `1/p` `L` rewrite + P5 |",
        f"| Median `%ΔN` | {_pct_fmt(med_3way)} | {_pct_fmt(med_mixed)} |",
        "| Still true | Larger elec share of `N` → larger `%ΔN` | Same ordering, larger amplitudes |",
        "",
        "### Contrast examples",
        "",
        *(_contrast_table(df)),
        "",
        "EPA end-use classes for these examples (from `build_end_use_map()` / "
        "`classify_industry_end_use`): **33641A Industrial**, **452000 Commercial**, "
        "**1121A0 Industrial**. None is Residential or Transportation.",
        "",
        "### Does Industrial always rise more than Commercial / Transportation "
        "(3-way → mixed)?",
        "",
        "**No — not always.** Industrial is higher **on average**, and every industrial "
        "sector in this run rises; some commercial/transport sectors fall slightly. "
        "The distributions **overlap**.",
        "",
        *(_end_use_summary_table(df)),
        "",
        f"Strict separation fails: `min(Industrial) = {_pct_fmt(min_ind)}` "
        f"< `max(Commercial∪Transportation) = {_pct_fmt(max_other)}`. "
        f"About **{n_ind_below_com_med}** industrial sectors sit below the commercial "
        "median `%ΔN` (3-way→mixed). Class mapping is **not** the mixed-units "
        "mechanism (`c_row` is flat `1/p`). **Elec share of `N`** and supply-chain "
        "structure still matter — Industrial ≠ always larger than Commercial/"
        "Transportation for every sector.",
        "",
        "### Reproduce",
        "",
        "```",
        "python -m bedrock.analysis.electricity.current.diagnostics.ef_comparison.analyze_n_variance",
        "```",
        "",
        "This regenerates `n_variance_analysis.csv`, `n_variance_mixed_analysis.csv`, "
        "and refreshes this section inside `n_variance_explained.md`.",
        "",
        MIXED_SECTION_END,
        "",
    ]
    return "\n".join(lines)


def upsert_marked_section(
    md_path: Path,
    section: str,
    begin: str,
    end: str,
    *,
    insert_before: str | None = None,
) -> None:
    """Insert or replace a marked section in the explained markdown."""
    md_path.parent.mkdir(parents=True, exist_ok=True)
    if md_path.exists():
        text = md_path.read_text(encoding="utf-8")
    else:
        text = (
            "# Why total-EF (N) moves under electricity disaggregation\n\n"
            "Auto-generated shell — re-run analyze_n_variance after adding narrative.\n\n"
        )

    if begin in text and end in text:
        pre = text.split(begin, 1)[0].rstrip()
        post = text.split(end, 1)[1].lstrip()
        new_text = pre + "\n\n" + section.rstrip() + "\n"
        if post.strip():
            new_text = new_text + "\n" + post
    elif insert_before and insert_before in text:
        idx = text.find(insert_before)
        new_text = (
            text[:idx].rstrip()
            + "\n\n"
            + section.rstrip()
            + "\n\n"
            + text[idx:].lstrip()
        )
    else:
        summary_at = text.find("\n## Summary\n")
        if summary_at >= 0:
            new_text = (
                text[:summary_at].rstrip()
                + "\n\n"
                + section.rstrip()
                + "\n\n"
                + text[summary_at:].lstrip()
            )
        else:
            new_text = text.rstrip() + "\n\n" + section.rstrip() + "\n"

    md_path.write_text(new_text, encoding="utf-8")


def upsert_mixed_section(md_path: Path, section: str) -> None:
    """Insert or replace the marked mixed-units section in the explained markdown."""
    upsert_marked_section(
        md_path,
        section,
        MIXED_SECTION_BEGIN,
        MIXED_SECTION_END,
    )

    text = md_path.read_text(encoding="utf-8")
    # Summary already mentions mixed CSV if the inserted section does; only patch
    # the trailing Summary reproduce blurb when it still lacks the mixed note.
    summary_at = text.find("\n## Summary\n")
    if summary_at >= 0:
        summary = text[summary_at:]
        if "mixed-units N panel" not in summary:
            old = (
                "python -m bedrock.analysis.electricity.current.diagnostics."
                "analyze_n_variance\n```"
            )
            new = (
                "python -m bedrock.analysis.electricity.current.diagnostics."
                "ef_comparison.analyze_n_variance\n```\n\n"
                "That regenerates `n_variance_analysis.csv`, "
                "`n_variance_mixed_analysis.csv`, and refreshes marked sections "
                "(high/low walkthrough + **mixed-units N panel**) in this file."
            )
            if old in summary:
                text = text[:summary_at] + summary.replace(old, new, 1)
                md_path.write_text(text, encoding="utf-8")


def upsert_walkthrough_section(md_path: Path, section: str) -> None:
    """Insert or replace the high/low electricity-share walkthrough section."""
    upsert_marked_section(
        md_path,
        section,
        WALKTHROUGH_SECTION_BEGIN,
        WALKTHROUGH_SECTION_END,
        insert_before=MIXED_SECTION_BEGIN,
    )


def detail(df: pd.DataFrame, foot: ModelVectors, split: ModelVectors) -> None:
    print("\n=== Electricity-sector DIRECT EF (kg CO2e / USD) ===")
    print(f"footing 221100          D = {foot.d[ELECTRICITY_AGGREGATE_SECTOR]:.4f}")
    for code in SPLIT_ELEC:
        print(f"split   {code}          D = {split.d[code]:.4f}")

    print("\n=== Worked examples: N = Celec + Crest ===")
    examples = ["517110", "452000", "447000", "1121A0", "562212"]
    for s in examples:
        if s not in df.index:
            continue
        r = df.loc[s]
        print(f"\nsector {s}")
        print(
            f"  footing: N={r['N_foot']:.4f} = Celec {r['Celec_foot']:.4f} "
            f"+ Crest {r['Crest_foot']:.4f}  (elec share {r['elec_share_N']:.3f})"
        )
        print(
            f"  split  : N={r['N_split']:.4f} = Celec {r['Celec_split']:.4f} "
            f"+ Crest {r['Crest_split']:.4f}"
        )
        print(
            f"  dN%={r['dN_pct']:.4f}  dCelec={r['dCelec']:.4f}  "
            f"dCrest={r['dCrest']:.4f}"
        )
        lf = float(cast(float, foot.ell.at[ELECTRICITY_AGGREGATE_SECTOR, s]))
        d_agg = float(foot.d[ELECTRICITY_AGGREGATE_SECTOR])
        print(f"  L_foot[221100->{s}] = {lf:.5f}  (x D {d_agg:.4f})")
        for code in SPLIT_ELEC:
            ls = (
                float(cast(float, split.ell.at[code, s]))
                if code in split.ell.index
                else 0.0
            )
            print(f"  L_split[{code}->{s}] = {ls:.5f}  (x D {split.d[code]:.4f})")

    print("\n=== Negative / smallest dN_pct outliers ===")
    print(
        df.sort_values("dN_pct")
        .head(5)[["N_foot", "N_split", "dN_pct", "elec_share_N", "dD_pct"]]
        .round(4)
        .to_string()
    )


def summarize(df: pd.DataFrame) -> None:
    n = len(df)
    print("\n=== Sample ===")
    print(f"non-electricity sectors analyzed: {n}")

    print("\n=== Point 2: sign of dN ===")
    pos = int((df["dN"] > 0).sum())
    neg = int((df["dN"] < 0).sum())
    print(f"dN > 0: {pos} ({pos / n:.1%}) | dN < 0: {neg} ({neg / n:.1%})")
    print("dN_pct quantiles:")
    print(df["dN_pct"].quantile([0.0, 0.25, 0.5, 0.75, 1.0]).round(4).to_string())

    print("\n=== D barely moves (direct EF) ===")
    print(
        f"median |dD_pct| = {df['dD_pct'].abs().median():.4%} | "
        f"max |dD_pct| = {df['dD_pct'].abs().max():.4%}"
    )

    print("\n=== Decomposition: dN = dCelec + dCrest ===")
    print(
        f"sum dN     = {df['dN'].sum():.3f}\n"
        f"sum dCelec = {df['dCelec'].sum():.3f} "
        f"({df['dCelec'].sum() / df['dN'].sum():.1%} of dN)\n"
        f"sum dCrest = {df['dCrest'].sum():.3f} "
        f"({df['dCrest'].sum() / df['dN'].sum():.1%} of dN)"
    )
    print(
        f"median |dCrest / dN| = "
        f"{(df['dCrest'].abs() / df['dN'].abs()).median():.2%}"
    )

    print("\n=== Hypothesis: dN_pct ~ elec_share_N ===")
    valid = df.dropna(subset=["elec_share_N", "dN_pct"])
    pear = float(np.corrcoef(valid["elec_share_N"], valid["dN_pct"])[0, 1])
    spear = float(
        cast(
            float, valid[["elec_share_N", "dN_pct"]].corr(method="spearman").iloc[0, 1]
        )
    )
    x = valid["elec_share_N"].to_numpy(dtype=float)
    y = valid["dN_pct"].to_numpy(dtype=float)
    k = float((x * y).sum() / (x * x).sum())
    resid = y - k * x
    ss_res = float((resid**2).sum())
    ss_tot = float((y**2).sum())
    r2_origin = 1.0 - ss_res / ss_tot
    print(f"Pearson r  = {pear:.4f}")
    print(f"Spearman r = {spear:.4f}")
    print(f"slope k (through origin) = {k:.4f}; R^2(origin) = {r2_origin:.4f}")

    print("\n=== Effective embodied electricity intensity (kg CO2e / $ elec) ===")
    print(
        f"footing median = {df['eff_int_foot'].median():.4f}\n"
        f"split   median = {df['eff_int_split'].median():.4f}\n"
        f"ratio (split/foot) median = "
        f"{(df['eff_int_split'] / df['eff_int_foot']).median():.4f}"
    )
    print(
        "electricity $ embodied ratio (Lelec_split/Lelec_foot) median = "
        f"{(df['Lelec_split'] / df['Lelec_foot']).median():.4f}"
    )
    print(
        "Celec ratio (split/foot) median = "
        f"{(df['Celec_split'] / df['Celec_foot']).median():.4f}"
    )

    print("\n=== Highest electricity-share sectors ===")
    top = df.sort_values("elec_share_N", ascending=False).head(12)
    print(
        top[["N_foot", "N_split", "dN_pct", "elec_share_N", "dD_pct"]]
        .round(4)
        .to_string()
    )

    print("\n=== Lowest electricity-share sectors (with dN_pct) ===")
    low = df.sort_values("elec_share_N").head(12)
    print(
        low[["N_foot", "N_split", "dN_pct", "elec_share_N", "dD_pct"]]
        .round(4)
        .to_string()
    )


def main() -> None:
    df, foot, split = build_analysis()
    PANEL_DIR.mkdir(parents=True, exist_ok=True)
    out_csv = PANEL_DIR / "n_variance_analysis.csv"
    df.to_csv(out_csv)
    print(f"Wrote {out_csv}")
    summarize(df)
    detail(df, foot, split)

    walkthrough = render_high_low_walkthrough_section(df, foot, split)
    upsert_walkthrough_section(EXPLAINED_MD, walkthrough)
    print(f"Updated high/low walkthrough section in {EXPLAINED_MD}")

    print("\n=== Mixed units (physical generation) ===")
    mixed = load_model(MIXED_CONFIG)
    mixed_df = build_mixed_analysis(df, split, mixed)
    out_mixed = PANEL_DIR / "n_variance_mixed_analysis.csv"
    mixed_df.to_csv(out_mixed)
    print(f"Wrote {out_mixed}")
    print(
        f"median dN_pct vs footing: 3-way={mixed_df['dN_pct'].median():.4f} "
        f"mixed={mixed_df['dN_pct_mixed_vs_foot'].median():.4f}"
    )
    print(
        f"median C_elec mixed/split = "
        f"{mixed_df['Celec_ratio_mixed_split'].median():.4f}"
    )
    print("end-use counts:\n", mixed_df["end_use"].value_counts().to_string())

    section = render_mixed_units_section(mixed_df, split, mixed)
    upsert_mixed_section(EXPLAINED_MD, section)
    print(f"Updated mixed-units section in {EXPLAINED_MD}")


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.WARNING, format="%(message)s")
    main()
