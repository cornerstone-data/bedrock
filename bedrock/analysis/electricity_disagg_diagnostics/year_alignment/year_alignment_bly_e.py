"""Probe BLy vs E under mixed-units with A/q year-alignment, and document year handling.

Tasks (mixed-units model only):
1. Compare national and electricity-block BLy vs E with a fully single-year 2017
   attempt (no A/q scale/inflate; 2017 GHG FBS). Document blockers (no 2017 eGRID
   inventory / eGRID FBS years only 2023–2024) and the proxies used.
2. Summarize how year changes are handled for E, B, A, q, L, D, N (+ x, y_nab, Vnorm).
3. Outline what a 2017–2023 D/N time series would require (no implementation).

    Run:
    python -m bedrock.analysis.electricity_disagg_diagnostics.year_alignment.year_alignment_bly_e
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from unittest.mock import patch

import numpy as np
import pandas as pd
import yaml

from bedrock.analysis.electricity_disagg_diagnostics.full_trace.full_trace import (
    _clear_model_caches,
)
from bedrock.analysis.electricity_disagg_diagnostics.paths import OUT_DIR
from bedrock.publish.model_objects import get_B, get_D, get_Ldom
from bedrock.transform.allocation.derived import derive_E_usa, load_E_from_flowsa
from bedrock.transform.eeio.cornerstone_disagg_pipeline import (
    electricity_conversion_factors,
)
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_Aq_mixed_units,
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_x_after_redefinition,
)
from bedrock.transform.eeio.electricity_disaggregation import (
    GENERATION_SECTOR,
    split_electricity_e_for_disaggregated_b,
)
from bedrock.utils.config.usa_config import (
    CONFIG_DIR,
    USAConfig,
    get_usa_config,
    reset_usa_config,
)
from bedrock.utils.math.formulas import backcompute_y_from_A_and_q
from bedrock.utils.schemas.cornerstone_schemas import (
    CORNERSTONE_INDUSTRIES_ELEC,
    ELECTRICITY_DISAGG_SECTORS,
)
from bedrock.utils.validation.calculate_national_accounting_balance_diagnostics import (
    _compute_bly_series,
)

logger = logging.getLogger(__name__)

MIXED_CONFIG = "2025_usa_cornerstone_v0_3_electricity_mixed_units"
# stewi eGRID inventories: 2016, 2018–2023 (no 2017). Proxy MWh for mixed units.
EGRID_MWH_PROXY_YEAR = 2018

PANEL_DIR = OUT_DIR / "year_alignment"
REPORT_MD = PANEL_DIR / "bly_e_year_alignment.md"
REPORT_JSON = PANEL_DIR / "bly_e_year_alignment.json"


@dataclass
class ScopeTotals:
    label: str
    E_Mt: float
    BLy_Mt: float
    D_dot_q_Mt: float
    D_dot_x_usd_Mt: float
    q_vs_Ldom_y_rel_rmse: float
    x_over_q_median: float


def _mt(kg: float) -> float:
    return kg / 1e9


def _install_usa_config(overrides: dict[str, Any]) -> USAConfig:
    """Install a USAConfig, allowing usa_ghg_data_year=2017 via model_construct."""
    import bedrock.utils.config.usa_config as uc  # noqa: PLC0415

    reset_usa_config()
    with open(Path(CONFIG_DIR) / f"{MIXED_CONFIG}.yaml", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    merged = USAConfig().model_dump(mode="python")
    merged.update(data)
    merged.update(overrides)
    cfg = USAConfig.model_construct(**merged)
    uc._usa_config = cfg
    return cfg


def _patch_egrid_mwh_for_missing_2017() -> Any:
    from bedrock.extract.disaggregation import egrid_generation as eg  # noqa: PLC0415

    real = eg.us_total_net_generation_mwh

    def _wrapped(year: int, download_if_missing: bool = True) -> float:
        if year == 2017:
            logger.warning(
                "eGRID has no 2017 inventory; using %s MWh for mixed-units c_col",
                EGRID_MWH_PROXY_YEAR,
            )
            return real(EGRID_MWH_PROXY_YEAR, download_if_missing=download_if_missing)
        return real(year, download_if_missing=download_if_missing)

    return patch(
        "bedrock.extract.disaggregation.egrid_generation.us_total_net_generation_mwh",
        _wrapped,
    )


def _patch_load_e_use_year_fbs_not_egrid() -> Any:
    """Load GHG_national_Cornerstone_2017 (2017 is not an eGRID FBS year) and split."""

    def _patched_load_E_from_flowsa() -> pd.DataFrame:
        usa = get_usa_config()
        if not (
            usa.use_cornerstone_ghg_model
            and usa.implement_electricity_disaggregation
            and int(usa.usa_ghg_data_year) == 2017
        ):
            return load_E_from_flowsa()

        logger.warning(
            "Bypassing eGRID FBS branch (2017 unsupported); loading "
            "GHG_national_Cornerstone_2017 then splitting 221100→G/T/D"
        )
        import bedrock.utils.config.usa_config as uc  # noqa: PLC0415

        dumped = usa.model_dump(mode="python")
        dumped["implement_electricity_disaggregation"] = False
        prev = uc._usa_config
        uc._usa_config = USAConfig.model_construct(**dumped)
        try:
            E_agg = load_E_from_flowsa()
        finally:
            uc._usa_config = prev

        E = split_electricity_e_for_disaggregated_b(E_agg)
        return E.reindex(
            columns=[str(c) for c in CORNERSTONE_INDUSTRIES_ELEC], fill_value=0.0
        )

    return patch(
        "bedrock.transform.allocation.derived.load_E_from_flowsa",
        _patched_load_E_from_flowsa,
    )


def _series_d_total(D_df: pd.DataFrame) -> pd.Series[float]:
    s = D_df.sum(axis=0).astype(float)
    s.index = s.index.astype(str)
    return s


def _collect_scope(
    *,
    label: str,
    E: pd.DataFrame,
    B: pd.DataFrame,
    D: pd.Series[float],
    q: pd.Series[float],
    x: pd.Series[float],
    Adom: pd.DataFrame,
    sectors: list[str] | None,
    c_col: float | None,
) -> ScopeTotals:
    if sectors is None:
        sectors = [str(c) for c in E.columns if str(c) in q.index]
    secs = [s for s in sectors if s in E.columns and s in q.index and s in D.index]

    E_sum = float(E[secs].to_numpy().sum())
    y = backcompute_y_from_A_and_q(A=Adom, q=q)
    bly = _compute_bly_series(B=B, Adom=Adom, y=y)
    BLy_sum = float(bly.reindex(secs).fillna(0.0).sum())

    q_s = q.reindex(secs).astype(float).fillna(0.0)
    x_s = x.reindex(secs).astype(float).fillna(0.0)
    d_s = D.reindex(secs).astype(float).fillna(0.0)

    d_usd = d_s.copy()
    if c_col and GENERATION_SECTOR in d_usd.index:
        d_usd.loc[GENERATION_SECTOR] = float(d_usd.loc[GENERATION_SECTOR]) * float(
            c_col
        )

    D_dot_q = float((d_s * q_s).sum())
    D_dot_x = float((d_usd * x_s).sum())

    Ldom = get_Ldom().reindex(index=q.index, columns=q.index).fillna(0.0)
    Ldom_y = pd.Series(
        Ldom.to_numpy() @ y.reindex(q.index).fillna(0.0).to_numpy(),
        index=q.index,
    )
    q_block = q_s.to_numpy()
    ly_block = Ldom_y.reindex(secs).fillna(0.0).to_numpy()
    denom = float(np.linalg.norm(q_block)) or 1.0
    rel_rmse = float(np.linalg.norm(q_block - ly_block) / denom)

    ratio = (x_s / q_s.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan)
    return ScopeTotals(
        label=label,
        E_Mt=_mt(E_sum),
        BLy_Mt=_mt(BLy_sum),
        D_dot_q_Mt=_mt(D_dot_q),
        D_dot_x_usd_Mt=_mt(D_dot_x),
        q_vs_Ldom_y_rel_rmse=rel_rmse,
        x_over_q_median=float(ratio.median()) if ratio.notna().any() else float("nan"),
    )


def run_scenario(
    name: str,
    *,
    overrides: dict[str, Any],
    use_2017_e_patch: bool,
    use_egrid_mwh_proxy: bool,
) -> dict[str, Any]:
    _clear_model_caches()
    cfg = _install_usa_config(overrides)
    patches: list[Any] = []
    if use_egrid_mwh_proxy:
        patches.append(_patch_egrid_mwh_for_missing_2017())
    if use_2017_e_patch:
        patches.append(_patch_load_e_use_year_fbs_not_egrid())

    entered: list[Any] = []
    try:
        for p in patches:
            entered.append(p)
            p.start()

        aq_scaled = derive_cornerstone_Aq_scaled()
        c_col, _c_row = electricity_conversion_factors(aq_scaled)
        aq = derive_cornerstone_Aq_mixed_units()
        q = aq.scaled_q.astype(float)
        q.index = q.index.astype(str)
        Adom = aq.Adom
        E = derive_E_usa()
        E.columns = E.columns.astype(str)
        B = get_B()
        D = _series_d_total(get_D())
        x = derive_cornerstone_x_after_redefinition().astype(float)
        x.index = x.index.astype(str)

        national = _collect_scope(
            label="national",
            E=E,
            B=B,
            D=D,
            q=q,
            x=x,
            Adom=Adom,
            sectors=None,
            c_col=c_col,
        )
        elec = _collect_scope(
            label="electricity_block",
            E=E,
            B=B,
            D=D,
            q=q,
            x=x,
            Adom=Adom,
            sectors=list(ELECTRICITY_DISAGG_SECTORS),
            c_col=c_col,
        )

        return {
            "scenario": name,
            "model_base_year": int(cfg.model_base_year),
            "usa_io_data_year": int(cfg.usa_io_data_year),
            "usa_ghg_data_year": int(cfg.usa_ghg_data_year),
            "usa_detail_original_year": int(cfg.usa_detail_original_year),
            "scale_a_matrix_with_useeio_method": bool(
                cfg.scale_a_matrix_with_useeio_method
            ),
            "apply_io_year_adjustments": bool(cfg.apply_io_year_adjustments),
            "use_E_data_year_for_x_in_B": bool(cfg.use_E_data_year_for_x_in_B),
            "use_ghg_year_x_in_B": bool(cfg.use_ghg_year_x_in_B),
            "c_col": float(c_col),
            "q_sum": float(q.sum()),
            "q_gen": float(q.get(GENERATION_SECTOR, np.nan)),
            "x_gen": float(x.get(GENERATION_SECTOR, np.nan)),
            "D_gen": float(D.get(GENERATION_SECTOR, np.nan)),
            "E_gen_Mt": (
                _mt(float(E[GENERATION_SECTOR].sum()))
                if GENERATION_SECTOR in E.columns
                else float("nan")
            ),
            "aq_scaled_q_sum": float(aq_scaled.scaled_q.sum()),
            "national": asdict(national),
            "electricity_block": asdict(elec),
        }
    finally:
        for p in reversed(entered):
            p.stop()
        _clear_model_caches()
        reset_usa_config()


def _fmt_scope(s: dict[str, Any]) -> str:
    ratio = s["BLy_Mt"] / s["E_Mt"] if s["E_Mt"] else float("nan")
    bly_over_dq = s["BLy_Mt"] / s["D_dot_q_Mt"] if s["D_dot_q_Mt"] else float("nan")
    return (
        f"| {s['label']} | {s['E_Mt']:.2f} | {s['BLy_Mt']:.2f} | "
        f"{s['BLy_Mt'] - s['E_Mt']:+.2f} | {ratio:.4f} | "
        f"{s['D_dot_q_Mt']:.2f} | {bly_over_dq:.10f} | "
        f"{s['D_dot_x_usd_Mt']:.2f} | "
        f"{s['q_vs_Ldom_y_rel_rmse']:.3e} | {s['x_over_q_median']:.4f} |"
    )


def _scope_table(title: str, result: dict[str, Any]) -> list[str]:
    return [
        f"### {title}",
        "",
        "| Scope | E (Mt) | BLy (Mt) | BLy−E | BLy/E | Σ D·q (Mt) | "
        "BLy/(Σ D·q) | Σ D_USD·x (Mt) | ‖q−L_dom@y‖/‖q‖ | median x/q |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        _fmt_scope(result["national"]),
        _fmt_scope(result["electricity_block"]),
        "",
    ]


def _fmt_identity_row(scenario: str, s: dict[str, Any]) -> str:
    bly = s["BLy_Mt"]
    dq = s["D_dot_q_Mt"]
    bly_over_dq = bly / dq if dq else float("nan")
    bly_over_e = bly / s["E_Mt"] if s["E_Mt"] else float("nan")
    return (
        f"| {scenario} | {s['label']} | "
        f"{s['q_vs_Ldom_y_rel_rmse']:.3e} | "
        f"{bly:.2f} | {dq:.2f} | {bly - dq:+.3e} | {bly_over_dq:.10f} | "
        f"{s['E_Mt']:.2f} | {bly_over_e:.4f} |"
    )


def _identity_table(baseline: dict[str, Any], y2017: dict[str, Any]) -> list[str]:
    return [
        "| Scenario | Scope | ‖q−L_dom@y‖/‖q‖ | BLy (Mt) | Σ D·q (Mt) | "
        "BLy−Σ D·q | BLy/(Σ D·q) | E (Mt) | BLy/E |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|",
        _fmt_identity_row("Baseline", baseline["national"]),
        _fmt_identity_row("Baseline", baseline["electricity_block"]),
        _fmt_identity_row("2017 attempt", y2017["national"]),
        _fmt_identity_row("2017 attempt", y2017["electricity_block"]),
        "",
    ]


def render_report(baseline: dict[str, Any], y2017: dict[str, Any]) -> str:
    model_year = baseline["model_base_year"]
    io_year = baseline["usa_io_data_year"]
    ghg_year = baseline["usa_ghg_data_year"]
    detail_year = baseline["usa_detail_original_year"]
    apply_io = bool(baseline.get("apply_io_year_adjustments", False))
    years_match_aq = model_year == ghg_year
    egrid_eia_lead = (
        f"Numerically yes for the current config — both are {model_year} — but "
        "they are not the same config field."
        if years_match_aq
        else (
            f"Numerically **no** for the current config: inflated A/q and eGRID MWh "
            f"use `model_base_year={model_year}`, while EIA Table 2.4 prices use "
            f"`usa_ghg_data_year={ghg_year}`."
        )
    )
    egrid_eia_trail = (
        f"eGRID matches the inflated A/q year by construction. Prices match only "
        f"because the baseline also sets `usa_ghg_data_year={ghg_year}`; if GHG "
        "year diverged from `model_base_year`, prices would follow GHG, not A/q."
        if years_match_aq
        else (
            "eGRID still matches the inflated A/q year by construction "
            "(`model_base_year`). Prices follow `usa_ghg_data_year`, which differs "
            "from `model_base_year` on this baseline."
        )
    )
    if apply_io:
        aq_baseline_bullets = (
            "- A/q: `apply_io_year_adjustments` — scale detail→IO year with "
            "dollar-year-rebased summary ratios, then inflate→model year with "
            "**commodity** PI"
        )
        aq_table_cell = (
            f"Scale detail→`usa_io_data_year` ({io_year}) with dollar-year-rebased "
            f"summary ratios; inflate→`model_base_year` ({model_year}) via "
            "commodity PI (`apply_io_year_adjustments`); then mixed-units rewrite "
            "of gen row/column and `q_110`"
        )
        x_table_cell = (
            "BEA GO at `usa_ghg_data_year` via `use_ghg_year_x_in_B` "
            "(`apply_io_year_adjustments`)"
        )
        b_flags = (
            "`apply_io_year_adjustments=True` → `use_ghg_year_x_in_B`, "
            "`deflate_x_to_detail_io_year_for_B=False`, "
            "`use_scaled_x_and_scaled_Vnorm_for_B=False` (no post-hoc B "
            "scale/inflate)"
        )
        d7_inflate = (
            f"which is not the same as pinning levels to {model_year} GO. Under "
            "`apply_io_year_adjustments`, the inflate hop uses **commodity** PI."
        )
    else:
        aq_baseline_bullets = (
            "- A/q: default scale 2017→IO year then inflate→model year "
            "(legacy industry-PI branch; no `apply_io_year_adjustments`)"
        )
        aq_table_cell = (
            f"Scale detail→`usa_io_data_year` ({io_year}) with summary "
            f"ratios; inflate→`model_base_year` ({model_year}) via industry PI "
            "(default branch); then mixed-units rewrite of gen row/column and "
            "`q_110`"
        )
        x_table_cell = "BEA GO at `usa_ghg_data_year` when `use_E_data_year_for_x_in_B`"
        b_flags = (
            "`use_E_data_year_for_x_in_B=True`, "
            "`deflate_x_to_detail_io_year_for_B=False`, "
            "`use_scaled_x_and_scaled_Vnorm_for_B=False` (no post-hoc B "
            "scale/inflate)"
        )
        d7_inflate = f"which is not the same as pinning levels to {model_year} GO."
    lines = [
        "# BLy vs E under mixed units — year alignment probe",
        "",
        "Mixed-units model only. Compares attributed production "
        "`BLy = diag(D) @ L_dom @ y_nab` to inventory `E`, nationally and for the "
        f"electricity block ({'/'.join(ELECTRICITY_DISAGG_SECTORS)}).",
        "",
        "## Setup",
        "",
        "### Baseline (v0.3.1 electricity mixed units)",
        "",
        f"- Config: `{MIXED_CONFIG}`",
        f"- `model_base_year={model_year}`, "
        f"`usa_io_data_year={io_year}`, "
        f"`usa_ghg_data_year={ghg_year}`",
        aq_baseline_bullets,
        f"- E: year-keyed **eGRID FBS** at `usa_ghg_data_year={ghg_year}` when "
        "electricity disaggregation is on (supported: 2023, 2024)",
        f"- `x` in B: GHG-year industry GO (`use_ghg_year_x_in_B="
        f"{baseline.get('use_ghg_year_x_in_B', False)}`)",
        "",
        "### Single-year 2017 attempt",
        "",
        "- `model_base_year=2017`, `usa_io_data_year=2017`, `usa_ghg_data_year=2017`",
        "- `scale_a_matrix_with_useeio_method=True` (and "
        "`apply_io_year_adjustments=False`) → A/q stay on 2017 detail base "
        "(no summary-ratio scale / no price inflation)",
        "- E: **`GHG_national_Cornerstone_2017`** on GCS; this probe bypasses the "
        "production eGRID branch and splits aggregate `221100` → G/T/D via "
        "`split_electricity_e_for_disaggregated_b` (SF₆→transmission; other gases→generation)",
        f"- Mixed-units MWh: stewi eGRID has **no 2017** inventory "
        f"(available: 2016, 2018–2023). Proxy: **eGRID {EGRID_MWH_PROXY_YEAR}** "
        "net generation for `c_col` / `c_row`",
        "- Table 2.4 retail prices: **2017** EIA EPA values are available",
        "",
        "**Blockers to a true fully single-year 2017 mixed model in production code:**",
        "",
        "1. `usa_ghg_data_year` Literal excludes 2017 (breaks "
        "`reconciling_data_years/model1.yaml`)",
        "2. Electricity-disagg eGRID FBS is only defined for 2023/2024 "
        "(`egrid_fbs_method_for_year`); 2017 has no eGRID-backed FBS",
        "3. No stewi eGRID 2017 inventory for physical MWh",
        "",
        "## Results",
        "",
        *_scope_table("Baseline", baseline),
        *_scope_table("2017 single-year attempt", y2017),
        "## Correct identity: `BLy ≈ Σ_j D_j q_j` (not `BLy = E`)",
        "",
        "`BLy = diag(D) @ L_dom @ y_nab`. With balanced domestic IO the operative "
        "identities are:",
        "",
        "1. `L_dom @ y_nab ≈ q`",
        "2. therefore `BLy ≈ Σ_j D_j q_j`",
        "",
        "`BLy = E` is **not** an accounting identity of this pipeline — even without "
        "A/q scale/inflate (2017 attempt).",
        "",
        "### Identity checks",
        "",
        *_identity_table(baseline, y2017),
        "Both checks hold to numerical precision in baseline and the 2017 attempt "
        "(`‖q−L_dom@y‖/‖q‖` ~ 1e-16; `BLy/(Σ D·q)` = 1). `BLy/E` stays away from 1, "
        "especially in the electricity block under production years.",
        "",
        "### Why `BLy ≠ E`",
        "",
        "1. **`D` uses industry `x`, not commodity `q`.** "
        "`B = (E / x) @ Vnorm`, `D = sum_g B`. Then `D·q` equals `E` only if "
        "`q = x` (and Vnorm maps cleanly). Empirically `median(x/q)` is not 1; "
        "compare `Σ D·q` vs `E` vs `Σ D_USD·x` in the Results tables.",
        "2. **Electricity undilution / split.** Child-sector `D_110` is large; "
        "`BLy_110 ≈ D_110 · q_110` can diverge from the electricity inventory slice "
        "when `q_110` and `x_110` diverge — same mechanism as in "
        "`electricity_full_trace.md`.",
        "3. **2017 proxy gaps.** The single-year attempt still uses eGRID "
        f"{EGRID_MWH_PROXY_YEAR} MWh for mixed units and a gas-row split of 2017 "
        "aggregate electricity E (not facility eGRID), so it is not a pure "
        "same-source year.",
        "",
        "Remaining in 2017 for A/q removes price scale/inflate drift and tightens "
        "`BLy/E` nationally, but **does not make `BLy = E`**.",
        "",
        "## How year changes are handled (relevant components)",
        "",
        "| Component | Current mixed unit model implementation | Relevant disaggregation step(s) | Notes |",
        "|---|---|---|---|",
        f"| **Detail IO (V, U, A base, q base)** | BEA {detail_year} detail "
        f"(`usa_detail_original_year={detail_year}`) | Reallocation, 3-way split | "
        "Disagg Make/Use in Cornerstone space when waste/elec disagg on |",
        f"| **A, q (scaled)** | {aq_table_cell} | "
        "Reallocation, 3-way split, mixed units | "
        "`scale_a_matrix_with_useeio_method` skips scale/inflate (2017 probe) |",
        "| **y_nab** | Backcomputed from scaled/mixed `Adom` and `q` → same "
        "dollar/unit year as A/q | Reallocation, 3-way split, mixed units | "
        "Mixed: gen row/column physical |",
        "| **L / L_dom** | `(I−A)^−1` from scaled/mixed A | "
        "Reallocation, 3-way split, mixed units | Year enters only through A |",
        "| **E** | `usa_ghg_data_year` FBS **unless** electricity disaggregation → "
        f"**year-keyed eGRID FBS** at `{ghg_year}` | 3-way split | 2017 FBS exists "
        "but is unused in production disagg path |",
        f"| **x (B denominator)** | {x_table_cell} | 3-way split | Not the same "
        "series as scaled commodity `q` |",
        "| **Vnorm** | From uninflated V and `q =` column sums of that V | "
        "3-way split | Maps industry E/x → commodity B. Current elec-disagg "
        "flags: `apply_inflation_to_V=False`, "
        "`use_scaled_x_and_scaled_Vnorm_for_B=False` |",
        "| **B** | `(E/x) @ Vnorm`, then mixed-units `/ c_col` on gen column | "
        "3-way split, mixed units | Intensity year = E and x year. Current "
        f"elec-disagg flags: {b_flags} |",
        "| **D** | Column sums of B (gen: kg/MWh after mixed) | "
        "3-way split, mixed units | Follows B |",
        "| **N** | `D`-weighted Leontief (`B @ L` characterized) | "
        "Reallocation, 3-way split, mixed units | Mixes B year with A/L year |",
        "| **BLy** | `diag(D) @ L_dom @ y_nab` | "
        "Reallocation, 3-way split, mixed units | Couples B/D year to A/q year |",
        "| **Mixed `c_col`/`c_row`** | MWh from eGRID@`model_base_year`; prices "
        "Table 2.4@`usa_ghg_data_year` | mixed units | 2017 MWh missing in stewi |",
        "",
        "### Why D7 GO correction does not force electricity `q ≈ x`",
        "",
        "A common expectation is that because electricity child A/q rows are "
        "corrected with BEA detail GO (UGO305), scaled `q` should match the BEA GO "
        "`x` used in `B = (E / x) @ Vnorm`. That is not what D7 does.",
        "",
        f"**D7 does not set electricity `q` to {model_year} BEA GO.** It only "
        f"adjusts the {detail_year}→{io_year} **scale** step so each child's "
        "*growth ratio* matches UGO305 GO growth instead of the flat Utilities "
        '`"22"` summary ratio. After that, `q` is still **inflated '
        f"{io_year}→{model_year}**, {d7_inflate}",
        "",
        "Three mismatches remain:",
        "",
        "1. **Ratio correction ≠ level matching.** D7 multiplies already-scaled "
        f"{detail_year} detail `q` by "
        f"`(GO_i[{io_year}]/GO_i[{detail_year}]) / "
        f"(q_22[{io_year}]/q_22[{detail_year}])`. Absolute `q_i` still comes from "
        "the disaggregated Make/Use structure, not `q_i := GO_i`.",
        f"2. **Wrong year endpoint for GO.** Scale/D7 target is "
        f"`usa_io_data_year={io_year}`. The B denominator `x` is "
        f"**{ghg_year}** GHG-year industry GO (`usa_ghg_data_year`). The last hop "
        f"to {model_year} on `q` is PI inflate, not another UGO305 GO update.",
        "3. **Commodity `q` vs industry `x`.** Even same-year, `x` is industry GO "
        "(aggregate `221100` expanded/split with V shares), while `q` is commodity "
        "output after scale/inflate. For generation, `BLy` uses `D·q` and inventory "
        "recovery is `D·x` (`Σ D_USD·x = E` in the elec block) — so the diagnostic "
        "gap is still **`q ≠ x`**, not a failure of `L_dom @ y ≈ q`.",
        "",
        "In short: GO correction makes G/T/D **scale differently from each other** "
        f"through {io_year}; it does **not** force model-year commodity `q` onto "
        "the same BEA GO vector used as `x` in `B`. That residual `q`–`x` wedge is "
        "what opens electricity-block `BLy/E` under production scale/inflate.",
        "",
        "### eGRID MWh and EIA prices vs inflated A/q year",
        "",
        egrid_eia_lead,
        "",
        "| Input | Year source | Current value |",
        "|---|---|---:|",
        f"| Inflated A/q | `model_base_year` | {model_year} |",
        f"| eGRID MWh (`c_col`) | `model_base_year` | {model_year} |",
        f"| EIA Table 2.4 prices (`c_row`) | `usa_ghg_data_year` | {ghg_year} |",
        "",
        egrid_eia_trail,
        "",
        "## What it would take to build D & N for 2017–2024 (outline only)",
        "",
        "Do **not** implement here; requirements:",
        "",
        "1. **Config / API**",
        "   - Extend `usa_ghg_data_year` to include 2017–2018 (FBS already on GCS).",
        "   - Parameterize electricity E source: eGRID FBS by year when available; "
        "else year GHG FBS + documented G/T/D split.",
        "   - Parameterize eGRID MWh year (or accept nearest-year proxy with flags).",
        "",
        "2. **Per-year inputs**",
        "   - GHG FBS `GHG_national_Cornerstone_{y}` for each y.",
        "   - Industry `x(y)` from BEA GO (already time-series capable).",
        "   - A/q(y): either freeze 2017 structure (`useeio` method), or run "
        "scale/inflate / nowcast / summary-ratio path to each `model_base_year=y`.",
        "   - Table 2.4 prices(y); eGRID MWh(y) where stewi supports it "
        "(gap at 2017).",
        "",
        "3. **Per-year compute**",
        "   - For each y: build mixed-units A/q (if desired), B(y)=E(y)/x(y)@Vnorm, "
        "D(y), L(y), N(y).",
        "   - Decide whether V/U/disagg weights are frozen at 2017 or year-specific "
        "(GO shares / Table 8.3).",
        "",
        "4. **Comparability rules**",
        "   - Report D/N in USD-equiv (apply `c_col` back-conversion for gen) for "
        "cross-year charts.",
        "   - Document dollar year of denominators (GHG-year x vs model-year q).",
        "   - Separate structural change (A) from inventory change (E) and price change "
        "(inflation).",
        "",
        "5. **Validation**",
        "   - Track `BLy` vs `E` and `Σ D·q` each year (expect persistent gap).",
        "   - Smoke-test 2017/2018/2022/2023/2024 against known mixed-units anchors.",
        "",
        "## Reproduce",
        "",
        "```",
        "python -m bedrock.analysis.electricity_disagg_diagnostics.year_alignment.year_alignment_bly_e",
        "```",
        "",
        f"Writes `{REPORT_MD.as_posix()}` and `{REPORT_JSON.as_posix()}`.",
        "",
        "Re-render markdown only (from existing JSON):",
        "",
        "```",
        "python -m bedrock.analysis.electricity_disagg_diagnostics.year_alignment.year_alignment_bly_e "
        "--report-only",
        "```",
        "",
    ]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Probe BLy vs E year alignment under mixed units."
    )
    parser.add_argument(
        "--report-only",
        action="store_true",
        help="Re-render markdown from existing JSON (skip model runs).",
    )
    args = parser.parse_args(argv)

    PANEL_DIR.mkdir(parents=True, exist_ok=True)

    if args.report_only:
        if not REPORT_JSON.is_file():
            raise SystemExit(f"Missing {REPORT_JSON}; run without --report-only first.")
        payload = json.loads(REPORT_JSON.read_text(encoding="utf-8"))
        REPORT_MD.write_text(
            render_report(payload["baseline"], payload["year_2017_attempt"]),
            encoding="utf-8",
        )
        print(f"Wrote {REPORT_MD} from {REPORT_JSON}")
        return

    print("=== Baseline mixed (v0.3.1 electricity) ===")
    baseline = run_scenario(
        "baseline_mixed_v0_3",
        overrides={},
        use_2017_e_patch=False,
        use_egrid_mwh_proxy=False,
    )
    print(
        "national BLy/E=",
        baseline["national"]["BLy_Mt"] / baseline["national"]["E_Mt"],
        "elec BLy/E=",
        baseline["electricity_block"]["BLy_Mt"] / baseline["electricity_block"]["E_Mt"],
    )

    print("=== 2017 single-year attempt ===")
    y2017 = run_scenario(
        "mixed_2017_single_year_attempt",
        overrides={
            "model_base_year": 2017,
            "usa_io_data_year": 2017,
            "usa_ghg_data_year": 2017,
            "apply_io_year_adjustments": False,
            "scale_a_matrix_with_useeio_method": True,
            "scale_a_matrix_with_ceda_method_as_fallback": False,
            "adjust_summary_A_and_q_dollar_year": False,
        },
        use_2017_e_patch=True,
        use_egrid_mwh_proxy=True,
    )
    print(
        "national BLy/E=",
        y2017["national"]["BLy_Mt"] / y2017["national"]["E_Mt"],
        "elec BLy/E=",
        y2017["electricity_block"]["BLy_Mt"] / y2017["electricity_block"]["E_Mt"],
    )

    payload = {"baseline": baseline, "year_2017_attempt": y2017}
    REPORT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    REPORT_MD.write_text(render_report(baseline, y2017), encoding="utf-8")
    print(f"Wrote {REPORT_MD}")
    print(f"Wrote {REPORT_JSON}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    main()
