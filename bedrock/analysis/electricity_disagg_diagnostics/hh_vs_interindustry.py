"""Household (F01000) vs interindustry MWh and BLy under mixed units.

Splits total attributed production ``BLy = diag(D) @ L_dom @ y_nab`` into
household personal consumption (BEA ``F01000``; often called F001) vs all other
final demand (which induces interindustry activity via ``L_dom``), and reports
221110 MWh to intermediate purchasers vs ``F01000``.

Compares generation MWh slices to EIA Electric Power Annual Table 2.2 (2023)
residential / commercial / industrial sales.

Run:
    python -m bedrock.analysis.electricity_disagg_diagnostics.hh_vs_interindustry
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

from bedrock.analysis.electricity_disagg_diagnostics.full_trace import (
    _clear_model_caches,
)
from bedrock.analysis.electricity_disagg_diagnostics.paths import OUT_DIR
from bedrock.publish.model_objects import get_B, get_D, get_Ldom
from bedrock.transform.eeio.cornerstone_disagg_pipeline import (
    derive_disagg_Ytot_with_trade,
    electricity_conversion_factors,
)
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_Aq_mixed_units,
    derive_cornerstone_Aq_scaled,
)
from bedrock.transform.eeio.electricity_disaggregation import GENERATION_SECTOR
from bedrock.utils.config.usa_config import (
    CONFIG_DIR,
    USAConfig,
    reset_usa_config,
)
from bedrock.utils.math.formulas import backcompute_y_from_A_and_q
from bedrock.utils.taxonomy.bea.v2017_final_demand import (
    USA_2017_FINAL_DEMAND_IMPORT_CODE,
    USA_2017_FINAL_DEMAND_PERSONAL_CONSUMPTION_EXPENDITURE_CODE,
)
from bedrock.utils.validation.calculate_national_accounting_balance_diagnostics import (
    _compute_bly_series,
)

logger = logging.getLogger(__name__)

MIXED_CONFIG = "2025_usa_cornerstone_v0_2_electricity_mixed_units"
# User "F001" → BEA PCE code in this schema.
HH_FD_CODE = USA_2017_FINAL_DEMAND_PERSONAL_CONSUMPTION_EXPENDITURE_CODE  # F01000

OUT_SUBDIR = OUT_DIR / "hh_vs_interindustry"
REPORT_MD = OUT_SUBDIR / "hh_vs_interindustry_mwh_bly.md"
REPORT_JSON = OUT_SUBDIR / "hh_vs_interindustry_mwh_bly.json"


def _mt(kg: float) -> float:
    return float(kg) / 1e9


def _install_mixed_config() -> USAConfig:
    import bedrock.utils.config.usa_config as uc

    reset_usa_config()
    with open(Path(CONFIG_DIR) / f"{MIXED_CONFIG}.yaml", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    merged = USAConfig().model_dump(mode="python")
    merged.update(data)
    cfg = USAConfig.model_construct(**merged)
    uc._usa_config = cfg
    return cfg


def _series_d_total(D_df: pd.DataFrame) -> pd.Series[float]:
    s = D_df.sum(axis=0).astype(float)
    s.index = s.index.astype(str)
    return s


def _bly_total_mt(D: pd.Series[float], Ldom: pd.DataFrame, y: pd.Series[float]) -> float:
    ly = pd.Series(
        Ldom.to_numpy() @ y.reindex(Ldom.columns).fillna(0.0).to_numpy(),
        index=Ldom.index,
        dtype=float,
    )
    d = D.reindex(ly.index).fillna(0.0).astype(float)
    return _mt(float((d * ly).sum()))


def _fd_share_matrix(Y: pd.DataFrame) -> pd.DataFrame:
    """Non-negative column shares of domestic FD (exclude imports) per commodity."""
    cols = [c for c in Y.columns if str(c) != USA_2017_FINAL_DEMAND_IMPORT_CODE]
    Y_dom = Y.reindex(columns=cols).astype(float).fillna(0.0)
    # Clip negatives to 0 for share weights only (inventory / scrap quirks).
    W = Y_dom.clip(lower=0.0)
    denom = W.sum(axis=1).replace(0.0, np.nan)
    return W.div(denom, axis=0).fillna(0.0)


def _eia_table_2_2_sales_mwh(year: int) -> dict[str, float]:
    from bedrock.extract.flowbyactivity import getFlowByActivity  # noqa: PLC0415

    df = getFlowByActivity("EIA_ElectricPowerAnnual", year)
    mask = (
        df["Description"].astype(str).str.contains("Table 2.2", na=False)
        & (df["ActivityProducedBy"] == "Total Electric Industry")
        & (df["Year"] == year)
    )
    sub = df.loc[mask]
    out: dict[str, float] = {}
    for _, row in sub.iterrows():
        out[str(row["ActivityConsumedBy"])] = float(row["FlowAmount"])
    return out


def analyze() -> dict[str, Any]:
    _clear_model_caches()
    cfg = _install_mixed_config()

    aq_scaled = derive_cornerstone_Aq_scaled()
    c_col, _ = electricity_conversion_factors(aq_scaled)
    aq = derive_cornerstone_Aq_mixed_units()
    q = aq.scaled_q.astype(float)
    q.index = q.index.astype(str)
    Adom = aq.Adom.copy()
    Adom.index = Adom.index.astype(str)
    Adom.columns = Adom.columns.astype(str)

    y_nab = backcompute_y_from_A_and_q(A=Adom, q=q)
    y_nab.index = y_nab.index.astype(str)

    D = _series_d_total(get_D())
    Ldom = get_Ldom()
    Ldom.index = Ldom.index.astype(str)
    Ldom.columns = Ldom.columns.astype(str)
    Ldom = Ldom.reindex(index=q.index, columns=q.index).fillna(0.0)

    B = get_B()
    bly_series = _compute_bly_series(B=B, Adom=Adom, y=y_nab)
    bly_total_mt = _mt(float(bly_series.sum()))

    # --- BLy split: F01000 vs other FD via 2017 Y composition shares on y_nab ---
    Y2017 = derive_disagg_Ytot_with_trade().copy()
    Y2017.index = Y2017.index.astype(str)
    shares = _fd_share_matrix(Y2017)
    hh_share = shares[HH_FD_CODE].reindex(y_nab.index).fillna(0.0)
    y_hh = y_nab * hh_share
    y_other = y_nab - y_hh

    bly_hh_mt = _bly_total_mt(D, Ldom, y_hh)
    bly_other_mt = _bly_total_mt(D, Ldom, y_other)

    # --- 221110 MWh: intermediate vs FD (F01000 vs other FD) ---
    gen = GENERATION_SECTOR
    u_row = Adom.loc[gen].astype(float) * q.astype(float)
    mwh_intermediate = float(u_row.sum())
    mwh_y_total = float(y_nab.loc[gen])
    mwh_q = float(q.loc[gen])

    y_gen_shares = shares.loc[gen] if gen in shares.index else shares.reindex([gen]).iloc[0]
    mwh_hh = mwh_y_total * float(y_gen_shares.get(HH_FD_CODE, 0.0))
    mwh_other_fd = mwh_y_total - mwh_hh

    # Generation-sector attributed BLy (commodity 221110 activity)
    bly_gen_mt = _mt(float(bly_series.reindex([gen]).fillna(0.0).sum()))
    # Allocate gen BLy by MWh use shares (intermediate vs HH FD vs other FD)
    mwh_use_total = mwh_intermediate + mwh_y_total
    if mwh_use_total > 0:
        bly_gen_inter_mt = bly_gen_mt * (mwh_intermediate / mwh_use_total)
        bly_gen_hh_mt = bly_gen_mt * (mwh_hh / mwh_use_total)
        bly_gen_other_fd_mt = bly_gen_mt * (mwh_other_fd / mwh_use_total)
    else:
        bly_gen_inter_mt = bly_gen_hh_mt = bly_gen_other_fd_mt = float("nan")

    eia = _eia_table_2_2_sales_mwh(int(cfg.model_base_year))
    eia_res = eia.get("Residential", float("nan"))
    eia_com = eia.get("Commercial", float("nan"))
    eia_ind = eia.get("Industrial", float("nan"))
    eia_trans = eia.get("Transportation", float("nan"))
    eia_direct = eia.get("Direct Use", float("nan"))
    eia_total_end = eia.get("Total End Use", float("nan"))
    eia_nonres = eia_com + eia_ind + eia_trans  # sales excl. residential & direct use

    payload: dict[str, Any] = {
        "config": MIXED_CONFIG,
        "model_base_year": int(cfg.model_base_year),
        "usa_ghg_data_year": int(cfg.usa_ghg_data_year),
        "hh_fd_code": HH_FD_CODE,
        "hh_fd_label": "Personal consumption expenditures (households)",
        "c_col": float(c_col),
        "bly": {
            "total_Mt": bly_total_mt,
            "household_F01000_Mt": bly_hh_mt,
            "other_final_demand_Mt": bly_other_mt,
            "household_share": bly_hh_mt / bly_total_mt if bly_total_mt else float("nan"),
            "other_fd_share": bly_other_mt / bly_total_mt if bly_total_mt else float("nan"),
            "recon_total_minus_parts_Mt": bly_total_mt - bly_hh_mt - bly_other_mt,
            "method": (
                "y_hh = y_nab * (Y_2017[F01000] / sum_domestic_FD Y_2017)_i; "
                "BLy_hh = 1^T diag(D) L_dom y_hh; other = y_nab - y_hh"
            ),
        },
        "mwh_221110": {
            "q_generation_MWh": mwh_q,
            "intermediate_MWh": mwh_intermediate,
            "final_demand_total_MWh": mwh_y_total,
            "household_F01000_MWh": mwh_hh,
            "other_final_demand_MWh": mwh_other_fd,
            "intermediate_plus_fd_MWh": mwh_use_total,
            "q_minus_uses_MWh": mwh_q - mwh_use_total,
            "intermediate_share_of_uses": mwh_intermediate / mwh_use_total
            if mwh_use_total
            else float("nan"),
            "household_share_of_uses": mwh_hh / mwh_use_total
            if mwh_use_total
            else float("nan"),
        },
        "bly_generation_sector_allocated_by_mwh_uses": {
            "bly_221110_Mt": bly_gen_mt,
            "allocated_to_intermediate_Mt": bly_gen_inter_mt,
            "allocated_to_household_F01000_Mt": bly_gen_hh_mt,
            "allocated_to_other_fd_Mt": bly_gen_other_fd_mt,
            "note": (
                "Splits commodity-221110 BLy by share of 221110 MWh uses "
                "(intermediate vs F01000 vs other FD), not the full national BLy split."
            ),
        },
        "eia_table_2_2": {
            "year": int(cfg.model_base_year),
            "source": "EIA_ElectricPowerAnnual Table 2.2 (Total Electric Industry)",
            "Residential_MWh": eia_res,
            "Commercial_MWh": eia_com,
            "Industrial_MWh": eia_ind,
            "Transportation_MWh": eia_trans,
            "Direct_Use_MWh": eia_direct,
            "Total_End_Use_MWh": eia_total_end,
            "Commercial_plus_Industrial_plus_Transportation_MWh": eia_nonres,
        },
        "comparisons": {
            "model_hh_MWh_vs_eia_residential": {
                "model_F01000_MWh": mwh_hh,
                "eia_Residential_MWh": eia_res,
                "ratio_model_over_eia": mwh_hh / eia_res if eia_res else float("nan"),
            },
            "model_intermediate_MWh_vs_eia_nonresidential_sales": {
                "model_intermediate_MWh": mwh_intermediate,
                "eia_Com_Ind_Trans_MWh": eia_nonres,
                "ratio_model_over_eia": mwh_intermediate / eia_nonres
                if eia_nonres
                else float("nan"),
            },
            "model_q_gen_vs_eia_total_end_use": {
                "model_q_221110_MWh": mwh_q,
                "eia_Total_End_Use_MWh": eia_total_end,
                "ratio_model_over_eia": mwh_q / eia_total_end
                if eia_total_end
                else float("nan"),
            },
        },
    }
    return payload


def _fmt_mwh(x: float) -> str:
    return f"{x / 1e6:,.1f}"


def _fmt_mt(x: float) -> str:
    return f"{x:,.2f}"


def render_report(p: dict[str, Any]) -> str:
    b = p["bly"]
    m = p["mwh_221110"]
    g = p["bly_generation_sector_allocated_by_mwh_uses"]
    e = p["eia_table_2_2"]
    c = p["comparisons"]
    year = p["model_base_year"]

    lines = [
        "# Household vs interindustry — MWh demand and BLy (mixed units)",
        "",
        f"Config: `{p['config']}` (`model_base_year={year}`, "
        f"`usa_ghg_data_year={p['usa_ghg_data_year']}`).",
        "",
        f"Household final demand is BEA **`{p['hh_fd_code']}`** "
        f"({p['hh_fd_label']}). In conversation this is the “F001” / household PCE "
        "bucket; Cornerstone/BEA code is `F01000`, not a literal `F001` column.",
        "",
        "## Framing",
        "",
        "Total attributed production is `BLy = diag(D) @ L_dom @ y_nab`. In EEIO, "
        "**all** `BLy` is attributed to final demand; intermediate transactions are "
        "endogenous in `L_dom @ y`. So:",
        "",
        f"1. **Household emissions** = `BLy` from the `{HH_FD_CODE}` piece of `y_nab`.",
        "2. **“Interindustry” emissions** here = `BLy` from **all other final-demand** "
        "columns (investment, government, exports, …). Those FD vectors induce "
        "interindustry electricity and other purchases through `L_dom`.",
        "3. **Related MWh** for generation commodity `221110`: intermediate row uses "
        f"`Adom[221110] ⊙ q` vs FD uses of `221110` split with 2017 Y shares "
        f"(including `{HH_FD_CODE}`).",
        "",
        "## 1–2. Model results",
        "",
        "### National BLy split (full economy)",
        "",
        "| Bucket | BLy (Mt CO2e) | Share of total BLy |",
        "|---|---:|---:|",
        f"| Household (`{HH_FD_CODE}`) | {_fmt_mt(b['household_F01000_Mt'])} | "
        f"{100 * b['household_share']:.1f}% |",
        f"| Other final demand (induces interindustry) | "
        f"{_fmt_mt(b['other_final_demand_Mt'])} | {100 * b['other_fd_share']:.1f}% |",
        f"| **Total BLy** | **{_fmt_mt(b['total_Mt'])}** | **100%** |",
        "",
        f"Reconciliation `total − hh − other` = "
        f"{b['recon_total_minus_parts_Mt']:.3e} Mt (should be ~0).",
        "",
        f"Method: `{b['method']}`.",
        "",
        "### Generation commodity (221110) MWh uses",
        "",
        "| Use of 221110 | MWh | TWh | Share of uses |",
        "|---|---:|---:|---:|",
        f"| Intermediate (all industries) | {m['intermediate_MWh']:,.0f} | "
        f"{_fmt_mwh(m['intermediate_MWh'])} | "
        f"{100 * m['intermediate_share_of_uses']:.1f}% |",
        f"| Household FD (`{HH_FD_CODE}`) | {m['household_F01000_MWh']:,.0f} | "
        f"{_fmt_mwh(m['household_F01000_MWh'])} | "
        f"{100 * m['household_share_of_uses']:.1f}% |",
        f"| Other final demand | {m['other_final_demand_MWh']:,.0f} | "
        f"{_fmt_mwh(m['other_final_demand_MWh'])} | "
        f"{100 * (m['other_final_demand_MWh'] / m['intermediate_plus_fd_MWh']):.1f}% |",
        f"| **Uses total** | **{m['intermediate_plus_fd_MWh']:,.0f}** | "
        f"**{_fmt_mwh(m['intermediate_plus_fd_MWh'])}** | **100%** |",
        f"| `q_221110` (mixed-units output) | {m['q_generation_MWh']:,.0f} | "
        f"{_fmt_mwh(m['q_generation_MWh'])} | — |",
        f"| `q − uses` | {m['q_minus_uses_MWh']:,.0f} | "
        f"{_fmt_mwh(m['q_minus_uses_MWh'])} | — |",
        "",
        "### Generation-sector BLy allocated by those MWh shares",
        "",
        "This is **not** the national BLy split above; it only apportions "
        f"`BLy_221110 = {_fmt_mt(g['bly_221110_Mt'])}` Mt by 221110 MWh use shares.",
        "",
        "| Allocation of BLy_221110 | Mt CO2e |",
        "|---|---:|",
        f"| Intermediate MWh | {_fmt_mt(g['allocated_to_intermediate_Mt'])} |",
        f"| Household `{HH_FD_CODE}` MWh | "
        f"{_fmt_mt(g['allocated_to_household_F01000_Mt'])} |",
        f"| Other FD MWh | {_fmt_mt(g['allocated_to_other_fd_Mt'])} |",
        "",
        f"Note: {g['note']}",
        "",
        f"## 3. Comparison to EIA Table 2.2 ({year})",
        "",
        f"Source already in-model: `{e['source']}` via "
        "`getFlowByActivity('EIA_ElectricPowerAnnual', year)`.",
        "",
        "| EIA sector | MWh | TWh |",
        "|---|---:|---:|",
        f"| Residential | {e['Residential_MWh']:,.0f} | "
        f"{_fmt_mwh(e['Residential_MWh'])} |",
        f"| Commercial | {e['Commercial_MWh']:,.0f} | {_fmt_mwh(e['Commercial_MWh'])} |",
        f"| Industrial | {e['Industrial_MWh']:,.0f} | {_fmt_mwh(e['Industrial_MWh'])} |",
        f"| Transportation | {e['Transportation_MWh']:,.0f} | "
        f"{_fmt_mwh(e['Transportation_MWh'])} |",
        f"| Direct use | {e['Direct_Use_MWh']:,.0f} | {_fmt_mwh(e['Direct_Use_MWh'])} |",
        f"| **Total end use** | **{e['Total_End_Use_MWh']:,.0f}** | "
        f"**{_fmt_mwh(e['Total_End_Use_MWh'])}** |",
        "",
        "### Model vs EIA",
        "",
        "| Comparison | Model | EIA | Model/EIA |",
        "|---|---:|---:|---:|",
        f"| Household `{HH_FD_CODE}` MWh vs Residential | "
        f"{_fmt_mwh(c['model_hh_MWh_vs_eia_residential']['model_F01000_MWh'])} TWh | "
        f"{_fmt_mwh(c['model_hh_MWh_vs_eia_residential']['eia_Residential_MWh'])} TWh | "
        f"{c['model_hh_MWh_vs_eia_residential']['ratio_model_over_eia']:.3f} |",
        f"| Intermediate MWh vs Com+Ind+Trans sales | "
        f"{_fmt_mwh(c['model_intermediate_MWh_vs_eia_nonresidential_sales']['model_intermediate_MWh'])} TWh | "
        f"{_fmt_mwh(c['model_intermediate_MWh_vs_eia_nonresidential_sales']['eia_Com_Ind_Trans_MWh'])} TWh | "
        f"{c['model_intermediate_MWh_vs_eia_nonresidential_sales']['ratio_model_over_eia']:.3f} |",
        f"| `q_221110` vs Total end use | "
        f"{_fmt_mwh(c['model_q_gen_vs_eia_total_end_use']['model_q_221110_MWh'])} TWh | "
        f"{_fmt_mwh(c['model_q_gen_vs_eia_total_end_use']['eia_Total_End_Use_MWh'])} TWh | "
        f"{c['model_q_gen_vs_eia_total_end_use']['ratio_model_over_eia']:.3f} |",
        "",
        "### Comparability notes",
        "",
        "- EIA **Residential** ≈ model household electricity purchases, but PCE "
        f"`{HH_FD_CODE}` is an IO final-demand construct (producer/purchaser and "
        "margin treatment differ from utility sales).",
        "- EIA **Commercial + Industrial (+ Transportation)** is the closest published "
        "sales analog to model **intermediate** 221110 use; IO intermediate also "
        "includes electricity used by utilities and other sectors that EIA may classify "
        "differently, and excludes some FD electricity (gov, investment).",
        "- Model `q_221110` is mixed-units generation **output** (eGRID net generation "
        "scaled via `c_col`), while EIA total end use is **sales + direct use** — "
        "related but not identical (losses, exports/imports, self-generation).",
        "",
        "## Reproduce",
        "",
        "```",
        "python -m bedrock.analysis.electricity_disagg_diagnostics.hh_vs_interindustry",
        "```",
        "",
        f"Writes `{REPORT_MD.as_posix()}` and `{REPORT_JSON.as_posix()}`.",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    OUT_SUBDIR.mkdir(parents=True, exist_ok=True)
    payload = analyze()
    REPORT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    REPORT_MD.write_text(render_report(payload), encoding="utf-8")
    print(f"Wrote {REPORT_MD}")
    print(f"Wrote {REPORT_JSON}")
    b = payload["bly"]
    m = payload["mwh_221110"]
    print(
        f"BLy total={b['total_Mt']:.2f} Mt; "
        f"HH={b['household_F01000_Mt']:.2f} ({100 * b['household_share']:.1f}%); "
        f"other FD={b['other_final_demand_Mt']:.2f}"
    )
    print(
        f"MWh inter={m['intermediate_MWh'] / 1e6:.1f} TWh; "
        f"HH={m['household_F01000_MWh'] / 1e6:.1f} TWh; "
        f"q={m['q_generation_MWh'] / 1e6:.1f} TWh"
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    main()
