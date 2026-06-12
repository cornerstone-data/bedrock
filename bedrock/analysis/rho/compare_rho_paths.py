"""Compare Rho / commodity PI derivations across useeior, bedrock-useeior-style, and V_norm.

Writes numeric stage-by-stage comparisons to bedrock/analysis/rho/output/ and
summary stats for the Rho analysis report.

Does not modify production bedrock code.
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Repo root on sys.path
_REPO = Path(__file__).resolve().parents[3]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from bedrock.transform.eeio.derived_cornerstone import (  # noqa: E402
    derive_cornerstone_V,
    derive_cornerstone_Vnorm_scrap_corrected,
)
from bedrock.utils.config.config_controllers import temp_usa_config  # noqa: E402
from bedrock.utils.economic.inflation_helpers_cornerstone import (  # noqa: E402
    _cornerstone_indexed_industry_pi,
    get_cornerstone_industry_price_ratio,
    get_vnorm_adjusted_commodity_price_ratio,
)
from bedrock.utils.math.formulas import compute_Vnorm_matrix, compute_q  # noqa: E402
from bedrock.utils.snapshots.loader import useeio_baseline_local_dir  # noqa: E402
from bedrock.utils.validation.useeio_excel_baseline import (  # noqa: E402
    ensure_useeio_xlsx_local,
    load_useeio_baseline_pin_overrides,
)

OUT = Path(__file__).resolve().parent / "output"
OUT.mkdir(parents=True, exist_ok=True)

IO_YEAR = 2017
COMPARE_YEARS = [2017, 2019, 2022, 2023, 2024]
_PIN_JSON = _REPO / "bedrock" / "utils" / "snapshots" / "useeio_baseline_pin.json"

_CACHE_MODULES = (
    "bedrock.transform.eeio.derived_cornerstone",
    "bedrock.utils.economic.inflation_helpers_cornerstone",
    "bedrock.transform.iot.derived_price_index",
)


def _strip_us(code: str) -> str:
    code = str(code).strip()
    return code[: -len("/US")] if code.endswith("/US") else code


def _load_matrix_sheet(path: str, sheet: str) -> pd.DataFrame:
    raw = pd.read_excel(path, sheet_name=sheet, header=None, engine="openpyxl")
    cols = raw.iloc[0, 1:].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    rows = raw.iloc[1:, 0].astype(str).str.strip()
    mat = raw.iloc[1:, 1:].astype(float)
    mat.index = [_strip_us(r) for r in rows]
    mat.columns = cols
    return mat


def _load_useeior_exports() -> dict[str, pd.DataFrame]:
    paths = {
        "industry_cpi": OUT / "useeior_MultiYearIndustryCPI.csv",
        "commodity_cpi": OUT / "useeior_MultiYearCommodityCPI.csv",
        "market_shares": OUT / "useeior_market_shares.csv",
        "rho": OUT / "useeior_Rho.csv",
    }
    missing = [p for p in paths.values() if not p.is_file()]
    if missing:
        raise FileNotFoundError(
            "Run export_useeior_cpi_rho.R first. Missing: "
            + ", ".join(p.name for p in missing)
        )
    out: dict[str, pd.DataFrame] = {}
    for key, p in paths.items():
        df = pd.read_csv(p, index_col=0)
        df.index = [_strip_us(i) for i in df.index]
        df.columns = [int(float(c)) if str(c).replace(".", "", 1).isdigit() else c for c in df.columns]
        if key != "market_shares":
            df.columns = pd.Index([int(c) for c in df.columns])
        out[key] = df
    return out


def _load_phoebe_workbook_rho() -> pd.DataFrame | None:
    if not _PIN_JSON.is_file():
        return None
    pin = load_useeio_baseline_pin_overrides(str(_PIN_JSON))
    gs_uri = pin["useeio_baseline_xlsx_gs_uri"]
    sha = pin["useeio_baseline_xlsx_sha256"]
    safe = re.sub(
        r"[^a-zA-Z0-9_.-]+",
        "_",
        gs_uri.removeprefix("gs://cornerstone-default/"),
    )
    local = os.path.join(
        useeio_baseline_local_dir(),
        safe if safe.lower().endswith(".xlsx") else f"{safe}.xlsx",
    )
    try:
        ensure_useeio_xlsx_local(gs_uri, sha, local)
    except Exception as exc:  # noqa: BLE001
        print(f"Could not load phoebe workbook: {exc}")
        return None
    try:
        return _load_matrix_sheet(local, "Rho")
    except Exception as exc:  # noqa: BLE001
        print(f"No Rho sheet in workbook: {exc}")
        return None


def _bedrock_industry_cpi_panel(years: list[int]) -> pd.DataFrame:
    """Cornerstone industry CPI levels (BEA-derived when update_inflation_factors)."""
    rows = {y: _cornerstone_indexed_industry_pi(y) for y in years}
    panel = pd.DataFrame(rows)
    panel.columns = pd.Index([int(c) for c in panel.columns])
    return panel


def _market_shares_no_scrap(V: pd.DataFrame, q: pd.Series) -> pd.DataFrame:
    """useeior generateMarketSharesfromMake: D = V @ diag(1/q), no scrap adjustment."""
    return compute_Vnorm_matrix(V=V, q=q)


def _market_shares_scrap_corrected() -> pd.DataFrame:
    return derive_cornerstone_Vnorm_scrap_corrected(
        apply_inflation=False, target_year=0
    )


def _commodity_cpi_from_weights(
    industry_cpi: pd.DataFrame, weights: pd.DataFrame
) -> pd.DataFrame:
    """Commodity CPI levels: pi_com[:, y] = W.T @ pi_ind[:, y] (useeior convention)."""
    W = weights.reindex(industry_cpi.index, axis=0).reindex(
        industry_cpi.index, axis=1, fill_value=0.0
    )
    # industry_cpi: industries x years; W: industries x commodities
    com = W.T.values @ industry_cpi.values  # commodities x years
    out = pd.DataFrame(
        com,
        index=W.columns,
        columns=pd.Index([int(c) for c in industry_cpi.columns]),
    )
    out[out == 0] = 100.0
    return out


def _int_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = pd.Index([int(c) for c in out.columns])
    return out


def _rho_from_cpi_levels(cpi: pd.DataFrame, io_year: int) -> pd.DataFrame:
    cpi = _int_columns(cpi)
    base = cpi[io_year]
    cols = {}
    for y in cpi.columns:
        cols[int(y)] = base / cpi[y]
    return pd.DataFrame(cols)


def _rho_from_ratio_of_ratios(
    industry_cpi: pd.DataFrame,
    weights_normed: pd.DataFrame,
    io_year: int,
) -> pd.DataFrame:
    """Bedrock get_vnorm_adjusted_commodity_price_ratio applied per target year."""
    cols = {}
    for y in industry_cpi.columns:
        if y == io_year:
            cols[y] = pd.Series(1.0, index=weights_normed.columns)
            continue
        r_ind = industry_cpi[y] / industry_cpi[io_year]
        r_ind = r_ind.reindex(weights_normed.index, fill_value=1.0)
        w = weights_normed.div(weights_normed.sum(axis=0).replace(0, 1.0), axis=1)
        r_com = r_ind @ w
        cols[y] = 1.0 / r_com  # inverse: IOYear/y dollars ratio = 1 / (y->IOYear infl)
    return pd.DataFrame(cols)


def _compare_series(
    a: pd.Series,
    b: pd.Series,
    label_a: str,
    label_b: str,
) -> dict:
    common = a.index.intersection(b.index)
    if len(common) == 0:
        return {"n": 0, "label_a": label_a, "label_b": label_b}
    da = a.reindex(common).astype(float)
    db = b.reindex(common).astype(float)
    diff = da - db
    rel = diff / db.replace(0, np.nan)
    return {
        "label_a": label_a,
        "label_b": label_b,
        "n": len(common),
        "mean_abs_diff": float(diff.abs().mean()),
        "max_abs_diff": float(diff.abs().max()),
        "median_abs_pct_diff": float((rel.abs() * 100).median()),
        "max_abs_pct_diff": float((rel.abs() * 100).max()),
        "corr": float(da.corr(db)) if len(common) > 2 else float("nan"),
    }


def _compare_at_year(
    rho_a: pd.DataFrame,
    rho_b: pd.DataFrame,
    year: int,
    label_a: str,
    label_b: str,
) -> dict:
    if year not in rho_a.columns or year not in rho_b.columns:
        return {"year": year, "skipped": True}
    return {"year": year, **_compare_series(rho_a[year], rho_b[year], label_a, label_b)}


def main() -> None:
    useeior = _load_useeior_exports()
    phoebe_rho = _load_phoebe_workbook_rho()

    configs = {
        "phoebe": "useeio_phoebe_23",
        "commodity_pi": "2025_usa_cornerstone_A_commodity_price_index",
        "full_model": "2025_usa_cornerstone_full_model",
    }

    # Bedrock derive_industry_price_index covers 2012–2025; intersect with useeior.
    bedrock_years = set(range(2012, 2026))
    useeior_years = set(int(y) for y in useeior["industry_cpi"].columns)
    all_years = sorted(bedrock_years & useeior_years)

    summary: dict = {"io_year": IO_YEAR, "comparisons": [], "configs": {}}

    # --- Path A: useeior R (recomputed Rho from exported CPI) ---
    useeior_rho_recalc = _rho_from_cpi_levels(useeior["commodity_cpi"], IO_YEAR)
    rho_check = _compare_series(
        useeior_rho_recalc[2022],
        useeior["rho"][2022],
        "useeior_Rho_recalc",
        "useeior_Rho_exported",
    )
    summary["useeior_rho_internal_check_2022"] = rho_check
    useeior_rho_recalc.to_csv(OUT / "path_a_useeior_rho_recalc.csv")

    # --- Bedrock paths per config ---
    for cfg_label, cfg_name in configs.items():
        with temp_usa_config(cfg_name, cache_bearing_modules=_CACHE_MODULES):
            ind_panel = _bedrock_industry_cpi_panel(all_years)
            V = derive_cornerstone_V()
            q = compute_q(V=V)
            ms_plain = _market_shares_no_scrap(V, q)
            ms_scrap = _market_shares_scrap_corrected()

            # Path B: bedrock useeior-style (market shares, no scrap; level CPI then ratio)
            com_cpi_ms = _commodity_cpi_from_weights(ind_panel, ms_plain)
            rho_b = _rho_from_cpi_levels(com_cpi_ms, IO_YEAR)

            # Path C1: V_norm scrap-corrected weights, ratio-of-levels
            com_cpi_vnorm = _commodity_cpi_from_weights(ind_panel, ms_scrap)
            rho_c_levels = _rho_from_cpi_levels(com_cpi_vnorm, IO_YEAR)

            # Path C2: V_norm column-renormalized, ratio-of-ratios (production helper)
            rho_c_ratio = pd.DataFrame(
                {
                    y: (
                        pd.Series(1.0, index=ms_scrap.columns)
                        if y == IO_YEAR
                        else 1.0
                        / get_vnorm_adjusted_commodity_price_ratio(IO_YEAR, y)
                    )
                    for y in all_years
                    if y in ind_panel.columns
                }
            )

            ind_panel.to_csv(OUT / f"bedrock_{cfg_label}_industry_cpi.csv")
            com_cpi_ms.to_csv(OUT / f"bedrock_{cfg_label}_commodity_cpi_market_shares.csv")
            com_cpi_vnorm.to_csv(OUT / f"bedrock_{cfg_label}_commodity_cpi_vnorm.csv")
            rho_b.to_csv(OUT / f"path_b_{cfg_label}_rho.csv")
            rho_c_levels.to_csv(OUT / f"path_c1_{cfg_label}_rho_levels.csv")
            rho_c_ratio.to_csv(OUT / f"path_c2_{cfg_label}_rho_ratio.csv")

            cfg_summary = {"config": cfg_name}
            for year in COMPARE_YEARS:
                if year not in useeior["rho"].columns:
                    continue
                # Map useeior codes to cornerstone via string match
                u_rho = useeior["rho"][year]
                cfg_summary[f"vs_useeior_{year}"] = {
                    "path_b": _compare_series(
                        rho_b[year], u_rho, f"bedrock_ms/{cfg_label}", "useeior"
                    ),
                    "path_c1_vnorm_levels": _compare_series(
                        rho_c_levels[year], u_rho, f"bedrock_vnorm_levels/{cfg_label}", "useeior"
                    ),
                    "path_c2_vnorm_ratio": _compare_series(
                        rho_c_ratio[year], u_rho, f"bedrock_vnorm_ratio/{cfg_label}", "useeior"
                    ),
                }
                cfg_summary[f"path_c1_vs_c2_{year}"] = _compare_series(
                    rho_c_levels[year],
                    rho_c_ratio[year],
                    "vnorm_levels",
                    "vnorm_ratio_of_ratios",
                )
            summary["configs"][cfg_label] = cfg_summary

    # Industry CPI: bedrock phoebe vs useeior (common BEA detail codes)
    with temp_usa_config("useeio_phoebe_23", cache_bearing_modules=_CACHE_MODULES):
        br_ind = _bedrock_industry_cpi_panel(all_years)
    common_ind = br_ind.index.intersection(useeior["industry_cpi"].index)
    for year in [2017, 2022, 2024]:
        if year in br_ind.columns and year in useeior["industry_cpi"].columns:
            summary.setdefault("industry_cpi_vs_useeior", {})[str(year)] = _compare_series(
                br_ind.loc[common_ind, year],
                useeior["industry_cpi"].loc[common_ind, year],
                "bedrock_industry_cpi",
                "useeior_industry_cpi",
            )

    # Phoebe workbook Rho vs useeior R package (if workbook loaded)
    if phoebe_rho is not None:
        phoebe_rho.to_csv(OUT / "phoebe_workbook_Rho.csv")
        for year in COMPARE_YEARS:
            ycol = str(year)
            if ycol in phoebe_rho.columns and year in useeior["rho"].columns:
                summary.setdefault("phoebe_workbook_vs_useeior_r", {})[str(year)] = (
                    _compare_series(
                        phoebe_rho[ycol],
                        useeior["rho"][year],
                        "phoebe_workbook",
                        "useeior_v2.3_waste_disagg",
                    )
                )

    with open(OUT / "comparison_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print(json.dumps(summary, indent=2))
    print(f"\nWrote outputs to {OUT}")


if __name__ == "__main__":
    main()
