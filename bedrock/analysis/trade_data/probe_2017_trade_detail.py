"""
2017 trade *detail* probe for bedrock#527 (Step D).

Maps Census NAICS-6 goods and BEA IntlServTrade service types to BEA 2017
Detail commodities and compares vectors to Use F04000 / F05000.

Concordances (existing resources):

* Goods: USEEIO ``Census_to_useeio2_sector_concordance.csv``
  (``BEA_Detail_2017``). Rare 1:m NAICS rows split evenly.
* Services: USEEIO ``BEA_service_to_useeio2_sector_concordance.csv``
  (``API BEA Service`` → ``BEA_Detail_2017``). Only mapped TypeOfService
  codes are used (avoids hierarchy double-count); 1:m split evenly
  (same rule as USEEIO ``get_bea_df``).

National extracts match ``probe_2017_trade_totals.py`` (no partner loop).

API keys: ``bedrock/extract/API_Keys.env`` (``Census``, ``BEA``).

Run from repo root::

    uv run python -m bedrock.analysis.trade_data.probe_2017_trade_detail

Writes under ``bedrock/analysis/trade_data/output/``:

* ``probe_2017_trade_detail_vectors.csv`` — commodity-level compare
* ``probe_2017_trade_detail_summary.csv`` — coverage / correlation stats
"""

from __future__ import annotations

import json
import urllib.parse
import urllib.request
from pathlib import Path

import pandas as pd

from bedrock.extract.iot.io_2017 import load_2017_Ytot_usa
from bedrock.utils.config.common import load_env_file_key

YEAR = 2017
OUT_DIR = Path(__file__).resolve().parent / "output"
REPO_ROOT = Path(__file__).resolve().parents[3]

# Prefer local USEEIO clone; fall back to path next to bedrock if present.
_USEEIO_CONC = (
    Path(r"c:\Users\BYoung\Code\src\USEEIO\import_emission_factors\concordances")
)
if not _USEEIO_CONC.is_dir():
    _USEEIO_CONC = REPO_ROOT.parent / "USEEIO" / "import_emission_factors" / "concordances"

CENSUS_CONC = _USEEIO_CONC / "Census_to_useeio2_sector_concordance.csv"
SERVICE_CONC = _USEEIO_CONC / "BEA_service_to_useeio2_sector_concordance.csv"


def _census_table(flow: str, value_col: str, api_key: str) -> pd.DataFrame:
    params = urllib.parse.urlencode(
        {
            "get": f"NAICS,{value_col}",
            "COMM_LVL": "NA6",
            "YEAR": str(YEAR),
            "MONTH": "12",
            "key": api_key,
        }
    )
    url = f"https://api.census.gov/data/timeseries/intltrade/{flow}/naics?{params}"
    with urllib.request.urlopen(url, timeout=180) as resp:
        payload = resp.read().decode("utf-8")
    if payload.lstrip().startswith("<"):
        raise RuntimeError(f"Census API returned HTML for {flow}")
    header, *rows = json.loads(payload)
    df = pd.DataFrame(rows, columns=header)
    df["NAICS"] = df["NAICS"].astype(str)
    df["usd"] = pd.to_numeric(df[value_col], errors="coerce").fillna(0.0)
    return df[["NAICS", "usd"]]


def _bea_services(direction: str, api_key: str) -> pd.DataFrame:
    params = urllib.parse.urlencode(
        {
            "UserID": api_key,
            "method": "GetData",
            "DataSetName": "IntlServTrade",
            "TradeDirection": direction,
            "Affiliation": "AllAffiliations",
            "AreaOrCountry": "AllCountries",
            "Year": str(YEAR),
            "ResultFormat": "json",
        }
    )
    url = f"https://apps.bea.gov/api/data/?{params}"
    with urllib.request.urlopen(url, timeout=180) as resp:
        raw = json.loads(resp.read().decode("utf-8"))
    data = raw.get("BEAAPI", {}).get("Results", {}).get("Data")
    if not data:
        raise RuntimeError(f"BEA IntlServTrade empty for {direction}")
    df = pd.DataFrame(data)
    df["musd"] = pd.to_numeric(df["DataValue"], errors="coerce").fillna(0.0)
    return df[["TypeOfService", "musd"]]


def _equal_split_to_detail(
    amounts: pd.DataFrame,
    concordance: pd.DataFrame,
    amount_key: str,
    left_key: str,
    conc_left: str,
    conc_detail: str = "BEA_Detail_2017",
) -> tuple[pd.Series, dict[str, float]]:
    """Map amounts to BEA Detail with equal split on 1:m; return series (M USD) + stats."""
    conc = concordance[[conc_left, conc_detail]].dropna().drop_duplicates()
    conc[conc_left] = conc[conc_left].astype(str)
    conc[conc_detail] = conc[conc_detail].astype(str)
    n_map = conc.groupby(conc_left)[conc_detail].transform("count")
    conc = conc.assign(_n=n_map)

    left = amounts.copy()
    left[left_key] = left[left_key].astype(str)
    total = float(left[amount_key].sum())
    merged = left.merge(conc, how="left", left_on=left_key, right_on=conc_left)
    mapped_mask = merged[conc_detail].notna()
    mapped_value = float(left.loc[left[left_key].isin(conc[conc_left]), amount_key].sum())
    unmapped_keys = sorted(set(left[left_key]) - set(conc[conc_left]))
    unmapped_value = total - mapped_value

    merged = merged.loc[mapped_mask].copy()
    merged["musd"] = merged[amount_key] / merged["_n"]
    detail = merged.groupby(conc_detail, sort=False)["musd"].sum()
    detail.index.name = "bea_detail"
    stats = {
        "source_total_musd": total,
        "mapped_total_musd": mapped_value,
        "unmapped_total_musd": unmapped_value,
        "mapped_share": (mapped_value / total) if total else float("nan"),
        "n_source": int(left[left_key].nunique()),
        "n_unmapped_keys": len(unmapped_keys),
        "n_detail": int(detail.shape[0]),
    }
    return detail, stats


def _use_trade_vectors() -> tuple[pd.Series, pd.Series]:
    Y = load_2017_Ytot_usa()
    # Y is in USD; convert to million USD to match extracts.
    exports = (Y["F04000"] / 1e6).astype(float)
    imports = (-Y["F05000"].clip(upper=0) / 1e6).astype(float)
    exports.index = exports.index.astype(str)
    imports.index = imports.index.astype(str)
    exports.name = "use_F040_musd"
    imports.name = "use_F050_abs_musd"
    return exports, imports


def _align_compare(
    extract: pd.Series, use: pd.Series, flow: str
) -> tuple[pd.DataFrame, dict[str, float]]:
    idx = sorted(set(extract.index) | set(use.index))
    e = extract.reindex(idx).fillna(0.0)
    u = use.reindex(idx).fillna(0.0)
    df = pd.DataFrame(
        {
            "bea_detail": idx,
            "flow": flow,
            "extract_musd": e.values,
            "use_musd": u.values,
        }
    )
    df["diff_musd"] = df["extract_musd"] - df["use_musd"]
    df["abs_diff_musd"] = df["diff_musd"].abs()

    both = (df["extract_musd"] != 0) | (df["use_musd"] != 0)
    sub = df.loc[both]
    if len(sub) >= 2:
        pearson = float(sub["extract_musd"].corr(sub["use_musd"]))
        # Rank correlation without scipy dependency.
        spearman = float(
            sub["extract_musd"].rank().corr(sub["use_musd"].rank())
        )
    else:
        pearson = spearman = float("nan")

    e_tot = float(df["extract_musd"].sum())
    u_tot = float(df["use_musd"].sum())
    # Top-20 share overlap (Jaccard on top-20 sets by value)
    top_e = set(df.nlargest(20, "extract_musd")["bea_detail"])
    top_u = set(df.nlargest(20, "use_musd")["bea_detail"])
    jaccard = len(top_e & top_u) / len(top_e | top_u) if (top_e | top_u) else float("nan")

    stats = {
        "flow": flow,
        "extract_total_musd": e_tot,
        "use_total_musd": u_tot,
        "ratio_extract_to_use": (e_tot / u_tot) if u_tot else float("nan"),
        "pearson": pearson,
        "spearman": spearman,
        "top20_jaccard": jaccard,
        "mae_musd": float(sub["abs_diff_musd"].mean()) if len(sub) else float("nan"),
        "n_commodities_nonzero": int(both.sum()),
    }
    return df, stats


def main() -> None:
    if not CENSUS_CONC.is_file() or not SERVICE_CONC.is_file():
        raise FileNotFoundError(
            f"USEEIO concordances not found under {_USEEIO_CONC}. "
            "Clone cornerstone-data/USEEIO or set path."
        )

    census_key = load_env_file_key("API_Key", "Census")
    bea_key = load_env_file_key("API_Key", "BEA")
    census_conc = pd.read_csv(CENSUS_CONC)
    service_conc = pd.read_csv(SERVICE_CONC)

    print("Fetching Census goods + BEA services...")
    goods_imp = _census_table("imports", "GEN_CIF_YR", census_key)
    goods_exp = _census_table("exports", "ALL_VAL_YR", census_key)
    # Census API returns USD; convert to million USD before mapping.
    goods_imp = goods_imp.assign(musd=lambda x: x["usd"] / 1e6)
    goods_exp = goods_exp.assign(musd=lambda x: x["usd"] / 1e6)

    svc_imp = _bea_services("Imports", bea_key)
    svc_exp = _bea_services("Exports", bea_key)
    # Restrict to concordance API codes (leaf-ish set used by USEEIO).
    api_codes = set(service_conc["API BEA Service"].astype(str))
    svc_imp_m = svc_imp.loc[svc_imp["TypeOfService"].isin(api_codes)]
    svc_exp_m = svc_exp.loc[svc_exp["TypeOfService"].isin(api_codes)]
    all_imp = float(svc_imp.loc[svc_imp["TypeOfService"] == "AllTypesOfService", "musd"].sum())
    all_exp = float(svc_exp.loc[svc_exp["TypeOfService"] == "AllTypesOfService", "musd"].sum())

    print("Mapping to BEA Detail...")
    g_imp_d, g_imp_s = _equal_split_to_detail(
        goods_imp, census_conc, "musd", "NAICS", "NAICS"
    )
    g_exp_d, g_exp_s = _equal_split_to_detail(
        goods_exp, census_conc, "musd", "NAICS", "NAICS"
    )
    s_imp_d, s_imp_s = _equal_split_to_detail(
        svc_imp_m.rename(columns={"TypeOfService": "api"}),
        service_conc,
        "musd",
        "api",
        "API BEA Service",
    )
    s_exp_d, s_exp_s = _equal_split_to_detail(
        svc_exp_m.rename(columns={"TypeOfService": "api"}),
        service_conc,
        "musd",
        "api",
        "API BEA Service",
    )

    extract_imp = g_imp_d.add(s_imp_d, fill_value=0.0)
    extract_exp = g_exp_d.add(s_exp_d, fill_value=0.0)

    print("Loading Use F040/F050...")
    use_exp, use_imp = _use_trade_vectors()
    vec_imp, st_imp = _align_compare(extract_imp, use_imp, "imports")
    vec_exp, st_exp = _align_compare(extract_exp, use_exp, "exports")

    coverage_rows = [
        {"piece": "census_goods_imports", **g_imp_s},
        {"piece": "census_goods_exports", **g_exp_s},
        {
            "piece": "bea_services_imports_mapped_types",
            **s_imp_s,
            "alltypes_musd": all_imp,
            "mapped_types_vs_alltypes": s_imp_s["source_total_musd"] / all_imp,
        },
        {
            "piece": "bea_services_exports_mapped_types",
            **s_exp_s,
            "alltypes_musd": all_exp,
            "mapped_types_vs_alltypes": s_exp_s["source_total_musd"] / all_exp,
        },
    ]
    summary = pd.DataFrame([st_imp, st_exp])
    coverage = pd.DataFrame(coverage_rows)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    vectors = pd.concat([vec_imp, vec_exp], ignore_index=True)
    vectors_path = OUT_DIR / "probe_2017_trade_detail_vectors.csv"
    summary_path = OUT_DIR / "probe_2017_trade_detail_summary.csv"
    coverage_path = OUT_DIR / "probe_2017_trade_detail_coverage.csv"
    vectors.to_csv(vectors_path, index=False)
    summary.to_csv(summary_path, index=False)
    coverage.to_csv(coverage_path, index=False)

    print("\n=== Coverage ===")
    print(
        coverage[
            [
                "piece",
                "source_total_musd",
                "mapped_share",
                "n_unmapped_keys",
                "n_detail",
            ]
        ].to_string(index=False, float_format=lambda x: f"{x:,.3f}")
    )
    print("\n=== Vector compare vs Use ===")
    print(summary.to_string(index=False, float_format=lambda x: f"{x:,.3f}"))

    for flow, vec in (("imports", vec_imp), ("exports", vec_exp)):
        print(f"\n=== Top 10 |diff| {flow} (extract - use), M USD ===")
        top = vec.nlargest(10, "abs_diff_musd")[
            ["bea_detail", "extract_musd", "use_musd", "diff_musd"]
        ]
        print(top.to_string(index=False, float_format=lambda x: f"{x:,.1f}"))

    print(f"\nWrote {vectors_path}")
    print(f"Wrote {summary_path}")
    print(f"Wrote {coverage_path}")


if __name__ == "__main__":
    main()
