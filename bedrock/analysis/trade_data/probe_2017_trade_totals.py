"""
2017 trade totals probe for bedrock#527.

Pulls national goods + services trade totals the same way the flowsa
``imports`` branch FBAs do in spirit:

* Census USA Trade NAICS-6, MONTH=12 YTD — imports ``GEN_CIF_YR``, exports
  ``ALL_VAL_YR`` (no partner-country loop; national aggregate).
* BEA ``IntlServTrade`` for ``AllCountries`` — uses the ``AllTypesOfService``
  row only (summing all TypeOfService rows double-counts the hierarchy).

Compares those extracts to 2017 Use F040/F050 and Supply MCIF already in
bedrock, plus published ITA goods+services calendar-year totals.

API keys: ``bedrock/extract/API_Keys.env`` via ``load_env_file_key``
(``Census``, ``BEA``). Never prints key values.

Run from repo root::

    uv run python -m bedrock.analysis.trade_data.probe_2017_trade_totals

Writes ``bedrock/analysis/trade_data/output/probe_2017_trade_totals.csv``.
"""

from __future__ import annotations

import json
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from bedrock.analysis.compare_NIPA_to_IOT.loaders import bea_matrix_column
from bedrock.utils.config.common import load_env_file_key

YEAR = 2017
OUT_DIR = Path(__file__).resolve().parent / "output"
OUT_CSV = OUT_DIR / "probe_2017_trade_totals.csv"

# Published ITA Table 1.1 calendar-year goods+services (million USD).
# Source: BEA international transactions tables (2017 release vintage).
ITA_2017_EXPORTS_GS_MUSD = 2_263_907.0
ITA_2017_IMPORTS_GS_MUSD = 2_764_352.0


@dataclass(frozen=True)
class ExtractTotals:
    census_goods_imports_cif_musd: float
    census_goods_exports_fas_musd: float
    bea_services_imports_musd: float
    bea_services_exports_musd: float
    census_import_naics_rows: int
    census_export_naics_rows: int

    @property
    def combined_imports_musd(self) -> float:
        return self.census_goods_imports_cif_musd + self.bea_services_imports_musd

    @property
    def combined_exports_musd(self) -> float:
        return self.census_goods_exports_fas_musd + self.bea_services_exports_musd


def _census_json(flow: str, get_fields: str, api_key: str) -> list[list[str]]:
    params = urllib.parse.urlencode(
        {
            "get": get_fields,
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
        raise RuntimeError(
            f"Census API returned HTML (likely missing/invalid key) for {flow}"
        )
    return json.loads(payload)


def _sum_census_column(table: list[list[str]], column: str) -> tuple[float, int]:
    header, *rows = table
    idx = header.index(column)
    total = sum(float(row[idx] or 0.0) for row in rows)
    return total / 1e6, len(rows)  # API values are USD → million USD


def fetch_census_goods(api_key: str) -> tuple[float, float, int, int]:
    imports_tbl = _census_json("imports", "NAICS,GEN_CIF_YR", api_key)
    exports_tbl = _census_json("exports", "NAICS,ALL_VAL_YR", api_key)
    imp_m, n_imp = _sum_census_column(imports_tbl, "GEN_CIF_YR")
    exp_m, n_exp = _sum_census_column(exports_tbl, "ALL_VAL_YR")
    return imp_m, exp_m, n_imp, n_exp


def _bea_intl_serv(direction: str, api_key: str) -> pd.DataFrame:
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
    results = raw.get("BEAAPI", {}).get("Results", {})
    if isinstance(results, dict) and results.get("Error"):
        raise RuntimeError(f"BEA API error for {direction}: {results['Error']}")
    data = results.get("Data") if isinstance(results, dict) else None
    if not data:
        raise RuntimeError(f"BEA IntlServTrade returned no Data for {direction}")
    df = pd.DataFrame(data)
    df["DataValue"] = pd.to_numeric(df["DataValue"], errors="coerce").fillna(0.0)
    return df


def fetch_bea_services(api_key: str) -> tuple[float, float]:
    """Return (imports, exports) in million USD from AllTypesOfService."""
    out: dict[str, float] = {}
    for direction in ("Imports", "Exports"):
        df = _bea_intl_serv(direction, api_key)
        row = df.loc[df["TypeOfService"] == "AllTypesOfService"]
        if row.empty:
            raise RuntimeError(
                f"BEA IntlServTrade missing AllTypesOfService for {direction}"
            )
        # UNIT_MULT=6 → DataValue is already million USD.
        out[direction] = float(row["DataValue"].iloc[0])
    return out["Imports"], out["Exports"]


def fetch_extracts() -> ExtractTotals:
    census_key = load_env_file_key("API_Key", "Census")
    bea_key = load_env_file_key("API_Key", "BEA")
    g_imp, g_exp, n_imp, n_exp = fetch_census_goods(census_key)
    s_imp, s_exp = fetch_bea_services(bea_key)
    return ExtractTotals(
        census_goods_imports_cif_musd=g_imp,
        census_goods_exports_fas_musd=g_exp,
        bea_services_imports_musd=s_imp,
        bea_services_exports_musd=s_exp,
        census_import_naics_rows=n_imp,
        census_export_naics_rows=n_exp,
    )


def fetch_benchmarks() -> dict[str, float]:
    f040 = bea_matrix_column("F04000", matrix="Use_MUT_detail_after_redef")
    f050 = bea_matrix_column("F05000", matrix="Use_MUT_detail_after_redef")
    mcif = bea_matrix_column("MCIF", matrix="Supply_SUT_detail")
    return {
        "use_F04000_exports_musd": float(f040.total),
        "use_F05000_imports_abs_musd": abs(float(f050.total)),
        "supply_MCIF_musd": float(mcif.total),
        "ita_exports_gs_musd": ITA_2017_EXPORTS_GS_MUSD,
        "ita_imports_gs_musd": ITA_2017_IMPORTS_GS_MUSD,
    }


def build_comparison(extract: ExtractTotals, bench: dict[str, float]) -> pd.DataFrame:
    rows = [
        {
            "series": "Census goods imports (CIF, NAICS-6)",
            "million_usd": extract.census_goods_imports_cif_musd,
            "notes": f"n_naics={extract.census_import_naics_rows}",
        },
        {
            "series": "BEA services imports (AllTypesOfService)",
            "million_usd": extract.bea_services_imports_musd,
            "notes": "IntlServTrade AllCountries",
        },
        {
            "series": "Combined extract imports (Census CIF + BEA services)",
            "million_usd": extract.combined_imports_musd,
            "notes": "May double-count freight/insurance vs BOP",
        },
        {
            "series": "Use MUT F05000 imports (abs)",
            "million_usd": bench["use_F05000_imports_abs_musd"],
            "notes": "PRO; target column for nowcast",
        },
        {
            "series": "Supply MCIF",
            "million_usd": bench["supply_MCIF_musd"],
            "notes": "BAS / CIF-family",
        },
        {
            "series": "ITA imports goods+services",
            "million_usd": bench["ita_imports_gs_musd"],
            "notes": "BOP calendar 2017",
        },
        {
            "series": "Census goods exports (ALL_VAL / FAS-family)",
            "million_usd": extract.census_goods_exports_fas_musd,
            "notes": f"n_naics={extract.census_export_naics_rows}",
        },
        {
            "series": "BEA services exports (AllTypesOfService)",
            "million_usd": extract.bea_services_exports_musd,
            "notes": "IntlServTrade AllCountries",
        },
        {
            "series": "Combined extract exports (Census + BEA services)",
            "million_usd": extract.combined_exports_musd,
            "notes": "",
        },
        {
            "series": "Use MUT F04000 exports",
            "million_usd": bench["use_F04000_exports_musd"],
            "notes": "PRO; SUT PUR total nearly identical",
        },
        {
            "series": "ITA exports goods+services",
            "million_usd": bench["ita_exports_gs_musd"],
            "notes": "BOP calendar 2017",
        },
    ]
    df = pd.DataFrame(rows)
    df["year"] = YEAR
    # Ratios vs primary Use targets
    df["ratio_to_use_F050_abs"] = (
        df["million_usd"] / bench["use_F05000_imports_abs_musd"]
    )
    df["ratio_to_use_F040"] = df["million_usd"] / bench["use_F04000_exports_musd"]
    return df


def main() -> None:
    print(f"Fetching Census + BEA extracts for {YEAR}...")
    extract = fetch_extracts()
    print("Loading bedrock Use F040/F050 and Supply MCIF...")
    bench = fetch_benchmarks()
    df = build_comparison(extract, bench)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_CSV, index=False)

    print()
    print(df.to_string(index=False, float_format=lambda x: f"{x:,.1f}"))
    print()
    print(
        "Import combined / Use|F050| = "
        f"{extract.combined_imports_musd / bench['use_F05000_imports_abs_musd']:.3f}"
    )
    print(
        "Import combined / MCIF = "
        f"{extract.combined_imports_musd / bench['supply_MCIF_musd']:.3f}"
    )
    print(
        "Export combined / Use F040 = "
        f"{extract.combined_exports_musd / bench['use_F04000_exports_musd']:.3f}"
    )
    print(f"Wrote {OUT_CSV}")


if __name__ == "__main__":
    main()
