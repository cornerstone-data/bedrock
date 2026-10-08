"""Probe issue #767 source instruments: affiliation split, ITA defense, NIPA IP.

Complements ``s00300_use_distribution_probe`` (published row + leaf budgets).

Run::

    uv run python -m bedrock.analysis.nowcasting.trade_data.s00300_issue_source_probe

Writes CSVs under ``output/s00300_issue_source_*.csv``.
"""

from __future__ import annotations

import json
import subprocess
import time
import urllib.parse
from pathlib import Path
from typing import Any

import pandas as pd

from bedrock.analysis.nowcasting.trade_data.s00300_use_distribution_probe import (
    CIP_LICENSES_LEAVES,
    OUT_DIR,
    S00300_LEAVES,
    breakdown_use_row,
    industry_column,
)
from bedrock.utils.config.common import load_env_file_key

AFFILIATIONS = (
    "AllAffiliations",
    "Affiliated",
    "Unaffiliated",
    "UsParents",
    "UsAffiliates",
)
PROBE_YEARS = (2012, 2017)

# Extra license leaves on imports crosswalk (some map to comparable Detail, not S00300).
EXTRA_LICENSE_LEAVES = (
    "CipLicensesCompSoftware",
    "CipLicensesBooksSoundRecord",
    "CipLicensesMoviesTv",
    "CipLicensesBroadcastLiveRecord",
    "CipLicensesAudVis",
    "CipLicensesFranchisesTrademarks",
)

# ITA API keys (DirectDefenseExpenditures discontinued post-2014; not in API).
ITA_GOVT_INDICATORS = (
    "ImpServGovtGoodsAndServicesNie",
    "ImpGdsUsMilAgencyBopAdj",
    "ExpServGovtGoodsAndServicesNie",
)

BEA_API_DELAY_S = 0.5


def _bea_curl(**params: str) -> dict[str, Any]:
    key = load_env_file_key("api_key", "BEA")
    base = {
        "UserID": key,
        "method": params.pop("method", "GetData"),
        "ResultFormat": "JSON",
    }
    base.update(params)
    url = "https://apps.bea.gov/api/data?" + urllib.parse.urlencode(base)
    proc = subprocess.run(
        ["curl.exe", "-sL", url],
        capture_output=True,
        text=True,
        check=True,
    )
    time.sleep(BEA_API_DELAY_S)
    return json.loads(proc.stdout)


def _bea_data_row(dataset: str, **params: str) -> dict[str, Any] | None:
    payload = _bea_curl(DataSetName=dataset, **params)
    results = payload.get("BEAAPI", {}).get("Results", {})
    if results.get("Error"):
        raise RuntimeError(f"BEA {dataset} error: {results['Error']}")
    data = results.get("Data")
    if isinstance(data, dict):
        return data
    if not data:
        return None
    return data[0]


def _parse_musd(row: dict[str, Any] | None) -> float | None:
    if not row:
        return None
    raw = str(row.get("DataValue", "")).strip().replace(",", "")
    if raw in {"", "...", "n.a."}:
        return None
    return float(raw)


def _expected_affiliation_rows() -> int:
    leaves = sorted(set(S00300_LEAVES) | set(EXTRA_LICENSE_LEAVES))
    return len(PROBE_YEARS) * len(leaves) * len(AFFILIATIONS)


def _load_affiliation_if_complete(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    df = pd.read_csv(path)
    if len(df) < _expected_affiliation_rows():
        return None
    if df["musd"].isna().all():
        return None
    return df


def pull_affiliation_splits() -> pd.DataFrame:
    """IntlServTrade imports by TypeOfService x Affiliation (million USD)."""
    leaves = sorted(set(S00300_LEAVES) | set(EXTRA_LICENSE_LEAVES))
    rows: list[dict[str, object]] = []
    for year in PROBE_YEARS:
        for leaf in leaves:
            for aff in AFFILIATIONS:
                row = _bea_data_row(
                    "IntlServTrade",
                    Year=str(year),
                    TradeDirection="Imports",
                    Affiliation=aff,
                    TypeOfService=leaf,
                    AreaOrCountry="AllCountries",
                )
                rows.append(
                    {
                        "year": year,
                        "type_of_service": leaf,
                        "affiliation": aff,
                        "musd": _parse_musd(row),
                        "in_s00300_leaf_set": leaf in S00300_LEAVES,
                    }
                )
    return pd.DataFrame(rows)


def pull_ita_govt_series() -> pd.DataFrame:
    """ITA annual imports for defense / government n.i.e. lines (million USD)."""
    rows: list[dict[str, object]] = []
    for year in PROBE_YEARS:
        for indicator in ITA_GOVT_INDICATORS:
            row = _bea_data_row(
                "ITA",
                Indicator=indicator,
                AreaOrCountry="AllCountries",
                Frequency="A",
                Year=str(year),
            )
            rows.append(
                {
                    "year": year,
                    "indicator": indicator,
                    "musd": _parse_musd(row),
                    "description": (
                        row.get("TimeSeriesDescription")
                        or row.get("IndicatorDescription")
                        if row
                        else None
                    ),
                }
            )
    return pd.DataFrame(rows)


def pull_nipa_ip_investment() -> pd.DataFrame:
    """NIPA Table 5.6.5 private IP investment by type (million USD)."""
    rows: list[dict[str, object]] = []
    for year in PROBE_YEARS:
        payload = _bea_curl(
            DataSetName="NIPA",
            TableName="T50605",
            Frequency="A",
            Year=str(year),
        )
        data = payload.get("BEAAPI", {}).get("Results", {}).get("Data") or []
        for row in data:
            rows.append(
                {
                    "year": year,
                    "line_number": int(row["LineNumber"]),
                    "line_description": row["LineDescription"],
                    "musd": _parse_musd(row),
                }
            )
    return pd.DataFrame(rows)


def published_govt_transport_shares() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Published S00300 intermediate: government S* and transport-ish columns."""
    govt_rows: list[dict[str, object]] = []
    transport_rows: list[dict[str, object]] = []
    transport_codes = (
        "481000",
        "482000",
        "483000",
        "484000",
        "485000",
        "486000",
        "487000",
        "488000",
        "492000",
        "493000",
        "324110",
    )
    for year in PROBE_YEARS:
        ind = industry_column(year)
        total = float(ind[ind > 0].sum())
        s_codes = [c for c in ind.index if str(c).startswith("S")]
        s_total = float(ind.reindex(s_codes).fillna(0).sum())
        for code in s_codes:
            val = float(ind.get(code, 0))
            if val:
                govt_rows.append(
                    {
                        "year": year,
                        "industry": code,
                        "musd": val / 1e6,
                        "share_of_intermediate": val / total if total else 0,
                        "share_of_s_star": val / s_total if s_total else 0,
                    }
                )
        for code in transport_codes:
            val = float(ind.get(code, 0))
            if val:
                transport_rows.append(
                    {
                        "year": year,
                        "industry": code,
                        "musd": val / 1e6,
                        "share_of_intermediate": val / total if total else 0,
                    }
                )
    return pd.DataFrame(govt_rows), pd.DataFrame(transport_rows)


def _affiliation_summary(aff: pd.DataFrame) -> pd.DataFrame:
    """Pivot affiliation columns; compute affiliated share of AllAffiliations."""
    wide = aff.pivot_table(
        index=["year", "type_of_service", "in_s00300_leaf_set"],
        columns="affiliation",
        values="musd",
        aggfunc="first",
    ).reset_index()
    all_col = wide["AllAffiliations"]
    for col in ("Affiliated", "Unaffiliated", "UsParents", "UsAffiliates"):
        if col in wide.columns:
            wide[f"{col.lower()}_share"] = wide[col] / all_col
    return wide


def _print_summary(
    aff: pd.DataFrame,
    ita: pd.DataFrame,
    nipa: pd.DataFrame,
    govt: pd.DataFrame,
    transport: pd.DataFrame,
) -> None:
    aff_sum = _affiliation_summary(aff)
    s003 = aff_sum[aff_sum["in_s00300_leaf_set"]].copy()

    print("\n=== Affiliation split (S00300 leaves, 2017) ===")
    s17 = s003[s003["year"] == 2017].sort_values("type_of_service")
    for _, r in s17.iterrows():
        print(
            f"  {r['type_of_service']:40s}  all={r['AllAffiliations']:8,.0f}  "
            f"aff={r.get('Affiliated', float('nan')):8,.0f} "
            f"({100*r.get('affiliated_share', 0):4.0f}%)  "
            f"unaff={r.get('Unaffiliated', float('nan')):8,.0f} "
            f"({100*r.get('unaffiliated_share', 0):4.0f}%)"
        )

    print("\n=== ITA government / defense (imports, $M) ===")
    print(ita.pivot(index="indicator", columns="year", values="musd").to_string())

    govt_leaf = aff[
        (aff["year"] == 2017)
        & (aff["type_of_service"] == "GovtGoodsAndServicesNie")
        & (aff["affiliation"] == "AllAffiliations")
    ]["musd"].iloc[0]

    def _ita_musd(indicator: str) -> float | None:
        sub = ita[(ita["year"] == 2017) & (ita["indicator"] == indicator)]["musd"]
        return float(sub.iloc[0]) if len(sub) else None

    ita_govt = _ita_musd("ImpServGovtGoodsAndServicesNie")
    ita_mil_gds = _ita_musd("ImpGdsUsMilAgencyBopAdj")
    print(f"\n  IEA GovtGoodsAndServicesNie leaf: {govt_leaf:,.0f}")
    if ita_govt is not None:
        print(
            f"  ITA ImpServGovtGoodsAndServicesNie: {ita_govt:,.0f}  "
            f"(ratio {ita_govt/govt_leaf:.2f}x)"
        )
    if ita_mil_gds is not None:
        print(
            f"  ITA ImpGdsUsMilAgencyBopAdj (goods): {ita_mil_gds:,.0f}  "
            f"(ratio {ita_mil_gds/govt_leaf:.2f}x of govt leaf)"
        )
    s_star = govt["musd"].sum() if not govt.empty else 0
    print(f"  Published S00300 S* intermediate:   {s_star:,.0f}")

    print("\n=== NIPA 5.6.5 vs S00300 F02N00 (2017) ===")
    n17 = nipa[(nipa["year"] == 2017) & (nipa["line_number"] <= 6)]
    f02 = breakdown_use_row(2017).f02n00_usd / 1e6
    lic = aff[
        (aff["year"] == 2017)
        & (aff["type_of_service"].isin(CIP_LICENSES_LEAVES))
        & (aff["affiliation"] == "AllAffiliations")
    ]["musd"].sum()
    lic_unaff = aff[
        (aff["year"] == 2017)
        & (aff["type_of_service"].isin(CIP_LICENSES_LEAVES))
        & (aff["affiliation"] == "Unaffiliated")
    ]["musd"].sum()
    print(n17[["line_number", "line_description", "musd"]].to_string(index=False))
    print(f"\n  Published S00300 x F02N00:           {f02:,.0f}")
    print(f"  IEA CipLicenses* (3 S00300 leaves):  {lic:,.0f}")
    print(f"    of which Unaffiliated:             {lic_unaff:,.0f}")
    print(f"  Implied cap rate F02N00 / lic leaves: {f02/lic:.1%}")

    print("\n=== Transport published shares (2017, $M) ===")
    t17 = transport[transport["year"] == 2017].sort_values("musd", ascending=False)
    for _, r in t17.iterrows():
        print(
            f"  {r['industry']:8s} {r['musd']:8,.0f}  ({100*r['share_of_intermediate']:4.1f}% of intermediate)"
        )


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    aff_path = OUT_DIR / "s00300_affiliation_split_2012_2017.csv"
    aff = _load_affiliation_if_complete(aff_path)
    if aff is not None:
        print(f"Reusing complete affiliation split from {aff_path.name}")
    else:
        print("Pulling IntlServTrade affiliation splits...")
        aff = pull_affiliation_splits()
        aff.to_csv(aff_path, index=False)

    aff_sum = _affiliation_summary(aff)
    aff_sum_path = OUT_DIR / "s00300_affiliation_split_summary.csv"
    aff_sum.to_csv(aff_sum_path, index=False)

    print("Pulling ITA defense / government indicators...")
    ita = pull_ita_govt_series()
    ita_path = OUT_DIR / "s00300_ita_govt_defense_2012_2017.csv"
    ita.to_csv(ita_path, index=False)

    print("Pulling NIPA Table 5.6.5...")
    nipa = pull_nipa_ip_investment()
    nipa_path = OUT_DIR / "s00300_nipa_t50605_2012_2017.csv"
    nipa.to_csv(nipa_path, index=False)

    print("Extracting published govt / transport shares...")
    govt, transport = published_govt_transport_shares()
    govt.to_csv(OUT_DIR / "s00300_published_govt_shares_2012_2017.csv", index=False)
    transport.to_csv(
        OUT_DIR / "s00300_published_transport_shares_2012_2017.csv", index=False
    )

    _print_summary(aff, ita, nipa, govt[govt["year"] == 2017], transport)

    print(f"\nWrote CSVs to {OUT_DIR}")


if __name__ == "__main__":
    main()
