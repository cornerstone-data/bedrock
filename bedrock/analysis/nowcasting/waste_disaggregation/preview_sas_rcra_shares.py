"""Phase 1 §4.1.4 preview: SAS child revenue shares + RCRA intersection drift.

Read-only analysis under waste_disaggregation/. Writes figures/ and cache/
artifacts here; STEWI/FBA may also write library caches elsewhere (allowed).

Run:
  .venv\\Scripts\\python.exe -m bedrock.analysis.nowcasting.waste_disaggregation.preview_sas_rcra_shares
"""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

OUT_DIR = Path(__file__).resolve().parent
FIG_DIR = OUT_DIR / "figures"
CACHE_DIR = OUT_DIR / "cache"

# From technical appendix / USEEIO waste method (workbook NAICS→Cornerstone).
SAS_NAICS_TO_CORNERSTONE: dict[str, str] = {
    "562111": "562111",
    "562112": "562HAZ",
    "562211": "562HAZ",
    "562119": "562HAZ",
    "562212": "562212",
    "562213": "562213",
    "562219": "562OTH",  # other nonhazardous treatment/disposal
    "562910": "562910",
    "56291": "562910",  # SAS Table 2 often publishes 5-digit
    "562920": "562920",
    "56292": "562920",
    "562991": "562OTH",
    "562998": "562OTH",
    "562999": "562OTH",
}

CHILDREN = [
    "562111",
    "562HAZ",
    "562212",
    "562213",
    "562910",
    "562920",
    "562OTH",
]

BUNDLED_USE = (
    Path(__file__).resolve().parents[3]
    / "extract"
    / "disaggregation"
    / "waste_disagg_inputs"
    / "WasteDisaggregationDetail2017_Use.csv"
)


def _strip_us(code: str) -> str:
    return str(code).replace("/US", "").strip()


def bundled_2017_column_row_shares() -> tuple[pd.Series, pd.Series]:
    du = pd.read_csv(BUNDLED_USE)
    du["IndustryCode"] = du["IndustryCode"].map(_strip_us)
    du["CommodityCode"] = du["CommodityCode"].map(_strip_us)
    col = (
        du[du["Note"] == "Use column sum, industry output"]
        .set_index("IndustryCode")["PercentUsed"]
        .reindex(CHILDREN)
    )
    row = (
        du[du["Note"] == "Use row sum, commodity output"]
        .set_index("CommodityCode")["PercentUsed"]
        .reindex(CHILDREN)
    )
    return col, row


def _map_sas_naics(code: str) -> str | None:
    digits = "".join(ch for ch in str(code) if ch.isdigit())
    if not digits.startswith("562") or digits == "562":
        return None
    if digits in SAS_NAICS_TO_CORNERSTONE:
        return SAS_NAICS_TO_CORNERSTONE[digits]
    if digits[:6] in SAS_NAICS_TO_CORNERSTONE:
        return SAS_NAICS_TO_CORNERSTONE[digits[:6]]
    if digits[:5] in SAS_NAICS_TO_CORNERSTONE:
        return SAS_NAICS_TO_CORNERSTONE[digits[:5]]
    return None


def load_sas_revenue_shares(years: list[int]) -> pd.DataFrame:
    """Return DataFrame index=year, columns=children, values=revenue shares."""
    from bedrock.extract.flowbyactivity import getFlowByActivity

    rows: list[dict] = []
    for year in years:
        fba = getFlowByActivity("Census_SAS", year)
        # Table 2 detailed NAICS revenue (ActivityProducedBy)
        df = fba.copy()
        desc = df["Description"].astype(str) if "Description" in df.columns else None
        if desc is not None:
            df = df.loc[desc.str.startswith("Table 2")]
        if "FlowName" in df.columns:
            df = df.loc[df["FlowName"].astype(str) == "Revenue"]
        if "ActivityProducedBy" not in df.columns:
            raise KeyError(f"Census_SAS {year}: missing ActivityProducedBy")

        child_amt: dict[str, float] = {c: 0.0 for c in CHILDREN}
        for naics, amt in df.groupby("ActivityProducedBy")["FlowAmount"].sum().items():
            child = _map_sas_naics(str(naics))
            if child is None:
                continue
            child_amt[child] += float(amt)

        total = sum(child_amt.values())
        if total <= 0:
            raise RuntimeError(f"Census_SAS {year}: zero mapped revenue total")
        row = {"year": year, **{c: child_amt[c] / total for c in CHILDREN}}
        rows.append(row)

    return pd.DataFrame(rows).set_index("year")


def load_rcra_intersection_shares(years: list[int]) -> dict[int, pd.DataFrame]:
    """Shipper→receiver mass shares among Cornerstone waste children."""
    from bedrock.transform.flowbysector import FlowBySector

    out: dict[int, pd.DataFrame] = {}
    for year in years:
        fbs = FlowBySector.generateFlowBySector(f"CRHW_national_{year}")
        df = pd.DataFrame(fbs) if not isinstance(fbs, pd.DataFrame) else fbs
        if "FlowName" in df.columns:
            waste = df["FlowName"].astype(str).str.contains("waste", case=False, na=False)
            if waste.any():
                df = df.loc[waste]
        prod = next(
            (
                c
                for c in (
                    "SectorProducedBy",
                    "ActivityProducedBy",
                    "SectorConsumedBy",
                )
                if c in df.columns
            ),
            None,
        )
        cons = next(
            (
                c
                for c in (
                    "SectorConsumedBy",
                    "ActivityConsumedBy",
                    "SectorProducedBy",
                )
                if c in df.columns and c != prod
            ),
            None,
        )
        if prod is None or cons is None or "FlowAmount" not in df.columns:
            raise RuntimeError(
                f"CRHW_national_{year}: unexpected columns {list(df.columns)}"
            )

        def map_sector(s: str) -> str | None:
            digits = "".join(ch for ch in str(s) if ch.isdigit())
            if not digits.startswith("562"):
                return None
            mapped = _map_sas_naics(digits)
            if mapped is not None:
                return mapped
            five = digits[:5]
            if five == "56211":
                return "562111"
            if five == "56221":
                return "562212"
            if five == "56291":
                return "562910"
            if five.startswith("56299"):
                return "562OTH"
            return None

        records = []
        for _, r in df.iterrows():
            i = map_sector(r[prod])
            j = map_sector(r[cons])
            if i is None or j is None:
                continue
            records.append((i, j, float(r["FlowAmount"])))
        if not records:
            raise RuntimeError(f"CRHW_national_{year}: no 562×562 mapped flows")
        mat = (
            pd.DataFrame(records, columns=["from", "to", "amt"])
            .groupby(["from", "to"], as_index=False)["amt"]
            .sum()
        )
        pivot = (
            mat.pivot(index="from", columns="to", values="amt")
            .reindex(index=CHILDREN, columns=CHILDREN)
            .fillna(0.0)
        )
        total = float(pivot.to_numpy().sum())
        if total <= 0:
            raise RuntimeError(f"CRHW_national_{year}: zero intersection total")
        out[year] = pivot / total
    return out


def plot_sas_shares(shares: pd.DataFrame, col_2017: pd.Series) -> Path:
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    ax = shares[CHILDREN].plot(marker="o", figsize=(10, 5))
    for c in CHILDREN:
        ax.axhline(float(col_2017[c]), linestyle="--", alpha=0.25)
    ax.set_title("SAS-mapped waste child revenue shares vs 2017 Use column-sum")
    ax.set_ylabel("Share")
    ax.set_xlabel("Year")
    ax.legend(loc="best", fontsize=8)
    path = FIG_DIR / "sas_child_revenue_shares.png"
    ax.figure.tight_layout()
    ax.figure.savefig(path, dpi=140)
    plt.close(ax.figure)
    return path


def plot_rcra_delta(delta: pd.DataFrame, year: int) -> Path:
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(7, 6))
    im = ax.imshow(delta.values, cmap="RdBu_r", vmin=-0.1, vmax=0.1)
    ax.set_xticks(range(len(CHILDREN)))
    ax.set_yticks(range(len(CHILDREN)))
    ax.set_xticklabels(CHILDREN, rotation=90)
    ax.set_yticklabels(CHILDREN)
    ax.set_title(f"RCRA intersection share Δ ({year} − 2017)")
    fig.colorbar(im, ax=ax, fraction=0.046)
    fig.tight_layout()
    path = FIG_DIR / f"rcra_intersection_delta_{year}_vs_2017.png"
    fig.savefig(path, dpi=140)
    plt.close(fig)
    return path


def main() -> int:
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    report: dict = {"status": "ok", "blockers": [], "artifacts": []}

    col_2017, row_2017 = bundled_2017_column_row_shares()
    col_2017.to_csv(CACHE_DIR / "bundled_2017_use_column_sum.csv", header=["share"])
    row_2017.to_csv(CACHE_DIR / "bundled_2017_use_row_sum.csv", header=["share"])

    # SAS years wired in Census_SAS.yaml
    sas_years = list(range(2017, 2023))
    try:
        shares = load_sas_revenue_shares(sas_years)
        shares.to_csv(CACHE_DIR / "sas_child_revenue_shares.csv")
        delta = shares.sub(col_2017, axis=1)
        delta.to_csv(CACHE_DIR / "sas_vs_2017_column_sum_delta.csv")
        fig = plot_sas_shares(shares, col_2017)
        report["artifacts"].append(str(fig.relative_to(OUT_DIR)))
        report["sas_max_abs_delta"] = float(delta.abs().to_numpy().max())
        report["sas_years"] = sas_years
    except Exception as exc:  # noqa: BLE001 — Phase 1 blocked-exit honesty
        report["blockers"].append(f"SAS preview blocked: {type(exc).__name__}: {exc}")
        report["status"] = "partial"

    # RCRA wired years only
    rcra_years = [2017, 2019, 2021]
    try:
        mats = load_rcra_intersection_shares(rcra_years)
        base = mats[2017]
        base.to_csv(CACHE_DIR / "rcra_intersection_shares_2017.csv")
        for y in (2019, 2021):
            mats[y].to_csv(CACHE_DIR / f"rcra_intersection_shares_{y}.csv")
            d = mats[y] - base
            d.to_csv(CACHE_DIR / f"rcra_intersection_delta_{y}_vs_2017.csv")
            fig = plot_rcra_delta(d, y)
            report["artifacts"].append(str(fig.relative_to(OUT_DIR)))
            report.setdefault("rcra_max_abs_delta", {})[str(y)] = float(
                d.abs().to_numpy().max()
            )
        report["rcra_years"] = rcra_years
    except Exception as exc:  # noqa: BLE001
        report["blockers"].append(f"RCRA preview blocked: {type(exc).__name__}: {exc}")
        report["status"] = "partial" if report["status"] == "ok" else report["status"]
        if not report.get("sas_years"):
            report["status"] = "blocked"

    # Coverage meter (Use CSV abs PercentUsed mass)
    du = pd.read_csv(BUNDLED_USE)
    mass = du.assign(m=du["PercentUsed"].abs()).groupby("Note")["m"].sum()
    total = float(mass.sum())
    previewable_notes = {
        "Use column sum, industry output",
        "Use row sum, commodity output",
        "Use table intersection",
    }
    preview_mass = float(mass.reindex(list(previewable_notes)).fillna(0).sum())
    report["coverage_meter"] = {
        "previewable_use_mass_pct": 100.0 * preview_mass / total if total else None,
        "previewable_notes": sorted(previewable_notes),
        "deferred_notes": sorted(set(mass.index) - previewable_notes),
        "total_abs_percentused": total,
    }

    out_json = CACHE_DIR / "preview_report.json"
    out_json.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0 if report["status"] != "blocked" else 1


if __name__ == "__main__":
    raise SystemExit(main())
