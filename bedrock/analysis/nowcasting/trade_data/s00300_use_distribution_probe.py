"""Probe published ``S00300`` Use rows and IEA leaf budgets (#767).

Loads BEA's published ``S00300`` row (who consumes noncomparable imports),
sums the IEA import categories that feed Supply ``MCIF`` (#766), and checks
whether each category plausibly maps to a single industry or PCE/IP column.

Each guess is graded as a ratio (leaf dollars / published cell dollars).
Ratios near 1.0 support the mapping; ratios far from 1.0 argue against it.
Nothing here is adopted until a 2012 holdout passes.

Progress notes: ``.cursor/plan/issue_767_progress.md``

Run from repo root::

    uv run python -m bedrock.analysis.nowcasting.trade_data.s00300_use_distribution_probe

Writes CSVs under ``bedrock/analysis/nowcasting/trade_data/output/``.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from bedrock.extract.flowbyactivity import getFlowByActivity
from bedrock.extract.iot.io_2017 import (
    _load_2017_detail_supply_use_usa,
    _load_benchmark_detail_supply_use_usa,
)
from bedrock.transform.flowbysector import FlowBySector
from bedrock.transform.iot.nowcast import _resolve_both_sector_columns
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES

OUT_DIR = Path(__file__).resolve().parent / "output"
CROSSWALK = (
    Path(__file__).resolve().parents[3]
    / "utils/mapping/activitytosectormapping/Sector_Crosswalk_BEA_IEA_imports.csv"
)
IEA_INPUT = Path(__file__).resolve().parents[3] / "extract/input_data/BEA_IEA"

BENCHMARK_YEARS = (2007, 2012, 2017)
HOLDOUT_BASE = 2012
HOLDOUT_TARGET = 2017
OVERLAY_YEARS = tuple(range(2012, 2025))

# IEA import leaves routed to S00300 on Supply MCIF (#766).
S00300_LEAVES: tuple[str, ...] = (
    "TransportAirPort",
    "TransportSeaFreight",
    "TransportSeaPort",
    "CipLicensesOutcomesResearchAndDev",
    "CipLicensesFranchiseFees",
    "CipLicensesTrademarks",
    "TradeRelated",
    "TravelPersonalOth",
    "TravelBusinessOth",
    "TravelEducation",
    "TravelShortTermWork",
    "FinExplicitAndOth",
    "GovtGoodsAndServicesNie",
    "OthBusinessNie",
    "ConstExpend",
    "ConstAbroadUs",
)

TRAVEL_LEAVES = (
    "TravelPersonalOth",
    "TravelBusinessOth",
    "TravelEducation",
    "TravelShortTermWork",
)
CIP_LICENSES_LEAVES = (
    "CipLicensesOutcomesResearchAndDev",
    "CipLicensesFranchiseFees",
    "CipLicensesTrademarks",
)
TRANSPORT_LEAVES = ("TransportAirPort", "TransportSeaFreight", "TransportSeaPort")

PCE_S00300_ACTIVITIES = (
    "Government employees' expenditures abroad",
    "Private employees' expenditures abroad",
    "U.S. student expenditures",
    "U.S. travel outside the United States",
)


def hypothesis_verdict(ratio: float) -> str:
    if ratio >= 0.9 and ratio <= 1.15:
        return "SUPPORTS"
    if ratio >= 0.75 and ratio < 0.9:
        return "LEAN YES"
    if ratio > 1.15 and ratio <= 1.5:
        return "LEAN NO (high)"
    return "DOES NOT SUPPORT"


def format_hypothesis_line(h: HypothesisResult) -> str:
    tag = hypothesis_verdict(h.ratio)
    return (
        f"  [{tag:18}] {h.name:40} ratio={h.ratio:.2f}  "
        f"(leaf={h.leaf_musd:,.0f}M vs target={h.target_musd:,.0f}M)"
    )


@dataclass(frozen=True)
class UseRowBreakdown:
    year: int
    intermediate_usd: float
    f01000_usd: float
    f02n00_usd: float
    t019_usd: float
    n_industries: int

    @property
    def intermediate_share(self) -> float:
        return self.intermediate_usd / self.t019_usd if self.t019_usd else 0.0

    @property
    def f01000_share(self) -> float:
        return self.f01000_usd / self.t019_usd if self.t019_usd else 0.0

    @property
    def f02n00_share(self) -> float:
        return self.f02n00_usd / self.t019_usd if self.t019_usd else 0.0


def _load_use_row(year: int) -> pd.Series:
    if year == 2017:
        table = _load_2017_detail_supply_use_usa("Use_SUT_detail")
    else:
        table = _load_benchmark_detail_supply_use_usa("Use_SUT_detail", year)
    row = pd.to_numeric(pd.Series(table.loc["S00300"]), errors="coerce").fillna(0.0)
    return row * MILLION_CURRENCY_TO_CURRENCY


def breakdown_use_row(year: int) -> UseRowBreakdown:
    row = _load_use_row(year)
    industries = row.reindex(list(USA_2017_INDUSTRY_CODES)).fillna(0.0)
    intermediate = float(industries.sum())
    f01000 = float(row.get("F01000", 0.0))
    f02n00 = float(row.get("F02N00", 0.0))
    t019 = float(row.get("T019", intermediate + f01000 + f02n00))
    n_ind = int((industries > 0).sum())
    return UseRowBreakdown(year, intermediate, f01000, f02n00, t019, n_ind)


def industry_column(year: int) -> pd.Series:
    row = _load_use_row(year)
    s = row.reindex(list(USA_2017_INDUSTRY_CODES)).fillna(0.0)
    s.index.name = "industry"
    return s.astype(float)


def _iea_imports_musd(year: int) -> pd.Series:
    path = IEA_INPUT / str(year) / f"BEA_IEA_{year}_Imports.csv"
    if not path.exists():
        raise FileNotFoundError(path)
    df = pd.read_csv(path)
    return (
        pd.to_numeric(df["DataValue"], errors="coerce")
        .groupby(df["TypeOfService"])
        .sum()
    )


def leaf_budget_musd(year: int) -> pd.Series:
    imp = _iea_imports_musd(year)
    leaves = imp.reindex(S00300_LEAVES).fillna(0.0)
    leaves.name = "musd"
    return leaves


def s00300_mcif_musd(year: int) -> float:
    if year == 2017:
        supply = _load_2017_detail_supply_use_usa("Supply_detail")
    else:
        supply = _load_benchmark_detail_supply_use_usa("Supply_detail", year)
    supply.columns = supply.columns.str.strip()
    return (
        float(pd.to_numeric(supply.loc["S00300", "MCIF"], errors="coerce"))
        * MILLION_CURRENCY_TO_CURRENCY
    )


@dataclass(frozen=True)
class HypothesisResult:
    name: str
    leaf_musd: float
    target_musd: float
    ratio: float
    note: str


def _sum_leaves(imp: pd.Series, leaves: tuple[str, ...]) -> float:
    return float(imp.reindex(list(leaves)).fillna(0).sum())


def grade_head_hypotheses(year: int) -> list[HypothesisResult]:
    imp = _iea_imports_musd(year)
    ind = industry_column(year)
    bd = breakdown_use_row(year)
    results: list[HypothesisResult] = []

    pairs: list[tuple[str, tuple[str, ...], str | None, str]] = [
        (
            "FinExplicitAndOth -> 523A00",
            ("FinExplicitAndOth",),
            "523A00",
            "explicit financial fees",
        ),
        (
            "TransportAirPort -> 481000",
            ("TransportAirPort",),
            "481000",
            "airport services abroad",
        ),
        (
            "GovtGoodsAndServicesNie -> S00500+S00600",
            ("GovtGoodsAndServicesNie",),
            None,
            "DoD / federal govt component",
        ),
        (
            "TransportSeaFreight -> 324110",
            ("TransportSeaFreight",),
            "324110",
            "foreign vessel freight to refineries",
        ),
    ]
    for name, leaves, industry, note in pairs:
        leaf = _sum_leaves(imp, leaves)
        if industry is None:
            target = float(ind.get("S00500", 0) + ind.get("S00600", 0)) / 1e6
        else:
            target = float(ind.get(industry, 0)) / 1e6
        ratio = leaf / target if target else float("nan")
        results.append(HypothesisResult(name, leaf, target, ratio, note))

    travel = _sum_leaves(imp, TRAVEL_LEAVES)
    results.append(
        HypothesisResult(
            "sum Travel* -> F01000 PCE",
            travel,
            bd.f01000_usd / 1e6,
            travel / (bd.f01000_usd / 1e6) if bd.f01000_usd else float("nan"),
            "type-1 travel abroad",
        )
    )
    cip = _sum_leaves(imp, CIP_LICENSES_LEAVES)
    results.append(
        HypothesisResult(
            "sum CipLicenses* -> F02N00 IP",
            cip,
            bd.f02n00_usd / 1e6,
            cip / (bd.f02n00_usd / 1e6) if bd.f02n00_usd else float("nan"),
            "licensing; capitalization TBD",
        )
    )
    return results


def concentration_table(ind: pd.Series) -> pd.DataFrame:
    pos = ind[ind > 0].sort_values(ascending=False)
    total = float(pos.sum())
    rows: list[dict[str, object]] = []
    cum = 0.0
    for rank, (code, val) in enumerate(pos.items(), start=1):
        cum += float(val)
        rows.append(
            {
                "rank": rank,
                "industry": code,
                "usd": val,
                "share": val / total if total else 0.0,
                "cumulative_share": cum / total if total else 0.0,
            }
        )
    return pd.DataFrame(rows)


def holdout_industry_share_stability(
    base_year: int = HOLDOUT_BASE, target_year: int = HOLDOUT_TARGET
) -> pd.DataFrame:
    """Rank industries by share movement between two benchmark years."""
    b = industry_column(base_year)
    t = industry_column(target_year)
    b_pos = b[b > 0]
    t_pos = t[t > 0]
    b_share = b_pos / b_pos.sum()
    t_share = t_pos / t_pos.sum()
    merged = pd.DataFrame({"share_base": b_share, "share_target": t_share}).fillna(0.0)
    merged["share_delta_pp"] = (merged["share_target"] - merged["share_base"]) * 100
    merged = merged.sort_values("share_target", ascending=False)
    merged.index.name = "industry"
    return merged


def holdout_hypothesis_comparison(
    base_year: int = HOLDOUT_BASE, target_year: int = HOLDOUT_TARGET
) -> pd.DataFrame:
    """Grade the same mapping guesses on two years side by side."""
    rows: list[dict[str, object]] = []
    by_year = {
        base_year: grade_head_hypotheses(base_year),
        target_year: grade_head_hypotheses(target_year),
    }
    for name in by_year[target_year]:
        key = name.name
        b = next(h for h in by_year[base_year] if h.name == key)
        t = next(h for h in by_year[target_year] if h.name == key)
        rows.append(
            {
                "hypothesis": key,
                "base_year": base_year,
                "target_year": target_year,
                "leaf_base_musd": b.leaf_musd,
                "leaf_target_musd": t.leaf_musd,
                "target_base_musd": b.target_musd,
                "target_target_musd": t.target_musd,
                "ratio_base": b.ratio,
                "ratio_target": t.ratio,
                "verdict_base": hypothesis_verdict(b.ratio),
                "verdict_target": hypothesis_verdict(t.ratio),
                "holdout_pass": hypothesis_verdict(b.ratio)
                == hypothesis_verdict(t.ratio)
                and hypothesis_verdict(t.ratio) in {"SUPPORTS", "LEAN YES"},
            }
        )
    return pd.DataFrame(rows)


def pce_s00300_double_count_check(year: int = 2017) -> pd.DataFrame:
    """Check whether NIPA PCE build already puts mass on S00300 x F01000.

    ``NIPA_final_dom_uses`` FBS rows carry sector columns only (no
    ``ActivityProducedBy``). Compare published NIPA travel-abroad PCE lines to
    resolved FBS commodity x final-demand pairs.
    """
    fbs = _resolve_both_sector_columns(
        pd.DataFrame(
            FlowBySector.generateFlowBySector(
                f'NIPA_final_dom_uses_{year}', download_sources_ok=False
            )
        )
    )
    s00300_f01000 = fbs.loc[
        (fbs['SectorProducedBy'] == 'S00300') & (fbs['SectorConsumedBy'] == 'F01000')
    ]
    all_s00300 = fbs.loc[
        (fbs['SectorProducedBy'] == 'S00300') | (fbs['SectorConsumedBy'] == 'S00300')
    ]

    nipa = pd.DataFrame(
        getFlowByActivity('BEA_NIPA', year=year, download_FBA_if_missing=False)
    )

    rows: list[dict[str, object]] = []
    fbs_s00300_f01000_usd = float(s00300_f01000['FlowAmount'].sum())
    for activity in PCE_S00300_ACTIVITIES:
        sub = nipa.loc[nipa['ActivityProducedBy'].astype(str) == activity]
        rows.append(
            {
                'activity': activity,
                'nipa_rows': int(len(sub)),
                'nipa_usd': float(sub['FlowAmount'].sum()) if len(sub) else 0.0,
                'fbs_s00300_f01000_usd': fbs_s00300_f01000_usd,
                'note': 'FBS is activity-blind; total is aggregate S00300 x F01000',
            }
        )
    rows.append(
        {
            'activity': '_SUM_travel_abroad_NIPA',
            'nipa_rows': int(
                nipa['ActivityProducedBy'].astype(str).isin(PCE_S00300_ACTIVITIES).sum()
            ),
            'nipa_usd': float(
                nipa.loc[
                    nipa['ActivityProducedBy'].astype(str).isin(PCE_S00300_ACTIVITIES),
                    'FlowAmount',
                ].sum()
            ),
            'fbs_s00300_f01000_usd': float(s00300_f01000['FlowAmount'].sum()),
            'note': 'double-count if fbs > 0 while we also add Travel* leaves',
        }
    )
    rows.append(
        {
            'activity': '_ALL_S00300_in_NIPA_FD_FBS',
            'nipa_rows': int(len(all_s00300)),
            'nipa_usd': float(all_s00300['FlowAmount'].sum()),
            'fbs_s00300_f01000_usd': float(s00300_f01000['FlowAmount'].sum()),
            'note': 'any S00300 row in resolved NIPA final-dom FBS',
        }
    )
    return pd.DataFrame(rows)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # --- published Use rows ---
    breakdown_rows: list[dict[str, object]] = []
    for year in BENCHMARK_YEARS:
        bd = breakdown_use_row(year)
        breakdown_rows.append(
            {
                "year": bd.year,
                "intermediate_usd": bd.intermediate_usd,
                "f01000_usd": bd.f01000_usd,
                "f02n00_usd": bd.f02n00_usd,
                "t019_usd": bd.t019_usd,
                "intermediate_share": bd.intermediate_share,
                "f01000_share": bd.f01000_share,
                "f02n00_share": bd.f02n00_share,
                "n_industries": bd.n_industries,
                "mcif_usd": s00300_mcif_musd(year),
            }
        )
    breakdown_df = pd.DataFrame(breakdown_rows)
    breakdown_df.to_csv(OUT_DIR / "s00300_use_breakdown_benchmarks.csv", index=False)

    ind17 = industry_column(2017)
    conc = concentration_table(ind17)
    conc.to_csv(OUT_DIR / "s00300_industry_concentration_2017.csv", index=False)

    top = ind17[ind17 > 0].sort_values(ascending=False).head(25)
    top_m = top / 1e6
    top_df = pd.DataFrame({"industry": top_m.index, "musd": top_m.values})
    top_df.to_csv(OUT_DIR / "s00300_top_industries_2017.csv", index=False)

    # --- IEA leaf budget ---
    leaf_rows: list[dict[str, object]] = []
    for year in OVERLAY_YEARS:
        path = IEA_INPUT / str(year) / f"BEA_IEA_{year}_Imports.csv"
        if not path.exists():
            continue
        leaves = leaf_budget_musd(year)
        total = float(leaves.sum())
        for leaf, val in leaves.items():
            leaf_rows.append(
                {
                    "year": year,
                    "leaf": leaf,
                    "musd": val,
                    "share_of_leaves": val / total if total else 0.0,
                }
            )
        leaf_rows.append(
            {
                "year": year,
                "leaf": "_TOTAL",
                "musd": total,
                "share_of_leaves": 1.0,
            }
        )
    leaf_df = pd.DataFrame(leaf_rows)
    leaf_df.to_csv(OUT_DIR / "s00300_iea_leaf_budget.csv", index=False)

    # --- head hypotheses (2017 and holdout base year) ---
    for year in (HOLDOUT_TARGET, HOLDOUT_BASE):
        hyp = grade_head_hypotheses(year)
        hyp_df = pd.DataFrame([h.__dict__ for h in hyp])
        hyp_df.to_csv(OUT_DIR / f"s00300_head_hypotheses_{year}.csv", index=False)

    holdout_hyp = holdout_hypothesis_comparison(HOLDOUT_BASE, HOLDOUT_TARGET)
    holdout_hyp.to_csv(OUT_DIR / "s00300_holdout_hypotheses_2012_2017.csv", index=False)

    pce_check = pce_s00300_double_count_check(HOLDOUT_TARGET)
    pce_check.to_csv(OUT_DIR / "s00300_pce_double_count_check_2017.csv", index=False)

    # --- holdout: 2012 vs 2017 industry shares ---
    holdout = holdout_industry_share_stability(HOLDOUT_BASE, HOLDOUT_TARGET)
    holdout.to_csv(OUT_DIR / "s00300_holdout_industry_shares_2012_2017.csv")

    # --- console summary ---
    print("=== S00300 Use row (benchmark years, USD) ===")
    print(breakdown_df.to_string(index=False))

    print("\n=== 2017 industry concentration ===")
    for n in (10, 25, 50):
        sub = conc.loc[conc["rank"] <= n, "cumulative_share"]
        if not sub.empty:
            print(f"  top {n:2d} industries: {sub.iloc[-1]:.1%} of intermediate")

    print("\n=== 2017 mapping guesses (ratio = leaf / published; ~1.0 = fits) ===")
    for h in grade_head_hypotheses(HOLDOUT_TARGET):
        print(format_hypothesis_line(h))

    print(f"\n=== {HOLDOUT_BASE} mapping guesses (holdout base year) ===")
    for h in grade_head_hypotheses(HOLDOUT_BASE):
        print(format_hypothesis_line(h))

    print(f"\n=== Holdout: same guess on {HOLDOUT_BASE} vs {HOLDOUT_TARGET} ===")
    for _, row in holdout_hyp.iterrows():
        status = "PASS" if row["holdout_pass"] else "FAIL"
        print(
            f"  [{status:4}] {row['hypothesis']:40}  "
            f"{HOLDOUT_BASE}={row['ratio_base']:.2f} ({row['verdict_base']})  "
            f"{HOLDOUT_TARGET}={row['ratio_target']:.2f} ({row['verdict_target']})"
        )

    print("\n=== PCE double-count check (NIPA_final_dom_uses_2017) ===")
    for _, row in pce_check.iterrows():
        print(
            f"  {row['activity'][:45]:45}  "
            f"NIPA=${row['nipa_usd']/1e6:,.0f}M  "
            f"FBS S00300/F01000=${row['fbs_s00300_f01000_usd']/1e6:,.0f}M"
        )

    print("\n=== IEA leaf total ($M) by overlay year ===")
    totals = leaf_df[leaf_df["leaf"] == "_TOTAL"].set_index("year")["musd"]
    print(totals.to_string())

    print("\n=== Holdout: top 2017 industries, share delta 2012-2017 (pp) ===")
    head_codes = list(top_df["industry"].head(10))
    for code in head_codes:
        if code in holdout.index:
            row = holdout.loc[code]
            print(
                f"  {code}: share_2017={row['share_target']:.1%}  "
                f"delta_pp={row['share_delta_pp']:+.2f}"
            )

    print(f"\nWrote CSVs to {OUT_DIR}")


if __name__ == "__main__":
    main()
