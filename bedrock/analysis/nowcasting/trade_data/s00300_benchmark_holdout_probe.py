"""Deep benchmark holdout for #767 — licenses, affiliation policies, synthetic Use rows.

Extends ``s00300_use_distribution_probe`` and ``s00300_issue_source_probe`` with:

- 2007 / 2012 / 2017 published Use-row benchmarks
- License policy scenarios (unaffiliated/affiliated split, cap-rate holdouts)
- STEC Table 6.2 IP-share routing for affiliated license mass
- Partial synthetic ``S00300`` Use rows vs published Detail panels
- PCE double-count check for 2012 and 2017

Run::

    uv run python -m bedrock.analysis.nowcasting.trade_data.s00300_benchmark_holdout_probe

Writes CSVs under ``output/s00300_benchmark_holdout_*.csv``.
"""

from __future__ import annotations

import json
import subprocess
import time
import urllib.parse
from dataclasses import dataclass

import numpy as np
import pandas as pd

from bedrock.analysis.nowcasting.trade_data.s00300_use_distribution_probe import (
    CIP_LICENSES_LEAVES,
    OUT_DIR,
    S00300_LEAVES,
    TRAVEL_LEAVES,
    _iea_imports_musd,
    _sum_leaves,
    breakdown_use_row,
    hypothesis_verdict,
    industry_column,
    pce_s00300_double_count_check,
    s00300_mcif_musd,
)
from bedrock.utils.config.common import load_env_file_key
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES

BENCHMARK_YEARS = (2007, 2012, 2017)
IEA_HOLDOUT_YEARS = (2012, 2017)
CAP_RATE_YEARS = (2007, 2012, 2017)
AFFILIATION_PATH = OUT_DIR / "s00300_affiliation_split_summary.csv"
STEC_PATH = OUT_DIR / "stec_table_62_imports_2012_2017.csv"

EXTRA_LICENSE_LEAVES = (
    "CipLicensesCompSoftware",
    "CipLicensesBooksSoundRecord",
    "CipLicensesMoviesTv",
    "CipLicensesBroadcastLiveRecord",
    "CipLicensesAudVis",
    "CipLicensesFranchisesTrademarks",
)

# Rough Detail proxies for STEC major industries (validation only, not production).
MAJOR_INDUSTRY_PREFIXES: dict[str, tuple[str, ...]] = {
    "Mining": ("211", "212", "213"),
    "Manufacturing": ("31", "32", "33"),
    "Wholesale trade": ("42",),
    "Retail trade": ("44", "45"),
    "Information": ("51", "517"),
    "Finance and insurance": ("52", "523"),
    "Real estate and rental and leasing": ("53",),
    "Professional, scientific, and technical services": ("541",),
}

TRANSPORT_INDUSTRY_CODES = (
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
GOVT_INDUSTRY_CODES = ("S00500", "S00600", "S00102")


@dataclass(frozen=True)
class LicenseMass:
    year: int
    all_musd: float
    unaffiliated_musd: float
    affiliated_musd: float
    f02n00_published_musd: float

    @property
    def cap_rate_all(self) -> float:
        return self.f02n00_published_musd / self.all_musd if self.all_musd else 0.0

    @property
    def cap_rate_unaff(self) -> float:
        return (
            self.f02n00_published_musd / self.unaffiliated_musd
            if self.unaffiliated_musd
            else float("nan")
        )


def _load_affiliation_summary() -> pd.DataFrame:
    if not AFFILIATION_PATH.exists():
        raise FileNotFoundError(
            f"Missing {AFFILIATION_PATH}; run s00300_issue_source_probe first"
        )
    return pd.read_csv(AFFILIATION_PATH)


def _load_stec() -> pd.DataFrame:
    if not STEC_PATH.exists():
        raise FileNotFoundError(f"Missing {STEC_PATH}; run stec_table_62_pull first")
    return pd.read_csv(STEC_PATH)


def license_mass_by_year(year: int, aff: pd.DataFrame) -> LicenseMass:
    imp = _iea_imports_musd(year)
    all_m = _sum_leaves(imp, CIP_LICENSES_LEAVES)
    sub = aff[(aff["year"] == year) & aff["type_of_service"].isin(CIP_LICENSES_LEAVES)]
    unaff = float(sub["Unaffiliated"].fillna(0).sum())
    affd = float(sub["Affiliated"].fillna(0).sum())
    f02 = breakdown_use_row(year).f02n00_usd / 1e6
    return LicenseMass(year, all_m, unaff, affd, f02)


def comparable_license_mass(year: int) -> float:
    """IEA license leaves routed to comparable Detail (not ``S00300``)."""
    imp = _iea_imports_musd(year)
    return float(imp.reindex(list(EXTRA_LICENSE_LEAVES)).fillna(0).sum())


def license_leaf_detail(year: int, aff: pd.DataFrame) -> pd.DataFrame:
    imp = _iea_imports_musd(year)
    rows: list[dict[str, object]] = []
    for leaf in CIP_LICENSES_LEAVES:
        row = aff[(aff["year"] == year) & (aff["type_of_service"] == leaf)]
        if row.empty:
            continue
        r = row.iloc[0]
        rows.append(
            {
                "year": year,
                "leaf": leaf,
                "all_musd": float(imp.get(leaf, 0)),
                "affiliated_musd": float(r.get("Affiliated") or 0),
                "unaffiliated_musd": float(r.get("Unaffiliated") or 0),
                "affiliated_share": float(r.get("affiliated_share") or 0),
                "usparents_musd": (
                    float(r.get("UsParents") or 0)
                    if pd.notna(r.get("UsParents"))
                    else None
                ),
            }
        )
    return pd.DataFrame(rows)


def license_leaves_musd(year: int) -> float:
    """S00300 ``CipLicenses*`` import leaves (million USD)."""
    if year in IEA_HOLDOUT_YEARS:
        imp = _iea_imports_musd(year)
        return _sum_leaves(imp, CIP_LICENSES_LEAVES)
    return _pull_license_leaves_api(year)


def _pull_license_leaves_api(year: int) -> float:
    """Pull license leaves for years without local IEA cache (e.g. 2007)."""
    key = load_env_file_key("api_key", "BEA")
    total = 0.0
    for leaf in CIP_LICENSES_LEAVES:
        params = {
            "UserID": key,
            "method": "GetData",
            "ResultFormat": "JSON",
            "DataSetName": "IntlServTrade",
            "Year": str(year),
            "TradeDirection": "Imports",
            "Affiliation": "AllAffiliations",
            "TypeOfService": leaf,
            "AreaOrCountry": "AllCountries",
        }
        url = "https://apps.bea.gov/api/data?" + urllib.parse.urlencode(params)
        proc = subprocess.run(
            ["curl.exe", "-sL", url], capture_output=True, text=True, check=True
        )
        time.sleep(0.5)
        payload = json.loads(proc.stdout)
        results = payload.get("BEAAPI", {}).get("Results", {})
        if results.get("Error"):
            raise RuntimeError(f"BEA IntlServTrade {year} {leaf}: {results['Error']}")
        row = results.get("Data")
        if isinstance(row, list):
            row = row[0] if row else {}
        raw = str((row or {}).get("DataValue", "")).strip().replace(",", "")
        if raw:
            total += float(raw)
    return total


def cap_rate_holdout_matrix() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Cap rate = published F02N00 / license leaves; cross-year holdout matrix."""
    summary_rows: list[dict[str, object]] = []
    caps: dict[int, float] = {}
    for year in CAP_RATE_YEARS:
        lic = license_leaves_musd(year)
        f02 = breakdown_use_row(year).f02n00_usd / 1e6
        cap = f02 / lic if lic else float("nan")
        caps[year] = cap
        summary_rows.append(
            {
                "year": year,
                "license_leaves_musd": lic,
                "f02n00_published_musd": f02,
                "cap_rate": cap,
                "licenses_over_f02n00": lic / f02 if f02 else float("nan"),
                "iea_source": "local_cache" if year in IEA_HOLDOUT_YEARS else "bea_api",
            }
        )
    summary = pd.DataFrame(summary_rows)

    matrix_rows: list[dict[str, object]] = []
    for train in CAP_RATE_YEARS:
        for test in CAP_RATE_YEARS:
            lic_test = float(
                summary.loc[summary["year"] == test, "license_leaves_musd"].iloc[0]
            )
            f02_test = float(
                summary.loc[summary["year"] == test, "f02n00_published_musd"].iloc[0]
            )
            pred = lic_test * caps[train]
            ratio = pred / f02_test if f02_test else float("nan")
            matrix_rows.append(
                {
                    "train_year": train,
                    "test_year": test,
                    "train_cap_rate": caps[train],
                    "predicted_f02n00_musd": pred,
                    "actual_f02n00_musd": f02_test,
                    "ratio": ratio,
                    "verdict": (
                        hypothesis_verdict(ratio) if np.isfinite(ratio) else "N/A"
                    ),
                }
            )
    return summary, pd.DataFrame(matrix_rows)


def grade_license_policies(aff: pd.DataFrame) -> pd.DataFrame:
    """National license routing scenarios; holdout = both years near 1.0."""
    masses = {y: license_mass_by_year(y, aff) for y in IEA_HOLDOUT_YEARS}
    m12, m17 = masses[2012], masses[2017]

    scenarios: list[tuple[str, str, float, float]] = []

    def add(name: str, note: str, r12: float, r17: float) -> None:
        scenarios.append((name, note, r12, r17))

    # Baseline failures.
    add(
        "all_licenses_to_F02N00",
        "Naive: all S00300 license leaves -> F02N00",
        m12.all_musd / m12.f02n00_published_musd,
        m17.all_musd / m17.f02n00_published_musd,
    )
    add(
        "unaffiliated_raw_to_F02N00",
        "Unaffiliated only -> F02N00 (no scale)",
        m12.unaffiliated_musd / m12.f02n00_published_musd,
        m17.unaffiliated_musd / m17.f02n00_published_musd,
    )
    add(
        "F02N00_over_unaffiliated",
        "Published F02N00 / unaffiliated (cap multiplier)",
        (
            m12.f02n00_published_musd / m12.unaffiliated_musd
            if m12.unaffiliated_musd
            else float("nan")
        ),
        (
            m17.f02n00_published_musd / m17.unaffiliated_musd
            if m17.unaffiliated_musd
            else float("nan")
        ),
    )

    # Constant cap-rate holdouts (train on one year, apply to other).
    cap17 = m17.cap_rate_all
    cap12 = m12.cap_rate_all
    add(
        "cap_rate_2017_applied_to_2012",
        f"2017 cap rate ({cap17:.1%}) x 2012 all licenses vs 2012 F02N00",
        (m12.all_musd * cap17) / m12.f02n00_published_musd,
        1.0,
    )
    add(
        "cap_rate_2012_applied_to_2017",
        f"2012 cap rate ({cap12:.1%}) x 2017 all licenses vs 2017 F02N00",
        1.0,
        (m17.all_musd * cap12) / m17.f02n00_published_musd,
    )

    # Policy: unaffiliated -> F02N00 scaled to match published each year (fit, not predict).
    add(
        "unaff_scaled_to_F02N00",
        "Policy fit: scale unaffiliated to published F02N00 each year",
        1.0,
        1.0,
    )

    # Residual affiliated mass after unaffiliated capitalized to published F02N00.
    res12 = m12.all_musd - min(m12.unaffiliated_musd, m12.f02n00_published_musd)
    res17 = m17.all_musd - min(m17.unaffiliated_musd, m17.f02n00_published_musd)
    scenarios.append(
        (
            "affiliated_residual_after_unaff_cap",
            "All licenses minus min(unaff, F02N00) -> intermediate budget ($M)",
            res12,
            res17,
        )
    )

    rd12 = float(
        license_leaf_detail(2012, aff)
        .loc[
            lambda d: d["leaf"] == "CipLicensesOutcomesResearchAndDev",
            "unaffiliated_musd",
        ]
        .sum()
    )
    rd17 = float(
        license_leaf_detail(2017, aff)
        .loc[
            lambda d: d["leaf"] == "CipLicensesOutcomesResearchAndDev",
            "unaffiliated_musd",
        ]
        .sum()
    )
    add(
        "rd_unaff_only_to_F02N00",
        "Only CipLicensesOutcomesResearchAndDev unaffiliated vs F02N00",
        rd12 / m12.f02n00_published_musd,
        rd17 / m17.f02n00_published_musd,
    )

    rows: list[dict[str, object]] = []
    for name, note, r12, r17 in scenarios:
        holdout: bool | str | None
        if name == "affiliated_residual_after_unaff_cap":
            verdict12 = f"INTERMEDIATE_{r12:,.0f}M"
            verdict17 = f"INTERMEDIATE_{r17:,.0f}M"
            holdout = None
        elif name == "unaff_scaled_to_F02N00":
            verdict12 = "POLICY_FIT"
            verdict17 = "POLICY_FIT"
            holdout = "FIT_ONLY"
        else:
            verdict12 = hypothesis_verdict(r12) if np.isfinite(r12) else "N/A"
            verdict17 = hypothesis_verdict(r17) if np.isfinite(r17) else "N/A"
            holdout = (
                verdict12 == verdict17
                and verdict12 in {"SUPPORTS", "LEAN YES"}
                and verdict17 in {"SUPPORTS", "LEAN YES"}
            )
        rows.append(
            {
                "scenario": name,
                "note": note,
                "ratio_2012": r12,
                "ratio_2017": r17,
                "verdict_2012": verdict12,
                "verdict_2017": verdict17,
                "holdout_pass": holdout,
            }
        )
    return pd.DataFrame(rows)


def stec_ip_shares(year: int, stec: pd.DataFrame) -> pd.Series:
    sub = stec[
        (stec["year"] == year)
        & (stec["service_type"] == "charges_ip")
        & (stec["major_industry"] != "All industries")
    ].dropna(subset=["musd"])
    total = float(sub["musd"].sum())
    if not total:
        return pd.Series(dtype=float)
    shares = sub.set_index("major_industry")["musd"] / total
    return shares.sort_values(ascending=False)


def stec_share_stability(stec: pd.DataFrame) -> pd.DataFrame:
    s12 = stec_ip_shares(2012, stec)
    s17 = stec_ip_shares(2017, stec)
    merged = pd.DataFrame({"share_2012": s12, "share_2017": s17}).fillna(0.0)
    merged["delta_pp"] = (merged["share_2017"] - merged["share_2012"]) * 100
    merged["abs_delta_pp"] = merged["delta_pp"].abs()
    merged = merged.sort_values("share_2017", ascending=False)
    merged.index.name = "major_industry"
    return merged.reset_index()


def _industries_for_major(code: str, majors: dict[str, tuple[str, ...]]) -> list[str]:
    prefixes = majors.get(code, ())
    out: list[str] = []
    for ind in USA_2017_INDUSTRY_CODES:
        s = str(ind)
        if s.startswith("S"):
            continue
        if any(s.startswith(p) for p in prefixes):
            out.append(s)
    return out


def published_major_proxy_shares(year: int) -> pd.Series:
    """Published intermediate mass by STEC major proxy (Detail prefix groups)."""
    ind = industry_column(year)
    pos = ind[ind > 0]
    total = float(pos.sum())
    shares: dict[str, float] = {}
    assigned: set[str] = set()
    for major, prefixes in MAJOR_INDUSTRY_PREFIXES.items():
        codes = _industries_for_major(str(major), MAJOR_INDUSTRY_PREFIXES)
        val = float(pos.reindex(codes).fillna(0).sum())
        shares[major] = val / total if total else 0.0
        assigned.update(codes)
    other = float(
        pos.drop(labels=[c for c in pos.index if c in assigned], errors="ignore").sum()
    )
    shares["_other_detail"] = other / total if total else 0.0
    return pd.Series(shares).sort_values(ascending=False)


def affiliated_license_stec_routing(
    year: int, aff: pd.DataFrame, stec: pd.DataFrame
) -> pd.DataFrame:
    """Allocate affiliated license mass by STEC IP shares; compare to published proxies."""
    lic = license_mass_by_year(year, aff)
    stec_sh = stec_ip_shares(year, stec)
    pub_sh = published_major_proxy_shares(year)

    rows: list[dict[str, object]] = []
    for major in stec_sh.index:
        pred_share = float(stec_sh[major])
        pred_musd = lic.affiliated_musd * pred_share
        pub_share = float(pub_sh.get(major, 0.0))
        rows.append(
            {
                "year": year,
                "major_industry": major,
                "affiliated_license_musd": lic.affiliated_musd,
                "stec_ip_share": pred_share,
                "predicted_intermediate_musd": pred_musd,
                "published_proxy_share": pub_share,
                "share_ratio_stec_over_pub": (
                    pred_share / pub_share if pub_share else float("nan")
                ),
            }
        )
    return pd.DataFrame(rows)


def _major_proxy_usd(ind: pd.Series, major: str) -> float:
    codes = _industries_for_major(major, MAJOR_INDUSTRY_PREFIXES)
    return float(ind.reindex(codes).fillna(0).sum())


@dataclass
class PolicySpec:
    name: str
    note: str


def _empty_use_vectors() -> tuple[pd.Series, float, float]:
    ind = pd.Series(0.0, index=list(USA_2017_INDUSTRY_CODES))
    return ind, 0.0, 0.0


def _allocate_license_intermediate_proportional(
    ind: pd.Series,
    pub: pd.Series,
    mass_usd: float,
    *,
    exclude: set[str] | None = None,
) -> pd.Series:
    """Route license intermediate mass proportional to published positive industries."""
    pool = pub[pub > 0].copy()
    if exclude:
        pool = pool.drop(
            labels=[c for c in exclude if c in pool.index], errors='ignore'
        )
    if mass_usd <= 0 or pool.sum() <= 0:
        return ind
    weights = pool / pool.sum()
    return ind.add(weights * mass_usd, fill_value=0.0)


def _allocate_license_intermediate_stec(
    ind: pd.Series,
    pub: pd.Series,
    mass_usd: float,
    year: int,
    stec: pd.DataFrame,
) -> pd.Series:
    """Route license intermediate mass by STEC IP major-industry shares."""
    if mass_usd <= 0:
        return ind
    stec_sh = stec_ip_shares(year, stec)
    for major, share in stec_sh.items():
        codes = _industries_for_major(str(major), MAJOR_INDUSTRY_PREFIXES)
        if not codes:
            continue
        within = pub.reindex(codes).fillna(0)
        within = within[within > 0]
        if within.sum() <= 0:
            continue
        w = within / within.sum()
        ind.loc[w.index] += mass_usd * share * w
    return ind


def license_intermediate_mass_usd(year: int, imp: pd.Series | None = None) -> float:
    """Intermediate budget from license leaves after cap-rate capitalization."""
    if imp is None:
        imp = _iea_imports_musd(year)
    lic_all = _sum_leaves(imp, CIP_LICENSES_LEAVES) * 1e6
    cap = breakdown_use_row(year).f02n00_usd / (
        _sum_leaves(imp, CIP_LICENSES_LEAVES) * 1e6
    )
    return lic_all * (1.0 - cap)


def grade_license_slice_ab(
    year: int,
    _aff: pd.DataFrame,
    stec: pd.DataFrame,
) -> dict[str, object]:
    """Compare proportional vs STEC routing for the license intermediate slice."""
    pub = industry_column(year)
    imp = _iea_imports_musd(year)
    mass = license_intermediate_mass_usd(year, imp)

    prop = _allocate_license_intermediate_proportional(
        pd.Series(0.0, index=pub.index), pub, mass
    )
    stec_ind = _allocate_license_intermediate_stec(
        pd.Series(0.0, index=pub.index), pub, mass, year, stec
    )

    pub_lic_proxy = pub[pub > 0]
    # Industries that receive license-related mass in published row: use all positive
    # industries; the slice is graded on where published puts intermediate license mass.
    common = pub_lic_proxy.index
    pub_v = pub.reindex(common).fillna(0)
    prop_err = (prop.reindex(common).fillna(0) - pub_v).abs()
    stec_err = (stec_ind.reindex(common).fillna(0) - pub_v).abs()
    rel_pub = pub_v.replace(0, np.nan)
    prop_mape = float((prop_err / rel_pub).mean())
    stec_mape = float((stec_err / rel_pub).mean())

    top10 = pub[pub > 0].sort_values(ascending=False).head(10).index
    prop_top10 = float((prop_err / rel_pub).reindex(top10).mean())
    stec_top10 = float((stec_err / rel_pub).reindex(top10).mean())

    return {
        'year': year,
        'license_intermediate_usd': mass,
        'proportional_total_abs_error_usd': float(prop_err.sum()),
        'stec_total_abs_error_usd': float(stec_err.sum()),
        'proportional_all_industry_mape': prop_mape,
        'stec_all_industry_mape': stec_mape,
        'proportional_top10_mape': prop_top10,
        'stec_top10_mape': stec_top10,
        'proportional_wins_top10': prop_top10 < stec_top10,
        'proportional_wins_total_abs': float(prop_err.sum()) < float(stec_err.sum()),
        'skip_stec_concordance': prop_top10 <= stec_top10 * 1.05,
    }


def per_industry_error_table(
    year: int,
    policy: str,
    aff: pd.DataFrame,
    stec: pd.DataFrame,
    transport_share_ref_year: int | None = None,
) -> pd.DataFrame:
    """Per-industry synthetic vs published errors for a policy."""
    pub = industry_column(year)
    syn, _, _ = build_synthetic_use(year, policy, aff, stec, transport_share_ref_year)
    common = pub[pub > 0].index.union(syn[syn > 0].index)
    pub_v = pub.reindex(common).fillna(0)
    syn_v = syn.reindex(common).fillna(0)
    abs_err = (syn_v - pub_v).abs()
    rel_err = abs_err / pub_v.replace(0, np.nan)
    rows: list[dict[str, object]] = []
    for code in common:
        rows.append(
            {
                'year': year,
                'policy': policy,
                'industry': code,
                'published_usd': float(pub_v[code]),
                'synthetic_usd': float(syn_v[code]),
                'abs_error_usd': float(abs_err[code]),
                'rel_error': float(rel_err[code]) if pub_v[code] else float('nan'),
            }
        )
    return pd.DataFrame(rows).sort_values('published_usd', ascending=False)


def build_synthetic_use(
    year: int,
    policy: str,
    aff: pd.DataFrame,
    stec: pd.DataFrame,
    transport_share_ref_year: int | None = 2017,
) -> tuple[pd.Series, float, float]:
    """Build a partial synthetic ``S00300`` Use row under a named policy."""
    imp = _iea_imports_musd(year)
    ind, f01000, f02n00 = _empty_use_vectors()
    pub = industry_column(year)
    pub_pos = pub[pub > 0]

    if policy == "published_baseline":
        return (
            pub.copy(),
            breakdown_use_row(year).f01000_usd,
            breakdown_use_row(year).f02n00_usd,
        )

    # --- licenses ---
    lic = license_mass_by_year(year, aff)
    if policy in {
        "P1_unaff_F02_aff_STEC",
        "P2_heads_plus_licenses",
        "P3_block_routing",
    }:
        f02n00 = lic.unaffiliated_musd * 1e6  # scale-to-unaff policy variant
        # Match published F02N00 exactly (policy fit per year).
        f02n00 = breakdown_use_row(year).f02n00_usd
        aff_mass = lic.affiliated_musd * 1e6
        stec_sh = stec_ip_shares(year, stec)
        for major, share in stec_sh.items():
            codes = _industries_for_major(str(major), MAJOR_INDUSTRY_PREFIXES)
            if not codes:
                continue
            major_pub = float(pub.reindex(codes).fillna(0).sum())
            if major_pub <= 0:
                continue
            within = pub.reindex(codes).fillna(0)
            within = within[within > 0]
            w = within / within.sum()
            ind.loc[w.index] += aff_mass * share * w
    elif policy == "P_cap_rate_F02":
        cap = breakdown_use_row(year).f02n00_usd / (
            _sum_leaves(imp, CIP_LICENSES_LEAVES) * 1e6
        )
        f02n00 = _sum_leaves(imp, CIP_LICENSES_LEAVES) * cap * 1e6
        aff_mass = (1 - cap) * _sum_leaves(imp, CIP_LICENSES_LEAVES) * 1e6
        ind = _allocate_license_intermediate_stec(ind, pub, aff_mass, year, stec)
    elif policy == "P_cap_rate_prop":
        cap = breakdown_use_row(year).f02n00_usd / (
            _sum_leaves(imp, CIP_LICENSES_LEAVES) * 1e6
        )
        f02n00 = _sum_leaves(imp, CIP_LICENSES_LEAVES) * cap * 1e6
        aff_mass = (1 - cap) * _sum_leaves(imp, CIP_LICENSES_LEAVES) * 1e6
        ind = _allocate_license_intermediate_proportional(ind, pub, aff_mass)
    elif policy == "P_phase1":
        cap = breakdown_use_row(year).f02n00_usd / (
            _sum_leaves(imp, CIP_LICENSES_LEAVES) * 1e6
        )
        f02n00 = _sum_leaves(imp, CIP_LICENSES_LEAVES) * cap * 1e6
        fin = float(imp.get("FinExplicitAndOth", 0)) * 1e6
        ind["523A00"] = ind.get("523A00", 0) + fin
        air = float(imp.get("TransportAirPort", 0)) * 1e6
        ind["481000"] = ind.get("481000", 0) + air
        assigned = {c for c in ind.index if ind.get(c, 0) > 0}
        lic_int = (1 - cap) * _sum_leaves(imp, CIP_LICENSES_LEAVES) * 1e6
        ind = _allocate_license_intermediate_proportional(
            ind, pub, lic_int, exclude=assigned
        )
        assigned.update(c for c in ind.index if ind.get(c, 0) > 0)
        govt_leaf = float(imp.get("GovtGoodsAndServicesNie", 0)) * 1e6
        govt_pub = pub.reindex(list(GOVT_INDUSTRY_CODES)).fillna(0)
        if govt_pub.sum() > 0:
            for code, val in (govt_pub / govt_pub.sum() * govt_leaf).items():
                ind[code] = ind.get(code, 0) + val
        ref = transport_share_ref_year or year
        pub_ref = industry_column(ref)
        t_pub = pub_ref.reindex(list(TRANSPORT_INDUSTRY_CODES)).fillna(0)
        t_pub = t_pub[t_pub > 0]
        sea_mass = (
            float(imp.get("TransportSeaFreight", 0))
            + float(imp.get("TransportSeaPort", 0))
        ) * 1e6
        if t_pub.sum() > 0 and sea_mass > 0:
            for code, val in (t_pub / t_pub.sum() * sea_mass).items():
                ind[code] = ind.get(code, 0) + val
        assigned_leaves = {
            "FinExplicitAndOth",
            "TransportAirPort",
            "GovtGoodsAndServicesNie",
            *TRAVEL_LEAVES,
            *CIP_LICENSES_LEAVES,
            "TransportSeaFreight",
            "TransportSeaPort",
        }
        residual_m = (
            _sum_leaves(
                imp,
                tuple(leaf for leaf in S00300_LEAVES if leaf not in assigned_leaves),
            )
            * 1e6
        )
        residual_pool = pub_pos.copy()
        for code in ind.index:
            if ind.get(code, 0) > 0:
                residual_pool[code] = 0
        if residual_pool.sum() > 0 and residual_m > 0:
            w = residual_pool / residual_pool.sum()
            ind = ind.add(w * residual_m, fill_value=0)
        f01000 = breakdown_use_row(year).f01000_usd
        return ind, f01000, f02n00
    elif policy == "P0_all_licenses_F02N00":
        f02n00 = _sum_leaves(imp, CIP_LICENSES_LEAVES) * 1e6
    else:
        f02n00 = breakdown_use_row(year).f02n00_usd

    if policy in {"P2_heads_plus_licenses", "P3_block_routing"}:
        fin = float(imp.get("FinExplicitAndOth", 0)) * 1e6
        ind["523A00"] = ind.get("523A00", 0) + fin
        air = float(imp.get("TransportAirPort", 0)) * 1e6
        ind["481000"] = ind.get("481000", 0) + air

    if policy == "P3_block_routing":
        # Travel -> published F01000 (NIPA-reconciled policy: use published level).
        f01000 = breakdown_use_row(year).f01000_usd

        # Govt -> S* by published within-year shares.
        govt_leaf = float(imp.get("GovtGoodsAndServicesNie", 0)) * 1e6
        govt_pub = pub.reindex(list(GOVT_INDUSTRY_CODES)).fillna(0)
        if govt_pub.sum() > 0:
            for code, val in (govt_pub / govt_pub.sum() * govt_leaf).items():
                ind[code] = ind.get(code, 0) + val

        # Transport sea/port -> published transport code shares (ref year or same year).
        ref = transport_share_ref_year or year
        pub_ref = industry_column(ref)
        t_pub = pub_ref.reindex(list(TRANSPORT_INDUSTRY_CODES)).fillna(0)
        t_pub = t_pub[t_pub > 0]
        sea_mass = (
            float(imp.get("TransportSeaFreight", 0))
            + float(imp.get("TransportSeaPort", 0))
        ) * 1e6
        if t_pub.sum() > 0 and sea_mass > 0:
            for code, val in (t_pub / t_pub.sum() * sea_mass).items():
                ind[code] = ind.get(code, 0) + val

        # Residual intermediate: OthBusiness, TradeRelated, Const*, etc.
        assigned_leaves = {
            "FinExplicitAndOth",
            "TransportAirPort",
            "GovtGoodsAndServicesNie",
            *TRAVEL_LEAVES,
            *CIP_LICENSES_LEAVES,
            "TransportSeaFreight",
            "TransportSeaPort",
        }
        residual_m = (
            _sum_leaves(
                imp,
                tuple(leaf for leaf in S00300_LEAVES if leaf not in assigned_leaves),
            )
            * 1e6
        )
        # Spread residual + unassigned published mass proportionally.
        residual_pool = pub_pos.copy()
        for code in ind.index:
            if ind.get(code, 0) > 0:
                residual_pool[code] = 0
        if residual_pool.sum() > 0 and residual_m > 0:
            w = residual_pool / residual_pool.sum()
            ind = ind.add(w * residual_m, fill_value=0)

    return ind, f01000, f02n00


def grade_synthetic_vs_published(
    year: int,
    policy: str,
    aff: pd.DataFrame,
    stec: pd.DataFrame,
    transport_share_ref_year: int | None = None,
) -> dict[str, object]:
    pub = industry_column(year)
    pub_bd = breakdown_use_row(year)
    syn_ind, syn_f01000, syn_f02n00 = build_synthetic_use(
        year, policy, aff, stec, transport_share_ref_year
    )

    pub_int = float(pub.sum())
    syn_int = float(syn_ind.sum())
    mcif = s00300_mcif_musd(year)

    # Industry-level error on positive published industries.
    common = pub[pub > 0].index.union(syn_ind[syn_ind > 0].index)
    pub_v = pub.reindex(common).fillna(0)
    syn_v = syn_ind.reindex(common).fillna(0)
    abs_err = (syn_v - pub_v).abs()
    rel_err = abs_err / pub_v.replace(0, np.nan)

    top10_pub = pub[pub > 0].sort_values(ascending=False).head(10).index
    top10_mape = float(rel_err.reindex(top10_pub).mean())

    return {
        "year": year,
        "policy": policy,
        "transport_share_ref_year": transport_share_ref_year,
        "mcif_usd": mcif,
        "published_intermediate_usd": pub_int,
        "synthetic_intermediate_usd": syn_int,
        "intermediate_ratio": syn_int / pub_int if pub_int else float("nan"),
        "published_f01000_usd": pub_bd.f01000_usd,
        "synthetic_f01000_usd": syn_f01000,
        "f01000_ratio": (
            syn_f01000 / pub_bd.f01000_usd if pub_bd.f01000_usd else float("nan")
        ),
        "published_f02n00_usd": pub_bd.f02n00_usd,
        "synthetic_f02n00_usd": syn_f02n00,
        "f02n00_ratio": (
            syn_f02n00 / pub_bd.f02n00_usd if pub_bd.f02n00_usd else float("nan")
        ),
        "top10_industry_mape": top10_mape,
        "total_abs_error_usd": float(abs_err.sum()),
        "n_industries_with_error": int((abs_err > 1e3).sum()),
    }


def expanded_benchmark_breakdown() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for year in BENCHMARK_YEARS:
        bd = breakdown_use_row(year)
        rows.append(
            {
                "year": year,
                "intermediate_usd": bd.intermediate_usd,
                "f01000_usd": bd.f01000_usd,
                "f02n00_usd": bd.f02n00_usd,
                "t019_usd": bd.t019_usd,
                "intermediate_share": bd.intermediate_share,
                "f01000_share": bd.f01000_share,
                "f02n00_share": bd.f02n00_share,
                "n_industries": bd.n_industries,
                "mcif_usd": s00300_mcif_musd(year),
                "has_iea_leaves": year in IEA_HOLDOUT_YEARS,
            }
        )
    return pd.DataFrame(rows)


def affiliation_stability(aff: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for leaf in sorted(set(CIP_LICENSES_LEAVES) | set(S00300_LEAVES)):
        sub = aff[aff["type_of_service"] == leaf]
        if sub.empty:
            continue
        r12 = sub[sub["year"] == 2012]
        r17 = sub[sub["year"] == 2017]
        if r12.empty or r17.empty:
            continue
        s12 = r12.iloc[0]
        s17 = r17.iloc[0]
        rows.append(
            {
                "type_of_service": leaf,
                "all_2012": s12.get("AllAffiliations"),
                "all_2017": s17.get("AllAffiliations"),
                "aff_share_2012": s12.get("affiliated_share"),
                "aff_share_2017": s17.get("affiliated_share"),
                "aff_share_delta_pp": (
                    (
                        float(s17.get("affiliated_share") or 0)
                        - float(s12.get("affiliated_share") or 0)
                    )
                    * 100
                    if pd.notna(s17.get("affiliated_share"))
                    and pd.notna(s12.get("affiliated_share"))
                    else None
                ),
                "in_s00300_leaf_set": bool(s12.get("in_s00300_leaf_set")),
            }
        )
    return pd.DataFrame(rows)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    aff = _load_affiliation_summary()
    stec = _load_stec()

    # --- expanded published benchmarks (2007/2012/2017) ---
    bench = expanded_benchmark_breakdown()
    bench.to_csv(OUT_DIR / "s00300_benchmark_breakdown_2007_2012_2017.csv", index=False)

    # --- license leaf detail ---
    lic_detail = pd.concat(
        [license_leaf_detail(y, aff) for y in IEA_HOLDOUT_YEARS], ignore_index=True
    )
    lic_detail.to_csv(OUT_DIR / "s00300_license_leaf_detail_2012_2017.csv", index=False)

    comp_rows = [
        {
            "year": y,
            "comparable_license_musd": comparable_license_mass(y),
            "s00300_license_musd": license_mass_by_year(y, aff).all_musd,
        }
        for y in IEA_HOLDOUT_YEARS
    ]
    pd.DataFrame(comp_rows).to_csv(
        OUT_DIR / "s00300_comparable_vs_noncomparable_licenses.csv", index=False
    )

    # --- license policy scenarios ---
    lic_pol = grade_license_policies(aff)
    lic_pol.to_csv(OUT_DIR / "s00300_license_policy_holdout.csv", index=False)

    cap_summary, cap_matrix = cap_rate_holdout_matrix()
    cap_summary.to_csv(
        OUT_DIR / "s00300_cap_rate_summary_2007_2012_2017.csv", index=False
    )
    cap_matrix.to_csv(OUT_DIR / "s00300_cap_rate_holdout_matrix.csv", index=False)

    lic_mass_rows = []
    for y in IEA_HOLDOUT_YEARS:
        m = license_mass_by_year(y, aff)
        lic_mass_rows.append(
            {
                "year": y,
                "all_musd": m.all_musd,
                "unaffiliated_musd": m.unaffiliated_musd,
                "affiliated_musd": m.affiliated_musd,
                "f02n00_published_musd": m.f02n00_published_musd,
                "cap_rate_all": m.cap_rate_all,
                "cap_rate_unaff": m.cap_rate_unaff,
            }
        )
    pd.DataFrame(lic_mass_rows).to_csv(
        OUT_DIR / "s00300_license_mass_summary_2012_2017.csv", index=False
    )

    # --- STEC IP share stability + affiliated routing ---
    stec_stab = stec_share_stability(stec)
    stec_stab.to_csv(OUT_DIR / "s00300_stec_ip_share_stability.csv", index=False)

    stec_route = pd.concat(
        [affiliated_license_stec_routing(y, aff, stec) for y in IEA_HOLDOUT_YEARS],
        ignore_index=True,
    )
    stec_route.to_csv(
        OUT_DIR / "s00300_affiliated_license_stec_routing.csv", index=False
    )

    pub_major = pd.concat(
        [
            published_major_proxy_shares(y)
            .rename("published_proxy_share")
            .reset_index()
            .rename(columns={"index": "major_industry"})
            .assign(year=y)
            for y in IEA_HOLDOUT_YEARS
        ],
        ignore_index=True,
    )
    pub_major.to_csv(OUT_DIR / "s00300_published_major_proxy_shares.csv", index=False)

    # --- synthetic policy grading ---
    policies = [
        ("P0_all_licenses_F02N00", "Naive all licenses -> F02N00 only", None),
        (
            "P_cap_rate_F02",
            "Cap-rate split: F02N00 = cap x all licenses; rest via STEC",
            None,
        ),
        (
            "P_cap_rate_prop",
            "Cap-rate split: F02N00 = cap x all licenses; rest proportional",
            None,
        ),
        (
            "P1_unaff_F02_aff_STEC",
            "Unaff->F02 (fit) + aff licenses via STEC IP shares",
            None,
        ),
        ("P2_heads_plus_licenses", "P1 + Fin->523A00 + Air->481000", None),
        ("P3_block_routing", "P2 + travel/govt/transport/residual blocks", None),
        (
            "P_phase1",
            "Production Phase 1: heads + cap-rate + proportional license tail",
            None,
        ),
        ("P3_block_routing", "P3 with 2017 transport shares applied to 2012", 2017),
    ]
    syn_rows = [
        grade_synthetic_vs_published(y, pol, aff, stec, ref)
        for pol, _, ref in policies
        for y in IEA_HOLDOUT_YEARS
    ]
    syn_df = pd.DataFrame(syn_rows)
    syn_df.to_csv(OUT_DIR / "s00300_synthetic_policy_grades.csv", index=False)

    # --- license slice A/B (proportional vs STEC) ---
    lic_ab = pd.DataFrame(
        [grade_license_slice_ab(y, aff, stec) for y in IEA_HOLDOUT_YEARS]
    )
    lic_ab.to_csv(OUT_DIR / "s00300_license_slice_ab.csv", index=False)

    # --- per-industry holdout for Phase 1 ---
    ind_err = pd.concat(
        [per_industry_error_table(y, "P_phase1", aff, stec) for y in IEA_HOLDOUT_YEARS],
        ignore_index=True,
    )
    ind_err.to_csv(OUT_DIR / "s00300_per_industry_holdout_phase1.csv", index=False)

    # --- affiliation stability all leaves ---
    aff_stab = affiliation_stability(aff)
    aff_stab.to_csv(OUT_DIR / "s00300_affiliation_stability_2012_2017.csv", index=False)

    # --- PCE double-count 2012 + 2017 ---
    pce_rows = []
    for y in (2012, 2017):
        row: dict[str, object] = {"year": y}
        try:
            chk = pce_s00300_double_count_check(y)
            tot = chk[chk["activity"] == "_SUM_travel_abroad_NIPA"].iloc[0]
            row.update(
                {
                    "nipa_travel_abroad_musd": tot["nipa_usd"] / 1e6,
                    "fbs_s00300_f01000_musd": tot["fbs_s00300_f01000_usd"] / 1e6,
                    "ratio_nipa_to_fbs": (
                        tot["nipa_usd"] / tot["fbs_s00300_f01000_usd"]
                        if tot["fbs_s00300_f01000_usd"]
                        else float("nan")
                    ),
                    "source": "NIPA_final_dom_uses FBS",
                }
            )
        except Exception as exc:  # noqa: BLE001 — probe documents missing FBS years
            imp = _iea_imports_musd(y)
            travel = _sum_leaves(imp, TRAVEL_LEAVES)
            f010 = breakdown_use_row(y).f01000_usd / 1e6
            row.update(
                {
                    "nipa_travel_abroad_musd": None,
                    "fbs_s00300_f01000_musd": None,
                    "ratio_nipa_to_fbs": None,
                    "iea_travel_leaves_musd": travel,
                    "published_f01000_musd": f010,
                    "iea_travel_over_f01000": travel / f010 if f010 else float("nan"),
                    "source": f"IEA vs published only ({type(exc).__name__})",
                }
            )
        pce_rows.append(row)
    pce_df = pd.DataFrame(pce_rows)
    pce_df.to_csv(OUT_DIR / "s00300_pce_double_count_2012_2017.csv", index=False)

    # --- console summary ---
    print("=== Published benchmark breakdown (2007 / 2012 / 2017) ===")
    print(bench.to_string(index=False))

    print("\n=== License mass summary ===")
    print(pd.DataFrame(lic_mass_rows).to_string(index=False))

    print("\n=== Cap rate holdout (2007 / 2012 / 2017) ===")
    print(cap_summary.to_string(index=False))
    print("\nCross-applied (train cap -> test F02N00):")
    off_diag = cap_matrix[cap_matrix["train_year"] != cap_matrix["test_year"]]
    for _, r in off_diag.iterrows():
        print(
            f"  train {int(r['train_year'])} -> test {int(r['test_year'])}: "
            f"{r['ratio']:.2f}x ({r['verdict']})"
        )

    print("\n=== License policy holdout (ratio ~1.0 = good) ===")
    for _, r in lic_pol.iterrows():
        if r["scenario"] in {
            "unaff_scaled_to_F02N00",
            "affiliated_residual_after_unaff_cap",
        }:
            print(f"  {r['scenario']:35s}  {r['note']}")
            continue
        status = "PASS" if r["holdout_pass"] else "FAIL"
        print(
            f"  [{status:4}] {r['scenario']:35s}  "
            f"2012={r['ratio_2012']:.2f} ({r['verdict_2012']})  "
            f"2017={r['ratio_2017']:.2f} ({r['verdict_2017']})"
        )

    print("\n=== STEC IP share stability (major industries) ===")
    print(stec_stab.to_string(index=False))

    print("\n=== License slice A/B (proportional vs STEC intermediate) ===")
    print(lic_ab.to_string(index=False))

    print("\n=== Phase 1 per-industry holdout (top 10 published, 2017) ===")
    top17 = ind_err[(ind_err["year"] == 2017)].head(10)
    for _, r in top17.iterrows():
        print(
            f"  {r['industry']:8s}  pub=${r['published_usd']/1e6:8,.0f}M  "
            f"syn=${r['synthetic_usd']/1e6:8,.0f}M  "
            f"rel_err={r['rel_error']:.1%}"
            if pd.notna(r["rel_error"])
            else f"  {r['industry']:8s}  pub=${r['published_usd']/1e6:8,.0f}M  "
            f"syn=${r['synthetic_usd']/1e6:8,.0f}M"
        )

    print("\n=== Synthetic policy grades (2012 / 2017) ===")
    for _, r in syn_df.iterrows():
        ref = r["transport_share_ref_year"]
        ref_s = f" ref={int(ref)}" if pd.notna(ref) else ""
        print(
            f"  {r['policy']}{ref_s}  y={int(r['year'])}  "
            f"int_ratio={r['intermediate_ratio']:.2f}  "
            f"f02_ratio={r['f02n00_ratio']:.2f}  "
            f"top10_mape={r['top10_industry_mape']:.1%}"
        )

    print("\n=== PCE double-count (2012 + 2017) ===")
    print(pce_df.to_string(index=False))

    print(f"\nWrote CSVs to {OUT_DIR}")


if __name__ == "__main__":
    main()
