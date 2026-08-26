"""
Review Census_USATrade → BEA Detail goods Crosswalk for #658.

Checks:
  1. Sector targets that are not valid 2017 SUT commodity codes (industry-only).
  2. 1:m mappings (same Activity → multiple Sectors).
  3. BEA Detail goods-family commodity codes with nonzero 2017 F040/MCIF that have
     no Census NAICS activity mapping into them (coverage holes).
  4. Known issues flagged in the Trade README.
  5. Census FBA activities: unmapped, non-2017 NAICS, or legacy vintage codes.

Run::

    uv run python -m bedrock.analysis.nowcasting.trade_data.review_census_crosswalk
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from bedrock.extract.flowbyactivity import getFlowByActivity
from bedrock.extract.iot.io_2017 import _load_2017_detail_supply_use_usa
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES

_CROSSWALK = (
    Path(__file__).resolve().parents[3]
    / "utils"
    / "mapping"
    / "activitytosectormapping"
    / "Sector_Crosswalk_Census_USATrade.csv"
)
_NAICS_2017 = (
    Path(__file__).resolve().parents[3]
    / "utils"
    / "mapping"
    / "naics"
    / "NAICS_2017_Crosswalk.csv"
)
_NAICS_CONCORDANCE = (
    Path(__file__).resolve().parents[3]
    / "utils"
    / "mapping"
    / "naics"
    / "NAICS_Year_Concordance.csv"
)

_CENSUS_SPECIAL = frozenset({"910000", "930000", "990000", "980000"})
_RESIDUAL_RE = re.compile(r"^\d{4}XX$|^\d{5}X$")
_DIGIT6_RE = re.compile(r"^\d{6}$")

_SERVICE_PREFIXES = (
    "4",
    "5",
    "6",
    "7",
    "8",
    "S00",
    "GFE",
    "GFGD",
    "GFGN",
    "GSLE",
    "GSLG",
    "HS",
    "ORE",
)


def _is_service_or_special(code: str) -> bool:
    return any(code.startswith(p) for p in _SERVICE_PREFIXES)


def _naics_2017_leaves() -> set[str]:
    df = pd.read_csv(_NAICS_2017, dtype=str)
    return {
        c.strip() for c in df["NAICS_6"].dropna() if c.strip() and c.strip() != "nan"
    }


def _legacy_naics_2017_targets() -> dict[str, str]:
    """Pre-2017 NAICS codes that concordance maps to a different 2017 leaf."""
    conc = pd.read_csv(_NAICS_CONCORDANCE, dtype=str)
    out: dict[str, str] = {}
    for _, row in conc.iterrows():
        for vintage_col in ("NAICS_2012_Code", "NAICS_2007_Code", "NAICS_2002_Code"):
            src = str(row.get(vintage_col, "")).strip()
            tgt = str(row.get("NAICS_2017_Code", "")).strip()
            if src and tgt and src != tgt and src not in out:
                out[src] = tgt
    return out


def _classify_census_activity(
    activity: str,
    *,
    naics_2017: set[str],
    legacy: dict[str, str],
) -> str:
    if activity in _CENSUS_SPECIAL:
        return "census_special"
    if _RESIDUAL_RE.match(activity):
        return "census_residual"
    if not _DIGIT6_RE.match(activity):
        return "invalid_format"
    if activity in naics_2017:
        return "naics_2017"
    if activity in legacy:
        return "legacy_naics"
    return "not_in_official_hierarchy"


def review_census_naics_vintage(year: int = 2017) -> pd.DataFrame:
    """Audit Census_USATrade activities vs Crosswalk and 2017 NAICS vintage."""
    fba = getFlowByActivity("Census_USATrade", year)
    flow_m = (fba.groupby("ActivityProducedBy")["FlowAmount"].sum() / 1e6).rename(
        "flow_M"
    )
    xw = pd.read_csv(_CROSSWALK, dtype=str)
    mapped = set(xw["Activity"].astype(str).str.strip())
    naics_2017 = _naics_2017_leaves()
    legacy = _legacy_naics_2017_targets()

    rows: list[dict[str, object]] = []
    for activity in sorted(flow_m.index.astype(str)):
        act = activity.strip()
        vintage = _classify_census_activity(
            act, naics_2017=naics_2017, legacy=legacy, mapped=mapped
        )
        rows.append(
            {
                "activity": act,
                "flow_M": float(flow_m.loc[activity]),
                "in_crosswalk": act in mapped,
                "vintage": vintage,
                "naics_2017_target": legacy.get(
                    act, act if vintage == "naics_2017" else ""
                ),
            }
        )
    return pd.DataFrame(rows)


def _print_naics_vintage_audit(df: pd.DataFrame, year: int) -> None:
    print(f"\n## 5. Census USATrade {year} NAICS vs Crosswalk / 2017 vintage")
    summary = (
        df.groupby(["vintage", "in_crosswalk"], dropna=False)
        .agg(n=("activity", "count"), flow_M=("flow_M", "sum"))
        .reset_index()
    )
    with pd.option_context("display.max_rows", 20, "display.width", 120):
        print(summary.to_string(index=False, float_format=lambda x: f"{x:,.1f}"))

    unmapped_2017 = df.loc[
        (~df["in_crosswalk"]) & (df["vintage"] == "naics_2017")
    ].sort_values("flow_M", ascending=False)
    print(
        f"\n   Unmapped valid 2017 NAICS ({len(unmapped_2017)}): "
        f"{'(none)' if unmapped_2017.empty else ''}"
    )
    if not unmapped_2017.empty:
        print(unmapped_2017.to_string(index=False, float_format=lambda x: f"{x:,.1f}"))

    unmapped_other = df.loc[
        (~df["in_crosswalk"]) & (df["vintage"] != "naics_2017")
    ].sort_values("flow_M", ascending=False)
    print(
        f"\n   Unmapped non-2017 / special ({len(unmapped_other)}): "
        f"{'(none)' if unmapped_other.empty else ''}"
    )
    if not unmapped_other.empty:
        print(unmapped_other.to_string(index=False, float_format=lambda x: f"{x:,.1f}"))

    legacy_mapped = df.loc[
        df["in_crosswalk"] & (df["vintage"] == "legacy_naics")
    ].sort_values("flow_M", ascending=False)
    print(
        f"\n   Mapped legacy (pre-2017) NAICS ({len(legacy_mapped)}): "
        "Census still reports old code; Crosswalk bridges to BEA Detail"
    )
    if not legacy_mapped.empty:
        show = legacy_mapped[["activity", "flow_M", "naics_2017_target"]].copy()
        print(show.to_string(index=False, float_format=lambda x: f"{x:,.1f}"))

    orphan = df.loc[
        df["in_crosswalk"] & (df["vintage"] == "not_in_official_hierarchy")
    ].sort_values("flow_M", ascending=False)
    print(
        f"\n   Mapped but not in 2017 hierarchy or concordance ({len(orphan)}): "
        "review manually"
    )
    if not orphan.empty:
        print(orphan.to_string(index=False, float_format=lambda x: f"{x:,.1f}"))


def main() -> None:
    xw = pd.read_csv(_CROSSWALK)
    valid_comms = set(USA_2017_COMMODITY_CODES)
    comm_idx = pd.Index(USA_2017_COMMODITY_CODES, name="commodity")

    print("=" * 72)
    print("Census USATrade Crosswalk review (#658)")
    print("=" * 72)

    # 1. Invalid sector targets
    invalid = xw.loc[~xw["Sector"].isin(valid_comms)]
    print(
        f"\n## 1. Sector targets NOT in USA_2017_COMMODITY_CODES ({len(invalid)} rows)"
    )
    for _, row in invalid.iterrows():
        print(f"   Activity {row['Activity']} -> Sector {row['Sector']}")

    # 2. 1:m mappings
    counts = xw.groupby("Activity")["Sector"].nunique()
    multi = counts[counts > 1]
    print(f"\n## 2. Activities with 1:m mapping ({len(multi)} activities)")
    for act, n in multi.items():
        targets = xw.loc[xw["Activity"] == act, "Sector"].tolist()
        print(f"   {act} -> {targets}")

    # 3. Coverage holes
    use_df = _load_2017_detail_supply_use_usa("Use_SUT_detail")
    supply_df = _load_2017_detail_supply_use_usa("Supply_detail")

    f040 = pd.to_numeric(use_df["F04000"], errors="coerce").reindex(comm_idx).fillna(0)
    mcif = pd.to_numeric(supply_df["MCIF"], errors="coerce").reindex(comm_idx).fillna(0)

    reached = set(xw["Sector"].unique())

    goods_holes = []
    for code in comm_idx:
        if code in reached or _is_service_or_special(code):
            continue
        f_val = float(f040.loc[code])
        m_val = float(mcif.loc[code])
        if abs(f_val) > 0 or abs(m_val) > 0:
            goods_holes.append({"commodity": code, "F040_M": f_val, "MCIF_M": m_val})

    holes_df = (
        pd.DataFrame(goods_holes).sort_values("MCIF_M", ascending=False, key=abs)
        if goods_holes
        else pd.DataFrame(columns=["commodity", "F040_M", "MCIF_M"])
    )

    print(
        f"\n## 3. Goods-family SUT holes not reached by Census Crosswalk ({len(holes_df)} codes)"
    )
    if not holes_df.empty:
        with pd.option_context("display.max_rows", 100, "display.width", 120):
            print(holes_df.to_string(index=False, float_format=lambda x: f"{x:,.1f}"))

    # 4. Summary of documented issues
    print("\n## 4. Known issues")
    issues = [
        ("331314", "FIXED: was industry-only target; now maps to 33131B."),
        (
            "311824->3118A0",
            "CORRECT per BEA NAICS crosswalk. 311824 is dry pasta/dough/flour mixes (3118A0), not bakery (311810).",
        ),
        (
            "11211X",
            "Only 1:m row (-> 1121A0, 112120). No finer 1121* activities on Census trade.",
        ),
        ("980000", "Low-value shipments. Omitted; no BEA Detail sector. Not fixable."),
        ("1121A0", "Import hole (1,659 M). Only reachable via 11211X 1:m split."),
    ]
    for code, note in issues:
        print(f"   {code}: {note}")

    _print_naics_vintage_audit(review_census_naics_vintage(2017), 2017)


if __name__ == "__main__":
    main()
