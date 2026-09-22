"""Validate AIES waste-child EXPS_TOT_DVAL shape (EXP01 2023 / BASIC 2024).

Prefers Census FBA ``Census_AIES_Waste_Child_Expenses``; falls back to Phase 1
FTP probe cache under analysis/nowcasting/waste_disaggregation/cache/aies_probe/.

Run:
  .venv\\Scripts\\python.exe -m bedrock.analysis.nowcasting.waste_disaggregation.scripts.test_aies_waste_child_expenses
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

from bedrock.extract.disaggregation.sas_waste_metrics import (
    map_sas_naics_to_cornerstone,
)
from bedrock.extract.disaggregation.waste_static_rules import (
    SAS_NAICS_TO_CORNERSTONE,
    WASTE_CHILDREN,
)
from bedrock.extract.flowbyactivity import getFlowByActivity

PROBE = Path(__file__).resolve().parents[1] / "cache" / "aies_probe"
EXPECTED_6DIGIT = sorted(
    {k for k in SAS_NAICS_TO_CORNERSTONE if len(k) == 6 and k.startswith("562")}
)


def _load_probe(year: int) -> pd.DataFrame:
    if year == 2023:
        path = PROBE / "exp01_2023" / "AIES00EXP01.dat"
    elif year == 2024:
        path = PROBE / "basic_2024" / "AIES00BASIC.dat"
    else:
        raise ValueError(year)
    if not path.exists():
        raise FileNotFoundError(path)
    df = pd.read_csv(path, sep="|", dtype=str, comment="#")
    # column names vary; find NAICS + EXPS_TOT_DVAL
    cols = {c.upper(): c for c in df.columns}
    naics_col = cols.get("NAICS2017") or cols.get("NAICS")
    val_col = cols.get("EXPS_TOT_DVAL")
    if naics_col is None or val_col is None:
        raise KeyError(
            f"Missing NAICS/EXPS_TOT_DVAL in {path}; cols={list(df.columns)}"
        )
    out = df[[naics_col, val_col]].rename(
        columns={naics_col: "NAICS", val_col: "EXPS_TOT_DVAL"}
    )
    out["EXPS_TOT_DVAL"] = pd.to_numeric(out["EXPS_TOT_DVAL"], errors="coerce")
    out = out[out["NAICS"].astype(str).str.fullmatch(r"562\d{3}")]
    out = out[out["EXPS_TOT_DVAL"].notna()]
    return out


def _assert_year(year: int, df: pd.DataFrame) -> None:
    codes = set(df["NAICS"].astype(str))
    missing = [c for c in EXPECTED_6DIGIT if c not in codes]
    # Aggregate-only pull would fail this hard gate
    if not any(c.startswith("562") and len(c) == 6 for c in codes):
        raise AssertionError(f"{year}: no 6-digit 562* codes (EXP02-shaped?)")
    if missing:
        print(f"WARN {year}: missing expected codes {missing} (may be suppression)")
    totals: dict[str, float] = dict.fromkeys(WASTE_CHILDREN, 0.0)
    for _, row in df.iterrows():
        child = map_sas_naics_to_cornerstone(row["NAICS"])
        if child:
            totals[child] += float(row["EXPS_TOT_DVAL"])
    s = pd.Series(totals)
    assert float(s.sum()) > 0, f"{year}: all-zero after Cornerstone fold"
    print(f"OK {year}: {len(codes)} six-digit codes; child shares:\n{s / s.sum()}")


def main() -> int:
    # Prefer FBA when API/cache available; else probe files
    for year in (2023, 2024):
        try:
            fba = getFlowByActivity(
                datasource="Census_AIES_Waste_Child_Expenses", year=year
            )
            df = fba.rename(
                columns={"ActivityConsumedBy": "NAICS", "FlowAmount": "EXPS_TOT_DVAL"}
            )
            print(f"{year}: loaded via FBA")
        except Exception as exc:  # noqa: BLE001
            print(f"{year}: FBA unavailable ({type(exc).__name__}); using probe cache")
            df = _load_probe(year)
        _assert_year(year, df)
    return 0


if __name__ == "__main__":
    sys.exit(main())
