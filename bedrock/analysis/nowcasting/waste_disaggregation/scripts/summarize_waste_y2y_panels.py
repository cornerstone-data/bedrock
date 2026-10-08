"""Thin Y2Y panels from year-aligned impact caches.

Reads ``control_N.parquet`` / ``treatment_N.parquet`` (and D) under
``cache/impact_{Y}_v0.3.0_92b7a8a/`` for 2018–2024. Writes:

- ``cache/y2y_waste_N.csv``
- ``cache/y2y_waste_D.csv``

Locked columns: ``year, sector, arm, N, N_yoy_vs_prior, N_yoy_vs_2018``
(and D equivalents). Smoke-asserts seven ``summary.json`` files + pin.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

from bedrock.analysis.nowcasting.waste_disaggregation.impact_pins import (
    GCS_MUT_VINTAGE,
    IMPACT_YEARS,
)
from bedrock.analysis.nowcasting.waste_disaggregation.scripts.run_waste_weight_impact_efs import (  # noqa: E501
    impact_cache_dir,
)
from bedrock.extract.disaggregation.waste_static_rules import WASTE_CHILDREN

PKG = Path(__file__).resolve().parents[1]
OUT_N = PKG / "cache" / "y2y_waste_N.csv"
OUT_D = PKG / "cache" / "y2y_waste_D.csv"

REQUIRED_N_COLS = (
    "year",
    "sector",
    "arm",
    "N",
    "N_yoy_vs_prior",
    "N_yoy_vs_2018",
)
REQUIRED_D_COLS = (
    "year",
    "sector",
    "arm",
    "D",
    "D_yoy_vs_prior",
    "D_yoy_vs_2018",
)


def _smoke_assert_caches() -> None:
    missing: list[str] = []
    for year in IMPACT_YEARS:
        d = impact_cache_dir(year, GCS_MUT_VINTAGE)
        summary = d / "summary.json"
        if not summary.is_file():
            missing.append(str(summary))
            continue
        data = json.loads(summary.read_text(encoding="utf-8"))
        pin = data.get("resolved_nowcast_mut_vintage")
        if pin != GCS_MUT_VINTAGE:
            raise SystemExit(
                f"Pin mismatch in {summary}: got {pin!r}, want {GCS_MUT_VINTAGE!r}"
            )
        for arm in ("control", "treatment"):
            for kind in ("N", "D"):
                pq = d / f"{arm}_{kind}.parquet"
                if not pq.is_file():
                    missing.append(str(pq))
    if missing:
        raise SystemExit(
            "Missing impact-cache artifacts (run run_gcs_impacts.py):\n"
            + "\n".join(missing)
        )


def _waste_series(path: Path) -> pd.Series:
    df = pd.read_parquet(path)
    s = df.iloc[:, 0].astype(float)
    idx = [c for c in WASTE_CHILDREN if c in s.index]
    return s.reindex(idx)


def _panel_for_kind(kind: str) -> pd.DataFrame:
    """Build long panel for N or D across years × arms × waste sectors."""
    # values[year][arm][sector] = float
    values: dict[int, dict[str, pd.Series]] = {}
    for year in IMPACT_YEARS:
        d = impact_cache_dir(year, GCS_MUT_VINTAGE)
        values[year] = {
            "control": _waste_series(d / f"control_{kind}.parquet"),
            "treatment": _waste_series(d / f"treatment_{kind}.parquet"),
        }

    base_year = 2018
    rows: list[dict[str, object]] = []
    val_col = kind
    prior_col = f"{kind}_yoy_vs_prior"
    vs2018_col = f"{kind}_yoy_vs_2018"

    for year in IMPACT_YEARS:
        prior_year = year - 1 if year > base_year else None
        for arm in ("control", "treatment"):
            cur = values[year][arm]
            base = values[base_year][arm]
            prior = values[prior_year][arm] if prior_year is not None else None
            for sector in cur.index:
                v = float(cur.loc[sector])
                v0 = float(base.loc[sector]) if sector in base.index else float("nan")
                if prior is not None and sector in prior.index:
                    vp = float(prior.loc[sector])
                    yoy_prior = (v - vp) / vp if vp != 0.0 else float("nan")
                else:
                    yoy_prior = float("nan")
                yoy_2018 = (v - v0) / v0 if v0 != 0.0 else float("nan")
                rows.append(
                    {
                        "year": year,
                        "sector": sector,
                        "arm": arm,
                        val_col: v,
                        prior_col: yoy_prior,
                        vs2018_col: yoy_2018,
                    }
                )
    return pd.DataFrame(rows)


def _assert_schema(df: pd.DataFrame, cols: tuple[str, ...], label: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise SystemExit(f"{label} missing columns: {missing}")
    years = sorted(df["year"].unique())
    if list(years) != list(IMPACT_YEARS):
        raise SystemExit(f"{label} unexpected years: {years}")
    arms = set(df["arm"].unique())
    if arms != {"control", "treatment"}:
        raise SystemExit(f"{label} unexpected arms: {arms}")
    n_expected = len(IMPACT_YEARS) * len(WASTE_CHILDREN) * 2
    if len(df) != n_expected:
        raise SystemExit(
            f"{label} row count {len(df)} != expected {n_expected} "
            f"(7 years × {len(WASTE_CHILDREN)} sectors × 2 arms)"
        )


def main() -> int:
    _smoke_assert_caches()
    df_n = _panel_for_kind("N")
    df_d = _panel_for_kind("D")
    _assert_schema(df_n, REQUIRED_N_COLS, "y2y_waste_N")
    _assert_schema(df_d, REQUIRED_D_COLS, "y2y_waste_D")
    OUT_N.parent.mkdir(parents=True, exist_ok=True)
    df_n.to_csv(OUT_N, index=False)
    df_d.to_csv(OUT_D, index=False)
    print(f"WROTE {OUT_N} ({len(df_n)} rows)")
    print(f"WROTE {OUT_D} ({len(df_d)} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
