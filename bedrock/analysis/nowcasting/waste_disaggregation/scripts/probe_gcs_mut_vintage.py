"""Phase 3.1 Track A gate: probe GCS NowcastMUT after-redef Make coverage.

Fail-closed on a single candidate: ``v0.3.0_92b7a8a``. Requires a full
2018–2024 after-redef Make panel. No shrink-years, no ``full_alt`` fallback,
no ``f709829`` / ``4276083`` selection.

On success writes ``cache/phase3_gcs_mut_vintage.json`` with ``status=ok``.
On miss: ``SystemExit`` (optionally writes ``status=failed``) — never
``status=ok`` with holes.

Run:
  .venv\\Scripts\\python.exe -m \\
    bedrock.analysis.nowcasting.waste_disaggregation.scripts.probe_gcs_mut_vintage
"""

from __future__ import annotations

import json
from pathlib import Path

from bedrock.analysis.nowcasting.waste_disaggregation.phase3_pins import (
    GCS_MUT_VINTAGE,
)
from bedrock.extract.iot.nowcast_mut_storage import (
    GCS_NOWCAST_MUT_DIR,
    latest_nowcast_mut_vintage,
    nowcast_mut_artifact_name,
)
from bedrock.utils.io.gcp import get_most_recent_from_bucket, list_bucket_files

YEARS = list(range(2018, 2025))
CANDIDATES = (GCS_MUT_VINTAGE,)
CACHE = Path(__file__).resolve().parents[1] / "cache" / "phase3_gcs_mut_vintage.json"


def _parquet_hits(vintage: str, year: int) -> list[str]:
    name = nowcast_mut_artifact_name("Make", year=year, stage="after", vintage=vintage)
    hits = get_most_recent_from_bucket(name, GCS_NOWCAST_MUT_DIR)
    return [h for h in hits if h.endswith(".parquet")]


def _coverage(vintage: str) -> tuple[list[int], list[int]]:
    ok: list[int] = []
    miss: list[int] = []
    for year in YEARS:
        if _parquet_hits(vintage, year):
            ok.append(year)
        else:
            miss.append(year)
    return ok, miss


def _vintages_by_year() -> dict[str, list[str]]:
    df = list_bucket_files(GCS_NOWCAST_MUT_DIR)
    stem_pref = "Nowcast_Detail_Make_after_redef_"
    sub = df[
        df["base_name"].str.startswith(stem_pref) & (df["extension"] == ".parquet")
    ].copy()
    sub["year"] = sub["base_name"].str.extract(r"_(\d{4})$")[0]
    out: dict[str, list[str]] = {}
    for year in YEARS:
        rows = sub[sub["year"] == str(year)]
        vintages = sorted(
            {
                f"{r.version}_{r.hash}"
                for _, r in rows.iterrows()
                if r.version and r.hash
            }
        )
        out[str(year)] = vintages
    return out


def _write_failed(coverage: dict[str, dict[str, list[int]]], note: str) -> None:
    report = {
        "status": "failed",
        "gcs_mut_vintage": GCS_MUT_VINTAGE,
        "years_covered": coverage.get(GCS_MUT_VINTAGE, {}).get("ok", []),
        "holes": coverage.get(GCS_MUT_VINTAGE, {}).get("miss", list(YEARS)),
        "note": note,
        "candidate_coverage": coverage,
        "plan_note": (
            "Phase 3.1 fail-closed: sole candidate v0.3.0_92b7a8a; "
            "do not merge on f709829 evidence; escalate GCS MUT completeness."
        ),
    }
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    CACHE.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
    print(f"WROTE {CACHE} (status=failed)")


def main() -> int:
    latests: dict[str, str | None] = {}
    for year in YEARS:
        try:
            latests[str(year)] = latest_nowcast_mut_vintage(year=year, stage="after")
        except Exception as exc:  # noqa: BLE001
            latests[str(year)] = None
            print(f"latest {year}: FAIL {type(exc).__name__}: {exc}")

    print("=== latest after-redef Make ===")
    for year in YEARS:
        print(f"  {year}: {latests[str(year)]}")

    coverage: dict[str, dict[str, list[int]]] = {}
    for cand in CANDIDATES:
        ok, miss = _coverage(cand)
        coverage[cand] = {"ok": ok, "miss": miss}
        print(f"=== {cand}: ok={ok} miss={miss}")

    assert CANDIDATES == (GCS_MUT_VINTAGE,)
    ok = coverage[GCS_MUT_VINTAGE]["ok"]
    miss = coverage[GCS_MUT_VINTAGE]["miss"]
    if miss:
        note = (
            f"Phase 3.1 Track A gate FAILED: {GCS_MUT_VINTAGE} missing after-redef "
            f"Make for years {miss}. Abort; do not shrink years or fall back to "
            f"f709829. Escalate GCS MUT completeness before retry."
        )
        _write_failed(coverage, note)
        raise SystemExit(note)

    report = {
        "status": "ok",
        "gcs_mut_vintage": GCS_MUT_VINTAGE,
        "years_covered": ok,
        "holes": [],
        "note": f"full 2018–2024 panel under {GCS_MUT_VINTAGE}",
        "latests_by_year": latests,
        "candidate_coverage": coverage,
        "all_vintages_by_year": _vintages_by_year(),
        "plan_note": (
            "Phase 3.1 fail-closed probe: sole candidate v0.3.0_92b7a8a; "
            "commit SHAs are NOT proof of MUT upload."
        ),
    }
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    CACHE.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
    print(f"WROTE {CACHE}")
    print(
        json.dumps(
            {
                k: report[k]
                for k in ("gcs_mut_vintage", "years_covered", "holes", "note")
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
