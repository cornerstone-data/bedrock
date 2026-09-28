"""Phase 3 Track A gate: probe GCS NowcastMUT after-redef Make coverage.

Candidate start: v0.3.0_f709829 (commit SHA is NOT proof of MUT upload).
Fallback documented in plan: v0.3.0_4276083 (electricity / v0.4 pins).

Writes ``cache/phase3_gcs_mut_vintage.json`` with the verified pin (or a
shrunk year panel + holes).

Run:
  .venv\\Scripts\\python.exe -m \\
    bedrock.analysis.nowcasting.waste_disaggregation.scripts.probe_gcs_mut_vintage
"""

from __future__ import annotations

import json
from pathlib import Path

from bedrock.extract.iot.nowcast_mut_storage import (
    GCS_NOWCAST_MUT_DIR,
    latest_nowcast_mut_vintage,
    nowcast_mut_artifact_name,
)
from bedrock.utils.io.gcp import get_most_recent_from_bucket, list_bucket_files

YEARS = list(range(2018, 2025))
CANDIDATES = ("v0.3.0_f709829", "v0.3.0_4276083")
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

    chosen: str | None = None
    years_covered = list(YEARS)
    holes: list[int] = []
    note = ""
    for cand in CANDIDATES:
        ok = coverage[cand]["ok"]
        miss = coverage[cand]["miss"]
        if not miss:
            chosen = cand
            years_covered = ok
            note = f"full 2018–2024 panel under {cand}"
            break
        if len(ok) > len(years_covered) - len(holes) or chosen is None:
            # Prefer first candidate with any coverage; may shrink later
            if ok and (chosen is None or cand == CANDIDATES[0]):
                chosen = cand
                years_covered = ok
                holes = miss
                note = (
                    f"shrunk Track A panel to {ok} under {cand}; "
                    f"missing years {miss}"
                )

    # If candidate0 has partial and candidate1 has full, prefer full (already
    # handled by loop). If neither full, keep best partial under first cand
    # that has any hits, else abort.
    full_alt = next(
        (c for c in CANDIDATES if not coverage[c]["miss"]),
        None,
    )
    if full_alt is not None:
        chosen = full_alt
        years_covered = coverage[full_alt]["ok"]
        holes = []
        note = f"full 2018–2024 panel under {full_alt}"

    if chosen is None:
        raise SystemExit(
            "Track A gate FAILED: no candidate GCS vintage has after-redef Make "
            f"for any of {YEARS}. Abort Track A."
        )

    report = {
        "status": "ok",
        "gcs_mut_vintage": chosen,
        "years_covered": years_covered,
        "holes": holes,
        "note": note,
        "latests_by_year": latests,
        "candidate_coverage": coverage,
        "all_vintages_by_year": _vintages_by_year(),
        "plan_note": (
            "Commit f709829a is NOT proof of MUT upload; pin is from GCS probe only."
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
