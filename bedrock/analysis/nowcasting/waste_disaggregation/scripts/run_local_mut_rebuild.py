"""Kick off local MUT rebuild (2018–2024 only).

Runs Step 5 → 6 → 7 with explicit years (no bare defaults, no ``--gcs``).
Records the exact ``default_nowcast_mut_vintage()`` string for local-MUT impacts.

Prerequisite: local ``EIA_MECS_Energy_2018`` FBA must include Tables 7.2 and
7.10 (#895). If Step 5 fails with a stale MECS cache, regenerate::

  generateFlowByActivity(source='EIA_MECS_Energy', year=2018)

Soft-balance failure for a year: that year is skipped in local-MUT impacts (hole in
report) — see ``cache/local_mut_rebuild.json``.
"""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from bedrock.extract.iot.nowcast_mut_storage import default_nowcast_mut_vintage

YEARS = list(range(2018, 2025))
CACHE = Path(__file__).resolve().parents[1] / "cache" / "local_mut_rebuild.json"
ROOT = Path(__file__).resolve().parents[5]


def _run(cmd: list[str]) -> None:
    print("===", " ".join(cmd), flush=True)
    subprocess.run(cmd, check=True, cwd=str(ROOT))


def _ensure_mecs_tables() -> None:
    """Regenerate EIA_MECS_Energy 2018/2022 so Tables 7.2/7.10 are present (#895)."""
    from bedrock.extract.generateflowbyactivity import (  # noqa: PLC0415
        generateFlowByActivity,
    )

    for year in (2018, 2022):
        print(
            f"Regenerating EIA_MECS_Energy {year} (local MUT rebuild prerequisite)...",
            flush=True,
        )
        generateFlowByActivity(source="EIA_MECS_Energy", year=year)


def main() -> int:
    vintage = default_nowcast_mut_vintage()
    started = datetime.now(timezone.utc).isoformat()
    report: dict[str, object] = {
        "status": "running",
        "started_utc": started,
        "intended_local_vintage": vintage,
        "years": YEARS,
        "steps": [],
        "failed_years": [],
        "note": "Do not pass --gcs. local-MUT impacts pins both arms to intended_local_vintage.",
    }
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    CACHE.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    _ensure_mecs_tables()
    report["steps"].append({"step": "mecs_fba_refresh", "status": "ok"})  # type: ignore[attr-defined]
    CACHE.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    py = sys.executable
    year_range = f"{YEARS[0]}-{YEARS[-1]}"
    try:
        _run(
            [
                py,
                "-m",
                "bedrock.transform.iot.nowcast_sut_assembly",
                "--years",
                year_range,
            ]
        )
        report["steps"].append({"step": "sut_assembly", "status": "ok"})  # type: ignore[attr-defined]
    except subprocess.CalledProcessError as exc:
        report["status"] = "failed"
        report["error"] = f"sut_assembly exit {exc.returncode}"
        CACHE.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        raise

    mut_cmd = [py, "-m", "bedrock.transform.iot.nowcast_mut"]
    for y in YEARS:
        mut_cmd.extend(["--year", str(y)])
    try:
        _run(mut_cmd)
        report["steps"].append({"step": "nowcast_mut", "status": "ok"})  # type: ignore[attr-defined]
    except subprocess.CalledProcessError as exc:
        report["status"] = "failed"
        report["error"] = f"nowcast_mut exit {exc.returncode}"
        CACHE.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        raise

    redef_cmd = [py, "-m", "bedrock.transform.iot.nowcast_redefinitions"]
    for y in YEARS:
        redef_cmd.extend(["--year", str(y)])
    try:
        _run(redef_cmd)
        report["steps"].append({"step": "nowcast_redefinitions", "status": "ok"})  # type: ignore[attr-defined]
    except subprocess.CalledProcessError as exc:
        report["status"] = "failed"
        report["error"] = f"nowcast_redefinitions exit {exc.returncode}"
        CACHE.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        raise

    # Re-read vintage after build (should match intended)
    final_vintage = default_nowcast_mut_vintage()
    report.update(
        {
            "status": "ok",
            "finished_utc": datetime.now(timezone.utc).isoformat(),
            "recorded_local_vintage": final_vintage,
        }
    )
    CACHE.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"WROTE {CACHE}")
    print(f"LOCAL_MUT_VINTAGE={final_vintage}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
