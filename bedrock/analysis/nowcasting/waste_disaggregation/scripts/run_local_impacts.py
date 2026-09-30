"""Run local-MUT impacts for all years on recorded local MUT vintage.

Writes nothing over GCS caches — uses ``recorded_local_vintage`` from
``cache/local_mut_rebuild.json`` as ``--mut-vintage-label``.

Prerequisite: local MUT rebuild finished (``status: ok``) and local control/treatment
YAMLs via ``write_local_yamls``.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

from bedrock.analysis.nowcasting.waste_disaggregation.impact_pins import IMPACT_YEARS
from bedrock.analysis.nowcasting.waste_disaggregation.scripts.run_waste_weight_impact_efs import (  # noqa: E501
    MODULE,
    impact_cache_dir,
)

ROOT = Path(__file__).resolve().parents[5]
REBUILD = (
    Path(__file__).resolve().parents[1] / "cache" / "local_mut_rebuild.json"
)
INDEX = Path(__file__).resolve().parents[1] / "cache" / "local_impact_index.json"

CONTROL_FMT = "2025_usa_cornerstone_v0_4_nowcast_{year}_waste_weights_control_local"
TREATMENT_FMT = "2025_usa_cornerstone_v0_4_nowcast_{year}_waste_weights_match_io_local"


def _local_vintage() -> tuple[str, list[int]]:
    if not REBUILD.is_file():
        raise SystemExit(f"Missing {REBUILD}; finish local MUT rebuild first")
    data = json.loads(REBUILD.read_text(encoding="utf-8"))
    if data.get("status") != "ok":
        raise SystemExit(
            f"local MUT rebuild status={data.get('status')!r}; wait for status=ok before local-MUT impacts"
        )
    failed = [int(y) for y in (data.get("failed_years") or [])]
    vintage = data.get("recorded_local_vintage") or data.get("intended_local_vintage")
    if not vintage:
        raise SystemExit("No recorded_local_vintage in rebuild JSON")
    return str(vintage), failed


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--years",
        default=",".join(str(y) for y in IMPACT_YEARS),
        help="Comma-separated years (default: 2018-2024)",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip years whose summary.json already exists under local cache dir",
    )
    parser.add_argument(
        "--write-yamls",
        action="store_true",
        help="Run write_local_yamls before impacts",
    )
    args = parser.parse_args(argv[1:])
    vintage, failed_years = _local_vintage()
    years = [int(x) for x in args.years.split(",") if x.strip()]
    years = [y for y in years if y not in failed_years]
    if failed_years:
        print(f"Skipping local MUT rebuild failed_years={failed_years}", flush=True)

    if args.write_yamls:
        subprocess.run(
            [
                sys.executable,
                "-m",
                "bedrock.analysis.nowcasting.waste_disaggregation.scripts.write_local_yamls",
                "--vintage",
                vintage,
            ],
            check=True,
            cwd=str(ROOT),
        )

    index: dict[str, object] = {
        "local_mut_vintage": vintage,
        "years": years,
        "skipped_failed_years": failed_years,
        "results": {},
    }
    py = sys.executable
    for year in years:
        out = impact_cache_dir(year, vintage)
        if args.skip_existing and (out / "summary.json").exists():
            print(f"SKIP {year} (existing {out})", flush=True)
            index["results"][str(year)] = {"status": "skipped", "out_dir": str(out)}  # type: ignore[index]
            continue
        print(f"=== local-MUT impacts year={year} vintage={vintage} ===", flush=True)
        subprocess.run(
            [
                py,
                "-m",
                MODULE,
                "--year",
                str(year),
                "--control",
                CONTROL_FMT.format(year=year),
                "--treatment",
                TREATMENT_FMT.format(year=year),
                "--mut-vintage-label",
                vintage,
            ],
            check=True,
            cwd=str(ROOT),
        )
        index["results"][str(year)] = {"status": "ok", "out_dir": str(out)}  # type: ignore[index]
        INDEX.parent.mkdir(parents=True, exist_ok=True)
        INDEX.write_text(json.dumps(index, indent=2) + "\n", encoding="utf-8")

    INDEX.parent.mkdir(parents=True, exist_ok=True)
    INDEX.write_text(json.dumps(index, indent=2) + "\n", encoding="utf-8")
    print(f"WROTE {INDEX}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
