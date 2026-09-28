"""Run Phase 3 Track A impact for all years with verified GCS pin.

Does not overwrite Track C dirs (uses GCS_MUT_VINTAGE as cache label).
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

from bedrock.analysis.nowcasting.waste_disaggregation.phase3_pins import (
    GCS_MUT_VINTAGE,
    PHASE3_YEARS,
    control_config_name,
    treatment_config_name,
)
from bedrock.analysis.nowcasting.waste_disaggregation.scripts.run_waste_weight_impact_efs import (  # noqa: E501
    MODULE,
    impact_cache_dir,
)

ROOT = Path(__file__).resolve().parents[5]
CACHE = Path(__file__).resolve().parents[1] / "cache" / "phase3_gcs_impact_index.json"


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--years",
        default=",".join(str(y) for y in PHASE3_YEARS),
        help="Comma-separated years (default: 2018-2024)",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip years whose summary.json already exists under GCS cache dir",
    )
    args = parser.parse_args(argv[1:])
    years = [int(x) for x in args.years.split(",") if x.strip()]

    index: dict[str, object] = {
        "gcs_mut_vintage": GCS_MUT_VINTAGE,
        "years": years,
        "results": {},
    }
    py = sys.executable
    for year in years:
        out = impact_cache_dir(year, GCS_MUT_VINTAGE)
        if args.skip_existing and (out / "summary.json").exists():
            print(f"SKIP {year} (existing {out})", flush=True)
            index["results"][str(year)] = {"status": "skipped", "out_dir": str(out)}  # type: ignore[index]
            continue
        print(f"=== Track A year={year} ===", flush=True)
        subprocess.run(
            [
                py,
                "-m",
                MODULE,
                "--year",
                str(year),
                "--control",
                control_config_name(year),
                "--treatment",
                treatment_config_name(year),
                "--mut-vintage-label",
                GCS_MUT_VINTAGE,
            ],
            check=True,
            cwd=str(ROOT),
        )
        index["results"][str(year)] = {"status": "ok", "out_dir": str(out)}  # type: ignore[index]
        CACHE.parent.mkdir(parents=True, exist_ok=True)
        CACHE.write_text(json.dumps(index, indent=2) + "\n", encoding="utf-8")

    CACHE.parent.mkdir(parents=True, exist_ok=True)
    CACHE.write_text(json.dumps(index, indent=2) + "\n", encoding="utf-8")
    print(f"WROTE {CACHE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
