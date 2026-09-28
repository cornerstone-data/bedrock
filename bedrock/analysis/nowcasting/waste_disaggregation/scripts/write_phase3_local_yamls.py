"""After Track B: write Track C YAMLs pinned to recorded local MUT vintage.

Reads ``cache/phase3_local_mut_rebuild.json`` for ``recorded_local_vintage``
(or ``intended_local_vintage`` if rebuild still running but stamp known).
Does **not** overwrite Track A GCS-pinned YAMLs.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from bedrock.analysis.nowcasting.waste_disaggregation.phase3_pins import (
    ANALYSIS_CONFIG_DIR,
    PHASE3_YEARS,
)

CONFIG_DIR = ANALYSIS_CONFIG_DIR
REBUILD = (
    Path(__file__).resolve().parents[1] / "cache" / "phase3_local_mut_rebuild.json"
)


def _yaml(year: int, *, vintage: str, treatment: bool) -> str:
    ww = "match_io" if treatment else "2017"
    role = "TREATMENT" if treatment else "CONTROL"
    return f"""# Phase 3 Track C analysis-only {role} — local MUT vintage.
#
# Pins nowcast_mut_vintage to the Track B recorded local cut. Do not use these
# arms against GCS-pinned Track A configs. Electricity off (Decision 6).

#####
# Model base settings
#####
model_base_year: {year}
iot_before_or_after_redefinition: after

#####
# Data selection
#####
usa_detail_io_source: nowcast
usa_base_io_data_year: {year}
usa_ghg_data_year: {year}
nowcast_mut_vintage: '{vintage}'

#####
# Methodology selection
#####
use_cornerstone_ghg_model: True
apply_io_year_adjustments: False
implement_waste_disaggregation: True
waste_weights_year: {ww}
cornerstone_industry_avg_margins: True
"""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--vintage",
        default=None,
        help="Override local vintage (default: from phase3_local_mut_rebuild.json)",
    )
    parser.add_argument(
        "--years",
        default=",".join(str(y) for y in PHASE3_YEARS),
    )
    args = parser.parse_args(argv)
    vintage = args.vintage
    if vintage is None:
        if not REBUILD.is_file():
            raise SystemExit(f"Missing {REBUILD}; pass --vintage or finish Track B")
        data = json.loads(REBUILD.read_text(encoding="utf-8"))
        vintage = data.get("recorded_local_vintage") or data.get(
            "intended_local_vintage"
        )
        if not vintage:
            raise SystemExit("No local vintage in rebuild JSON")
    years = [int(x) for x in args.years.split(",") if x.strip()]
    safe = vintage.replace("/", "_")
    for year in years:
        c = (
            CONFIG_DIR
            / f"2025_usa_cornerstone_v0_4_nowcast_{year}_waste_weights_control_local.yaml"
        )
        t = (
            CONFIG_DIR
            / f"2025_usa_cornerstone_v0_4_nowcast_{year}_waste_weights_match_io_local.yaml"
        )
        c.write_text(_yaml(year, vintage=vintage, treatment=False), encoding="utf-8")
        t.write_text(_yaml(year, vintage=vintage, treatment=True), encoding="utf-8")
        print("wrote", c.name, t.name)
    print(f"LOCAL_MUT_VINTAGE={vintage} (label hint: {safe})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
