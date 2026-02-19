#!/usr/bin/env python
"""
Temporary entrypoint: run overlap assessment (section 1) and write outputs.
Usage: from repo root, python -m scripts.ceda_fbs_registry_eval.run_overlap_report
       or python scripts/ceda_fbs_registry_eval/run_overlap_report.py
"""
from __future__ import annotations

import sys
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from scripts.ceda_fbs_registry_eval.overlap import run_overlap_assessment

OUTPUT_DIR = Path(__file__).resolve().parent / "output"
MAPPING_PATH = OUTPUT_DIR / "fbs_slice_to_registry_mapping.csv"


def main() -> None:
    fbs_slices, registry_df, overlap_report = run_overlap_assessment(
        method="GHG_national_CEDA_2023",
        output_dir=OUTPUT_DIR,
        mapping_path=MAPPING_PATH,
    )
    print(f"FBS slices: {len(fbs_slices)}")
    print(f"Registry sources: {len(registry_df)}")
    print(f"Overlap report rows: {len(overlap_report)}")
    print(f"Outputs written to {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
