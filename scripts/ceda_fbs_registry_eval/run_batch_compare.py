#!/usr/bin/env python
"""
Temporary entrypoint: run batch FBS vs registry comparison (section 2).
Usage: python -m scripts.ceda_fbs_registry_eval.run_batch_compare
       or python scripts/ceda_fbs_registry_eval/run_batch_compare.py
"""
from __future__ import annotations

import sys
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from scripts.ceda_fbs_registry_eval.compare import run_batch_comparison  # noqa: E402

OUTPUT_DIR = Path(__file__).resolve().parent / "output"
MAPPING_PATH = OUTPUT_DIR / "fbs_slice_to_registry_mapping.csv"


def main() -> None:
    if not MAPPING_PATH.exists():
        print(
            f"Mapping file not found: {MAPPING_PATH}. Run overlap assessment first, "
            "then add (fbs_slice, emissions_source) pairs to fbs_slice_to_registry_mapping.csv."
        )
        print("Run: python -m scripts.ceda_fbs_registry_eval.run_overlap_report")
        sys.exit(1)
    summary = run_batch_comparison(
        mapping_path=MAPPING_PATH,
        fbs_methodname="GHG_national_CEDA_2023",
        output_dir=OUTPUT_DIR,
    )
    print(f"Compared {summary['compared'].sum()} of {len(summary)} pairs.")
    print(f"Summary written to {OUTPUT_DIR / 'comparison_summary.csv'}")


if __name__ == "__main__":
    main()
