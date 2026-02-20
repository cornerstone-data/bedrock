"""
Temporary: run a single FBS source or activity set by generating a temporary
method YAML (Option B). Section 3 of CEDA FBS vs Registry alignment plan.
Temp files are for iteration only and should not be committed.
"""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from bedrock.transform.flowbysector import getFlowBySector
from bedrock.utils.config.common import load_yaml_dict
from bedrock.utils.config.settings import transformpath

# Scratch directory for temporary FBS method YAMLs (user can override)
DEFAULT_SCRATCH_DIR = Path(__file__).resolve().parent / "temp_fbs_methods"


def run_single_fbs_slice(
    source_name: str,
    activity_set: str | None = None,
    method: str = "GHG_national_CEDA_2023",
    scratch_dir: Path | None = None,
    download_fba_ok: bool = False,
    download_fbs_ok: bool = False,
) -> pd.DataFrame:
    """
    Build a temporary FBS method YAML with only one source (and optionally one
    activity set), run getFlowBySector, and return the FBS DataFrame.
    Does not modify core code. Temp YAML is written to scratch_dir; do not commit.
    """
    scratch_dir = Path(scratch_dir or DEFAULT_SCRATCH_DIR)
    scratch_dir.mkdir(parents=True, exist_ok=True)
    ghg_folder = transformpath / "ghg"
    full_config = load_yaml_dict(method, "FBS", ghg_folder)
    # Deep copy and restrict to one source
    config = copy.deepcopy(full_config)
    sources = config.get("source_names", {})
    if source_name not in sources:
        raise ValueError(
            f"Source {source_name} not in method {method}. "
            f"Available: {list(sources.keys())}"
        )
    source_config = copy.deepcopy(sources[source_name])
    if activity_set is not None:
        activity_sets = source_config.get("activity_sets")
        if not activity_sets or activity_set not in activity_sets:
            raise ValueError(
                f"Activity set {activity_set} not in source {source_name}. "
                f"Available: {list(activity_sets.keys()) if activity_sets else []}"
            )
        source_config["activity_sets"] = {
            activity_set: copy.deepcopy(activity_sets[activity_set]),
        }
    config["source_names"] = {source_name: source_config}
    # Method name for temp file
    method_suffix = f"{source_name}_{activity_set}" if activity_set else source_name
    method_name = f"{method}_single_{method_suffix}"
    yaml_path = scratch_dir / f"{method_name}.yaml"
    with open(yaml_path, "w", encoding="utf-8") as f:
        yaml.dump(
            _sanitize_for_dump(config),
            f,
            default_flow_style=False,
            allow_unicode=True,
            sort_keys=False,
        )
    fbs = getFlowBySector(
        methodname=method_name,
        fbsconfigpath=str(scratch_dir),
        download_FBAs_if_missing=download_fba_ok,
        download_FBS_if_missing=download_fbs_ok,
    )
    return pd.DataFrame(fbs)


def _sanitize_for_dump(obj: Any) -> Any:
    """Recursively convert config to types that yaml.dump handles cleanly."""
    if isinstance(obj, dict):
        return {str(k): _sanitize_for_dump(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize_for_dump(x) for x in obj]
    if isinstance(obj, (str, int, float, bool, type(None))):
        return obj
    return str(obj)


if __name__ == "__main__":
    df = run_single_fbs_slice("EPA_GHGI_T_2_1", activity_set="electric_power")
