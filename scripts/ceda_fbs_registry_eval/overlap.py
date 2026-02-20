"""
Temporary: FBS slice enumeration and registry overlap assessment.
Section 1 of CEDA FBS vs Registry alignment plan.
Uses PrimaryActivity from FBS and activity strings from allocation modules to improve matching.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd

from bedrock.extract.allocation.epa_constants import (
    return_emissions_source_table_numbers,
)
from bedrock.transform.allocation.constants import EmissionsSource
from bedrock.transform.allocation.registry import ALLOCATED_EMISSIONS_REGISTRY
from bedrock.utils.config.common import load_yaml_dict
from bedrock.utils.config.settings import transformpath

# Allocation dirs scanned by return_emissions_source_table_numbers (must stay in sync)
_ALLOCATION_DIRS = [
    transformpath / "allocation/co2",
    transformpath / "allocation/ch4",
    transformpath / "allocation/n2o",
    transformpath / "allocation/other_gases",
]
# Regex to capture activity string from .loc[("GAS", "Activity Name")] or .loc[('GAS', 'Activity')]
_LOC_ACTIVITY_PATTERN = re.compile(
    r"\.loc\s*\[\s*\(\s*[\"'][^\"']*[\"']\s*,\s*[\"']([^\"']+)[\"']\s*\)",
    re.MULTILINE,
)


def get_fbs_config(method: str = "GHG_national_CEDA_2023") -> dict[str, Any]:
    """Load resolved FBS method config (with !include resolved)."""
    ghg_folder = transformpath / "ghg"
    return load_yaml_dict(method, "FBS", ghg_folder)


def _normalize_activity(s: str) -> str:
    """Normalize activity string for matching: strip comments, lowercase, collapse spaces."""
    s = str(s).split("#")[0].strip()
    s = re.sub(r"\s+", " ", s).lower()
    return s


def _primary_activities_from_config(sel: dict[str, Any]) -> list[str]:
    """Extract PrimaryActivity values from selection_fields; return normalized list."""
    raw = sel.get("PrimaryActivity")
    if raw is None:
        return []
    if isinstance(raw, str):
        activities = [raw]
    elif isinstance(raw, list):
        activities = list(raw)
    elif isinstance(raw, dict):
        # "Activity: SubActivity" format: use both key and value for matching
        activities = []
        for k, v in raw.items():
            activities.append(f"{k}: {v}" if v else k)
    else:
        return []
    out = []
    for a in activities:
        a = str(a).split("#")[0].strip()
        if a:
            out.append(a)
    return out


def enumerate_fbs_slices(method: str = "GHG_national_CEDA_2023") -> pd.DataFrame:
    """
    Enumerate all FBS slices from the method config.
    Returns a DataFrame with columns: fbs_slice, source_name, activity_set, flows,
    primary_activities (pipe-separated), primary_activities_normalized (for matching).
    """
    config = get_fbs_config(method)
    source_names = config.get("source_names", {})
    rows = []
    for source_name, source_config in source_names.items():
        activity_sets = source_config.get("activity_sets")
        if activity_sets:
            for activity_set in activity_sets.keys():
                fbs_slice = f"{source_name}.{activity_set}"
                as_cfg = activity_sets.get(activity_set, {})
                sel = as_cfg.get("selection_fields", {})
                flows: list[str] = []
                if "FlowName" in sel:
                    fn = sel["FlowName"]
                    flows = [fn] if isinstance(fn, str) else list(fn)
                primary = _primary_activities_from_config(sel)
                primary_norm = "|".join(_normalize_activity(a) for a in primary)
                rows.append(
                    {
                        "fbs_slice": fbs_slice,
                        "source_name": source_name,
                        "activity_set": activity_set,
                        "flows": "|".join(flows) if flows else "",
                        "primary_activities": "|".join(primary) if primary else "",
                        "primary_activities_normalized": primary_norm,
                    }
                )
        else:
            sel = source_config.get("selection_fields", {})
            primary = _primary_activities_from_config(sel)
            primary_norm = "|".join(_normalize_activity(a) for a in primary)
            rows.append(
                {
                    "fbs_slice": source_name,
                    "source_name": source_name,
                    "activity_set": "",
                    "flows": "",
                    "primary_activities": "|".join(primary) if primary else "",
                    "primary_activities_normalized": primary_norm,
                }
            )
    return pd.DataFrame(rows)


def _extract_activities_from_allocation_modules() -> dict[str, list[str]]:
    """
    Scan allocation .py files for .loc[("GAS", "Activity Name")] and return
    file_stem -> list of activity strings. Used to match registry sources to FBS PrimaryActivity.
    """
    stem_to_activities: dict[str, list[str]] = {}
    for directory in _ALLOCATION_DIRS:
        if not directory.exists():
            continue
        for path in directory.glob("*.py"):
            if path.name.startswith("_"):
                continue
            stem = path.stem
            text = path.read_text(encoding="utf-8")
            activities = list(
                {m.group(1).strip() for m in _LOC_ACTIVITY_PATTERN.finditer(text)}
            )
            if activities:
                stem_to_activities[stem] = activities
    return stem_to_activities


def _emissions_source_to_activities() -> dict[str, list[str]]:
    """Map each registry emissions_source (enum value) to list of activity strings from its allocator."""
    stem_to_value = _stem_to_emissions_source_value()
    stem_to_activities = _extract_activities_from_allocation_modules()
    out: dict[str, list[str]] = {}
    for stem, activities in stem_to_activities.items():
        es_value = stem_to_value.get(stem)
        if es_value:
            out[es_value] = activities
    return out


def _stem_to_emissions_source_value() -> dict[str, str]:
    """Build mapping from allocator file stem to EmissionsSource enum value from registry."""
    stem_to_value: dict[str, str] = {}
    for es in EmissionsSource:
        if es not in ALLOCATED_EMISSIONS_REGISTRY:
            continue
        allocator = ALLOCATED_EMISSIONS_REGISTRY[es]
        # Allocator __name__ is e.g. "allocate_adipic_acid"; file stem is "adipic_acid"
        name = getattr(allocator, "__name__", "")
        if name.startswith("allocate_"):
            stem = name.replace("allocate_", "", 1)
            stem_to_value[stem] = es.value
    return stem_to_value


def enumerate_registry_sources() -> pd.DataFrame:
    """
    Enumerate registry emissions sources with their GHGI table numbers and gas.
    Returns a DataFrame with columns: emissions_source (enum value), gas, table_number_1, ...
    """
    tbl = return_emissions_source_table_numbers()
    if tbl.empty:
        return pd.DataFrame(columns=["emissions_source", "gas", "table_number_1"])
    stem_to_value = _stem_to_emissions_source_value()
    gas_by_value = {es.value: es.gas for es in EmissionsSource}
    # tbl "emissions_source" column = file stem (e.g. adipic_acid, natural_gas_systems)
    enum_col = tbl["emissions_source"].map(lambda s: stem_to_value.get(str(s)))
    tbl = tbl.copy()
    tbl["emissions_source_enum"] = enum_col
    tbl = tbl.dropna(subset=["emissions_source_enum"])
    tbl = tbl.rename(columns={"emissions_source": "allocator_stem"})
    tbl = tbl.rename(columns={"emissions_source_enum": "emissions_source"})
    tbl["gas"] = tbl["emissions_source"].map(gas_by_value)
    cols = ["emissions_source", "gas"] + [
        c for c in tbl.columns if c.startswith("table_number_")
    ]
    return tbl[[c for c in cols if c in tbl.columns]]


def _activity_match(
    fbs_normalized: str,
    registry_activities: list[str],
) -> bool:
    """True if any registry activity (normalized) overlaps with FBS primary_activities (normalized)."""
    if not fbs_normalized or not registry_activities:
        return False
    fbs_set = {p.strip() for p in fbs_normalized.split("|") if p.strip()}
    for ra in registry_activities:
        ra_norm = _normalize_activity(ra)
        if not ra_norm:
            continue
        for fb in fbs_set:
            if ra_norm in fb or fb in ra_norm:
                return True
    return False


def build_overlap_report(
    fbs_slices: pd.DataFrame,
    registry_df: pd.DataFrame,
    mapping_path: Path | None = None,
    use_activity_matching: bool = True,
) -> pd.DataFrame:
    """
    Build overlap report: candidate (fbs_slice, emissions_source) pairs by
    same GHGI table and same gas. When use_activity_matching is True, also
    compares PrimaryActivity (FBS) with activity strings extracted from
    allocation modules and sets activity_match and match_quality.
    """
    # Flatten registry table numbers to one row per (emissions_source, table)
    tbl_cols = [c for c in registry_df.columns if c.startswith("table_number_")]
    registry_long = []
    for _, row in registry_df.iterrows():
        for c in tbl_cols:
            t = row.get(c)
            if pd.notna(t) and t:
                registry_long.append(
                    {
                        "emissions_source": row["emissions_source"],
                        "gas": row["gas"],
                        "ghgi_table": t,
                    }
                )
    if not registry_long:
        return pd.DataFrame(
            columns=[
                "fbs_slice",
                "emissions_source",
                "gas",
                "ghgi_table",
                "source",
                "activity_match",
                "match_quality",
                "fbs_primary_activities",
                "registry_activities",
            ]
        )
    reg = pd.DataFrame(registry_long)

    es_to_activities = (
        _emissions_source_to_activities() if use_activity_matching else {}
    )

    overlaps = []
    for _, r in fbs_slices.iterrows():
        fbs_slice = r["fbs_slice"]
        source_name = r["source_name"]
        fbs_norm = r.get("primary_activities_normalized", "") or ""
        matches = reg[reg["ghgi_table"] == source_name]
        for _, m in matches.iterrows():
            es = m["emissions_source"]
            reg_activities = es_to_activities.get(es, [])
            activity_match = _activity_match(fbs_norm, reg_activities)
            match_quality = (
                "table_gas_and_activity" if activity_match else "table_gas_only"
            )
            overlaps.append(
                {
                    "fbs_slice": fbs_slice,
                    "emissions_source": es,
                    "gas": m["gas"],
                    "ghgi_table": source_name,
                    "source": "candidate",
                    "activity_match": activity_match,
                    "match_quality": match_quality,
                    "fbs_primary_activities": r.get("primary_activities", "") or "",
                    "registry_activities": (
                        "|".join(reg_activities) if reg_activities else ""
                    ),
                }
            )
    overlap_df = pd.DataFrame(overlaps)

    if mapping_path and mapping_path.exists():
        mapping = pd.read_csv(mapping_path)
        if "fbs_slice" in mapping.columns and "emissions_source" in mapping.columns:
            for _, row in mapping.iterrows():
                fs, es = row["fbs_slice"], row["emissions_source"]
                mask = (overlap_df["fbs_slice"] == fs) & (
                    overlap_df["emissions_source"] == es
                )
                overlap_df.loc[mask, "source"] = "mapping_table"
                overlap_df.loc[mask, "activity_match"] = True
                overlap_df.loc[mask, "match_quality"] = "mapping_table"
    return overlap_df


def run_overlap_assessment(
    method: str = "GHG_national_CEDA_2023",
    output_dir: Path | None = None,
    mapping_path: Path | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Run full overlap assessment: FBS slices, registry sources, overlap report.
    Writes CSVs to output_dir if provided. Returns (fbs_slices, registry_df, overlap_report).
    """
    fbs_slices = enumerate_fbs_slices(method)
    registry_df = enumerate_registry_sources()
    overlap_report = build_overlap_report(fbs_slices, registry_df, mapping_path)

    if output_dir:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        fbs_slices.to_csv(output_dir / "fbs_slices.csv", index=False)
        registry_df.to_csv(output_dir / "registry_sources.csv", index=False)
        overlap_report.to_csv(output_dir / "overlap_report.csv", index=False)
        # Stub mapping table if not present
        stub_path = output_dir / "fbs_slice_to_registry_mapping.csv"
        if not stub_path.exists():
            pd.DataFrame(
                columns=["fbs_slice", "emissions_source", "gas", "notes"]
            ).to_csv(stub_path, index=False)

    return fbs_slices, registry_df, overlap_report
