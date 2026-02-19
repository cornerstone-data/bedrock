"""
Temporary: harmonization (preserving MetaSources), slice filter, and FBS vs registry comparison.
Section 2 of CEDA FBS vs Registry alignment plan.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from bedrock.transform.allocation.constants import EmissionsSource
from bedrock.transform.allocation.derived import derive_E_usa_emissions_sources
from bedrock.transform.flowbysector import getFlowBySector
from bedrock.utils.emissions.ghg import GHG_MAPPING
from bedrock.utils.emissions.gwp import GWP100_AR6_CEDA
from bedrock.utils.mapping.sectormapping import map_to_BEA_sectors
from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from bedrock.utils.taxonomy.mappings.bea_v2017_industry__bea_v2017_commodity import (
    load_bea_v2017_industry_to_bea_v2017_commodity,
)

logger = logging.getLogger(__name__)

# Same as in derived.load_E_from_flowsa / load_E_from_flowsa_long (reuse logic, no edit)
GAS_MAP = {
    "Carbon dioxide": "CO2",
    "Methane": "CH4_fossil",
    "Nitrous oxide": "N2O",
    "Nitrogen trifluoride": "NF3",
    "Sulfur hexafluoride": "SF6",
    "HFC, PFC and SF6 F-HTFs": "HFCs",
    "HFCs and PFCs, unspecified": "HFCs",
    "Carbon tetrafluoride": "CF4",
    "Hexafluoroethane": "C2F6",
    "PFC": "PFCs",
    "Perfluorocyclobutane": "c-C4F8",
    "Perfluoropropane": "C3F8",
}
FLOW_TO_GAS = {m: g for g, members in GHG_MAPPING.items() for m in members}
FLOW_TO_GAS["CH4_fossil"] = "CH4"
FLOW_TO_GAS["HFC-227ea"] = "HFCs"
FLOW_TO_GAS["c-C4F8"] = "PFCs"


def harmonize_fbs_long_preserve_metasources(
    fbs_methodname: str = "GHG_national_CEDA_2023",
) -> pd.DataFrame:
    """
    Load FBS, harmonize to BEA/CEDA schema and CO2e, but keep full MetaSources
    (e.g. EPA_GHGI_T_2_1.electricity_transmission) for atomic comparison.
    Returns long DataFrame: Flowable (gas), Sector, MetaSources, FlowAmount (CO2e).
    """
    fbs = getFlowBySector(methodname=fbs_methodname)
    fbs = fbs.assign(Sector=fbs["SectorProducedBy"]).drop(
        columns=["SectorProducedBy", "SectorConsumedBy"]
    )
    fbs = map_to_BEA_sectors(
        fbs, region="national", io_level="detail", output_year=2022, bea_year=2017
    )
    fbs["Flowable"] = fbs["Flowable"].map(GAS_MAP).fillna(fbs["Flowable"])
    ghg_mapping: dict[str, float] = {str(k): float(v) for k, v in GWP100_AR6_CEDA.items()}
    ghg_mapping["CH4"] = float(GWP100_AR6_CEDA["CH4_fossil"])
    ghg_mapping["HFCs"] = 1.0
    ghg_mapping["PFCs"] = 1.0
    fbs["FlowAmount"] = fbs["FlowAmount"] * fbs["Flowable"].map(ghg_mapping)
    fbs["Flowable"] = fbs["Flowable"].replace(
        {"CH4_fossil": "CH4", "HFC-227ea": "HFCs", "c-C4F8": "PFCs"}
    )
    # Collapse flows to 7-gas index
    fbs["Gas"] = fbs["Flowable"].map(FLOW_TO_GAS).fillna(fbs["Flowable"])
    # Collapse sectors to CEDA_V7
    bea_mapping = load_bea_v2017_industry_to_bea_v2017_commodity()
    sector_to_ceda = {k: v[0] for k, v in bea_mapping.items()}
    fbs["SectorCEDA"] = fbs["Sector"].map(sector_to_ceda)
    fbs = fbs.dropna(subset=["SectorCEDA"])
    out_df = fbs.groupby(
        ["Gas", "SectorCEDA", "MetaSources"], as_index=False
    )["FlowAmount"].sum()
    out_df = out_df.rename(columns={"SectorCEDA": "Sector", "FlowAmount": "CO2e"})
    return out_df[["Gas", "Sector", "MetaSources", "CO2e"]]


def get_fbs_slice_matrix(
    long_df: pd.DataFrame,
    meta_source: str | list[str],
    wide: bool = True,
) -> pd.DataFrame:
    """
    Filter long-form harmonized FBS by MetaSources; return that slice only.
    If wide=True, return matrix (flow/gas x sector); else return long form.
    """
    if isinstance(meta_source, str):
        meta_source = [meta_source]
    sub = long_df[long_df["MetaSources"].isin(meta_source)]
    if sub.empty:
        return pd.DataFrame()
    if wide:
        return sub.pivot_table(
            index="Gas", columns="Sector", values="CO2e", aggfunc="sum", fill_value=0
        )
    return sub


def compare_fbs_slice_to_registry(
    fbs_slice_id: str,
    emissions_source: str,
    fbs_methodname: str = "GHG_national_CEDA_2023",
    harmonized_long: pd.DataFrame | None = None,
    registry_df: pd.DataFrame | None = None,
) -> dict[str, Any] | None:
    """
    Compare one FBS slice to one registry emissions source.
    Returns a report dict with totals_by_gas, diff, rel_diff, optional diff_df.
    If FBS slice is empty or registry source has no row, logs warning and returns None.
    """
    es_member = next(
        (e for e in EmissionsSource if e.value == emissions_source), None
    )
    if es_member is None:
        logger.warning("Unknown EmissionsSource %s, skipping pair", emissions_source)
        return None
    es = es_member
    if harmonized_long is None:
        harmonized_long = harmonize_fbs_long_preserve_metasources(fbs_methodname)
    slice_wide = get_fbs_slice_matrix(harmonized_long, fbs_slice_id, wide=True)
    if slice_wide.empty:
        logger.warning("FBS slice %s has no rows, skipping pair", fbs_slice_id)
        return None
    if registry_df is None:
        registry_df = derive_E_usa_emissions_sources()
    if emissions_source not in registry_df.index:
        logger.warning(
            "Registry has no row for %s, skipping pair", emissions_source
        )
        return None
    reg_row: pd.Series[float] = (
        registry_df.loc[emissions_source].reindex(CEDA_V7_SECTORS).fillna(0)
    )
    gas = es.gas
    fbs_total = slice_wide.loc[gas].sum() if gas in slice_wide.index else 0.0
    reg_total = float(reg_row.sum())
    abs_diff = fbs_total - reg_total
    rel_diff = (fbs_total / reg_total - 1.0) if reg_total != 0 else None
    # Sector-level diff for same gas
    fbs_series = (
        slice_wide.loc[gas]
        if gas in slice_wide.index
        else pd.Series(0.0, index=CEDA_V7_SECTORS)
    )
    fbs_series = fbs_series.reindex(CEDA_V7_SECTORS).fillna(0)
    diff_series = fbs_series - reg_row
    return {
        "fbs_slice_id": fbs_slice_id,
        "emissions_source": emissions_source,
        "gas": gas,
        "fbs_total": float(fbs_total),
        "registry_total": reg_total,
        "abs_diff": float(abs_diff),
        "rel_diff": rel_diff,
        "totals_by_gas_fbs": (
            slice_wide.sum(axis=1).to_dict() if not slice_wide.empty else {}
        ),
        "diff_series": diff_series,
        "diff_df": pd.DataFrame(
            {"Sector": diff_series.index, "Diff": diff_series.values}
        ),
    }


# Cache filename for derive_E_usa_emissions_sources output (in output_dir)
_E_USA_EMISSIONS_SOURCES_CACHE = "E_usa_emissions_sources.parquet"


def run_batch_comparison(
    mapping_path: Path,
    fbs_methodname: str = "GHG_national_CEDA_2023",
    output_dir: Path | None = None,
) -> pd.DataFrame:
    """
    Run compare_fbs_slice_to_registry for each (fbs_slice, emissions_source) pair
    in the mapping table only. The mapping file is the single source of truth for
    which pairs to compare (not the overlap report).
    Writes summary CSV to output_dir. When output_dir is set, registry data
    (derive_E_usa_emissions_sources) is cached there to avoid re-running on subsequent calls.
    Returns summary DataFrame.
    """
    mapping_path = Path(mapping_path)
    if not mapping_path.exists():
        raise FileNotFoundError(
            f"Mapping file not found: {mapping_path}. "
            "Run overlap assessment, then edit fbs_slice_to_registry_mapping.csv with the pairs to compare."
        )
    mapping = pd.read_csv(mapping_path)
    if "fbs_slice" not in mapping.columns or "emissions_source" not in mapping.columns:
        raise ValueError(
            "mapping file must have fbs_slice and emissions_source columns"
        )
    # Drop rows with empty fbs_slice or emissions_source (e.g. header-only stub)
    mapping = mapping.dropna(subset=["fbs_slice", "emissions_source"])
    mapping = mapping[
        mapping["fbs_slice"].astype(str).str.strip().ne("")
        & mapping["emissions_source"].astype(str).str.strip().ne("")
    ]

    # Load or compute registry DataFrame; cache to output_dir when provided
    cache_path = (
        Path(output_dir) / _E_USA_EMISSIONS_SOURCES_CACHE
        if output_dir
        else None
    )
    if cache_path and cache_path.exists():
        registry_df = pd.read_parquet(cache_path)
    else:
        registry_df = derive_E_usa_emissions_sources()
        if cache_path:
            Path(cache_path).parent.mkdir(parents=True, exist_ok=True)
            registry_df.to_parquet(cache_path, index=True)

    harmonized = harmonize_fbs_long_preserve_metasources(fbs_methodname)
    rows = []
    for _, row in mapping.drop_duplicates(
        subset=["fbs_slice", "emissions_source"]
    ).iterrows():
        fbs_slice = str(row["fbs_slice"]).strip()
        es = str(row["emissions_source"]).strip()
        report = compare_fbs_slice_to_registry(
            fbs_slice,
            es,
            fbs_methodname,
            harmonized_long=harmonized,
            registry_df=registry_df,
        )
        if report is None:
            rows.append(
                {
                    "fbs_slice": fbs_slice,
                    "emissions_source": es,
                    "gas": row.get("gas", ""),
                    "fbs_total": None,
                    "registry_total": None,
                    "abs_diff": None,
                    "rel_diff": None,
                    "compared": False,
                }
            )
            continue
        rows.append(
            {
                "fbs_slice": fbs_slice,
                "emissions_source": es,
                "gas": report["gas"],
                "fbs_total": report["fbs_total"],
                "registry_total": report["registry_total"],
                "abs_diff": report["abs_diff"],
                "rel_diff": report["rel_diff"],
                "compared": True,
            }
        )
    summary = pd.DataFrame(rows)
    if output_dir:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        summary.to_csv(output_dir / "comparison_summary.csv", index=False)
    return summary
