import numpy as np
import pandas as pd

from bedrock.transform.flowbysector import FlowBySector, getFlowBySector
from bedrock.utils.config.schema import dq_fields
from bedrock.utils.validation.validation import compare_FBS

baseline = "GHG_national_Cornerstone_2023"
test_method = "GHG_national_Cornerstone_2023"

# Optional: show all data (baseline vs test side by side) for these MetaSources.
# Leave empty to skip. Uses merge only — does not filter to differences.
METASOURCES_TO_COMPARE: list[str] = [
    "EPA_GHGI_T_3_13",
    "EPA_GHGI_T_3_14",
    "EPA_GHGI_T_3_15",
    # "EPA_GHGI_T_3_45",
    # "EPA_GHGI_T_3_47",
    # "EPA_GHGI_T_3_49",
]

# Download and load from GCS (local directory needs to be empty of this
# method to force new download)
fbs_baseline = getFlowBySector(baseline, download_FBS_if_missing=True)

# Compare to newly generated version
FlowBySector.generateFlowBySector(test_method, download_sources_ok=False)
fbs_test = getFlowBySector(test_method)


def _filter_to_metasources(df: pd.DataFrame, metasources: list[str]) -> pd.DataFrame:
    """Subset df to rows whose MetaSources value starts with any string in metasources."""
    if not metasources or "MetaSources" not in df.columns:
        return df
    mask = (
        df["MetaSources"]
        .astype(str)
        .apply(lambda x: any(x.startswith(ms) for ms in metasources))
    )
    return df[mask].copy()


def merge_fbs_all_rows(
    df1: pd.DataFrame, df2: pd.DataFrame, ignore_metasources: bool = False
) -> pd.DataFrame:
    """Merge two FBS dataframes on key columns; keep all rows (baseline and test values side by side)."""
    d1 = df1.rename(columns={"FlowAmount": "Baseline"})
    d2 = df2.rename(columns={"FlowAmount": "Update"})
    merge_cols = [
        c
        for c in d2.select_dtypes(include=["object", "int"]).columns
        if c not in dq_fields
    ]
    if ignore_metasources:
        for e in [
            "MetaSources",
            "AttributionSources",
            "SourceName",
            "SectorSourceName",
            "ProducedBySectorType",
            "ConsumedBySectorType",
            "Unit_other",
            "AllocationSources",
            "FlowName",
        ]:
            try:
                merge_cols.remove(e)
            except ValueError:
                pass
    for c in ["SectorProducedBy", "SectorConsumedBy"]:
        d1[c] = d1[c].astype(str)
        d2[c] = d2[c].astype(str)
    for c in ["SectorSourceName"]:
        d1 = d1.drop(columns=c, errors="ignore")
        d2 = d2.drop(columns=c, errors="ignore")
        merge_cols = [x for x in merge_cols if x != c]
    fill_cols = [c for c in merge_cols if d2[c].dtype == "object"]
    d1[fill_cols] = d1[fill_cols].replace(["nan", np.nan], "")
    d2[fill_cols] = d2[fill_cols].replace(["nan", np.nan], "")
    s1 = (
        d1[merge_cols + ["Baseline"]]
        .groupby(merge_cols, dropna=False)
        .agg({"Baseline": "sum"})
        .reset_index()
    )
    s2 = (
        d2[merge_cols + ["Update"]]
        .groupby(merge_cols, dropna=False)
        .agg({"Update": "sum"})
        .reset_index()
    )
    merged = pd.merge(s1, s2, how="outer")
    merged["FlowAmount_diff"] = merged["Update"].fillna(0) - merged["Baseline"].fillna(
        0
    )
    merged["Percent_Increase"] = (merged["FlowAmount_diff"] / merged["Baseline"]) * 100
    return merged.sort_values(
        ["Location", "SectorProducedBy", "SectorConsumedBy", "Flowable", "Context"],
        ignore_index=True,
    ).reset_index(drop=True)


# Full comparison (differences only)
df_m = compare_FBS(fbs_baseline, fbs_test, ignore_metasources=False)

# All data for listed metasources (no filter to differences)
if METASOURCES_TO_COMPARE:
    fbs_baseline_sub = _filter_to_metasources(fbs_baseline, METASOURCES_TO_COMPARE)
    fbs_test_sub = _filter_to_metasources(fbs_test, METASOURCES_TO_COMPARE)
    df_all_metasources = merge_fbs_all_rows(
        fbs_baseline_sub, fbs_test_sub, ignore_metasources=False
    )
    print(
        f"All rows for metasources {METASOURCES_TO_COMPARE}: {len(df_all_metasources)} rows"
    )
    # df_all_metasources has Baseline, Update, FlowAmount_diff, Percent_Increase
    df_all_metasources[
        [
            'Flowable',
            'SectorProducedBy',
            'Unit',
            'MetaSources',
            'Baseline',
            'Update',
            'FlowAmount_diff',
            'Percent_Increase',
        ]
    ].to_csv(f"{test_method}_diff.csv")
else:
    df_m.to_csv(f"{test_method}_diff.csv")
