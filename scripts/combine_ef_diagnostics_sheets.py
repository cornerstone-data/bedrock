#!/usr/bin/env python3
"""
Merge Bedrock EF diagnostics across multiple Google Sheets into one output sheet.

Reads diagnostics tabs directly from Google Sheets in a Drive folder (resolved
by filename), merges them, and writes results to an output Google Sheet.
No files are downloaded.

Output tabs:
  1) `D_and_diffs_merged`:
     `sector`, `sector_name`, plus one column per `config_name` containing `D_new`
  2) `N_and_diffs_merged`:
     `sector`, `sector_name`, plus one column per `config_name` containing `N_new`
  3) `D_net_diff` and `N_net_diff`:
     per-config net differences computed as `config - target_config`
     using `TARGET_COLUMN_BY_CONFIG_NAME`
  4) `totals`:
     concatenated rows from `BLy_and_E_orig_diffs` (columns B:E), with leading
     `config_name`
  5) `totals_net_diff`:
     net differences for `BLy (MtCO2e)` only, using the same target mapping
  6) `config_summary_merged`:
     all config fields across sheets, with first row `source_name`

Assumptions based on Bedrock's diagnostics writer:
  - `D_and_diffs` and `N_and_diffs` have a `sector_name` column and a sector
    code column named either `sector` or `index`.
  - `config_summary` has columns `config_field` and `value`, including a
    row where config_field == `config_name`.
"""

from __future__ import annotations

import typing as ta

import google.auth
import pandas as pd
from googleapiclient.discovery import build

from bedrock.utils.io.gcp import read_sheet_tab, update_sheet_tab

# ---------------------------------------------------------------------------
# Input configuration
# ---------------------------------------------------------------------------
# Google Drive folder containing the diagnostics sheets.
GOOGLE_DRIVE_FOLDER_ID: str = "1eJ648O86tPqQnQwetYsXo7FtZLeHAsqG"  # v0.2

# Ordered list of sheet names within the folder. Order controls column order
# in the merged output. If empty, all sheets in the folder are used (sorted
# by filename).
INPUT_SHEET_NAMES: list[str] = [
    "[2026-03-26 v0 baseline] EF diagnostics",
    "[2026-03-26 row 3, Cornerstone schema] EF diagnostics",
    "[2026-03-26 row 4, CEDA FBS] EF diagnostics",
    "[2026-03-26 row 5, Cornerstone schema and CEDA FBS] EF diagnostics",
    "[2026-03-26 row 6, refrigerants compared to GHG baseline] EF diagnostics",
    "[2026-03-26 row 7, ng and petrol systems compared to GHG baseline] EF diagnostics",
    "[2026-03-26 row 8, mobile transport compared to GHG baseline] EF diagnostics",
    "[2026-03-26 row 9, electricity compared to GHG baseline] EF diagnostics",
    "[2026-03-26 row 10, new activities compared to GHG baseline] EF diagnostics",
    "[2026-03-26 row 11, other gases update compared to GHG baseline] EF diagnostics",
    "[2026-03-26 row 12, ag and field emissions compared to GHG baseline] EF diagnostics",
    "[2026-03-26 row 13, CoA update compared to GHG baseline] EF diagnostics",
    "[2026-03-26 row 18, Waste disagg compared to GHG baseline] EF diagnostics",
    "[2026-03-26 row 20, full GHG model] EF diagnostics",
    "[2026-03-26 row 14, B transformation] EF diagnostics",
    "[2026-03-26 row 23, all changes] EF diagnostics",
]

# Google Sheet ID for the merged output.
OUTPUT_GOOGLE_SHEET_ID: str = "1TOLpjg80GBeb3C8sVKGvYRL9U5HfUgKSz_IHoWHainY"

# Target-column mapping for net-differences tabs.
#
# Keys should be the exact `config_name` strings found in each input
# sheet's `config_summary` tab. Values should be the *target column name*
# to subtract from (must exist as a column in the merged `D_and_diffs_merged`
# / `N_and_diffs_merged` tables).
#
# Edit this dict to include *all* config_name columns you expect.
TARGET_COLUMN_BY_CONFIG_NAME: dict[str, str] = {
    "v8_ceda_2025_usa": "v8_ceda_2025_usa",
    "2025_usa_ceda_ghg_from_flowsa": "v8_ceda_2025_usa",
    "2025_usa_cornerstone_taxonomy": "v8_ceda_2025_usa",
    "2025_usa_cornerstone_fbs_schema": "2025_usa_cornerstone_taxonomy",
    # GHG runs -> cornerstone_fbs_schema
    "2025_usa_cornerstone_ghg_refrigerants_foams": "2025_usa_cornerstone_fbs_schema",
    "2025_usa_cornerstone_ghg_petroleum_natgas": "2025_usa_cornerstone_fbs_schema",
    "2025_usa_cornerstone_ghg_mobile_combustion": "2025_usa_cornerstone_fbs_schema",
    "2025_usa_cornerstone_ghg_electricity": "2025_usa_cornerstone_fbs_schema",
    "2025_usa_cornerstone_ghg_new_activities": "2025_usa_cornerstone_fbs_schema",
    "2025_usa_cornerstone_ghg_other_gases": "2025_usa_cornerstone_fbs_schema",
    "2025_usa_cornerstone_ghg_ag_soils": "2025_usa_cornerstone_fbs_schema",
    "2025_usa_cornerstone_ghg_updated_coa": "2025_usa_cornerstone_fbs_schema",
    "2025_usa_cornerstone_ghg": "2025_usa_cornerstone_fbs_schema",
    # Non-GHG configs (edit if your comparison target differs)
    "2025_usa_cornerstone_taxonomy_and_B_transformation": "2025_usa_cornerstone_taxonomy",
    "2025_usa_cornerstone_taxonomy_and_waste_disagg": "2025_usa_cornerstone_fbs_schema",
    "2025_usa_cornerstone_full_model": "2025_usa_cornerstone_taxonomy",
}


# ---------------------------------------------------------------------------
# Drive helpers — resolve sheet names to IDs without downloading anything
# ---------------------------------------------------------------------------


def _drive_client() -> ta.Any:
    """Build a Drive API client with read-only scope."""
    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
    return build("drive", "v3", credentials=credentials)


def _list_sheets_in_folder(folder_id: str) -> list[dict[str, str]]:
    """List Google Sheets (not Excel files) in a Drive folder."""
    if not folder_id:
        raise ValueError("GOOGLE_DRIVE_FOLDER_ID must be set.")
    client = _drive_client()
    query = (
        f"'{folder_id}' in parents and trashed=false "
        "and mimeType='application/vnd.google-apps.spreadsheet'"
    )
    resp = (
        client.files()
        .list(
            q=query,
            fields="files(id,name)",
            pageSize=1000,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
    )
    return ta.cast(list[dict[str, str]], resp.get("files", []))


def _resolve_sheet_ids(
    folder_id: str,
    names_in_order: list[str],
) -> list[tuple[str, str]]:
    """
    Match sheet names to their Google Sheet IDs within a Drive folder.

    Returns:
        Ordered list of (sheet_id, display_name) tuples.
    """
    files = _list_sheets_in_folder(folder_id)
    by_name: dict[str, str] = {f["name"]: f["id"] for f in files}

    if names_in_order:
        missing: list[str] = []
        results: list[tuple[str, str]] = []
        for name in names_in_order:
            file_id = by_name.get(name)
            if file_id is None:
                missing.append(name)
                continue
            results.append((file_id, name))

        if missing:
            raise FileNotFoundError(
                "Could not find the following sheets in Google Drive folder "
                f"{folder_id!r}: {missing}"
            )
        return results
    else:
        # Empty names => use all sheets in the folder, sorted by name.
        return [(by_name[n], n) for n in sorted(by_name.keys())]


# ---------------------------------------------------------------------------
# Merge helpers
# ---------------------------------------------------------------------------


def _dedupe_preserve_order(names: list[str]) -> list[str]:
    """Deduplicate while preserving order by appending __<n> suffixes."""
    seen: dict[str, int] = {}
    out: list[str] = []
    for n in names:
        if n not in seen:
            seen[n] = 1
            out.append(n)
            continue
        seen[n] += 1
        out.append(f"{n}__{seen[n]}")
    return out


def _find_sector_code_column(df: pd.DataFrame, tab: str) -> str:
    """
    Identify the sector-code column in `D_and_diffs` / `N_and_diffs`.

    Bedrock's writer often resets index and uses `index` as the sector code
    column name, but some exports may already use `sector`.
    """
    cols_lower = {c.lower(): c for c in df.columns}
    for candidate in ("sector", "index"):
        if candidate in cols_lower:
            return cols_lower[candidate]

    # Fallback: take the first column that's not `sector_name`.
    if "sector_name" in df.columns:
        for c in df.columns:
            if c != "sector_name":
                return c

    raise ValueError(
        f"Could not identify sector-code column in tab '{tab}'. "
        f"Columns: {list(df.columns)}"
    )


def _read_config_name(sheet_id: str) -> str:
    cfg = read_sheet_tab(sheet_id, "config_summary")

    cols_lower = {c.lower(): c for c in cfg.columns}
    if "config_field" not in cols_lower or "value" not in cols_lower:
        raise ValueError(
            f"`config_summary` in sheet '{sheet_id}' must have columns "
            f"`config_field` and `value`. Found: {list(cfg.columns)}"
        )

    config_field_col = cols_lower["config_field"]
    value_col = cols_lower["value"]

    matches = cfg.loc[cfg[config_field_col].astype(str) == "config_name", value_col]
    if matches.empty:
        raise ValueError(
            f"`config_summary` in sheet '{sheet_id}' does not contain "
            f"a row with config_field == 'config_name'."
        )
    return str(matches.iloc[0])


def _read_sector_and_values(
    sheet_id: str,
    tab: str,
    metric_col: str,
) -> tuple[pd.DataFrame, pd.Series]:
    df = read_sheet_tab(sheet_id, tab)
    if "sector_name" not in df.columns:
        raise ValueError(
            f"Tab '{tab}' in sheet '{sheet_id}' must have a `sector_name` column. "
            f"Found: {list(df.columns)}"
        )
    if metric_col not in df.columns:
        raise ValueError(
            f"Tab '{tab}' in sheet '{sheet_id}' must have a `{metric_col}` column. "
            f"Found: {list(df.columns)}"
        )

    sector_code_col = _find_sector_code_column(df, tab=tab)

    # Normalize dtypes so merges/joining behave consistently.
    out = df[[sector_code_col, "sector_name", metric_col]].copy()

    # Drop rows with missing sector codes — blank trailing rows from the
    # Sheets API would otherwise become the literal string "nan" after
    # astype(str), breaking indexing.
    out = out.dropna(subset=[sector_code_col])

    out[sector_code_col] = out[sector_code_col].astype(str)
    out["sector_name"] = out["sector_name"].astype(str)

    # Duplicate sector codes can appear (e.g. from copy/paste in Sheets).
    # Condense deterministically: first sector_name, mean of metric.
    out[metric_col] = pd.to_numeric(out[metric_col], errors="coerce")

    grouped = (
        out.groupby(sector_code_col, dropna=False)
        .agg(
            sector_name=("sector_name", "first"),
            metric=(metric_col, "mean"),
        )
        .rename(columns={"metric": metric_col})
    )

    grouped = grouped.reset_index().rename(columns={sector_code_col: "sector"})

    sector_meta = grouped[["sector", "sector_name"]]

    values = grouped.set_index("sector")[metric_col]
    values.index = values.index.astype(str)
    return sector_meta, values


def _build_net_diff_table(
    merged_df: pd.DataFrame,
    config_names_in_order: list[str],
) -> pd.DataFrame:
    """Build a table where each config column equals col - target_col."""
    required_base_cols = {"sector", "sector_name"}
    if not required_base_cols.issubset(set(merged_df.columns)):
        raise KeyError(
            "Expected columns 'sector' and 'sector_name' in merged_df. "
            f"Got: {list(merged_df.columns)}"
        )

    net_df = merged_df[["sector", "sector_name"]].copy()
    for cfg_name in config_names_in_order:
        if cfg_name not in TARGET_COLUMN_BY_CONFIG_NAME:
            raise KeyError(
                "No target-column mapping for config_name="
                f"{cfg_name!r}. Update TARGET_COLUMN_BY_CONFIG_NAME to include "
                "all config_name columns present in your input sheets."
            )
        target_col = TARGET_COLUMN_BY_CONFIG_NAME[cfg_name]
        if cfg_name not in merged_df.columns:
            raise KeyError(
                f"Expected config column {cfg_name!r} in merged_df columns. "
                f"Got: {list(merged_df.columns)}"
            )
        if target_col not in merged_df.columns:
            raise KeyError(
                f"Expected target column {target_col!r} for config column {cfg_name!r} "
                f"but it is missing from merged_df columns: {list(merged_df.columns)}"
            )
        net_df[cfg_name] = merged_df[cfg_name] - merged_df[target_col]
    return net_df


def _read_config_summary_map(sheet_id: str) -> dict[str, object]:
    cfg = read_sheet_tab(sheet_id, "config_summary")
    cols_lower = {c.lower(): c for c in cfg.columns}
    if "config_field" not in cols_lower or "value" not in cols_lower:
        raise ValueError(
            f"`config_summary` in sheet '{sheet_id}' must have columns "
            f"`config_field` and `value`. Found: {list(cfg.columns)}"
        )
    config_field_col = cols_lower["config_field"]
    value_col = cols_lower["value"]
    return (
        cfg[[config_field_col, value_col]]
        .dropna(subset=[config_field_col])
        .astype({config_field_col: str})
        .set_index(config_field_col)[value_col]
        .to_dict()
    )


# ---------------------------------------------------------------------------
# Main merge logic
# ---------------------------------------------------------------------------


def merge_diagnostics(
    input_sheets: list[tuple[str, str]],
    output_sheet_id: str,
) -> dict[str, pd.DataFrame]:
    """
    Read diagnostics from multiple Google Sheets and merge into one output sheet.

    Args:
        input_sheets: Ordered list of (sheet_id, display_name) tuples.
        output_sheet_id: Google Sheet ID to write merged results to.

    Returns:
        Dict mapping tab name to the DataFrame written.
    """
    if not input_sheets:
        raise ValueError("Need at least one input sheet.")

    sheet_ids = [sid for sid, _ in input_sheets]
    display_names = [name for _, name in input_sheets]

    config_names = [_read_config_name(sid) for sid in sheet_ids]
    config_names = _dedupe_preserve_order(config_names)

    sector_codes_in_order: list[str] = []
    sector_name_by_code: dict[str, str] = {}

    d_new_by_config: dict[str, pd.Series] = {}
    n_new_by_config: dict[str, pd.Series] = {}

    for sheet_id, display_name, config_name in zip(
        sheet_ids, display_names, config_names
    ):
        print(f"Reading diagnostics for {config_name} ({display_name})...")
        sector_meta, d_new = _read_sector_and_values(
            sheet_id=sheet_id,
            tab="D_and_diffs",
            metric_col="D_new",
        )
        _, n_new = _read_sector_and_values(
            sheet_id=sheet_id,
            tab="N_and_diffs",
            metric_col="N_new",
        )

        for _, r in sector_meta.iterrows():
            code = str(r["sector"])
            name = str(r["sector_name"])
            if code not in sector_name_by_code:
                sector_codes_in_order.append(code)
                sector_name_by_code[code] = name
            elif sector_name_by_code[code] != name:
                raise ValueError(
                    f"Sector name mismatch for code '{code}' between sheets. "
                    f"Earlier='{sector_name_by_code[code]}' vs later='{name}'."
                )

        d_new_by_config[config_name] = d_new
        n_new_by_config[config_name] = n_new

    merged_d = pd.DataFrame(
        {
            "sector": sector_codes_in_order,
            "sector_name": [sector_name_by_code[c] for c in sector_codes_in_order],
        }
    )
    # Column order: sector/meta, then one column per input in provided order.
    for config_name in config_names:
        merged_d[config_name] = (
            d_new_by_config[config_name].reindex(merged_d["sector"]).values
        )

    merged_n = pd.DataFrame(
        {
            "sector": sector_codes_in_order,
            "sector_name": [sector_name_by_code[c] for c in sector_codes_in_order],
        }
    )
    for config_name in config_names:
        merged_n[config_name] = (
            n_new_by_config[config_name].reindex(merged_n["sector"]).values
        )

    net_d = _build_net_diff_table(merged_d, config_names_in_order=config_names)
    net_n = _build_net_diff_table(merged_n, config_names_in_order=config_names)

    config_maps = [_read_config_summary_map(sid) for sid in sheet_ids]

    config_fields_in_order: list[str] = []
    config_fields_seen: set[str] = set()
    for cfg_map in config_maps:
        for k in cfg_map.keys():
            if k not in config_fields_seen:
                config_fields_in_order.append(k)
                config_fields_seen.add(k)

    config_fields_merged = pd.DataFrame({"config_field": config_fields_in_order})
    for cfg_map, config_name in zip(config_maps, config_names):
        config_fields_merged[config_name] = config_fields_merged["config_field"].map(
            cfg_map
        )

    # Prepend first row with source names so it is easy to trace each column.
    source_row: dict[str, object] = {"config_field": "source_name"}
    for config_name, display_name in zip(config_names, display_names):
        source_row[config_name] = display_name

    config_merged = pd.concat(
        [pd.DataFrame([source_row]), config_fields_merged],
        ignore_index=True,
    )

    # Totals tab: rows from BLy_and_E_orig_diffs, columns B:E (positional 1..4).
    totals_frames: list[pd.DataFrame] = []
    for sheet_id, config_name in zip(sheet_ids, config_names):
        df_totals = read_sheet_tab(sheet_id, "BLy_and_E_orig_diffs")
        if df_totals.shape[1] < 5:
            raise ValueError(
                f"Tab 'BLy_and_E_orig_diffs' in sheet '{sheet_id}' must have at "
                f"least 5 columns (to select B:E). Found {df_totals.shape[1]} columns."
            )
        # Use positional slicing to match Excel's column letters.
        totals = df_totals.iloc[:, 1:5].copy()
        # Sheets API returns all values as strings; coerce numeric columns.
        for col in totals.columns:
            totals[col] = pd.to_numeric(totals[col], errors="coerce").fillna(
                totals[col]
            )
        totals.insert(0, "config_name", config_name)
        totals.insert(1, "row_order", range(len(totals)))
        totals_frames.append(totals)
    totals_merged = pd.concat(totals_frames, ignore_index=True)

    # Net differences for totals tab based on target mapping dictionary.
    totals_net_frames: list[pd.DataFrame] = []
    for cfg_name in config_names:
        if cfg_name not in TARGET_COLUMN_BY_CONFIG_NAME:
            raise KeyError(
                "No target-column mapping for config_name="
                f"{cfg_name!r}. Update TARGET_COLUMN_BY_CONFIG_NAME to include "
                "all config_name values."
            )

        target_cfg = TARGET_COLUMN_BY_CONFIG_NAME[cfg_name]
        src = totals_merged[totals_merged["config_name"] == cfg_name].copy()
        tgt = totals_merged[totals_merged["config_name"] == target_cfg].copy()
        if tgt.empty:
            raise KeyError(
                f"Target config_name {target_cfg!r} for {cfg_name!r} was not found "
                "in totals_merged config_name values."
            )

        out = pd.DataFrame(
            {
                "config_name": cfg_name,
                "target_config_name": target_cfg,
                "row_order": src["row_order"],
            }
        )

        # Only evaluate net diff for BLy.
        bly_col = "BLy (MtCO2e)"
        if bly_col not in src.columns:
            raise KeyError(
                f"Expected column {bly_col!r} in totals data for {cfg_name!r}. "
                f"Got: {list(src.columns)}"
            )
        src_bly = pd.to_numeric(src[bly_col], errors="coerce")

        merged_tot = src.merge(
            tgt[["row_order", bly_col]],
            on="row_order",
            how="left",
            suffixes=("", "__target"),
        )
        tgt_bly = pd.to_numeric(merged_tot[f"{bly_col}__target"], errors="coerce")
        out[bly_col] = src_bly.values - tgt_bly.values

        totals_net_frames.append(out)

    totals_net_diff = pd.concat(totals_net_frames, ignore_index=True)

    # row_order is internal for alignment; hide from final output tabs.
    totals_output = totals_merged.drop(columns=["row_order"])
    totals_net_output = totals_net_diff.drop(columns=["row_order"])

    output_tables: dict[str, pd.DataFrame] = {
        "D_and_diffs_merged": merged_d,
        "N_and_diffs_merged": merged_n,
        "totals": totals_output,
        "totals_net_diff": totals_net_output,
        "D_net_diff": net_d,
        "N_net_diff": net_n,
        "config_summary_merged": config_merged,
    }

    if not output_sheet_id:
        raise ValueError("OUTPUT_GOOGLE_SHEET_ID must be set.")

    for tab_name, df in output_tables.items():
        print(f"Writing tab '{tab_name}' to output sheet...")
        update_sheet_tab(output_sheet_id, tab_name, df, clean_nans=True)

    return output_tables


if __name__ == "__main__":
    input_sheets = _resolve_sheet_ids(
        folder_id=GOOGLE_DRIVE_FOLDER_ID,
        names_in_order=INPUT_SHEET_NAMES,
    )
    merge_diagnostics(
        input_sheets=input_sheets,
        output_sheet_id=OUTPUT_GOOGLE_SHEET_ID,
    )
    print(f"Done. Output written to sheet: {OUTPUT_GOOGLE_SHEET_ID}")
