#!/usr/bin/env python3
"""
Merge Bedrock EF diagnostics Excel workbooks into one comparison workbook.

Reads diagnostics from local files or Google Drive, then writes:
  - a local merged workbook (`OUTPUT_EXCEL_PATH`)
  - Google Sheets tabs (`OUTPUT_GOOGLE_SHEET_ID`) when in Drive mode

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
     all config fields across files, with first row `filename`

Assumptions based on Bedrock's diagnostics writer:
  - `D_and_diffs` and `N_and_diffs` have a `sector_name` column and a sector
    code column named either `sector` or `index`.
  - `config_summary` has columns `config_field` and `value`, including a
    row where config_field == `config_name`.
"""

from __future__ import annotations

import os
import tempfile
import typing as ta

import google.auth
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from bedrock.utils.io.gcp import update_sheet_tab

# -----------------------------
# Input configuration (Drive-first)
# -----------------------------
# Default mode is Google Drive. Configure folder ID, then set stems.
# If INPUT_FILE_STEMS is empty, all .xlsx files in the Drive folder are used
# (sorted by filename). When stems are provided, order here controls merge order.
READ_INPUTS_FROM_GOOGLE_DRIVE: bool = False
GOOGLE_DRIVE_FOLDER_ID: str = "1eJ648O86tPqQnQwetYsXo7FtZLeHAsqG"  # v0.2

INPUT_FILE_STEMS: list[str] = [
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
    "[2026-04-14 row 14, B transformation] EF diagnostics",
    "[2026-04-15 row 21, B transformation AND waste] EF diagnostics",
    "[2026-04-08 row 23, all changes] EF diagnostics",
]
INPUT_FILE_EXTENSION: str = ".xlsx"

# Local fallback mode config (used only when READ_INPUTS_FROM_GOOGLE_DRIVE=False).
INPUT_EXCEL_DIR: str = r""

OUTPUT_EXCEL_PATH: str = r"ef_diagnostics_merged.xlsx"

# Google Sheets output config for Drive mode.
# In Drive input mode, output tabs are pushed to this sheet.
# In local mode, no Google Sheets write is performed.
OUTPUT_GOOGLE_SHEET_ID: str = "1TOLpjg80GBeb3C8sVKGvYRL9U5HfUgKSz_IHoWHainY"


# Target-column mapping for net-differences tabs.
#
# Keys should be the exact `config_name` strings found in each input
# workbook's `config_summary` tab. Values should be the *target column name*
# to subtract from (must exist as a column in the merged `D_and_diffs_merged`
# / `N_and_diffs_merged` tables).
#
# Edit this dict to include *all* config_name columns you expect.
TARGET_COLUMN_BY_CONFIG_NAME: dict[str, str] = {
    "v8_ceda_2025_usa": "v8_ceda_2025_usa",
    "2025_usa_ceda_ghg_from_flowsa": "v8_ceda_2025_usa",
    "2025_usa_cornerstone_taxonomy": "v8_ceda_2025_usa",
    "2025_usa_cornerstone_fbs_schema": "v8_ceda_2025_usa",
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
    "2025_usa_cornerstone_taxonomy_and_waste_disagg": "2025_usa_cornerstone_fbs_schema",
    "2025_usa_cornerstone_taxonomy_and_B_transformation": "v8_ceda_2025_usa",
    "2025_usa_cornerstone_B_transformation_and_waste_disaggregation": "2025_usa_cornerstone_taxonomy_and_waste_disagg",
    "2025_usa_cornerstone_full_model": "v8_ceda_2025_usa",
}


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


def _drive_client() -> ta.Any:
    """Build a Drive API client."""
    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
    return build("drive", "v3", credentials=credentials)


def _list_drive_files_in_folder(folder_id: str) -> list[dict[str, str]]:
    """List non-trashed files in a Google Drive folder."""
    if not folder_id:
        raise ValueError("GOOGLE_DRIVE_FOLDER_ID must be set for Drive input mode.")
    client = _drive_client()
    query = (
        f"'{folder_id}' in parents and trashed=false "
        "and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'"
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


def _download_drive_file(file_id: str, destination_path: str) -> None:
    """Download a Drive file by ID to a local path."""
    client = _drive_client()
    request = client.files().get_media(fileId=file_id, supportsAllDrives=True)
    os.makedirs(os.path.dirname(destination_path) or ".", exist_ok=True)
    with open(destination_path, "wb") as f:
        downloader = MediaIoBaseDownload(f, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()


def _resolve_drive_inputs(
    folder_id: str,
    stems_in_order: list[str],
    extension: str,
    download_dir: str,
) -> list[str]:
    """Resolve stems to Drive files, download in-order, return local paths."""
    files = _list_drive_files_in_folder(folder_id)
    by_name: dict[str, str] = {f["name"]: f["id"] for f in files}
    local_paths: list[str] = []

    if stems_in_order:
        missing: list[str] = []
        for stem in stems_in_order:
            filename_with_ext = f"{stem}{extension}"
            # Drive files may or may not include ".xlsx" in the displayed name.
            matched_name: str | None = None
            if stem in by_name:
                matched_name = stem
            elif filename_with_ext in by_name:
                matched_name = filename_with_ext

            file_id = by_name.get(matched_name) if matched_name is not None else None
            if file_id is None:
                missing.append(stem)
                continue
            out_path = os.path.join(download_dir, matched_name)
            _download_drive_file(file_id=file_id, destination_path=out_path)
            local_paths.append(out_path)

        if missing:
            raise FileNotFoundError(
                "Could not find the following stems in Google Drive folder "
                f"{folder_id!r} (checked both stem and stem+extension): {missing}"
            )
    else:
        # Empty stems => use all files in the folder (already filtered by Drive
        # query to Excel mime type), regardless of filename extension.
        all_names = sorted(by_name.keys())
        for filename in all_names:
            out_path = os.path.join(download_dir, filename)
            _download_drive_file(
                file_id=by_name[filename],
                destination_path=out_path,
            )
            local_paths.append(out_path)

    return local_paths


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


def _read_config_name(excel_path: str) -> str:
    cfg = pd.read_excel(excel_path, sheet_name="config_summary")

    cols_lower = {c.lower(): c for c in cfg.columns}
    if "config_field" not in cols_lower or "value" not in cols_lower:
        raise ValueError(
            f"`config_summary` in '{excel_path}' must have columns "
            f"`config_field` and `value`. Found: {list(cfg.columns)}"
        )

    config_field_col = cols_lower["config_field"]
    value_col = cols_lower["value"]

    matches = cfg.loc[cfg[config_field_col].astype(str) == "config_name", value_col]
    if matches.empty:
        raise ValueError(
            f"`config_summary` in '{excel_path}' does not contain "
            f"a row with config_field == 'config_name'."
        )
    return str(matches.iloc[0])


def _read_sector_and_values(
    excel_path: str,
    tab: str,
    metric_col: str,
) -> tuple[pd.DataFrame, pd.Series]:
    df = pd.read_excel(excel_path, sheet_name=tab)
    if "sector_name" not in df.columns:
        raise ValueError(
            f"Tab '{tab}' in '{excel_path}' must have a `sector_name` column. "
            f"Found: {list(df.columns)}"
        )
    if metric_col not in df.columns:
        raise ValueError(
            f"Tab '{tab}' in '{excel_path}' must have a `{metric_col}` column. "
            f"Found: {list(df.columns)}"
        )

    sector_code_col = _find_sector_code_column(df, tab=tab)

    # Normalize dtypes so merges/joining behave consistently.
    out = df[[sector_code_col, "sector_name", metric_col]].copy()

    # Drop rows with missing sector codes. Some Excel exports can carry extra
    # blank rows that would otherwise become the literal string "nan" after
    # astype(str), which then breaks indexing/duplicate handling.
    out = out.dropna(subset=[sector_code_col])

    out[sector_code_col] = out[sector_code_col].astype(str)
    out["sector_name"] = out["sector_name"].astype(str)

    # Some exports can contain duplicate sector codes (e.g., due to filtering
    # and then copy/paste in Excel). Since the merge expects one value per
    # sector, condense duplicates deterministically:
    # - sector_name: take the first non-null value (after string cast)
    # - metric_col: coerce to numeric and take the mean (ignores NaNs)
    out[metric_col] = pd.to_numeric(out[metric_col], errors="coerce")

    grouped = (
        out.groupby(sector_code_col, dropna=False)
        .agg(
            sector_name=("sector_name", "first"),
            metric=(metric_col, "mean"),
        )
        .rename(columns={"metric": metric_col})
    )

    # `grouped` has unique sector_code_col index, so downstream reindex()
    # works without duplicate-label failures.
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
                "all config_name columns present in your input workbooks."
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


def _read_config_summary_map(excel_path: str) -> dict[str, object]:
    cfg = pd.read_excel(excel_path, sheet_name="config_summary")
    cols_lower = {c.lower(): c for c in cfg.columns}
    if "config_field" not in cols_lower or "value" not in cols_lower:
        raise ValueError(
            f"`config_summary` in '{excel_path}' must have columns "
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


def merge_excels(
    excel_paths_in_order: list[str],
    output_path: str,
) -> dict[str, pd.DataFrame]:
    if not excel_paths_in_order:
        raise ValueError("Need at least one input Excel path.")

    config_names = [_read_config_name(p) for p in excel_paths_in_order]
    config_names = _dedupe_preserve_order(config_names)

    sector_codes_in_order: list[str] = []
    sector_name_by_code: dict[str, str] = {}

    d_new_by_config: dict[str, pd.Series] = {}
    n_new_by_config: dict[str, pd.Series] = {}

    for excel_path, config_name in zip(excel_paths_in_order, config_names):
        sector_meta, d_new = _read_sector_and_values(
            excel_path=excel_path,
            tab="D_and_diffs",
            metric_col="D_new",
        )
        _, n_new = _read_sector_and_values(
            excel_path=excel_path,
            tab="N_and_diffs",
            metric_col="N_new",
        )

        for _, r in sector_meta.iterrows():
            code = str(r["sector"])
            name = str(r["sector_name"])
            if code not in sector_name_by_code:
                sector_codes_in_order.append(code)
                sector_name_by_code[code] = name
            else:
                if sector_name_by_code[code] != name:
                    raise ValueError(
                        f"Sector name mismatch for code '{code}' between files. "
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

    # Column order: sector/meta, then for each file in provided order.
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

    # New net-difference tabs.
    net_d = _build_net_diff_table(merged_d, config_names_in_order=config_names)
    net_n = _build_net_diff_table(merged_n, config_names_in_order=config_names)

    config_maps = [_read_config_summary_map(p) for p in excel_paths_in_order]

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

    # Prepend first row with filenames so it is easy to trace each column.
    filenames_in_order = [os.path.basename(p) for p in excel_paths_in_order]
    filename_row: dict[str, object] = {"config_field": "filename"}
    for config_name, filename in zip(config_names, filenames_in_order):
        filename_row[config_name] = filename

    config_merged = pd.concat(
        [pd.DataFrame([filename_row]), config_fields_merged],
        ignore_index=True,
    )

    # totals tab: rows from BLy_and_E_orig_diffs, cols B:E (positional 1..4)
    totals_frames: list[pd.DataFrame] = []
    for excel_path, config_name in zip(excel_paths_in_order, config_names):
        df_totals = pd.read_excel(excel_path, sheet_name="BLy_and_E_orig_diffs")
        if df_totals.shape[1] < 5:
            raise ValueError(
                f"Tab 'BLy_and_E_orig_diffs' in '{excel_path}' must have at least "
                f"5 columns (to select B:E). Found {df_totals.shape[1]} columns."
            )
        # Use positional slicing to match Excel's column letters.
        totals = df_totals.iloc[:, 1:5].copy()
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

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for tab_name, df in output_tables.items():
            df.to_excel(writer, index=False, sheet_name=tab_name)

    should_write_to_google_sheet = READ_INPUTS_FROM_GOOGLE_DRIVE
    if should_write_to_google_sheet:
        if not OUTPUT_GOOGLE_SHEET_ID:
            raise ValueError(
                "Google Sheets output is enabled but OUTPUT_GOOGLE_SHEET_ID is empty."
            )
        for tab_name, df in output_tables.items():
            update_sheet_tab(OUTPUT_GOOGLE_SHEET_ID, tab_name, df, clean_nans=True)

    return output_tables


if __name__ == "__main__":
    if READ_INPUTS_FROM_GOOGLE_DRIVE:
        with tempfile.TemporaryDirectory(prefix="bedrock_diag_merge_") as tmp_dir:
            input_excel_files = _resolve_drive_inputs(
                folder_id=GOOGLE_DRIVE_FOLDER_ID,
                stems_in_order=INPUT_FILE_STEMS,
                extension=INPUT_FILE_EXTENSION,
                download_dir=tmp_dir,
            )
            merge_excels(
                excel_paths_in_order=input_excel_files,
                output_path=OUTPUT_EXCEL_PATH,
            )
    else:
        if INPUT_FILE_STEMS:
            input_excel_files = [
                os.path.join(INPUT_EXCEL_DIR, stem + INPUT_FILE_EXTENSION)
                for stem in INPUT_FILE_STEMS
            ]
        else:
            # Empty stems => use all matching files in INPUT_EXCEL_DIR.
            input_excel_files = sorted(
                [
                    os.path.join(INPUT_EXCEL_DIR, name)
                    for name in os.listdir(INPUT_EXCEL_DIR)
                    if name.endswith(INPUT_FILE_EXTENSION)
                ]
            )
        if not input_excel_files:
            raise ValueError("No input Excel files found based on current settings.")
        merge_excels(
            excel_paths_in_order=input_excel_files,
            output_path=OUTPUT_EXCEL_PATH,
        )
    print(f"Wrote merged workbook: {OUTPUT_EXCEL_PATH}")
