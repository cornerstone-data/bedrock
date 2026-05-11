#!/usr/bin/env python3
"""
Merge multiple EF diagnostics Google Sheets into one comparison workbook.

Reads diagnostics tabs directly from Google Sheets (no xlsx export step
required) and writes:

- a local merged Excel workbook (default ``ef_diagnostics_merged.xlsx``)
- merged Google Sheets tabs on the combo's output Sheet (optional)

Per-run inputs are picked up from a registered ``ComboSpec`` in
``combinations.py``: each combo names a Drive folder of diagnostics Sheets,
the ordered Sheet titles to merge, the per-``config_name`` target mapping
for net-diff tabs, and the default output Sheet to push merged tabs to.
Tab data is fetched via ``analysis.fetch.load_tab``, which caches each tab
as parquet under ``analysis/.cache/<sheet_id>/<tab>.parquet`` so repeated
runs skip the network.

Output tabs (identical schema to the legacy xlsx flow):
  1) ``D_and_diffs_merged`` -- ``sector``, ``sector_name``, one column per
     ``config_name`` containing ``D_new``.
  2) ``N_and_diffs_merged`` -- ``sector``, ``sector_name``, one column per
     ``config_name`` containing ``N_new``.
  3) ``D_net_diff`` / ``N_net_diff`` -- per-config net differences computed
     as ``config - target_config`` using the combo's target mapping.
  4) ``totals`` -- concatenated rows from ``BLy_and_E_orig_diffs`` (columns
     B:E in the source tab), with a leading ``config_name``.
  5) ``totals_net_diff`` -- net differences for ``BLy (MtCO2e)`` only.
  6) ``config_summary_merged`` -- all config fields across runs, with a
     first row labelled ``filename`` holding each input Sheet's title.

Assumptions about the input Sheets (matching ``generate_diagnostics.py``):
  - ``D_and_diffs`` / ``N_and_diffs`` have a ``sector_name`` column and a
    sector code column named either ``sector`` or ``index``.
  - ``config_summary`` has columns ``config_field`` and ``value``, including
    a row where ``config_field == 'config_name'``.

Usage::

    python -m bedrock.utils.validation.analysis.combine_ef_diagnostics \\
        --combo v0.2 [--refresh] [--output-xlsx PATH] [--output-sheet-id ID]
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import click
import numpy as np
import pandas as pd

from bedrock.utils.io.gcp import (
    DRIVE_MIME_SPREADSHEET,
    list_drive_folder,
    update_sheet_tab,
)
from bedrock.utils.validation.analysis.combinations import COMBINATIONS, ComboSpec
from bedrock.utils.validation.analysis.fetch import load_tab

logger = logging.getLogger(__name__)

# Default output xlsx lives next to diagnostics_plots' output under
# analysis/output/<combo>/ef_diagnostics_merged.xlsx. Override with
# --output-xlsx <path>; pass --output-xlsx "" to skip the local file.
OUTPUT_ROOT = Path(__file__).resolve().parent / "output"
DEFAULT_OUTPUT_XLSX_NAME = "ef_diagnostics_merged.xlsx"


def _default_output_xlsx_path(combo_name: str) -> Path:
    """Return the canonical default xlsx path for a combo."""
    return OUTPUT_ROOT / combo_name / DEFAULT_OUTPUT_XLSX_NAME


def _dedupe_preserve_order(names: list[str]) -> list[str]:
    """Deduplicate while preserving order by appending ``__<n>`` suffixes."""
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


def _resolve_sheets_from_folder(
    folder_id: str,
    titles_in_order: list[str],
) -> list[tuple[str, str]]:
    """Resolve an ordered list of Sheet titles in a Drive folder to ``(id, title)`` pairs.

    Filters the folder to native Google Sheets, matches on title, and raises
    if any requested title is missing.
    """
    rows = list_drive_folder(folder_id, mime_type=DRIVE_MIME_SPREADSHEET)
    by_name: dict[str, str] = {r["name"]: r["id"] for r in rows}

    resolved: list[tuple[str, str]] = []
    missing: list[str] = []
    for title in titles_in_order:
        if title in by_name:
            resolved.append((by_name[title], title))
        else:
            missing.append(title)
    if missing:
        raise FileNotFoundError(
            "Could not find the following Sheet titles in Drive folder "
            f"{folder_id!r}: {missing}"
        )
    return resolved


def _find_sector_code_column(df: pd.DataFrame, tab: str) -> str:
    """Identify the sector-code column in ``D_and_diffs`` / ``N_and_diffs``.

    Bedrock's diagnostics writer often resets index and uses ``index`` as
    the sector code column name, but some exports may already use ``sector``.
    """
    cols_lower = {c.lower(): c for c in df.columns}
    for candidate in ("sector", "index"):
        if candidate in cols_lower:
            return cols_lower[candidate]

    if "sector_name" in df.columns:
        for c in df.columns:
            if c != "sector_name":
                return c

    raise ValueError(
        f"Could not identify sector-code column in tab '{tab}'. "
        f"Columns: {list(df.columns)}"
    )


def _read_config_name(sheet_id: str, label: str, *, refresh: bool = False) -> str:
    """Return the ``config_name`` value from a run's ``config_summary`` tab."""
    cfg = load_tab(sheet_id, "config_summary", refresh=refresh)

    cols_lower = {c.lower(): c for c in cfg.columns}
    if "config_field" not in cols_lower or "value" not in cols_lower:
        raise ValueError(
            f"`config_summary` in Sheet '{label}' ({sheet_id}) must have columns "
            f"`config_field` and `value`. Found: {list(cfg.columns)}"
        )

    config_field_col = cols_lower["config_field"]
    value_col = cols_lower["value"]

    matches = cfg.loc[cfg[config_field_col].astype(str) == "config_name", value_col]
    if matches.empty:
        raise ValueError(
            f"`config_summary` in Sheet '{label}' ({sheet_id}) does not contain "
            f"a row with config_field == 'config_name'."
        )
    return str(matches.iloc[0])


def _read_sector_and_values(
    sheet_id: str,
    label: str,
    tab: str,
    metric_col: str,
    *,
    refresh: bool = False,
) -> tuple[pd.DataFrame, pd.Series]:
    """Read one ``D_and_diffs`` / ``N_and_diffs`` tab and return ``(sector_meta, values)``."""
    df = load_tab(sheet_id, tab, refresh=refresh)
    if "sector_name" not in df.columns:
        raise ValueError(
            f"Tab '{tab}' in Sheet '{label}' ({sheet_id}) must have a `sector_name` "
            f"column. Found: {list(df.columns)}"
        )
    if metric_col not in df.columns:
        raise ValueError(
            f"Tab '{tab}' in Sheet '{label}' ({sheet_id}) must have a `{metric_col}` "
            f"column. Found: {list(df.columns)}"
        )

    sector_code_col = _find_sector_code_column(df, tab=tab)

    out = df[[sector_code_col, "sector_name", metric_col]].copy()

    # Drop rows with missing sector codes. Some exports can carry extra blank
    # rows that would otherwise become the literal string "nan" after astype(str)
    # and then break duplicate handling below.
    out = out.dropna(subset=[sector_code_col])

    out[sector_code_col] = out[sector_code_col].astype(str)
    out["sector_name"] = out["sector_name"].astype(str)

    # Some exports can contain duplicate sector codes (e.g. due to filtering
    # and copy/paste). Condense duplicates deterministically: sector_name -> first,
    # metric -> mean (NaN-safe via pd.to_numeric(..., errors="coerce")).
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
    target_mapping: dict[str, str],
) -> pd.DataFrame:
    """Build a table where each config column equals ``col - target_col``."""
    required_base_cols = {"sector", "sector_name"}
    if not required_base_cols.issubset(set(merged_df.columns)):
        raise KeyError(
            "Expected columns 'sector' and 'sector_name' in merged_df. "
            f"Got: {list(merged_df.columns)}"
        )

    net_df = merged_df[["sector", "sector_name"]].copy()
    for cfg_name in config_names_in_order:
        if cfg_name not in target_mapping:
            raise KeyError(
                "No target-column mapping for config_name="
                f"{cfg_name!r}. Update the combo's target_mapping to include "
                "all config_name columns present in your input Sheets."
            )
        target_col = target_mapping[cfg_name]
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


def _read_config_summary_map(
    sheet_id: str,
    label: str,
    *,
    refresh: bool = False,
) -> dict[str, object]:
    """Return the full field->value dict from a run's ``config_summary`` tab."""
    cfg = load_tab(sheet_id, "config_summary", refresh=refresh)
    cols_lower = {c.lower(): c for c in cfg.columns}
    if "config_field" not in cols_lower or "value" not in cols_lower:
        raise ValueError(
            f"`config_summary` in Sheet '{label}' ({sheet_id}) must have columns "
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


def merge_sheets(
    sheet_inputs_in_order: list[tuple[str, str]],
    *,
    target_mapping: dict[str, str],
    output_xlsx_path: str | None,
    output_sheet_id: str | None = None,
    refresh: bool = False,
) -> dict[str, pd.DataFrame]:
    """Merge multiple diagnostics Sheets into the 7 comparison tabs.

    Args:
        sheet_inputs_in_order: Ordered ``(sheet_id, source_label)`` pairs. The
            label is used in error messages and as the ``filename``-row entry
            in ``config_summary_merged`` (typically the source Sheet title).
        target_mapping: Per-``config_name`` target column for net-diff tabs.
        output_xlsx_path: Local Excel workbook to write. Pass ``None`` or an
            empty string to skip the local file.
        output_sheet_id: Optional Google Sheet to push merged tabs to. Pass
            ``None`` or an empty string to skip the Sheets write.
        refresh: When True, re-fetch every tab from Sheets, bypassing the
            parquet cache in ``analysis/.cache/``.

    Returns:
        Dict of ``{tab_name: DataFrame}`` for the 7 merged output tabs.
    """
    if not sheet_inputs_in_order:
        raise ValueError("Need at least one input Sheet.")

    config_names_raw = [
        _read_config_name(sid, label, refresh=refresh)
        for sid, label in sheet_inputs_in_order
    ]
    config_names = _dedupe_preserve_order(config_names_raw)

    sector_codes_in_order: list[str] = []
    sector_name_by_code: dict[str, str] = {}

    d_new_by_config: dict[str, pd.Series] = {}
    n_new_by_config: dict[str, pd.Series] = {}

    for (sheet_id, label), config_name in zip(sheet_inputs_in_order, config_names):
        sector_meta, d_new = _read_sector_and_values(
            sheet_id=sheet_id,
            label=label,
            tab="D_and_diffs",
            metric_col="D_new",
            refresh=refresh,
        )
        _, n_new = _read_sector_and_values(
            sheet_id=sheet_id,
            label=label,
            tab="N_and_diffs",
            metric_col="N_new",
            refresh=refresh,
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
                        f"Sector name mismatch for code '{code}' between Sheets. "
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

    net_d = _build_net_diff_table(
        merged_d, config_names_in_order=config_names, target_mapping=target_mapping
    )
    net_n = _build_net_diff_table(
        merged_n, config_names_in_order=config_names, target_mapping=target_mapping
    )

    config_maps = [
        _read_config_summary_map(sid, label, refresh=refresh)
        for sid, label in sheet_inputs_in_order
    ]

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

    # Prepend a first row with the source Sheet titles so each column is
    # easy to trace back to its input Sheet. Column header stays "filename"
    # for parity with the original xlsx-driven schema.
    labels_in_order = [label for _sid, label in sheet_inputs_in_order]
    filename_row: dict[str, object] = {"config_field": "filename"}
    for config_name, label in zip(config_names, labels_in_order):
        filename_row[config_name] = label

    config_merged = pd.concat(
        [pd.DataFrame([filename_row]), config_fields_merged],
        ignore_index=True,
    )

    # totals tab: rows from BLy_and_E_orig_diffs, cols B:E (positional 1..4).
    # read_sheet_tab preserves header order so positional slicing matches the
    # original xlsx behavior.
    totals_frames: list[pd.DataFrame] = []
    for (sheet_id, label), config_name in zip(sheet_inputs_in_order, config_names):
        df_totals = load_tab(sheet_id, "BLy_and_E_orig_diffs", refresh=refresh)
        if df_totals.shape[1] < 5:
            raise ValueError(
                f"Tab 'BLy_and_E_orig_diffs' in Sheet '{label}' ({sheet_id}) must "
                f"have at least 5 columns (to select B:E). Found {df_totals.shape[1]} "
                "columns."
            )
        totals = df_totals.iloc[:, 1:5].copy()
        totals.insert(0, "config_name", config_name)
        totals.insert(1, "row_order", range(len(totals)))
        totals_frames.append(totals)
    totals_merged = pd.concat(totals_frames, ignore_index=True)

    # Net differences for totals: BLy only, against the target mapping.
    totals_net_frames: list[pd.DataFrame] = []
    for cfg_name in config_names:
        if cfg_name not in target_mapping:
            raise KeyError(
                "No target-column mapping for config_name="
                f"{cfg_name!r}. Update the combo's target_mapping to include "
                "all config_name values."
            )

        target_cfg = target_mapping[cfg_name]
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
        bly_src = np.array(src_bly, dtype=np.float64)
        bly_tgt = np.array(tgt_bly, dtype=np.float64)
        out[bly_col] = bly_src - bly_tgt

        totals_net_frames.append(out)

    totals_net_diff = pd.concat(totals_net_frames, ignore_index=True)

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

    if output_xlsx_path:
        os.makedirs(os.path.dirname(output_xlsx_path) or ".", exist_ok=True)
        with pd.ExcelWriter(output_xlsx_path, engine="openpyxl") as writer:
            for tab_name, df in output_tables.items():
                df.to_excel(writer, index=False, sheet_name=tab_name)
        logger.info("Wrote merged workbook: %s", output_xlsx_path)
    else:
        logger.info("Skipped local Excel output (output_xlsx_path is empty).")

    if output_sheet_id:
        for tab_name, df in output_tables.items():
            update_sheet_tab(output_sheet_id, tab_name, df, clean_nans=True)
        logger.info("Pushed merged tabs to Sheet %s", output_sheet_id)
    else:
        logger.info("Skipped Google Sheets push (output_sheet_id is empty).")

    return output_tables


def _resolve_combo(combo_name: str) -> ComboSpec:
    if combo_name not in COMBINATIONS:
        raise click.UsageError(
            f"Unknown combo {combo_name!r}. Registered: {sorted(COMBINATIONS)}."
        )
    return COMBINATIONS[combo_name]


@click.command(
    help=(
        "Merge multiple EF diagnostics Google Sheets into one comparison "
        "workbook. Default writes a local .xlsx with all 7 comparison tabs; "
        "pass --output-sheet-id (or rely on the combo default) to also push "
        "the tabs to a Google Sheet."
    )
)
@click.option(
    "--combo",
    "combo_name",
    required=True,
    type=click.Choice(list(COMBINATIONS)),
    help="Registered combination from combinations.COMBINATIONS.",
)
@click.option(
    "--refresh",
    is_flag=True,
    default=False,
    help="Force re-fetch from Google Sheets, overwriting the parquet cache.",
)
@click.option(
    "--output-xlsx",
    type=str,
    default=None,
    help=(
        "Local Excel workbook path. Defaults to "
        f"analysis/output/<combo>/{DEFAULT_OUTPUT_XLSX_NAME}. "
        "Pass an empty string to skip the local file."
    ),
)
@click.option(
    "--output-sheet-id",
    type=str,
    default=None,
    help=(
        "Google Sheet ID for the merged output. When omitted, no Sheets "
        "push happens and only the local xlsx is written."
    ),
)
def main(
    combo_name: str,
    refresh: bool,
    output_xlsx: str | None,
    output_sheet_id: str | None,
) -> None:
    spec = _resolve_combo(combo_name)
    sheet_inputs = _resolve_sheets_from_folder(
        folder_id=spec.drive_folder_id,
        titles_in_order=spec.names_in_order,
    )
    if output_xlsx is None:
        effective_xlsx_path: str | None = str(_default_output_xlsx_path(combo_name))
    else:
        effective_xlsx_path = output_xlsx or None
    merge_sheets(
        sheet_inputs_in_order=sheet_inputs,
        target_mapping=spec.target_mapping,
        output_xlsx_path=effective_xlsx_path,
        output_sheet_id=output_sheet_id or None,
        refresh=refresh,
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s"
    )
    main()
