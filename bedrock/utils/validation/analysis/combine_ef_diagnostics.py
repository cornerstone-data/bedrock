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
     ``config_name`` containing ``D_new_inflated`` when that column exists on
     the run's tab, otherwise ``D_new``. When the combo's
     ``target_mapping`` references ``pinned_useeio_baseline`` (only
     meaningful when the first run is a USEEIO Excel-baseline comparison),
     an extra ``pinned_useeio_baseline`` column sourced from that run's
     ``D_old_inflated`` is inserted before the per-config columns.
  2) ``N_and_diffs_merged`` -- ``sector``, ``sector_name``, one column per
     ``config_name`` containing ``N_new_inflated`` when present, otherwise
     ``N_new``. Mirrors (1) for ``pinned_useeio_baseline``.
  3) ``D_net_diff`` / ``N_net_diff`` -- per-config net differences computed
     as ``config - target_config`` using the combo's target mapping.
  4) ``totals`` -- one USA-level row per ``config_name``. Source depends
     on the diagnostics mode of the input runs:
       * Release-vs-snapshot mode -- columns B:E of the producer's
         ``BLy_and_E_orig_diffs`` tab: ``BLy``, ``E_orig``,
         ``BLy - E_orig``, ``(BLy - E_orig) / E_orig (%)`` (unchanged
         from the legacy xlsx flow).
       * USEEIO Excel-baseline mode -- per-sector
         ``BLy_new_vs_BLy_old`` summed across sectors:
         ``BLy_new``, ``BLy_old``, ``BLy_new - BLy_old``,
         ``(BLy_new - BLy_old) / BLy_old (%)``. The producer doesn't
         write ``BLy_and_E_orig_diffs`` in this mode (no ``E_old`` for
         the Excel path) but always writes ``BLy_new_vs_BLy_old``.
     A combo is treated as USEEIO mode if any input run is a USEEIO
     comparison; otherwise the legacy schema is used.
  5) ``totals_net_diff`` -- net differences against the target mapping,
     on the BLy column appropriate to the source tab
     (``BLy (MtCO2e)`` for release-vs-snapshot, ``BLy_new (MtCO2e)``
     for USEEIO mode).
  6) ``config_summary_merged`` -- all config fields across runs, with a
     first row labelled ``filename`` holding each input Sheet's title.
     When the combo opts into ``pinned_useeio_baseline`` (USEEIO mode),
     an extra column carries the first run's pin metadata
     (``useeio_baseline_pin_*`` etc.) and other rows are blank.

Assumptions about the input Sheets (matching ``generate_diagnostics.py``):
  - ``D_and_diffs`` / ``N_and_diffs`` have a ``sector_name`` column and a
    sector code column named either ``sector`` or ``index``.
  - ``config_summary`` has columns ``config_field`` and ``value``, including
    rows where ``config_field`` is ``config_name`` and
    ``diagnostics_baseline_source``.

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
from bedrock.utils.validation.analysis.fetch import load_tab, load_tabs_optional

logger = logging.getLogger(__name__)

# Default output xlsx lives next to diagnostics_plots' output under
# analysis/output/<combo>/ef_diagnostics_merged.xlsx. Override with
# --output-xlsx <path>; pass --output-xlsx "" to skip the local file.
OUTPUT_ROOT = Path(__file__).resolve().parent / 'output'
DEFAULT_OUTPUT_XLSX_NAME = 'ef_diagnostics_merged.xlsx'

# Value of ``diagnostics_baseline_source`` (in a run's ``config_summary``)
# that flags the run as a "USEEIO comparison" -- i.e. the diagnostics
# producer compared the live model against the pinned USEEIO Excel
# baseline rather than a prior parquet snapshot release. Two features of
# the merger key off this:
#   * The producer SKIPS the ``BLy_and_E_orig_diffs`` tab in this mode
#     (no ``E_old`` for the Excel baseline path), so combos made entirely
#     of USEEIO-comparison runs cannot produce ``totals`` / ``totals_net_diff``.
#   * Only USEEIO-comparison runs populate the ``useeio_baseline_pin_*``
#     fields that identify the pinned Excel artifact, which is what makes
#     the synthetic ``pinned_useeio_baseline`` column (below) meaningful.
USEEIO_EXCEL_BASELINE_SOURCE = 'gcs_useeio_xlsx'

# Synthetic baseline column, injected into merged_d / merged_n /
# config_summary_merged ONLY when a combo's target_mapping references it.
# Values come from the FIRST input run's ``D_old_inflated`` / ``N_old_inflated``
# columns -- i.e. whatever pinned baseline that run was compared against in
# its own diagnostics. Only meaningful when that first run is a USEEIO
# comparison (see USEEIO_EXCEL_BASELINE_SOURCE above): then the column
# represents the pinned USEEIO Excel baseline and its pin metadata
# populates ``config_summary_merged.pinned_useeio_baseline``. Letting
# ``target_mapping`` opt in keeps combos that don't need it (e.g. v0.2-style
# release-vs-release runs) on their original output schema.
PINNED_USEEIO_BASELINE_COLUMN = 'pinned_useeio_baseline'

# config_summary fields surfaced in the ``pinned_useeio_baseline`` column.
# These only carry signal for USEEIO-comparison runs -- a non-USEEIO run
# leaves them blank (or just records ``diagnostics_baseline_source='gcs_snapshot'``).
# Other config_summary fields (config_name, git_*, etc.) describe the run
# that consumed the baseline, not the baseline itself, so they're left
# blank in the ``pinned_useeio_baseline`` column.
_USEEIO_BASELINE_PIN_FIELDS: frozenset[str] = frozenset(
    {
        'diagnostics_baseline_source',
        'useeio_baseline_pin_gs_uri',
        'useeio_baseline_pin_sha256',
        'useeio_baseline_pin_model_version_label',
        'model_version_label',
        'baseline_snapshot_key_used',
    }
)


def _is_useeio_comparison(config_summary_map: dict[str, object]) -> bool:
    """Return True iff the run's diagnostics were a USEEIO Excel baseline comparison.

    Mirrors the producer's branching in ``calculate_ef_diagnostics`` /
    ``calculate_national_accounting_balance_diagnostics``: when the live
    config has ``diagnostics_baseline_source == 'gcs_useeio_xlsx'``, the
    run targets the pinned USEEIO Excel baseline and omits
    ``BLy_and_E_orig_diffs``; otherwise it's a release-vs-snapshot run
    that produces that tab and carries no meaningful ``useeio_baseline_pin_*``
    metadata.
    """
    return (
        str(config_summary_map.get('diagnostics_baseline_source', '')).strip()
        == USEEIO_EXCEL_BASELINE_SOURCE
    )


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
        out.append(f'{n}__{seen[n]}')
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
    by_name: dict[str, str] = {r['name']: r['id'] for r in rows}

    resolved: list[tuple[str, str]] = []
    missing: list[str] = []
    for title in titles_in_order:
        if title in by_name:
            resolved.append((by_name[title], title))
        else:
            missing.append(title)
    if missing:
        raise FileNotFoundError(
            'Could not find the following Sheet titles in Drive folder '
            f'{folder_id!r}: {missing}'
        )
    return resolved


def _find_sector_code_column(df: pd.DataFrame, tab: str) -> str:
    """Identify the sector-code column in ``D_and_diffs`` / ``N_and_diffs``.

    Bedrock's diagnostics writer often resets index and uses ``index`` as
    the sector code column name, but some exports may already use ``sector``.
    """
    cols_lower = {c.lower(): c for c in df.columns}
    for candidate in ('sector', 'index'):
        if candidate in cols_lower:
            return cols_lower[candidate]

    if 'sector_name' in df.columns:
        for c in df.columns:
            if c != 'sector_name':
                return c

    raise ValueError(
        f"Could not identify sector-code column in tab '{tab}'. "
        f'Columns: {list(df.columns)}'
    )


def _extract_config_name(config_summary_map: dict[str, object], label: str) -> str:
    """Return the ``config_name`` value from a run's parsed config_summary map."""
    if 'config_name' not in config_summary_map:
        raise ValueError(
            f"`config_summary` in Sheet '{label}' does not contain a row with "
            f"config_field == 'config_name'."
        )
    return str(config_summary_map['config_name'])


def _resolve_metric_column(
    df: pd.DataFrame,
    *,
    tab: str,
    label: str,
    sheet_id: str,
    preferred_col: str,
    fallback_col: str,
) -> str:
    """Return ``preferred_col`` when present on the tab, else ``fallback_col``."""
    if preferred_col in df.columns:
        return preferred_col
    if fallback_col in df.columns:
        logger.info(
            "Tab '%s' in Sheet '%s' (%s): no `%s`; using `%s`.",
            tab,
            label,
            sheet_id,
            preferred_col,
            fallback_col,
        )
        return fallback_col
    raise ValueError(
        f"Tab '{tab}' in Sheet '{label}' ({sheet_id}) must have `{preferred_col}` "
        f'or `{fallback_col}`. Found: {list(df.columns)}'
    )


def _read_sector_and_values(
    sheet_id: str,
    label: str,
    tab: str,
    *,
    preferred_metric_col: str,
    fallback_metric_col: str | None = None,
    refresh: bool = False,
) -> tuple[pd.DataFrame, pd.Series]:
    """Read one ``D_and_diffs`` / ``N_and_diffs`` tab and return ``(sector_meta, values)``."""
    df = load_tab(sheet_id, tab, refresh=refresh)
    if 'sector_name' not in df.columns:
        raise ValueError(
            f"Tab '{tab}' in Sheet '{label}' ({sheet_id}) must have a `sector_name` "
            f'column. Found: {list(df.columns)}'
        )
    if fallback_metric_col is None:
        if preferred_metric_col not in df.columns:
            raise ValueError(
                f"Tab '{tab}' in Sheet '{label}' ({sheet_id}) must have a "
                f'`{preferred_metric_col}` column. Found: {list(df.columns)}'
            )
        metric_col = preferred_metric_col
    else:
        metric_col = _resolve_metric_column(
            df,
            tab=tab,
            label=label,
            sheet_id=sheet_id,
            preferred_col=preferred_metric_col,
            fallback_col=fallback_metric_col,
        )

    sector_code_col = _find_sector_code_column(df, tab=tab)

    out = df[[sector_code_col, 'sector_name', metric_col]].copy()

    # Drop rows with missing sector codes. Some exports can carry extra blank
    # rows that would otherwise become the literal string "nan" after astype(str)
    # and then break duplicate handling below.
    out = out.dropna(subset=[sector_code_col])

    out[sector_code_col] = out[sector_code_col].astype(str)
    out['sector_name'] = out['sector_name'].astype(str)

    # Some exports can contain duplicate sector codes (e.g. due to filtering
    # and copy/paste). Condense duplicates deterministically: sector_name -> first,
    # metric -> mean (NaN-safe via pd.to_numeric(..., errors="coerce")).
    out[metric_col] = pd.to_numeric(out[metric_col], errors='coerce')

    grouped = (
        out.groupby(sector_code_col, dropna=False)
        .agg(
            sector_name=('sector_name', 'first'),
            metric=(metric_col, 'mean'),
        )
        .rename(columns={'metric': metric_col})
    )

    grouped = grouped.reset_index().rename(columns={sector_code_col: 'sector'})

    sector_meta = grouped[['sector', 'sector_name']]

    values = grouped.set_index('sector')[metric_col]
    values.index = values.index.astype(str)
    return sector_meta, values


def _build_net_diff_table(
    merged_df: pd.DataFrame,
    config_names_in_order: list[str],
    target_mapping: dict[str, str],
) -> pd.DataFrame:
    """Build a table where each config column equals ``col - target_col``."""
    required_base_cols = {'sector', 'sector_name'}
    if not required_base_cols.issubset(set(merged_df.columns)):
        raise KeyError(
            "Expected columns 'sector' and 'sector_name' in merged_df. "
            f'Got: {list(merged_df.columns)}'
        )

    net_df = merged_df[['sector', 'sector_name']].copy()
    for cfg_name in config_names_in_order:
        if cfg_name not in target_mapping:
            raise KeyError(
                'No target-column mapping for config_name='
                f"{cfg_name!r}. Update the combo's target_mapping to include "
                'all config_name columns present in your input Sheets.'
            )
        target_col = target_mapping[cfg_name]
        if cfg_name not in merged_df.columns:
            raise KeyError(
                f'Expected config column {cfg_name!r} in merged_df columns. '
                f'Got: {list(merged_df.columns)}'
            )
        if target_col not in merged_df.columns:
            raise KeyError(
                f'Expected target column {target_col!r} for config column {cfg_name!r} '
                f'but it is missing from merged_df columns: {list(merged_df.columns)}'
            )
        net_df[cfg_name] = merged_df[cfg_name] - merged_df[target_col]
    return net_df


def _aggregate_bly_new_vs_old_to_usa_row(
    df: pd.DataFrame, *, sheet_label: str, sheet_id: str
) -> pd.DataFrame:
    """Collapse per-sector ``BLy_new_vs_BLy_old`` to a single USA-totals row.

    The producer's ``BLy_new_vs_BLy_old`` tab has one row per sector with
    columns ``BLy_new (MtCO2e)``, ``BLy_old (MtCO2e)``,
    ``BLy_new - BLy_old (MtCO2e)``, ``(BLy_new - BLy_old) / BLy_old (%)``.
    We sum the kg columns across sectors and recompute the percent from
    the summed numerator and denominator (so the result is a true
    weighted USA-level total, not a mean of per-sector percents).
    """
    required = ['BLy_new (MtCO2e)', 'BLy_old (MtCO2e)']
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"Tab 'BLy_new_vs_BLy_old' in Sheet '{sheet_label}' ({sheet_id}) "
            f'is missing required columns {missing}. Got: {list(df.columns)}'
        )
    sum_new = float(pd.to_numeric(df['BLy_new (MtCO2e)'], errors='coerce').sum())
    sum_old = float(pd.to_numeric(df['BLy_old (MtCO2e)'], errors='coerce').sum())
    diff = sum_new - sum_old
    perc = diff / sum_old if sum_old else float('nan')
    return pd.DataFrame(
        {
            'BLy_new (MtCO2e)': [sum_new],
            'BLy_old (MtCO2e)': [sum_old],
            'BLy_new - BLy_old (MtCO2e)': [diff],
            '(BLy_new - BLy_old) / BLy_old (%)': [perc],
        }
    )


def _read_config_summary_map(
    sheet_id: str,
    label: str,
    *,
    refresh: bool = False,
) -> dict[str, object]:
    """Return the full field->value dict from a run's ``config_summary`` tab."""
    cfg = load_tab(sheet_id, 'config_summary', refresh=refresh)
    cols_lower = {c.lower(): c for c in cfg.columns}
    if 'config_field' not in cols_lower or 'value' not in cols_lower:
        raise ValueError(
            f"`config_summary` in Sheet '{label}' ({sheet_id}) must have columns "
            f'`config_field` and `value`. Found: {list(cfg.columns)}'
        )
    config_field_col = cols_lower['config_field']
    value_col = cols_lower['value']
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
        raise ValueError('Need at least one input Sheet.')

    # Read each run's config_summary upfront and classify it. This is the
    # source of truth for two behaviors that both depend on whether a run
    # is a USEEIO Excel-baseline comparison:
    #   * USEEIO-comparison runs do NOT emit ``BLy_and_E_orig_diffs`` -- so
    #     combos containing any such run cannot produce the ``totals`` tabs.
    #   * Only USEEIO-comparison runs carry meaningful pin metadata for the
    #     synthetic ``pinned_useeio_baseline`` column.
    # See ``_is_useeio_comparison`` and the producer-side branching in
    # ``calculate_ef_diagnostics`` /
    # ``calculate_national_accounting_balance_diagnostics``.
    config_maps = [
        _read_config_summary_map(sid, label, refresh=refresh)
        for sid, label in sheet_inputs_in_order
    ]
    config_names_raw = [
        _extract_config_name(cfg_map, label)
        for cfg_map, (_sid, label) in zip(config_maps, sheet_inputs_in_order)
    ]
    config_names = _dedupe_preserve_order(config_names_raw)
    is_useeio_by_run = [_is_useeio_comparison(cfg_map) for cfg_map in config_maps]
    any_useeio = any(is_useeio_by_run)

    sector_codes_in_order: list[str] = []
    sector_name_by_code: dict[str, str] = {}

    d_new_by_config: dict[str, pd.Series] = {}
    n_new_by_config: dict[str, pd.Series] = {}

    # Opt-in synthetic baseline column. Gated by the combo's target_mapping
    # referencing PINNED_USEEIO_BASELINE_COLUMN, which is only a sensible
    # thing to do when the first input run is a USEEIO Excel-baseline
    # comparison (its D_old_inflated / N_old_inflated then reflect the
    # pinned baseline, and the pin metadata in its config_summary is what
    # feeds the pinned_useeio_baseline column of config_summary_merged).
    # Combos that don't reference it (e.g. v0.2-style release-vs-release
    # runs) skip this entirely and keep their original output schema.
    include_pinned_useeio_baseline = (
        PINNED_USEEIO_BASELINE_COLUMN in target_mapping.values()
    )
    pinned_baseline_d: pd.Series | None = None
    pinned_baseline_n: pd.Series | None = None
    if include_pinned_useeio_baseline:
        if not is_useeio_by_run[0]:
            logger.warning(
                'target_mapping references %r but the first input run %r is not '
                'a USEEIO Excel-baseline comparison '
                '(diagnostics_baseline_source != %r). The pinned_useeio_baseline '
                "column will carry that run's D_old_inflated / N_old_inflated "
                'values but its pin metadata will be empty.',
                PINNED_USEEIO_BASELINE_COLUMN,
                sheet_inputs_in_order[0][1],
                USEEIO_EXCEL_BASELINE_SOURCE,
            )
        first_sheet_id, first_label = sheet_inputs_in_order[0]
        _, pinned_baseline_d = _read_sector_and_values(
            sheet_id=first_sheet_id,
            label=first_label,
            tab='D_and_diffs',
            preferred_metric_col='D_old_inflated',
            refresh=refresh,
        )
        _, pinned_baseline_n = _read_sector_and_values(
            sheet_id=first_sheet_id,
            label=first_label,
            tab='N_and_diffs',
            preferred_metric_col='N_old_inflated',
            refresh=refresh,
        )

    for (sheet_id, label), config_name in zip(sheet_inputs_in_order, config_names):
        sector_meta, d_new = _read_sector_and_values(
            sheet_id=sheet_id,
            label=label,
            tab='D_and_diffs',
            preferred_metric_col='D_new_inflated',
            fallback_metric_col='D_new',
            refresh=refresh,
        )
        _, n_new = _read_sector_and_values(
            sheet_id=sheet_id,
            label=label,
            tab='N_and_diffs',
            preferred_metric_col='N_new_inflated',
            fallback_metric_col='N_new',
            refresh=refresh,
        )

        for _, r in sector_meta.iterrows():
            code = str(r['sector'])
            name = str(r['sector_name'])
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
            'sector': sector_codes_in_order,
            'sector_name': [sector_name_by_code[c] for c in sector_codes_in_order],
        }
    )
    if include_pinned_useeio_baseline and pinned_baseline_d is not None:
        merged_d[PINNED_USEEIO_BASELINE_COLUMN] = pinned_baseline_d.reindex(
            merged_d['sector']
        ).values
    for config_name in config_names:
        merged_d[config_name] = (
            d_new_by_config[config_name].reindex(merged_d['sector']).values
        )

    merged_n = pd.DataFrame(
        {
            'sector': sector_codes_in_order,
            'sector_name': [sector_name_by_code[c] for c in sector_codes_in_order],
        }
    )
    if include_pinned_useeio_baseline and pinned_baseline_n is not None:
        merged_n[PINNED_USEEIO_BASELINE_COLUMN] = pinned_baseline_n.reindex(
            merged_n['sector']
        ).values
    for config_name in config_names:
        merged_n[config_name] = (
            n_new_by_config[config_name].reindex(merged_n['sector']).values
        )

    net_d = _build_net_diff_table(
        merged_d, config_names_in_order=config_names, target_mapping=target_mapping
    )
    net_n = _build_net_diff_table(
        merged_n, config_names_in_order=config_names, target_mapping=target_mapping
    )

    config_fields_in_order: list[str] = []
    config_fields_seen: set[str] = set()
    for cfg_map in config_maps:
        for k in cfg_map.keys():
            if k not in config_fields_seen:
                config_fields_in_order.append(k)
                config_fields_seen.add(k)

    config_fields_merged = pd.DataFrame({'config_field': config_fields_in_order})
    if include_pinned_useeio_baseline:
        # Synthetic baseline column carries only the first run's pin /
        # baseline-identifying fields (see _USEEIO_BASELINE_PIN_FIELDS).
        # These fields are populated by the producer only when the run is
        # a USEEIO Excel-baseline comparison; the warning above fires if
        # that's not the case. Other config_summary fields describe the
        # run that consumed the baseline, not the baseline itself, so
        # they're left blank for the pinned_useeio_baseline column.
        first_cfg_map = config_maps[0]
        baseline_pin_map = {
            k: v for k, v in first_cfg_map.items() if k in _USEEIO_BASELINE_PIN_FIELDS
        }
        config_fields_merged[PINNED_USEEIO_BASELINE_COLUMN] = config_fields_merged[
            'config_field'
        ].map(baseline_pin_map)
    for cfg_map, config_name in zip(config_maps, config_names):
        config_fields_merged[config_name] = config_fields_merged['config_field'].map(
            cfg_map
        )

    # Prepend a first row with the source Sheet titles so each column is
    # easy to trace back to its input Sheet. Column header stays "filename"
    # for parity with the original xlsx-driven schema.
    labels_in_order = [label for _sid, label in sheet_inputs_in_order]
    filename_row: dict[str, object] = {'config_field': 'filename'}
    if include_pinned_useeio_baseline:
        filename_row[PINNED_USEEIO_BASELINE_COLUMN] = (
            f'pinned_useeio_baseline (from {labels_in_order[0]})'
        )
    for config_name, label in zip(config_names, labels_in_order):
        filename_row[config_name] = label

    config_merged = pd.concat(
        [pd.DataFrame([filename_row]), config_fields_merged],
        ignore_index=True,
    )

    # totals tab: one USA-level row per config. Source depends on mode:
    #   * Non-USEEIO runs (``diagnostics_baseline_source == 'gcs_snapshot'``):
    #     use the producer's pre-aggregated ``BLy_and_E_orig_diffs`` tab and
    #     take cols B:E (BLy, E_orig, diff, %). This preserves v0.2's
    #     schema unchanged.
    #   * USEEIO Excel-baseline runs (``diagnostics_baseline_source ==
    #     'gcs_useeio_xlsx'``): the producer skips ``BLy_and_E_orig_diffs``
    #     (no ``E_old`` for the Excel path), but always writes
    #     ``BLy_new_vs_BLy_old`` (per-sector BLy_new vs BLy_old). We sum
    #     across sectors here to recover the same one-row-per-config
    #     "USA totals" semantic, with columns BLy_new / BLy_old / diff / %.
    # The chosen source applies to every run in the combo (we key off
    # ``any_useeio`` so a mixed combo still picks a consistent schema).
    if any_useeio:
        totals_source_tab = 'BLy_new_vs_BLy_old'
        totals_value_col = 'BLy_new (MtCO2e)'
    else:
        totals_source_tab = 'BLy_and_E_orig_diffs'
        totals_value_col = 'BLy (MtCO2e)'

    totals_frames: list[pd.DataFrame] = []
    totals_skipped_reason: str | None = None
    for (sheet_id, label), config_name in zip(sheet_inputs_in_order, config_names):
        df_src = load_tabs_optional(sheet_id, [totals_source_tab], refresh=refresh)[
            totals_source_tab
        ]
        if df_src is None:
            totals_skipped_reason = (
                f"tab {totals_source_tab!r} not found in Sheet '{label}' ({sheet_id})"
            )
            break
        if any_useeio:
            totals = _aggregate_bly_new_vs_old_to_usa_row(
                df_src, sheet_label=label, sheet_id=sheet_id
            )
        else:
            if df_src.shape[1] < 5:
                raise ValueError(
                    f"Tab {totals_source_tab!r} in Sheet '{label}' "
                    f'({sheet_id}) must have at least 5 columns (to select '
                    f'B:E). Found {df_src.shape[1]} columns.'
                )
            totals = df_src.iloc[:, 1:5].copy()
        totals.insert(0, 'config_name', config_name)
        totals.insert(1, 'row_order', range(len(totals)))
        totals_frames.append(totals)

    output_tables: dict[str, pd.DataFrame] = {
        'D_and_diffs_merged': merged_d,
        'N_and_diffs_merged': merged_n,
    }

    if totals_skipped_reason is None:
        totals_merged = pd.concat(totals_frames, ignore_index=True)

        # Net differences for totals: BLy only, against the target mapping.
        totals_net_frames: list[pd.DataFrame] = []
        for cfg_name in config_names:
            if cfg_name not in target_mapping:
                raise KeyError(
                    'No target-column mapping for config_name='
                    f"{cfg_name!r}. Update the combo's target_mapping to include "
                    'all config_name values.'
                )

            target_cfg = target_mapping[cfg_name]
            # ``pinned_useeio_baseline`` is allowed in target_mapping for D/N
            # tabs but isn't a config column in the totals data. For USEEIO
            # combos this is fine: the same diff is already visible directly
            # in the totals row's ``BLy_new - BLy_old (MtCO2e)`` column
            # (since BLy_old IS the pinned baseline in USEEIO mode).
            if target_cfg == PINNED_USEEIO_BASELINE_COLUMN:
                logger.info(
                    'Skipping totals_net_diff for %r (targets %r); see the '
                    'BLy_new - BLy_old (MtCO2e) column of the totals row for '
                    'the same comparison.',
                    cfg_name,
                    target_cfg,
                )
                continue

            src = totals_merged[totals_merged['config_name'] == cfg_name].copy()
            tgt = totals_merged[totals_merged['config_name'] == target_cfg].copy()
            if tgt.empty:
                raise KeyError(
                    f'Target config_name {target_cfg!r} for {cfg_name!r} was not '
                    'found in totals_merged config_name values.'
                )

            out = pd.DataFrame(
                {
                    'config_name': cfg_name,
                    'target_config_name': target_cfg,
                    'row_order': src['row_order'],
                }
            )

            if totals_value_col not in src.columns:
                raise KeyError(
                    f'Expected column {totals_value_col!r} in totals data for '
                    f'{cfg_name!r}. Got: {list(src.columns)}'
                )
            src_bly = pd.to_numeric(src[totals_value_col], errors='coerce')

            merged_tot = src.merge(
                tgt[['row_order', totals_value_col]],
                on='row_order',
                how='left',
                suffixes=('', '__target'),
            )
            tgt_bly = pd.to_numeric(
                merged_tot[f'{totals_value_col}__target'], errors='coerce'
            )
            bly_src = np.array(src_bly, dtype=np.float64)
            bly_tgt = np.array(tgt_bly, dtype=np.float64)
            out[totals_value_col] = bly_src - bly_tgt

            totals_net_frames.append(out)

        totals_net_diff = pd.concat(totals_net_frames, ignore_index=True)

        totals_output = totals_merged.drop(columns=['row_order'])
        totals_net_output = totals_net_diff.drop(columns=['row_order'])

        output_tables['totals'] = totals_output
        output_tables['totals_net_diff'] = totals_net_output
    else:
        logger.warning(
            'Skipping totals / totals_net_diff output tabs: %s',
            totals_skipped_reason,
        )

    output_tables['D_net_diff'] = net_d
    output_tables['N_net_diff'] = net_n
    output_tables['config_summary_merged'] = config_merged

    if output_xlsx_path:
        os.makedirs(os.path.dirname(output_xlsx_path) or '.', exist_ok=True)
        with pd.ExcelWriter(output_xlsx_path, engine='openpyxl') as writer:
            for tab_name, df in output_tables.items():
                df.to_excel(writer, index=False, sheet_name=tab_name)
        logger.info('Wrote merged workbook: %s', output_xlsx_path)
    else:
        logger.info('Skipped local Excel output (output_xlsx_path is empty).')

    if output_sheet_id:
        for tab_name, df in output_tables.items():
            update_sheet_tab(output_sheet_id, tab_name, df, clean_nans=True)
        logger.info('Pushed merged tabs to Sheet %s', output_sheet_id)
    else:
        logger.info('Skipped Google Sheets push (output_sheet_id is empty).')

    return output_tables


def _resolve_combo(combo_name: str) -> ComboSpec:
    if combo_name not in COMBINATIONS:
        raise click.UsageError(
            f'Unknown combo {combo_name!r}. Registered: {sorted(COMBINATIONS)}.'
        )
    return COMBINATIONS[combo_name]


@click.command(
    help=(
        'Merge multiple EF diagnostics Google Sheets into one comparison '
        'workbook. Default writes a local .xlsx with all 7 comparison tabs; '
        'pass --output-sheet-id (or rely on the combo default) to also push '
        'the tabs to a Google Sheet.'
    )
)
@click.option(
    '--combo',
    'combo_name',
    required=True,
    type=click.Choice(list(COMBINATIONS)),
    help='Registered combination from combinations.COMBINATIONS.',
)
@click.option(
    '--refresh',
    is_flag=True,
    default=False,
    help='Force re-fetch from Google Sheets, overwriting the parquet cache.',
)
@click.option(
    '--output-xlsx',
    type=str,
    default=None,
    help=(
        'Local Excel workbook path. Defaults to '
        f'analysis/output/<combo>/{DEFAULT_OUTPUT_XLSX_NAME}. '
        'Pass an empty string to skip the local file.'
    ),
)
@click.option(
    '--output-sheet-id',
    type=str,
    default=None,
    help=(
        'Google Sheet ID for the merged output. When omitted, no Sheets '
        'push happens and only the local xlsx is written.'
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


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO, format='%(asctime)s %(name)s %(levelname)s %(message)s'
    )
    main()
