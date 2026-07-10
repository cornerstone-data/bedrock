"""Percent-diff loaders for release-progression EF histogram panels."""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from bedrock.utils.validation.analysis.diagnostics_plots import (
    _drop_old_only,
    _normalize_schema,
)
from bedrock.utils.validation.analysis.ef_hist_panels import pct_values
from bedrock.utils.validation.analysis.fetch import load_tab

logger = logging.getLogger(__name__)

KIND_TAB = {"N": "N_and_diffs", "D": "D_and_diffs"}

# IO@2023 / GHG@2024 — cross-year inflation does not apply (plot-ef-diagnostics).
_SKIP_INFLATION_CONFIG_SUBSTRINGS = ("umd_2024_ghgia",)


def _config_name(sheet_id: str) -> str:
    cs = load_tab(sheet_id, "config_summary")
    mapping = dict(zip(cs["config_field"], cs["value"], strict=False))
    return str(mapping.get("config_name", "")).strip()


def _model_base_year(sheet_id: str) -> int:
    cs = load_tab(sheet_id, "config_summary")
    mapping = dict(zip(cs["config_field"], cs["value"], strict=False))
    for key in ("model_base_year", "usa_io_data_year"):
        raw = mapping.get(key)
        if raw is not None and str(raw).strip():
            return int(float(raw))
    return 0


def _tab_frame(sheet_id: str, ef_kind: str) -> pd.DataFrame:
    return _drop_old_only(_normalize_schema(load_tab(sheet_id, KIND_TAB[ef_kind])))


def pct_fractions_vs_v0(sheet_id: str, ef_kind: str) -> np.ndarray:
    """In-sheet % diff from ``{kind}_perc_diff`` (or absolute columns as fallback).

    On USEEIO-baseline sheets with purchaser columns, ``N_perc_diff`` compares
    purchaser price, not producer. Use :func:`pct_fractions_producer_vs_old`
    when the run targets producer footing.
    """
    return pct_values(_tab_frame(sheet_id, ef_kind), ef_kind)


def pct_fractions_producer_vs_old(sheet_id: str, ef_kind: str) -> np.ndarray:
    """In-sheet producer % diff: ``{kind}_new[_inflated]`` vs ``{kind}_old_inflated``.

    Ignores ``{kind}_perc_diff``, which on USEEIO-baseline sheets may reflect
    purchaser price when ``N_new_purchaser`` is emitted.
    """
    df = _tab_frame(sheet_id, ef_kind)
    inflated_col = f"{ef_kind}_new_inflated"
    new_col = f"{ef_kind}_new"
    old_col = f"{ef_kind}_old_inflated"
    if (
        inflated_col in df.columns
        and pd.to_numeric(df[inflated_col], errors="coerce").notna().any()
    ):
        new = pd.to_numeric(df[inflated_col], errors="coerce")
    else:
        new = pd.to_numeric(df[new_col], errors="coerce")
    old = pd.to_numeric(df[old_col], errors="coerce")
    pdiff = (new - old) / old.abs()
    arr = pdiff.dropna().to_numpy(dtype=float)
    return arr[np.isfinite(arr)]


def pct_fractions_useeio_purchaser_vs_v0(sheet_id: str) -> np.ndarray:
    """Purchaser-price N % diff vs pinned USEEIO on a USEEIO-baseline sheet."""
    df = _tab_frame(sheet_id, "N")
    if "N_new_purchaser" in df.columns and df["N_new_purchaser"].notna().any():
        num = pd.to_numeric(df["N_new_purchaser"], errors="coerce")
        den = pd.to_numeric(df["N_old_purchaser"], errors="coerce")
        return ((num - den) / den.abs()).dropna().to_numpy(dtype=float)
    logger.warning(
        "Sheet %s has no usable N_new_purchaser; falling back to producer N vs v0.",
        sheet_id,
    )
    return pct_fractions_vs_v0(sheet_id, "N")


def _inflation_ratio_2023_to_2024(
    step_sheet_id: str,
    ref_2023_sheet_id: str,
    ef_kind: str,
) -> pd.Series:
    """Per-sector ``N_old_inflated`` ratio (2024 sheet / 2023 sheet)."""
    old_col = f"{ef_kind}_old_inflated"
    step = _tab_frame(step_sheet_id, ef_kind).set_index("sector")
    ref = _tab_frame(ref_2023_sheet_id, ef_kind).set_index("sector")
    step_old = pd.to_numeric(step[old_col], errors="coerce")
    ref_old = pd.to_numeric(ref[old_col], errors="coerce")
    return step_old / ref_old


def _skip_inflation(config_name: str) -> bool:
    return any(s in config_name for s in _SKIP_INFLATION_CONFIG_SUBSTRINGS)


def _absolute_ef_column(
    sheet_id: str,
    ef_kind: str,
    *,
    prefer_purchaser: bool = False,
) -> str:
    """Return the absolute EF column to read from a diagnostics tab."""
    if prefer_purchaser and ef_kind == "N":
        df = _tab_frame(sheet_id, ef_kind)
        if "N_new_purchaser" in df.columns and df["N_new_purchaser"].notna().any():
            return "N_new_purchaser"
        logger.warning(
            "Sheet %s has no usable N_new_purchaser; falling back to N_new.",
            sheet_id,
        )
    return f"{ef_kind}_new"


def pct_fractions_vs_baseline_sheet(
    step_sheet_id: str,
    baseline_sheet_id: str,
    ef_kind: str,
    *,
    value_col: str | None = None,
    ref_2023_sheet_id: str | None,
    baseline_year: int,
    prefer_purchaser: bool = False,
) -> tuple[np.ndarray, bool]:
    """Cross-sheet % diff vs another run's absolute EF column.

    Returns ``(fractions, inflation_applied)``. When the step is a 2024-model
    run and the baseline is 2023, the baseline EF is inflated per-sector using
    ``{kind}_old_inflated`` on the step sheet vs ``ref_2023_sheet_id``.
    """
    step_col = value_col or _absolute_ef_column(
        step_sheet_id, ef_kind, prefer_purchaser=prefer_purchaser
    )
    base_col = value_col or _absolute_ef_column(
        baseline_sheet_id, ef_kind, prefer_purchaser=prefer_purchaser
    )
    cfg = _tab_frame(step_sheet_id, ef_kind)
    base = _tab_frame(baseline_sheet_id, ef_kind)
    cfg_vals = cfg[["sector", "sector_name", step_col]].copy()
    cfg_vals[step_col] = pd.to_numeric(cfg_vals[step_col], errors="coerce")
    base_vals = base[["sector", base_col]].copy()
    base_vals[base_col] = pd.to_numeric(base_vals[base_col], errors="coerce")
    base_vals = base_vals.rename(columns={base_col: "_base"})

    merged = cfg_vals.merge(base_vals, on="sector", how="inner")
    config_name = _config_name(step_sheet_id)
    inflation_applied = False
    step_year = _model_base_year(step_sheet_id)

    if (
        step_year == 2024
        and baseline_year == 2023
        and not _skip_inflation(config_name)
        and ref_2023_sheet_id is not None
    ):
        ratio = _inflation_ratio_2023_to_2024(step_sheet_id, ref_2023_sheet_id, ef_kind)
        merged = merged.set_index("sector")
        merged["_base"] = merged["_base"] * ratio.reindex(merged.index)
        merged = merged.reset_index()
        inflation_applied = True

    pdiff = (merged[step_col] - merged["_base"]) / merged["_base"].abs()
    arr = pdiff.dropna().to_numpy(dtype=float)
    return arr[np.isfinite(arr)], inflation_applied


def pct_fractions_stacked_group(
    step_sheet_id: str,
    prior_sheet_id: str | None,
    ef_kind: str,
    *,
    ref_2023_sheet_id: str | None,
    prefer_purchaser: bool = False,
) -> tuple[np.ndarray, bool]:
    """Marginal % diff for one stacked group vs its prior endpoint.

    When ``prior_sheet_id`` is ``None`` (G1), returns in-sheet diff vs CEDA v0
    or USEEIO purchaser baseline. Otherwise cross-sheet diff vs the prior
    group absolute EF column, with 2023→2024 inflation when applicable.
    """
    if prior_sheet_id is None:
        if prefer_purchaser and ef_kind == "N":
            return pct_fractions_useeio_purchaser_vs_v0(step_sheet_id), False
        return pct_fractions_producer_vs_old(step_sheet_id, ef_kind), False
    baseline_year = _model_base_year(prior_sheet_id)
    return pct_fractions_vs_baseline_sheet(
        step_sheet_id,
        prior_sheet_id,
        ef_kind,
        ref_2023_sheet_id=ref_2023_sheet_id,
        baseline_year=baseline_year,
        prefer_purchaser=prefer_purchaser,
    )
