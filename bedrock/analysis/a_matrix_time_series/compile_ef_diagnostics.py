"""Compile per-(approach, baseline) EF diagnostics Sheets into one workbook.

Step 6 / Phase 2 of epic #337. Reads ``output/results/ef_run_index.csv`` for
the ``(approach, baseline, sheet_id)`` triples, pulls the EF diff tabs from
each Sheet, and produces:

- ``output/results/ef_comparison.xlsx``:
    - One tab per ``(approach, baseline)`` carrying the full ``N_and_diffs``
      joined with ``D_and_diffs``.
    - ``summary_vs_useeio`` and ``summary_vs_ceda``: per-approach roll-up
      with p50 / p95 / max of ``|N_perc_diff|`` and ``|D_perc_diff|``, plus
      ``n_significant`` (sectors where the percent diff exceeds
      ``SIGNIFICANT_PCT_THRESHOLD``).
- ``output/results/ef_scatter_coords.parquet``: long-format coordinates
  ``(approach, baseline, ef_kind, sector, x_baseline, y_approach)`` for the
  Phase 3 scatter plots. ``x_baseline`` is ``*_old_inflated`` (the
  baseline's EF, inflation-adjusted to the candidate's base year) and
  ``y_approach`` is ``*_new``.
- ``ef_summary_vs_useeio`` and ``ef_summary_vs_ceda`` tabs appended to the
  run-report Sheet (sheet ID from ``last_run_sheet_id.txt``, written by
  Step 1). Skipped with a warning if that file is missing.

The compile script reads only ``sheet_id`` from ``ef_run_index.csv``;
``run_id``, ``useeio_box_ticked`` and ``triggered_at`` are audit-only.

Usage:
    python -m bedrock.analysis.a_matrix_time_series.compile_ef_diagnostics
"""

from __future__ import annotations

import logging

import pandas as pd

from bedrock.analysis.a_matrix_time_series.constants import (
    LAST_RUN_SHEET_ID_PATH,
    PLOTS_DIR,
    RESULTS_DIR,
)
from bedrock.utils.io.gcp import read_sheet_tab, update_sheet_tab
from bedrock.utils.validation.diagnostics_helpers import (
    inflation_adjust_ef_denom_to_new_base_year,
)

logger = logging.getLogger(__name__)

EF_RUN_INDEX_PATH = RESULTS_DIR / "ef_run_index.csv"
EF_COMPARISON_XLSX_PATH = RESULTS_DIR / "ef_comparison.xlsx"
EF_SCATTER_COORDS_PATH = RESULTS_DIR / "ef_scatter_coords.parquet"

# A row counts as "significantly different" when |perc_diff| exceeds this.
SIGNIFICANT_PCT_THRESHOLD = 0.10

# Time-series cells differ in `model_base_year`, so each cell's `D_new` /
# `N_new` lives in its own dollar year. Deflate them to this common reference
# so values are commensurable across years.
REFERENCE_DOLLAR_YEAR = 2023

# Tab names produced by `calculate_ef_diagnostics.py` per run. Both
# baseline modes (CEDA-only and USEEIO-checked) emit identical column
# headers — confirmed via spot-check.
TAB_N = "N_and_diffs"
TAB_D = "D_and_diffs"

# Numeric columns to coerce after `read_sheet_tab` (which returns all str).
_N_NUMERIC_COLS = ("N_new", "N_old", "N_old_inflated", "N_perc_diff")
_D_NUMERIC_COLS = ("D_new", "D_old", "D_old_inflated", "D_perc_diff")


def _coerce_numeric(df: pd.DataFrame, cols: tuple[str, ...]) -> pd.DataFrame:
    """Coerce string columns to numeric, handling percent-formatted cells.

    Sheets returns formatted display values by default, so a column with
    percent cell formatting comes back like ``"0.23%"`` even though the
    underlying value is the fraction ``0.0023``. Strip the suffix and
    divide by 100; raw-float columns pass through unchanged.
    """
    for col in cols:
        if col not in df.columns:
            continue
        s = df[col].astype(str).str.strip()
        is_pct = s.str.endswith("%")
        cleaned = s.str.rstrip("%").str.replace(",", "", regex=False)
        numeric = pd.to_numeric(cleaned, errors="coerce")
        df[col] = numeric.mask(is_pct, numeric / 100)
    return df


def _read_pair(sheet_id: str) -> pd.DataFrame:
    """Return ``N_and_diffs`` joined with ``D_and_diffs`` on the sector index."""
    n = _coerce_numeric(read_sheet_tab(sheet_id, TAB_N), _N_NUMERIC_COLS)
    d = _coerce_numeric(read_sheet_tab(sheet_id, TAB_D), _D_NUMERIC_COLS)
    # Both tabs come back with the sector code as the first column; align on it.
    sector_col = n.columns[0]
    n = n.set_index(sector_col)
    d = d.set_index(sector_col)
    # `sector_name` and `comparison_type` are duplicated across N and D — keep
    # them from N only.
    drop_from_d = [c for c in ("sector_name", "comparison_type") if c in d.columns]
    return n.join(d.drop(columns=drop_from_d), how="outer")


def _deflate_new_to_ref(
    joined: pd.DataFrame, source_year: int, ref_year: int
) -> pd.DataFrame:
    """Add ``D_new_ref`` / ``N_new_ref`` columns deflated to ``ref_year`` dollars.

    Each diagnostics cell is run with ``model_base_year=source_year``, so
    ``D_new`` / ``N_new`` are denominated in ``source_year`` dollars. Time-series
    plots over years require a common dollar reference; this multiplies by the
    same per-sector price ratio used for baseline alignment.
    """
    if source_year == ref_year:
        if "D_new" in joined.columns:
            joined["D_new_ref"] = joined["D_new"]
        if "N_new" in joined.columns:
            joined["N_new_ref"] = joined["N_new"]
        return joined
    for new_col, ref_col in (("D_new", "D_new_ref"), ("N_new", "N_new_ref")):
        if new_col not in joined.columns:
            continue
        joined[ref_col] = inflation_adjust_ef_denom_to_new_base_year(
            old_ef_vector=joined[new_col].astype(float),
            new_base_year=ref_year,
            old_base_year=source_year,
        )
    return joined


def _summarize(joined: pd.DataFrame, approach: str) -> pd.Series:
    n_perc = joined["N_perc_diff"].abs()
    d_perc = joined["D_perc_diff"].abs()
    return pd.Series(
        {
            "approach": approach,
            "n_sectors": int(joined.shape[0]),
            "N_p50": float(n_perc.quantile(0.50)),
            "N_p95": float(n_perc.quantile(0.95)),
            "N_max": float(n_perc.max()),
            "N_n_significant": int((n_perc > SIGNIFICANT_PCT_THRESHOLD).sum()),
            "D_p50": float(d_perc.quantile(0.50)),
            "D_p95": float(d_perc.quantile(0.95)),
            "D_max": float(d_perc.max()),
            "D_n_significant": int((d_perc > SIGNIFICANT_PCT_THRESHOLD).sum()),
        }
    )


def _scatter_coords(joined: pd.DataFrame, approach: str, baseline: str) -> pd.DataFrame:
    """Long-format `(approach, baseline, ef_kind, sector, x_baseline, y_approach)`."""
    rows: list[pd.DataFrame] = []
    for kind, new_col, old_col in (
        ("N", "N_new", "N_old_inflated"),
        ("D", "D_new", "D_old_inflated"),
    ):
        if new_col not in joined.columns or old_col not in joined.columns:
            continue
        chunk = joined[[new_col, old_col]].dropna().copy()
        chunk.columns = pd.Index(["y_approach", "x_baseline"])
        chunk = chunk.reset_index().rename(columns={chunk.index.name: "sector"})
        chunk["approach"] = approach
        chunk["baseline"] = baseline
        chunk["ef_kind"] = kind
        rows.append(chunk)
    if not rows:
        return pd.DataFrame()
    combined = pd.concat(rows, ignore_index=True)
    return combined.loc[
        :,
        ["approach", "baseline", "ef_kind", "sector", "x_baseline", "y_approach"],
    ]


def main() -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    if not EF_RUN_INDEX_PATH.exists():
        raise FileNotFoundError(
            f"{EF_RUN_INDEX_PATH} not found.\n"
            "Two ways to populate it:\n"
            "  (a) Auto-rebuild from existing Sheets in the diagnostics "
            "Drive folder:\n"
            "      python -m bedrock.analysis.a_matrix_time_series."
            "rebuild_run_index_from_drive --folder-id <DRIVE_FOLDER_ID>\n"
            "  (b) Hand-write a CSV with at least 3 columns "
            "(approach, baseline, sheet_id), one row per Sheet."
        )
    index_df = pd.read_csv(EF_RUN_INDEX_PATH)
    required = {"approach", "baseline", "sheet_id"}
    missing = required - set(index_df.columns)
    if missing:
        raise ValueError(f"{EF_RUN_INDEX_PATH} missing columns: {sorted(missing)}")

    # `scenario` and `year` are optional. Step 6 runs lack them; Step 7
    # time-series dispatch populates both. Default to empty so the same
    # script handles both schemas.
    if "scenario" not in index_df.columns:
        index_df["scenario"] = ""
    if "year" not in index_df.columns:
        index_df["year"] = ""

    summaries_by_baseline: dict[str, list[pd.Series]] = {}
    scatter_chunks: list[pd.DataFrame] = []
    per_pair_tables: dict[str, pd.DataFrame] = {}

    for _, row in index_df.iterrows():
        approach = str(row["approach"])
        baseline = str(row["baseline"])
        sheet_id = str(row["sheet_id"])
        scenario = str(row["scenario"]) if pd.notna(row["scenario"]) else ""
        year = str(row["year"]) if pd.notna(row["year"]) else ""
        cell_label = ", ".join(
            f"{k}={v}"
            for k, v in (
                ("scenario", scenario),
                ("approach", approach),
                ("year", year),
                ("baseline", baseline),
            )
            if v
        )
        logger.info("Pulling tabs for %s", cell_label)
        joined = _read_pair(sheet_id)
        if joined.empty:
            logger.warning("%s returned empty data; skipping", cell_label)
            continue
        if year:
            joined = _deflate_new_to_ref(
                joined, source_year=int(float(year)), ref_year=REFERENCE_DOLLAR_YEAR
            )
        # Build a deterministic 31-char-bounded tab name including any
        # populated scenario/year prefix.
        prefix = "_".join(p for p in (scenario, year) if p)
        pair_key = (
            f"{prefix}_{approach}__vs_{baseline}"
            if prefix
            else f"{approach}__vs_{baseline}"
        )
        per_pair_tables[pair_key[:31]] = joined.reset_index()

        summary_row = _summarize(joined, approach)
        # Stamp the optional dimensions onto the row so the summary tab is
        # navigable in time-series mode.
        if scenario:
            summary_row["scenario"] = scenario
        if year:
            summary_row["year"] = year
        summaries_by_baseline.setdefault(baseline, []).append(summary_row)
        scatter_chunks.append(_scatter_coords(joined, approach, baseline))

    summaries: dict[str, pd.DataFrame] = {
        baseline: pd.DataFrame(rows).set_index("approach")
        for baseline, rows in summaries_by_baseline.items()
    }

    with pd.ExcelWriter(EF_COMPARISON_XLSX_PATH, engine="openpyxl") as writer:
        for baseline, summary in summaries.items():
            summary.reset_index().to_excel(
                writer, sheet_name=f"summary_vs_{baseline}", index=False
            )
        for tab, df in per_pair_tables.items():
            df.to_excel(writer, sheet_name=tab[:31], index=False)
    logger.info("Wrote %s", EF_COMPARISON_XLSX_PATH)

    if scatter_chunks:
        all_coords = pd.concat(scatter_chunks, ignore_index=True)
        all_coords.to_parquet(EF_SCATTER_COORDS_PATH)
        logger.info("Wrote %s (%d rows)", EF_SCATTER_COORDS_PATH, len(all_coords))

    if LAST_RUN_SHEET_ID_PATH.exists():
        run_sheet_id = LAST_RUN_SHEET_ID_PATH.read_text().strip()
        for baseline, summary in summaries.items():
            update_sheet_tab(
                run_sheet_id,
                f"ef_summary_vs_{baseline}",
                summary.reset_index(),
                clean_nans=True,
            )
        logger.info("Appended ef_summary_vs_* tabs to run Sheet %s", run_sheet_id)
    else:
        logger.warning(
            "%s not found — skipping run-report Sheet upload",
            LAST_RUN_SHEET_ID_PATH,
        )

    for baseline, summary in summaries.items():
        print(f"\n=== summary_vs_{baseline} ===")
        print(summary.round(4).to_string())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()
