"""Step 3 of epic #337: cross-approach comparison at fixed target year (2024).

Reads the parquet caches produced by ``derive_A_time_series.py`` (Step 1) and
produces:

- ``pairwise_hexbins_{dom,imp}.png`` — 1×3 grid of log-log hexbin
  density plots between the three alternative approaches at 2024
  (``summary_tables`` vs ``industry_price_index``,
  ``summary_tables`` vs ``commodity_price_index``,
  ``industry_price_index`` vs ``commodity_price_index``). Hexbin density
  uses ``bins='log'`` because A cells span ~6 orders of magnitude and a
  linear color scale would be dominated by the near-zero peak. Step 2
  already covers alternative-vs-baseline scatters at 2024 (linear axes);
  this figure adds the alternative-vs-alternative angle that Step 2 can't
  show, on log-log axes that resolve the dense low-magnitude region.

- ``column_cap_audit.csv`` — every (year, dom_or_imp, col_sector)
  where the 0.98 column-cap inside ``scale_cornerstone_A`` fired on the
  ``summary_tables`` approach. Cap-fired detection: column sum within
  ``CAP_TOL`` of 0.98 after scaling. Columns just below 0.98 (sum > 0.97
  but cap not reached) are also written so reviewers can see how close
  the cap was to firing.

- Sheet tab ``column_cap_audit`` appended to the run-report Sheet
  (sheet ID read from ``last_run_sheet_id.txt``).

Usage:
    python -m bedrock.analysis.a_matrix_time_series.compare_approaches
"""

from __future__ import annotations

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from bedrock.analysis.a_matrix_time_series.constants import (
    LAST_RUN_SHEET_ID_PATH,
    LATEST_TARGET_YEAR,
    PLOTS_DIR,
    RESULTS_DIR,
)
from bedrock.utils.io.gcp import update_sheet_tab

logger = logging.getLogger(__name__)

TARGET_YEAR = LATEST_TARGET_YEAR

ALTERNATIVE_APPROACHES: tuple[str, ...] = (
    "summary_tables",
    "industry_price_index",
    "commodity_price_index",
)
BASELINE_APPROACHES: tuple[str, ...] = ("useeio", "ceda_default")
PAIRS: tuple[tuple[str, str], ...] = (
    ("summary_tables", "industry_price_index"),
    ("summary_tables", "commodity_price_index"),
    ("industry_price_index", "commodity_price_index"),
)
KINDS: tuple[str, ...] = ("dom", "imp")

# Column cap inside scale_cornerstone_A — re-stated here as a constant so the
# audit can test for it without importing the production helper.
COLUMN_CAP = 0.98
CAP_TOL = 1e-9
NEAR_CAP_THRESHOLD = 0.97  # report borderline columns too


def _load_pair(approach: str, year: int) -> dict[str, pd.DataFrame]:
    """Load the (Adom, Aimp) parquet for one (approach, year) into a dict."""
    combined = pd.read_parquet(RESULTS_DIR / f"A_{approach}_{year}.parquet")
    return {
        "dom": pd.DataFrame(combined.loc["dom"]),
        "imp": pd.DataFrame(combined.loc["imp"]),
    }


def _load_all_at_year(year: int) -> dict[str, dict[str, pd.DataFrame]]:
    """Returns ``{kind: {approach: A_matrix}}`` at ``year``.

    Only loads the alternative approaches — Step 3's hexbins compare
    alternatives against each other, not against baselines.
    """
    out: dict[str, dict[str, pd.DataFrame]] = {kind: {} for kind in KINDS}
    for approach in ALTERNATIVE_APPROACHES:
        pair = _load_pair(approach, year)
        for kind in KINDS:
            out[kind][approach] = pair[kind]
    return out


def _aligned_arrays(a: pd.DataFrame, b: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    """Reindex ``b`` to ``a``'s row/column order and return raveled values."""
    b_aligned = b.reindex(index=a.index, columns=a.columns)
    return a.to_numpy().ravel(), b_aligned.to_numpy().ravel()


# ---------------------------------------------------------------------------
# Pairwise hexbins
# ---------------------------------------------------------------------------


def plot_pairwise_hexbins(
    matrices: dict[str, dict[str, pd.DataFrame]], kind: str, path: Path
) -> None:
    """1×3 log-log hexbin density grid: pairs of alternative approaches.

    Cells where both values are zero (joint sparsity in A) are dropped — they
    inflate the density at the origin and obscure the disagreement story.
    Each panel reports n_cells, R², and the share of cells off the y=x
    diagonal by more than 1× (i.e. ratio outside [0.5, 2]).
    """
    fig, axes = plt.subplots(1, len(PAIRS), figsize=(5 * len(PAIRS), 5), squeeze=False)
    fig.suptitle(f"Pairwise A-matrix comparison — {kind} — {TARGET_YEAR}", fontsize=12)
    eps = 1e-12

    for ax_idx, (a_name, b_name) in enumerate(PAIRS):
        ax = axes[0][ax_idx]
        x, y = _aligned_arrays(matrices[kind][a_name], matrices[kind][b_name])
        mask = ~(np.isnan(x) | np.isnan(y)) & ~((x == 0) & (y == 0))
        x_pos = x[mask]
        y_pos = y[mask]
        if x_pos.size == 0:
            ax.text(0.5, 0.5, "no data", transform=ax.transAxes, ha="center")
            continue

        # Log-log because A spans many orders of magnitude. eps shifts joint
        # zero-on-one-side cells onto the plot rather than dropping them.
        log_x = np.log10(x_pos + eps)
        log_y = np.log10(y_pos + eps)
        hb = ax.hexbin(log_x, log_y, gridsize=60, bins="log", cmap="viridis")
        cbar = fig.colorbar(hb, ax=ax)
        cbar.set_label("log10(count)", fontsize=9)

        lo = float(min(log_x.min(), log_y.min()))
        hi = float(max(log_x.max(), log_y.max()))
        ax.plot([lo, hi], [lo, hi], "r--", lw=0.8, alpha=0.8, label="y=x")
        ax.set_xlim(lo, hi)
        ax.set_ylim(lo, hi)
        ax.set_aspect("equal", adjustable="box")
        ax.set_xlabel(f"log10({a_name})")
        ax.set_ylabel(f"log10({b_name})")
        ax.set_title(f"{a_name} vs {b_name}", fontsize=10)
        ax.grid(True, alpha=0.3)

        # Stats: R² in original (non-log) space; off-diagonal share = cells
        # where ratio is outside [0.5, 2] (i.e. one side ≥2× the other).
        x_f = np.asarray(x_pos, dtype=float)
        y_f = np.asarray(y_pos, dtype=float)
        r2 = (
            float(np.corrcoef(x_f, y_f)[0, 1] ** 2)
            if x_f.std() > 0 and y_f.std() > 0
            else float("nan")
        )
        denom = np.where(np.abs(x_f) > 0, np.abs(x_f), np.nan)
        ratio = np.abs(y_f / denom)
        valid = ~np.isnan(ratio)
        off_diag_share = (
            float(((ratio[valid] < 0.5) | (ratio[valid] > 2.0)).mean())
            if valid.any()
            else float("nan")
        )
        stats_text = (
            f"n = {x_pos.size:,}\n"
            f"R² = {r2:.4f}\n"
            f"|y/x| outside [0.5,2]: {off_diag_share:.1%}"
        )
        ax.text(
            0.02,
            0.98,
            stats_text,
            transform=ax.transAxes,
            va="top",
            ha="left",
            fontsize=9,
            family="monospace",
            bbox={
                "boxstyle": "round,pad=0.3",
                "facecolor": "white",
                "alpha": 0.85,
                "edgecolor": "gray",
            },
        )

    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


# ---------------------------------------------------------------------------
# Column-cap audit (summary_tables only, all years)
# ---------------------------------------------------------------------------


def column_cap_audit(years: list[int]) -> pd.DataFrame:
    """For each (year, kind) of ``summary_tables``, list every column whose
    sum is at the 0.98 cap or close to it.

    The cap fires inside ``scale_cornerstone_A`` whenever the post-scaling
    column sum exceeds 1; the column is then rescaled to exactly 0.98. So
    "cap fired" ⇔ column sum within ``CAP_TOL`` of 0.98 after scaling.
    Columns just below the cap (sum > 0.97) are also reported so reviewers
    can see how close the cap was to engaging.
    """
    rows: list[dict[str, object]] = []
    for year in years:
        pair = _load_pair("summary_tables", year)
        for kind in KINDS:
            col_sum = pair[kind].sum(axis=0)
            for col, val in col_sum.items():
                cap_fired = abs(val - COLUMN_CAP) <= CAP_TOL
                if cap_fired or float(val) > NEAR_CAP_THRESHOLD:
                    rows.append(
                        {
                            "year": year,
                            "dom_or_imp": kind,
                            "col_sector": col,
                            "col_sum": float(val),
                            "cap_fired": bool(cap_fired),
                        }
                    )
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(
            ["year", "dom_or_imp", "cap_fired", "col_sum"],
            ascending=[True, True, False, False],
        ).reset_index(drop=True)
    return df


# ---------------------------------------------------------------------------
# Sheet publish
# ---------------------------------------------------------------------------


def _publish_cap_audit_tab(cap_audit_df: pd.DataFrame) -> None:
    """Append the column-cap audit tab to the run-report Sheet, if available."""
    if not LAST_RUN_SHEET_ID_PATH.exists():
        logger.warning(
            "No %s found — skipping Sheet publish. Run derive_A_time_series "
            "first (with valid Drive auth) to create the run report.",
            LAST_RUN_SHEET_ID_PATH,
        )
        return
    sheet_id = LAST_RUN_SHEET_ID_PATH.read_text().strip()
    try:
        update_sheet_tab(sheet_id, "column_cap_audit", cap_audit_df)
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "Sheet publish skipped (%s: %s). Local artifacts still complete.",
            type(e).__name__,
            e,
        )
        return
    logger.info("Updated column_cap_audit tab on sheet %s", sheet_id)


def main() -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("Loading matrices for %d", TARGET_YEAR)
    matrices = _load_all_at_year(TARGET_YEAR)

    for kind in KINDS:
        plot_pairwise_hexbins(
            matrices, kind, PLOTS_DIR / f"pairwise_hexbins_{kind}.png"
        )

    # Cap audit spans all available summary_tables years to give a full picture
    # of how often the 0.98 cap engages, not just at the focus year.
    audit_years = sorted(
        int(p.stem.rsplit("_", 1)[-1])
        for p in RESULTS_DIR.glob("A_summary_tables_*.parquet")
    )
    cap_audit_df = column_cap_audit(audit_years)
    cap_audit_df.to_csv(RESULTS_DIR / "column_cap_audit.csv", index=False)

    _publish_cap_audit_tab(cap_audit_df)
    logger.info("Step 3 outputs written to %s and %s", RESULTS_DIR, PLOTS_DIR)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
