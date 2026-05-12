"""Phase A.2 — Cell-level LMDI decomposition of summary_tables vs PI.

Reads A snapshots produced by ``derive_A_snapshots.py``. For each
A-matrix cell (i, j, kind ∈ {dom, imp}) and each year-transition, computes:

- ``Q_phys_ij(y)  = A_summary_ij(y)  / A_pi_ij(y)``  — physical-shift multiplier
- ``phys_ij(y)    = ln Q_phys_ij(y)  − ln Q_phys_ij(y-1)``  — YoY physical effect

Symbols:

- ``A_summary_ij(y)``: the 2017 detail-benchmark coefficient ``A_2017_ij``
  rescaled to year ``y`` using BEA summary-table ratios — carries BOTH price
  and physical change observed in the summary tables. Sourced from
  ``2025_usa_cornerstone_A_summary_tables``.
- ``A_pi_ij(y) = (p_i(y) / p_j(y)) · A_2017_ij``: the same ``A_2017``
  rescaled to year ``y`` using ONLY commodity prices, i.e., the sandwich
  ``diag(p(y)) · A_2017 · diag(1/p(y))``. Because both diagonals are
  diagonal, this collapses to per-cell scalar reweighting: ``p_i`` inflates
  the numerator of ``A_ij = z_ij / x_j`` (i-commodity dollars), ``1/p_j``
  inflates the denominator (j-output dollars). Quantities are held fixed
  at 2017, so ``A_pi`` is the price-only counterfactual. Sourced from
  ``2025_usa_cornerstone_A_commodity_price_index`` (V-norm-weighted
  commodity PI; see ``signal_noise_plan.md`` on why commodity PI rather
  than industry PI).
- ``Q_phys_ij(y) = A_summary / A_pi``: the residual after stripping the
  price channel. Equivalent to ``A_real(y) / A_2017`` — the multiplicative
  change in the *price-deflated* input coefficient since 2017. Both
  numerator and denominator of A are implicitly deflated by the division,
  because ``A_pi`` carries both inflation factors and dividing cancels
  them at once. Anchored at ``Q_phys(2017) ≡ 1`` because
  ``p_i(2017) = p_j(2017) = 1``.

Algebraic identity: ``ln(A_summary_ij(y)) − ln(A_summary_ij(y-1))`` decomposes
exactly into ``[ln(A_pi(y)) − ln(A_pi(y-1))]`` (price effect, captured by PI)
plus ``[ln(Q_phys(y)) − ln(Q_phys(y-1))]`` (physical effect). The latter is
what ``summary_tables`` adds beyond PI.

Aggregation: LMDI logarithmic-mean weights yield exact additive decomposition
of column-sum changes. For each output sector j and transition (y-1, y):

    Δ_phys_j(y) = Σ_i  L(A_sum_ij(y), A_sum_ij(y-1)) · ln(Q_phys_ij(y) / Q_phys_ij(y-1))

where ``L(a, b) = (a − b) / ln(a/b)`` is the logarithmic mean (and
``L(a, a) = a``, ``L(0, ·) = 0`` by convention).

NAICS-3 aggregation: BEA detail codes are NAICS-based; the first 3 characters
are the NAICS-3 group (codes with letters like ``2122A0`` still have a clean
3-digit prefix). Sum cell-level physical effects within each NAICS-3 of the
output sector j.

Outputs:
- ``output/results/lmdi_phys_cells.parquet`` — per (i, j, kind, transition).
- ``output/results/lmdi_phys_by_output_sector.csv`` — aggregated to output j.
- ``output/results/lmdi_phys_by_naics3.csv`` — aggregated to NAICS-3 of j.

Usage:
    python -m bedrock.analysis.a_matrix_time_series.signal_noise.compute_lmdi_phys
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from bedrock.analysis.a_matrix_time_series.constants import RESULTS_DIR

logger = logging.getLogger(__name__)

A_SNAPSHOTS_DIR = RESULTS_DIR / "A_snapshots"
CELLS_OUT_PATH = RESULTS_DIR / "lmdi_phys_cells.parquet"
BY_OUTPUT_SECTOR_PATH = RESULTS_DIR / "lmdi_phys_by_output_sector.csv"
BY_NAICS3_PATH = RESULTS_DIR / "lmdi_phys_by_naics3.csv"

YEARS: tuple[int, ...] = (2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024)
TRANSITIONS: tuple[tuple[int, int], ...] = tuple(
    (YEARS[i], YEARS[i + 1]) for i in range(len(YEARS) - 1)
)
# Year-gap per transition. 2017→2019 spans 2 years (BEA doesn't support 2018);
# all others span 1 year. Useful for annualizing the log-effect in Phase B.
TRANSITION_YEAR_GAP: dict[str, int] = {f"{a}->{b}": (b - a) for a, b in TRANSITIONS}

# A cell is "active" if both A_summary and A_pi exceed this threshold in
# both endpoints of a transition. Cells below this are effectively zero in
# the 2017 base and stay zero under both scaling methods — we drop them to
# avoid log-of-near-zero blowups.
ACTIVE_THRESHOLD = 1e-12

# BEA "special" output codes that aren't real producing industries — they're
# I-O accounting balances (noncomparable imports, used/secondhand goods,
# inventory adjustments) or government enterprises. Physical-shift framing
# doesn't apply, so we exclude them from the j side. (S00102 also has a
# small mismatch between scale_cornerstone_A and inflate_..._with_commodity_pi
# at year=2017 that violates the Q_phys(2017)=1 anchor on 100 cells; filtering
# it out resolves that too.)
EXCLUDED_J_CODES: frozenset[str] = frozenset(
    {
        "S00102",  # Noncomparable imports / rest-of-world adjustment
        "S00203",  # Used and secondhand goods
        "S00500",  # Scrap
        "S00600",  # Inventory valuation adjustment
        "GSLGE",  # State & local government enterprises (education)
        "GSLGH",  # State & local government enterprises (hospital)
        "GSLGO",  # State & local government enterprises (other)
    }
)


def _load_method_year(method: str, year: int) -> pd.DataFrame:
    path = A_SNAPSHOTS_DIR / f"{method}_{year}_A.parquet"
    df = pd.read_parquet(path)
    df["year"] = year
    df["method"] = method
    return df


def _build_paired_panel() -> pd.DataFrame:
    """Return long panel keyed by (i, j, kind, year) with columns
    ``A_summary`` and ``A_pi`` side-by-side.

    ``A_pi`` is sourced from the ``commodity_price_index`` method — the
    V-norm-weighted commodity-space PI — so residual price motion in
    volatile commodity sectors does not leak into ``Q_phys``.
    """
    summary_frames = [_load_method_year("summary_tables", y) for y in YEARS]
    pi_frames = [_load_method_year("commodity_price_index", y) for y in YEARS]
    sum_df = pd.concat(summary_frames, ignore_index=True).rename(
        columns={"value": "A_summary"}
    )[["i", "j", "kind", "year", "A_summary"]]
    pi_df = pd.concat(pi_frames, ignore_index=True).rename(columns={"value": "A_pi"})[
        ["i", "j", "kind", "year", "A_pi"]
    ]
    merged = sum_df.merge(pi_df, on=["i", "j", "kind", "year"], how="inner")
    # Drop BEA-special output codes (accounting balances, government enterprises)
    # — not real producing industries.
    merged = merged[~merged["j"].isin(list(EXCLUDED_J_CODES))].reset_index(drop=True)
    return merged


def _logmean(a: pd.Series, b: pd.Series) -> pd.Series:
    """Logarithmic mean L(a, b) = (a - b) / ln(a/b), with L(a, a) = a and
    L(0, ·) or L(·, 0) = 0. Returns 0 where either input is non-positive."""
    a_arr = a.to_numpy(dtype=float)
    b_arr = b.to_numpy(dtype=float)
    out = np.zeros_like(a_arr)
    eq = np.isclose(a_arr, b_arr)
    pos = (a_arr > 0) & (b_arr > 0) & ~eq
    out[eq] = a_arr[eq]
    with np.errstate(divide="ignore", invalid="ignore"):
        out[pos] = (a_arr[pos] - b_arr[pos]) / np.log(a_arr[pos] / b_arr[pos])
    return pd.Series(out, index=a.index)


def _compute_cell_effects(panel: pd.DataFrame) -> pd.DataFrame:
    """Cell-level Q_phys and YoY physical effect per transition.

    Vectorized over the full panel via groupby+shift: each cell's row for
    year ``y`` carries the prior present year's values as ``*_prev``.
    Works for arbitrary year gaps (shift takes the previous present row).
    """
    p = panel.sort_values(["i", "j", "kind", "year"], kind="mergesort")
    grp = p.groupby(["i", "j", "kind"], sort=False)
    df = p.assign(
        A_sum_prev=grp["A_summary"].shift(1),
        A_pi_prev=grp["A_pi"].shift(1),
        y_prev=grp["year"].shift(1),
    ).rename(columns={"A_summary": "A_sum_curr", "A_pi": "A_pi_curr", "year": "y_curr"})
    df = df.dropna(subset=["A_sum_prev"])
    active = (
        (df["A_sum_prev"] > ACTIVE_THRESHOLD)
        & (df["A_sum_curr"] > ACTIVE_THRESHOLD)
        & (df["A_pi_prev"] > ACTIVE_THRESHOLD)
        & (df["A_pi_curr"] > ACTIVE_THRESHOLD)
    )
    df = df.loc[active].copy()
    df["y_prev"] = df["y_prev"].astype(int)
    df["year_gap"] = df["y_curr"] - df["y_prev"]
    df["transition"] = df["y_prev"].astype(str) + "->" + df["y_curr"].astype(str)
    df["Q_phys_prev"] = df["A_sum_prev"] / df["A_pi_prev"]
    df["Q_phys_curr"] = df["A_sum_curr"] / df["A_pi_curr"]
    df["ln_phys_delta"] = np.log(df["Q_phys_curr"] / df["Q_phys_prev"])
    # Annualized so multi-year transitions (e.g. 2017→2019) stay comparable
    # to single-year ones.
    df["ln_phys_delta_annual"] = df["ln_phys_delta"] / df["year_gap"]
    df["lmdi_weight"] = _logmean(df["A_sum_prev"], df["A_sum_curr"])
    return df.reset_index(drop=True)


def _aggregate_by_output_sector(cells: pd.DataFrame) -> pd.DataFrame:
    """LMDI-weighted column-sum decomposition per output sector j.

    For each (j, transition, kind), compute the additive physical-effect
    contribution to the change in the column sum of A_summary:

        Δ_phys_j = Σ_i  L(A_sum_ij(y), A_sum_ij(y-1)) · ln(Q_phys_ij(y) / Q_phys_ij(y-1))

    Also report the cell-count, the column-sum endpoints, and the simple
    weighted-average physical effect for interpretability.
    """
    return (
        cells.assign(_weighted_phys=cells["ln_phys_delta"] * cells["lmdi_weight"])
        .groupby(["j", "kind", "transition"], as_index=False)
        .agg(
            n_active_cells=("i", "count"),
            year_gap=("year_gap", "first"),
            A_sum_prev_total=("A_sum_prev", "sum"),
            A_sum_curr_total=("A_sum_curr", "sum"),
            lmdi_phys_contrib=("_weighted_phys", "sum"),
            lmdi_weight_total=("lmdi_weight", "sum"),
        )
        .assign(
            ln_total_change=lambda d: np.log(
                d["A_sum_curr_total"] / d["A_sum_prev_total"]
            ),
            phys_effect_avg=lambda d: d["lmdi_phys_contrib"] / d["lmdi_weight_total"],
            phys_effect_avg_annual=lambda d: d["lmdi_phys_contrib"]
            / d["lmdi_weight_total"]
            / d["year_gap"],
        )
    )


def _aggregate_by_naics3(per_sector: pd.DataFrame) -> pd.DataFrame:
    """Aggregate output-sector results to NAICS-3 (first 3 chars of j)."""
    return (
        per_sector.assign(naics3=per_sector["j"].astype(str).str[:3])
        .groupby(["naics3", "kind", "transition"], as_index=False)
        .agg(
            n_sectors=("j", "nunique"),
            n_active_cells=("n_active_cells", "sum"),
            year_gap=("year_gap", "first"),
            A_sum_prev_total=("A_sum_prev_total", "sum"),
            A_sum_curr_total=("A_sum_curr_total", "sum"),
            lmdi_phys_contrib=("lmdi_phys_contrib", "sum"),
            lmdi_weight_total=("lmdi_weight_total", "sum"),
        )
        .assign(
            phys_effect_avg=lambda d: d["lmdi_phys_contrib"] / d["lmdi_weight_total"],
            phys_effect_avg_annual=lambda d: d["lmdi_phys_contrib"]
            / d["lmdi_weight_total"]
            / d["year_gap"],
            ln_total_change=lambda d: np.log(
                d["A_sum_curr_total"] / d["A_sum_prev_total"]
            ),
        )
    )


def _print_sniff(by_naics3: pd.DataFrame) -> None:
    print("\n=== Top 10 NAICS-3 by |phys_effect_avg|, kind=total (dom+imp) ===")
    # Sum dom + imp for a per-NAICS-3 total kind summary.
    naics_total = by_naics3.groupby(["naics3", "transition"], as_index=False).agg(
        lmdi_phys_contrib=("lmdi_phys_contrib", "sum"),
        lmdi_weight_total=("lmdi_weight_total", "sum"),
    )
    naics_total["phys_effect_avg"] = (
        naics_total["lmdi_phys_contrib"] / naics_total["lmdi_weight_total"]
    )
    # Pivot for readability: rows = naics3, cols = transition.
    pivot = naics_total.pivot(
        index="naics3", columns="transition", values="phys_effect_avg"
    )
    pivot["abs_max"] = pivot.abs().max(axis=1)
    pivot = pivot.sort_values("abs_max", ascending=False).head(10)
    print(pivot.round(4).to_string())


def main() -> None:
    logger.info("Loading A snapshots...")
    panel = _build_paired_panel()
    logger.info("  panel rows: %s", f"{len(panel):,}")

    logger.info("Computing cell-level Q_phys and YoY effects...")
    cells = _compute_cell_effects(panel)
    logger.info("  active (i,j,kind,transition) rows: %s", f"{len(cells):,}")
    CELLS_OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    cells[
        [
            "i",
            "j",
            "kind",
            "transition",
            "y_prev",
            "y_curr",
            "year_gap",
            "A_sum_prev",
            "A_sum_curr",
            "A_pi_prev",
            "A_pi_curr",
            "Q_phys_prev",
            "Q_phys_curr",
            "ln_phys_delta",
            "ln_phys_delta_annual",
            "lmdi_weight",
        ]
    ].to_parquet(CELLS_OUT_PATH, index=False)
    logger.info("  → %s", CELLS_OUT_PATH)

    logger.info("Aggregating to output sector j...")
    by_sector = _aggregate_by_output_sector(cells)
    by_sector.to_csv(BY_OUTPUT_SECTOR_PATH, index=False)
    logger.info("  → %s (%d rows)", BY_OUTPUT_SECTOR_PATH, len(by_sector))

    logger.info("Aggregating to NAICS-3 of j...")
    by_naics3 = _aggregate_by_naics3(by_sector)
    by_naics3.to_csv(BY_NAICS3_PATH, index=False)
    logger.info("  → %s (%d rows)", BY_NAICS3_PATH, len(by_naics3))

    _print_sniff(by_naics3)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()
