"""Compare V-norm-derived commodity price ratio vs industry price ratio.

Step 0 sanity-check companion to epic #337. Confirms whether the two
inflation approaches produce meaningfully different price ratios; if they
collapse to the same numbers the downstream analysis is moot.

Outputs (gitignored under `output/`):
- ratio_summary.csv  : per-year distribution stats for both ratios.
- ratio_per_code.csv : long-format with `vnorm_col_sum` for diagnosing
                        zero-V_norm-column anomalies.
- ratio_scatter.png  : per-year scatter, industry (x) vs commodity (y),
                        with `y=x` reference.

Usage:
    python -m bedrock.analysis.a_matrix_time_series.compare_price_ratios
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import cast

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_Vnorm_scrap_corrected,
)
from bedrock.utils.economic.inflate_cornerstone_to_target_year import (
    get_cornerstone_price_ratio,
    get_vnorm_adjusted_commodity_price_ratio,
)

logger = logging.getLogger(__name__)

ORIGINAL_YEAR = 2017
TARGET_YEARS: list[int] = [2018, 2019, 2020, 2021, 2022, 2023, 2024]
OUTPUT_DIR = Path(__file__).parent / "output"
RESULTS_DIR = OUTPUT_DIR / "results"
PLOTS_DIR = OUTPUT_DIR / "plots"
INFLATE_V = False  # inflates V to prepare Vnorm for use in commodity ratios


def build_comparison_long(
    target_years: list[int], original_year: int = ORIGINAL_YEAR
) -> pd.DataFrame:

    rows: list[pd.DataFrame] = []
    for year in target_years:
        Vnorm = derive_cornerstone_Vnorm_scrap_corrected(
            apply_inflation=INFLATE_V, target_year=year
        )
        vnorm_col_sum = Vnorm.sum(axis=0).rename("vnorm_col_sum")
        industry = get_cornerstone_price_ratio(original_year, year).rename(
            "industry_ratio"
        )
        commodity = get_vnorm_adjusted_commodity_price_ratio(
            original_year, year, inflate_V=INFLATE_V
        ).rename("commodity_ratio")
        df = pd.concat([industry, commodity, vnorm_col_sum], axis=1).reset_index(
            names="code"
        )
        df["year"] = year
        df["abs_delta"] = (df["commodity_ratio"] - df["industry_ratio"]).abs()
        df["rel_delta"] = df["abs_delta"] / df["industry_ratio"].replace(0, np.nan)
        rows.append(df)
    return pd.concat(rows, ignore_index=True)[
        [
            "year",
            "code",
            "industry_ratio",
            "commodity_ratio",
            "abs_delta",
            "rel_delta",
            "vnorm_col_sum",
        ]
    ]


def summarize(long: pd.DataFrame) -> pd.DataFrame:
    def stats(s: pd.Series) -> dict[str, float]:
        return {
            "mean": float(s.mean()),
            "median": float(s.median()),
            "p05": float(s.quantile(0.05)),
            "p95": float(s.quantile(0.95)),
            "min": float(s.min()),
            "max": float(s.max()),
        }

    rows = []
    for year, group in long.groupby("year"):
        rows.append(
            {
                "year": cast("int", year),
                **{f"ind_{k}": v for k, v in stats(group["industry_ratio"]).items()},
                **{f"com_{k}": v for k, v in stats(group["commodity_ratio"]).items()},
                "rel_delta_p95": float(group["rel_delta"].quantile(0.95)),
                "n_rel_delta_gt_1pct": int((group["rel_delta"] > 0.01).sum()),
                "n_zero_vnorm_col": int((group["vnorm_col_sum"] < 1e-6).sum()),
            }
        )
    return pd.DataFrame(rows)


def plot_scatter(long: pd.DataFrame, path: Path) -> None:
    years = sorted(long["year"].unique())
    n = len(years)
    cols = 3
    rows = (n + cols - 1) // cols
    fig, axes = plt.subplots(rows, cols, figsize=(4 * cols, 4 * rows), squeeze=False)
    for i, year in enumerate(years):
        ax = axes[i // cols][i % cols]
        sub = long[long["year"] == year]
        ax.scatter(sub["industry_ratio"], sub["commodity_ratio"], s=8, alpha=0.4)
        lo = float(min(sub["industry_ratio"].min(), sub["commodity_ratio"].min()))
        hi = float(max(sub["industry_ratio"].max(), sub["commodity_ratio"].max()))
        ax.plot([lo, hi], [lo, hi], "k--", lw=0.5, label="y=x")
        ax.set_xlabel("industry price ratio")
        ax.set_ylabel("V-norm commodity price ratio")
        ax.set_title(f"2017 → {year}")
        ax.legend(loc="lower right", fontsize=8)
    for j in range(n, rows * cols):
        axes[j // cols][j % cols].axis("off")
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def main() -> None:
    addon = ""
    if INFLATE_V:
        addon = "_V_inflated"
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    long = build_comparison_long(TARGET_YEARS)
    long.to_csv(RESULTS_DIR / ("ratio_per_code" + addon + ".csv"), index=False)
    summary = summarize(long)
    summary.to_csv(RESULTS_DIR / ("ratio_summary" + addon + ".csv"), index=False)
    plot_scatter(long, PLOTS_DIR / ("ratio_scatter" + addon + ".png"))
    logger.info("Wrote outputs to %s and %s", RESULTS_DIR, PLOTS_DIR)
    print(summary.to_string(index=False))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
