"""Phase C — External validation against BEA-BLS KLEMS productivity data.

Correlates our Phase A LMDI physical-effect against KLEMS-derived measures
of real physical change at the NAICS-3 level for top-impact sectors.

KLEMS workbook covers 1997-2024 with 2017=100 base — matches our LMDI
anchor exactly. Two independent KLEMS series serve as physical-change
references:

1. **Integrated TFP Index** — productivity (real output relative to combined
   inputs). YoY log-growth captures technical change.
2. **Materials / Output ratio** — real materials per unit real output. Most
   direct analogue to the column-sum of A (intermediate-input intensity).
   YoY log-change should track the cell-aggregated physical effect closely.

If Phase A's physical residual carries real signal, sectors should show
non-zero correlation with at least one KLEMS series. If it's noise, no
correlation in either.

KLEMS uses BEA "Production Account Codes" (PAC) that map to NAICS-3 either
1:1 or as aggregations (e.g., 3361MV+3364OT = NAICS-3 336; 311FT = NAICS-3
311+312 lightly contaminated by 312 beverages). Aggregation rule for
many-to-one: geometric mean of YoY growth rates across constituent PAC
codes (assumes equal weight; coarse but interpretable).

Phase A 'kind' is collapsed to dom+imp total, because KLEMS measures the
industry's total real input use regardless of origin.

Outputs:
- output/results/klems_validation_per_transition.csv — per (NAICS-3, transition)
- output/results/klems_validation_summary.csv — per NAICS-3 Pearson r
- output/plots/klems_validation_scatter.png — small-multiples scatter

KLEMS source: ``gs://cornerstone-default/extract/input-data/BEA_KLEMS/``.
Auto-downloaded on first run to ``bedrock/extract/input_data/BEA_KLEMS/``
following the codebase convention. Override the local path via env var
``KLEMS_XLSX`` (e.g., for offline use against a hand-placed copy).

Usage:
    python -m bedrock.analysis.a_matrix_time_series.signal_noise.validate_klems
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import TypedDict

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from bedrock.analysis.a_matrix_time_series.constants import PLOTS_DIR, RESULTS_DIR
from bedrock.utils.io.gcp import download_gcs_file_if_not_exists
from bedrock.utils.io.local_extract_input_data import local_extract_input_dir

logger = logging.getLogger(__name__)


class _KlemsSpec(TypedDict):
    klems_descriptions: list[str]
    note: str


KLEMS_GCS_SUB_BUCKET = "extract/input-data/BEA_KLEMS"
KLEMS_FILENAME = "BEA-BLS-industry-level-production-account-1997-2024.xlsx"


def _ensure_klems_local() -> Path:
    """Return the local KLEMS xlsx path, downloading from GCS if absent.

    Honors ``$KLEMS_XLSX`` env var as an explicit override (handy for offline
    runs or pinning a specific local copy). Otherwise pulls from
    ``gs://cornerstone-default/extract/input-data/BEA_KLEMS/`` into the
    standard local cache ``bedrock/extract/input_data/BEA_KLEMS/``.
    """
    override = os.environ.get("KLEMS_XLSX")
    if override:
        pth = Path(override)
        if not pth.exists():
            raise FileNotFoundError(f"$KLEMS_XLSX set but file not found: {pth}")
        return pth
    local_dir = Path(local_extract_input_dir("BEA_KLEMS"))
    local_pth = local_dir / KLEMS_FILENAME
    download_gcs_file_if_not_exists(
        name=KLEMS_FILENAME,
        sub_bucket=KLEMS_GCS_SUB_BUCKET,
        pth=str(local_pth),
    )
    if not local_pth.exists():
        raise FileNotFoundError(
            f"Failed to download KLEMS workbook from "
            f"gs://cornerstone-default/{KLEMS_GCS_SUB_BUCKET}/{KLEMS_FILENAME}"
        )
    return local_pth


# Resolved lazily inside main()/_load_klems_sheet so import-time doesn't hit GCS.
_KLEMS_XLSX: Path | None = None


def _klems_xlsx() -> Path:
    global _KLEMS_XLSX
    if _KLEMS_XLSX is None:
        _KLEMS_XLSX = _ensure_klems_local()
    return _KLEMS_XLSX


BY_NAICS3_PATH = RESULTS_DIR / "lmdi_phys_by_naics3.csv"
PER_TRANSITION_OUT = RESULTS_DIR / "klems_validation_per_transition.csv"
SUMMARY_OUT = RESULTS_DIR / "klems_validation_summary.csv"
SCATTER_PLOT = PLOTS_DIR / "klems_validation_scatter.png"

# Sectors to validate. Map each Phase A NAICS-3 to the list of KLEMS
# Production Account Codes (industry descriptions) that compose it.
# Notes:
# - ``contamination_note`` flags imperfect mappings to caveat the reads.
# - Order matches the Phase A top-impact ranking on dom; the signal-clean
#   exception (327, cement) is included to test whether Phase B's flag holds.
NAICS3_TO_KLEMS: dict[str, _KlemsSpec] = {
    "336": {
        "klems_descriptions": [
            "Motor vehicles, bodies and trailers, and parts",
            "Other transportation equipment",
        ],
        "note": "aggregates 3361MV + 3364OT (full coverage of NAICS-3 336)",
    },
    "325": {"klems_descriptions": ["Chemical products"], "note": "1:1"},
    "334": {"klems_descriptions": ["Computer and electronic products"], "note": "1:1"},
    "324": {"klems_descriptions": ["Petroleum and coal products"], "note": "1:1"},
    "332": {"klems_descriptions": ["Fabricated metal products"], "note": "1:1"},
    "311": {
        "klems_descriptions": ["Food and beverage and tobacco products"],
        "note": "covers 311+312 (mild contamination from 312 beverages/tobacco)",
    },
    "333": {"klems_descriptions": ["Machinery"], "note": "1:1"},
    "327": {
        "klems_descriptions": ["Nonmetallic mineral products"],
        "note": "1:1 — signal-clean from Phase B (r=+0.66)",
    },
    "339": {"klems_descriptions": ["Miscellaneous manufacturing"], "note": "1:1"},
    "212": {"klems_descriptions": ["Mining, except oil and gas"], "note": "1:1"},
    "112": {
        "klems_descriptions": ["Farms"],
        "note": "covers 111+112 (moderate contamination — 111 crops)",
    },
}

# Our LMDI window. KLEMS covers 1997-2024 so this is a subset.
YEARS: tuple[int, ...] = (2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024)


def _load_klems_sheet(sheet: str) -> pd.DataFrame:
    """Load a KLEMS quantity-index sheet into long format: (description, year, value)."""
    raw = pd.read_excel(_klems_xlsx(), sheet_name=sheet, header=1)
    raw = raw.rename(columns={"Industry Description": "description"})
    # Year columns arrive as strings like '1997' or floats like 1997.0 depending
    # on dtype inference; coerce anything castable to an int year in 1900-2100.
    new_cols: dict[object, int] = {}
    for c in raw.columns:
        try:
            as_int = int(float(c))
        except (TypeError, ValueError):
            continue
        if 1900 <= as_int <= 2100:
            new_cols[c] = as_int
    raw = raw.rename(columns=new_cols)
    raw = raw[raw["description"].notna()].copy()
    year_cols = sorted(new_cols.values())
    long = raw.melt(
        id_vars=["description"],
        value_vars=year_cols,
        var_name="year",
        value_name="value",
    )
    long["year"] = long["year"].astype(int)
    long["value"] = pd.to_numeric(long["value"], errors="coerce")
    long["description"] = long["description"].astype(str).str.strip()
    return long.dropna(subset=["value"]).reset_index(drop=True)


def _klems_growth_per_naics3(
    metric_long: pd.DataFrame,
    metric_name: str,
) -> pd.DataFrame:
    """Compute YoY log-growth per NAICS-3 × transition from a long KLEMS series.

    For NAICS-3 mapped to multiple KLEMS codes (e.g., 336), takes the
    arithmetic mean of per-code log-growth — equivalent to geometric mean
    of the underlying ratios, equal-weighted.
    """
    rows: list[dict[str, object]] = []
    for naics3, spec in NAICS3_TO_KLEMS.items():
        descs = spec["klems_descriptions"]
        sub = metric_long[metric_long["description"].isin(descs)].copy()
        if sub.empty:
            logger.warning("KLEMS %s: no rows for NAICS-3 %s", metric_name, naics3)
            continue
        # Wide: rows=description, cols=year.
        wide = sub.pivot_table(index="description", columns="year", values="value")
        for y_prev, y_curr in zip(YEARS[:-1], YEARS[1:]):
            if y_prev not in wide.columns or y_curr not in wide.columns:
                continue
            ratios = wide[y_curr] / wide[y_prev]
            log_growths = np.log(ratios.dropna().astype(float))
            if log_growths.size == 0:
                continue
            rows.append(
                {
                    "naics3": naics3,
                    "transition": f"{y_prev}->{y_curr}",
                    f"{metric_name}_log_growth": float(np.mean(log_growths)),
                    f"{metric_name}_log_growth_annual": float(np.mean(log_growths))
                    / (y_curr - y_prev),
                    f"{metric_name}_pct_annual": float(
                        (np.exp(np.mean(log_growths) / (y_curr - y_prev)) - 1.0) * 100.0
                    ),
                    f"{metric_name}_n_codes": int(log_growths.size),
                }
            )
    return pd.DataFrame(rows)


def _materials_per_output_log_growth(
    materials_long: pd.DataFrame,
    output_long: pd.DataFrame,
) -> pd.DataFrame:
    """YoY log-change of the Materials/Output ratio, per NAICS-3 × transition."""
    rows: list[dict[str, object]] = []
    mats = materials_long.set_index(["description", "year"])["value"]
    outp = output_long.set_index(["description", "year"])["value"]
    for naics3, spec in NAICS3_TO_KLEMS.items():
        descs = spec["klems_descriptions"]
        per_code: dict[tuple[int, int], list[float]] = {}
        for desc in descs:
            for y_prev, y_curr in zip(YEARS[:-1], YEARS[1:]):
                try:
                    m_prev = float(mats.loc[(desc, y_prev)])
                    m_curr = float(mats.loc[(desc, y_curr)])
                    o_prev = float(outp.loc[(desc, y_prev)])
                    o_curr = float(outp.loc[(desc, y_curr)])
                except KeyError:
                    continue
                if m_prev <= 0 or m_curr <= 0 or o_prev <= 0 or o_curr <= 0:
                    continue
                ratio_curr = m_curr / o_curr
                ratio_prev = m_prev / o_prev
                log_change = float(np.log(ratio_curr / ratio_prev))
                per_code.setdefault((y_prev, y_curr), []).append(log_change)
        for (y_prev, y_curr), vals in per_code.items():
            avg = float(np.mean(vals))
            rows.append(
                {
                    "naics3": naics3,
                    "transition": f"{y_prev}->{y_curr}",
                    "mat_over_out_log_growth": avg,
                    "mat_over_out_log_growth_annual": avg / (y_curr - y_prev),
                    "mat_over_out_pct_annual": float(
                        (np.exp(avg / (y_curr - y_prev)) - 1.0) * 100.0
                    ),
                }
            )
    return pd.DataFrame(rows)


def _phase_a_total_per_naics3() -> pd.DataFrame:
    """Collapse Phase A dom+imp into one 'total' phys-effect per (NAICS-3, transition)."""
    by_naics3 = pd.read_csv(BY_NAICS3_PATH, dtype={"naics3": str})
    by_naics3 = by_naics3[by_naics3["naics3"].isin(NAICS3_TO_KLEMS.keys())].copy()
    out = by_naics3.groupby(["naics3", "transition"], as_index=False).agg(
        year_gap=("year_gap", "first"),
        lmdi_phys_contrib=("lmdi_phys_contrib", "sum"),
        lmdi_weight_total=("lmdi_weight_total", "sum"),
    )
    out["phase_a_log_effect_annual"] = (
        out["lmdi_phys_contrib"] / out["lmdi_weight_total"] / out["year_gap"]
    )
    out["phase_a_pct_annual"] = (np.exp(out["phase_a_log_effect_annual"]) - 1.0) * 100.0
    return out[
        [
            "naics3",
            "transition",
            "year_gap",
            "phase_a_log_effect_annual",
            "phase_a_pct_annual",
        ]
    ]


def _pearson(x: np.ndarray, y: np.ndarray) -> float:
    if x.size < 2 or np.std(x) == 0 or np.std(y) == 0:
        return float("nan")
    return float(np.corrcoef(x, y)[0, 1])


def _summarize(merged: pd.DataFrame) -> pd.DataFrame:
    """Pearson r per NAICS-3 between Phase A and each KLEMS series."""
    rows: list[dict[str, object]] = []
    for naics3, grp in merged.groupby("naics3"):
        x = grp["phase_a_log_effect_annual"].to_numpy(dtype=float)
        rows.append(
            {
                "naics3": naics3,
                "klems_note": NAICS3_TO_KLEMS[str(naics3)]["note"],
                "n_transitions": int(grp.shape[0]),
                "r_phase_a_vs_tfp": _pearson(
                    x, grp["tfp_log_growth_annual"].to_numpy(dtype=float)
                ),
                "r_phase_a_vs_mat_over_out": _pearson(
                    x, grp["mat_over_out_log_growth_annual"].to_numpy(dtype=float)
                ),
                "phase_a_mean_pct_annual": float(grp["phase_a_pct_annual"].mean()),
                "tfp_mean_pct_annual": float(grp["tfp_pct_annual"].mean()),
                "mat_over_out_mean_pct_annual": float(
                    grp["mat_over_out_pct_annual"].mean()
                ),
            }
        )
    df = pd.DataFrame(rows)
    return df.sort_values("naics3").reset_index(drop=True)


def _transition_sort_key(t: str) -> int:
    return int(t.split("->")[0])


def _plot_scatter(merged: pd.DataFrame, summary: pd.DataFrame) -> None:
    """Per-NAICS-3 small-multiples: Phase A phys-effect vs KLEMS series."""
    naics_order = list(NAICS3_TO_KLEMS.keys())
    naics_present = [n for n in naics_order if n in set(merged["naics3"].unique())]
    n_sec = len(naics_present)
    n_cols = 4
    n_rows = (n_sec + n_cols - 1) // n_cols
    fig, axes = plt.subplots(
        n_rows, n_cols, figsize=(4 * n_cols, 3.6 * n_rows), squeeze=False
    )
    summary_idx = summary.set_index("naics3")

    transitions_sorted = sorted(merged["transition"].unique(), key=_transition_sort_key)
    cmap = plt.get_cmap("viridis")
    color_by_trans = {
        t: cmap(i / max(1, len(transitions_sorted) - 1))
        for i, t in enumerate(transitions_sorted)
    }

    for idx, naics3 in enumerate(naics_present):
        ax = axes[idx // n_cols][idx % n_cols]
        grp = merged[merged["naics3"] == naics3].sort_values(
            "transition", key=lambda s: s.map(_transition_sort_key)
        )
        # Use Materials/Output ratio change as the primary x-axis comparison
        # (more direct analogue to A column-sum than TFP).
        xs = grp["mat_over_out_pct_annual"].to_numpy(dtype=float)
        ys = grp["phase_a_pct_annual"].to_numpy(dtype=float)
        cols = [color_by_trans[t] for t in grp["transition"]]
        ax.axhline(0, color="gray", linewidth=0.6, alpha=0.5)
        ax.axvline(0, color="gray", linewidth=0.6, alpha=0.5)
        for x, y, t, c in zip(xs, ys, grp["transition"], cols):
            ax.scatter(x, y, color=c, s=60, edgecolor="black", linewidth=0.4, zorder=3)
            ax.annotate(
                t.replace("2017->2018", "17→18").replace("->", "→"),
                (x, y),
                fontsize=7,
                alpha=0.75,
                xytext=(4, 4),
                textcoords="offset points",
            )
        # y = x reference (perfect correspondence).
        lim = max(
            float(np.nanmax(np.abs(xs))) if xs.size else 1.0,
            float(np.nanmax(np.abs(ys))) if ys.size else 1.0,
            5.0,
        )
        lim = min(lim * 1.15, 30.0)
        ax.plot(
            [-lim, lim],
            [-lim, lim],
            color="red",
            linewidth=0.8,
            linestyle=":",
            alpha=0.5,
        )
        ax.set_xlim(-lim, lim)
        ax.set_ylim(-lim, lim)
        ax.set_aspect("equal")

        r_mat = float(summary_idx.loc[naics3, "r_phase_a_vs_mat_over_out"])  # type: ignore[arg-type]
        r_tfp = float(summary_idx.loc[naics3, "r_phase_a_vs_tfp"])  # type: ignore[arg-type]
        note = NAICS3_TO_KLEMS[naics3]["note"]
        # Per-NAICS-3 verdict text (short).
        if abs(r_mat) > 0.5:
            verdict = (
                "Mat/Out CORROBORATES Phase A"
                if r_mat > 0
                else "Mat/Out CONTRADICTS Phase A"
            )
            verdict_color = "darkgreen" if r_mat > 0 else "darkred"
        else:
            verdict = "Mat/Out ~uncorrelated with Phase A"
            verdict_color = "dimgray"
        ax.set_title(
            f"NAICS-3 {naics3}    r(Mat/Out)={r_mat:+.2f}  r(TFP)={r_tfp:+.2f}\n"
            f"{verdict}",
            fontsize=9,
            color=verdict_color,
        )
        ax.set_xlabel("KLEMS Δ(Materials/Output)/yr (%)", fontsize=8)
        ax.set_ylabel("Phase A phys-effect/yr (%)", fontsize=8)
        ax.tick_params(labelsize=8)
        ax.grid(alpha=0.25)
        ax.text(
            0.03,
            0.04,
            note,
            fontsize=6.5,
            alpha=0.7,
            transform=ax.transAxes,
            va="bottom",
            ha="left",
            wrap=True,
            style="italic",
        )

    # Hide unused panels.
    for idx in range(n_sec, n_rows * n_cols):
        axes[idx // n_cols][idx % n_cols].set_visible(False)

    fig.suptitle(
        "Phase C — KLEMS validation: Phase A LMDI physical-effect vs BEA-BLS KLEMS Δ(Materials/Output)\n"
        "Each point = one 1-year transition (2017→2018 .. 2023→2024). "
        "Red dotted = y=x (perfect correspondence). Positive r → KLEMS corroborates Phase A's residual.",
        fontsize=12,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.97))
    SCATTER_PLOT.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(SCATTER_PLOT, dpi=120, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    klems_pth = _klems_xlsx()
    logger.info("Loading KLEMS sheets from %s", klems_pth.name)
    tfp_long = _load_klems_sheet("Integrated TFP Index")
    output_long = _load_klems_sheet("Gross Output_Quantity")
    mat_long = _load_klems_sheet("Materials_Quantity")

    logger.info("Computing KLEMS YoY series per NAICS-3...")
    tfp = _klems_growth_per_naics3(tfp_long, "tfp")
    mat_over_out = _materials_per_output_log_growth(mat_long, output_long)

    logger.info("Loading Phase A and collapsing dom+imp...")
    phase_a = _phase_a_total_per_naics3()

    merged = phase_a.merge(tfp, on=["naics3", "transition"], how="inner").merge(
        mat_over_out, on=["naics3", "transition"], how="inner"
    )
    PER_TRANSITION_OUT.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(PER_TRANSITION_OUT, index=False)
    logger.info("  → %s (%d rows)", PER_TRANSITION_OUT, len(merged))

    summary = _summarize(merged)
    summary.to_csv(SUMMARY_OUT, index=False)
    logger.info("  → %s (%d rows)", SUMMARY_OUT, len(summary))

    logger.info("Plotting per-NAICS-3 scatter...")
    _plot_scatter(merged, summary)
    logger.info("  → %s", SCATTER_PLOT)

    print("\n=== Phase C — KLEMS validation summary ===")
    print(
        summary[
            [
                "naics3",
                "n_transitions",
                "r_phase_a_vs_tfp",
                "r_phase_a_vs_mat_over_out",
                "phase_a_mean_pct_annual",
                "tfp_mean_pct_annual",
                "mat_over_out_mean_pct_annual",
                "klems_note",
            ]
        ]
        .round(3)
        .to_string(index=False)
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()
