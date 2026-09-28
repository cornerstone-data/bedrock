"""Local paired EF comparison for 2024 waste-weight pilot (no Google Sheet required).

Runs control then treatment under fresh processes, writes N/D parquet + a
comparison CSV under ``cache/impact_2024/``, and figures under ``figures/``.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[5]
OUT = Path(__file__).resolve().parents[1] / "cache" / "impact_2024"
FIG = Path(__file__).resolve().parents[1] / "figures"
CONTROL = "2025_usa_cornerstone_v0_4_nowcast_2024"
TREATMENT = "2025_usa_cornerstone_v0_4_nowcast_2024_waste_weights_match_io"

WASTE_CODES = [
    "562111",
    "562HAZ",
    "562212",
    "562213",
    "562910",
    "562920",
    "562OTH",
    "562000",
]


def _worker(config: str, tag: str) -> None:
    """Pull D/N for one config; must be a fresh process (global USA config once)."""
    from bedrock.extract.disaggregation.waste_weight_types import (  # noqa: PLC0415
        WeightDerivationProvenance,
    )
    from bedrock.transform.eeio.cornerstone_disagg_pipeline import (  # noqa: PLC0415
        get_waste_disagg_provenance,
        get_waste_disagg_weights,
    )
    from bedrock.utils.config.usa_config import set_global_usa_config  # noqa: PLC0415
    from bedrock.utils.validation.diagnostics_helpers import (  # noqa: PLC0415
        pull_efs_for_diagnostics,
    )

    set_global_usa_config(config)
    # Force waste weights / disagg onto the hot path before EF pull
    _ = get_waste_disagg_weights()
    prov = get_waste_disagg_provenance()
    efs = pull_efs_for_diagnostics()

    OUT.mkdir(parents=True, exist_ok=True)
    d = efs.D_new.copy()
    d.columns = ["D"]
    n = efs.N_new.copy()
    n.columns = ["N"]
    d.to_parquet(OUT / f"{tag}_D.parquet")
    n.to_parquet(OUT / f"{tag}_N.parquet")
    meta = {
        "config": config,
        "tag": tag,
        "n_sectors_D": int(len(d)),
        "n_sectors_N": int(len(n)),
    }
    if isinstance(prov, WeightDerivationProvenance):
        meta["provenance"] = prov.to_dict()
    elif prov is not None:
        meta["provenance"] = {
            k: getattr(prov, k, None)
            for k in (
                "target_year",
                "rcra_source_year",
                "ec_source_year",
                "aies_source_year",
                "aies_table",
                "sas_source_year",
                "fallback_notes",
                "naics_map_version",
                "mut_dollar_year",
            )
        }
    (OUT / f"{tag}_meta.json").write_text(json.dumps(meta, indent=2, default=str))
    print(f"WROTE {tag} D/N + meta", flush=True)


def _load_vec(tag: str, kind: str) -> pd.Series:
    df = pd.read_parquet(OUT / f"{tag}_{kind}.parquet")
    return df.iloc[:, 0].astype(float)


def _paired_table(control: pd.Series, treatment: pd.Series, kind: str) -> pd.DataFrame:
    idx = control.index.intersection(treatment.index)
    c = control.reindex(idx)
    t = treatment.reindex(idx)
    diff = t - c
    perc = (diff / c.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan)
    out = pd.DataFrame(
        {
            f"{kind}_control": c,
            f"{kind}_treatment": t,
            f"{kind}_diff": diff,
            f"{kind}_perc_diff": perc,
        }
    )
    out.index.name = "sector"
    return out


def _plot_hist(perc: pd.Series, title: str, path: Path, color: str) -> dict[str, float]:
    vals = perc.replace([np.inf, -np.inf], np.nan).dropna() * 100.0
    clipped = vals.clip(-100, 100)
    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.hist(clipped, bins=60, color=color, edgecolor="white", linewidth=0.3)
    ax.axvline(0, color="black", linewidth=1)
    med = float(clipped.median())
    p95 = float(clipped.abs().quantile(0.95))
    ax.set_title(title)
    ax.set_xlabel("Percent difference (treatment vs control), clipped ±100%")
    ax.set_ylabel("Sector count")
    ax.text(
        0.02,
        0.98,
        f"n={len(clipped)}\nmedian={med:.2f}%\np95(|·|)={p95:.2f}%",
        transform=ax.transAxes,
        va="top",
        ha="left",
        fontsize=9,
        bbox={"facecolor": "white", "alpha": 0.85, "edgecolor": "none"},
    )
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return {"n": float(len(clipped)), "median_pct": med, "p95_abs_pct": p95}


def compare_and_plot() -> None:
    FIG.mkdir(parents=True, exist_ok=True)
    n_tab = _paired_table(_load_vec("control", "N"), _load_vec("treatment", "N"), "N")
    d_tab = _paired_table(_load_vec("control", "D"), _load_vec("treatment", "D"), "D")
    paired = n_tab.join(d_tab, how="outer")
    paired.to_csv(OUT / "paired_control_vs_treatment.csv")

    # Waste focus
    waste = paired.reindex([c for c in WASTE_CODES if c in paired.index]).copy()
    waste.to_csv(OUT / "waste_sectors_control_vs_treatment.csv")

    n_stats = _plot_hist(
        paired["N_perc_diff"],
        "Total EF (N): treatment vs control — all sectors",
        FIG / "impact_2024_N_perc_diff_hist.png",
        "#ff7f0e",
    )
    d_stats = _plot_hist(
        paired["D_perc_diff"],
        "Direct EF (D): treatment vs control — all sectors",
        FIG / "impact_2024_D_perc_diff_hist.png",
        "#1f77b4",
    )

    # Top movers by |N %|
    movers = (
        paired.assign(abs_n=paired["N_perc_diff"].abs())
        .sort_values("abs_n", ascending=False)
        .head(25)
        .drop(columns=["abs_n"])
    )
    movers.to_csv(OUT / "top25_N_perc_movers.csv")

    summary = {
        "n_stats": n_stats,
        "d_stats": d_stats,
        "waste_sectors": waste.reset_index().to_dict(orient="records"),
        "share_n_abs_gt_1pct": float((paired["N_perc_diff"].abs() > 0.01).mean()),
        "share_n_abs_gt_5pct": float((paired["N_perc_diff"].abs() > 0.05).mean()),
        "control_meta": json.loads((OUT / "control_meta.json").read_text()),
        "treatment_meta": json.loads((OUT / "treatment_meta.json").read_text()),
    }
    (OUT / "summary.json").write_text(json.dumps(summary, indent=2, default=str))
    print(json.dumps(summary, indent=2, default=str))


def main(argv: list[str]) -> int:
    if len(argv) >= 2 and argv[1] == "worker":
        _worker(argv[2], argv[3])
        return 0
    if len(argv) >= 2 and argv[1] == "compare":
        compare_and_plot()
        return 0

    py = sys.executable
    for tag, cfg in (("control", CONTROL), ("treatment", TREATMENT)):
        print(f"=== Running {tag}: {cfg} ===", flush=True)
        subprocess.run(
            [
                py,
                "-m",
                "bedrock.analysis.nowcasting.waste_disaggregation.scripts.run_waste_weight_impact_efs",
                "worker",
                cfg,
                tag,
            ],
            check=True,
            cwd=str(ROOT),
        )
    compare_and_plot()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
