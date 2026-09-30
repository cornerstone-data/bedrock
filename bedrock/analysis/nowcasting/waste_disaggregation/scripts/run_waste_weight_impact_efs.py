"""Paired EF comparison for waste-weight A/B (no Google Sheet required).

Generalized beyond the 2024 pilot: CLI selects year + control/treatment
configs; outputs land under ``cache/impact_{year}_{mut_vintage_label}/`` so
GCS year-aligned vs 2017 runs never share a cache with private local re-runs.

Meta records resolved ``nowcast_mut_vintage``, ``rcra_path=br_bypass``, and
arm roles. Subprocess-per-arm keeps the global USA config isolated.

Examples::

  # GCS 2024 re-baseline (defaults from impact_pins)
  python -m ...run_waste_weight_impact_efs --year 2024

  # Explicit configs / vintage label override for cache dir
  python -m ...run_waste_weight_impact_efs --year 2024 \\
    --control CFG_A --treatment CFG_B --mut-vintage-label v0.3.0_92b7a8a

  # Worker / compare entry points (used by the driver)
  python -m ...run_waste_weight_impact_efs worker CFG tag OUTDIR
  python -m ...run_waste_weight_impact_efs compare OUTDIR [--figure-prefix PREFIX]
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[5]
PKG_DIR = Path(__file__).resolve().parents[1]
FIG = PKG_DIR / "figures"
MODULE = (
    "bedrock.analysis.nowcasting.waste_disaggregation.scripts."
    "run_waste_weight_impact_efs"
)

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


def _safe_vintage_label(vintage: str) -> str:
    """Filesystem-safe label for cache directory names."""
    return re.sub(r"[^\w.\-]+", "_", vintage.strip()) or "unknown_vintage"


def impact_cache_dir(year: int, mut_vintage: str) -> Path:
    label = _safe_vintage_label(mut_vintage)
    return PKG_DIR / "cache" / f"impact_{year}_{label}"


def _worker(config: str, tag: str, out_dir: Path) -> None:
    """Pull D/N for one config; must be a fresh process (global USA config once)."""
    from bedrock.analysis.nowcasting.waste_disaggregation.impact_pins import (  # noqa: PLC0415
        ANALYSIS_CONFIG_DIR,
        install_analysis_usa_config,
    )
    from bedrock.extract.disaggregation.waste_weight_types import (  # noqa: PLC0415
        WeightDerivationProvenance,
    )
    from bedrock.transform.eeio.cornerstone_disagg_pipeline import (  # noqa: PLC0415
        get_waste_disagg_provenance,
        get_waste_disagg_weights,
    )
    from bedrock.utils.config.usa_config import get_usa_config  # noqa: PLC0415
    from bedrock.utils.validation.diagnostics_helpers import (  # noqa: PLC0415
        pull_efs_for_diagnostics,
    )

    stem = config if config.endswith(".yaml") else f"{config}.yaml"
    install_analysis_usa_config(ANALYSIS_CONFIG_DIR / stem)
    cfg = get_usa_config()
    _ = get_waste_disagg_weights()
    prov = get_waste_disagg_provenance()
    efs = pull_efs_for_diagnostics()

    out_dir.mkdir(parents=True, exist_ok=True)
    d = efs.D_new.copy()
    d.columns = ["D"]
    n = efs.N_new.copy()
    n.columns = ["N"]
    d.to_parquet(out_dir / f"{tag}_D.parquet")
    n.to_parquet(out_dir / f"{tag}_N.parquet")

    fallback_notes: list[str] = []
    if isinstance(prov, WeightDerivationProvenance):
        fallback_notes = list(prov.fallback_notes)
    elif prov is not None:
        fallback_notes = list(getattr(prov, "fallback_notes", []) or [])

    meta: dict[str, object] = {
        "config": config,
        "tag": tag,
        "arm_role": tag,
        "n_sectors_D": int(len(d)),
        "n_sectors_N": int(len(n)),
        "nowcast_mut_vintage": cfg.nowcast_mut_vintage,
        "usa_base_io_data_year": int(cfg.usa_base_io_data_year),
        "waste_weights_year": cfg.waste_weights_year,
        "rcra_path": (
            "br_bypass"
            if any("rcra_path=br_bypass" in str(n) for n in fallback_notes)
            else ("bundled_2017" if tag == "control" else "unknown")
        ),
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
    (out_dir / f"{tag}_meta.json").write_text(
        json.dumps(meta, indent=2, default=str), encoding="utf-8"
    )
    print(f"WROTE {tag} D/N + meta -> {out_dir}", flush=True)


def _load_vec(out_dir: Path, tag: str, kind: str) -> pd.Series:
    df = pd.read_parquet(out_dir / f"{tag}_{kind}.parquet")
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


def _assert_matching_vintages(out_dir: Path) -> str:
    control = json.loads((out_dir / "control_meta.json").read_text(encoding="utf-8"))
    treatment = json.loads(
        (out_dir / "treatment_meta.json").read_text(encoding="utf-8")
    )
    c_v = control.get("nowcast_mut_vintage")
    t_v = treatment.get("nowcast_mut_vintage")
    if c_v is None or t_v is None:
        raise ValueError(
            "Impact requires both arms to resolve nowcast_mut_vintage; "
            f"got control={c_v!r} treatment={t_v!r}"
        )
    if c_v != t_v:
        raise ValueError(
            "Control and treatment nowcast_mut_vintage must match; "
            f"got control={c_v!r} treatment={t_v!r}"
        )
    return str(c_v)


def compare_and_plot(out_dir: Path, *, figure_prefix: str) -> None:
    FIG.mkdir(parents=True, exist_ok=True)
    resolved_vintage = _assert_matching_vintages(out_dir)

    n_tab = _paired_table(
        _load_vec(out_dir, "control", "N"),
        _load_vec(out_dir, "treatment", "N"),
        "N",
    )
    d_tab = _paired_table(
        _load_vec(out_dir, "control", "D"),
        _load_vec(out_dir, "treatment", "D"),
        "D",
    )
    paired = n_tab.join(d_tab, how="outer")
    paired.to_csv(out_dir / "paired_control_vs_treatment.csv")

    waste = paired.reindex([c for c in WASTE_CODES if c in paired.index]).copy()
    waste.to_csv(out_dir / "waste_sectors_control_vs_treatment.csv")

    n_stats = _plot_hist(
        paired["N_perc_diff"],
        f"Total EF (N): treatment vs control — {figure_prefix}",
        FIG / f"{figure_prefix}_N_perc_diff_hist.png",
        "#ff7f0e",
    )
    d_stats = _plot_hist(
        paired["D_perc_diff"],
        f"Direct EF (D): treatment vs control — {figure_prefix}",
        FIG / f"{figure_prefix}_D_perc_diff_hist.png",
        "#1f77b4",
    )

    movers = (
        paired.assign(abs_n=paired["N_perc_diff"].abs())
        .sort_values("abs_n", ascending=False)
        .head(25)
        .drop(columns=["abs_n"])
    )
    movers.to_csv(out_dir / "top25_N_perc_movers.csv")

    # Waste-sector bar chart
    if not waste.empty and "N_perc_diff" in waste.columns:
        fig, ax = plt.subplots(figsize=(8, 4.5))
        w = waste.dropna(subset=["N_perc_diff"])
        x = np.arange(len(w))
        width = 0.35
        ax.bar(x - width / 2, w["N_perc_diff"] * 100, width, label="N %")
        if "D_perc_diff" in w.columns:
            ax.bar(x + width / 2, w["D_perc_diff"] * 100, width, label="D %")
        ax.set_xticks(x)
        ax.set_xticklabels([str(i) for i in w.index], rotation=45, ha="right")
        ax.axhline(0, color="black", linewidth=0.8)
        ax.set_ylabel("Percent Δ (treatment vs control)")
        ax.set_title(f"Waste sectors — {figure_prefix}")
        ax.legend()
        fig.tight_layout()
        fig.savefig(FIG / f"{figure_prefix}_waste_sectors_N_D_pct.png", dpi=150)
        plt.close(fig)

    summary = {
        "out_dir": str(out_dir),
        "figure_prefix": figure_prefix,
        "resolved_nowcast_mut_vintage": resolved_vintage,
        "n_stats": n_stats,
        "d_stats": d_stats,
        "waste_sectors": waste.reset_index().to_dict(orient="records"),
        "share_n_abs_gt_1pct": float((paired["N_perc_diff"].abs() > 0.01).mean()),
        "share_n_abs_gt_5pct": float((paired["N_perc_diff"].abs() > 0.05).mean()),
        "control_meta": json.loads(
            (out_dir / "control_meta.json").read_text(encoding="utf-8")
        ),
        "treatment_meta": json.loads(
            (out_dir / "treatment_meta.json").read_text(encoding="utf-8")
        ),
    }
    (out_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, default=str), encoding="utf-8"
    )
    print(json.dumps(summary, indent=2, default=str))


def _run_pair(
    *,
    year: int,
    control: str,
    treatment: str,
    mut_vintage_label: str,
) -> Path:
    out_dir = impact_cache_dir(year, mut_vintage_label)
    if out_dir.exists() and (out_dir / "summary.json").exists():
        print(
            f"NOTE: {out_dir} already has summary.json — will overwrite arm "
            "outputs in this directory only (local-MUT arms must use a different "
            "mut-vintage-label).",
            flush=True,
        )
    out_dir.mkdir(parents=True, exist_ok=True)
    py = sys.executable
    for tag, cfg in (("control", control), ("treatment", treatment)):
        print(f"=== Running {tag}: {cfg} -> {out_dir} ===", flush=True)
        subprocess.run(
            [py, "-m", MODULE, "worker", cfg, tag, str(out_dir)],
            check=True,
            cwd=str(ROOT),
        )
    prefix = f"impact_{year}_{_safe_vintage_label(mut_vintage_label)}"
    compare_and_plot(out_dir, figure_prefix=prefix)
    return out_dir


def main(argv: list[str]) -> int:
    if len(argv) >= 2 and argv[1] == "worker":
        if len(argv) < 5:
            raise SystemExit("worker requires: worker CONFIG TAG OUTDIR")
        _worker(argv[2], argv[3], Path(argv[4]))
        return 0
    if len(argv) >= 2 and argv[1] == "compare":
        if len(argv) < 3:
            raise SystemExit("compare requires: compare OUTDIR [--figure-prefix P]")
        out_dir = Path(argv[2])
        prefix = f"impact_{out_dir.name}"
        if "--figure-prefix" in argv:
            i = argv.index("--figure-prefix")
            prefix = argv[i + 1]
        compare_and_plot(out_dir, figure_prefix=prefix)
        return 0

    from bedrock.analysis.nowcasting.waste_disaggregation.impact_pins import (  # noqa: PLC0415
        GCS_MUT_VINTAGE,
        control_config_name,
        treatment_config_name,
    )

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument(
        "--control",
        default=None,
        help="Control USAConfig name (default: waste_weights_control)",
    )
    parser.add_argument(
        "--treatment",
        default=None,
        help="Treatment USAConfig name (default: waste_weights_match_io)",
    )
    parser.add_argument(
        "--mut-vintage-label",
        default=None,
        help=(
            "Cache-dir vintage label (default: GCS_MUT_VINTAGE). local-MUT arms must "
            "pass the recorded local vintage so GCS artifacts are not overwritten."
        ),
    )
    args = parser.parse_args(argv[1:])
    control = args.control or control_config_name(args.year)
    treatment = args.treatment or treatment_config_name(args.year)
    vintage_label = args.mut_vintage_label or GCS_MUT_VINTAGE
    _run_pair(
        year=args.year,
        control=control,
        treatment=treatment,
        mut_vintage_label=vintage_label,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
