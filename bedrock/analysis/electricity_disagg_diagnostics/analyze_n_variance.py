"""Analyze why total-EF (N) rises for downstream sectors at the 3-way split.

Decomposes ``N_j = sum_i D_i * L_ij`` for the v0.2 footing and the 3-way
electricity split, isolating the electricity-supply-chain contribution so we
can test whether the per-sector N change is driven by each sector's
electricity share of N (user hypothesis) and why the change is always positive.

Run:
    python -m bedrock.analysis.electricity_disagg_diagnostics.analyze_n_variance
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

import numpy as np
import pandas as pd

from bedrock.analysis.electricity_disagg_diagnostics.full_trace import (
    _clear_model_caches,
)
from bedrock.analysis.electricity_disagg_diagnostics.paths import OUT_DIR
from bedrock.publish.model_objects import get_D, get_L, get_N
from bedrock.utils.config.usa_config import reset_usa_config, set_global_usa_config
from bedrock.utils.schemas.cornerstone_schemas import (
    ELECTRICITY_AGGREGATE_SECTOR,
    ELECTRICITY_DISAGG_SECTORS,
)

FOOTING_CONFIG = "2025_usa_cornerstone_v0_2"
SPLIT_CONFIG = "2025_usa_cornerstone_v0_2_electricity_disaggregation"

FOOTING_ELEC = [ELECTRICITY_AGGREGATE_SECTOR]
SPLIT_ELEC = list(ELECTRICITY_DISAGG_SECTORS)


@dataclass
class ModelVectors:
    d: pd.Series  # characterized direct EF (kg CO2e / USD), index=sector
    n: pd.Series  # characterized total EF (kg CO2e / USD), index=sector
    ell: pd.DataFrame  # Leontief inverse L (index=input i, cols=output j)


def _series_over_sectors(df: pd.DataFrame) -> pd.Series:
    """Collapse an (impact x sector) frame to a per-sector CO2e Series."""
    s = df.sum(axis=0)
    s.index = s.index.astype(str)
    return s.astype(float)


def load_model(config: str) -> ModelVectors:
    reset_usa_config()
    _clear_model_caches()
    set_global_usa_config(config)
    d = _series_over_sectors(get_D())
    n = _series_over_sectors(get_N())
    ell = get_L().copy()
    ell.index = ell.index.astype(str)
    ell.columns = ell.columns.astype(str)
    return ModelVectors(d=d, n=n, ell=ell)


def elec_contribution(mv: ModelVectors, elec_sectors: list[str]) -> pd.Series:
    """C_elec_j = sum_{i in elec} D_i * L_ij (kg CO2e / USD of j)."""
    present = [s for s in elec_sectors if s in mv.ell.index and s in mv.d.index]
    d_sub = mv.d.reindex(present).to_numpy()
    l_sub = mv.ell.loc[present]  # (elec x j)
    contrib = pd.Series(d_sub @ l_sub.to_numpy(), index=l_sub.columns, dtype=float)
    return contrib


def elec_dollars_embodied(mv: ModelVectors, elec_sectors: list[str]) -> pd.Series:
    """L_elec_j = sum_{i in elec} L_ij (electricity $ embodied per $ of j)."""
    present = [s for s in elec_sectors if s in mv.ell.index]
    return mv.ell.loc[present].sum(axis=0).astype(float)


def build_analysis() -> tuple[pd.DataFrame, ModelVectors, ModelVectors]:
    foot = load_model(FOOTING_CONFIG)
    split = load_model(SPLIT_CONFIG)

    # Verify the decomposition identity N_j == sum_i D_i L_ij on the footing.
    n_check = pd.Series(
        foot.d.reindex(foot.ell.index).to_numpy() @ foot.ell.to_numpy(),
        index=foot.ell.columns,
    )
    max_resid = float((n_check - foot.n.reindex(n_check.index)).abs().max())
    print(f"footing N decomposition max |resid| = {max_resid:.3e} (should be ~0)")

    celec_foot = elec_contribution(foot, FOOTING_ELEC)
    celec_split = elec_contribution(split, SPLIT_ELEC)
    ldollars_foot = elec_dollars_embodied(foot, FOOTING_ELEC)
    ldollars_split = elec_dollars_embodied(split, SPLIT_ELEC)

    # Restrict to non-electricity sectors present in both models.
    drop = set(FOOTING_ELEC) | set(SPLIT_ELEC)
    common = [s for s in foot.n.index if s in split.n.index and s not in drop]

    df = pd.DataFrame(index=pd.Index(common, name="sector"))
    df["N_foot"] = foot.n.reindex(common)
    df["N_split"] = split.n.reindex(common)
    df["D_foot"] = foot.d.reindex(common)
    df["D_split"] = split.d.reindex(common)
    df["Celec_foot"] = celec_foot.reindex(common)
    df["Celec_split"] = celec_split.reindex(common)
    df["Lelec_foot"] = ldollars_foot.reindex(common)
    df["Lelec_split"] = ldollars_split.reindex(common)
    df = df.dropna(subset=["N_foot", "N_split"])
    df = df[df["N_foot"] > 0]

    df["dN"] = df["N_split"] - df["N_foot"]
    df["dN_pct"] = df["dN"] / df["N_foot"]
    df["dD"] = df["D_split"] - df["D_foot"]
    df["dD_pct"] = df["dD"] / df["D_foot"].where(df["D_foot"] != 0, np.nan)
    df["elec_share_N"] = df["Celec_foot"] / df["N_foot"]
    df["Crest_foot"] = df["N_foot"] - df["Celec_foot"]
    df["Crest_split"] = df["N_split"] - df["Celec_split"]
    df["dCelec"] = df["Celec_split"] - df["Celec_foot"]
    df["dCrest"] = df["Crest_split"] - df["Crest_foot"]
    # Effective embodied electricity intensity (kg CO2e per $ of embodied elec).
    df["eff_int_foot"] = df["Celec_foot"] / df["Lelec_foot"].where(
        df["Lelec_foot"] != 0, np.nan
    )
    df["eff_int_split"] = df["Celec_split"] / df["Lelec_split"].where(
        df["Lelec_split"] != 0, np.nan
    )
    return df, foot, split


def detail(df: pd.DataFrame, foot: ModelVectors, split: ModelVectors) -> None:
    print("\n=== Electricity-sector DIRECT EF (kg CO2e / USD) ===")
    print(f"footing 221100          D = {foot.d[ELECTRICITY_AGGREGATE_SECTOR]:.4f}")
    for code in SPLIT_ELEC:
        print(f"split   {code}          D = {split.d[code]:.4f}")

    print("\n=== Worked examples: N = Celec + Crest ===")
    examples = ["452000", "447000", "1121A0", "562212"]
    for s in examples:
        if s not in df.index:
            continue
        r = df.loc[s]
        print(f"\nsector {s}")
        print(
            f"  footing: N={r['N_foot']:.4f} = Celec {r['Celec_foot']:.4f} "
            f"+ Crest {r['Crest_foot']:.4f}  (elec share {r['elec_share_N']:.3f})"
        )
        print(
            f"  split  : N={r['N_split']:.4f} = Celec {r['Celec_split']:.4f} "
            f"+ Crest {r['Crest_split']:.4f}"
        )
        print(
            f"  dN%={r['dN_pct']:.4f}  dCelec={r['dCelec']:.4f}  "
            f"dCrest={r['dCrest']:.4f}"
        )
        # Raw L contributions into this column.
        lf = float(cast(float, foot.ell.at[ELECTRICITY_AGGREGATE_SECTOR, s]))
        d_agg = float(foot.d[ELECTRICITY_AGGREGATE_SECTOR])
        print(f"  L_foot[221100->{s}] = {lf:.5f}  (x D {d_agg:.4f})")
        for code in SPLIT_ELEC:
            ls = (
                float(cast(float, split.ell.at[code, s]))
                if code in split.ell.index
                else 0.0
            )
            print(f"  L_split[{code}->{s}] = {ls:.5f}  (x D {split.d[code]:.4f})")

    print("\n=== Negative / smallest dN_pct outliers ===")
    print(
        df.sort_values("dN_pct")
        .head(5)[["N_foot", "N_split", "dN_pct", "elec_share_N", "dD_pct"]]
        .round(4)
        .to_string()
    )


def summarize(df: pd.DataFrame) -> None:
    n = len(df)
    print("\n=== Sample ===")
    print(f"non-electricity sectors analyzed: {n}")

    print("\n=== Point 2: sign of dN ===")
    pos = int((df["dN"] > 0).sum())
    neg = int((df["dN"] < 0).sum())
    print(f"dN > 0: {pos} ({pos / n:.1%}) | dN < 0: {neg} ({neg / n:.1%})")
    print("dN_pct quantiles:")
    print(df["dN_pct"].quantile([0.0, 0.25, 0.5, 0.75, 1.0]).round(4).to_string())

    print("\n=== D barely moves (direct EF) ===")
    print(
        f"median |dD_pct| = {df['dD_pct'].abs().median():.4%} | "
        f"max |dD_pct| = {df['dD_pct'].abs().max():.4%}"
    )

    print("\n=== Decomposition: dN = dCelec + dCrest ===")
    print(
        f"sum dN     = {df['dN'].sum():.3f}\n"
        f"sum dCelec = {df['dCelec'].sum():.3f} "
        f"({df['dCelec'].sum() / df['dN'].sum():.1%} of dN)\n"
        f"sum dCrest = {df['dCrest'].sum():.3f} "
        f"({df['dCrest'].sum() / df['dN'].sum():.1%} of dN)"
    )
    print(
        f"median |dCrest / dN| = "
        f"{(df['dCrest'].abs() / df['dN'].abs()).median():.2%}"
    )

    print("\n=== Hypothesis: dN_pct ~ elec_share_N ===")
    valid = df.dropna(subset=["elec_share_N", "dN_pct"])
    pear = float(np.corrcoef(valid["elec_share_N"], valid["dN_pct"])[0, 1])
    spear = float(
        cast(
            float, valid[["elec_share_N", "dN_pct"]].corr(method="spearman").iloc[0, 1]
        )
    )
    # slope through origin
    x = valid["elec_share_N"].to_numpy(dtype=float)
    y = valid["dN_pct"].to_numpy(dtype=float)
    k = float((x * y).sum() / (x * x).sum())
    resid = y - k * x
    ss_res = float((resid**2).sum())
    ss_tot = float((y**2).sum())
    r2_origin = 1.0 - ss_res / ss_tot
    print(f"Pearson r  = {pear:.4f}")
    print(f"Spearman r = {spear:.4f}")
    print(f"slope k (through origin) = {k:.4f}; R^2(origin) = {r2_origin:.4f}")

    print("\n=== Effective embodied electricity intensity (kg CO2e / $ elec) ===")
    print(
        f"footing median = {df['eff_int_foot'].median():.4f}\n"
        f"split   median = {df['eff_int_split'].median():.4f}\n"
        f"ratio (split/foot) median = "
        f"{(df['eff_int_split'] / df['eff_int_foot']).median():.4f}"
    )
    print(
        "electricity $ embodied ratio (Lelec_split/Lelec_foot) median = "
        f"{(df['Lelec_split'] / df['Lelec_foot']).median():.4f}"
    )
    print(
        "Celec ratio (split/foot) median = "
        f"{(df['Celec_split'] / df['Celec_foot']).median():.4f}"
    )

    print("\n=== Highest electricity-share sectors ===")
    top = df.sort_values("elec_share_N", ascending=False).head(12)
    print(
        top[["N_foot", "N_split", "dN_pct", "elec_share_N", "dD_pct"]]
        .round(4)
        .to_string()
    )

    print("\n=== Lowest electricity-share sectors (with dN_pct) ===")
    low = df.sort_values("elec_share_N").head(12)
    print(
        low[["N_foot", "N_split", "dN_pct", "elec_share_N", "dD_pct"]]
        .round(4)
        .to_string()
    )


def main() -> None:
    df, foot, split = build_analysis()
    out_csv = OUT_DIR / "ef" / "panel" / "n_variance_analysis.csv"
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv)
    print(f"Wrote {out_csv}")
    summarize(df)
    detail(df, foot, split)


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.WARNING, format="%(message)s")
    main()
