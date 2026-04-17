"""Decompose d = colsum(B) into E and x contributions (industry space).

B is computed as `Bi = E / x` in industry space, then `B = Bi @ Vnorm` in
commodity space. For sectors where industry ≈ commodity (e.g. electricity
221100), industry-space d ≈ commodity-space d.

We compare:
- E: live derive_E_usa() vs v0 E_USA_ES snapshot
- x: live x (flag ON, derive_cornerstone_x_after_redefinition) vs live x
  (flag OFF, derive_cornerstone_x()). v0 predates the flag, so flag-off x
  computed today is the closest approximation to the x v0 used.

Additive log decomposition:
    ln(Bi_col_new / Bi_col_old) = ln(E_col_new / E_col_old) − ln(x_new / x_old)
"""
from __future__ import annotations

import logging
import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("decompose_d_E_x")

OUT = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUT, exist_ok=True)

from bedrock.utils.config.usa_config import set_global_usa_config

set_global_usa_config("2025_usa_cornerstone_full_model")

from bedrock.transform.allocation.derived import derive_E_usa
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_x,
    derive_cornerstone_x_after_redefinition,
)
from bedrock.utils.snapshots.loader import load_configured_snapshot

log.info("computing live E, both x's...")
E_new = derive_E_usa()
x_new = derive_cornerstone_x_after_redefinition()  # flag ON (live convention)
x_old = derive_cornerstone_x()  # flag OFF (v0 convention approximation)
log.info("E_new=%s  x_new=%s  x_old=%s", E_new.shape, x_new.shape, x_old.shape)

log.info("loading v0 E_USA_ES snapshot...")
E_old = load_configured_snapshot("E_USA_ES")
log.info("E_old=%s", E_old.shape)

# Reduce E to per-industry (sum over ghg rows)
E_new_col = E_new.sum(axis=0).astype(float)
E_old_col = E_old.sum(axis=0).astype(float)

# Industry-space direct-intensity d_i = E_col_i / x_i
idx = (
    E_new_col.index.union(E_old_col.index).union(x_new.index).union(x_old.index)
).sort_values()
E_n = E_new_col.reindex(idx)
E_o = E_old_col.reindex(idx)
x_n = x_new.reindex(idx).astype(float)
x_o = x_old.reindex(idx).astype(float)

with np.errstate(divide="ignore", invalid="ignore"):
    Bi_n = E_n / x_n
    Bi_o = E_o / x_o
    ln_Bi = np.log(Bi_n / Bi_o)
    ln_E = np.log(E_n / E_o)
    ln_x = np.log(x_n / x_o)
    residual = ln_Bi - (ln_E - ln_x)

tbl = pd.DataFrame(
    {
        "E_old": E_o,
        "E_new": E_n,
        "E_pct": (E_n / E_o - 1) * 100,
        "x_old": x_o,
        "x_new": x_n,
        "x_pct": (x_n / x_o - 1) * 100,
        "Bi_old": Bi_o,
        "Bi_new": Bi_n,
        "Bi_pct": (Bi_n / Bi_o - 1) * 100,
        "dln_Bi": ln_Bi,
        "dln_E": ln_E,
        "minus_dln_x": -ln_x,
        "residual_ln": residual,
    },
    index=idx,
)
tbl.index.name = "sector"
tbl.to_csv(os.path.join(OUT, "d_E_x_decomposition.csv"))

# Focus: 221100
print("\n=== Sector 221100 (Electricity) — industry-space E / x decomposition ===")
if "221100" in tbl.index:
    r = tbl.loc["221100"]
    print(f"  E_old             = {r['E_old']:.4e}")
    print(f"  E_new             = {r['E_new']:.4e}   ({r['E_pct']:+.2f}%)")
    print(f"  x_old             = {r['x_old']:.4e}")
    print(f"  x_new             = {r['x_new']:.4e}   ({r['x_pct']:+.2f}%)")
    print(f"  Bi_old = E/x old  = {r['Bi_old']:.4e}")
    print(f"  Bi_new = E/x new  = {r['Bi_new']:.4e}   ({r['Bi_pct']:+.2f}%)")
    print()
    print(f"  Δln(Bi)     = {r['dln_Bi']:+.4f}   (= target)")
    print(f"  Δln(E)      = {r['dln_E']:+.4f}   (E-contribution)")
    print(f"  −Δln(x)     = {r['minus_dln_x']:+.4f}   (x-contribution)")
    print(f"  residual    = {r['residual_ln']:+.4e}   (should be ~0)")
    if abs(r["dln_Bi"]) > 1e-12:
        share_E = r["dln_E"] / r["dln_Bi"] * 100
        share_x = r["minus_dln_x"] / r["dln_Bi"] * 100
        print(f"\n  => E drives  {share_E:+.1f}% of the Bi change")
        print(f"  => x drives  {share_x:+.1f}% of the Bi change")

# Top sectors by |Δln(Bi)|
print("\n=== Top 15 sectors by |Δln(Bi)| (both sides present, finite) ===")
finite: pd.DataFrame = tbl.dropna(subset=["dln_Bi", "dln_E", "minus_dln_x"]).copy()
finite = finite.loc[np.isfinite(finite["dln_Bi"])]
finite["abs_dln_Bi"] = finite["dln_Bi"].astype(float).abs()  # type: ignore[union-attr]
top = finite.nlargest(15, "abs_dln_Bi")  # type: ignore[call-overload]
print(top[["Bi_old", "Bi_new", "dln_Bi", "dln_E", "minus_dln_x", "residual_ln"]].to_string())  # type: ignore[call-overload]

# Visual: per-sector stacked bar — Δln(Bi) decomposed into E and -x for top 20
top20 = finite.nlargest(20, "abs_dln_Bi").copy()  # type: ignore[call-overload]
fig, ax = plt.subplots(figsize=(11, 8))
y_pos = np.arange(len(top20))
ax.barh(y_pos, top20["dln_E"], color="tab:orange", alpha=0.85, label="Δln(E)")
ax.barh(
    y_pos,
    top20["minus_dln_x"],
    left=top20["dln_E"],
    color="tab:purple",
    alpha=0.85,
    label="−Δln(x)",
)
ax.plot(
    top20["dln_Bi"],
    y_pos,
    "k|",
    markersize=14,
    markeredgewidth=2,
    label="Δln(Bi) (target)",
)
ax.set_yticks(y_pos)
ax.set_yticklabels([str(s) for s in top20.index], fontsize=9)
ax.invert_yaxis()
ax.axvline(0, color="k", lw=0.5)
ax.set_xlabel("log ratio (new/old)")
ax.set_title("Decomposition of Δln(Bi) = Δln(E) − Δln(x)  — top 20 sectors by |Δln(Bi)|")
ax.legend(loc="best")
ax.grid(True, axis="x", ls=":", alpha=0.3)
fig.tight_layout()
fig.savefig(os.path.join(OUT, "d_E_x_decomposition_top20.png"), dpi=120)
plt.close(fig)

# E vs x %-change scatter
fig, ax = plt.subplots(figsize=(7, 7))
mask = np.isfinite(tbl["E_pct"]) & np.isfinite(tbl["x_pct"])
ax.scatter(tbl.loc[mask, "x_pct"], tbl.loc[mask, "E_pct"], s=10, alpha=0.5)
if "221100" in tbl.index:
    r = tbl.loc["221100"]
    ax.scatter([r["x_pct"]], [r["E_pct"]], s=80, color="tab:red", zorder=5, label="221100")
    ax.annotate(
        "221100",
        (r["x_pct"], r["E_pct"]),
        textcoords="offset points",
        xytext=(8, 6),
        color="tab:red",
    )
ax.axhline(0, color="k", lw=0.5)
ax.axvline(0, color="k", lw=0.5)
ax.set_xlabel("x % change (new/old − 1)")
ax.set_ylabel("E % change (new/old − 1)")
ax.set_title("Per-industry: E change vs x change (clipped)")
ax.set_xlim(-100, 200)
ax.set_ylim(-100, 200)
ax.legend()
ax.grid(True, ls=":", alpha=0.3)
fig.tight_layout()
fig.savefig(os.path.join(OUT, "d_E_x_scatter.png"), dpi=120)
plt.close(fig)

print(f"\nOutputs written to: {OUT}")
