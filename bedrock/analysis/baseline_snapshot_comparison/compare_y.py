"""Compare y_new (live) vs y_old (v0 snapshot) under 2025_usa_cornerstone_full_model."""
from __future__ import annotations

import logging
import os
import time

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("compare_y")

OUT = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUT, exist_ok=True)

from bedrock.utils.config.usa_config import set_global_usa_config

set_global_usa_config("2025_usa_cornerstone_full_model")

from bedrock.transform.eeio.derived import derive_y_for_national_accounting_balance_usa
from bedrock.utils.snapshots.loader import load_snapshot, resolve_snapshot_key

snap_key = resolve_snapshot_key()
log.info("snapshot key resolved: %s", snap_key)

log.info("computing y_new (live)...")
t0 = time.time()
y_new = derive_y_for_national_accounting_balance_usa().astype(float)
log.info("y_new computed in %.1fs, shape=%s", time.time() - t0, y_new.shape)

log.info("loading y_old from snapshot...")
y_old_obj = load_snapshot("y_nab_USA", snap_key)
y_old = (
    y_old_obj.iloc[:, 0].astype(float)
    if isinstance(y_old_obj, pd.DataFrame)
    else y_old_obj.astype(float)
)
log.info("y_old shape=%s", y_old.shape)

idx = y_new.index.union(y_old.index).sort_values()
a = y_new.reindex(idx)
b = y_old.reindex(idx)
diff = a.fillna(0) - b.fillna(0)
with np.errstate(divide="ignore", invalid="ignore"):
    pct_arr = np.where((b.notna()) & (b != 0), (a - b) / b, np.nan)
pct_ser = pd.Series(pct_arr, index=idx, name="pct")

df = pd.DataFrame(
    {"y_new": a, "y_old": b, "diff": diff, "abs_diff": diff.abs(), "pct": pct_ser},
    index=idx,
)
df.index.name = "sector"

tot_new = float(a.sum(skipna=True))
tot_old = float(b.sum(skipna=True))
both_idx = a.dropna().index.intersection(b.dropna().index)
summary = pd.DataFrame(
    {
        "metric": [
            "|sectors|",
            "both sides",
            "only y_new",
            "only y_old",
            "sum(y_new)",
            "sum(y_old)",
            "sum diff",
            "sum diff (%)",
            "max |diff|",
            "sector at max |diff|",
            "mean pct (where defined)",
            "median pct (where defined)",
        ],
        "value": [
            len(idx),
            len(both_idx),
            len(set(a.dropna().index) - set(b.dropna().index)),
            len(set(b.dropna().index) - set(a.dropna().index)),
            f"{tot_new:.3e}",
            f"{tot_old:.3e}",
            f"{tot_new - tot_old:.3e}",
            f"{(tot_new - tot_old) / tot_old * 100:.3f}%" if tot_old else "n/a",
            f"{df['abs_diff'].max():.3e}",
            str(df["abs_diff"].idxmax()),
            f"{np.nanmean(pct_arr) * 100:.3f}%",
            f"{np.nanmedian(pct_arr) * 100:.3f}%",
        ],
    }
)
print("\n=== SUMMARY ===")
print(summary.to_string(index=False))

df.to_csv(os.path.join(OUT, "y_per_sector.csv"))

plt.rcParams["figure.dpi"] = 120
plt.rcParams["savefig.dpi"] = 120

fig, ax = plt.subplots(figsize=(7, 7))
mask = a.notna() & b.notna()
ax.scatter(b[mask], a[mask], s=10, alpha=0.5)
lim_lo = min(b[mask].min(), a[mask].min())
lim_hi = max(b[mask].max(), a[mask].max())
ax.plot([lim_lo, lim_hi], [lim_lo, lim_hi], "r--", lw=1, label="y=x")
ax.set_xscale("symlog")
ax.set_yscale("symlog")
ax.set_xlabel("y_old (v0 snapshot)")
ax.set_ylabel("y_new (live)")
ax.set_title("y_new vs y_old — per sector (symlog)")
ax.legend()
ax.grid(True, which="both", ls=":", alpha=0.3)
fig.tight_layout()
fig.savefig(os.path.join(OUT, "y_scatter.png"))
plt.close(fig)

top = df.dropna(subset=["diff"]).nlargest(20, "abs_diff")
fig, ax = plt.subplots(figsize=(10, 7))
y_pos = np.arange(len(top))
ax.barh(y_pos, top["y_old"], color="gray", alpha=0.6, label="y_old")
ax.barh(y_pos, top["y_new"], height=0.4, color="tab:blue", alpha=0.9, label="y_new")
ax.set_yticks(y_pos)
ax.set_yticklabels([str(s)[:50] for s in top.index], fontsize=8)
ax.invert_yaxis()
ax.set_xlabel("y value")
ax.set_title("Top 20 sectors by |y_new − y_old|")
ax.legend()
ax.grid(True, axis="x", ls=":", alpha=0.3)
fig.tight_layout()
fig.savefig(os.path.join(OUT, "y_top20_levels.png"))
plt.close(fig)

fig, ax = plt.subplots(figsize=(10, 7))
ax.barh(
    y_pos,
    top["diff"],
    color=["tab:red" if v < 0 else "tab:green" for v in top["diff"]],
)
ax.set_yticks(y_pos)
ax.set_yticklabels([str(s)[:50] for s in top.index], fontsize=8)
ax.invert_yaxis()
ax.set_xlabel("y_new − y_old")
ax.set_title("Top 20 sectors by |y_new − y_old| (signed diff)")
ax.axvline(0, color="k", lw=0.5)
ax.grid(True, axis="x", ls=":", alpha=0.3)
fig.tight_layout()
fig.savefig(os.path.join(OUT, "y_top20_diffs.png"))
plt.close(fig)

finite_pct = pct_arr[np.isfinite(pct_arr)]
fig, ax = plt.subplots(figsize=(9, 5))
ax.hist(np.clip(finite_pct * 100, -100, 100), bins=60, color="tab:blue", alpha=0.8)
ax.axvline(0, color="k", lw=0.5)
ax.set_xlabel("% diff = (y_new − y_old) / y_old × 100 (clipped to ±100%)")
ax.set_ylabel("sector count")
ax.set_title(f"Distribution of per-sector % diffs (n={len(finite_pct)})")
ax.grid(True, ls=":", alpha=0.3)
fig.tight_layout()
fig.savefig(os.path.join(OUT, "y_pct_hist.png"))
plt.close(fig)

q_med = df["y_old"].abs().quantile(0.5)
meaningful: pd.DataFrame = df.loc[
    (df["y_old"].abs() > q_med) & df["pct"].notna()
].copy()
meaningful["abs_pct_val"] = meaningful["pct"].astype(float).abs()  # type: ignore[union-attr]
top_pct = meaningful.nlargest(20, "abs_pct_val")  # type: ignore[call-overload]
fig, ax = plt.subplots(figsize=(10, 7))
y_pos = np.arange(len(top_pct))
ax.barh(
    y_pos,
    top_pct["pct"] * 100,
    color=["tab:red" if v < 0 else "tab:green" for v in top_pct["pct"]],
)
ax.set_yticks(y_pos)
ax.set_yticklabels([str(s)[:50] for s in top_pct.index], fontsize=8)
ax.invert_yaxis()
ax.set_xlabel("% diff (restricted to y_old above median |y_old|)")
ax.set_title("Top 20 sectors by |% diff| (meaningful denominators only)")
ax.axvline(0, color="k", lw=0.5)
ax.grid(True, axis="x", ls=":", alpha=0.3)
fig.tight_layout()
fig.savefig(os.path.join(OUT, "y_top20_pct.png"))
plt.close(fig)

print(f"\nOutputs written to: {OUT}")
