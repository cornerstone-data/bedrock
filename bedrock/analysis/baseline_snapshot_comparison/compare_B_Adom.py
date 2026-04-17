"""Compare B and Adom (live vs v0 snapshot) under 2025_usa_cornerstone_full_model."""

from __future__ import annotations

import logging
import os
import time

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(message)s')
log = logging.getLogger('compare_ba')

OUT = os.path.join(os.path.dirname(__file__), 'output')
os.makedirs(OUT, exist_ok=True)

from bedrock.utils.config.usa_config import set_global_usa_config  # noqa: E402

set_global_usa_config('2025_usa_cornerstone_full_model')

from bedrock.transform.eeio.derived import (  # noqa: E402
    derive_Aq_usa,
    derive_B_usa_non_finetuned,
)
from bedrock.utils.snapshots.loader import (  # noqa: E402
    load_snapshot,
    resolve_snapshot_key,
)

snap_key = resolve_snapshot_key()
log.info('snap key: %s', snap_key)

log.info('computing B_new, Aq (live)...')
t0 = time.time()
B_new = derive_B_usa_non_finetuned()
Aq = derive_Aq_usa()
Adom_new = Aq.Adom
log.info(
    'live matrices in %.1fs — B_new=%s, Adom_new=%s',
    time.time() - t0,
    B_new.shape,
    Adom_new.shape,
)

log.info('loading B_old, Adom_old from v0...')
B_old = load_snapshot('B_USA_non_finetuned', snap_key)
Adom_old = load_snapshot('Adom_USA', snap_key)
log.info('snapshot shapes — B_old=%s, Adom_old=%s', B_old.shape, Adom_old.shape)


def pct(a_arr: np.ndarray, b_arr: np.ndarray) -> np.ndarray:
    with np.errstate(divide='ignore', invalid='ignore'):
        return np.where(
            np.isfinite(b_arr) & (b_arr != 0), (a_arr - b_arr) / b_arr, np.nan
        )


def compare_series(a: pd.Series, b: pd.Series, name: str) -> pd.DataFrame:
    idx = a.index.union(b.index).sort_values()
    a_r = a.reindex(idx)
    b_r = b.reindex(idx)
    diff = a_r.fillna(0) - b_r.fillna(0)
    p = pct(a_r.to_numpy(dtype=float), b_r.to_numpy(dtype=float))
    df = pd.DataFrame(
        {'new': a_r, 'old': b_r, 'diff': diff, 'abs_diff': diff.abs(), 'pct': p},
        index=idx,
    )
    df.index.name = 'sector'

    tot_n, tot_o = float(a_r.sum(skipna=True)), float(b_r.sum(skipna=True))
    only_new = sorted(set(a.dropna().index) - set(b.dropna().index))
    only_old = sorted(set(b.dropna().index) - set(a.dropna().index))
    print(f'\n=== {name} SUMMARY ===')
    print(
        f'|sectors| union={len(idx)}  new_only={len(only_new)}  old_only={len(only_old)}'
    )
    print(
        f'sum(new)={tot_n:.3e}  sum(old)={tot_o:.3e}  diff={tot_n - tot_o:.3e}  '
        f'({(tot_n - tot_o) / tot_o * 100 if tot_o else 0:.3f}%)'
    )
    print(f'max |diff|={df["abs_diff"].max():.3e} at {df["abs_diff"].idxmax()}')
    finite_p = p[np.isfinite(p)]
    print(
        f'pct diff (n={len(finite_p)}):  mean={np.mean(finite_p) * 100:.4f}%  '
        f'median={np.median(finite_p) * 100:.4f}%  max|pct|={np.max(np.abs(finite_p)) * 100:.3f}%'
    )

    df.to_csv(os.path.join(OUT, f'{name}_per_sector.csv'))

    fig, ax = plt.subplots(figsize=(6.5, 6.5))
    mask = a_r.notna() & b_r.notna()
    ax.scatter(b_r[mask], a_r[mask], s=8, alpha=0.4)
    lo = min(b_r[mask].min(), a_r[mask].min())
    hi = max(b_r[mask].max(), a_r[mask].max())
    ax.plot([lo, hi], [lo, hi], 'r--', lw=1, label='y=x')
    ax.set_xscale('symlog')
    ax.set_yscale('symlog')
    ax.set_xlabel(f'{name}_old (v0)')
    ax.set_ylabel(f'{name}_new (live)')
    ax.set_title(f'{name}: new vs old — per sector (symlog)')
    ax.legend()
    ax.grid(True, which='both', ls=':', alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, f'{name}_scatter.png'), dpi=120)
    plt.close(fig)

    top = df.dropna(subset=['diff']).nlargest(20, 'abs_diff')
    fig, ax = plt.subplots(figsize=(10, 7))
    y_pos = np.arange(len(top))
    ax.barh(
        y_pos,
        top['diff'],
        color=['tab:red' if v < 0 else 'tab:green' for v in top['diff']],
    )
    ax.set_yticks(y_pos)
    ax.set_yticklabels([str(s)[:50] for s in top.index], fontsize=8)
    ax.invert_yaxis()
    ax.set_xlabel(f'{name}_new − {name}_old')
    ax.set_title(f'Top 20 sectors by |{name}_new − {name}_old|')
    ax.axvline(0, color='k', lw=0.5)
    ax.grid(True, axis='x', ls=':', alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, f'{name}_top20_diff.png'), dpi=120)
    plt.close(fig)

    finite = p[np.isfinite(p)]
    fig, ax = plt.subplots(figsize=(9, 4.5))
    ax.hist(np.clip(finite * 100, -100, 100), bins=60, color='tab:blue', alpha=0.8)
    ax.axvline(0, color='k', lw=0.5)
    ax.set_xlabel(
        f'% diff = ({name}_new − {name}_old) / {name}_old × 100 (clipped ±100%)'
    )
    ax.set_ylabel('sector count')
    ax.set_title(f'{name}: per-sector % diff distribution (n={len(finite)})')
    ax.grid(True, ls=':', alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, f'{name}_pct_hist.png'), dpi=120)
    plt.close(fig)

    return df


# === B: d = column sums (direct emission intensity per $ output per sector) ===
def column_sum_to_sector_series(M: pd.DataFrame, label: str) -> pd.Series:
    n_y = 405
    if M.shape[1] == n_y or abs(M.shape[1] - n_y) < abs(M.shape[0] - n_y):
        s = M.sum(axis=0)
    else:
        s = M.sum(axis=1)
    log.info('[%s] shape=%s  result_len=%d', label, M.shape, len(s))
    return s.astype(float)


d_new = column_sum_to_sector_series(B_new, 'B_new')
d_old = column_sum_to_sector_series(B_old, 'B_old')
compare_series(d_new, d_old, 'd_B_colsum')

common_rows = B_new.index.intersection(B_old.index)
common_cols = B_new.columns.intersection(B_old.columns)
bn = B_new.loc[common_rows, common_cols].astype(float).to_numpy()
bo = B_old.loc[common_rows, common_cols].astype(float).to_numpy()
B_elem_diff = bn - bo
print('\n=== B RAW ELEMENT-WISE (intersection) ===')
print(f'intersection shape: {bn.shape}')
print(f'max |diff|: {np.abs(B_elem_diff).max():.3e}')
print(
    f'frobenius(diff) / frobenius(old): '
    f'{np.linalg.norm(B_elem_diff) / np.linalg.norm(bo):.3e}'
)
finite_bp = pct(bn.ravel(), bo.ravel())
finite_bp = finite_bp[np.isfinite(finite_bp)]
print(
    f'nonzero-cell % diff: n={len(finite_bp)}  mean={np.mean(finite_bp) * 100:.4f}%  '
    f'median={np.median(finite_bp) * 100:.4f}%  max|%|={np.max(np.abs(finite_bp)) * 100:.3f}%'
)

# === Adom: column sums ===
cs_new = Adom_new.sum(axis=0).astype(float)
cs_old = Adom_old.sum(axis=0).astype(float)
compare_series(cs_new, cs_old, 'Adom_colsum')

common_rows_a = Adom_new.index.intersection(Adom_old.index)
common_cols_a = Adom_new.columns.intersection(Adom_old.columns)
an = Adom_new.loc[common_rows_a, common_cols_a].astype(float).to_numpy()
ao = Adom_old.loc[common_rows_a, common_cols_a].astype(float).to_numpy()
A_elem_diff = an - ao
print('\n=== Adom RAW ELEMENT-WISE (intersection) ===')
print(f'intersection shape: {an.shape}')
print(f'max |diff|: {np.abs(A_elem_diff).max():.3e} (entries in [0,1])')
print(
    f'frobenius(diff) / frobenius(old): '
    f'{np.linalg.norm(A_elem_diff) / np.linalg.norm(ao):.3e}'
)
finite_ap = pct(an.ravel(), ao.ravel())
finite_ap = finite_ap[np.isfinite(finite_ap)]
print(
    f'nonzero-cell % diff: n={len(finite_ap)}  mean={np.mean(finite_ap) * 100:.4f}%  '
    f'median={np.median(finite_ap) * 100:.4f}%  max|%|={np.max(np.abs(finite_ap)) * 100:.3f}%'
)

fig, ax = plt.subplots(figsize=(8, 7))
with np.errstate(divide='ignore'):
    hm = np.log10(np.abs(A_elem_diff) + 1e-20)
im = ax.imshow(hm, cmap='viridis', aspect='auto', vmin=-10, vmax=0)
ax.set_xlabel('consuming sector (col)')
ax.set_ylabel('producing sector (row)')
ax.set_title('log10 |Adom_new − Adom_old| element-wise')
fig.colorbar(im, ax=ax, label='log10 |diff|')
fig.tight_layout()
fig.savefig(os.path.join(OUT, 'Adom_diff_heatmap.png'), dpi=120)
plt.close(fig)

print(f'\nOutputs written to: {OUT}')
