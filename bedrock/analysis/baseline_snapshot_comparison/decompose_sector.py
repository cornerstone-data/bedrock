"""Decompose BLy drift for a single sector into d, L·y, and residual contributions."""

from __future__ import annotations

import logging
import os
import sys

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(message)s')
log = logging.getLogger('decompose')

SECTOR = sys.argv[1] if len(sys.argv) > 1 else '221100'

from bedrock.utils.config.usa_config import set_global_usa_config  # noqa: E402

set_global_usa_config('2025_usa_cornerstone_full_model')

from bedrock.transform.eeio.derived import (  # noqa: E402
    derive_Aq_usa,
    derive_B_usa_non_finetuned,
    derive_y_for_national_accounting_balance_usa,
)
from bedrock.utils.math.formulas import compute_d, compute_L_matrix  # noqa: E402
from bedrock.utils.snapshots.loader import (  # noqa: E402
    load_snapshot,
    resolve_snapshot_key,
)

snap_key = resolve_snapshot_key()

log.info('computing live B, Adom, y...')
B_new = derive_B_usa_non_finetuned()
Adom_new = derive_Aq_usa().Adom
y_new = derive_y_for_national_accounting_balance_usa().astype(float)

log.info('loading v0 snapshot B, Adom, y...')
B_old = load_snapshot('B_USA_non_finetuned', snap_key)
Adom_old = load_snapshot('Adom_USA', snap_key)
y_old_obj = load_snapshot('y_nab_USA', snap_key)
y_old = (
    y_old_obj.iloc[:, 0].astype(float)
    if isinstance(y_old_obj, pd.DataFrame)
    else y_old_obj.astype(float)
)

log.info('computing d, L, Ly, BLy for both sides...')
d_new = compute_d(B=B_new)
d_old = compute_d(B=B_old)
L_new = compute_L_matrix(A=Adom_new)
L_old = compute_L_matrix(A=Adom_old)
Ly_new = L_new @ y_new
Ly_old = L_old @ y_old
BLy_new = d_new * Ly_new
BLy_old = d_old * Ly_old


def at(s: pd.Series, sec: str) -> float:
    if sec in s.index:
        return float(s.loc[sec])
    return float('nan')


print(f'\n=== Decomposition for sector {SECTOR} ===')
print(f'{"metric":<22} {"old":>15} {"new":>15} {"diff":>15} {"pct":>10}')


def row(label: str, new_v: float, old_v: float) -> None:
    diff = new_v - old_v
    p = (
        (diff / old_v * 100)
        if old_v not in (0, float('nan')) and not np.isnan(old_v)
        else float('nan')
    )
    print(f'{label:<22} {old_v:>15.4e} {new_v:>15.4e} {diff:>15.4e} {p:>9.2f}%')


row('y_j', at(y_new, SECTOR), at(y_old, SECTOR))
row('d_j (direct int.)', at(d_new, SECTOR), at(d_old, SECTOR))
row('(Ly)_j (total out.)', at(Ly_new, SECTOR), at(Ly_old, SECTOR))
row('BLy_j = d_j·(Ly)_j', at(BLy_new, SECTOR), at(BLy_old, SECTOR))

# Counterfactuals: hold one side fixed at old, move only the other
try:
    d_o = at(d_old, SECTOR)
    d_n = at(d_new, SECTOR)
    ly_o = at(Ly_old, SECTOR)
    ly_n = at(Ly_new, SECTOR)
    bly_o = d_o * ly_o
    bly_n = d_n * ly_n
    only_d = d_n * ly_o
    only_ly = d_o * ly_n
    delta_total = bly_n - bly_o
    delta_from_d = only_d - bly_o
    delta_from_ly = only_ly - bly_o
    interaction = delta_total - delta_from_d - delta_from_ly
    print('\n=== Contribution decomposition (BLy_new - BLy_old) ===')
    print(
        f'  from d change alone:       {delta_from_d:>15.4e}  ({delta_from_d / delta_total * 100 if delta_total else 0:>6.1f}%)'
    )
    print(
        f'  from (Ly) change alone:    {delta_from_ly:>15.4e}  ({delta_from_ly / delta_total * 100 if delta_total else 0:>6.1f}%)'
    )
    print(
        f'  interaction term:          {interaction:>15.4e}  ({interaction / delta_total * 100 if delta_total else 0:>6.1f}%)'
    )
    print(f'  total:                     {delta_total:>15.4e}')
except Exception as e:
    log.warning('decomposition failed: %s', e)

# What drives (Ly)_j? It's row j of L times y. Break into L contribution vs y contribution.
# (Ly_new - Ly_old)_j = sum_k (L_new[j,k] * y_new[k] - L_old[j,k] * y_old[k])
if SECTOR in L_new.index and SECTOR in L_old.index:
    common_k = (
        y_new.index.intersection(y_old.index)
        .intersection(L_new.columns)
        .intersection(L_old.columns)
    )
    L_new_row = L_new.loc[SECTOR].reindex(common_k).astype(float).to_numpy()
    L_old_row = L_old.loc[SECTOR].reindex(common_k).astype(float).to_numpy()
    y_new_k = y_new.reindex(common_k).to_numpy()
    y_old_k = y_old.reindex(common_k).to_numpy()
    ly_diff_per_k = pd.Series(
        L_new_row * y_new_k - L_old_row * y_old_k,
        index=common_k,
        name='Ly_diff_contribution',
    )
    print(f'\n=== Top 10 upstream sectors k driving Δ(Ly)_{SECTOR} ===')
    top_k = ly_diff_per_k.reindex(
        ly_diff_per_k.abs().sort_values(ascending=False).index
    ).head(10)
    print(top_k.to_string())

OUT = os.path.join(os.path.dirname(__file__), 'output')
os.makedirs(OUT, exist_ok=True)
with open(os.path.join(OUT, f'decompose_{SECTOR}.txt'), 'w') as f:
    f.write(f'Decomposition for {SECTOR}\n')
    f.write(f'y:  old={at(y_old, SECTOR):.6e}  new={at(y_new, SECTOR):.6e}\n')
    f.write(f'd:  old={at(d_old, SECTOR):.6e}  new={at(d_new, SECTOR):.6e}\n')
    f.write(f'Ly: old={at(Ly_old, SECTOR):.6e}  new={at(Ly_new, SECTOR):.6e}\n')
    f.write(f'BLy: old={at(BLy_old, SECTOR):.6e}  new={at(BLy_new, SECTOR):.6e}\n')
print(f'\nSummary text written to {OUT}/decompose_{SECTOR}.txt')
