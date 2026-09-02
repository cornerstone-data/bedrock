"""Out-of-sample span test of the flow-anchored redefinition method, summary level.

Learn at 2017: (1) the Make movement pattern - which off-diagonal cells move,
and the moved fraction of each cell (partial at summary because whole-cell
detail moves aggregate); (2) per industry column, the Use movement vector per
dollar of moved output, g_j = dU17[:, j] / dx17_j.

Apply to each year 2018-2024 using that year's OWN published before-redef
summary tables: moved amount = 2017 fraction x the year's cell; Use movement =
g_j x the year's moved output. Score against the published after-redef tables
(2024 vintage), with do-nothing as the baseline. This is the same instrument
that measured the frozen-ratio carry; the difference is what the 2017
structure is anchored to - the year's own redefinition flow, not GO.
"""

import numpy as np
import pandas as pd

from bedrock.extract.iot.io_2017 import (
    load_summary_Utot_before_redef_usa,
    load_summary_Utot_usa_2024_vintage,
    load_summary_V_before_redef_usa,
    load_summary_V_usa_2024_vintage,
)

M = 1e6
YEARS = range(2017, 2025)


def aligned(year):
    vb = load_summary_V_before_redef_usa(year).astype(float)
    va = load_summary_V_usa_2024_vintage(year).astype(float)
    ub = load_summary_Utot_before_redef_usa(year).astype(float)
    ua = load_summary_Utot_usa_2024_vintage(year).astype(float)
    va = va.reindex(index=vb.index, columns=vb.columns).fillna(0.0)
    ua = ua.reindex(index=ub.index, columns=ub.columns).fillna(0.0)
    return vb, va, ub, ua


# ---- learn at 2017 ---------------------------------------------------------
V_b17, V_a17, U_b17, U_a17 = aligned(2017)
diff17 = V_a17 - V_b17

pattern = {}  # (donor industry, commodity) -> moved fraction of the cell
for (i, c), d in diff17.stack().items():
    if i != c and d < -0.5 * M and c in diff17.index:
        cell = V_b17.at[i, c]
        if cell > 0:
            pattern[(i, c)] = min(-d / cell, 1.0)
print(
    f'2017 summary pattern: {len(pattern)} moving cells, '
    f'${-sum(diff17.at[i, c] for (i, c) in pattern)/M:,.0f}M'
)

dx17 = diff17.sum(axis=1)  # net output moved per industry
dU17 = U_a17 - U_b17
g = {}  # industry -> Use movement vector per $ of moved output
for j in U_b17.columns:
    if j in dx17.index and abs(dx17[j]) > 100 * M:
        g[j] = dU17[j] / dx17[j]
print(
    f'Use vectors learned for {len(g)} industries ' f'(|moved output| > $100M at 2017)'
)


def predict(year):
    vb, va, ub, ua = aligned(year)
    # Make: move the 2017 fraction of the year's own cell.
    v_pred = vb.copy()
    moved = pd.Series(0.0, index=vb.index)
    for (i, c), f in pattern.items():
        amount = f * vb.at[i, c]
        v_pred.at[i, c] -= amount
        v_pred.at[c, c] += amount
        moved[i] -= amount
        moved[c] += amount
    # Use: per-column learned vector scaled by the year's moved output.
    u_pred = ub.copy()
    for j, vec in g.items():
        u_pred[j] = u_pred[j] + vec.reindex(ub.index).fillna(0.0) * moved.get(j, 0.0)
    return vb, va, ub, ua, v_pred, u_pred


def l1(a, b):
    return float((a - b).abs().sum().sum())


print(
    f'\n{"year":>5} {"V nothing":>10} {"V method":>10} {"cut":>6}   '
    f'{"U nothing":>10} {"U method":>10} {"cut":>6}'
)
for year in YEARS:
    vb, va, ub, ua, v_pred, u_pred = predict(year)
    vmass, umass = va.abs().sum().sum(), ua.abs().sum().sum()
    vn, vm = l1(vb, va) / vmass, l1(v_pred, va) / vmass
    un, um = l1(ub, ua) / umass, l1(u_pred, ua) / umass
    print(
        f'{year:>5} {vn:>10.3%} {vm:>10.3%} {1-vm/vn:>6.1%}   '
        f'{un:>10.3%} {um:>10.3%} {1-um/un:>6.1%}'
    )

# composition view: error concentrated on the movement footprint
print('\nfootprint view (columns of the industries with learned vectors):')
cols = [j for j in g]
for year in (2018, 2021, 2024):
    vb, va, ub, ua, v_pred, u_pred = predict(year)
    mass = ua[cols].abs().sum().sum()
    un = l1(ub[cols], ua[cols]) / mass
    um = l1(u_pred[cols], ua[cols]) / mass
    print(f'  {year}: U do-nothing {un:.3%} -> method {um:.3%} ({1-um/un:.1%} cut)')

# ---- head-to-head vs the frozen cell-ratio carry (#775's method) ----------
rU = (U_a17 / U_b17).replace([np.inf, -np.inf], np.nan).fillna(1.0)
rV = (V_a17 / V_b17).replace([np.inf, -np.inf], np.nan).fillna(1.0)

print('\nhead-to-head, same instrument (cell L1 as % of after-table mass):')
print(
    f'{"year":>5} {"V ratio":>9} {"V flow":>9}   {"U ratio":>9} {"U flow":>9}   '
    f'{"U ratio ft":>10} {"U flow ft":>10}'
)
for year in YEARS:
    vb, va, ub, ua, v_pred, u_pred = predict(year)
    v_ratio = vb * rV
    u_ratio = ub * rU
    vmass, umass = va.abs().sum().sum(), ua.abs().sum().sum()
    ftmass = ua[cols].abs().sum().sum()
    print(
        f'{year:>5} {l1(v_ratio, va)/vmass:>9.3%} {l1(v_pred, va)/vmass:>9.3%}   '
        f'{l1(u_ratio, ua)/umass:>9.3%} {l1(u_pred, ua)/umass:>9.3%}   '
        f'{l1(u_ratio[cols], ua[cols])/ftmass:>10.3%} '
        f'{l1(u_pred[cols], ua[cols])/ftmass:>10.3%}'
    )

# column-total accuracy: which method predicts the moved column sums better?
print('\ncolumn-total L1 on footprint columns ($B):')
for year in (2018, 2021, 2024):
    vb, va, ub, ua, v_pred, u_pred = predict(year)
    u_ratio = ub * rU
    for name, table in (('ratio', u_ratio), ('flow', u_pred)):
        ce = (table[cols].sum(axis=0) - ua[cols].sum(axis=0)).abs().sum()
        print(f'  {year} {name:>6}: ${ce/1e9:,.1f}B')


# hybrid: ratio-carried cells, each footprint column rescaled so its total
# change matches the flow-predicted (moved-output-anchored) column change.
def hybrid(year):
    vb, va, ub, ua, v_pred, u_pred = predict(year)
    u_ratio = ub * rU
    out = u_ratio.copy()
    for j in cols:
        delta_cells = u_ratio[j] - ub[j]
        target = u_pred[j].sum() - ub[j].sum()
        got = delta_cells.sum()
        if abs(got) > 1e-6:
            out[j] = ub[j] + delta_cells * (target / got)
    return out, ua


print('\nhybrid (ratio cells, flow column control), footprint cell L1:')
for year in (2018, 2021, 2024):
    vb, va, ub, ua, v_pred, u_pred = predict(year)
    u_ratio = ub * rU
    hyb, _ = hybrid(year)
    ftmass = ua[cols].abs().sum().sum()
    print(
        f'  {year}: ratio {l1(u_ratio[cols], ua[cols])/ftmass:.3%}  '
        f'flow {l1(u_pred[cols], ua[cols])/ftmass:.3%}  '
        f'hybrid {l1(hyb[cols], ua[cols])/ftmass:.3%}'
    )

# cells materially wrong (>10% and >$100M off), the composition lens
print('\ncells off by >10% and >$100M (Use, movement-footprint columns):')
for year in (2018, 2021, 2024):
    vb, va, ub, ua, v_pred, u_pred = predict(year)
    u_ratio = ub * rU
    for name, table in (('ratio', u_ratio), ('flow', u_pred)):
        e = (table[cols] - ua[cols]).abs()
        base = ua[cols].abs().clip(lower=1.0)
        bad = int(((e / base > 0.10) & (e > 100 * M)).sum().sum())
        print(f'  {year} {name:>6}: {bad} bad cells')
