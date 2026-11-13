"""Summary span score of the Use closure: old (all deltas) vs new (same-sign deltas).

Same instrument as method_probes/redef_span_probe.py: learn the Use cell ratios at summary 2017,
apply them to each year's published before-redefinitions summary Use, close the rows, and score
cell L1 against the published after-redefinitions table (2024 vintage). 2017 is in-sample.
"""

import numpy as np
import pandas as pd

from bedrock.extract.iot.io_2017 import (
    load_summary_Utot_before_redef_usa,
    load_summary_Utot_usa_2024_vintage,
)

M = 1e6
ATOL = 1.0 * M


def aligned(year):
    ub = load_summary_Utot_before_redef_usa(year).astype(float)
    ua = load_summary_Utot_usa_2024_vintage(year).astype(float)
    ua = ua.reindex(index=ub.index, columns=ub.columns).fillna(0.0)
    return ub, ua


def close(carried, before, frozen, same_sign):
    delta = carried - before
    net = delta.sum(axis=1)
    weight = (
        delta.mul(np.sign(net), axis=0).clip(lower=0.0) if same_sign else delta.abs()
    )
    excluded = weight.where(~frozen, 0.0)
    needs = (excluded.sum(axis=1) == 0.0) & (net.abs() > ATOL)
    weight = excluded.where(~needs, weight)
    weight = weight.div(weight.sum(axis=1).replace(0.0, np.nan), axis=0).fillna(0.0)
    return carried - weight.mul(net, axis=0)


U_b17, U_a17 = aligned(2017)
# interior only: rows and columns shared by commodity x industry (summary codes are not V00x / F0x)
rows = [r for r in U_b17.index if not str(r).startswith(('V0', 'T0'))]
cols = [c for c in U_b17.columns if not str(c).startswith(('F0', 'T0'))]
ratios = (
    (U_a17.loc[rows, cols] / U_b17.loc[rows, cols])
    .replace([np.inf, -np.inf], np.nan)
    .fillna(1.0)
)

out = []
for year in range(2017, 2025):
    ub, ua = aligned(year)
    b, a = ub.loc[rows, cols], ua.loc[rows, cols]
    r = ratios.reindex_like(b).fillna(1.0)
    carried = b * r
    mass = a.abs().sum().sum()
    rec = {
        'year': year,
        'do_nothing': (b - a).abs().sum().sum() / mass,
        'carry': (carried - a).abs().sum().sum() / mass,
    }
    for name, ss in (('old', False), ('new', True)):
        c = close(carried, b, r == 0.0, ss)
        rec[f'{name}_L1'] = (c - a).abs().sum().sum() / mass
        rec[f'{name}_neg_cells'] = int((c < -ATOL).sum().sum())
        rec[f'{name}_bad_cells'] = int(
            (((c - a).abs() > 0.1 * a.abs()) & ((c - a).abs() > 100 * M)).sum().sum()
        )
    out.append(rec)

t = pd.DataFrame(out).set_index('year')
for c in ('do_nothing', 'carry', 'old_L1', 'new_L1'):
    t[c] = (100 * t[c]).round(3)
pd.set_option('display.width', 200)
print('cell L1 as % of after-table mass; bad = off by >10% and >$100M; 2017 in-sample')
print(t.to_string())
