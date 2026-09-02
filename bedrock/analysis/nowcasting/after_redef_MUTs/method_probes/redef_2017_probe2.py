"""Second probe: the partial cells, and what actually moves on the Use side."""

import numpy as np
import pandas as pd

from bedrock.extract.iot.io_2017 import (
    load_2017_Utot_after_redef_usa,
    load_2017_Utot_before_redef_usa,
    load_2017_V_after_redef_usa,
    load_2017_V_before_redef_usa,
)

M = 1e6
V_b = load_2017_V_before_redef_usa().astype(float)
V_a = load_2017_V_after_redef_usa().astype(float)
U_b = load_2017_Utot_before_redef_usa().astype(float)
U_a = load_2017_Utot_after_redef_usa().astype(float)
V_a = pd.DataFrame(V_a.to_numpy(), index=V_b.index, columns=V_b.columns)
U_a = pd.DataFrame(U_a.to_numpy(), index=U_b.index, columns=U_b.columns)

diff = V_a - V_b
off = diff.copy()
for c in off.columns:
    if c in off.index:
        off.at[c, c] = 0.0
losses = off[off < -0.5 * M].stack()
before_vals = pd.Series(
    [V_b.at[i, c] for i, c in losses.index], index=losses.index, dtype=float
)
frac = (-losses) / before_vals

print('=== the partial cells (moved fraction < 99.9%) ===')
partial = frac[frac < 0.999].sort_values()
for (i, c), f in partial.items():
    print(f'  donor {i} -> commodity {c}: {f:6.1%} of ${before_vals[(i, c)]/M:,.0f}M '
          f'(moved ${-losses[(i, c)]/M:,.0f}M)')

print('\n=== per-industry moved-input intensity at 2017 ===')
# Net output moved per industry (negative = donor), and the observed net
# intermediate-input movement in that industry's Use column.
out_moved = diff.sum(axis=1)
int_moved = (U_a - U_b).sum(axis=0)

frame = pd.DataFrame({'output_moved': out_moved, 'intermediate_moved': int_moved})
frame = frame[frame['output_moved'].abs() > 100 * M]
frame['intensity'] = frame['intermediate_moved'] / frame['output_moved']

total_out = frame['output_moved'].abs().sum() / 2
total_int = frame['intermediate_moved'].abs().sum() / 2
print(f'industries with >|$100M| output moved: {len(frame)}')
print(f'aggregate intermediate-per-output intensity: '
      f'{(frame["intermediate_moved"].abs().sum() / frame["output_moved"].abs().sum()):.3f}')

donors = frame[frame['output_moved'] < 0].copy()
receivers = frame[frame['output_moved'] > 0].copy()
for name, block in (('donors (lose output)', donors), ('receivers (gain output)', receivers)):
    w = block['output_moved'].abs()
    mean_int = (block['intensity'] * w).sum() / w.sum()
    print(f'\n{name}: n={len(block)}, output ${w.sum()/M:,.0f}M, '
          f'weighted intensity {mean_int:.3f}')
    print(f'  intensity quartiles: '
          f'{block["intensity"].quantile([0.25, 0.5, 0.75]).round(3).tolist()}')

print('\nlargest receivers, intensity each:')
for code, row in receivers.sort_values('output_moved', ascending=False).head(12).iterrows():
    print(f'  {code}: gained ${row.output_moved/M:>10,.0f}M output, '
          f'${row.intermediate_moved/M:>10,.0f}M intermediates -> intensity {row.intensity:5.2f}')

print('\nlargest donors, intensity each:')
for code, row in donors.sort_values('output_moved').head(12).iterrows():
    print(f'  {code}: lost ${-row.output_moved/M:>10,.0f}M output, '
          f'${-row.intermediate_moved/M:>10,.0f}M intermediates -> intensity {row.intensity:5.2f}')

# Sanity: industry output identity after redef
x_gap = (V_a.sum(axis=1) - (U_a.sum(axis=0) + (V_a.sum(axis=1) - U_a.sum(axis=0)))).abs().max()
print(f'\n(identity check placeholder: {x_gap:.1f})')
