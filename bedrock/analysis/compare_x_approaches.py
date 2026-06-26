"""Compare the two candidate x vectors for derive_cornerstone_Vnorm_scrap_corrected.

Current approach (line 304):
    derive_cornerstone_x_after_redefinition()
    → scale_cornerstone_x(derive_cornerstone_x(), target_year, original_year)
    Scales 2017 industry row-sums by a single summary x ratio per sector.

Alternative approach:
    compute_x(V=V)  where V = scale_cornerstone_V(derive_cornerstone_V(), ...)
    Takes row sums of the already-scaled Make table directly.

Usage:
    python -m bedrock.analysis.compare_x_approaches
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from bedrock.transform.eeio.cornerstone_year_scaling import (
    scale_cornerstone_V,
    scale_cornerstone_x,
)
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_V,
    derive_cornerstone_x,
    derive_cornerstone_x_after_redefinition,
)
from bedrock.utils.config.usa_config import get_usa_config, set_global_usa_config
from bedrock.utils.math.formulas import compute_x

CONFIG = '2025_usa_cornerstone_full_model_scaling_for_A_and_B'

set_global_usa_config(CONFIG)
cfg = get_usa_config()

assert (
    cfg.use_scaled_x_and_scaled_Vnorm_for_B
), f'Config {CONFIG} must have use_scaled_x_and_scaled_Vnorm_for_B=True'

target = cfg.model_base_year
original = cfg.usa_detail_original_year

# --- approach A: current code path ---
x_current = derive_cornerstone_x_after_redefinition()

# --- approach B: row sums of the scaled V (same V used for Vnorm) ---
V_scaled = scale_cornerstone_V(
    derive_cornerstone_V(),
    target_year=target,
    original_year=original,
)
x_alt = compute_x(V=V_scaled)

# --- comparison ---
diff = x_alt - x_current
rel_diff = diff / x_current.replace(0, np.nan)

summary = pd.DataFrame(
    {
        'x_current (scale_cornerstone_x)': x_current,
        'x_alt (compute_x from scaled_V)': x_alt,
        'abs_diff': diff,
        'rel_diff_pct': rel_diff * 100,
    }
)

print(f'\nConfig: {CONFIG}')
print(f'target_year={target}, original_year={original}')
print(f'\nSectors:          {len(x_current)}')
print(f'Max |abs diff|:   {diff.abs().max():,.0f}')
print(f'Mean |abs diff|:  {diff.abs().mean():,.0f}')
print(f'Max |rel diff|:   {rel_diff.abs().max() * 100:.2f}%')
print(f'Mean |rel diff|:  {rel_diff.abs().mean() * 100:.2f}%')
print(f'Sectors with >1% rel diff: {(rel_diff.abs() > 0.01).sum()}')
print(f'Sectors with >5% rel diff: {(rel_diff.abs() > 0.05).sum()}')

print('\nTop 15 sectors by absolute difference:')
print(
    summary.reindex(diff.abs().nlargest(15).index)[
        [
            'x_current (scale_cornerstone_x)',
            'x_alt (compute_x from scaled_V)',
            'abs_diff',
            'rel_diff_pct',
        ]
    ].to_string(float_format='{:,.1f}'.format)
)

print('\nTop 15 sectors by relative difference (|rel_diff| > 0):')
print(
    summary[rel_diff.abs() > 0]
    .reindex(rel_diff.abs().nlargest(15).index)[
        [
            'x_current (scale_cornerstone_x)',
            'x_alt (compute_x from scaled_V)',
            'abs_diff',
            'rel_diff_pct',
        ]
    ]
    .to_string(float_format='{:,.2f}'.format)
)

# Also show: how does each compare to scale_cornerstone_x directly
x_2017_row_sums = compute_x(V=derive_cornerstone_V())
x_scaled_from_2017 = scale_cornerstone_x(
    derive_cornerstone_x(), target_year=target, original_year=original
)
print(
    f'\nSanity: derive_cornerstone_x_after_redefinition matches scale_cornerstone_x: '
    f'{np.allclose(np.asarray(x_current.values, dtype=float), np.asarray(x_scaled_from_2017.reindex(x_current.index).values, dtype=float))}'
)
