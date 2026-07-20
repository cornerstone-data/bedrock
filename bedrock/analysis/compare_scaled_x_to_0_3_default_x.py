"""Compare x (gross industry output) between the scaling and default v0.3 approaches.

Scaled config (2025_usa_cornerstone_v0_3_scaling_for_A_and_B):
    use_scaled_x_and_scaled_Vnorm_for_B=True
    → derive_cornerstone_x_after_redefinition()
    → scale_cornerstone_x(derive_cornerstone_x(), target_year=model_base_year, original_year=usa_detail_original_year)
    Scales 2017 industry row-sums by a single summary x ratio per sector.

Default config (2025_usa_cornerstone_v0_3):
    use_scaled_x_and_scaled_Vnorm_for_B=False
    → derive_cornerstone_x_after_redefinition()
    → BEA gross-output time series at usa_ghg_data_year, expanded to Cornerstone industries.

Both configs are pinned to model_base_year / usa_ghg_data_year = 2024, so both x
vectors land in 2024 (nominal) dollars — no separate deflation is needed, but the
script asserts the two years match before comparing.

Usage:
    python -m bedrock.analysis.compare_x_approaches
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_x_after_redefinition,
)
from bedrock.utils.config.config_controllers import temp_usa_config
from bedrock.utils.config.usa_config import get_usa_config

SCALED_CONFIG = '2025_usa_cornerstone_v0_3_scaling_for_A_and_B'
DEFAULT_CONFIG = '2025_usa_cornerstone_v0_3'

_CACHE_BEARING_MODULE_PATHS = (
    'bedrock.transform.eeio.derived_cornerstone',
    'bedrock.transform.eeio.cornerstone_bea_intermediates',
    'bedrock.transform.eeio.cornerstone_year_scaling',
    'bedrock.utils.economic.inflation_helpers_cornerstone',
)


def x_scaled() -> tuple[pd.Series[float], int]:
    """x from the scaling approach, in cfg.model_base_year dollars."""
    with temp_usa_config(
        SCALED_CONFIG, cache_bearing_modules=_CACHE_BEARING_MODULE_PATHS
    ):
        cfg = get_usa_config()
        assert cfg.use_scaled_x_and_scaled_Vnorm_for_B, (
            f'Config {SCALED_CONFIG} must have use_scaled_x_and_scaled_Vnorm_for_B=True'
        )
        return derive_cornerstone_x_after_redefinition(), cfg.model_base_year


def x_default() -> tuple[pd.Series[float], int]:
    """x from the current default approach, in cfg.usa_ghg_data_year dollars."""
    with temp_usa_config(
        DEFAULT_CONFIG, cache_bearing_modules=_CACHE_BEARING_MODULE_PATHS
    ):
        cfg = get_usa_config()
        assert not cfg.use_scaled_x_and_scaled_Vnorm_for_B, (
            f'Config {DEFAULT_CONFIG} must have use_scaled_x_and_scaled_Vnorm_for_B=False'
        )
        return derive_cornerstone_x_after_redefinition(), cfg.usa_ghg_data_year


def main() -> None:
    x_a, year_a = x_scaled()
    x_b, year_b = x_default()
    assert year_a == year_b, (
        f'Dollar years differ: scaled={year_a}, default={year_b} — '
        'not directly comparable without deflating one to the other.'
    )
    year = year_a

    idx = x_a.index.union(x_b.index)
    x_a = x_a.reindex(idx)
    x_b = x_b.reindex(idx)

    diff = x_a - x_b
    rel_diff = diff / x_b.replace(0, np.nan)

    summary = pd.DataFrame(
        {
            'x_scaled': x_a,
            'x_default': x_b,
            'abs_diff': diff,
            'rel_diff_pct': rel_diff * 100,
        }
    )

    print(f'\nScaled config:  {SCALED_CONFIG}  (use_scaled_x_and_scaled_Vnorm_for_B=True)')
    print(f'Default config: {DEFAULT_CONFIG}')
    print(f'Dollar year (both): {year}')
    print(f'\nSectors:          {len(x_a)}')
    print(f'Max |abs diff|:   {diff.abs().max():,.0f}')
    print(f'Mean |abs diff|:  {diff.abs().mean():,.0f}')
    print(f'Max |rel diff|:   {rel_diff.abs().max() * 100:.2f}%')
    print(f'Mean |rel diff|:  {rel_diff.abs().mean() * 100:.2f}%')
    print(f'Sectors with >1% rel diff: {(rel_diff.abs() > 0.01).sum()}')
    print(f'Sectors with >5% rel diff: {(rel_diff.abs() > 0.05).sum()}')

    print('\nTop 15 sectors by absolute difference:')
    print(
        summary.reindex(diff.abs().nlargest(15).index).to_string(
            float_format='{:,.1f}'.format
        )
    )

    print('\nTop 15 sectors by relative difference (|rel_diff| > 0):')
    print(
        summary[rel_diff.abs() > 0]
        .reindex(rel_diff.abs().nlargest(15).index)
        .to_string(float_format='{:,.2f}'.format)
    )


if __name__ == '__main__':
    main()
