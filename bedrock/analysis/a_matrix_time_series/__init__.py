"""Pin analysis-specific config flags for every script in this package.

- `update_inflation_factors=True` — preserves pre-#369 BEA-derived industry
  PI path, which this analysis universally assumes. Production defaults to
  `False` to keep the legacy parquet flow.
- `apply_inflation_to_V=True` — inflates V to `cfg.model_base_year` when
  computing the V-norm-derived commodity price ratio. Only consumed by
  `commodity_price_index` approach but cheap to set globally.

`get_cornerstone_industry_price_ratio` and
`get_vnorm_adjusted_commodity_price_ratio` are `@functools.cache`'d on
`(original_year, target_year)` only, so flags must be set before any
helper call or the default-branch result becomes the cached answer for
the rest of the process. Setting them here guarantees ordering.

Scripts that swap the global config mid-run (e.g. `derive_A_time_series`
iterating over approach YAMLs) must re-set these flags inside their swap
helper — the toggle here only covers the initial config.
"""

from bedrock.utils.config.usa_config import get_usa_config

_cfg = get_usa_config()
_cfg.update_inflation_factors = True
_cfg.apply_inflation_to_V = True
