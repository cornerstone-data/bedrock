"""Force BEA-derived industry price index for every script in this package.

The `update_inflation_factors=False` default in `USAConfig` preserves
pre-#369 production behavior on the Cornerstone A-matrix path. This
analysis package universally assumes the new BEA-derived path, so the
flag is flipped once on first import.

`get_cornerstone_industry_price_ratio` and
`get_vnorm_adjusted_commodity_price_ratio` are `@functools.cache`'d on
`(original_year, target_year)` only, so the flag must be set before any
helper call or the False-branch result becomes the cached answer for the
rest of the process. Putting the flip here guarantees ordering.

Scripts that swap the global config mid-run (e.g. `derive_A_time_series`
iterating over approach YAMLs) must re-set the flag inside their swap
helper — the toggle here only covers the initial config.
"""

from bedrock.utils.config.usa_config import get_usa_config

get_usa_config().update_inflation_factors = True
