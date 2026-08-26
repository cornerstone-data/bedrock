"""Write the pre-MECS freeze (dollar Industrial manufacturing weights).

After the MECS default lands, this path is the dollar-weight counterfactual
of current EIA-anchored G/T/D mixed units — the production output immediately
before Table 7.7 shares. Not a CI gate.

    python -m bedrock.analysis.electricity.historical.pre_mecs_industrial_weights.write_freeze
"""

from __future__ import annotations

from unittest.mock import patch

from bedrock.analysis.electricity.historical.pre_mecs_industrial_weights.paths import (
    MIXED_CONFIG,
    config_dir,
)
from bedrock.publish.cache_reset import clear_all_publish_caches
from bedrock.transform.eeio.derived_cornerstone import derive_cornerstone_Aq_mixed_units
from bedrock.utils.config.usa_config import reset_usa_config, set_global_usa_config
from bedrock.utils.validation.diagnostics_helpers import pull_efs_for_diagnostics


def _dollar_allocate() -> object:
    import bedrock.transform.eeio.electricity_gtd_allocation as gtd  # noqa: PLC0415

    orig = gtd.allocate_purchaser_gtd

    def _wrapped(*args: object, **kwargs: object) -> object:
        kwargs = dict(kwargs)
        kwargs['industrial_weights'] = 'dollars'
        return orig(*args, **kwargs)

    return patch.object(gtd, 'allocate_purchaser_gtd', _wrapped)


def write_pre_mecs_freeze(config: str = MIXED_CONFIG) -> None:
    reset_usa_config()
    clear_all_publish_caches()
    set_global_usa_config(config)
    with _dollar_allocate():
        aq = derive_cornerstone_Aq_mixed_units()
        efs = pull_efs_for_diagnostics()
    out = config_dir(config)
    out.mkdir(parents=True, exist_ok=True)
    aq.scaled_q.astype(float).rename('q').to_frame().to_parquet(out / 'q.parquet')
    efs.N_new.to_parquet(out / 'N.parquet')
    print(f'Wrote {out / "q.parquet"} and {out / "N.parquet"}')


if __name__ == '__main__':
    write_pre_mecs_freeze()
