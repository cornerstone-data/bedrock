"""Write the pre-MECS freeze (dollar Industrial manufacturing weights).

After the MECS default lands, this path is the dollar-weight counterfactual
of current EIA-anchored G/T/D — the production output immediately before
Table 7.7 shares. Not a CI gate.

    python -m bedrock.analysis.electricity.historical.pre_mecs_industrial_weights.write_freeze
    python -m bedrock.analysis.electricity.historical.pre_mecs_industrial_weights.write_freeze \\
        --config 2025_usa_cornerstone_v0_3_electricity_disaggregation
"""

from __future__ import annotations

import json
from unittest.mock import patch

import click
import pandas as pd

from bedrock.analysis.electricity.historical.pre_mecs_industrial_weights.paths import (
    DISAGG_CONFIG,
    MIXED_CONFIG,
    config_dir,
)
from bedrock.publish.cache_reset import clear_all_publish_caches
from bedrock.utils.config.usa_config import (
    get_usa_config,
    reset_usa_config,
    set_global_usa_config,
)


def _dollar_allocate() -> object:
    import bedrock.transform.eeio.electricity_gtd_allocation as gtd  # noqa: PLC0415

    orig = gtd.allocate_purchaser_gtd

    def _wrapped(*args: object, **kwargs: object) -> object:
        kwargs = dict(kwargs)
        kwargs['industrial_weights'] = 'dollars'
        return orig(*args, **kwargs)

    return patch.object(gtd, 'allocate_purchaser_gtd', _wrapped)


def _write_class_mwh(folder) -> None:
    from bedrock.analysis.electricity.current.eia_gtd.purchaser_tables import (  # noqa: PLC0415
        allocated_class_mwh,
        class_mwh_targets,
    )
    from bedrock.transform.eeio.electricity_gtd_allocation import (  # noqa: PLC0415
        get_reanchored_eia_purchaser_allocation,
    )

    alloc = get_reanchored_eia_purchaser_allocation()
    if alloc is None:
        return
    eia_year = int(get_usa_config().model_base_year)
    model = allocated_class_mwh(alloc)
    targets = class_mwh_targets(alloc, eia_year)
    rows = [
        {
            'class': str(cls),
            'value': float(model.loc[cls]),
            'unit': 'MWh',
            'target': float(targets.get(str(cls), float('nan'))),
        }
        for cls in model.index
    ]
    pd.DataFrame(rows).to_parquet(folder / 'class_generation_mwh.parquet')


def write_pre_mecs_freeze(config: str = MIXED_CONFIG) -> None:
    from bedrock.transform.eeio.cornerstone_disagg_pipeline import (  # noqa: PLC0415
        electricity_conversion_factors,
        electricity_mixed_units_enabled,
    )
    from bedrock.transform.eeio.derived_cornerstone import (  # noqa: PLC0415
        derive_cornerstone_Aq_mixed_units,
        derive_cornerstone_Aq_scaled,
    )
    from bedrock.utils.validation.diagnostics_helpers import (  # noqa: PLC0415
        pull_efs_for_diagnostics,
    )

    reset_usa_config()
    clear_all_publish_caches()
    set_global_usa_config(config)
    with _dollar_allocate():
        if electricity_mixed_units_enabled():
            aq_mon = derive_cornerstone_Aq_scaled()
            c_col, _c_row = electricity_conversion_factors(aq_mon)
            aq = derive_cornerstone_Aq_mixed_units()
        else:
            aq = derive_cornerstone_Aq_scaled()
            c_col = None
        efs = pull_efs_for_diagnostics()
    out = config_dir(config)
    out.mkdir(parents=True, exist_ok=True)
    aq.scaled_q.astype(float).rename('q').to_frame().to_parquet(out / 'q.parquet')
    efs.N_new.to_parquet(out / 'N.parquet')
    efs.D_new.to_parquet(out / 'D.parquet')
    (out / 'run_metadata.json').write_text(
        json.dumps(
            {
                'config': config,
                'c_col': c_col,
                'mixed_units': bool(electricity_mixed_units_enabled()),
                'industrial_weights': 'dollars',
            },
            indent=2,
        ),
        encoding='utf-8',
    )
    _write_class_mwh(out)
    print(f'Wrote freeze under {out}')


@click.command()
@click.option(
    '--config',
    default=MIXED_CONFIG,
    type=click.Choice((MIXED_CONFIG, DISAGG_CONFIG)),
    show_default=True,
)
def main(config: str) -> None:
    write_pre_mecs_freeze(config)


if __name__ == '__main__':
    main()
