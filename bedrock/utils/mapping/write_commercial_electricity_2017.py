"""Write ``commercial_electricity_2017.csv``: each industry's 2017 electricity bill and other inputs.

The base of :func:`~bedrock.transform.eeio.electricity_delivery.commercial_kwh_basis`
(#899, #705): a commercial buyer's kWh move with its other intermediate inputs, or
with its survey electricity bill, from its own 2017 values. Taken from the model's
2017 tables, the same tables the 2017 split reads, so the basis equals the bill in
2017 and the 2017 split doesn't change.

``bill_usd`` is the domestic ``221100`` cell the split divides; ``other_inputs_usd``
is the column's domestic and imported intermediate inputs other than electricity.

The CSV is committed so a year's build doesn't build 2017 inside it. Rerun this to
refresh it, then commit the diff.

Run::

    uv run python -m bedrock.utils.mapping.write_commercial_electricity_2017
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

OUTPUT = Path('bedrock/transform/allocation/data/commercial_electricity_2017.csv')
CONFIG = '2026_usa_cornerstone_v1_0_2017'


def base_2017() -> pd.DataFrame:
    """The 2017 bill and other inputs per industry, USD, from the 2017 v1.0 tables."""
    from bedrock.analysis.electricity.current.eia_gtd.electricity_split_checks import (  # noqa: PLC0415
        CACHE_MODULES,
    )
    from bedrock.transform.eeio import electricity_delivery as ed  # noqa: PLC0415
    from bedrock.transform.eeio import sector_disagg_pipeline as pipe  # noqa: PLC0415
    from bedrock.transform.eeio.electricity_disaggregation import (  # noqa: PLC0415
        reallocate_electricity_coproduction,
    )
    from bedrock.utils.config.config_controllers import (  # noqa: PLC0415
        temp_usa_config,
    )

    with temp_usa_config(CONFIG, cache_bearing_modules=CACHE_MODULES):
        _, udom, uimp, _ = reallocate_electricity_coproduction(
            pipe.derive_V_after_waste(),
            *pipe.derive_U_after_waste(),
            pipe.derive_VA_after_waste(),
        )
        bill = pd.Series(udom.loc[ed.GENERATION_SECTOR], dtype=float)
        other = ed.other_intermediate_inputs(udom, uimp)
    frame = pd.DataFrame({'bill_usd': bill, 'other_inputs_usd': other})
    frame.index.name = 'industry'
    return frame[(frame['bill_usd'] > 0.0) & (frame['other_inputs_usd'] > 0.0)]


def main() -> None:
    frame = base_2017()
    frame.to_csv(OUTPUT)
    print(f'{len(frame)} industries, bills ${frame["bill_usd"].sum() / 1e9:,.1f}bn')
    print(f'wrote {OUTPUT}')


if __name__ == '__main__':
    main()
