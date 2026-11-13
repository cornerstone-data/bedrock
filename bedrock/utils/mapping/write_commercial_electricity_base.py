"""Write ``commercial_electricity_base.csv``: the base years of the commercial kWh basis.

:func:`~bedrock.transform.eeio.electricity_delivery.commercial_kwh_basis` (#899,
#705) moves a commercial buyer's kWh from its own 2017 bill, on its survey
electricity bill through 2022 and on its other intermediate inputs otherwise. Per
industry, in USD:

- ``bill_2017_usd``: the domestic ``221100`` cell the 2017 split divides;
- ``other_inputs_2017_usd``: the 2017 column's domestic and imported intermediate
  inputs other than electricity;
- ``other_inputs_2022_usd``: the same in 2022, which 2023 and 2024 move on from
  the survey's last SAS year.

Taken from the model's own tables, so the basis equals the bill in 2017 and has
no step at 2022. The CSV is committed so a year's build doesn't build another
year inside it. Rerun this after a rebuild that changes the 2017 or 2022 tables,
then commit the diff.

Run::

    uv run python -m bedrock.utils.mapping.write_commercial_electricity_base
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

OUTPUT = Path('bedrock/transform/allocation/data/commercial_electricity_base.csv')
CONFIG_TEMPLATE = '2026_usa_cornerstone_v1_0_{year}'


def _year_tables(year: int, vintage: str | None) -> tuple[pd.Series, pd.Series]:
    """The domestic electricity bill and the other inputs per industry for *year*, USD.

    *vintage* names the nowcast tables to read; the year's config pin by default.
    """
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

    overrides = {} if vintage is None else {'nowcast_mut_vintage': vintage}
    with temp_usa_config(
        CONFIG_TEMPLATE.format(year=year),
        cache_bearing_modules=CACHE_MODULES,
        **overrides,
    ):
        _, udom, uimp, _ = reallocate_electricity_coproduction(
            pipe.derive_V_after_waste(),
            *pipe.derive_U_after_waste(),
            pipe.derive_VA_after_waste(),
        )
        bill = pd.Series(udom.loc[ed.GENERATION_SECTOR], dtype=float)
        other = ed.other_intermediate_inputs(udom, uimp)
    return bill, other


def base(vintage: str | None = None) -> pd.DataFrame:
    """The base-year columns per industry, USD, from the v1.0 tables."""
    bill_2017, other_2017 = _year_tables(2017, vintage)
    _, other_2022 = _year_tables(2022, vintage)
    frame = pd.DataFrame(
        {
            'bill_2017_usd': bill_2017,
            'other_inputs_2017_usd': other_2017,
            'other_inputs_2022_usd': other_2022.reindex(bill_2017.index),
        }
    )
    frame.index.name = 'industry'
    keep = (frame['bill_2017_usd'] > 0.0) & (frame['other_inputs_2017_usd'] > 0.0)
    return frame[keep & (frame['other_inputs_2022_usd'] > 0.0)]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    parser.add_argument(
        '--vintage',
        help='nowcast tables to read, e.g. v1.0.0_54dbcea; the pin by default',
    )
    frame = base(parser.parse_args().vintage)
    frame.to_csv(OUTPUT)
    print(
        f'{len(frame)} industries, 2017 bills ${frame["bill_2017_usd"].sum() / 1e9:,.1f}bn'
    )
    print(f'wrote {OUTPUT}')


if __name__ == '__main__':
    main()
