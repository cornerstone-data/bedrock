"""#1215 check: EIA-861 sales-for-resale ``p_gen`` vs EIA Table 8.3.

Table 8.3 (FERC Form 1) publishes **purchased-power expense dollars** for major
investor-owned utilities only — no MWh — so it cannot yield a second ¢/kWh.
The honest check is whether industry-wide 861 sales-for-resale dollars (and the
``p_gen`` built from them) move with that IOU purchased-power expense.

Run::

    python -m bedrock.analysis.electricity.v3.PR1220.ferc_table83_check
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

OUT_DIR = Path(__file__).resolve().parent / 'output'
YEARS = range(2017, 2025)


def build() -> pd.DataFrame:
    from bedrock.analysis.nowcasting.utilities_expense_seed import (  # noqa: PLC0415
        resale_revenue,
        resale_volume,
    )
    from bedrock.extract.disaggregation.egrid_generation import (  # noqa: PLC0415
        eia_purchased_power_usd,
        eia_table_8_3_line,
    )
    from bedrock.transform.eeio.electricity_price import resale_price  # noqa: PLC0415

    rev = resale_revenue()
    vol = resale_volume()
    rows: list[dict[str, float | int]] = []
    for year in YEARS:
        revenue_m = float(rev.loc[year])
        volume_mwh = float(vol.loc[year])
        purchased = eia_purchased_power_usd(year)
        fuel = eia_table_8_3_line(year, 'expenses: Cost of Fuel')
        rows.append(
            {
                'year': year,
                'p_gen_cents_per_kwh': resale_price(revenue_m, volume_mwh),
                'resale_revenue_bn': revenue_m / 1e3,
                'resale_twh': volume_mwh / 1e6,
                'table83_purchased_power_bn': purchased / 1e9,
                'table83_fuel_bn': fuel / 1e9,
                'table83_fuel_plus_purchased_bn': (purchased + fuel) / 1e9,
            }
        )
    frame = pd.DataFrame(rows)
    frame['resale_over_table83_pp'] = (
        frame['resale_revenue_bn'] / frame['table83_purchased_power_bn']
    )
    for col in (
        'p_gen_cents_per_kwh',
        'resale_revenue_bn',
        'table83_purchased_power_bn',
        'table83_fuel_plus_purchased_bn',
    ):
        frame[f'{col}_index_2017'] = frame[col] / float(frame[col].iloc[0]) * 100.0
    return frame


def summary(frame: pd.DataFrame) -> dict[str, float]:
    return {
        'corr_yoy_p_gen_vs_table83_pp': float(
            frame['p_gen_cents_per_kwh']
            .pct_change()
            .corr(frame['table83_purchased_power_bn'].pct_change())
        ),
        'corr_yoy_resale_vs_table83_pp': float(
            frame['resale_revenue_bn']
            .pct_change()
            .corr(frame['table83_purchased_power_bn'].pct_change())
        ),
        'corr_yoy_p_gen_vs_table83_fuel_pp': float(
            frame['p_gen_cents_per_kwh']
            .pct_change()
            .corr(frame['table83_fuel_plus_purchased_bn'].pct_change())
        ),
        'mean_resale_over_table83_pp': float(frame['resale_over_table83_pp'].mean()),
    }


def main() -> None:
    frame = build()
    stats = summary(frame)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUT_DIR / 'ferc_table83_check.csv'
    frame.to_csv(out, index=False)
    print(frame.round(3).to_string(index=False))
    print()
    for key, value in stats.items():
        print(f'{key}: {value:.3f}')
    print(f'wrote {out}')


if __name__ == '__main__':
    main()
