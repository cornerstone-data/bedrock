"""Implied class MWh from #1219 prices vs EIA Table 2.2 sales.

The #902 / upstack allocator does::

    kWh_i = bill_i / price_i, then scale so each class matches EIA sales

So **after** scaling, class totals equal EIA by construction. The interesting
comparison for the Discussion #85 failure mode is the **unscaled** total::

    unscaled_class_MWh = sum_i (bill_i / price_i)

If that is far from EIA (e.g. residential ~0.5×), a flat retail-price decode of
IO bills is still broken. If it is near 1×, the new prices land close to EIA
and scaling is a small correction.

Prices: manufacturing from :func:`manufacturing_price` (#1219); everyone else
the EIA class price. Class map follows upstack #1221: only rail/transit stay in
Transportation; other NAICS 48-49 move to Commercial. Electric own-use and
imports are excluded from the four sales classes.

Run::

    python -m bedrock.analysis.electricity.PR1219.class_mwh_vs_eia
    python -m bedrock.analysis.electricity.PR1219.class_mwh_vs_eia --years 2024
    python -m bedrock.analysis.electricity.PR1219.class_mwh_vs_eia --mut-vintage v1.0.0_e5fa35b
"""

from __future__ import annotations

import argparse
import typing as ta
from pathlib import Path

import pandas as pd

OUT_DIR = Path(__file__).resolve().parent / 'output'

ELECTRIC_OWN_USE = frozenset({'221100', 'S00101', 'S00202'})
EIA_TRANSPORTATION_BUYERS = frozenset({'482000', '485000', 'S00201'})
MANUFACTURING_PREFIXES = ('31', '32', '33')
SCALED_CLASSES = ('Residential', 'Commercial', 'Industrial', 'Transportation')
IMPORT_FD = 'F05000'


def _activate(year: int, mut_vintage: str | None) -> str:
    from bedrock.analysis.electricity.current.eia_gtd.electricity_row_control import (  # noqa: PLC0415
        _activate as activate_year,
    )

    return activate_year(year, mut_vintage)


def _bills_usd(year: int, mut_vintage: str | None) -> tuple[pd.Series, str]:
    """Domestic electricity Use + Y purchases by buyer, USD."""
    from bedrock.analysis.electricity.current.eia_gtd.electricity_row_control import (  # noqa: PLC0415
        load_year,
    )

    vintage = _activate(year, mut_vintage)
    panel = load_year(year, mut_vintage)
    bills = panel.elec.add(panel.y, fill_value=0.0).astype(float)
    bills = bills[bills > 0.0]
    if IMPORT_FD in bills.index:
        bills = bills.drop(index=IMPORT_FD)
    return bills, vintage


def _classes(codes: ta.Iterable[str]) -> pd.Series:
    from bedrock.transform.eeio.electricity_end_use_mapping import (  # noqa: PLC0415
        build_end_use_map,
    )

    base = build_end_use_map()
    out: dict[str, str] = {}
    for code in map(str, codes):
        cls = base.get(code, 'Commercial')
        if code in ELECTRIC_OWN_USE:
            cls = 'Own use'
        elif cls == 'Transportation' and code not in EIA_TRANSPORTATION_BUYERS:
            cls = 'Commercial'
        out[code] = cls
    return pd.Series(out, dtype=object)


def _buyer_prices(year: int, classes: pd.Series) -> pd.Series:
    from bedrock.transform.eeio.electricity_price import (  # noqa: PLC0415
        class_price,
        manufacturing_price,
    )

    mfg = manufacturing_price(year)
    prices: dict[str, float] = {}
    for code, cls in classes.items():
        key = str(code)
        if cls in ('Own use', 'Exports'):
            continue
        if key.startswith(MANUFACTURING_PREFIXES) and key in mfg.index:
            prices[key] = float(mfg[key])
        else:
            prices[key] = float(class_price(year, str(cls)))
    return pd.Series(prices, dtype=float)


def _class_table(year: int, mut_vintage: str | None) -> tuple[pd.DataFrame, str]:
    from bedrock.extract.disaggregation.egrid_generation import (  # noqa: PLC0415
        eia_retail_revenue_usd,
        eia_table_2_2_end_use_mwh,
    )
    from bedrock.transform.eeio.electricity_price import class_price  # noqa: PLC0415

    bills, vintage = _bills_usd(year, mut_vintage)
    classes = _classes(bills.index)
    prices = _buyer_prices(year, classes)
    sales = eia_table_2_2_end_use_mwh(year)

    rows: list[dict[str, float | str | int]] = []
    for cls in SCALED_CLASSES:
        members = [c for c in bills.index if classes[c] == cls and c in prices.index]
        bill_usd = float(bills.loc[members].sum()) if members else 0.0
        # price is ¢/kWh; bill/price → kWh, then /1e6 → TWh (= million MWh / 1e3? )
        # bill USD / (¢/kWh / 100) = bill / (price/100) = kWh
        raw_kwh = (
            float((bills.loc[members] / (prices.loc[members] / 100.0)).sum())
            if members
            else 0.0
        )
        # EIA Table 2.2 is in MWh; bill/price yields kWh.
        eia_mwh = float(sales[cls])
        eia_twh = eia_mwh / 1e6
        unscaled_twh = raw_kwh / 1e9
        eia_rev = float(eia_retail_revenue_usd(year, cls))
        eia_px = float(class_price(year, cls))
        rows.append(
            {
                'year': year,
                'class': cls,
                'buyers': len(members),
                'bill_bn_usd': bill_usd / 1e9,
                'eia_revenue_bn_usd': eia_rev / 1e9,
                'bill_over_eia_revenue': (
                    bill_usd / eia_rev if eia_rev else float('nan')
                ),
                'price_used_cents_kwh_bill_weighted': (
                    (bill_usd / raw_kwh * 100.0) if raw_kwh else float('nan')
                ),
                'eia_class_price_cents_kwh': eia_px,
                'unscaled_twh': unscaled_twh,
                'eia_sales_twh': eia_twh,
                'unscaled_over_eia': (
                    unscaled_twh / eia_twh if eia_twh else float('nan')
                ),
                'scale_to_eia': (
                    eia_twh / unscaled_twh if unscaled_twh else float('nan')
                ),
                'scaled_twh': eia_twh,
                'scaled_over_eia': 1.0,
            }
        )
    return pd.DataFrame(rows), vintage


def run(years: list[int], mut_vintage: str | None) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    vintages: dict[int, str] = {}
    for year in years:
        frame, vintage = _class_table(year, mut_vintage)
        frames.append(frame)
        vintages[year] = vintage
        print(f'=== {year} (MUT {vintage}) ===')
        show = frame[
            [
                'class',
                'bill_bn_usd',
                'eia_revenue_bn_usd',
                'bill_over_eia_revenue',
                'price_used_cents_kwh_bill_weighted',
                'eia_class_price_cents_kwh',
                'unscaled_twh',
                'eia_sales_twh',
                'unscaled_over_eia',
                'scale_to_eia',
            ]
        ]
        print(show.to_string(index=False, float_format=lambda v: f'{v:,.3f}'))
        print()
    out = pd.concat(frames, ignore_index=True)
    out.attrs['mut_vintages'] = vintages
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--years',
        default='2017-2024',
        help='Single year, or start-end inclusive (default 2017-2024)',
    )
    parser.add_argument('--mut-vintage', default=None)
    args = parser.parse_args()
    if '-' in args.years:
        start, end = args.years.split('-', 1)
        years = list(range(int(start), int(end) + 1))
    else:
        years = [int(y) for y in args.years.split(',')]

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    table = run(years, args.mut_vintage)
    path = OUT_DIR / 'class_mwh_vs_eia.csv'
    table.to_csv(path, index=False)
    print(f'wrote {path}')

    # Compact year × class matrix of unscaled / EIA
    pivot = table.pivot(index='year', columns='class', values='unscaled_over_eia')
    pivot = pivot.reindex(columns=list(SCALED_CLASSES))
    print(
        '=== unscaled MWh / EIA sales (1.0 = match; 0.5 = Discussion #85-style miss) ==='
    )
    print(pivot.to_string(float_format=lambda v: f'{v:,.3f}'))
    pivot_path = OUT_DIR / 'unscaled_over_eia_pivot.csv'
    pivot.to_csv(pivot_path)
    print(f'wrote {pivot_path}')


if __name__ == '__main__':
    main()
