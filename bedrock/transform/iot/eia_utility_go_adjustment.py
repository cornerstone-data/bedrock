"""Rebase utility gross output on EIA volume x published price (#1009).

BEA's published gross output for electric power generation, transmission and
distribution (``221100``, ``UGO305-A``) **falls 9.6%** from 2022 to 2024 while
EIA's published retail revenue for the same industry **rises 6.0%**. That
divergence is not a bedrock derivation defect -- the nowcast reproduces BEA
faithfully -- but the row control is the ceiling on the whole electricity row,
so the shape propagates into the intermediate block, into the purchaser split,
and into the emission factor.

The test that isolates it is BEA's **implied price per kWh**: gross output
divided by EIA Table 2.2 retail sales, against Table 2.4's published average
price.

===== ================== ==================== =========================
year  BEA implied c/kWh  EIA published c/kWh  BEA over published
===== ================== ==================== =========================
2017  10.46              10.48                **-0.2%**
2019  11.10              10.54                +5.3%
2021  12.68              11.10                **+14.2%**
2022  14.17              12.36                **+14.7%**
2023  13.10              12.68                +3.3%
2024  12.65              12.94                **-2.2%**
===== ================== ==================== =========================

✅ **They agree to 0.2% at 2017**, which is what validates the units and the
mapping. Then BEA runs to +14.7% above the published price and ends 2.2% below
it. The published price rose smoothly (10.48 -> 12.94, +23.5%) on roughly flat
volume (+6.8%), so BEA's series carries something in 2021-23 that is neither
kWh nor price per kWh, and it unwinds by 2024.

Two legs, not one index
-----------------------

⚠️ **Indexing the whole row on retail revenue is wrong**, and the way it fails
is instructive. BEA's 2017 base already contains a wholesale wedge -- total
electric output (``221100`` plus government electric) exceeds EIA retail
revenue by **$78bn in 2017** -- and a single retail index forces that wedge to
grow at retail rates. Measured, that drives ``T005`` for the electric power
industry to **$126.6bn in 2022**, *below* the **$129.5bn** that investor-owned
utilities alone report spending on fuel and purchased power. An industry cannot
buy less than one ownership class within it.

So each controlled industry declares **two legs**:

``retail``
    What is sold to ultimate customers. Moves on EIA Table 2.3 revenue, which
    is Table 2.2 volume times Table 2.4 published price.
``wholesale``
    Sales for resale between utilities -- present in gross output, absent from
    retail by construction. Moves on EIA Table 8.3 purchased power (FERC Form
    1). ⚠️ Investor-owned only, so it is used as an **index**, never a level.

The 2017 split assigns the measured wedge across the electric industries in
proportion to their published gross output, then holds each leg's 2017 dollars
and moves them on their own index. At 2017 both indices are 1.0 by
construction, so the panel is returned unchanged -- the benchmark is observed
and outranks any index.

What it buys, measured
----------------------

===== ========== ============ ============ ==================
year  BEA $bn    two-leg $bn  implied      EIA published
                              c/kWh        c/kWh
===== ========== ============ ============ ==================
2017  389.4      389.4        10.46        10.48
2021  482.4      432.8        11.37        11.10
2022  556.6      509.5        12.97        12.36
2024  502.9      512.1        12.88        12.94
===== ========== ============ ============ ==================

Worst implied-price deviation falls from **+14.7% to +4.9%**, and the residual
excess sits in 2021-22 where wholesale prices genuinely outran retail -- which
is the wholesale leg doing its job rather than an error. ``T005`` stays at
**1.2-1.7x** investor-owned fuel-plus-purchased-power across the span and never
goes below it.

First-order emission-factor effect, emissions held flat and 2017 = 100:

========= ===== ===== ===== ===== ===== ===== ===== =====
basis     2017  2018  2019  2020  2021  2022  2023  2024
========= ===== ===== ===== ===== ===== ===== ===== =====
BEA today 100   92.2  91.3  97.3  80.9  71.4  77.4  77.5
two-leg   100   95.9  97.2  99.1  91.7  79.3  79.1  75.9
========= ===== ===== ===== ===== ===== ===== ===== =====

BEA's basis dips to 71.4 in 2022 and **rebounds**; the two-leg basis declines
monotonically after 2020. A rebound in emissions per dollar is the artefact
this removes.

Why the gross-output panel is the right place
---------------------------------------------

Every consumer of
:func:`~bedrock.transform.iot.derived_intermediate_and_value_added.detail_gross_output_panel`
-- the value-added allocation, ``T005``, ``T17``, the Supply GO control -- moves
together, which is the same argument
:mod:`~bedrock.transform.iot.ec_go_adjustment` makes for adjusting there.

Where the change lands, and why value added does not move
---------------------------------------------------------

``T1`` pins ``T005 + VAPRO = GO`` and ``T18`` pins ``VAPRO``, so together they
determine ``T005``. Value added is allocated from BEA's 191-row underlying
frame, and **``221100`` is alone in underlying line 13** -- a singleton group,
where the allocation rescale is the identity. So changing its gross output does
not move its ``VAPRO`` at all, and the whole change lands on ``T005``, the
industry's own intermediate purchases.

✅ That is the correct destination. Resale churn inflates both the seller's
output and the buyer's purchased-power expense; removing it from output and
from ``T005`` together removes both sides of one transaction. Value added is
untouched because resale adds no value.

⚠️ **Government electric is held out, and the reason is the wedge split rather
than value added.** The wedge is apportioned in proportion to published gross
output, which assumes every electric industry resells at the same intensity.
That is false for federal power: ``S00101`` is largely Bonneville and TVA, which
sell mostly **at wholesale**, so its true share of the wedge is far above its
3.4% share of output. Applying an output-share split would move its gross output
by -26% in 2021 on an assumption known to be wrong for it. Splitting the wedge
correctly needs resale by ownership class, which EIA does not publish at this
frame.

The value-added side was measured and is the *smaller* objection. Neither
government row is a singleton -- ``S00101`` shares underlying line 182 with
``491000`` (postal service) and ``S00102``; ``S00202`` shares line 188 with
``S00201`` (passenger transit) and ``S00203`` -- so rebasing them reallocates
``VAPRO``. Measured across 2017-2024 the worst case is **-2.34 billion USD out
of ``S00101`` in 2021**, of which **+2.32 billion lands on postal service**,
3.6% of its value added. ``S00201``, whose published ``VAPRO`` is negative,
moves by at most **0.03 billion** -- so the concern that transit would absorb a
reallocation does not survive measurement.

:data:`GOVERNMENT_ELECTRIC` is therefore wired and reported but excluded from
:data:`CONTROLLED` pending an ownership split of resale.

✅ **Gas distribution was tested on the same diagnostic and does not have this
defect.** ``221200`` also falls 4.9% from 2022 to 2024, so it was the obvious
second candidate. Built from the EIA v2 natural-gas API -- deliveries and prices
by consumer class, plus the published share of each class's deliveries that the
utility actually *sells* rather than merely transports -- the implied price per
Mcf sold against EIA's average delivered price runs:

===== ====== ====== ====== ====== ======
year  2017   2019   2021   2022   2024
===== ====== ====== ====== ====== ======
over  +3.4%  +8.6%  +17.1% +17.6% +34.4%
===== ====== ====== ====== ====== ======

⚠️ **That is a trend, not an excursion**, and it never returns. Electricity's
signature is a hump that unwinds -- +14.7% in 2022 back to -2.2% in 2024 -- which
is what marks it as churn. Gas diverges monotonically, and the mechanism is
observable: the share of deliveries the utility sells rather than transports
falls from **30.2% to 25.4%** across the span, so a growing part of its revenue
is distribution service on gas it never owns. Revenue per Mcf *sold* rises
because the denominator is shrinking, while revenue per Mcf *delivered* moves
only 2.81 -> 3.97 with the commodity price.

❌ There is also no base-year identity to validate a mapping against: gas starts
at +3.4% where electricity starts at -0.2%. And the two-leg decomposition does
not identify -- the residual after merchant revenue is **$2.3bn in 2017 rising
to $30.2bn in 2024**, a 13x move, and $2.3bn over the transported volume is
about **$0.13 per Mcf**, far below any real distribution tariff. A leg that
small at the base year cannot carry an index.

So gas stays uncontrolled, recorded in :data:`HELD_OUT` with that measurement
rather than as a missing source.
"""

from __future__ import annotations

import functools
import typing as ta

import numpy as np
import pandas as pd

if ta.TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Iterable

#: Base year. Both legs index to 1.0 here, so the panel is returned unchanged.
BASE_YEAR = 2017

#: Years the control is defined for. Table 8.3 and Tables 2.2/2.3/2.4 all run
#: the full span in the 2024 Electric Power Annual edition.
CONTROLLED_YEARS = (2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024)

#: Electric industries that share the wholesale wedge. Their published gross
#: output sets each one's share of it at the base year.
ELECTRIC_INDUSTRIES = ('221100', 'S00101', 'S00202')

#: Government electric utilities. Wired, reported, and **excluded** from
#: :data:`CONTROLLED`: the wedge is split on output share, which understates
#: federal power's resale intensity. See the module docstring.
GOVERNMENT_ELECTRIC = ('S00101', 'S00202')

#: Industries considered and deliberately **not** rebased, each mapped to the
#: measurement that ruled it out. Never silently skipped: :func:`report` prints
#: every entry so a held-out row stays visible rather than looking forgotten.
HELD_OUT = {
    '221200': (
        'tested and does not have the defect: the implied price runs +3.4% over '
        'published in 2017 rising monotonically to +34.4% in 2024, a trend '
        'rather than the electricity hump-and-unwind, driven by the utility '
        'sold share falling 30.2% -> 25.4%. The two-leg split also fails to '
        'identify -- a $2.3bn 2017 residual is $0.13/Mcf transported.'
    ),
    'S00101': (
        'the wedge is split on gross-output share, which understates federal '
        'power resale intensity -- Bonneville and TVA sell mostly at wholesale. '
        'Needs resale by ownership class, which EIA does not publish here.'
    ),
    'S00202': (
        'same output-share wedge problem as S00101, and not a singleton in the '
        'value-added allocation.'
    ),
}

#: Industries actually rebased. Deliberately just electricity: it is the only
#: one with both legs observed and a singleton value-added group.
CONTROLLED = ('221100',)


def _cell(panel: pd.DataFrame, industry: str, year: int) -> float:
    """One panel cell as a float; ``.at`` on a mixed-dtype frame is a union."""
    return float(np.asarray(panel.at[industry, year]).item())


def _electric_industries_in(panel: pd.DataFrame) -> list[str]:
    return [code for code in ELECTRIC_INDUSTRIES if code in panel.index]


@functools.cache
def retail_index(year: int) -> float:
    """EIA Table 2.3 retail revenue relative to :data:`BASE_YEAR`."""
    from bedrock.extract.disaggregation.egrid_generation import (  # noqa: PLC0415
        eia_retail_revenue_usd,
    )

    base = eia_retail_revenue_usd(BASE_YEAR)
    if base <= 0:
        raise ValueError(f'EIA retail revenue at {BASE_YEAR} is non-positive')
    return float(eia_retail_revenue_usd(year) / base)


@functools.cache
def wholesale_index(year: int) -> float:
    """EIA Table 8.3 purchased power relative to :data:`BASE_YEAR`.

    Investor-owned utilities only, so this is an index and not a level -- see
    the module docstring.
    """
    from bedrock.extract.disaggregation.egrid_generation import (  # noqa: PLC0415
        eia_purchased_power_usd,
    )

    base = eia_purchased_power_usd(BASE_YEAR)
    if base <= 0:
        raise ValueError(f'EIA purchased power at {BASE_YEAR} is non-positive')
    return float(eia_purchased_power_usd(year) / base)


def base_year_legs(panel: pd.DataFrame) -> pd.DataFrame:
    """Each electric industry's :data:`BASE_YEAR` retail and wholesale legs, $M.

    The wedge is measured once, on the whole electric industry -- published
    gross output of every code in :data:`ELECTRIC_INDUSTRIES` less EIA retail
    revenue -- and then split across them in proportion to that gross output.
    Assigning it per industry instead would need an ownership split of resale
    that EIA does not publish at this frame.
    """
    from bedrock.extract.disaggregation.egrid_generation import (  # noqa: PLC0415
        eia_retail_revenue_usd,
    )

    codes = _electric_industries_in(panel)
    if not codes:
        raise KeyError(f'none of {ELECTRIC_INDUSTRIES} is in the gross-output panel')
    go = panel.loc[codes, BASE_YEAR].astype(float)
    total = float(go.sum())
    if total <= 0:
        raise ValueError(f'electric gross output at {BASE_YEAR} is non-positive')
    retail_usd = eia_retail_revenue_usd(BASE_YEAR)
    wedge = total - retail_usd / 1e6
    if wedge <= 0:
        raise ValueError(
            f'{BASE_YEAR} wholesale wedge is non-positive ({wedge:,.0f} $M); '
            'published gross output should exceed retail revenue'
        )
    share = go / total
    wholesale = wedge * share
    return pd.DataFrame({'wholesale': wholesale, 'retail': go - wholesale})


def two_leg_gross_output(panel: pd.DataFrame, year: int) -> pd.Series:
    """Rebased gross output for the electric industries in *year*, $M.

    Returned for every code in :data:`ELECTRIC_INDUSTRIES`, whether or not it
    is in :data:`CONTROLLED`, so ``--check`` can grade the ones that are held.
    """
    if int(year) not in CONTROLLED_YEARS:
        raise ValueError(
            f'{year} is outside the controlled span {CONTROLLED_YEARS[0]}-'
            f'{CONTROLLED_YEARS[-1]}'
        )
    legs = base_year_legs(panel)
    return legs['retail'] * retail_index(int(year)) + legs['wholesale'] * (
        wholesale_index(int(year))
    )


def apply_eia_utility_adjustment(raw: pd.DataFrame) -> pd.DataFrame:
    """The gross-output panel with :data:`CONTROLLED` rows rebased on EIA.

    ``raw`` is industries x years in million USD; a new frame is returned and
    the argument is not mutated. Rows outside :data:`CONTROLLED` and years
    outside :data:`CONTROLLED_YEARS` come back bit-identical, and
    :data:`BASE_YEAR` is unchanged for every row because both indices are 1.0
    there.

    ⚠️ **Group totals are not preserved**, unlike
    :func:`~bedrock.transform.iot.ec_go_adjustment.apply_ec_adjustment`. That
    is the point: the level is what disagrees with EIA, so holding the summary
    group would reintroduce it. The compensating change lands in ``T005`` for
    the same industry, not in a sibling.
    """
    adjusted = raw.copy()
    codes = [code for code in CONTROLLED if code in raw.index]
    if not codes:
        return adjusted
    years = [year for year in CONTROLLED_YEARS if year in raw.columns]
    for year in years:
        rebased = two_leg_gross_output(raw, year)
        for code in codes:
            adjusted.at[code, year] = float(rebased.at[code])
    return adjusted


#: Table 2.2 classes that are **sold** to ultimate customers. ``Direct Use`` is
#: excluded: it is self-generated electricity that is never sold, so it has no
#: revenue behind it and belongs in neither leg.
RETAIL_SALES_CLASSES = ('Residential', 'Commercial', 'Industrial', 'Transportation')


def retail_sales_mwh(year: int) -> float:
    """EIA Table 2.2 sales to ultimate customers, MWh, excluding direct use."""
    from bedrock.extract.disaggregation.egrid_generation import (  # noqa: PLC0415
        eia_table_2_2_end_use_mwh,
    )

    sales = eia_table_2_2_end_use_mwh(int(year))
    mwh = sum(float(sales[key]) for key in RETAIL_SALES_CLASSES)
    if mwh <= 0:
        raise ValueError(f'EIA Table 2.2 retail sales are non-positive for {year}')
    return mwh


def implied_price_cents_per_kwh(panel: pd.DataFrame, year: int) -> float:
    """``221100`` gross output over EIA retail sales MWh, cents per kWh.

    The diagnostic that isolates the defect. Compared against Table 2.4's
    published average price it asks whether the row carries anything that is
    neither kWh nor price per kWh.

    ⚠️ **It is the controlled row alone over sales alone**, and the two scope
    mismatches are what make it usable rather than circular. ``221100``
    excludes government-owned utilities, which pushes the numerator *down*
    relative to EIA's whole-industry sales; it includes sales for resale, which
    pushes it *up*. At the 2017 benchmark those cancel to **-0.2%**, and that
    near-identity is the check that the units and the mapping are right. Adding
    government electric to the numerator or direct use to the denominator
    breaks the agreement and the diagnostic stops meaning anything.
    """
    usd = _cell(panel, '221100', int(year)) * 1e6
    return usd / retail_sales_mwh(int(year)) / 10.0


@functools.cache
def published_price_cents_per_kwh(year: int) -> float:
    """EIA Table 2.4 average price of electricity to ultimate customers, c/kWh."""
    from bedrock.extract.disaggregation.egrid_generation import (  # noqa: PLC0415
        _epa_fba,
        _table_mask,
    )

    df = _epa_fba(int(year))
    table = df.loc[_table_mask(df, int(year), 'Table 2.4')]
    rows = table.loc[
        (table['ActivityProducedBy'] == 'Total Electric Industry')
        & (table['ActivityConsumedBy'] == 'Total')
    ]
    if rows.empty:
        raise ValueError(f'Table 2.4 has no Total Electric Industry total for {year}')
    return float(rows['FlowAmount'].iloc[0])


def report(years: Iterable[int] = CONTROLLED_YEARS) -> pd.DataFrame:
    """Per-year gross output, implied price and published price, both bases."""
    from bedrock.transform.iot.derived_intermediate_and_value_added import (  # noqa: PLC0415
        detail_gross_output_panel,
    )

    raw = detail_gross_output_panel(ec_adjusted=False)
    adjusted = apply_eia_utility_adjustment(raw)
    rows = []
    for year in years:
        published = published_price_cents_per_kwh(year)
        raw_price = implied_price_cents_per_kwh(raw, year)
        new_price = implied_price_cents_per_kwh(adjusted, year)
        row: dict[str, object] = {
            'year': int(year),
            'published_c_kwh': published,
            'raw_implied_c_kwh': raw_price,
            'adjusted_implied_c_kwh': new_price,
            'raw_over_published_pct': 100.0 * (raw_price / published - 1.0),
            'adjusted_over_published_pct': 100.0 * (new_price / published - 1.0),
        }
        for code in ELECTRIC_INDUSTRIES:
            if code not in raw.index:
                continue
            row[f'{code}_raw'] = _cell(raw, code, int(year))
            row[f'{code}_two_leg'] = float(two_leg_gross_output(raw, year).loc[code])
            row[f'{code}_controlled'] = code in CONTROLLED
        rows.append(row)
    return pd.DataFrame(rows).set_index('year')


def check() -> int:
    """Validate the control. Returns a process exit code.

    Four assertions, each one a way the control could be wrong rather than
    merely different:

    1. :data:`BASE_YEAR` is untouched for every row -- the benchmark is
       observed and must outrank the index.
    2. Every controlled row stays positive.
    3. The adjusted implied price is closer to the published price than the raw
       one is, in every year off the base. If it were not, the control would be
       making the thing it exists to fix worse.
    4. No industry in :data:`HELD_OUT` is silently rebased.
    """
    from bedrock.transform.iot.derived_intermediate_and_value_added import (  # noqa: PLC0415
        detail_gross_output_panel,
    )

    raw = detail_gross_output_panel(ec_adjusted=False)
    adjusted = apply_eia_utility_adjustment(raw)
    failures: list[str] = []

    base_gap = (adjusted[BASE_YEAR] - raw[BASE_YEAR]).abs().max()
    if base_gap > 1e-6:
        failures.append(f'{BASE_YEAR} moved by {base_gap:,.3f} $M; it must not')

    uncontrolled = [code for code in raw.index if code not in CONTROLLED]
    side_gap = (adjusted.loc[uncontrolled] - raw.loc[uncontrolled]).abs().max().max()
    if side_gap > 1e-6:
        failures.append(
            f'an uncontrolled industry moved by {side_gap:,.3f} $M; only '
            f'{CONTROLLED} may change'
        )

    for code in CONTROLLED:
        if code not in adjusted.index:
            failures.append(f'{code} is controlled but absent from the panel')
            continue
        held = [year for year in CONTROLLED_YEARS if year in adjusted.columns]
        worst = min(_cell(adjusted, code, year) for year in held)
        if worst <= 0:
            failures.append(f'{code} gross output goes non-positive ({worst:,.0f} $M)')

    for code in HELD_OUT:
        if code in CONTROLLED:
            failures.append(
                f'{code} is in HELD_OUT and must not be controlled: '
                f'{HELD_OUT[code]}'
            )

    table = report()
    off_base = table.drop(index=[BASE_YEAR], errors='ignore')
    worse = off_base.loc[
        off_base['adjusted_over_published_pct'].abs()
        > off_base['raw_over_published_pct'].abs() + 1e-9
    ]
    if not worse.empty:
        failures.append(
            'the control moves the implied price further from published in '
            f'{list(worse.index)}'
        )

    print(table.round(2).to_string())
    print()
    for code, why in HELD_OUT.items():
        print(f'HELD OUT {code}: {why}')
    if GOVERNMENT_ELECTRIC:
        print(
            'HELD '
            + ', '.join(GOVERNMENT_ELECTRIC)
            + ': the wedge is split on gross-output share, which understates '
            'federal power resale intensity (Bonneville and TVA sell mostly at '
            'wholesale). Needs resale by ownership class, which EIA does not '
            'publish at this frame.'
        )
    if failures:
        print()
        for failure in failures:
            print(f'FAIL {failure}')
        return 1
    print('\nOK')
    return 0


def main() -> int:
    import argparse  # noqa: PLC0415

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--check', action='store_true', help='validate the control and exit non-zero'
    )
    parser.add_argument('--csv', type=str, default=None, help='write the report here')
    args = parser.parse_args()
    if args.csv:
        report().to_csv(args.csv)
        print(f'wrote {args.csv}')
    if args.check:
        return check()
    if not args.csv:
        print(report().round(2).to_string())
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
