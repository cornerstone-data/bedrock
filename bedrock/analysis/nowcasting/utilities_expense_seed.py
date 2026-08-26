"""Can EIA's fuel receipts seed the electric power columns?  (#719)

✅ **Yes.**  On :mod:`~.benchmark_holdout` -- seed the observed **2012** block
with EIA's 2012 -> 2017 movement, score against the observed **2017** block, at
detail -- the seed gains **+18.9% on dollars and +16.0% on impact**, and **all
three columns win on both weightings**:

============  ========  ========  ==========
column          frozen    seeded    ``N`` gain
============  ========  ========  ==========
``221100``     0.1937    0.1591     **+17.9%**
``S00101``     0.3404    0.3064      +10.0%
``S00202``     0.0672    0.0581      +13.5%
============  ========  ========  ==========

⚠️ **These are among the worst-drifting columns in the table**: ``S00101``
drifts **0.340** and ``221100`` **0.194**, against an economy-wide 0.0606.
✅ **This is the first live route into the held block** -- trade, agriculture's
neighbours and the government function bridge were all rejected, and $3,577B
sits at a frozen 2017 mix.

Why it works where the survey did not
---------------------------------------

§S6 refused this column because what it does is a **price-driven reweighting**
of the fuel bill, and the services panel's *share* index divides common
movement out by construction -- it moved the purchased-fuels line the wrong way
and made ``22`` the largest single drag on that block, -119% of its gain.

✅ **EIA measures receipts x delivered price, which is exactly a reweighting.**
And the fuel *mix* is doing the work, not a general "fuel got cheaper" signal:
one uniform index across all three fuels scores **+7.1%** against the per-fuel
**+16.0%** (:func:`legs`).

⚠️ **The coal leg carries it.**  Dropping coal takes the gain to **-0.3%**;
coal alone is **+10.3%**.  Gas and petroleum each add a few points and neither
is positive on its own.  ❌ **And coal is the leg with the least future**: it
was 51.7% of the fuel bill in 2012 and **16.6% in 2022**.  The span that could
be tested is the span coal dominated.

What the seed is, exactly
---------------------------

Four rows move (:func:`carriers`), and they carry **85.6% of the columns'**
``N``:

===========  ========  =============  ===========  ==========
row            EIA      BEA observed    2012 $M      ``N`` %
===========  ========  =============  ===========  ==========
``212100``    0.635        0.488         22,488       19.0
``211000``    0.937        0.849         34,118       24.6
``324110``    0.489        0.573         30,277       13.0
``221100``    0.927        0.784         14,645       29.0
===========  ========  =============  ===========  ==========

✅ **Every one of the four moves the way BEA observed**, and coal and petroleum
land close.  ⚠️ Gas and purchased power under-move, which is why the seeded gas
*share* still ends above the observed one -- the gain comes from coal and
petroleum being right, not from all four being right.

⚠️ **The index is not divided by the industry's expense growth**, unlike every
other seed here.  ``escalator`` is what the rows EIA does not name are assumed
to have done; **1.0, nominally flat, is both the best-scoring choice and the
one that keeps the seed independent of anything observed at the target year**
(:func:`escalators`).  Dividing by the column total scores +14.6%; the
observed growth of the untouched rows would score +21.3% and is **not available
in production**.

✅ **It is not a knife edge** (:func:`amplitude`).  The gain stays positive with
the index raised to any power from 0.25 to 2.0, peaking between 0.75 and 1.0.

⚠️ The risk in it: only gas matches on level
----------------------------------------------

:func:`scope` puts EIA's dollars beside the BEA rows they move, at 2017:

=======  ===========  ==========  ==========
fuel      BEA row       EIA $M      BEA $M
=======  ===========  ==========  ==========
coal      ``212100``     22,605      10,968
gas       ``211000``     30,019      28,972
oil       ``324110``        566      17,351
=======  ===========  ==========  ==========

✅ The four rows together are within **7%** of BEA's, and gas matches almost
exactly.  ❌ **Coal is 2.1x BEA's row and oil is 3% of it** -- BEA's ``324110``
row for the electric columns is not oil-fired generation, whatever else it is.
⚠️ **That is the ``ecnpurelec`` failure mode** -- indexing a concept the Use
table does not carry -- and the petroleum leg only works because refined
product **prices** moved both series the same way.  ⚠️ **It is the first leg to
drop** if the seed misbehaves in a year when prices and volumes part company.

What is not addressed
-----------------------

❌ **``221200`` gas distribution ($24.0B, drift 0.204) and ``221300`` water
($5.4B) stay held.**  Not for want of data: BEA books gas bought for resale
**net**, so ``ecnpurgas``'s $51.4B of resale purchases faces a ``221200``
own-row cell of **$1M**.  EIA form 176 measures the gross concept and there is
no cell entitled to receive it.

⚠️ **The purchased-power leg indexes a net concept on a gross one** for the same
reason -- $9.5B of own-row electricity against $100.7B bought for resale.  It
adds 3.5 points and is kept, with that stated.

⚠️ **Three columns, one span.**  2012 -> 2017 is the only pair with an observed
benchmark on both ends and EIA 923 coverage (the form starts in 2008, so the
2007 benchmark cannot be a second base).

What it says about the years it is wanted for
-----------------------------------------------

⚠️ **Not a test** -- nothing after 2017 is observed at detail -- but it is why
#719 exists.  Gas went **$27.1B (2020) -> $58.8B (2021) -> $85.7B (2022)**,
and the fuel bill's gas share **67% -> 82%**, while coal fell to 16.6%.  θ is
**0.0** across exactly this surge, so the held column is close to literally
frozen through the years that move most.

⚠️ **Nothing here is wired into the pipeline.**  ``derive_initial_U_intermediate``
still holds ``22`` at ``NOT_SEEDED``; this module measures the seed, it does not
install it.

⚠️ **A partial year reads as a collapse.**  PUDL publishes the current year
month by month, so :func:`fuel_costs` keeps only years with all twelve months.

Run::

    uv run python -m bedrock.analysis.nowcasting.utilities_expense_seed --all
"""

from __future__ import annotations

import argparse
import functools
import os

import numpy as np
import pandas as pd
import requests

#: PUDL's public S3 mirror -- no credentials, CC-BY-4.0, quarterly stable
#: releases.  ⚠️ **Check a table name against the bucket listing before
#: assuming it**: there is no ``core_eia176__yearly_company_data`` and no
#: ``out_eia861__yearly_sales``.
PUDL = 'https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/stable'

#: EIA form 923, every fuel receipt at every reporting power plant, 2008-2024,
#: with delivered cost.  ✅ **Covers independent power producers**, which is
#: what rules out EPA's Table 8.3 -- major investor-owned utilities only, with
#: 41-45% of generation missing.
FUEL_RECEIPTS = 'out_eia923__fuel_receipts_costs'

#: EIA form 861 operational data, for the purchased-power leg.
RESALE_REVENUE = 'core_eia861__yearly_operational_data_revenue'

#: PUDL's own fuel grouping -> the BEA detail commodity that carries it.
FUEL_TO_BEA: dict[str, str] = {
    'coal': '212100',
    'gas': '211000',
    'oil': '324110',
}

#: ⚠️ **The three electric columns are scored together.**  Cornerstone merges
#: government utilities into the private ones at Step 7 anyway, and BEA's split
#: between them is not one EIA can reproduce: EIA reports by plant and utility,
#: not by the ownership sector BEA books the column under.
ELECTRIC: tuple[str, ...] = ('221100', 'S00101', 'S00202')

#: ❌ **Not seeded, and not for want of data.**  ``221200`` gas distribution and
#: ``221300`` water have no cell for what EIA measures: BEA books gas bought for
#: resale **net**, so ``ecnpurgas``'s $51.4B of resale purchases faces a
#: ``221200`` own-row cell of **$1M**.  ⚠️ **$29.4B stays open.**
UNSEEDED: tuple[str, ...] = ('221200', '221300')

#: The purchased-power leg.  ⚠️ ``221100``'s own-row cell is $9.5B against
#: $100.7B of electricity bought for resale, so this indexes a **net** concept
#: on a **gross** one; :func:`legs` measures what it is worth on its own.
PURCHASED_POWER = '221100'


def _cost(frame: pd.DataFrame, year: int, fuel: str) -> float:
    """One cell as a float; pandas types ``.at`` as a very wide union."""
    return float(np.asarray(frame.at[year, fuel], dtype=float))


def _cached(table: str) -> str:
    """Download a PUDL table once into the extract input area."""
    from bedrock.utils.io.local_extract_input_data import (  # noqa: PLC0415
        local_extract_input_dir,
    )

    directory = local_extract_input_dir('PUDL')
    path = os.path.join(directory, f'{table}.parquet')
    if not os.path.exists(path):
        response = requests.get(f'{PUDL}/{table}.parquet', timeout=900)
        response.raise_for_status()
        with open(path, 'wb') as handle:
            handle.write(response.content)
    return path


@functools.cache
def fuel_costs() -> pd.DataFrame:
    """``year x {coal, gas, oil}`` delivered fuel cost at power plants, $M.

    ⚠️ **Incomplete years are dropped, and the trap is real**: PUDL publishes
    the current year month by month, so the table carries a partial year that
    would read as a collapse in fuel purchases.  A year survives only if all
    twelve months are present.
    """
    frame = pd.read_parquet(
        _cached(FUEL_RECEIPTS),
        columns=['report_date', 'fuel_type_code_pudl', 'total_fuel_cost'],
    )
    dates = pd.to_datetime(frame['report_date'])
    frame['year'] = dates.dt.year
    months = frame.assign(month=dates.dt.month).groupby('year')['month'].nunique()
    frame = frame[frame['year'].isin(months[months == 12].index)]
    table = frame.pivot_table(
        index='year',
        columns='fuel_type_code_pudl',
        values='total_fuel_cost',
        aggfunc='sum',
        observed=True,
    )
    return (table / 1e6).astype(float)


@functools.cache
def resale_revenue() -> 'pd.Series[float]':
    """``year -> sales-for-resale revenue``, $M -- the purchased-power leg."""
    frame = pd.read_parquet(
        _cached(RESALE_REVENUE), columns=['report_date', 'revenue_class', 'revenue']
    )
    frame['year'] = pd.to_datetime(frame['report_date']).dt.year
    resale = frame[frame['revenue_class'] == 'sales_for_resale']
    return (resale.groupby('year')['revenue'].sum() / 1e6).astype(float)


def relative_index(
    year: int,
    base_year: int,
    *,
    escalator: float = 1.0,
    purchased_power: bool = True,
) -> 'pd.Series[float]':
    """EIA's ``base_year -> year`` movement for the rows EIA names.

    ⚠️ **Unlike every other seed in this package the index is not divided by
    the industry's own expense growth**, and that is deliberate.  ``escalator``
    is what the rows EIA does *not* name are assumed to have done, and **1.0 --
    nominally flat -- scores better** than dividing by the column total
    (:func:`escalators`).  ✅ It also leaves the seed independent of everything
    observed at the target year, which removes the leakage question entirely.

    ⚠️ **The level is not claimed, only the movement** (:func:`scope`).
    """
    costs = fuel_costs()
    if year not in costs.index or base_year not in costs.index:
        raise ValueError(f'EIA 923 does not cover {base_year} -> {year}')
    values = {
        FUEL_TO_BEA[fuel]: _cost(costs, year, fuel)
        / _cost(costs, base_year, fuel)
        / escalator
        for fuel in FUEL_TO_BEA
        if fuel in costs.columns
    }
    if purchased_power:
        revenue = resale_revenue()
        values[PURCHASED_POWER] = (
            float(revenue.loc[year] / revenue.loc[base_year]) / escalator
        )
    return pd.Series(values)


def _score(index: 'pd.Series[float]', weighting: str) -> dict[str, float]:
    from bedrock.analysis.nowcasting.benchmark_holdout import (  # noqa: PLC0415
        aggregate,
        holdout_score,
    )

    return aggregate(holdout_score(lambda _column: index, list(ELECTRIC), weighting))


def utilities_holdout(
    weighting: str = 'impact',
    *,
    purchased_power: bool = True,
    escalator: float = 1.0,
) -> pd.DataFrame:
    """✅ **The test that decides this seed**, per :mod:`~.benchmark_holdout`.

    Seed the **observed 2012** benchmark detail block with EIA's 2012 -> 2017
    fuel movement and score against the **observed 2017** block, at detail.
    """
    from bedrock.analysis.nowcasting.benchmark_holdout import (  # noqa: PLC0415
        BASE,
        TARGET,
        holdout_score,
    )

    index = relative_index(
        TARGET, BASE, escalator=escalator, purchased_power=purchased_power
    )
    return holdout_score(lambda _column: index, list(ELECTRIC), weighting)


def carriers() -> pd.DataFrame:
    """Per row: what EIA says it did, what BEA observed, and what it carries.

    ⚠️ **The trade regrade's lesson, applied before the fact** -- a source that
    tracks only where the impact is not is worth nothing, so each row's share of
    the columns' ``N`` sits beside its movement.
    """
    from bedrock.analysis.nowcasting.benchmark_holdout import (  # noqa: PLC0415
        BASE,
        TARGET,
        block,
        intensity,
    )

    early = block(BASE)[list(ELECTRIC)].sum(axis=1)
    late = block(TARGET)[list(ELECTRIC)].sum(axis=1)
    weights = intensity()
    weighted = late * weights.reindex(late.index).fillna(0.0)
    index = relative_index(TARGET, BASE)

    records = [
        {
            'commodity': str(code),
            'eia': float(value),
            'bea_observed': float(late.loc[str(code)] / early.loc[str(code)]),
            f'{BASE}_$M': float(early.loc[str(code)]),
            f'{TARGET}_$M': float(late.loc[str(code)]),
            'share_N_%': 100 * float(weighted.loc[str(code)] / weighted.sum()),
        }
        for code, value in index.items()
    ]
    return pd.DataFrame(records).set_index('commodity')


def scope() -> pd.DataFrame:
    """EIA's dollars against the BEA rows they are asked to move.

    ❌ **Only gas matches on level.**  ⚠️ That is the ``ecnpurelec`` risk -- an
    index of a concept the Use table may not carry -- and it is why
    :func:`legs` reports each leg on its own.
    """
    from bedrock.analysis.nowcasting.benchmark_holdout import (  # noqa: PLC0415
        BASE,
        TARGET,
        block,
    )

    costs = fuel_costs()
    early = block(BASE)[list(ELECTRIC)].sum(axis=1)
    late = block(TARGET)[list(ELECTRIC)].sum(axis=1)
    records = [
        {
            'fuel': fuel,
            'bea_row': code,
            f'eia_{BASE}': _cost(costs, BASE, fuel),
            f'bea_{BASE}': float(early.loc[code]),
            f'eia_{TARGET}': _cost(costs, TARGET, fuel),
            f'bea_{TARGET}': float(late.loc[code]),
            'eia/bea': _cost(costs, TARGET, fuel) / float(late.loc[code]),
        }
        for fuel, code in FUEL_TO_BEA.items()
    ]
    return pd.DataFrame(records).set_index('fuel')


def legs(weighting: str = 'impact') -> pd.DataFrame:
    """Which leg carries the gain, one out and one in at a time.

    ⚠️ **The last line is the null this has to beat**: one index for all three
    fuels, carrying "fuel got cheaper" and nothing about which fuel.
    """
    from bedrock.analysis.nowcasting.benchmark_holdout import (  # noqa: PLC0415
        BASE,
        TARGET,
    )

    index = relative_index(TARGET, BASE)
    full = _score(index, weighting)
    records: list[dict[str, object]] = [
        {'variant': 'all four legs', 'gain_%': full['gain_%'], 'wins': full['wins']}
    ]
    for code in index.index:
        dropped = _score(index.drop(index=[code]), weighting)
        kept = _score(index.loc[[code]], weighting)
        records.append(
            {
                'variant': f'without {code}',
                'gain_%': dropped['gain_%'],
                'wins': dropped['wins'],
            }
        )
        records.append(
            {'variant': f'{code} alone', 'gain_%': kept['gain_%'], 'wins': kept['wins']}
        )
    costs = fuel_costs()
    ratio = float(costs.loc[TARGET].sum() / costs.loc[BASE].sum())
    flat = _score(pd.Series(dict.fromkeys(FUEL_TO_BEA.values(), ratio)), weighting)
    records.append(
        {
            'variant': 'one uniform fuel index',
            'gain_%': flat['gain_%'],
            'wins': flat['wins'],
        }
    )
    return pd.DataFrame(records).set_index('variant')


def escalators() -> pd.DataFrame:
    """What the rows EIA does not name are assumed to have done.

    ⚠️ **The last line is not available in production** -- it is the observed
    growth of exactly the rows the seed does not touch, and it is here as the
    ceiling on what a better escalator could buy.
    """
    from bedrock.analysis.nowcasting.benchmark_holdout import (  # noqa: PLC0415
        BASE,
        TARGET,
        block,
    )

    early = block(BASE)[list(ELECTRIC)]
    late = block(TARGET)[list(ELECTRIC)]
    named = [*FUEL_TO_BEA.values(), PURCHASED_POWER]
    total = float(late.to_numpy().sum() / early.to_numpy().sum())
    rest = float(
        late.drop(index=named).to_numpy().sum()
        / early.drop(index=named).to_numpy().sum()
    )
    records = []
    for label, value in (
        ('1.0 nominally flat (default)', 1.0),
        ('the column total', total),
        ('the untouched rows, observed', rest),
    ):
        index = relative_index(TARGET, BASE, escalator=value)
        row: dict[str, object] = {'escalator': label, 'value': value}
        for weighting in ('dollar', 'impact'):
            summary = _score(index, weighting)
            row[f'{weighting}_gain_%'] = summary['gain_%']
            row[f'{weighting}_wins'] = summary['wins']
        records.append(row)
    return pd.DataFrame(records).set_index('escalator')


def amplitude() -> pd.DataFrame:
    """Is the gain a knife edge?  The index raised to a power, both weightings."""
    from bedrock.analysis.nowcasting.benchmark_holdout import (  # noqa: PLC0415
        BASE,
        TARGET,
    )

    index = relative_index(TARGET, BASE)
    records = []
    for power in (0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 2.0):
        row: dict[str, object] = {'power': power}
        for weighting in ('dollar', 'impact'):
            summary = _score(index**power, weighting)
            row[f'{weighting}_gain_%'] = summary['gain_%']
            row[f'{weighting}_wins'] = summary['wins']
        records.append(row)
    return pd.DataFrame(records).set_index('power')


def surge(
    years: tuple[int, ...] = (2012, 2017, 2020, 2021, 2022, 2023, 2024),
) -> pd.DataFrame:
    """What EIA says about the years the seed is actually wanted for.

    ⚠️ **Not a test** -- there is no observed detail benchmark after 2017.  It
    is the movement #719 exists for: the services panel moved the
    purchased-fuels line the **wrong way** across 2021-22.
    """
    costs = fuel_costs()
    frame = costs.loc[list(years)]
    out = frame.copy()
    out['total'] = frame.sum(axis=1)
    for column in frame.columns:
        out[f'{column}_%'] = 100 * frame[column] / out['total']
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--holdout', action='store_true', help='THE test: 2012 -> observed 2017'
    )
    parser.add_argument('--carriers', action='store_true', help='row by row')
    parser.add_argument('--legs', action='store_true', help='which leg carries it')
    parser.add_argument('--scope', action='store_true', help='EIA against BEA')
    parser.add_argument('--surge', action='store_true', help='what EIA says 2020-24')
    parser.add_argument('--all', action='store_true', help='every measurement')
    args = parser.parse_args()
    chosen = args.holdout or args.carriers or args.legs or args.scope or args.surge
    pd.set_option('display.width', 200)

    if args.all or args.holdout or not chosen:
        print('\nTHE test: seed observed 2012, score against observed 2017\n')
        for weighting in ('dollar', 'impact'):
            summary = _score(relative_index(2017, 2012), weighting)
            print(
                f'  {weighting:>7}  frozen {summary["frozen"]:.4f} -> seeded '
                f'{summary["seeded"]:.4f}   gain {summary["gain_%"]:+.1f}%   '
                f'{int(summary["wins"])}/{int(summary["columns"])} columns win'
            )
        print()
        print(utilities_holdout('impact')[['frozen', 'seeded', 'gain_%']].round(4))
        print('\n  what the untouched rows are assumed to have done\n')
        print(escalators().round(2).to_string())
        print('\n  and whether the gain is a knife edge\n')
        print(amplitude().round(2).to_string())
    if args.all or args.carriers or not chosen:
        print('\nRow by row: EIA against what BEA observed\n')
        print(carriers().round(3).to_string())
    if args.all or args.legs or not chosen:
        print('\nWhich leg carries the gain\n')
        print(legs().round(2).to_string())
    if args.all or args.scope or not chosen:
        print('\nScope: EIA dollars against the BEA rows they move, $M\n')
        print(scope().round(1).to_string())
    if args.all or args.surge or not chosen:
        print('\nWhat EIA says about the years the seed is wanted for, $M\n')
        print(surge().round(1).to_string())


if __name__ == '__main__':
    main()
