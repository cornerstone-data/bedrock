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

❌ **``221200`` gas distribution ($24.0B, drift 0.204) stays held -- tested on
EIA form 176 and rejected** (:func:`gas_distribution`), for three reasons, any
one of which would be enough:

1. ⚠️ **The only index that wins is a form change.**
   ``citygate_receipts_sales_customers`` falls 45% across the span and scores
   **+29.0%** -- and EIA **split that line in 2014**, when
   ``citygate_receipts_transportation_customers`` first appears.  The tell is
   receipts over merchant sales volume: **1.2-1.6 through 2013, then 0.8 in
   every one of the eleven years since** (:func:`form_176`).  ✅ **Every concept
   continuous across the break loses** -- merchant volume -2.0%, merchant
   revenue -4.2%, implied price -2.2% -- and the merchant function is flat:
   6,959 -> 7,237 bcf, $57.0B -> $62.0B.  ⚠️ **The respondent count is smooth
   across 2014**, so it is the form, not the universe.
2. ❌ **Form 176 is not a survey of the utilities sector** (:func:`universe`).
   Its ~2,000 respondents include interstate and intrastate **pipelines**,
   storage and LNG operators and direct-delivery **producers**, each filing one
   report per state: **78-81% of the volume is pipeline movement** (``486000``)
   against **17%** delivered to consumers, and ~30 respondents a year report
   producer lease use (``211000``).  ⚠️ Its volumes are **not additive** --
   total disposition is about **5.7x** US consumption, the same molecule counted
   at each step of the chain.
3. ⚠️ **And the concept BEA carries is net anyway**: ``ecnpurgas``'s $51.4B of
   gas bought for resale faces a ``221200`` own-row cell of **$1M**.

⚠️ **The general lesson is the mirror of the trade regrade.**  There a wrong
answer key made a sound source look useless; here a **vintage break in the
source** makes a useless index look excellent.  ✅ **A holdout gain is only as
good as the source's continuity across the span** -- check the series through
the break years before believing one.

❌ **``221300`` water ($5.4B) has no EIA source at all.**

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

#: EIA form 176, the natural gas company annual report -- what BEA's Table C1
#: names for gas.  ⚠️ **Volumes only in the supply table**; the revenue is in
#: the by-consumer disposition table.
GAS_SUPPLY = 'core_eia176__yearly_gas_supply'
GAS_BY_CONSUMER = 'core_eia176__yearly_gas_disposition_by_consumer'
GAS_DISPOSITION = 'core_eia176__yearly_gas_disposition'

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

#: ❌ **Not seeded.**  ``221200`` gas distribution was **tested on form 176 and
#: rejected** (:func:`gas_distribution`) -- the only index that wins is a 2014
#: form change in disguise.  ``221300`` water has no EIA source at all.
#: ⚠️ **$29.4B stays open.**
UNSEEDED: tuple[str, ...] = ('221200', '221300')

#: The gas distribution column, tested and refused.
GAS_DISTRIBUTION = '221200'

#: The purchased-power leg.  ⚠️ ``221100``'s own-row cell is $9.5B against
#: $100.7B of electricity bought for resale, so this indexes a **net** concept
#: on a **gross** one; :func:`legs` measures what it is worth on its own.
PURCHASED_POWER = '221100'


def _cell(frame: pd.DataFrame, year: int, column: str) -> float:
    """One cell as a float; pandas types ``.at`` as a very wide union."""
    return float(np.asarray(frame.at[year, column], dtype=float))


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
        FUEL_TO_BEA[fuel]: _cell(costs, year, fuel)
        / _cell(costs, base_year, fuel)
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


def _use_2017_detail() -> pd.DataFrame:
    """2017 benchmark detail Use intermediate block, commodity x industry, $M."""
    from bedrock.analysis.nowcasting.inputs_structure import (  # noqa: PLC0415
        _use_2017_detail as use,
    )

    return use()


def utilities_seed(year: int, base_year: int = 2017) -> pd.DataFrame:
    """The seed: BEA's 2017 electric-power columns, moved on EIA's fuel index.

    ``commodity x BEA detail industry`` in $M on the benchmark Use axes, for the
    three :data:`ELECTRIC` columns -- the same shape ``inputs_structure``'s and
    ``services_transport_expense_seed``'s seeds return, so they compose in
    ``nowcast_intermediate.composed_seed``.

    Four rows move: coal ``212100``, gas ``211000``, oil ``324110`` and
    purchased power ``221100``. ⚠️ **Rows EIA does not name hold their 2017
    value**, which is :func:`relative_index`'s ``escalator = 1.0`` and is the
    configuration that was graded -- dividing by the column's own growth scored
    worse (:func:`escalators`).

    ⚠️ **The three columns take the same index.** BEA's split between private
    ``221100`` and the two government utility columns is not one EIA can
    reproduce, which is why :data:`ELECTRIC` is scored as a group; see its note.

    ⚠️ **No renormalisation**, matching ``ore_seed``. Only column *shares* are
    read downstream -- Step 3 owns the level through ``GO - VAPRO`` -- so a
    column rescale here would be a no-op on everything that consumes this.

    ⚠️ **``221200`` and ``221300`` are not seeded** and are not oversights:
    :data:`UNSEEDED` records that form 176 was tested on gas distribution and
    rejected, and that water has no EIA source at all. **$29.4B stays open.**

    ⚠️ **The graded span is the one coal dominated.** Coal carries the result --
    dropping it takes the gain from +16.0% to -0.3% -- and coal fell from 51.7%
    of the fuel bill in 2012 to 16.6% in 2022, so the later years lean on the
    leg that is least like the span the holdout could see.
    """
    use = _use_2017_detail()
    index = relative_index(year, base_year)
    seed = use[list(ELECTRIC)].copy()
    touched = [code for code in index.index if code in seed.index]
    seed.loc[touched, :] = seed.loc[touched, :].mul(
        index.reindex(touched).to_numpy(), axis=0
    )
    return seed


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
            f'eia_{BASE}': _cell(costs, BASE, fuel),
            f'bea_{BASE}': float(early.loc[code]),
            f'eia_{TARGET}': _cell(costs, TARGET, fuel),
            f'bea_{TARGET}': float(late.loc[code]),
            'eia/bea': _cell(costs, TARGET, fuel) / float(late.loc[code]),
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


@functools.cache
def form_176() -> pd.DataFrame:
    """EIA 176 by year: citygate receipts, merchant sales volume and revenue.

    ❌ **The reason ``221200`` cannot be seeded, in one table.**  The
    ``citygate_receipts_sales_customers`` line falls 45% between 2012 and 2017
    and that is **not economics**: EIA split it in **2014**, when
    ``citygate_receipts_transportation_customers`` first appears.  ✅ The tell
    is the last column -- receipts over merchant sales volume runs **1.2-1.6
    through 2013 and 0.8 in every one of the eleven years after**, a step, not
    a trend.
    """
    supply = pd.read_parquet(
        _cached(GAS_SUPPLY),
        columns=['report_year', 'supply_type', 'volume_mcf'],
    )
    consumer = pd.read_parquet(
        _cached(GAS_BY_CONSUMER),
        columns=['report_year', 'revenue_class', 'revenue', 'volume_mcf'],
    )
    receipts = (
        supply[supply['supply_type'] == 'citygate_receipts_sales_customers']
        .groupby('report_year')['volume_mcf']
        .sum()
        / 1e6
    )
    transport_receipts = (
        supply[supply['supply_type'] == 'citygate_receipts_transportation_customers']
        .groupby('report_year')['volume_mcf']
        .sum()
        / 1e6
    )
    sales = consumer[consumer['revenue_class'] == 'sales']
    frame = pd.DataFrame(
        {
            'citygate_sales_bcf': receipts,
            'citygate_transport_bcf': transport_receipts,
            'merchant_sales_bcf': sales.groupby('report_year')['volume_mcf'].sum()
            / 1e6,
            'merchant_revenue_$M': sales.groupby('report_year')['revenue'].sum() / 1e3,
        }
    )
    frame['$/mcf'] = frame['merchant_revenue_$M'] / frame['merchant_sales_bcf']
    frame['receipts/sales'] = frame['citygate_sales_bcf'] / frame['merchant_sales_bcf']
    return frame


def universe() -> pd.DataFrame:
    """❌ **Form 176 is not a survey of the utilities sector** (Wes, 2026-08-25).

    Its ~2,000 respondents are interstate and intrastate **pipelines**, LNG and
    storage operators and direct-delivery **producers** as well as distribution
    utilities, and each files **one report per state**.  So:

    ⚠️ **78-81% of the volume is pipeline movement** -- to other pipelines, out
    of state, or to distribution companies -- against **17%** delivered to
    consumers, which is the only part ``221200`` describes.  Roughly 30
    respondents a year report producer lease use, which is ``211000``.

    ⚠️ **And the volumes are not additive**: total disposition runs about
    **5.7x** US gas consumption, because the same molecule is counted at each
    step of the chain.  ❌ An index built on the national sum mixes ``486000``,
    ``221200`` and ``211000`` and double counts the lot.

    ✅ **The respondent count is smooth across 2014** -- 2,007, 2,025, 2,027 --
    which is what rules out a universe change as the explanation for the break
    in :func:`form_176`, and leaves the form's own line split.
    """
    disposition = pd.read_parquet(
        _cached(GAS_DISPOSITION),
        columns=[
            'report_year',
            'operator_id_eia',
            'disposition_distribution_companies_mcf',
            'disposition_other_pipelines_mcf',
            'disposition_out_of_state_mcf',
            'producer_lease_use_mcf',
            'total_disposition_mcf',
        ],
    )
    consumer = pd.read_parquet(
        _cached(GAS_BY_CONSUMER),
        columns=['report_year', 'operator_id_eia', 'volume_mcf'],
    )
    grouped = disposition.groupby('report_year')
    frame = pd.DataFrame(
        {
            'to_pipelines_bcf': (
                grouped['disposition_distribution_companies_mcf'].sum()
                + grouped['disposition_other_pipelines_mcf'].sum()
                + grouped['disposition_out_of_state_mcf'].sum()
            )
            / 1e6,
            'to_consumers_bcf': consumer.groupby('report_year')['volume_mcf'].sum()
            / 1e6,
            'producer_lease_use_bcf': grouped['producer_lease_use_mcf'].sum() / 1e6,
            'total_disposition_bcf': grouped['total_disposition_mcf'].sum() / 1e6,
            'respondents': grouped['operator_id_eia'].nunique(),
        }
    )
    frame['pipeline_%'] = (
        100 * frame['to_pipelines_bcf'] / frame['total_disposition_bcf']
    )
    frame['consumer_%'] = (
        100 * frame['to_consumers_bcf'] / frame['total_disposition_bcf']
    )
    return frame


def gas_distribution() -> pd.DataFrame:
    """❌ **Can form 176 seed ``221200``?  No -- and the winning index is why.**

    Every candidate index EIA 176 offers, scored on the one column it would
    move.  ⚠️ **The only one that wins is ``citygate_receipts_sales_customers``,
    and its 2012 -> 2017 fall is the 2014 form split** (:func:`form_176`), not
    a shrinking merchant function.  ❌ Every concept that is continuous across
    the break -- merchant sales volume, merchant revenue, the implied price --
    **loses**, by 2 to 5%.

    ⚠️ **This is the mirror of the trade lesson**: there a wrong answer key made
    a sound source look useless; here a vintage break in the source makes a
    useless index look excellent.  ✅ A holdout gain is only as good as the
    source's continuity across the span.

    ❌ **And there is a second, structural reason** that would stand even
    without the break: **form 176 is not a survey of the utilities sector**.
    See :func:`universe` -- 78-81% of its volume is pipeline movement
    (``486000``), it double counts the chain about 5.7x, and only 17% is the
    delivery to consumers that ``221200`` describes.
    """
    from bedrock.analysis.nowcasting.benchmark_holdout import (  # noqa: PLC0415
        BASE,
        TARGET,
        block,
        holdout_score,
    )

    frame = form_176()
    ratio = {
        'citygate receipts, sales customers': _cell(frame, TARGET, 'citygate_sales_bcf')
        / _cell(frame, BASE, 'citygate_sales_bcf'),
        'merchant sales volume': _cell(frame, TARGET, 'merchant_sales_bcf')
        / _cell(frame, BASE, 'merchant_sales_bcf'),
        'merchant sales revenue': _cell(frame, TARGET, 'merchant_revenue_$M')
        / _cell(frame, BASE, 'merchant_revenue_$M'),
        'implied price per mcf': _cell(frame, TARGET, '$/mcf')
        / _cell(frame, BASE, '$/mcf'),
    }
    early, late = block(BASE)[GAS_DISTRIBUTION], block(TARGET)[GAS_DISTRIBUTION]
    observed = float(late.loc['211000'] / early.loc['211000'])

    records = []
    for label, value in ratio.items():
        index: 'pd.Series[float]' = pd.Series({'211000': value})

        def index_for(
            _column: str, moved: 'pd.Series[float]' = index
        ) -> 'pd.Series[float]':
            return moved

        row: dict[str, object] = {
            'index': label,
            'eia': value,
            'bea_observed': observed,
        }
        for weighting in ('dollar', 'impact'):
            scored = holdout_score(index_for, [GAS_DISTRIBUTION], weighting)
            row[f'{weighting}_gain_%'] = float(scored['gain_%'].iloc[0])
        row['continuous?'] = label != 'citygate receipts, sales customers'
        records.append(row)
    return pd.DataFrame(records).set_index('index')


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
    parser.add_argument('--gas', action='store_true', help='form 176 on 221200')
    parser.add_argument('--surge', action='store_true', help='what EIA says 2020-24')
    parser.add_argument('--all', action='store_true', help='every measurement')
    args = parser.parse_args()
    chosen = (
        args.holdout
        or args.carriers
        or args.legs
        or args.scope
        or args.gas
        or args.surge
    )
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
    if args.all or args.gas or not chosen:
        print('\nForm 176 on 221200 gas distribution: every index it offers\n')
        print(gas_distribution().round(3).to_string())
        print('\n  and why the one that wins does not count\n')
        print(form_176().loc[2010:2019].round(2).to_string())
        print('\n  whose gas form 176 is actually measuring\n')
        print(
            universe()
            .loc[
                2010:2019,
                [
                    'to_pipelines_bcf',
                    'to_consumers_bcf',
                    'respondents',
                    'pipeline_%',
                    'consumer_%',
                ],
            ]
            .round(1)
            .to_string()
        )
        print(
            '\n  the transportation-customers line appears in 2014 and the'
            '\n  receipts/sales ratio steps 1.4 -> 0.9 and stays at 0.8 for'
            '\n  eleven years. The fall is the form, not the merchant function,'
            '\n  which is flat: 6,959 -> 7,237 bcf and $57.0B -> $62.0B.'
        )
    if args.all or args.surge or not chosen:
        print('\nWhat EIA says about the years the seed is wanted for, $M\n')
        print(surge().round(1).to_string())


if __name__ == '__main__':
    main()
