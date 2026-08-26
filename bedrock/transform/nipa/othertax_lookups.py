"""Step 2's ``T00OTOP`` weight vector, 2017-2024: two blocks looked up, the rest frozen.

What this is
------------

``NIPA_VA_othertax_<year>`` takes a *level* from ``T30500`` and distributes it
across the 402 BEA detail industries.  Until this module the distribution was
the frozen 2017 benchmark share, whole.  It no longer is::

    weight_d(t) = benchmark_d(2017) x group_scale_g(t)

for three groups ``g`` -- housing, farm, and everything else -- where the first
two are **rescaled to a published annual NIPA line** and the third takes the
remainder.  Within a group the 2017 shares still carry the shape;
``proportional`` attribution normalises afterwards, so these are weights and
never levels and the ``T30500`` control holds by construction.

Why two blocks and not an allocator
-----------------------------------

:mod:`bedrock.analysis.nowcasting.other_taxes_allocation` measured every
plausible economy-wide allocator for this row against the published 2017 detail
and the best is 71% error -- ``T00OTOP`` is a property tax, so it tracks the
capital stock and nothing on the industry axis tracks that.  What it *is* is
concentrated, and BEA publishes the two biggest blocks outright:

============  ==========================  =============  ==================
block         NIPA line                   2017 share      what it lands on
============  ==========================  =============  ==================
housing       ``T70405`` ``B1031C``       41.8%           ``531HSO``/``531HST``
farm          ``T70305`` ``B1017C``        1.5%           the ten 111/112 codes
============  ==========================  =============  ==================

✅ **Both are exact rather than approximate.**  ``B1031C`` equals the published
``531HSO + 531HST`` pair to the dollar in 2017, and equals the summary SUT's
``HS`` row to the dollar in every year 2018-2024; ``B1017C`` does the same for
``111CA``.  So 43.3% of the row stops being a frozen share and becomes an
observation.

⚠️ **The gain is real and bounded, and it is worth stating as both.**  Against
the held-out summary SUT the row error falls from 1.01% to 0.81% in 2018 and
from 2.05% to 1.66% in 2023 -- about a fifth of the remaining error, not a
transformation of the method.  The larger error is in the *control*: the 2021
NIPA vintage sits 2.9% above that year's workbook.  The frozen block below is
still 56.7% of the row, and it is still defensible for the reason it always
was, which composition drift of 1.01-2.10% on the summary SUT licenses.

⚠️ **Why this was not in the original build, and what changed.**  It is not a
new idea -- ``other_taxes_allocation.lookup_improvement`` measured it and
``NIPA_VA_othertax_2017.yaml`` explained at length why it was not carried.  The
reason was mechanical, not substantive: the lookups cannot be their own
activity sets, because NIPA states no *other taxes on production excluding
housing and farm* line, so a third set's control would still be the whole
608,533 and the three would sum to 872,044.  Folding them into the weight
vector instead appeared to need an ``FBS_outside_flowsa`` attribution source,
which does not work (#731).

**That blocker was bypassed for ``V00100`` and the bypass applies here
unchanged.**  A weight vector that is a *rescaling of an FBA already in the
method* needs no new source -- it needs a ``clean_fba`` socket on the existing
``BEA_Detail_Use_SUT`` attribution source, which is what
:func:`~bedrock.transform.nipa.compensation_movement.apply_qcew_movement` is.
This module is the same shape.  ``other_taxes_allocation``'s own note said the
hatch was "worth clearing before Step 2's largest row rather than after"; it was
cleared there and this is the row that was waiting on it.

⚠️ **The housing pair is split on the frozen share, deliberately.**  ``B1031C``
is the owner-plus-tenant total and does not state the split.  The 2017 benchmark
share and the T7.4.5 gross-value-added share disagree by **6.8 percentage
points**, and it is the frozen one that reproduces the published 2017 pair -- so
the GVA split is rejected on measurement rather than left untried.  See
``other_taxes_allocation.housing_split``.

⚠️ **Government stays at zero and does not need a rule here.**  All ten
government codes carry exactly zero in the benchmark, and a group rescale
preserves a zero.  The accounting rule is enforced by the crosswalk; this module
cannot break it, and ``--check`` confirms rather than assumes that.

Run::

    uv run python -m bedrock.transform.nipa.othertax_lookups
    uv run python -m bedrock.transform.nipa.othertax_lookups --check
"""

from __future__ import annotations

import argparse
import functools
import sys

import pandas as pd

from bedrock.extract.iot.io_2017 import _load_2017_detail_supply_use_usa
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES

#: The benchmark the shares are carried from, and the year the socket is the
#: identity in -- 2017 is both anchor and target there.
BENCHMARK_YEAR = 2017

#: The Use SUT row this module supplies weights for.
ROW = 'T00OTOP'

#: The two ``T30500`` lines that are the row's control.  Kept here as well as in
#: ``other_taxes_allocation`` because the remainder is ``control - housing -
#: farm`` and this module cannot import from the analysis layer.
CONTROL_LINES = (('T30500', 'LA000365'), ('T30500', 'LA000237'))

#: Housing: table 7.4.5's taxes on production and imports, and the two detail
#: codes BEA books it to -- owner-occupied and tenant-occupied.
HOUSING_LINE = ('T70405', 'B1031C')
HOUSING_CODES = ('531HSO', '531HST')

#: Farm: table 7.3.5's taxes on production and imports.  Lands on the ten crop
#: and animal production codes.
FARM_LINE = ('T70305', 'B1017C')
FARM_PREFIXES = ('111', '112')

#: The FBA is in USD; the Use SUT workbook and everything here is in millions.
MILLION = 1e6

#: The lookups may not grow into the rest of the row.  They are 43.3% of it in
#: 2017 and the block they leave behind has to stay comfortably positive; a
#: remainder share below this means a NIPA line has changed meaning rather than
#: that property tax has concentrated.
MIN_REMAINDER_SHARE = 0.25


@functools.lru_cache(maxsize=1)
def benchmark_othertax() -> pd.Series:
    """Published 2017 detail ``T00OTOP`` by industry, million USD."""
    workbook = _load_2017_detail_supply_use_usa('Use_SUT_detail')
    row = workbook.loc[ROW]
    if isinstance(row, pd.DataFrame):
        raise ValueError(
            f'{ROW} matches {len(row)} rows of the 2017 detail Use SUT; expected 1'
        )
    values = (
        pd.to_numeric(row, errors='coerce')
        .reindex(list(USA_2017_INDUSTRY_CODES))
        .astype(float)
    )
    if values.isna().any():
        missing = list(values.index[values.isna()])
        raise KeyError(f'2017 detail Use SUT is missing industries {missing}')
    values.index.name = 'industry'
    return values


def farm_industries() -> list[str]:
    """The ten BEA detail crop and animal production codes."""
    return [str(i) for i in USA_2017_INDUSTRY_CODES if str(i).startswith(FARM_PREFIXES)]


@functools.lru_cache(maxsize=16)
def nipa_series(table: str, code: str, year: int) -> float:
    """One NIPA series, million USD, from the ``BEA_NIPA`` FBA for ``year``.

    ⚠️ **From the FBA rather than from the flat files directly**, for two
    reasons.  The transform layer does not import from the analysis layer, where
    ``nipa_flat_table`` lives; and reading the same FBA the method itself reads
    means a lookup and the control it is subtracted from cannot land on
    different BEA vintages.  Same approach as
    :func:`bedrock.extract.bea.BEA_NIPA.motor_vehicle_auto_share`.

    ⚠️ Selected by **code**, not by line number.  The yaml selects the control
    by line because ``selection_fields`` reads the FBA's ``Line``; here there is
    no such constraint, and code is the safer key (#536).
    """
    from bedrock.extract.flowbyactivity import getFlowByActivity  # noqa: PLC0415

    fba = getFlowByActivity('BEA_NIPA', int(year))
    rows = fba[fba['Description'].str.startswith(f'{table}:', na=False)]
    codes = rows['Description'].str.split(': ').str[1].str.split(' - ').str[0]
    totals = rows.assign(code=codes).groupby('code')['FlowAmount'].sum()
    if code not in totals.index:
        raise ValueError(
            f'BEA_NIPA {year} carries no {table} {code} row. {table} is listed '
            f'in BEA_NIPA.yaml; an FBA cached before it was added there will '
            f'not have it, and getFlowByActivity returns the newest local file '
            f'without checking the config it was built from. Regenerate with '
            f'generateFlowByActivity(source="BEA_NIPA", year="{year}").'
        )
    return float(totals[code]) / MILLION


def control(year: int) -> float:
    """The row's NIPA control: state and local plus federal, million USD."""
    return sum(nipa_series(table, code, year) for table, code in CONTROL_LINES)


def lookup_totals(year: int) -> dict[str, float]:
    """The three group totals the weights are rescaled to, million USD."""
    housing = nipa_series(*HOUSING_LINE, year)
    farm = nipa_series(*FARM_LINE, year)
    total = control(year)
    remainder = total - housing - farm
    if remainder / total < MIN_REMAINDER_SHARE:
        raise ValueError(
            f'{year}: housing {housing:,.0f} plus farm {farm:,.0f} is '
            f'{1 - remainder / total:.1%} of a {total:,.0f} control, leaving '
            f'less than {MIN_REMAINDER_SHARE:.0%} for the other 389 '
            f'industries. One of the three NIPA lines has changed meaning.'
        )
    return {'housing': housing, 'farm': farm, 'rest': remainder, 'control': total}


def group_of() -> pd.Series:
    """Each detail industry's group: ``housing``, ``farm`` or ``rest``."""
    groups = pd.Series('rest', index=list(USA_2017_INDUSTRY_CODES), dtype=object)
    groups[list(HOUSING_CODES)] = 'housing'
    groups[farm_industries()] = 'farm'
    groups.index.name = 'industry'
    return groups


def group_scales(year: int) -> pd.Series:
    """The factor each industry's benchmark weight is multiplied by.

    One value per group, broadcast to its members.  In the benchmark year all
    three are ~1.0 by construction, which is what makes the socket the identity
    there.
    """
    benchmark = benchmark_othertax()
    groups = group_of()
    targets = lookup_totals(year)
    observed = benchmark.groupby(groups).sum()
    scales = {}
    for group in ('housing', 'farm', 'rest'):
        base = float(observed.get(group, 0.0))
        if base <= 0:
            raise ValueError(
                f'the {group} block carries {base:,.0f} of benchmark {ROW}, so '
                f'it cannot be rescaled to {targets[group]:,.0f}'
            )
        scales[group] = targets[group] / base
    return groups.map(scales).astype(float)


def lookup_weights(year: int) -> pd.Series:
    """The weight vector for ``year``: benchmark shape, published block totals."""
    return benchmark_othertax() * group_scales(year)


def apply_published_lookups(fba: pd.DataFrame, **_kwargs: object) -> pd.DataFrame:
    """``clean_fba`` socket: rescale the benchmark ``T00OTOP`` weights.

    Wired onto the **attribution source** of ``NIPA_VA_othertax_<year>``, not
    onto the NIPA control.  The attribution source is the published 2017
    ``T00OTOP`` row of ``BEA_Detail_Use_SUT``; this multiplies each industry's
    row by its group's scale, so the vector handed to ``proportional`` puts the
    published annual housing and farm totals on those blocks and the remainder
    on the rest, each shaped by its own 2017 shares.

    ⚠️ **Weights, never levels.**  ``proportional`` normalises over the single
    attribution group afterwards, so the ``T30500`` control holds by
    construction and only the *relative* size of the three blocks matters here.

    ⚠️ Reads ``movement_year`` from the FBA's config, not ``year`` -- ``year`` on
    this source is the 2017 benchmark and must stay that way, since 2017 is the
    only year BEA publishes a detail Use SUT for.  Same contract as
    :func:`~bedrock.transform.nipa.compensation_movement.apply_qcew_movement`.

    ⚠️ Rows the mapping does not recognise -- the melted table's own ``T001``,
    ``T004``, ``T007`` and ``T019`` totals -- keep a scale of 1.0.  They are
    dropped by ``exclusion_fields`` regardless; scaling them would be harmless
    and not scaling them is one less thing to reason about.
    """
    config = getattr(fba, 'config', {}) or {}
    year = config.get('movement_year')
    if year is None:
        raise ValueError(
            'apply_published_lookups needs `movement_year` on the attribution '
            "source's config; `year` there is the 2017 benchmark and is not "
            'the year being nowcast'
        )
    year = int(year)
    if year == BENCHMARK_YEAR:
        return fba
    scales = group_scales(year)
    moved = fba.copy()
    scale = (
        moved['ActivityConsumedBy'].astype(str).map(scales).astype(float).fillna(1.0)
    )
    moved['FlowAmount'] = moved['FlowAmount'].astype(float) * scale
    return moved


def report(years: tuple[int, ...] = tuple(range(2017, 2025))) -> pd.DataFrame:
    """One row per year: the three block totals and what they do to the shares."""
    benchmark = benchmark_othertax()
    base_shares = benchmark / benchmark.sum()
    rows = []
    for year in years:
        targets = lookup_totals(year)
        weights = lookup_weights(year)
        shares = weights / weights.sum()
        rows.append(
            {
                'year': year,
                'control': targets['control'],
                'housing': targets['housing'],
                'farm': targets['farm'],
                'rest': targets['rest'],
                'housing_share': targets['housing'] / targets['control'],
                'drift_vs_frozen': float((shares - base_shares).abs().sum()),
            }
        )
    return pd.DataFrame(rows)


def check() -> int:
    """Assert what the socket rests on, against the real workbooks.

    Real-data assertions live behind a ``--check`` flag rather than in unit
    tests, per the convention the nowcasting analysis modules follow.
    """
    failures = []
    benchmark = benchmark_othertax()

    # The 2017 lookups have to equal the benchmark blocks, or the whole premise
    # -- "BEA publishes these two blocks outright" -- is not true.
    housing_2017 = nipa_series(*HOUSING_LINE, BENCHMARK_YEAR)
    farm_2017 = nipa_series(*FARM_LINE, BENCHMARK_YEAR)
    published_housing = float(benchmark[list(HOUSING_CODES)].sum())
    published_farm = float(benchmark[farm_industries()].sum())
    if abs(housing_2017 - published_housing) > 0.01 * published_housing:
        failures.append(
            f'housing: NIPA {housing_2017:,.0f} against a published '
            f'{published_housing:,.0f}; the lookup is no longer the pair'
        )
    if abs(farm_2017 - published_farm) > 0.01 * published_farm:
        failures.append(
            f'farm: NIPA {farm_2017:,.0f} against a published '
            f'{published_farm:,.0f}; the lookup is no longer the farm block'
        )

    # The socket is the identity in the benchmark year, which is what keeps
    # NIPA_VA_othertax_2017 reproducing the published row.
    scales_2017 = group_scales(BENCHMARK_YEAR)
    if float((scales_2017 - 1.0).abs().max()) > 0.01:
        failures.append(
            f'the 2017 group scales reach '
            f'{float((scales_2017 - 1.0).abs().max()):.3f} from 1.0; the '
            f'socket is not the identity in the benchmark year'
        )

    for year in range(2017, 2025):
        targets = lookup_totals(year)
        weights = lookup_weights(year)

        # Every block positive, and the whole vector summing to the control.
        if abs(float(weights.sum()) - targets['control']) > 1.0:
            failures.append(
                f'{year}: weights sum to {float(weights.sum()):,.0f} against a '
                f'control of {targets["control"]:,.0f}'
            )
        if (weights < 0).any():
            failures.append(f'{year}: {int((weights < 0).sum())} negative weights')

        # Government is zero by an accounting rule, and a rescale preserves it.
        government = [
            str(i) for i in USA_2017_INDUSTRY_CODES if str(i).startswith(('S00', 'G'))
        ]
        if float(weights[government].sum()) != 0.0:
            failures.append(
                f'{year}: government carries {float(weights[government].sum()):,.0f}'
            )

        # No industry may be deleted -- a zeroed block would silently drop 389
        # industries to fewer, the way a QCEW suppression does for V00100.
        if int((weights > 0).sum()) != int((benchmark > 0).sum()):
            failures.append(
                f'{year}: {int((weights > 0).sum())} industries populated '
                f"against the benchmark's {int((benchmark > 0).sum())}"
            )

    if failures:
        print(f'{len(failures)} finding(s) no longer hold:')
        for failure in failures:
            print(f'  - {failure}')
        return 1
    print(
        f'{ROW} lookups: housing and farm reproduce the benchmark blocks, the '
        f'socket is the identity in {BENCHMARK_YEAR}, and every year rescales '
        f'to its control with government at zero and no industry deleted.'
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--check',
        action='store_true',
        help='assert the findings rather than printing the report',
    )
    args = parser.parse_args()
    if args.check:
        return check()
    frame = report()
    with pd.option_context('display.float_format', lambda v: f'{v:,.4f}'):
        print(frame.to_string(index=False))
    return 0


if __name__ == '__main__':
    sys.exit(main())
