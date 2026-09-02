"""The import matrix, allocated along the producer-price Use row.

Each commodity's imports are spread across the columns that buy it, in
proportion to that commodity's own use shares along its row::

    Uimp[c, j] = imports[c] x Use[c, j] / sum_j Use[c, j]

Two things decide whether it is right, and both are silent when wrong: the Use
row must be the **producer-price** one, and the spread must be restricted to
:data:`ALLOCATION_COLUMNS`. Run :func:`check` before trusting a change.
"""

from __future__ import annotations

import argparse
import sys

import numpy as np
import pandas as pd

from bedrock.extract.iot.io_2017 import (
    _load_benchmark_detail_import_before_redef_usa,
)
from bedrock.transform.iot.sut_use_to_mut_use import (
    REPLAY_ATOL,
    ReplayReport,
    published_mut_use,
    score_replay,
)
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.taxonomy.bea.matrix_mappings import (
    USA_BENCHMARK_DETAIL_SUT_YEARS,
)
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_final_demand import (
    USA_2017_FINAL_DEMAND_CODES,
    USA_2017_FINAL_DEMAND_IMPORT_CODE,
)
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES

# --- the allocation --------------------------------------------------------


def import_matrix_from_use(
    use_producer: pd.DataFrame,
    imports: pd.Series,
) -> pd.DataFrame:
    """The import matrix, commodity x (industry + final demand). USD.

    402 x 422, the axes :func:`published_import_matrix` returns. *imports* is
    each commodity's total, **positive**; it lands negative in ``F05000`` as BEA
    publishes it. :data:`ZERO_IMPORT_FINAL_DEMAND_CODES` are written as explicit
    zeros, because BEA measured them at zero rather than leaving them out.
    """
    commodities = list(USA_2017_COMMODITY_CODES)
    weights = allocation_weights(use_producer)
    control = imports.reindex(commodities).astype(float)

    negative = control[control < -NEGATIVE_CONTROL_TOLERANCE]
    assert negative.empty, (
        f'the import control is negative on {sorted(negative.index)}. A gross '
        f'import total is nonnegative by construction, so one this far below '
        f'zero is a sign convention that broke upstream - see '
        f'NEGATIVE_CONTROL_TOLERANCE.'
    )

    table = pd.DataFrame(
        0.0,
        index=commodities,
        columns=[*USA_2017_INDUSTRY_CODES, *USA_2017_FINAL_DEMAND_CODES],
        dtype=float,
    )
    table.loc[commodities, list(ALLOCATION_COLUMNS)] = weights.mul(
        control, axis=0
    ).to_numpy()
    table.loc[commodities, USA_2017_FINAL_DEMAND_IMPORT_CODE] = -control.to_numpy()
    table.index.name = 'row'
    table.columns.name = 'column'
    return table


def allocation_weights(use_producer: pd.DataFrame) -> pd.DataFrame:
    """Each commodity's use shares along its row, over :data:`ALLOCATION_COLUMNS`.

    Rows sum to 1, or to 0 on :data:`DEAD_ROW_COMMODITIES`.

    ⚠️ **Negative cells are clipped before normalising** - 82 cells and
    -390,320 $M at 2017, on the trade and transport rows that give margin up.
    A negative weight would book negative imports and shrink the denominator.
    """
    columns = list(ALLOCATION_COLUMNS)
    missing = [c for c in columns if c not in use_producer.columns]
    assert not missing, (
        f'the producer Use table is missing {len(missing)} allocation columns '
        f'({missing[:5]}...). The scope is fixed by what BEA populates, so a '
        f'missing column is a truncated input rather than a narrower spread.'
    )

    row = use_producer.reindex(index=list(USA_2017_COMMODITY_CODES))[columns]
    positive = row.astype(float).clip(lower=0.0)
    total = positive.sum(axis=1)
    return positive.div(total.where(total > 0), axis=0).fillna(0.0)


def import_control(supply_bridge: pd.DataFrame) -> pd.Series:
    """Each commodity's total imports for the matrix, positive, USD.

    ``MCIF + MDTY`` - the import matrix is valued at the **domestic port**:
    c.i.f. (which already carries each commodity's own freight and insurance
    charges) plus customs duties, duty left on the commodity that bore it.
    This reproduces the published matrix's own import column **exactly**:
    47 $M gross at 2017 and 52 $M at 2012 over 402 commodities, zero
    commodities off by more than $10M - whole-million rounding.

    ⚠️ **MADJ does not belong here.** The bridge's c.i.f./f.o.b. adjustment
    exists so *total supply* nets out the freight double-count; it sits on six
    service commodities (water, truck, couriers, air, insurance, rail) whose
    rows the import matrix keeps at their **gross** import values. Including
    it - the previous rule - missed by exactly ``-MADJ`` on exactly those six
    (23,163 $M at 2017). Their difference from the Use table's ``F05000`` is
    useeior's ``InternationalTradeAdjustment``: ``MADJ`` on the six plus the
    ``4200ID`` duty credit.

    ⚠️ **Not** ``sut_use_to_mut_use.f05000_column``, which credits the duty
    total back onto ``4200ID``: the import matrix has no such row.
    """
    missing = [c for c in ('MCIF', 'MDTY') if c not in supply_bridge.columns]
    assert not missing, f'the supply bridge is missing {missing}'

    return (
        (supply_bridge['MCIF'].astype(float) + supply_bridge['MDTY'].astype(float))
        .reindex(list(USA_2017_COMMODITY_CODES))
        .fillna(0.0)
        .rename('imports')
    )


#: The twelve final-demand columns BEA populates. The same twelve at 2007, 2012
#: and 2017.
IMPORT_FINAL_DEMAND_CODES: tuple[str, ...] = (
    'F01000',
    'F02E00',
    'F02N00',
    'F02R00',
    'F02S00',
    'F03000',
    'F06E00',
    'F06N00',
    'F07E00',
    'F07N00',
    'F10E00',
    'F10N00',
)

#: The seven BEA leaves **identically zero** in every vintage: exports, and the
#: government consumption and structures columns. A naive spread that ignores
#: them misplaces 291,890 $M -- 10.9% of imports -- with the row totals still
#: balancing. Government probably lands in the industry columns because these
#: accounts carry it as an industry.
ZERO_IMPORT_FINAL_DEMAND_CODES: tuple[str, ...] = (
    'F04000',
    'F06C00',
    'F06S00',
    'F07C00',
    'F07S00',
    'F10C00',
    'F10S00',
)

#: Where imports may land. ``F05000`` is excluded because it *is* the import
#: total, so counting it would double-count every share.
ALLOCATION_COLUMNS: tuple[str, ...] = (
    *USA_2017_INDUSTRY_CODES,
    *IMPORT_FINAL_DEMAND_CODES,
)

#: How negative a control may go before it reads as a broken sign convention.
#: ⚠️ A negative total is rare but real: at 2017 BEA books ``S00900`` at -26 $M
#: of PCE imports. Nothing else in any vintage goes negative.
NEGATIVE_CONTROL_TOLERANCE: float = 100 * MILLION_CURRENCY_TO_CURRENCY

#: The benchmark years the workbook publishes, all on the 2017 code basis.
PUBLISHED_YEARS: tuple[USA_BENCHMARK_DETAIL_SUT_YEARS, ...] = (2007, 2012, 2017)

#: The years scored. 2007 loads but its producer-price Use answer key is
#: unverified.
REPLAY_YEARS: tuple[USA_BENCHMARK_DETAIL_SUT_YEARS, ...] = (2012, 2017)

#: Per year: cells outside (1%, 0.5 $M), and gross absolute difference in $M.
#: ⚠️ **The ceiling of the proportionality assumption on published inputs**, not
#: a target to beat - a score far below means the answer key leaked in.
REPLAY_EXPECTATIONS: tuple[tuple[USA_BENCHMARK_DETAIL_SUT_YEARS, int, int], ...] = (
    (2012, 15_021, 638_578),
    (2017, 15_675, 630_814),
)

#: No positive producer-price use to spread imports over, so an all-zero weight
#: row. The published matrix books 26 $M across all seven at 2017 and nothing at
#: 2012, so the dead row costs nothing measurable.
DEAD_ROW_COMMODITIES: tuple[str, ...] = (
    '4200ID',
    'GSLGE',
    'GSLGH',
    'GSLGO',
    'S00500',
    'S00600',
    'S00900',
)


# --- the answer key --------------------------------------------------------


def published_import_matrix(
    year: USA_BENCHMARK_DETAIL_SUT_YEARS = 2017,
) -> pd.DataFrame:
    """The published before-redefinitions import matrix, whole. USD.

    402 commodity rows x 422 columns, the axes the allocation has to produce.
    The workbook publishes 2007, 2012 and 2017 on one code basis, so the
    allocation replays on more than one benchmark year without a crosswalk.
    """
    raw = _load_benchmark_detail_import_before_redef_usa(year)
    commodities = list(USA_2017_COMMODITY_CODES)
    columns = [*USA_2017_INDUSTRY_CODES, *USA_2017_FINAL_DEMAND_CODES]
    table = raw.loc[commodities, columns].astype(float) * MILLION_CURRENCY_TO_CURRENCY
    table.index = pd.Index(commodities, name='row')
    table.columns = pd.Index(columns, name='column')
    return table


def published_import_control(
    year: USA_BENCHMARK_DETAIL_SUT_YEARS = 2017,
) -> pd.Series:
    """The published matrix's own import total per commodity, positive, USD.

    ``-F05000``, which is its row total over :data:`ALLOCATION_COLUMNS` to
    1,066 $M over 402 commodities at 2017 - whole-million rounding. So it is
    what the published interior was spread from, and what to score against.
    """
    column = published_import_matrix(year)[USA_2017_FINAL_DEMAND_IMPORT_CODE]
    return (-column).rename('imports')


# --- scoring ---------------------------------------------------------------


def proportionality_strain(
    diff: pd.DataFrame, published: pd.DataFrame | None = None
) -> pd.DataFrame:
    """Per commodity, how far the spread lands from the published row. USD.

    ⚠️ **A signal, not a defect list.** Proportionality's known weakness is
    commodities whose import mix genuinely differs by buyer, and those are the
    rows at the top here. Ranked worst first.
    """
    answer = published_import_matrix() if published is None else published
    gross = diff.abs().sum(axis=1, skipna=True)
    scale = answer.reindex(index=gross.index).abs().sum(axis=1, skipna=True)
    return pd.DataFrame(
        {
            'gross': gross,
            'published': scale,
            'share': gross / scale.replace(0.0, np.nan),
        }
    ).sort_values('gross', ascending=False)


def summary_divergence(candidate: pd.DataFrame, year: int) -> pd.DataFrame:
    """The detail matrix aggregated to summary, against BEA's summary. USD.

    BEA publishes the summary import matrix through 2024, so this is the only
    validation axis left after 2017. Reads the workbook rather than
    ``load_summary_Uimp_usa`` / ``load_summary_Yimp_usa``, which drop ``Used``
    and ``Other`` and with them 125bn of false divergence.
    """
    from bedrock.extract.iot.io_2017 import _load_usa_summary_mut  # noqa: PLC0415
    from bedrock.utils.taxonomy.bea.v2017_commodity_summary import (  # noqa: PLC0415
        USA_2017_SUMMARY_COMMODITY_CODES,
    )
    from bedrock.utils.taxonomy.bea.v2017_industry_summary import (  # noqa: PLC0415
        USA_2017_SUMMARY_INDUSTRY_CODES,
    )

    columns = [
        *USA_2017_SUMMARY_INDUSTRY_CODES,
        *_SUMMARY_IMPORT_FINAL_DEMAND_CODES,
    ]
    published = (
        _load_usa_summary_mut('Import_summary', year)  # type: ignore[arg-type]
        .loc[list(USA_2017_SUMMARY_COMMODITY_CODES), columns]
        .astype(float)
        .sum(axis=1)
        * MILLION_CURRENCY_TO_CURRENCY
    )

    groups = _commodity_to_summary().reindex(candidate.index)
    ours = (
        candidate[list(ALLOCATION_COLUMNS)]
        .sum(axis=1)
        .groupby(groups)
        .sum()
        .reindex(published.index)
        .fillna(0.0)
    )

    difference = ours - published
    out = pd.DataFrame(
        {
            'ours': ours,
            'published': published,
            'difference': difference,
            'share': difference / published.replace(0.0, np.nan),
        }
    ).sort_values('difference', key=abs, ascending=False)
    out.index.name = 'summary_commodity'
    return out


# --- report / check --------------------------------------------------------


def report() -> None:
    """Score the allocation at both replayable benchmark years."""
    million = MILLION_CURRENCY_TO_CURRENCY

    print('producer Use row -> import matrix')
    for year in REPLAY_YEARS:
        score = _allocated_score(year)
        allocated = float(
            published_import_matrix(year)[list(ALLOCATION_COLUMNS)]
            .abs()
            .to_numpy(na_value=0.0)
            .sum()
        )
        print(f'\n  {year}')
        print(f'    comparable cells      {score.n_cells:>12,}')
        print(f'    outside tolerance     {score.n_outside:>12,}')
        print(f'    gross absolute diff   {score.gross / million:>12,.0f} $M')
        print(f'    published gross       {allocated / million:>12,.0f} $M')
        print(f'    gross / published     {score.gross / allocated:>12.3f}')

    score = _allocated_score(2017)
    strain = proportionality_strain(score.diff, published_import_matrix())
    print('\n  where proportionality strains most, 2017 ($M):')
    for commodity, row in strain.head(10).iterrows():
        print(
            f'    {commodity:<8} {row["gross"] / million:>10,.0f}  of '
            f'{row["published"] / million:>10,.0f} published'
        )

    residual = _control_residual(2017)
    outside = int((residual.abs() > REPLAY_ATOL).sum())
    print(
        f'\n  MCIF + MDTY against the published F05000: '
        f'{residual.abs().sum() / million:,.0f} $M gross, {outside} commodities '
        f'outside tolerance'
    )
    print(
        f'  a naive spread over every non-F05000 column would misplace '
        f'{_naive_misplacement(2017) / million:,.0f} $M into the seven BEA-zero '
        f'columns'
    )


def check() -> int:
    """Assert the published-table facts the allocation is built on. Exit code."""
    million = MILLION_CURRENCY_TO_CURRENCY
    failures: list[str] = []

    for year in PUBLISHED_YEARS:
        zero = published_import_matrix(year)[list(ZERO_IMPORT_FINAL_DEMAND_CODES)]
        mass = float(zero.abs().to_numpy().sum()) / million
        if mass != 0.0:
            failures.append(
                f'{year} puts {mass:,.0f} $M in the seven columns BEA leaves '
                f'identically zero; the allocation scope is built on those '
                f'being empty in every vintage.'
            )

    misplaced = _naive_misplacement(2017) / million
    if abs(misplaced - 291_890) > 1_000:
        failures.append(
            f'a naive all-column spread now misplaces {misplaced:,.0f} $M '
            f'against a measured 291,890 $M, so an input changed vintage.'
        )

    gross = float(_control_residual(2017).abs().sum()) / million
    if abs(gross - 23_163) > 500:
        failures.append(
            f'MCIF + MADJ + MDTY is {gross:,.0f} $M from the published F05000 '
            f'against a measured 23,163 $M.'
        )

    for year, expected_outside, expected_gross in REPLAY_EXPECTATIONS:
        score = _allocated_score(year)
        if abs(score.n_outside - expected_outside) > 200:
            failures.append(
                f'{year} replays {score.n_outside:,} cells outside tolerance '
                f'against a measured {expected_outside:,}.'
            )
        if abs(score.gross / million - expected_gross) > 2_000:
            failures.append(
                f'{year} replays {score.gross / million:,.0f} $M gross against a '
                f'measured {expected_gross:,} $M - the ceiling of the '
                f'proportionality assumption on published inputs, so a score '
                f'far below it means the answer key leaked into the candidate.'
            )

    if failures:
        print('FAILED:')
        for failure in failures:
            print(f'  - {failure}')
        return 1
    score = _allocated_score(2017)
    print(
        f'OK: all findings hold (proportionality lands '
        f'{score.gross / million:,.0f} $M from the published 2017 matrix across '
        f'{score.n_outside:,} cells, and that is its ceiling)'
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--check',
        action='store_true',
        help='assert the published-table facts instead of printing the report',
    )
    args = parser.parse_args()
    if args.check:
        return check()
    report()
    return 0


#: BEA's summary final-demand codes are the detail codes truncated to four.
_SUMMARY_IMPORT_FINAL_DEMAND_CODES: tuple[str, ...] = tuple(
    code[:4] for code in IMPORT_FINAL_DEMAND_CODES
)


def _commodity_to_summary() -> pd.Series:
    from bedrock.utils.taxonomy.mappings.bea_v2017_commodity__bea_v2017_summary import (  # noqa: PLC0415
        load_bea_v2017_commodity_to_bea_v2017_summary,
    )

    mapping = load_bea_v2017_commodity_to_bea_v2017_summary()
    return pd.Series({code: parents[0] for code, parents in mapping.items()})


def _allocated(year: USA_BENCHMARK_DETAIL_SUT_YEARS) -> pd.DataFrame:
    """Allocate at a benchmark year off published inputs.

    Published Use row and published control, so the score measures the
    proportionality assumption alone.
    """
    return import_matrix_from_use(
        published_mut_use(year), published_import_control(year)
    )


def _allocated_score(year: USA_BENCHMARK_DETAIL_SUT_YEARS) -> ReplayReport:
    return score_replay(_allocated(year), published_import_matrix(year))


def _control_residual(year: USA_BENCHMARK_DETAIL_SUT_YEARS) -> pd.Series:
    """``MCIF + MDTY`` less the published matrix's import total, USD."""
    from bedrock.extract.iot.io_2017 import (  # noqa: PLC0415
        _load_benchmark_detail_supply_use_usa,
    )

    supply = _load_benchmark_detail_supply_use_usa('Supply_detail', year).rename(
        columns=lambda column: str(column).strip()
    )
    bridge = (
        supply.loc[list(USA_2017_COMMODITY_CODES), ['MCIF', 'MDTY']].astype(float)
        * MILLION_CURRENCY_TO_CURRENCY
    )
    return import_control(bridge) - published_import_control(year)


def _naive_misplacement(year: USA_BENCHMARK_DETAIL_SUT_YEARS) -> float:
    """What a spread over every non-``F05000`` column puts in BEA's zero columns.

    The counterfactual the scope restriction exists to avoid, in USD.
    """
    columns = [
        c
        for c in [*USA_2017_INDUSTRY_CODES, *USA_2017_FINAL_DEMAND_CODES]
        if c != USA_2017_FINAL_DEMAND_IMPORT_CODE
    ]
    row = published_mut_use(year).reindex(index=list(USA_2017_COMMODITY_CODES))[columns]
    positive = row.astype(float).clip(lower=0.0)
    total = positive.sum(axis=1)
    weights = positive.div(total.where(total > 0), axis=0).fillna(0.0)
    spread = weights.mul(published_import_control(year), axis=0)
    return float(spread[list(ZERO_IMPORT_FINAL_DEMAND_CODES)].to_numpy().sum())


if __name__ == '__main__':
    sys.exit(main())
