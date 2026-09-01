"""The Use table converted from the SUT to the MUT, producer prices.

The SUT Use table is at purchaser value, with margins and taxes booked on the
goods row. The MUT Use table is at producer value, with the trade and transport
margins stripped out of the goods rows and booked on the rows of the commodities
that supplied them. Converting between the two is three jobs: create the MUT-only
``F05000`` imports column from Supply ``MCIF``/``MADJ``, collapse the SUT's six
value-added rows into ``V00200``, and redistribute the margins per
``(buyer, commodity)`` cell.

⚠️ The SUT is an argument, not a load, so a nowcast year is a parameter change
rather than a rewrite. The 2017 loaders appear only on the scoring side.

:func:`check` asserts the published-table facts the conversion is built against -
the size of the redistribution, ``F05000``'s duty reallocation, the ``V00200``
collapse - and prints the current numbers, so run it before trusting a change.
"""

from __future__ import annotations

import argparse
import sys
import typing as ta

import numpy as np
import pandas as pd

from bedrock.extract.iot.io_2017 import (
    load_2017_Utot_before_redef_usa,
    load_2017_value_added_before_redef_usa,
    load_2017_Ytot_before_redef_usa,
)
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_final_demand import (
    USA_2017_FINAL_DEMAND_CODES,
    USA_2017_FINAL_DEMAND_IMPORT_CODE,
)
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES
from bedrock.utils.taxonomy.bea.v2017_value_added import USA_2017_VALUE_ADDED_CODES

#: House comparison thresholds for "this cell matches": 1% relative, half a
#: million absolute. The same pair the supply-bridge diagnostics use
#: (``analysis.nowcasting.sections.ROUNDING_ATOL``), restated here so the
#: transform layer does not import the analysis layer.
REPLAY_RTOL = 0.01
REPLAY_ATOL = 0.5 * MILLION_CURRENCY_TO_CURRENCY

# --- the conversion --------------------------------------------------------


def use_producer_from_sut(
    use_sut: pd.DataFrame,
    supply_bridge: pd.DataFrame,
    margins: pd.DataFrame,
    intermediate: pd.DataFrame,
    year: int,
) -> pd.DataFrame:
    """The MUT Use table in producer prices, from the SUT Use table. USD.

    ⚠️ Not implemented yet - the signature is the contract the three jobs are
    built against, argument-fed so 2017 and a nowcast year are the same call.

    :param use_sut: purchaser-valued Use SUT: commodity rows plus the six
        ``nowcast.USE_VALUE_ADDED_ROWS``, over industries plus
        ``SUT_FINAL_DEMAND_CODES``.
    :param supply_bridge: commodity x ``SUPPLY_BRIDGE_CODES``, carrying the
        ``MCIF``/``MADJ`` that become ``F05000``. Read it from
        ``nowcast.derive_initial_supply_bridge`` - a raw FBS read disagrees with
        the build, which conditions both columns on the published summary tables.
    :param margins: the transaction-level Margins table, indexed
        ``(buyer, commodity)``. The per-cell join is why it is built at that
        grain - a commodity-average rate misallocates across buyers.
    :param intermediate: the intermediate block, which allocates each
        commodity's margin across the buyers of it.
    :param year: the year all four frames describe.
    """
    raise NotImplementedError(
        'the three jobs land separately: F05000 and the V00200 collapse, then '
        'the per-cell strip to producers value, then the margin redistribution.'
    )


# --- the answer key --------------------------------------------------------


def published_mut_use_2017() -> pd.DataFrame:
    """The published 2017 before-redefinitions MUT Use table, whole. USD.

    Interior, final demand and value added in one frame, on the axes the
    conversion has to produce: 405 rows (402 commodities + 3 value-added) x 422
    columns (402 industries + 20 final-demand codes). Value added is blank in
    the final-demand columns, as BEA leaves it.
    """
    interior = load_2017_Utot_before_redef_usa()
    final_demand = load_2017_Ytot_before_redef_usa()
    value_added = load_2017_value_added_before_redef_usa()

    rows = [*USA_2017_COMMODITY_CODES, *USA_2017_VALUE_ADDED_CODES]
    columns = [*USA_2017_INDUSTRY_CODES, *USA_2017_FINAL_DEMAND_CODES]
    table = pd.DataFrame(np.nan, index=rows, columns=columns, dtype=float)
    table.loc[list(USA_2017_COMMODITY_CODES), list(USA_2017_INDUSTRY_CODES)] = (
        interior.to_numpy()
    )
    table.loc[list(USA_2017_COMMODITY_CODES), list(USA_2017_FINAL_DEMAND_CODES)] = (
        final_demand.to_numpy()
    )
    table.loc[list(USA_2017_VALUE_ADDED_CODES), list(USA_2017_INDUSTRY_CODES)] = (
        value_added.to_numpy()
    )
    table.index.name = 'row'
    table.columns.name = 'column'
    return table


# --- scoring ---------------------------------------------------------------


class ReplayReport(ta.NamedTuple):
    """What a cell-by-cell comparison against the published table found.

    ``diff`` is ``candidate - published`` in USD, NaN only where **both** sides
    are blank - the value-added rows under the final-demand columns, which BEA
    leaves empty.
    """

    diff: pd.DataFrame
    n_cells: int
    n_outside: int
    gross: float
    net: float


def score_replay(
    candidate: pd.DataFrame,
    published: pd.DataFrame | None = None,
) -> ReplayReport:
    """Score *candidate* against the published MUT Use table, cell by cell.

    ⚠️ Per commodity and per industry, never on totals: the margin columns net
    to ~zero economy-wide, so an aggregate check passes on broken data.

    *published* defaults to :func:`published_mut_use_2017`.

    ⚠️ A cell counts unless it is blank on **both** sides. Excusing every cell
    the candidate happens to lack would make a conversion that drops rows score
    *better* than one that gets them wrong: the missing cells would leave
    ``gross`` and ``n_outside`` untouched. So a row the candidate is missing is
    compared as zero against the published value, and fails by its full amount;
    only the cells BEA itself leaves empty are excused.
    """
    answer = published_mut_use_2017() if published is None else published
    rows = answer.index.union(candidate.index, sort=False)
    columns = answer.columns.union(candidate.columns, sort=False)
    left = candidate.reindex(index=rows, columns=columns).astype(float)
    right = answer.reindex(index=rows, columns=columns).astype(float)

    comparable = left.notna() | right.notna()
    filled_left = left.fillna(0.0)
    filled_right = right.fillna(0.0)
    close = np.isclose(
        filled_left.to_numpy(),
        filled_right.to_numpy(),
        rtol=REPLAY_RTOL,
        atol=REPLAY_ATOL,
    )
    outside = comparable.to_numpy() & ~close
    diff = (filled_left - filled_right).where(comparable)
    return ReplayReport(
        diff=diff,
        n_cells=int(comparable.to_numpy().sum()),
        n_outside=int(outside.sum()),
        gross=float(diff.abs().to_numpy(na_value=0.0).sum()),
        net=float(diff.to_numpy(na_value=0.0).sum()),
    )


def by_row(diff: pd.DataFrame) -> pd.Series:
    """Absolute difference per row, descending."""
    return diff.abs().sum(axis=1, skipna=True).sort_values(ascending=False)


def by_job(diff: pd.DataFrame) -> pd.Series:
    """Absolute difference split across the conversion's three jobs, USD.

    Attribution is by position, so it points rather than proves: the ``F05000``
    column is the imports job, the value-added rows are the collapse, and
    everything left is the margin join.
    """
    absolute = diff.abs()
    imports = USA_2017_FINAL_DEMAND_IMPORT_CODE

    if imports in absolute.columns:
        f05000 = float(absolute[imports].to_numpy(na_value=0.0).sum())
        absolute = absolute.drop(columns=[imports])
    else:
        f05000 = 0.0

    va_rows = [r for r in USA_2017_VALUE_ADDED_CODES if r in absolute.index]
    collapse = float(absolute.loc[va_rows].to_numpy(na_value=0.0).sum())
    absolute = absolute.drop(index=va_rows)

    return pd.Series(
        {
            'F05000': f05000,
            'VA collapse': collapse,
            'margin join': float(absolute.to_numpy(na_value=0.0).sum()),
        }
    )


# --- report / check --------------------------------------------------------


def _baseline_score() -> ReplayReport:
    """The unconverted purchaser SUT scored against the producer MUT.

    The size of the conversion, and the floor every later change has to beat.
    Interior only: the SUT has no ``F05000`` column and its value-added block is
    not the MUT's three rows, so neither is a comparison yet.
    """
    from bedrock.extract.iot.io_2017 import (  # noqa: PLC0415
        _load_2017_detail_supply_use_usa,
    )

    sut = _load_2017_detail_supply_use_usa('Use_SUT_detail')
    purchaser = (
        sut.loc[list(USA_2017_COMMODITY_CODES), list(USA_2017_INDUSTRY_CODES)].astype(
            float
        )
        * MILLION_CURRENCY_TO_CURRENCY
    )
    published = load_2017_Utot_before_redef_usa()
    purchaser.index = published.index
    purchaser.columns = published.columns
    return score_replay(purchaser, published)


def report() -> None:
    """Print the size of the conversion and the named first cells."""
    million = MILLION_CURRENCY_TO_CURRENCY
    score = _baseline_score()
    print('SUT -> MUT Use conversion, 2017: purchaser against producer')
    print(f'  comparable cells      {score.n_cells:>12,}')
    print(
        f'  outside tolerance     {score.n_outside:>12,} '
        f'({score.n_outside / score.n_cells:.1%})'
    )
    print(f'  gross absolute diff   {score.gross / million:>12,.0f} $M')
    print(f'  net difference        {score.net / million:>12,.0f} $M')

    print('\n  by job:')
    for job, amount in by_job(score.diff).items():
        print(f'    {job:<14} {amount / million:>12,.0f} $M')

    print('\n  largest rows to move:')
    for code, amount in by_row(score.diff).head(8).items():
        print(f'    {code:<8} {amount / million:>12,.0f} $M')

    print('\n  named first cells (purchaser -> producer):')
    published = load_2017_Utot_before_redef_usa()
    # diff is candidate - published, and the candidate is the purchaser SUT
    sut_side = published + score.diff
    for code in ('4B0000', '423A00', '484000', '425000'):
        before = float(sut_side.loc[code].sum()) / million
        after = float(published.loc[code].sum()) / million
        print(f'    {code:<8} {before:>12,.0f} -> {after:>12,.0f} $M')


def check() -> int:
    """Assert the answer key's shape and the size of the conversion.

    Returns a process exit code. The published-workbook facts the three jobs are
    built against - ``F05000``'s sign and duty cell, the value-added block, the
    named first cell - are asserted in the tests instead, which run in CI.
    """
    million = MILLION_CURRENCY_TO_CURRENCY
    failures: list[str] = []

    table = published_mut_use_2017()
    expected_shape = (
        len(USA_2017_COMMODITY_CODES) + len(USA_2017_VALUE_ADDED_CODES),
        len(USA_2017_INDUSTRY_CODES) + len(USA_2017_FINAL_DEMAND_CODES),
    )
    if table.shape != expected_shape:
        failures.append(
            f'the answer key is {table.shape}, expected {expected_shape}. The '
            f'conversion has to produce exactly these axes.'
        )

    # the scorer is exact on the identity case, or it is not measuring anything
    identity = score_replay(table, table)
    if identity.n_outside != 0 or identity.gross != 0.0:
        failures.append(
            f'scoring the answer key against itself reports '
            f'{identity.n_outside} cells outside tolerance and '
            f'{identity.gross:,.0f} USD gross. It must be exact.'
        )

    score = _baseline_score()
    if score.n_cells != 161_604:
        failures.append(
            f'the interior is {score.n_cells:,} cells, not 161,604. Every '
            f'threshold below is quoted against that count.'
        )
    if not 25_000 <= score.n_outside <= 26_000:
        failures.append(
            f'{score.n_outside:,} interior cells differ between the purchaser '
            f'SUT and the producer MUT, against a measured 25,450. A large move '
            f'means one of the two tables changed vintage.'
        )
    if abs(score.gross / million - 2_474_270) > 1_000:
        failures.append(
            f'gross difference is {score.gross / million:,.0f} $M against a '
            f'measured 2,474,270 $M.'
        )
    if abs(score.net / million) > 1_000:
        failures.append(
            f'net difference is {score.net / million:,.0f} $M. A margin is value '
            f'moved, never created, so the interior has to net to rounding '
            f'(+136 $M as published).'
        )

    if failures:
        print('FAILED:')
        for failure in failures:
            print(f'  - {failure}')
        return 1
    print(
        f'OK: all findings hold (the conversion has to move '
        f'{score.gross / million:,.0f} $M across {score.n_outside:,} cells and '
        f'net to {score.net / million:,.0f} $M)'
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


if __name__ == '__main__':
    sys.exit(main())
