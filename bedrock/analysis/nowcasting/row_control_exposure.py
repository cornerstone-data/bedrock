"""What do the imports and exports estimates cost the Use table's interior?

Step 3's commodity row is not estimated -- it is a residual::

    T001[c]  =  T016[c]  -  sum_FD Y[c, ...]        (total supply less final uses)

and Step 5 imposes it hard as ``T016 = T019`` (T11).  So every dollar of error in
the two least settled terms of that identity lands somewhere in the intermediate
block.  The two are:

``MCIF``
    imports at CIF value, the Supply bridge column that carries the largest share
    of ``T016`` after domestic output.  Step 4b.

``F04000``
    exports, a final-demand column -- commodity output that leaves the country
    and so is *not* available for domestic industry use.  Step 1d.

Both come from the same Census + BEA trade extract, and both currently show
``PARTIAL`` / ``MISS`` / ``EXTRA`` cells against the published 2017 detail SUT.

The metric
----------

For each commodity, the signed error in the row residual::

    dT001[c]  =  err_MCIF[c]  -  err_F04000[c]

then weighted by how much of that commodity actually goes to industry, because an
error on a commodity that is 95% final demand mostly lands in final demand::

    exposure[c]      =  |dT001[c]| * iota[c],   iota[c] = T001[c] / T019[c]
    exposure_pct[c]  =  |dT001[c]| / T019[c]

``iota`` is the published 2017 split, used as the allocation the balance will
approximately reproduce: the row identity is hard, the FD columns are softly
targeted and the intermediate row carries no target of its own, so a row-total
error distributes roughly in proportion to the row's existing shape.

⚠️ **This measures 2017, where the answer is published.**  It is a diagnostic on
the *estimates*, not on the nowcast years -- but the extract is the same extract,
so a commodity that misses here is a commodity to distrust in 2018-2025.

⚠️ **The exports candidate is the pinned CSV export by default**, the same
fallback :mod:`~.sections` uses, because a live ``derive_initial_Y_pur`` run is
slow.  ``--live`` runs the FBS instead.

Run::

    uv run python -m bedrock.analysis.nowcasting.row_control_exposure
    uv run python -m bedrock.analysis.nowcasting.row_control_exposure --live --top 40
"""

from __future__ import annotations

import argparse

import numpy as np
import pandas as pd

from bedrock.analysis.nowcasting.sections import (
    SUPPLY_BRIDGE_DETAIL_SUT,
    USE_FD_DETAIL_SUT,
    initial_Y_pur_exported_candidate,
    use_sut_final_demand_reference,
)
from bedrock.analysis.nowcasting.table_match import compare_tables
from bedrock.extract.iot.io_2017 import _load_2017_detail_supply_use_usa
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES

#: :class:`~.table_match.CellStatus` values, by name.
STATUS = {0: 'absent', 1: 'match', 2: 'partial', 3: 'miss', 4: 'extra'}

#: The Supply column and the final-demand column the row residual depends on.
IMPORTS_CODE = 'MCIF'
EXPORTS_CODE = 'F04000'


def _exports_match(live: bool) -> object:
    """Compare our ``F04000`` against the published one, live FBS or pinned CSV."""
    if live:
        return USE_FD_DETAIL_SUT.run(2017)
    return compare_tables(
        initial_Y_pur_exported_candidate(2017),
        use_sut_final_demand_reference(2017),
        tolerance=USE_FD_DETAIL_SUT.tolerance,
        rows=pd.Index(USE_FD_DETAIL_SUT.rows, name='commodity'),
        columns=pd.Index(USE_FD_DETAIL_SUT.columns, name='final_demand_code'),
    )


def exposure(live: bool = False) -> pd.DataFrame:
    """Per-commodity error in the intermediate row control, and what it costs.

    Amounts are millions of dollars, as BEA publishes them.
    """
    commodities = list(USA_2017_COMMODITY_CODES)
    supply = SUPPLY_BRIDGE_DETAIL_SUT.run(2017)
    exports = _exports_match(live)

    use = _load_2017_detail_supply_use_usa('Use_SUT_detail')
    description = use['Commodity Description']
    intermediate = use.loc[commodities, 'T001'].astype(float)
    total_use = use.loc[commodities, 'T019'].astype(float)

    def column(match: object, code: str) -> tuple[pd.Series, pd.Series, pd.Series]:
        candidate = match.candidate[code].reindex(commodities)  # type: ignore[attr-defined]
        reference = match.reference[code].reindex(commodities)  # type: ignore[attr-defined]
        status = match.status[code].reindex(commodities).map(STATUS)  # type: ignore[attr-defined]
        scale = MILLION_CURRENCY_TO_CURRENCY
        return candidate / scale, reference / scale, status

    imports_candidate, imports_reference, imports_status = column(supply, IMPORTS_CODE)
    exports_candidate, exports_reference, exports_status = column(exports, EXPORTS_CODE)

    # A candidate that is NaN produced nothing at all, which is an error of the
    # full published amount -- not a missing observation to be skipped.
    imports_error = (
        (imports_candidate - imports_reference).fillna(-imports_reference).fillna(0.0)
    )
    exports_error = (
        (exports_candidate - exports_reference).fillna(-exports_reference).fillna(0.0)
    )

    iota = (intermediate / total_use.replace(0, np.nan)).fillna(0.0)
    residual_error = imports_error - exports_error
    return pd.DataFrame(
        {
            'desc': [str(description.get(c))[:36] for c in commodities],
            'T001': intermediate,
            'iota': iota,
            'MCIF_ref': imports_reference,
            'MCIF_err': imports_error,
            'MCIF_status': imports_status,
            'F04000_ref': exports_reference,
            'F04000_err': exports_error,
            'F04000_status': exports_status,
            'dT001': residual_error,
            'exposure': residual_error.abs() * iota,
            'exposure_pct': 100 * residual_error.abs() / total_use.replace(0, np.nan),
        }
    )


def summary(table: pd.DataFrame) -> pd.DataFrame:
    """Economy-wide totals, and the split between the two sources."""
    intermediate = float(table['T001'].sum())
    gross = float(table['dT001'].abs().sum())
    exposed = float(table['exposure'].sum())
    return pd.DataFrame(
        [
            {
                'quantity': 'total intermediate T001',
                '$M': intermediate,
                '% of T001': 100.0,
            },
            {
                'quantity': 'net error in the row control',
                '$M': float(table['dT001'].sum()),
                '% of T001': 100 * float(table['dT001'].sum()) / intermediate,
            },
            {
                'quantity': 'gross error, sum |dT001|',
                '$M': gross,
                '% of T001': 100 * gross / intermediate,
            },
            {
                'quantity': 'landing in the intermediate block',
                '$M': exposed,
                '% of T001': 100 * exposed / intermediate,
            },
            {
                'quantity': '  ... attributable to MCIF',
                '$M': float((table['MCIF_err'].abs() * table['iota']).sum()),
                '% of T001': 100
                * float((table['MCIF_err'].abs() * table['iota']).sum())
                / intermediate,
            },
            {
                'quantity': '  ... attributable to F04000',
                '$M': float((table['F04000_err'].abs() * table['iota']).sum()),
                '% of T001': 100
                * float((table['F04000_err'].abs() * table['iota']).sum())
                / intermediate,
            },
        ]
    ).set_index('quantity')


def concentration(table: pd.DataFrame) -> pd.DataFrame:
    """How few commodities carry the exposure."""
    ranked = table['exposure'].sort_values(ascending=False)
    total = float(ranked.sum())
    return pd.DataFrame(
        [
            {
                'top_n': n,
                'share_of_exposure_%': 100 * float(ranked.head(n).sum()) / total,
            }
            for n in (1, 5, 10, 20, 50, 100)
        ]
    ).set_index('top_n')


def by_status(table: pd.DataFrame) -> pd.DataFrame:
    """Exposure grouped by how the ``MCIF`` cell was classified."""
    return table.groupby('MCIF_status').agg(
        n=('T001', 'size'),
        MCIF_ref=('MCIF_ref', 'sum'),
        abs_err=('MCIF_err', lambda s: float(s.abs().sum())),
        exposure=('exposure', 'sum'),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--live', action='store_true', help='run the FBS for F04000')
    parser.add_argument('--top', type=int, default=20, help='rows to list')
    args = parser.parse_args()

    table = exposure(live=args.live)
    columns = [
        'desc',
        'T001',
        'iota',
        'MCIF_ref',
        'MCIF_err',
        'MCIF_status',
        'F04000_ref',
        'F04000_err',
        'F04000_status',
        'exposure',
        'exposure_pct',
    ]
    pd.set_option('display.width', 260)

    print('\nWhat the imports and exports estimates cost the intermediate block, 2017')
    print('(millions of dollars)\n')
    print(summary(table).round(1).to_string())
    print('\nTop commodities by exposure\n')
    print(
        table.sort_values('exposure', ascending=False)
        .head(args.top)[columns]
        .round(1)
        .to_string()
    )
    print('\nExposure by MCIF cell status\n')
    print(by_status(table).round(0).to_string())
    print('\nConcentration\n')
    print(concentration(table).round(1).to_string())
    print('\nMCIF EXTRA - we book imports where BEA books none\n')
    print(
        table[table['MCIF_status'] == 'extra']
        .sort_values('MCIF_err', ascending=False)[
            ['desc', 'T001', 'iota', 'MCIF_err', 'exposure']
        ]
        .round(1)
        .to_string()
    )
    print()


if __name__ == '__main__':
    main()
