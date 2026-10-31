"""Step 6 driver: the before-redefinitions MUT quartet from the balanced SUT.

One call per nowcast year turns the Step 5 products - the balanced Supply and
purchaser-price Use SUT parquets in ``transform/output_data`` - into the four
tables Step 7 works on: Make, producer-price Use, the import matrix, and the
hyper-detailed Margins table. The converters themselves live in
:mod:`supply_to_make`, :mod:`sut_use_to_mut_use`,
:mod:`mut_use_to_import_matrix` and the margins machinery of
:mod:`margin_rates` / :mod:`use_price_bridge`; this module only loads, wires,
gates and saves.

**The Margins table is the placement authority.** For a nowcast year nobody
publishes the per-cell producer value, so it is built here first - 2017 rate
panel on the year's balanced purchaser Use, margin mass placed per margin
family - and the Use conversion then *reads* producer values from it, exactly
as the 2017 replay reads them from the published table. The stored layout
follows the published convention: goods rows close
``PRO + Transportation + Wholesale + Retail = PUR`` cell by cell, while
margin-commodity rows carry the margin routed onto them in ``Producers'
Value`` against a direct-purchase ``Purchasers' Value``. The per-margin-
commodity columns (the seller placement, #815) sit alongside the five
published columns.

⚠️ **2017-anchored, tax-inclusive.** The rates and placement shares are the
2017 benchmark's (:func:`margin_rates.build_rate_panel`), applied to the
year's own purchaser values - the year-portable design, not an approximation
of a published number that exists. The margin columns keep BEA's
tax-inclusive definition (``fiscal=None``): splitting a nowcast year's sales
tax out of its trade margins needs the year's own fiscal layer, which is the
#823 generalisation and not built here. Published 2017 confirms the
convention: each good's stripped margin is its Supply ``TRADE + TRANS`` plus
the trade-level share of its ``TOP``, to 1.1bn $M across all goods.

**The anchor is only the pattern; the Supply table sets the amounts (#1161).**
Make's tax wedge is fitted to the *balanced* tax rows and columns
(:func:`balanced_make_wedge`), and the margin placement to each good's Supply
margin and each margin commodity's Make output (:func:`_reconcile_placement`),
so every commodity's producer-price Use row equals its Make output. What
Step 6 cannot close it hands on, gated and named: where the balance moved a
code's tax row and tax column apart, and the margin columns' failure to net
(#809). :func:`_commodity_identity` checks that nothing else is left and the
sidecar records both.

⚠️ **The import control is ``MCIF + MDTY``** per #816's benchmark finding;
whether a nowcast-year ``MCIF`` is c.i.f. per commodity (making that control
carry over unchanged) is #822 and open. The sidecar names the control used.

Two commodities, ``4200ID`` and ``S00900``, are not balance labels.
``4200ID`` enters the conversion as a zero row and still receives the
customs-duty credit in ``F05000`` and the duty wedge in Make, by rule.
``S00900``, the rest-of-world adjustment, enters as its two final-demand
cells: the year's PCE adjustment (minus nonresidents' spending in the United
States) and the same amount in exports (#1158).

Testing: no direct unit tests, on purpose - like
:mod:`nowcast_sut_assembly`, this is wiring over heavy cached inputs, and the
per-year identity gates in :func:`mut_from_balanced` are the test: they run
on the real objects every build, and a violation refuses the save.
"""

from __future__ import annotations

import argparse
import functools
import json
import posixpath
import sys
import typing as ta
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from bedrock.extract.iot.nowcast_mut_storage import (
    GCS_NOWCAST_MUT_DIR,
    MARGINS_VALUE_COLUMNS,
    MutTable,
    default_nowcast_mut_vintage,
    nowcast_mut_artifact_name,
)
from bedrock.transform.iot.margin_rates import (
    BUYER_CODES,
    MARGIN_COMMODITIES,
    RatePanel,
    build_rate_panel,
    goods_commodities,
)
from bedrock.transform.iot.mut_use_to_import_matrix import import_matrix_from_use
from bedrock.transform.iot.nowcast_product_taxes import trade_level_share
from bedrock.transform.iot.nowcast_trade_margins import trade_commodity_prior
from bedrock.transform.iot.nowcast_transport_margins import transport_commodity_prior
from bedrock.transform.iot.nowcast_va_taxes import (
    CUSTOMS_INDUSTRY,
    t00sub_cells,
    t00top_cells,
    trade_commodities,
)
from bedrock.transform.iot.supply_to_make import make_from_sut
from bedrock.transform.iot.sut_use_to_mut_use import (
    V00200_COMPONENTS,
    f05000_column,
    use_producer_from_sut,
)
from bedrock.transform.iot.use_price_bridge import MARGIN_FAMILIES, margin_columns
from bedrock.utils.config.settings import (
    FBS_DIR,
    GIT_BRANCH,
    GIT_HASH,
    GIT_HASH_LONG,
    PKG_VERSION_NUMBER,
)
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.metadata.metadata import source_lineage
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_final_demand import SUT_FINAL_DEMAND_CODES
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES

#: Years the Step 5 balance publishes products for. 2017 is the anchor year:
#: its balanced tables track the published benchmark on totals and identities,
#: and building its MUT quartet through the same pipeline keeps every model
#: year - the anchor included - on one provenance (and gives the conversion a
#: free end-to-end consistency check against the published 2017 MUTs).
NOWCAST_MUT_YEARS: tuple[int, ...] = tuple(range(2017, 2025))

#: Identity-gate tolerance. BEA publishes nothing below $1M, and every gate
#: here checks a relation that holds by construction, so a breach is a bug,
#: not noise.
GATE_ATOL = 1.0 * MILLION_CURRENCY_TO_CURRENCY

#: Stopping rule for the two biproportional fits below (the Make tax wedge and
#: the margin reconciliation): worst constrained total, USD. A tenth of the
#: gate tolerance, so a converged fit never trips a gate on its own rounding.
_FIT_TOLERANCE = 0.1 * GATE_ATOL
_FIT_ITERATIONS = 5000

#: The six value-added rows of the balanced Use SUT, in balance order.
_SUT_VA_ROWS = ('V00100', 'T00OTOP', 'T00OSUB', 'V00300', 'T00TOP', 'T00SUB')

#: Bridge columns the conversion consumes from the balanced Supply table.
_BRIDGE_COLUMNS = ('MCIF', 'MADJ', 'MDTY')

_PRO = "Producers' Value"
_PUR = "Purchasers' Value"

#: How the published three margin columns collapse from the margin families.
_FAMILY_COLUMN = {
    'transport': 'Transportation',
    'wholesale': 'Wholesale',
    'retail': 'Retail',
}


class MutTables(ta.NamedTuple):
    """One year's Step 6 output, USD, before redefinitions."""

    year: int
    make: pd.DataFrame
    use: pd.DataFrame
    imports: pd.DataFrame
    margins: pd.DataFrame
    #: The ``Balanced_Detail_*`` stems the build read, for the sidecar.
    sources: tuple[str, ...]
    #: The commodity identity residual Step 5 hands over, $M (#1161).
    identity_residual: ta.Mapping[str, float] = {}


# --- loading the Step 5 products -------------------------------------------


def _balanced_path(kind: str, year: int, directory: Path) -> Path:
    """The newest ``Balanced_Detail_<kind>_<year>_*.parquet`` in *directory*."""
    matches = sorted(
        directory.glob(f'Balanced_Detail_{kind}_{year}_*.parquet'),
        key=lambda p: p.stat().st_mtime,
    )
    if not matches:
        raise FileNotFoundError(
            f'no Balanced_Detail_{kind}_{year}_*.parquet in {directory}; '
            'run bedrock.transform.iot.nowcast_sut_assembly for the year first'
        )
    return matches[-1]


def load_balanced_sut(
    year: int, directory: Path | None = None
) -> tuple[pd.DataFrame, pd.DataFrame, tuple[str, ...]]:
    """The year's balanced Supply and Use SUT, converted from $M to USD."""
    where = Path(directory) if directory is not None else Path(FBS_DIR)
    supply_path = _balanced_path('Supply', year, where)
    use_path = _balanced_path('Use_SUT', year, where)
    supply = pd.read_parquet(supply_path) * MILLION_CURRENCY_TO_CURRENCY
    use = pd.read_parquet(use_path) * MILLION_CURRENCY_TO_CURRENCY
    return supply, use, (supply_path.stem, use_path.stem)


def _conform_use_sut(use: pd.DataFrame, rest_of_world_pce: float) -> pd.DataFrame:
    """The balanced Use SUT on the full conversion axes, zero-filled.

    ``4200ID`` and ``S00900`` are not balance labels. ``4200ID``'s row enters
    as zeros. ``S00900`` enters as the rest-of-world adjustment: PCE
    ``rest_of_world_pce`` (negative) and exports its opposite, so the row nets
    to zero against a Supply that carries no ``S00900`` output or imports.
    Missing labels beyond those two mean the input is not a balanced Use SUT,
    so they raise instead of being papered over.
    """
    if rest_of_world_pce > 0:
        raise ValueError(
            f'rest-of-world PCE adjustment is {rest_of_world_pce:,.0f} USD; '
            'it removes nonresidents\' spending, so it cannot be positive'
        )
    commodities = list(USA_2017_COMMODITY_CODES)
    unexpected = set(commodities) - set(use.index) - {'4200ID', 'S00900'}
    if unexpected:
        raise ValueError(
            f'balanced Use SUT is missing commodity rows {sorted(unexpected)}'
        )
    missing_va = [row for row in _SUT_VA_ROWS if row not in use.index]
    if missing_va:
        raise ValueError(f'balanced Use SUT is missing VA rows {missing_va}')
    fd_columns = [c for c in SUT_FINAL_DEMAND_CODES if c in use.columns]
    conformed = use.reindex(
        index=commodities + list(_SUT_VA_ROWS),
        columns=list(USA_2017_INDUSTRY_CODES) + fd_columns,
    ).fillna(0.0)
    conformed.loc['S00900', 'F01000'] = rest_of_world_pce
    conformed.loc['S00900', 'F04000'] = -rest_of_world_pce
    return conformed


def _supply_bridge(supply: pd.DataFrame) -> pd.DataFrame:
    missing = [c for c in _BRIDGE_COLUMNS if c not in supply.columns]
    if missing:
        raise ValueError(f'balanced Supply table is missing bridge columns {missing}')
    return (
        supply[list(_BRIDGE_COLUMNS)]
        .reindex(list(USA_2017_COMMODITY_CODES))
        .fillna(0.0)
    )


def _supply_block(supply: pd.DataFrame) -> pd.DataFrame:
    return (
        supply.drop(
            columns=[c for c in supply.columns if c not in USA_2017_INDUSTRY_CODES]
        )
        .reindex(
            index=list(USA_2017_COMMODITY_CODES),
            columns=list(USA_2017_INDUSTRY_CODES),
        )
        .fillna(0.0)
    )


def _supply_column(supply: pd.DataFrame, name: str) -> pd.Series:
    """One Supply column by name, commodity-indexed. The ``TRADE`` header
    carries a trailing space in the balanced parquet."""
    columns = {str(c).strip(): c for c in supply.columns}
    if name not in columns:
        raise ValueError(f'balanced Supply table has no {name} column')
    return (
        supply[columns[name]]
        .astype(float)
        .reindex(list(USA_2017_COMMODITY_CODES))
        .fillna(0.0)
    )


def _fit(
    prior: pd.DataFrame,
    rows: pd.Series,
    columns: pd.Series,
    fallback: pd.DataFrame,
) -> pd.DataFrame:
    """Non-negative *prior* scaled to the *rows* totals and to the non-NaN
    *columns* totals (biproportional; NaN columns are free).

    A row or constrained column that has a target but no prior mass takes its
    pattern from *fallback*, so the money is placed rather than dropped.

    ⚠️ **Rows win.** Each pass ends on the row scaling, so the rows are exact
    on return and any column the pattern cannot reach keeps the shortfall. The
    two callers both need that order: Make's industry totals must equal the
    balanced Use's tax rows, and each good must give up exactly its Supply
    margin. Stops at :data:`_FIT_TOLERANCE` on the columns or after
    :data:`_FIT_ITERATIONS`; what is left shows in the identity report.
    """
    cells = prior.clip(lower=0.0).copy()
    backup = fallback.reindex_like(cells).fillna(0.0).clip(lower=0.0)
    empty_rows = (cells.sum(axis=1) <= 0.0) & (rows > 0.0)
    cells.loc[empty_rows] = backup.loc[empty_rows]
    constrained = columns.notna()
    empty_columns = (cells.sum(axis=0) <= 0.0) & constrained & (columns > 0.0)
    cells.loc[:, empty_columns] = backup.loc[:, empty_columns]

    x = cells.to_numpy(copy=True)
    row_target = rows.reindex(cells.index).fillna(0.0).to_numpy()
    column_target = columns.reindex(cells.columns).to_numpy()
    mask = ~np.isnan(column_target)
    for _ in range(_FIT_ITERATIONS):
        got = x.sum(axis=0)
        scale = np.ones_like(got)
        np.divide(column_target, got, out=scale, where=mask & (got > 0.0))
        x *= scale[None, :]
        got = x.sum(axis=1)
        x *= np.divide(row_target, got, out=np.zeros_like(got), where=got > 0.0)[
            :, None
        ]
        column_gap = np.abs(x.sum(axis=0) - column_target)[mask].max(initial=0.0)
        if column_gap <= _FIT_TOLERANCE:
            break
    return pd.DataFrame(x, index=cells.index, columns=cells.columns)


def balanced_make_wedge(
    year: int, block: pd.DataFrame, use_sut: pd.DataFrame, supply: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """The Make tax wedge on the balanced taxes, industry x commodity, USD.

    Returns the cells and the ``T00TOP``/``T00SUB`` rows they sum to. Step 5
    moves both the Use table's tax rows and the Supply table's ``TOP``/``SUB``
    columns - by 89bn $M on ``T00TOP`` in 2022 - so the pre-balance operator
    (:func:`~bedrock.transform.iot.nowcast_va_taxes.t00top_cells`,
    :func:`~bedrock.transform.iot.nowcast_va_taxes.t00sub_cells`) is only the
    pattern here. Its cells are fitted to the balanced Use rows (so Make's
    industry totals agree with ``V00200``) and to the balanced Supply columns
    (so each commodity's wedge is the tax the Supply table charges on it,
    #1161):

    - ``TOP``: each commodity carries its producer-level share of the balanced
      ``TOP``, ``4200ID`` carries ``MDTY``, and the wholesale and retail
      commodity columns are free - they take the trade-level tax, which BEA
      books inside the trade margin.
    - ``SUB``: each commodity carries its balanced ``SUB``, scaled to the row
      total where the balance left the two a rounding apart.

    ⚠️ The balance moves the rows and the columns separately, so they can
    disagree on one code - ``531HSO``'s ``T00SUB`` is 2,444 $M in 2017 against
    a ``SUB`` of 2,206 on the commodity only that industry makes. No cell
    pattern meets both; the rows win (:func:`_fit`), and the difference stays
    on that commodity's identity, where it is a Step 5 residual to report
    rather than a placement to fake.
    """
    commodities = list(USA_2017_COMMODITY_CODES)
    industries = list(USA_2017_INDUSTRY_CODES)
    fallback = block.reindex(index=commodities, columns=industries).fillna(0.0).T

    top_rows = pd.Series(use_sut.loc['T00TOP']).reindex(industries).astype(float)
    top_columns = _supply_column(supply, 'TOP') * (
        1.0 - trade_level_share().reindex(commodities).fillna(0.0)
    )
    top_columns[CUSTOMS_INDUSTRY] += float(_supply_column(supply, 'MDTY').sum())
    top_columns[trade_commodities()] = np.nan
    top = _fit(t00top_cells(year, block=block), top_rows, top_columns, fallback)

    sub_rows = -pd.Series(use_sut.loc['T00SUB']).reindex(industries).astype(float)
    sub_columns = -_supply_column(supply, 'SUB')
    if float(sub_columns.sum()) > 0.0:
        sub_columns *= float(sub_rows.sum()) / float(sub_columns.sum())
    sub = _fit(-t00sub_cells(year), sub_rows, sub_columns, fallback)

    rows = pd.DataFrame({'T00TOP': top_rows, 'T00SUB': -sub_rows}).T.rename_axis(
        index='value_added_code', columns='industry'
    )
    return top - sub, rows


# --- the margins table ------------------------------------------------------


def margin_commodity_prior() -> dict[str, pd.DataFrame]:
    """Per family, how a purchased commodity's margin splits across its codes.

    The prior :func:`margins_table` places margin by (#1140), so the
    per-margin-commodity columns follow the commodity purchased rather than
    repeating each buyer's split. Transport is the 2017 mode x commodity fit;
    wholesale and retail are the 2017 Economic Census product lines by kind of
    business.
    """
    return {
        'transport': transport_commodity_prior(),
        'wholesale': trade_commodity_prior('wholesale'),
        'retail': trade_commodity_prior('retail'),
    }


#: The two families the Supply table separates: ``TRANS`` against the transport
#: codes, ``TRADE`` against wholesale and retail together.
RECONCILED_FAMILIES: dict[str, tuple[str, ...]] = {
    'transport': MARGIN_FAMILIES['transport'],
    'trade': (*MARGIN_FAMILIES['wholesale'], *MARGIN_FAMILIES['retail']),
}


def _reconcile_placement(
    placed: ta.Mapping[str, pd.DataFrame],
    purchaser: pd.DataFrame,
    give_up: ta.Mapping[str, pd.Series],
    book: pd.Series,
) -> dict[str, pd.DataFrame]:
    """Placed margin rescaled to the Supply table's margins (#1161).

    Per :data:`RECONCILED_FAMILIES` entry, the goods x margin-commodity totals
    of the placement are fitted so each good gives up *give_up* (its Supply
    ``TRANS``; its ``TRADE`` plus the trade-level tax BEA keeps inside the
    trade margin) and each margin commodity books *book* (what its Make output
    leaves its producer-price Use row to carry). Each ``(good, margin
    commodity)`` pair keeps its buyer split; a good with a target but no
    placed margin takes its buyers' purchaser split and the family's
    economy-wide commodity mix.

    The margin columns of a balanced SUT need not net to zero (#809), so the
    two sides of a family can disagree. The goods side is exact - it is what
    the Supply table says each good carries - and the margin-commodity targets
    are scaled to it, which leaves the residual on the commodities whose
    column failed to net.
    """
    goods = list(purchaser.index)
    row_total = purchaser.sum(axis=1)
    buyer_split = purchaser.div(row_total.where(row_total > 0.0), axis=0).fillna(0.0)
    out = dict(placed)
    for family, codes in RECONCILED_FAMILIES.items():
        totals = pd.DataFrame({code: placed[code].sum(axis=1) for code in codes})
        rows = give_up[family].reindex(goods).fillna(0.0).clip(lower=0.0)
        columns = book.reindex(list(codes)).fillna(0.0).clip(lower=0.0)
        if float(columns.sum()) > 0.0:
            columns *= float(rows.sum()) / float(columns.sum())
        mix = totals.sum(axis=0) / float(totals.to_numpy().sum())
        fallback = pd.DataFrame({code: float(mix[code]) for code in codes}, index=goods)
        fitted = _fit(totals, rows, columns, fallback)
        for code in codes:
            before = totals[code]
            scale = (fitted[code] / before.where(before > 0.0)).fillna(0.0)
            seeded = (before <= 0.0) & (fitted[code] > 0.0)
            out[code] = placed[code].mul(scale, axis=0) + buyer_split.mul(
                fitted[code].where(seeded, 0.0), axis=0
            )
    return out


def margins_table(
    use_sut: pd.DataFrame,
    panel: RatePanel | None = None,
    prior: ta.Mapping[str, pd.DataFrame] | None = None,
    give_up: ta.Mapping[str, pd.Series] | None = None,
    book: pd.Series | None = None,
) -> pd.DataFrame:
    """The year's hyper-detailed Margins table, USD, from the purchaser Use.

    Index ``(Industry Code, Commodity Code)``; columns are the five published
    value columns followed by one column per margin commodity. Rows carrying
    nothing on any value column are left out.

    Goods rows: the margin columns are :func:`margin_columns`' placement
    (tax-inclusive) and ``Producers' Value`` is the purchaser value less that
    placement, so the identity ``PRO + T + W + R = PUR`` closes cell by cell;
    unreconciled, that is the panel's ``pro`` rate on the cell. *prior* (default
    :func:`margin_commodity_prior`) splits each family by commodity purchased,
    fitted so what each margin commodity earns from each buyer - and so its
    Use row - is the anchor placement's, gated below. Margin-commodity rows follow
    the published convention: ``Purchasers' Value`` is the buyer's direct
    purchase of the margin service, ``Producers' Value`` adds the margin
    routed onto that commodity, and the margin columns are zero.

    *give_up* and *book* reconcile the placement to the Supply table's margins
    after the anchor placement is gated (:func:`_reconcile_placement`, #1161).
    Without them the table is the anchor placement alone, which conserves
    margin mass but leaves each commodity's producer-price Use row off its
    Make output.
    """
    rates = panel if panel is not None else build_rate_panel(2017)
    goods = goods_commodities()
    buyers = [b for b in BUYER_CODES if b in use_sut.columns]
    purchaser = use_sut.loc[goods, buyers].astype(float)

    # The panel spans all 422 buyer codes; the SUT has no F05000 column, so
    # alignment inside margin_columns grows an all-NaN column - restrict back
    # to the live buyers before anything is stored.
    placed = {
        code: frame.reindex(index=goods, columns=buyers).fillna(0.0)
        for code, frame in margin_columns(
            purchaser,
            rates,
            None,
            margin_commodity_prior() if prior is None else prior,
        ).items()
    }
    # The commodity split moves margin between a buyer's goods only: what each
    # margin commodity earns from each buyer, which books its Use row, must
    # be exactly the buyer split's.
    buyer_split = margin_columns(purchaser, rates, None)
    earnings_gap = max(
        float(
            (
                placed[code].sum(axis=0)
                - buyer_split[code]
                .reindex(index=goods, columns=buyers)
                .fillna(0.0)
                .sum(axis=0)
            )
            .abs()
            .max()
        )
        for code in MARGIN_COMMODITIES
    )
    _gate('margin commodity earnings per buyer (worst cell)', earnings_gap)
    if give_up is not None:
        if book is None:
            raise ValueError("give_up needs the margin commodities' book too")
        placed = _reconcile_placement(placed, purchaser, give_up, book)

    stripped = functools.reduce(lambda a, b: a + b, placed.values())
    wide: dict[str, pd.DataFrame] = {_PRO: purchaser - stripped, _PUR: purchaser}
    for family, codes in MARGIN_FAMILIES.items():
        wide[_FAMILY_COLUMN[family]] = functools.reduce(
            lambda a, b: a + b, (placed[code] for code in codes)
        )
    for code in MARGIN_COMMODITIES:
        wide[code] = placed[code]

    direct = use_sut.reindex(index=list(MARGIN_COMMODITIES), columns=buyers).fillna(0.0)
    booked = pd.DataFrame(
        {code: frame.sum(axis=0) for code, frame in placed.items()}
    ).T.reindex(index=list(MARGIN_COMMODITIES), columns=buyers)

    columns = [*MARGINS_VALUE_COLUMNS, *MARGIN_COMMODITIES]
    goods_block = pd.DataFrame({name: wide[name].stack() for name in columns}).reindex(
        columns=columns
    )
    margin_block = pd.DataFrame(0.0, index=direct.stack().index, columns=columns)
    margin_block[_PUR] = direct.stack()
    margin_block[_PRO] = (direct + booked).stack()

    table = pd.concat([goods_block, margin_block])
    table = table.loc[(table != 0.0).any(axis=1)]
    # The wide frames stack commodity-major; the published table and the Use
    # conversion key on (buyer, commodity).
    table.index = table.index.set_names(['Commodity Code', 'Industry Code'])
    return table.swaplevel().sort_index()


# --- the build --------------------------------------------------------------


def _gate(label: str, gap_usd: float) -> None:
    if abs(gap_usd) > GATE_ATOL:
        raise AssertionError(
            f'{label} is off by ${gap_usd / MILLION_CURRENCY_TO_CURRENCY:,.3f}M; '
            'this identity holds by construction, so the input or the wiring '
            'is wrong'
        )


def _commodity_identity(
    year: int,
    identity: pd.Series,
    margins: pd.DataFrame,
    give_up: ta.Mapping[str, pd.Series],
    tax_gap: pd.Series,
) -> dict[str, float]:
    """Gate the commodity identity on what Step 6 controls; return the rest.

    ``identity`` is Make output less the producer-price Use row. Step 6 places
    the margins, so each good must give up exactly its Supply margin (gated).
    It cannot place the tax wedge where the balanced Use rows and Supply
    columns disagree, so on every other commodity the identity must equal that
    disagreement, *tax_gap*, and nothing else (gated). What is left - the
    disagreement and the margin columns' failure to net (#809) - is a Step 5
    residual, returned in $M for the sidecar.
    """
    commodity = margins.index.get_level_values('Commodity Code')
    goods = margins.loc[~commodity.isin(MARGIN_COMMODITIES)]
    by_good = goods.groupby(level='Commodity Code')
    stripped = {
        'transport': by_good['Transportation'].sum(),
        'trade': by_good['Wholesale'].sum() + by_good['Retail'].sum(),
    }
    on_goods = ~identity.index.isin(MARGIN_COMMODITIES)
    unexplained = identity[on_goods] - tax_gap.reindex(identity.index)[on_goods]
    for family, target in give_up.items():
        got = stripped[family].reindex(target.index).fillna(0.0)
        gap = (got - target.clip(lower=0.0))[on_goods]
        _gate(
            f'{year} {family} margin given up vs Supply (worst commodity)',
            float(gap.abs().max()),
        )
        unexplained += (target - got)[on_goods]
    _gate(
        f'{year} commodity identity beyond the Step 5 tax disagreement '
        f'(worst commodity, {unexplained.abs().idxmax()})',
        float(unexplained.abs().max()),
    )
    million = MILLION_CURRENCY_TO_CURRENCY
    return {
        'net_usd_m': float(identity.sum()) / million,
        'sum_abs_usd_m': float(identity.abs().sum()) / million,
        'tax_disagreement_sum_abs_usd_m': float(identity[on_goods].abs().sum())
        / million,
        'margin_commodities_sum_abs_usd_m': float(identity[~on_goods].abs().sum())
        / million,
    }


def mut_from_balanced(
    year: int,
    supply: pd.DataFrame,
    use: pd.DataFrame,
    *,
    rest_of_world_pce: float,
    panel: RatePanel | None = None,
    sources: tuple[str, ...] = (),
) -> MutTables:
    """The year's MUT quartet from its balanced Supply and Use SUT (USD).

    *rest_of_world_pce* is ``Y[S00900, F01000]``, USD, which the balance does
    not carry; see :func:`_conform_use_sut`. Every gate below checks a relation
    the construction guarantees; see :data:`GATE_ATOL`.
    """
    use_sut = _conform_use_sut(use, rest_of_world_pce)
    supply_block = _supply_block(supply)
    bridge = _supply_bridge(supply)
    commodities = list(USA_2017_COMMODITY_CODES)
    industries = list(USA_2017_INDUSTRY_CODES)
    fd_columns = [c for c in use_sut.columns if c not in industries]

    wedge, taxes = balanced_make_wedge(year, supply_block, use_sut, supply)
    make = make_from_sut(supply_block, taxes, year, wedge_cells=wedge)

    output = make.sum(axis=0).reindex(commodities).fillna(0.0)
    shortfall = (
        output
        - use_sut.loc[commodities, industries + fd_columns].sum(axis=1)
        - f05000_column(bridge).reindex(commodities).fillna(0.0)
    )
    give_up = {
        'transport': _supply_column(supply, 'TRANS'),
        'trade': _supply_column(supply, 'TRADE')
        + _supply_column(supply, 'TOP')
        * trade_level_share().reindex(commodities).fillna(0.0),
    }
    margins = margins_table(use_sut, panel=panel, give_up=give_up, book=shortfall)
    goods_rows = margins.loc[
        ~margins.index.get_level_values('Commodity Code').isin(MARGIN_COMMODITIES)
    ]
    closure = (
        goods_rows[MARGINS_VALUE_COLUMNS[:-1]].sum(axis=1) - goods_rows[_PUR]
    ).abs()
    _gate(f'{year} margins goods-row identity (worst cell)', float(closure.max()))
    stripped = float((goods_rows[_PUR] - goods_rows[_PRO]).sum())
    margin_rows = margins.loc[
        margins.index.get_level_values('Commodity Code').isin(MARGIN_COMMODITIES)
    ]
    routed = float((margin_rows[_PRO] - margin_rows[_PUR]).sum())
    _gate(f'{year} margin mass conservation', stripped - routed)

    converted = use_producer_from_sut(use_sut, bridge, margins, year)
    interior = converted.loc[commodities, industries + fd_columns]
    if interior.isna().any().any():
        raise AssertionError(f'{year} producer-price Use carries NaN cells')
    column_gap = (
        interior.sum(axis=0)
        - use_sut.loc[commodities, industries + fd_columns].sum(axis=0)
    ).abs()
    _gate(
        f'{year} Use buyer-total preservation (worst column)', float(column_gap.max())
    )
    v00200_gap = (
        converted.loc[['V00200'], industries].iloc[0]
        - use_sut.loc[list(V00200_COMPONENTS), industries].sum(axis=0)
    ).abs()
    _gate(f'{year} V00200 collapse identity (worst industry)', float(v00200_gap.max()))

    tax_target = (_supply_column(supply, 'TOP') + _supply_column(supply, 'SUB')) - (
        give_up['trade'] - _supply_column(supply, 'TRADE')
    )
    tax_target[CUSTOMS_INDUSTRY] += float(_supply_column(supply, 'MDTY').sum())
    tax_gap = wedge.sum(axis=0).reindex(commodities).fillna(0.0) - tax_target
    identity = output - converted.loc[commodities].fillna(0.0).sum(axis=1)
    residual = _commodity_identity(year, identity, margins, give_up, tax_gap)

    import_control = bridge['MCIF'].astype(float) + bridge['MDTY'].astype(float)
    imports = import_matrix_from_use(converted.loc[commodities], import_control)
    allocated = imports.drop(columns=['F05000'], errors='ignore').sum().sum()
    _gate(f'{year} import allocation total', float(allocated - import_control.sum()))

    return MutTables(
        year=year,
        make=make,
        use=converted,
        imports=imports,
        margins=margins,
        sources=sources,
        identity_residual=residual,
    )


def build(year: int, directory: Path | None = None) -> MutTables:
    """Load the year's balanced SUT and convert it. The one-call entry point."""
    # Deferred import: nowcast pulls in the FBS machinery.
    from bedrock.transform.iot.nowcast import rest_of_world_pce_usd  # noqa: PLC0415

    supply, use, sources = load_balanced_sut(year, directory)
    return mut_from_balanced(
        year,
        supply,
        use,
        rest_of_world_pce=rest_of_world_pce_usd(year),
        sources=sources,
    )


# --- saving -----------------------------------------------------------------

_TABLE_ATTR: dict[MutTable, str] = {
    'Make': 'make',
    'Use': 'use',
    'Import': 'imports',
    'Margins': 'margins',
}


def save_mut(
    tables: MutTables,
    out_dir: Path | None = None,
    *,
    upload: bool = False,
) -> list[Path]:
    """Persist the quartet as versioned parquet + metadata sidecars.

    Files land in *out_dir* (default ``transform/output_data``) under the
    names :func:`nowcast_mut_storage.nowcast_mut_artifact_name` resolves, so
    the detail loaders find a local build without touching GCS. With
    *upload*, both files also go to :data:`GCS_NOWCAST_MUT_DIR`, which needs
    credentials (``scripts/google-login``). Step 6 output is before
    redefinitions by construction; Step 7 owns the ``after`` stage.
    """
    directory = Path(out_dir) if out_dir is not None else Path(FBS_DIR)
    directory.mkdir(parents=True, exist_ok=True)
    vintage = default_nowcast_mut_vintage()

    written: list[Path] = []
    for table, attr in _TABLE_ATTR.items():
        frame: pd.DataFrame = getattr(tables, attr)
        name = nowcast_mut_artifact_name(
            table, year=tables.year, stage='before', vintage=vintage
        )
        parquet_path = directory / name
        frame.to_parquet(parquet_path)
        meta = {
            'tool': 'bedrock',
            'category': 'NowcastMUT',
            'name_data': Path(name).stem,
            'tool_version': PKG_VERSION_NUMBER,
            'git_hash': GIT_HASH,
            'ext': 'parquet',
            'date_created': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'tool_meta': {
                'step': 'Step 6 - SUT to MUT conversion, before redefinitions',
                'vintage': vintage,
                'units': 'USD',
                'branch': GIT_BRANCH,
                'commit': GIT_HASH_LONG,
                'balanced_inputs': list(tables.sources),
                'primary_source_meta': source_lineage(tables.sources),
                'margin_anchor': (
                    '2017 rate panel and placement shares, tax-inclusive '
                    '(BEA definition); the nowcast-year fiscal split is #823'
                ),
                'import_control': 'MCIF + MDTY per #816; nowcast basis is #822',
                'commodity_identity_residual': dict(tables.identity_residual),
                'builder': 'bedrock.transform.iot.nowcast_mut',
            },
        }
        meta_path = directory / f'{Path(name).stem}_metadata.json'
        meta_path.write_text(json.dumps(meta, indent=4))
        written.extend([parquet_path, meta_path])

    if upload:
        # Deferred import: the CLI must not need GCS credentials to build.
        from bedrock.utils.io.gcp import (  # noqa: PLC0415
            GCS_CORNERSTONE,
            upload_file_to_gcs,
        )

        for path in written:
            upload_file_to_gcs(
                str(path),
                posixpath.join(GCS_CORNERSTONE, GCS_NOWCAST_MUT_DIR, path.name),
            )
    return written


# --- CLI --------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        '--year',
        type=int,
        action='append',
        choices=NOWCAST_MUT_YEARS,
        help='year to convert; repeatable. Default: every year with balanced products',
    )
    parser.add_argument(
        '--no-save',
        action='store_true',
        help='run the conversion and its gates without persisting the tables',
    )
    parser.add_argument(
        '--gcs',
        action='store_true',
        help='also upload the saved products to GCS (needs credentials)',
    )
    args = parser.parse_args(argv)

    from bedrock.transform.iot.nowcast import rest_of_world_pce_usd  # noqa: PLC0415

    years = tuple(args.year) if args.year else NOWCAST_MUT_YEARS
    panel = build_rate_panel(2017)
    for year in years:
        supply, use, sources = load_balanced_sut(year)
        tables = mut_from_balanced(
            year,
            supply,
            use,
            rest_of_world_pce=rest_of_world_pce_usd(year),
            panel=panel,
            sources=sources,
        )
        interior = tables.use.loc[
            list(USA_2017_COMMODITY_CODES), list(USA_2017_INDUSTRY_CODES)
        ]
        print(
            f'{year}: Make {tables.make.shape}, Use {tables.use.shape}, '
            f'Import {tables.imports.shape}, Margins {len(tables.margins):,} rows; '
            f'intermediate total '
            f'${interior.sum().sum() / MILLION_CURRENCY_TO_CURRENCY:,.0f}M'
        )
        if not args.no_save:
            written = save_mut(tables, upload=args.gcs)
            print(f'  saved {len(written)} files to {written[0].parent}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
