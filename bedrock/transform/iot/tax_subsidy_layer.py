"""The per-cell tax and subsidy layer of the Use table (#815 part 2, for #584).

BEA publishes the fiscal wedges only as commodity totals - the Supply bridge's
``TOP``/``MDTY``/``SUB`` columns - while the layered Margins product #584
specifies needs them per (buyer, commodity). This module allocates them, on a
partition the two published tables pin down jointly:

* **Transportation carries no tax**: the Margins table's goods-row
  Transportation sums match the bridge's ``TRANS`` to rounding (-$1.0bn on
  $415bn, both benchmark years).
* **The Wholesale/Retail margin columns carry a tax wedge**: their goods-row
  sums exceed the bridge's ``TRADE`` by $391.2bn (2017) / $322.6bn (2012) -
  the sales and excise taxes BEA books inside margins but inside ``TOP`` on
  the bridge (refined petroleum's motor-fuel taxes are the largest single
  piece). The same wedge reappears as the gap between the margin commodities'
  routed mass and the bridge's negative offsets (agreement to 0.15%), and it
  is bounded by ``0 <= wedge <= TOP`` for every commodity in both years.

So each goods commodity's ``TOP`` splits into a **margin-collected part** (the
wedge, split between wholesale and retail and allocated across buyers by
each kind's margin dollars) and a **producer-level part** (the rest,
allocated by producers'-value use, like ``MDTY`` and ``SUB``). Exports
(``F04000``) carry none of the margin-collected tax, the named exception of
#815.

⚠️ **The wedge splits by kind first** (#1247). It is ``sum(Wholesale +
Retail) - TRADE``, so it sits in both margins: each kind's total is observed
from its give-up, and :func:`sales_tax_by_kind` splits each commodity's
wedge between the two. Each kind's tax then follows that kind's margin. Split
by retail alone, a buyer with little retail margin carried more tax than its
whole trade margin, which left it a negative tax-free margin.

⚠️ **Where the layers sit in the price build-up.** The producer-level taxes
come *before* producers' value; the margin-collected taxes come *after* it,
inside the published Wholesale/Retail margins:

    basic + top_rest + duties + subsidies = producers' value
    producers' value + tax-free margins + transport + sales_tax = purchasers' value

Column sums reproduce the bridge by construction: ``sales_tax + top_rest``
sums to ``TOP`` per commodity, ``duties`` to ``MDTY``, ``subsidies`` to
``SUB``.

⚠️ **No published per-cell answer key exists for this layer.** The wedge and
its bases are observed; the per-cell split of the producer-level wedges is
proportional allocation. It is graded through column sums and through #584's
collapse check, never cell by cell.
"""

from __future__ import annotations

import argparse
import logging
import sys
import typing as ta

import numpy as np
import pandas as pd

from bedrock.extract.iot.io_2017 import (
    _load_benchmark_detail_supply_use_usa,
    load_benchmark_margins_before_redef_usa,
)
from bedrock.transform.iot.margin_rates import (
    BUYER_CODES,
    MARGIN_FAMILIES,
    goods_commodities,
)
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.taxonomy.bea.matrix_mappings import (
    USA_BENCHMARK_DETAIL_SUT_YEARS,
)

logger = logging.getLogger(__name__)

_PRO = "Producers' Value"
_TRANS = 'Transportation'
_WHL = 'Wholesale'
_RET = 'Retail'
_PUR = "Purchasers' Value"


class FiscalLayer(ta.NamedTuple):
    """Per-cell allocations of one benchmark year's fiscal wedges, USD.

    Each frame is goods-commodity x buyer. ``wholesale_tax`` and
    ``retail_tax`` are the margin-collected part of ``TOP``
    (post-producers'-value), each inside its own kind of trade margin
    (#1247); ``top_rest``, ``duties`` and ``subsidies`` are producer-level
    (``subsidies`` is negative, balance convention).
    """

    year: int
    wholesale_tax: pd.DataFrame
    retail_tax: pd.DataFrame
    top_rest: pd.DataFrame
    duties: pd.DataFrame
    subsidies: pd.DataFrame

    @property
    def sales_tax(self) -> pd.DataFrame:
        """The margin-collected tax, both kinds."""
        return self.wholesale_tax + self.retail_tax


def _goods_wide(margins: pd.DataFrame, column: str) -> pd.DataFrame:
    block = margins[column].unstack('Industry Code')
    return block.reindex(index=goods_commodities(), columns=list(BUYER_CODES)).fillna(
        0.0
    )


def _bridge(year: USA_BENCHMARK_DETAIL_SUT_YEARS) -> pd.DataFrame:
    supply = _load_benchmark_detail_supply_use_usa('Supply_detail', year)
    supply = supply.rename(columns=str.strip)
    return (
        supply[['TRADE', 'TRANS', 'TOP', 'MDTY', 'SUB']]
        .apply(pd.to_numeric, errors='coerce')
        .reindex(goods_commodities())
        .fillna(0.0)
        * MILLION_CURRENCY_TO_CURRENCY
    )


def sales_tax_wedge(
    year: USA_BENCHMARK_DETAIL_SUT_YEARS, clip: bool = True
) -> pd.Series:
    """The margin-collected part of ``TOP`` per goods commodity, USD.

    ``sum(Wholesale + Retail) - TRADE``, clipped to ``[0, TOP]`` unless
    *clip* is off. The clip is a guard and binds only on rounding.
    """
    margins = load_benchmark_margins_before_redef_usa(year)
    whl_ret = _goods_wide(margins, _WHL) + _goods_wide(margins, _RET)
    bridge = _bridge(year)
    wedge = whl_ret.sum(axis=1) - bridge['TRADE']
    if not clip:
        return wedge
    return wedge.clip(lower=0.0, upper=bridge['TOP'].clip(lower=0.0))


def top_sales_share(year: USA_BENCHMARK_DETAIL_SUT_YEARS = 2017) -> pd.Series:
    """``sales_tax_wedge / TOP`` per commodity - the anchor ratio a nowcast
    year freezes, since its own margins table does not exist. 0 where TOP is."""
    bridge = _bridge(year)
    top = bridge['TOP']
    return (sales_tax_wedge(year) / top.where(top != 0.0)).fillna(0.0)


#: The two kinds of trade margin and their Margins table columns.
_KINDS: tuple[tuple[str, str], ...] = (('wholesale', _WHL), ('retail', _RET))

#: Each kind's :class:`FiscalLayer` frame.
SALES_TAX_FRAMES: dict[str, str] = {kind: f'{kind}_tax' for kind, _ in _KINDS}

#: The published tables are in whole millions, so a total built from them
#: matches another only to this, USD.
_PUBLISHED_ROUNDING = 1.0 * MILLION_CURRENCY_TO_CURRENCY

#: Bisection bracket and steps for :func:`fit_wholesale_tilt`. The bracket
#: spans any split from all-retail to all-wholesale, and geometric bisection
#: over it reaches machine precision well inside the step count.
_TILT_BRACKET = (1e-9, 1e9)
_TILT_STEPS = 200


def fit_wholesale_tilt(
    tax: pd.Series, share: pd.Series, target: float
) -> tuple[pd.Series, float]:
    """Each commodity's *tax* split so its wholesale part sums to *target*.

    One scalar, solved by bisection::

        wholesale[c] = tax[c] * tilt * s[c] / (tilt * s[c] + 1 - s[c])

    with *share* ``s[c]`` the commodity's wholesale share of its trade margin.
    The total is monotone in the tilt, so bisection hits *target*; the tilt is
    the tax a dollar of wholesale margin carries against a dollar of retail.
    Returns the wholesale part and the tilt.

    Raises ``ValueError`` if no tilt reaches *target* to within
    :data:`CONSERVATION_TOLERANCE`: the tax forced onto commodities with only
    one kind of margin already exceeds it.
    """

    def wholesale_tax(tilt: float) -> pd.Series:
        tilted = tilt * share
        denominator = tilted + (1.0 - share)
        return tax * (tilted / denominator.where(denominator > 0.0)).fillna(0.0)

    low, high = _TILT_BRACKET
    least, most = float(wholesale_tax(low).sum()), float(wholesale_tax(high).sum())
    slack = CONSERVATION_TOLERANCE
    if not least - slack <= target <= most + slack:
        raise ValueError(
            f'A wholesale total of {target:,.0f} USD is outside what any tilt '
            f'reaches ({least:,.0f} to {most:,.0f}), so no split by kind exists.'
        )
    for _ in range(_TILT_STEPS):
        middle = (low * high) ** 0.5
        if wholesale_tax(middle).sum() < target:
            low = middle
        else:
            high = middle
    tilt = (low * high) ** 0.5
    return wholesale_tax(tilt), tilt


def sales_tax_by_kind(year: USA_BENCHMARK_DETAIL_SUT_YEARS) -> pd.DataFrame:
    """The sales-tax wedge per goods commodity, split between wholesale and
    retail. USD, columns ``wholesale`` and ``retail``; the fitted tilt is in
    ``.attrs['tilt']``.

    Each kind's total is observed. A margin commodity's ``TRADE`` give-up is
    its margin net of the tax it collected, so::

        tax[wholesale] = sum(Wholesale) - wholesale give-up

    and likewise for retail. Per commodity the split is one equation in two
    unknowns, so :func:`fit_wholesale_tilt` tilts it on one scalar to hit the
    wholesale total. The share it tilts is each commodity's wholesale share of
    its *non-export* trade margin, because exports carry no sales tax (#815).
    On that base the tilt is 0.852 in 2017 and 0.805 in 2012.
    ``nowcast_trade_margins.trade_level_tax_by_kind_2017`` fits the same split
    on gross shares, where it is 0.796.

    ⚠️ **Why by kind** (#1247). A split by retail margin alone put the whole
    wedge, wholesale's $175bn of it in 2017 included, on the retail buyers. A
    buyer with little retail margin then carried more tax than its whole
    trade margin: 34 cells in 2017 ($4.0bn), 22 in 2012 ($1.5bn). Split by
    kind, no cell carries more than 0.48 of its trade margin.

    Raises ``ValueError`` if no tilt reaches the wholesale total, if the
    retail part misses the retail total by more than the wedge's clip, or if
    a kind's tax on a commodity exceeds its non-export margin of that kind.
    """
    margins = load_benchmark_margins_before_redef_usa(year)
    wide = {kind: _goods_wide(margins, column) for kind, column in _KINDS}
    # The kind totals take every buyer's margin, since the give-up nets the
    # tax against all of it; the split per commodity takes the margin the tax
    # can sit on, which leaves out exports (#815).
    gross = pd.DataFrame({kind: frame.sum(axis=1) for kind, frame in wide.items()})
    live = pd.DataFrame(
        {
            kind: frame.clip(lower=0.0).drop(columns='F04000').sum(axis=1)
            for kind, frame in wide.items()
        }
    )
    supply = _load_benchmark_detail_supply_use_usa('Supply_detail', year)
    give_up = pd.to_numeric(
        supply.rename(columns=str.strip)['TRADE'], errors='coerce'
    ).fillna(0.0)
    target = {
        kind: float(gross[kind].sum())
        + float(give_up.reindex(list(MARGIN_FAMILIES[kind])).fillna(0.0).sum())
        * MILLION_CURRENCY_TO_CURRENCY
        for kind, _ in _KINDS
    }
    wedge = sales_tax_wedge(year)

    total = live.sum(axis=1)
    share = (live['wholesale'] / total.where(total > 0.0)).fillna(0.0)
    wholesale, tilt = fit_wholesale_tilt(wedge, share, target['wholesale'])
    split = pd.DataFrame({'wholesale': wholesale, 'retail': wedge - wholesale})
    split.attrs['tilt'] = tilt

    # The wedge is clipped to [0, TOP], so the retail part can miss its
    # observed total only by what the clip took off, to the tables' rounding.
    clipped = float(sales_tax_wedge(year, clip=False).sum() - wedge.sum())
    retail_gap = float(split['retail'].sum()) - target['retail']
    if abs(retail_gap + clipped) > _PUBLISHED_ROUNDING:
        raise ValueError(
            f'{year} retail sales tax sums to {split["retail"].sum():,.0f} USD '
            f'against {target["retail"]:,.0f} observed; the {retail_gap:,.0f} '
            f'gap is not the wedge clip ({clipped:,.0f}).'
        )
    above = (split > live + CONSERVATION_TOLERANCE).any(axis=1)
    if above.any():
        raise ValueError(
            f'{year} sales tax split by kind exceeds the non-export margin of its '
            f'kind on {sorted(split.index[above])[:5]}.'
        )
    return split


def _shares(base: pd.DataFrame) -> pd.DataFrame:
    """Row shares of the *positive* part of *base*.

    ⚠️ Negative cells (inventory draw-downs on ``F03000``) take no share. A
    signed base nets toward zero on some rows - ``S00402`` nets to $3M on
    $287bn gross - and the shares then put huge offsetting amounts on its
    cells (#1240). On a positive base every share is in ``[0, 1]``, so each
    cell carries the sign of its commodity's total.
    """
    positive = base.clip(lower=0.0)
    total = positive.sum(axis=1)
    return positive.div(total.where(total != 0.0), axis=0).fillna(0.0)


def _fill_under_cap(
    amount: pd.Series, weight: pd.DataFrame, cap: pd.DataFrame
) -> pd.DataFrame:
    """Each row's *amount* spread by *weight*, with no cell above *cap*.

    Cells that reach their cap stop there and the rest goes to the row's other
    cells by *weight*, repeated until the row is placed; where no weighted cell
    has room left, it goes by the room. Positive amounts only. A row whose
    amount exceeds its total cap keeps the excess unplaced, so the caller's
    conservation check names it.
    """
    w = weight.reindex_like(cap).fillna(0.0).clip(lower=0.0).to_numpy(dtype=float)
    c = cap.clip(lower=0.0).to_numpy(dtype=float)
    left: np.ndarray = (
        amount.reindex(cap.index).fillna(0.0).clip(lower=0.0).to_numpy(dtype=float)
    )
    x = np.zeros_like(c)
    for _ in range(c.shape[1] + 1):
        if (left <= _FILL_TOLERANCE).all():
            break
        room = c - x
        live = np.where(room > 0.0, w, 0.0)
        empty = (live.sum(axis=1) <= 0.0) & (left > _FILL_TOLERANCE)
        live[empty] = room[empty]
        total = live.sum(axis=1)
        per_weight = np.divide(left, total, out=np.zeros_like(left), where=total > 0.0)
        add = np.minimum(per_weight[:, None] * live, room)
        x += add
        left -= add.sum(axis=1)
    return pd.DataFrame(x, index=cap.index, columns=cap.columns)


#: Stopping rule for :func:`_fill_under_cap`, USD per row.
_FILL_TOLERANCE: float = 1e-3


#: The per-cell frames of a :class:`FiscalLayer`, sales tax first.
FISCAL_FRAMES: tuple[str, ...] = tuple(f for f in FiscalLayer._fields if f != 'year')

#: How far any one commodity total may move in the re-spread, USD. The
#: module's 0.0 $M standard: the re-spread conserves each total by
#: construction, so this is float noise on totals up to about 1e11 USD. Tax
#: with no live cell to go to is not noise, so it is held to the same bound.
CONSERVATION_TOLERANCE: float = 1.0


def build_fiscal_layer(
    year: USA_BENCHMARK_DETAIL_SUT_YEARS = 2017,
    cells: pd.DataFrame | None = None,
    trade: ta.Mapping[str, pd.DataFrame] | None = None,
) -> FiscalLayer:
    """Allocate the year's published wedges per cell. See the module docstring.

    Bases: the sales-tax wedge is split by kind (:func:`sales_tax_by_kind`)
    and each kind follows its own margin dollars, exports excluded (#1247);
    ``top_rest``/``duties``/``subsidies``
    follow producers'-value use. Both bases count positive cells only.

    ⚠️ **Pass *cells* for any table other than the anchor itself.** The
    allocation runs on *year*'s own cell pattern. A Use table with another
    pattern - any nowcast year, or a subset - has no cell for some of that tax.
    *cells* is that table, commodity x buyer, and *trade* its wholesale and
    retail margin per cell; see :func:`restrict_to_cells`.
    """
    layer = _allocate(year)
    return layer if cells is None else restrict_to_cells(layer, cells, trade)


def _target_grid(cells: pd.DataFrame) -> pd.DataFrame:
    """*cells* on the layer's grid, goods x :data:`BUYER_CODES`, zero-filled.

    Raises ``ValueError`` if *cells* carries values outside that grid - a total
    column such as ``T001``, or a non-goods row - so they cannot become live
    cells by accident.
    """
    frame = cells.fillna(0.0).astype(float)
    goods, buyers = goods_commodities(), list(BUYER_CODES)
    outside = frame.loc[~frame.index.isin(goods)].abs().sum(axis=1).gt(0.0).sum() + (
        frame.loc[:, ~frame.columns.isin(buyers)].abs().sum().gt(0.0).sum()
    )
    if outside:
        extra = [c for c in frame.columns if c not in buyers][:5]
        raise ValueError(
            f'cells has {outside} nonzero rows or columns off the goods x buyer '
            f'grid (columns such as {extra}); pass the buyer block only.'
        )
    return frame.reindex(index=goods, columns=buyers).fillna(0.0)


def restrict_to_cells(
    layer: FiscalLayer,
    cells: pd.DataFrame,
    trade: ta.Mapping[str, pd.DataFrame] | None = None,
) -> FiscalLayer:
    """*layer* re-spread onto the live cells of *cells*, USD.

    A cell is live where the target buys a positive amount. For each kind of
    sales tax it must also carry a positive margin of that kind in *trade*
    (``{'wholesale': ..., 'retail': ...}``, the target's margin per cell),
    because the tax comes back out of that margin in
    ``use_price_bridge._strip_sales_tax``; exports are never live for it.
    Leave *trade* off only for a table with no margins, and the sales tax
    then follows purchases.

    Within each commodity, live cells keep the tax they carry and the tax on
    dead cells is added onto them pro rata: the rescale ``kept * before /
    after``. The allocation puts each commodity's tax on one sign only, so the
    rescale cannot flip a cell. A commodity whose live cells carry no anchor
    tax spreads on the target's own base instead: its purchases, or its
    margin of the kind for sales tax.

    With *trade*, each kind of sales tax is held to each live cell's margin of
    that kind (:func:`_fill_under_cap`, #1247), since a pro-rata rescale alone
    can push a cell over it. Sales tax the commodity's margins have no room
    for is not margin-collected in the target table, so it joins the
    commodity's ``top_rest`` and is re-spread with it: ``TOP`` is conserved
    and the move is logged.

    The result is on the goods x :data:`BUYER_CODES` grid and zero off the
    live cells.

    Raises ``ValueError`` if any one commodity total, or the total across
    commodities, moves by :data:`CONSERVATION_TOLERANCE` or more. That happens
    only where a commodity has nowhere to go, and its tax would vanish. Also
    raises if a cell's tax flips sign, which only a hand-built layer that
    mixes signs within a commodity can cause.
    """
    grid = _target_grid(cells)
    purchases = grid.clip(lower=0.0)
    bases = {name: purchases for name in FISCAL_FRAMES}
    for kind, name in SALES_TAX_FRAMES.items():
        margin = (
            _target_grid(trade[kind]).clip(lower=0.0).where(purchases > 0.0, 0.0)
            if trade is not None
            else purchases.copy()
        )
        margin['F04000'] = 0.0
        bases[name] = margin

    frames = {}
    moved_to_top_rest = pd.Series(0.0, index=grid.index)
    for name in FISCAL_FRAMES:
        frame = getattr(layer, name)
        base = bases[name]
        capped = trade is not None and name in SALES_TAX_FRAMES.values()
        before = frame.sum(axis=1).reindex(grid.index).fillna(0.0)
        if name == 'top_rest':
            before = before + moved_to_top_rest
        kept = frame.reindex_like(grid).fillna(0.0).where(base > 0.0, 0.0)
        if capped:
            kept = kept.clip(upper=base)
        moved = before - kept.sum(axis=1)
        weight = kept.abs()
        unweighted = weight.sum(axis=1) == 0.0
        weight.loc[unweighted] = base.loc[unweighted]
        if capped:
            spread = kept + _fill_under_cap(moved, weight, base - kept)
            no_room = before - spread.sum(axis=1)
            moved_to_top_rest += no_room
        else:
            spread = kept + _shares(weight).mul(moved, axis=0)

        drift = before - spread.sum(axis=1)
        if capped:
            drift = drift - no_room
        lost = drift.abs()[drift.abs() >= CONSERVATION_TOLERANCE]
        if not lost.empty or abs(drift.sum()) >= CONSERVATION_TOLERANCE:
            if name not in SALES_TAX_FRAMES.values():
                where = 'no cell the target buys'
            else:
                where = 'no live cell other than exports'
            largest = lost.idxmax() if not lost.empty else drift.abs().idxmax()
            raise ValueError(
                f'{drift.abs().sum():,.0f} USD of {name} on {len(lost)} commodities '
                f'(largest {largest}, {drift.abs().max():,.0f} USD) would vanish: '
                f'the target table has {where} to re-spread it onto.'
            )
        flipped = (spread * kept < 0.0) & (spread.abs() >= CONSERVATION_TOLERANCE)
        if flipped.to_numpy().any():
            commodity = flipped.any(axis=1).idxmax()
            raise ValueError(
                f'{name} on {int(flipped.to_numpy().sum())} cells flips sign in the '
                f're-spread (first {commodity}): the layer mixes signs within a '
                f'commodity, which the allocation never does.'
            )
        frames[name] = spread

    shifted = moved_to_top_rest[moved_to_top_rest >= CONSERVATION_TOLERANCE]
    if not shifted.empty:
        logger.warning(
            'Sales tax of %s USD on %d commodities (largest %s, %s USD) has no '
            "room in the target table's trade margins and moved to the "
            'producer-level tax (top_rest).',
            f'{shifted.sum():,.0f}',
            len(shifted),
            shifted.idxmax(),
            f'{shifted.max():,.0f}',
        )
    return layer._replace(**frames)


def _allocate(year: USA_BENCHMARK_DETAIL_SUT_YEARS) -> FiscalLayer:
    """The allocation on *year*'s own cell pattern. See :func:`build_fiscal_layer`.

    Raises ``ValueError`` if a kind's tax lands above that kind's margin on a
    cell, which :func:`sales_tax_by_kind` rules out per commodity; the check
    is here so the error names the anchor rather than the strip, far
    downstream.
    """
    margins = load_benchmark_margins_before_redef_usa(year)
    bridge = _bridge(year)

    # The named exception (#815): exports carry none of the margin-collected
    # taxes, so they leave both bases.
    by_kind = sales_tax_by_kind(year)
    taxes = {}
    for kind, column in _KINDS:
        margin = _goods_wide(margins, column).assign(F04000=0.0)
        tax = _shares(margin).mul(by_kind[kind], axis=0)
        unplaced = (by_kind[kind] - tax.sum(axis=1)).abs()
        if float(unplaced.max()) >= CONSERVATION_TOLERANCE:
            raise ValueError(
                f'{year} {kind} sales tax on {unplaced.idxmax()} has no non-export '
                f'{kind} margin to sit on ({unplaced.max():,.0f} USD).'
            )
        over = tax - margin.clip(lower=0.0)
        if float(over.to_numpy().max()) >= CONSERVATION_TOLERANCE:
            raise ValueError(
                f'{year} {kind} sales tax exceeds the {kind} margin on '
                f'{int((over >= CONSERVATION_TOLERANCE).to_numpy().sum())} cells '
                f'(first {over.max(axis=1).idxmax()}).'
            )
        taxes[SALES_TAX_FRAMES[kind]] = tax

    sales_tax = taxes['wholesale_tax'] + taxes['retail_tax']
    pro_share = _shares(_goods_wide(margins, _PRO))
    top_rest = pro_share.mul(bridge['TOP'] - sales_tax.sum(axis=1), axis=0)
    duties = pro_share.mul(bridge['MDTY'], axis=0)
    subsidies = pro_share.mul(bridge['SUB'], axis=0)
    return FiscalLayer(
        year=int(year),
        **taxes,
        top_rest=top_rest,
        duties=duties,
        subsidies=subsidies,
    )


# --- the reconciliation ----------------------------------------------------


def check(argv: list[str] | None = None) -> int:
    """Print the partition facts and the layer's column-sum identities."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--year', type=int, default=2017)
    args = parser.parse_args(argv)
    year = ta.cast(USA_BENCHMARK_DETAIL_SUT_YEARS, args.year)

    million = MILLION_CURRENCY_TO_CURRENCY
    margins = load_benchmark_margins_before_redef_usa(year)
    bridge = _bridge(year)
    trans_gap = (_goods_wide(margins, _TRANS).sum(axis=1) - bridge['TRANS']).sum()
    print(f'transport vs TRANS, goods rows: {trans_gap / million:,.0f} $M')
    wedge = sales_tax_wedge(year)
    print(
        f'sales-tax wedge: {wedge.sum() / million:,.0f} $M '
        f'({100 * wedge.sum() / bridge["TOP"].sum():.1f}% of TOP)'
    )

    layer = build_fiscal_layer(year)
    top_gap = layer.sales_tax.sum(axis=1) + layer.top_rest.sum(axis=1) - bridge['TOP']
    duty_gap = layer.duties.sum(axis=1) - bridge['MDTY']
    sub_gap = layer.subsidies.sum(axis=1) - bridge['SUB']
    for name, gap in (('TOP', top_gap), ('MDTY', duty_gap), ('SUB', sub_gap)):
        unallocated = gap.abs().sum() / million
        print(f'{name} column-sum residual: {unallocated:,.1f} $M')
    exports = layer.sales_tax.get('F04000')
    if exports is not None:
        print(f'sales tax allocated to exports: {exports.sum() / million:,.1f} $M')
    return 0


if __name__ == '__main__':
    sys.exit(check())
