"""Check the 2017 margin anchor against the published Supply table.

The validation half of Step 4c phase 1
([#610](https://github.com/cornerstone-data/bedrock/issues/610)):
``bedrock/transform/iot/nowcast_margins.py`` derives the receiving sets and the
per-(buyer, commodity, margin type) rates from the transaction-level Margins
table, and this checks that aggregating them reproduces the Supply table's
margin columns **commodity by commodity**.

Per commodity, never in aggregate. ``T014`` nets to about 1 economy-wide against
7.4 trillion of gross mass - a trade margin is added to the good and subtracted
from the trade commodity that earned it - so a totals check does not merely risk
passing on broken data, it passes on anything.

The two identities, and what closes them:

```
sum_buyers ( Wholesale + Retail )  =  TRADE[c] + TOP[c]
sum_buyers   Transportation        =  TRANS[c]
```

The trade identity carries the tax term because the two tables sit in different
frameworks - the Margins table is make-use, the Supply table supply-use, and
wholesale and retail trade commodity tax is inside the margin columns of the one
and in taxes on products in the other (B. Jolliff, BEA, 2025-05-30). It still
does not close exactly, and the residual is not noise: sales tax sits in the
margin columns but excise sits in ``Producers' Value``, and both land in ``TOP``,
so what is left over is the **producer-level** share of the tax. It lands where
the tax law puts it - severance tax on oil, gas and coal at 80-90% of ``TOP``,
federal excise on tobacco and alcohol at 35-37%, and motor fuel tax at
essentially zero because it is collected at the pump and behaves like a sales
tax.

Both sides are before redefinitions: the published detail SUT is a
before-redefinitions construct and the margins anchor is
``load_2017_margins_before_redef_usa``. Pairing the after-redefinitions Margins
table with this Supply table closes the trade identity for 226 commodities
rather than 236 - a *redefinition* mismatch, not a framework one, and not a
number to chase. Redefinition moves 6,007 million of trade margin across 111
commodities, of which ``333914`` pump and pumping equipment is 4,139: it is
redefined out of the after-redefinitions table altogether while the
before-redefinitions Supply table still carries ``TRADE`` for it.

Run from the repo root::

    uv run python -m bedrock.analysis.nowcasting.margins_2017_baseline
    uv run python -m bedrock.analysis.nowcasting.margins_2017_baseline --check

``--check`` exits non-zero if any identity or bound check comes in below the
counts measured when this landed, which is what makes it useful in a rebuild.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd

from bedrock.extract.iot.io_2017 import _load_2017_detail_supply_use_usa
from bedrock.transform.iot.nowcast_margins import (
    COMMODITY_LEVEL,
    MARGIN_TYPE_LEVEL,
    RATE_BASIS,
    margin_rate_table,
    margins_by_commodity,
    rate_dispersion_by_commodity,
    receiving_set_summary,
)
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.taxonomy.bea.v2017_commodity import (
    USA_2017_COMMODITY_CODES,
    USA_2017_COMMODITY_DESC,
)

logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).parent / 'output'
IDENTITY_CSV_PATH = OUTPUT_DIR / 'margins_2017_identities_by_commodity.csv'
RATE_TABLE_CSV_PATH = OUTPUT_DIR / 'margins_2017_rate_table.csv'

#: The Supply table's bridge columns, in published order. ``TRADE`` carries a
#: trailing space in the workbook header; :func:`load_supply_bridge_2017` strips
#: it rather than propagating the surprise.
SUPPLY_BRIDGE_COLUMNS = [
    'T007',
    'MCIF',
    'MADJ',
    'T013',
    'TRADE',
    'TRANS',
    'T014',
    'MDTY',
    'TOP',
    'SUB',
    'T015',
    'T016',
]

#: An identity "holds" for a commodity when the two sides agree within this,
#: relative to the published side.
IDENTITY_TOLERANCE = 0.01

#: A commodity is margin-bearing for the tax decomposition only if it actually
#: carries trade margin: with no wholesale or retail, both sides of the trade
#: identity are zero and the residual degenerates to the whole of ``TOP``, which
#: reads as a spurious 100% producer-level tax on restaurants, electric power,
#: insurance and legal services.
MIN_TOP_FOR_TAX_DECOMPOSITION = 100 * MILLION_CURRENCY_TO_CURRENCY

#: Measured when #610 landed, on the before-redefinitions pair. ``--check``
#: treats these as floors; a rebuild that comes in under one of them has changed
#: something.
BASELINE_TRANS_COMMODITIES_HOLDING = 180
BASELINE_TRADE_COMMODITIES_HOLDING = 236


def load_supply_bridge_2017() -> pd.DataFrame:
    """
    The published 2017 detail Supply table's bridge columns, per commodity. USD.

    These are the commodity-level control on the transaction-level Margins
    table: ``TRADE`` and ``TRANS`` are one value per commodity where the Margins
    table is per (buyer, commodity), and they reconcile only by aggregation - the
    reverse is impossible, since commodity totals cannot be split back across
    buyers without reintroducing the assumption the Margins table exists to
    carry.
    """
    supply = _load_2017_detail_supply_use_usa('Supply_detail').rename(
        columns=lambda column: column.strip()
    )
    commodities = [code for code in USA_2017_COMMODITY_CODES if code in supply.index]
    bridge = (
        supply.loc[commodities, SUPPLY_BRIDGE_COLUMNS].astype(float)
        * MILLION_CURRENCY_TO_CURRENCY
    )
    bridge.index.name = COMMODITY_LEVEL
    return bridge


#: Each identity as ``(name, derived column, published target, margin column)``.
#: The trade identity's target carries the tax term where its margin column does
#: not, which is why the two are named separately.
IDENTITIES: tuple[tuple[str, str, str, str], ...] = (
    ('trade', 'trade_margins', 'trade_target', 'TRADE'),
    ('transport', 'transport_margins', 'TRANS', 'TRANS'),
)


def _relative_difference(derived: pd.Series, published: pd.Series) -> pd.Series:
    """``derived / published - 1``, NaN where the published side is zero."""
    return derived / published.replace(0.0, np.nan) - 1


def verify_supply_margin_identities() -> pd.DataFrame:
    """
    Both identities, commodity by commodity.

    Columns: the derived aggregates, the published columns they target, the
    difference each way, and the producer-level tax the trade residual
    decomposes into. ``*_holds`` is True where the two sides agree within
    :data:`IDENTITY_TOLERANCE` **or** both are zero - 144 commodities bear no
    transportation margin at all, and counting those as failures would be as
    misleading as counting them as passes silently.
    """
    derived = margins_by_commodity()
    published = load_supply_bridge_2017()
    out = pd.concat([derived, published], axis=1).fillna(0.0)
    out.insert(0, 'commodity_name', out.index.map(dict(USA_2017_COMMODITY_DESC)))

    out['trade_target'] = out['TRADE'] + out['TOP']
    for name, derived_column, published_column, _ in IDENTITIES:
        difference = out[derived_column] - out[published_column]
        relative = _relative_difference(out[derived_column], out[published_column])
        out[f'{name}_diff'] = difference
        out[f'{name}_rel_diff'] = relative
        out[f'{name}_holds'] = relative.abs().le(IDENTITY_TOLERANCE) | (
            (out[derived_column] == 0) & (out[published_column] == 0)
        )

    # The residual of the trade identity is the producer-level (excise) share of
    # TOP; the rest is trade-level tax already inside the margin columns.
    out['producer_level_tax'] = -out['trade_diff']
    decomposable = (out['trade_margins'] != 0) & (
        out['TOP'] > MIN_TOP_FOR_TAX_DECOMPOSITION
    )
    out['tax_decomposable'] = decomposable
    out['producer_level_tax_share'] = (
        out['producer_level_tax'] / out['TOP'].replace(0.0, np.nan)
    ).where(decomposable)
    return out


def identity_summary(identities: pd.DataFrame | None = None) -> pd.DataFrame:
    """
    How each identity does, split by the sign of the Supply margin column.

    The three populations answer different questions, and reporting only the
    first is how a margin check flatters itself:

    ``all commodities``
        includes the 144 commodities with no transportation margin on either
        side, which hold trivially. 320 of 402 sounds better than the
        informative 180 of 258.
    ``margin > 0``
        the receiving side - the commodities this anchor is built to reproduce.
    ``margin < 0``
        the supplying side. The Margins table carries only the receiving half of
        each margin, so the derived column is zero here **by construction** and
        every commodity fails. Those are the 19 trade and 5 transport
        commodities that give up 96.8% and 56.8% of their own output; rebuilding
        them is Step 4a's job, not this one's.
    """
    frame = verify_supply_margin_identities() if identities is None else identities
    rows = []
    for name, derived_column, target_column, margin_column in IDENTITIES:
        for population, selection in (
            ('all commodities', pd.Series(True, index=frame.index)),
            (f'{margin_column} > 0', frame[margin_column] > 0),
            (f'{margin_column} < 0', frame[margin_column] < 0),
        ):
            subset = frame.loc[selection]
            published = subset[target_column].sum()
            rows.append(
                {
                    'identity': name,
                    'population': population,
                    'commodities': int(selection.sum()),
                    'holds': int(subset[f'{name}_holds'].sum()),
                    'derived': subset[derived_column].sum(),
                    'published': published,
                    'net_diff': subset[f'{name}_diff'].sum(),
                    'net_rel_diff': (
                        subset[f'{name}_diff'].sum() / published
                        if published
                        else np.nan
                    ),
                }
            )
    return pd.DataFrame(rows)


def give_up_check(identities: pd.DataFrame | None = None) -> pd.DataFrame:
    """
    The supplying side: the 24 commodities whose Supply margin column is
    negative, against what the Margins table says they gave up.

    The negative side was scoped as Step 4a's problem - a trade or transport
    commodity gives up nearly all of its own output, so it was to be rebuilt
    from 4a's output vector. It does not have to be: the published
    ``Purchasers' Value`` already carries it, because on a trade commodity's own
    rows purchasers' value is *below* the sum of the components by exactly the
    margin moved onto the goods (:func:`margins_by_commodity`).

    Against the transport commodities it is a direct read - all five within
    0.2%, 20 million out of 415,570. Against the trade commodities it runs high
    by the trade-level tax the receiving side is short by, and the two sides
    measure that tax independently to within 0.3%: 391,761 million given up
    above the negative Supply columns here, against 391,162 million by which
    ``sum(Wholesale + Retail)`` exceeds ``TRADE`` on the receiving side - 12.0%
    of positive ``TRADE``, and 90% of the ``TOP`` those commodities carry.
    Petroleum wholesalers are the extreme at 2.2x, and consistently so - motor
    fuel tax is collected at the pump, so nearly all of petroleum's ``TOP`` is
    trade-level.
    """
    frame = verify_supply_margin_identities() if identities is None else identities
    supplying = frame.loc[(frame['TRADE'] < 0) | (frame['TRANS'] < 0)].copy()
    supplying['supply_negative'] = -(
        supplying['TRADE'].clip(upper=0) + supplying['TRANS'].clip(upper=0)
    )
    supplying['give_up_ratio'] = (
        supplying['margin_given_up'] / supplying['supply_negative']
    )
    supplying['implied_trade_level_tax'] = (
        supplying['margin_given_up'] - supplying['supply_negative']
    )
    columns = [
        'commodity_name',
        'margin_given_up',
        'TRADE',
        'TRANS',
        'supply_negative',
        'give_up_ratio',
        'implied_trade_level_tax',
    ]
    return supplying[columns].sort_values('supply_negative', ascending=False)


def trade_identity_misses(identities: pd.DataFrame | None = None) -> pd.DataFrame:
    """
    The commodities where the trade identity does not hold, and the tax that
    accounts for them.

    They are not a scatter of near misses: they are the excise and severance
    goods, in order. What the identity's tax term corrects for is *trade-level*
    tax, which sits inside the Wholesale and Retail columns; excise sits in
    ``Producers' Value`` instead and never enters the left-hand side, so a
    commodity misses by exactly its producer-level tax.
    """
    frame = verify_supply_margin_identities() if identities is None else identities
    positive = frame['TRADE'] > 0
    misses = frame.loc[positive & ~frame['trade_holds']]
    columns = [
        'commodity_name',
        'trade_target',
        'trade_margins',
        'trade_diff',
        'trade_rel_diff',
        'TOP',
        'producer_level_tax_share',
    ]
    return misses[columns].sort_values('trade_diff')


def _margin_cell_profile(margin_type: str) -> pd.DataFrame:
    """Per commodity, how many cells carry *margin_type* and how big they are."""
    margins = margin_rate_table().xs(margin_type, level=MARGIN_TYPE_LEVEL)['margin']
    grouped = margins.groupby(level=COMMODITY_LEVEL)
    return pd.DataFrame({'cells': grouped.size(), 'median_cell': grouped.median()})


def transport_rounding_profile(
    identities: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    The transportation identity's misses, bucketed by how big the published
    cells are.

    The misses are a publication artifact rather than a method error, and this
    is the evidence. BEA publishes the Margins table rounded to a million, and
    the smallest non-zero transportation cell in it is exactly 1 million - so
    every cell that would round below half a million is published as zero and
    drops out of the commodity sum. The shortfall is therefore one-sided (195 of
    258 commodities come in short, 22 over) and scales with how thinly a
    commodity's margin is spread: commodities whose median cell is about a
    million miss by ~3%, those above 20 million by 0.02%. Economy-wide it is
    -0.25%, and the 78 missing commodities carry 7.6% of ``TRANS``.

    Nothing to fix here - a nowcast rebuilt from these rates inherits the same
    rounding, and the alternative is to invent cells BEA suppressed.
    """
    frame = verify_supply_margin_identities() if identities is None else identities
    profile = _margin_cell_profile('Transportation').join(
        frame[['TRANS', 'transport_diff', 'transport_rel_diff', 'transport_holds']]
    )
    profile = profile.loc[profile['TRANS'] > 0]
    buckets = pd.cut(
        profile['median_cell'] / MILLION_CURRENCY_TO_CURRENCY,
        [0, 1, 2, 5, 20, np.inf],
        labels=['<= 1M', '1-2M', '2-5M', '5-20M', '> 20M'],
    )
    return (
        profile.groupby(buckets, observed=True)
        .agg(
            commodities=('TRANS', 'size'),
            holds=('transport_holds', 'sum'),
            median_cells=('cells', 'median'),
            median_rel_diff=('transport_rel_diff', 'median'),
        )
        .rename_axis('median published cell')
    )


def rate_bound_check(identities: pd.DataFrame | None = None) -> pd.DataFrame:
    """
    Which denominator a margin rate is actually bounded by.

    ``TRADE / T013`` exceeds 1 for 21 commodities carrying a fifth of all
    positive ``TRADE`` - apparel at 1.84, ``S00402`` used and secondhand goods at
    16.02 - and that is correct rather than broken: margin is added to basic
    value, not carved out of it, so a rate on ``T013`` is unbounded. ``T013``
    remains the right *allocation* base, per BEA's "margins are distributed based
    on the value in column OR" and the manual's interim-supply weighting, but
    "rate > 1" is not a validation rule. ``T016`` is where the bound is real.
    """
    frame = verify_supply_margin_identities() if identities is None else identities
    rows = []
    for numerator in ('TRADE', 'TRANS'):
        for denominator in ('T013', 'T016'):
            ratio = frame[numerator] / frame[denominator].replace(0.0, np.nan)
            positive = frame[numerator] > 0
            above = positive & ratio.gt(1)
            rows.append(
                {
                    'ratio': f'{numerator} / {denominator}',
                    'commodities_positive': int(positive.sum()),
                    'above_1': int(above.sum()),
                    'max': ratio[positive].max(),
                    'share_of_value_above_1': (
                        frame.loc[above, numerator].sum()
                        / frame.loc[positive, numerator].sum()
                    ),
                }
            )
    return pd.DataFrame(rows)


def export_identities(path: Path = IDENTITY_CSV_PATH) -> Path:
    """Write the per-commodity identity comparison to CSV."""
    path.parent.mkdir(parents=True, exist_ok=True)
    verify_supply_margin_identities().to_csv(path)
    logger.info('Wrote per-commodity identity comparison to %s', path)
    return path


def export_rate_table(path: Path = RATE_TABLE_CSV_PATH) -> Path:
    """Write the per-(buyer, commodity, margin type) rate table to CSV."""
    path.parent.mkdir(parents=True, exist_ok=True)
    margin_rate_table().to_csv(path)
    logger.info('Wrote rate table to %s', path)
    return path


def _print_frame(frame: pd.DataFrame, float_format: str = '{:,.3f}') -> None:
    with pd.option_context(
        'display.width', 200, 'display.float_format', float_format.format
    ):
        print(frame.to_string())


def main(check: bool = False) -> int:
    """Print the phase-1 report; with *check*, fail if a count has regressed."""
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    millions = 1 / MILLION_CURRENCY_TO_CURRENCY

    summary = receiving_set_summary()
    print('Receiving sets (margin in millions):')
    shown = summary.assign(
        margin=summary['margin'] * millions,
        rate_margin=summary['rate_margin'] * millions,
        level_margin=summary['level_margin'] * millions,
    )
    _print_frame(shown)

    dispersion = rate_dispersion_by_commodity()
    print('\nWithin-commodity rate dispersion (no commodity has a uniform rate):')
    _print_frame(
        dispersion.groupby(level=MARGIN_TYPE_LEVEL).agg(
            commodities=('cv', 'size'),
            median_cv=('cv', 'median'),
            uniform=('cv', lambda cv: int((cv < 0.01).sum())),
        )
    )

    identities = verify_supply_margin_identities()
    print('\nSupply-column identities, per commodity (millions):')
    summary_frame = identity_summary(identities)
    _print_frame(
        summary_frame.assign(
            derived=summary_frame['derived'] * millions,
            published=summary_frame['published'] * millions,
            net_diff=summary_frame['net_diff'] * millions,
        ),
        '{:,.4f}',
    )

    print('\nWhich denominator bounds a margin rate:')
    _print_frame(rate_bound_check(identities))

    decomposable = identities.loc[identities['tax_decomposable']]
    producer_share = (
        decomposable['producer_level_tax'].sum() / decomposable['TOP'].sum()
    )
    print(
        f'\nProducer-level tax on the {len(decomposable)} margin-bearing '
        f'commodities with TOP > 100 million: '
        f'{producer_share:.1%} of TOP '
        f'({decomposable["producer_level_tax"].sum() * millions:,.0f} of '
        f'{decomposable["TOP"].sum() * millions:,.0f} million)'
    )

    misses = trade_identity_misses(identities)
    print(
        f'\nTrade identity: the {len(misses)} misses are the excise and '
        f'severance goods (millions):'
    )
    _print_frame(
        misses.assign(
            trade_target=misses['trade_target'] * millions,
            trade_margins=misses['trade_margins'] * millions,
            trade_diff=misses['trade_diff'] * millions,
            TOP=misses['TOP'] * millions,
        )
    )

    print(
        '\nTransport identity: the misses are publication rounding - BEA '
        'publishes no cell below 1 million, so thinly spread commodities lose '
        'the suppressed ones:'
    )
    _print_frame(transport_rounding_profile(identities), '{:,.4f}')

    give_up = give_up_check(identities)
    transport_give_up = give_up.loc[give_up['TRANS'] < 0]
    print(
        f'\nSupplying side: the {len(give_up)} commodities with a negative '
        f'Supply margin column, against what they give up in the Margins table '
        f'(millions). Transport reads directly; trade runs high by the '
        f'trade-level tax ({give_up["implied_trade_level_tax"].sum() * millions:,.0f} '
        f'in total, {transport_give_up["implied_trade_level_tax"].sum() * millions:,.0f} '
        f'of it on transport):'
    )
    _print_frame(
        give_up.assign(
            margin_given_up=give_up['margin_given_up'] * millions,
            TRADE=give_up['TRADE'] * millions,
            TRANS=give_up['TRANS'] * millions,
            supply_negative=give_up['supply_negative'] * millions,
            implied_trade_level_tax=give_up['implied_trade_level_tax'] * millions,
        )
    )

    identity_path = export_identities()
    rate_path = export_rate_table()
    print(f'\nWrote {identity_path}\nWrote {rate_path}')

    if not check:
        return 0

    failures = []
    holding = (
        summary_frame.set_index(['identity', 'population'])['holds']
        .astype(int)
        .to_dict()
    )
    for identity, population, floor in (
        ('transport', 'TRANS > 0', BASELINE_TRANS_COMMODITIES_HOLDING),
        ('trade', 'TRADE > 0', BASELINE_TRADE_COMMODITIES_HOLDING),
    ):
        got = holding[identity, population]
        if got < floor:
            failures.append(
                f'{identity} identity holds for {got} commodities, below the '
                f'{floor} measured when #610 landed'
            )
    above_one = (
        rate_bound_check(identities).set_index('ratio')['above_1'].astype(int).to_dict()
    )
    for ratio in ('TRADE / T016', 'TRANS / T016'):
        if above_one[ratio]:
            failures.append(
                f'{ratio} exceeds 1 for {above_one[ratio]} commodities; expected none'
            )
    rates = margin_rate_table()
    unfitted = int(rates.loc[rates['basis'] == RATE_BASIS, 'rate'].isna().sum())
    if unfitted:
        failures.append(f'{unfitted} rate-basis transactions have no rate')

    if failures:
        for failure in failures:
            print(f'FAIL: {failure}')
        return 1
    print('\nOK: identities, bounds and rate coverage all at or above baseline.')
    return 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--check',
        action='store_true',
        help='exit non-zero if an identity or bound check has regressed',
    )
    raise SystemExit(main(**vars(parser.parse_args())))
