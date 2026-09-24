"""Moving the split *inside* manufacturing's fuel bill on MECS, 2018 and 2022.

:data:`~.inputs_structure.EXPENSE_TO_BEA` maps ASM's ``CSTFU`` fuels bucket onto
three BEA commodities -- gas distribution ``221200``, refined petroleum
``324110`` and coal ``212100`` -- and moves all three together on one survey
index, which leaves **BEA's 2017 split between them frozen for every year**.
MECS observes that split directly, and says it does not hold still: across
manufacturing the fuel bill goes **coal 17.8% -> 11.8%** and **gas 74.5% ->
81.6%** between 2018 and 2022.

This module supplies that movement, and only that movement.

The derivation, and why it is checkable
---------------------------------------

MECS publishes no expenditure by fuel.  It publishes **Table 7.2**, average
price in USD per million Btu, and **Table 3.2**, fuel consumption in trillion
Btu.  Their product is already million USD -- the unit **Table 7.10** publishes
for the three carriers it does cover -- so the two tables the derivation does
*not* use are an independent check on it rather than a plausibility argument::

    fuel     price x 3.2 quantity  =  24,621     7.10 publishes  28,246
    feedstock  price x 2.2 quantity =  3,707
                                      ------
                                      28,328                     ratio 1.003

✅ **Natural gas reconciles to 1.003 (2018) and 1.018 (2022)** once feedstock is
added back, which it must be: 7.10 is expenditure on every therm purchased,
while 3.2 is the *fuel* half and 2.2 is the feedstock half.

✅ **Electricity sits at 0.976 and 0.971 and has no feedstock to explain it.**
That residual is a scope difference in the other table: 3.2 reports *net*
electricity, purchases less any the establishment sells back, against 7.10's
gross purchases.

⚠️ **7.2 is an average price**, so this reproduces a *summed* expenditure only up
to the dispersion inside each cell.  That is why the output here is used as a
**share**, never as a level.

Why the level cannot be used, only the movement
-----------------------------------------------

❌ **MECS's fuel-bill shares must not be written onto the BEA rows.**  The two
partition manufacturing's fuel bill completely differently, and MECS is not the
one that is wrong:

======================  ==========  ==========
share of the group      BEA 2017    MECS 2018
======================  ==========  ==========
``221200`` gas               15.0%       74.5%
``324110`` petroleum         65.1%        7.8%
``212100`` coal              19.9%       17.8%
======================  ==========  ==========

BEA's ``324110`` manufacturing row is **$38.2B and is mostly not fuel**.  Its
largest buyers are refineries taking their own streams ($7.6B), asphalt paving
($5.0B), basic organic chemicals ($3.2B), other petroleum and coal products
($2.7B) and plastics resin ($2.4B) -- feedstock and materials, which ``CSTFU``
excludes by definition and MECS books under nonfuel use.

So the disagreement is a scope difference, not an error to be corrected, and it
is the same reason :func:`~.inputs_structure.expense_scope` gives for every
other kind carrying an index rather than a level.  The device here is that rule
applied one level deeper: **BEA sets the split, MECS moves it.**

    ``weight[c, i, t] = Use2017[c, i] x mecs_share[c, i, t] / mecs_share[c, i, 2018]``

renormalised so each column's group total is untouched.  The feedstock
contamination is in the base and cancels out of the ratio.

⚠️ **The base is 2018, not 2017**, matching the anchor #898 chose for the
electricity row.  It follows that 2017 *and* 2018 are unchanged -- an index off
2018 is 1.0 at 2018 -- and the adjustment grows toward 2022.  There is no seam,
because what is carried is change and change is zero at the base.

Coverage, and the direction the NAICS match has to run
-------------------------------------------------------

✅ **All 232 BEA manufacturing columns are reached, carrying 100% of the group's
$58.6B.**  MECS publishes 80-82 manufacturing rows, most at three digits and 20
at six.

⚠️ **Match from the BEA column to the MECS row, not the other way.**  Mapping
each MECS row to *one* BEA column through the NAICS crosswalk reaches **37 of
232** and $44.6B, because a three-digit MECS row resolves to a single BEA code
and the other columns beneath it get nothing.  Reading the deepest MECS row that
prefixes each BEA column's NAICS instead broadcasts it correctly.  The two
readings also disagree about the aggregate, so this is not only a coverage
question.
"""

from __future__ import annotations

import argparse
import functools

import numpy as np
import pandas as pd

from bedrock.analysis.nowcasting.inputs_structure import (
    EXPENSE_TO_BEA,
    SHIPPED_FORM,
    _manufacturing_bea_industries,
    _use_2017_detail,
    interpolate_shares,
)
from bedrock.extract.flowbyactivity import getFlowByActivity

#: The MECS vintages that carry Tables 7.2 and 7.10.  ⚠️ Quadrennial, and both
#: were unfetched until #895 -- ``tables:`` had them commented out, so no
#: vintage had ever produced them.
MECS_VINTAGES = (2018, 2022)

#: The vintage the index is taken against; see the module docstring.
MECS_BASE = MECS_VINTAGES[0]

#: ``Table 3.2 carrier -> Table 7.2 carrier``.  ⚠️ **The two tables do not share
#: a fuel vocabulary**, and the mismatch is silent -- an unmapped name simply
#: finds no price and drops out, so a first pass matched three carriers and
#: looked like it had worked.
#:
#: ⚠️ ``Coke and Breeze`` is priced at ``Coal Coke``.  7.2 publishes coke and
#: breeze separately at 12.08 and 11.48 USD per million Btu, within 5% of each
#: other, so collapsing them onto the coke price is small against the coke line
#: and smaller against the bucket.
TABLE_7_2_PRICE = {
    'Natural Gas': 'Natural Gas Total',
    'Distillate Fuel Oil': 'Total Diesel Fuel and Distillate Fuel Oil',
    'Residual Fuel Oil': 'Residual Fuel Oil',
    'Hydrocarbon Gas Liquids, excluding natural gasoline': (
        'HGL (excluding natural gasoline)'
    ),
    'Coal': 'Coal Total',
    'Coke and Breeze': 'Coal Coke',
    'Net Electricity': 'Electricity Total',
}

#: ``Table 3.2 carrier -> the BEA commodity the buyer books it against``.
#:
#: ⚠️ **Coke is routed to ``212100`` coal, not to ``324190``.**  An integrated
#: steel mill buys coal and cokes it on site, so BEA records the *coal*
#: purchase while MECS records the *coke* consumed -- 331110 buys $2.1B of
#: ``212100`` against $0.8B of ``324190`` in 2017.  Routing coke to its own row
#: would move a purchase that is not there.  ``324190``'s own manufacturing row
#: is asphalt and roofing, not coke.
#:
#: ⚠️ **Electricity is not here.**  ASM reports it separately as ``CSTELEC`` and
#: it is a different kind; realigning that row is #898.
CARRIER_TO_BEA = {
    'Natural Gas': '221200',
    'Distillate Fuel Oil': '324110',
    'Residual Fuel Oil': '324110',
    'Hydrocarbon Gas Liquids, excluding natural gasoline': '324110',
    'Coal': '212100',
    'Coke and Breeze': '212100',
}

#: The BEA commodities ``CSTFU`` covers, read from the seed's own mapping so the
#: two cannot drift apart.
FUEL_GROUP = EXPENSE_TO_BEA['CSTFU']

#: Table names, so a rename upstream fails loudly here rather than silently
#: returning an empty frame.
FUEL_QUANTITY, NONFUEL_QUANTITY = 'Table 3.2', 'Table 2.2'
PRICE, EXPENDITURE = 'Table 7.2', 'Table 7.10'

#: MECS reports the national total under this ``Location``.
NATIONAL = '00000'

#: ⚠️ A share that was zero in 2018 has no movement to carry, and dividing by it
#: is an infinity rather than a large number.  Those cells hold at 1.0.  The
#: index is capped as well: the largest uncapped value observed is on cells
#: carrying a fraction of a percent of the group, where MECS's own suppression
#: dominates the signal.
_ZERO_SHARE = 1e-9
INDEX_CAP = 5.0

#: ⚠️ **A carrier that is a tenth of the bill in one vintage and *exactly* zero
#: in the other is withheld, not abandoned.**  The price fallback in
#: :func:`_published_price` cannot reach this case, because here it is the
#: Table 3.2 *quantity* that is suppressed: beverages (``3121``) publish 50
#: trillion Btu of natural gas in 2018 and 0 in 2022, which read as a complete
#: switch off gas at a ``switch`` score of 0.928.
#:
#: ✅ **Those columns hold rather than move.**  Withholding and substitution are
#: indistinguishable from the published cell, so the honest response is to carry
#: no movement rather than to carry a fabricated one.  The threshold is
#: deliberately not near zero: a carrier genuinely appearing or disappearing at
#: the margin is ordinary, and :func:`unreliable_columns` reports what it caught
#: so the cost is visible.
WITHHELD_SHARE = 0.10


@functools.cache
def _mecs(year: int) -> pd.DataFrame:
    """The national MECS frame for a vintage, with the columns this needs."""
    if year not in MECS_VINTAGES:
        raise ValueError(
            f'MECS has no {year} vintage with Tables 7.2 and 7.10; '
            f'the fetched vintages are {list(MECS_VINTAGES)}.'
        )
    fba = getFlowByActivity('EIA_MECS_Energy', year)
    frame = fba[fba['Location'].astype(str) == NATIONAL].copy()
    frame['naics'] = frame['ActivityConsumedBy'].astype(str)
    frame['carrier'] = frame['FlowName'].astype(str)
    frame['table'] = frame['Description'].astype(str)
    present = set(frame['table'])
    missing = {FUEL_QUANTITY, PRICE, EXPENDITURE} - present
    if missing:
        raise ValueError(
            f'MECS {year} is missing {sorted(missing)}; it carries '
            f'{sorted(present)}. Regenerate the FBA -- #895 uncommented these '
            f'from the yaml and the parquet is cached on the git hash.'
        )
    return frame


def _cell(year: int, table: str) -> pd.Series:
    """``(naics, carrier) -> FlowAmount`` for one MECS table."""
    frame = _mecs(year)
    return frame[frame['table'] == table].set_index(['naics', 'carrier'])['FlowAmount']


def _price_parents(naics: str) -> list[str]:
    """Where to look for a withheld price, nearest first, ending at all of 31-33."""
    return [naics[:n] for n in (4, 3) if len(naics) > n] + ['31-33']


def _published_price(price: 'pd.Series[float]', naics: str, carrier: str) -> float:
    """This industry's price for a carrier, or the nearest parent's if withheld.

    ⚠️ **MECS publishes a withheld cell as ``0``, and a zero price silently
    zeroes the whole carrier.**  322110 pulp mills is the case that found this:
    its natural gas *quantity* rises from 40 to 55 trillion Btu between the two
    vintages while its 2022 gas *price* cell is withheld, so pricing it as
    published reported gas going to zero -- and the industry then read as having
    switched completely off gas, at a ``switch`` score of 0.784.

    ✅ **A positive quantity at a zero price is withholding, not a free fuel.**
    Every carrier mapped in :data:`CARRIER_TO_BEA` is one a plant buys; MECS
    books the genuinely self-produced fuels -- blast furnace gas, coke oven gas,
    black liquor -- on Table 3.2's ``Other`` line, which is not mapped here and
    so never reaches this function.

    The fallback is the same device :func:`~.inputs_structure._recover_from_
    published_parent` uses for suppressed census cells: take the nearest level
    that did publish, rather than dropping the cell or inventing a number.
    """
    own = price.get((naics, carrier))
    if own is not None and not pd.isna(own) and float(own) > 0.0:
        return float(own)
    for parent in _price_parents(naics):
        up = price.get((parent, carrier))
        if up is not None and not pd.isna(up) and float(up) > 0.0:
            return float(up)
    return 0.0


def mecs_fuel_expenditure(year: int, nonfuel: bool = False) -> pd.DataFrame:
    """Expenditure by MECS industry and carrier, in million USD.

    ``naics x carrier``, derived as Table 7.2 price times Table 3.2 quantity --
    USD per million Btu times trillion Btu, which is million USD with no
    conversion.  :func:`expenditure_validation` is the check.

    ``nonfuel`` prices Table 2.2 feedstock quantities instead, which is not part
    of the ``CSTFU`` bucket and exists here only so the validation can add the
    two halves back together.

    ⚠️ **A withheld price is recovered from the nearest published parent**, not
    read as a free fuel; see :func:`_published_price` for the pulp-mill case
    that makes this necessary rather than tidy.
    """
    quantity = _cell(year, NONFUEL_QUANTITY if nonfuel else FUEL_QUANTITY)
    price = _cell(year, PRICE)
    records = []
    for key, amount in quantity.items():
        naics, carrier = str(key[0]), str(key[1])  # type: ignore[index]
        priced_as = TABLE_7_2_PRICE.get(carrier)
        if priced_as is None or pd.isna(amount) or float(amount) <= 0.0:
            continue
        rate = _published_price(price, naics, priced_as)
        if rate <= 0.0:
            continue
        records.append(
            {
                'naics': naics,
                'carrier': carrier,
                'million_usd': float(amount) * rate,
            }
        )
    if not records:
        return pd.DataFrame()
    return (
        pd.DataFrame(records)
        .groupby(['naics', 'carrier'])['million_usd']
        .sum()
        .unstack('carrier')
        .fillna(0.0)
    )


def _scalar(spend: pd.DataFrame, carrier: str) -> float:
    """Manufacturing's total for one carrier, or 0.0 if the table lacks it."""
    if spend.empty or carrier not in spend.columns or '31-33' not in spend.index:
        return 0.0
    return float(spend.loc[['31-33'], carrier].sum())


def expenditure_validation() -> pd.DataFrame:
    """Derived expenditure against Table 7.10, which the derivation never reads.

    ✅ **Gas reconciles to 1.003 and 1.018** once Table 2.2 feedstock is added to
    Table 3.2 fuel, because 7.10 prices every purchased therm and 3.2 is only
    the fuel half.

    ✅ **Electricity holds at 0.976 and 0.971 with no feedstock term at all**,
    which is the other scope difference: 3.2 publishes electricity *net* of what
    the establishment sells back, against 7.10's gross purchases.

    Steam is the third carrier 7.10 covers and has no 3.2 quantity, so it is not
    derivable and is left out rather than reported as zero.
    """
    records = []
    for year in MECS_VINTAGES:
        fuel = mecs_fuel_expenditure(year)
        feed = mecs_fuel_expenditure(year, nonfuel=True)
        published = _cell(year, EXPENDITURE)
        for carrier, as_published in (
            ('Natural Gas', 'Natural Gas Total'),
            ('Net Electricity', 'Electricity Total'),
        ):
            raw = published.get(('31-33', as_published))
            if raw is None or pd.isna(raw):
                continue
            want = float(raw)
            if want == 0.0:
                continue
            burned = _scalar(fuel, carrier)
            stock = _scalar(feed, carrier)
            records.append(
                {
                    'year': year,
                    'carrier': carrier,
                    'fuel_$M': burned,
                    'feedstock_$M': stock,
                    'derived_$M': burned + stock,
                    'table_7.10_$M': want,
                    'ratio': (burned + stock) / want,
                }
            )
    return pd.DataFrame(records).set_index(['year', 'carrier'])


def _deepest_mecs_row(table: pd.DataFrame, bea_industry: str) -> pd.Series | None:
    """The finest MECS row whose NAICS prefixes this BEA column's digits.

    ⚠️ **This direction is the point** -- see the module docstring.  Reading each
    MECS row onto one BEA column through the NAICS crosswalk covers 37 of 232
    columns; reading each BEA column back to its MECS parent covers all 232.
    """
    digits = ''.join(ch for ch in str(bea_industry) if ch.isdigit())
    for length in (6, 5, 4, 3):
        prefix = digits[:length]
        if prefix in table.index:
            found = table.loc[prefix]
            return found.iloc[0] if isinstance(found, pd.DataFrame) else found
    return None


def fuel_mix_shares(year: int) -> pd.DataFrame:
    """The fuel bill's split across :data:`FUEL_GROUP`, by BEA detail industry.

    ``commodity x BEA manufacturing industry``, each column summing to 1.0 --
    the same orientation as the Use table's column shares, so
    :func:`~.inputs_structure.interpolate_shares` applies unchanged.

    ⚠️ **Shares, deliberately, not levels.**  The level disagrees with BEA's
    group by a factor of four on gas, for the scope reason the module docstring
    sets out, and the ratio in :func:`fuel_mix_index` is what cancels it.
    """
    spend = mecs_fuel_expenditure(year)
    routed = pd.DataFrame(
        {
            commodity: spend[
                [c for c in spend.columns if CARRIER_TO_BEA.get(c) == commodity]
            ].sum(axis=1)
            for commodity in FUEL_GROUP
        }
    )
    columns = {}
    for industry in _manufacturing_bea_industries():
        row = _deepest_mecs_row(routed, industry)
        if row is None or float(row.sum()) <= 0.0:
            continue
        columns[industry] = row / float(row.sum())
    return pd.DataFrame(columns).reindex(index=list(FUEL_GROUP)).fillna(0.0)


def fuel_mix_index(year: int, form: str = SHIPPED_FORM) -> pd.DataFrame:
    """How far each column's fuel split has moved from its 2018 MECS base.

    ``commodity x industry``, 1.0 meaning no movement.  Interpolated between the
    two MECS vintages on :data:`~.inputs_structure.SHIPPED_FORM` and held
    outside them -- geometric between observations and flat past the last, which
    is the form :func:`~.inputs_structure.interior_form_holdout` already scored
    on the benchmark panel rather than a fresh choice here.

    ⚠️ **1.0 at 2017 and at 2018 both.**  An index against 2018 is 1.0 at 2018 by
    construction, and 2017 holds it, so the published benchmark cross-section is
    untouched and the adjustment grows toward 2022.  Movement is what is
    observed; it is zero at the base.

    ⚠️ **A commodity with no 2018 share holds at 1.0.**  There is no movement to
    measure from zero, and the ratio would be an infinity rather than a large
    number.  The index is capped at :data:`INDEX_CAP` for the same reason in
    softer form -- a share of a fraction of a percent moving is MECS suppression
    as often as it is substitution.
    """
    base = fuel_mix_shares(MECS_BASE)
    end = fuel_mix_shares(MECS_VINTAGES[-1])
    shared = [c for c in base.columns if c in end.columns]
    base, end = base[shared], end[shared]

    span = MECS_VINTAGES[-1] - MECS_BASE
    t = float(np.clip((year - MECS_BASE) / span, 0.0, 1.0))
    moved = interpolate_shares(base, end, t, form=form)

    index = moved.div(base.where(base > _ZERO_SHARE))
    index = index.fillna(1.0).clip(upper=INDEX_CAP)
    held = unreliable_columns(base, end)
    index[held] = 1.0
    return index


def unreliable_columns(
    base: pd.DataFrame | None = None, end: pd.DataFrame | None = None
) -> list[str]:
    """Columns where a carrier collapsed to or appeared from *exactly* zero.

    See :data:`WITHHELD_SHARE`.  MECS suppression is published as ``0``, so a
    carrier worth a tenth of the bill in one vintage and exactly nothing in the
    other is a withheld cell far more often than a plant that re-piped, and the
    two cannot be told apart from the published table.  Those columns carry no
    movement at all rather than a fabricated one.
    """
    if base is None or end is None:
        base = fuel_mix_shares(MECS_BASE)
        end = fuel_mix_shares(MECS_VINTAGES[-1])
        shared = [c for c in base.columns if c in end.columns]
        base, end = base[shared], end[shared]
    vanished = (base >= WITHHELD_SHARE) & (end <= _ZERO_SHARE)
    appeared = (end >= WITHHELD_SHARE) & (base <= _ZERO_SHARE)
    suspect = vanished | appeared
    return [str(c) for c in suspect.columns[suspect.any(axis=0)]]


def fuel_split_weights(year: int, form: str = SHIPPED_FORM) -> pd.DataFrame:
    """BEA's 2017 fuel cells, reweighted by MECS movement, group total intact.

    ``commodity x industry`` in $M.  This is what replaces ``Use2017[c, i]``
    inside :func:`~.inputs_structure.nonmaterial_seed` for the ``CSTFU`` kind --
    each column still carries exactly the dollars BEA gave it, divided the way
    MECS says it has come to divide.

    ✅ **Every column's group total is preserved exactly**, so the survey index
    that moves the bucket and the MECS index that splits it are independent and
    neither can overwrite the other.
    """
    use = _use_2017_detail()
    rows = [c for c in FUEL_GROUP if c in use.index]
    man = _manufacturing_bea_industries()
    base = use.loc[rows, man]

    weighted = base.mul(fuel_mix_index(year, form=form).reindex_like(base).fillna(1.0))
    target, actual = base.sum(axis=0), weighted.sum(axis=0)
    scale = (target / actual.where(actual != 0)).fillna(1.0)
    return weighted.mul(scale, axis=1)


def mix_movement(form: str = SHIPPED_FORM) -> pd.DataFrame:
    """What the reweighting does to the group, per year, against frozen BEA.

    The size of the correction, measured rather than asserted -- the counterpart
    of :func:`~.inputs_structure.nonmaterial_movement` for the split inside the
    bucket.  ``dissimilarity`` is half the summed absolute share change,
    weighted by each column's 2017 group dollars.
    """
    use = _use_2017_detail()
    rows = [c for c in FUEL_GROUP if c in use.index]
    man = _manufacturing_bea_industries()
    base = use.loc[rows, man]
    weights = base.sum(axis=0)
    frozen = base.div(weights.where(weights != 0), axis=1).fillna(0.0)

    records = []
    for year in range(2017, 2025):
        split = fuel_split_weights(year, form=form)
        shares = split.div(weights.where(weights != 0), axis=1).fillna(0.0)
        per_column = (shares - frozen).abs().sum(axis=0) / 2.0
        records.append(
            {
                'year': year,
                'weighted_dissimilarity': float(
                    (per_column * weights).sum() / weights.sum()
                ),
                'median_column': float(per_column.median()),
                'columns_over_0.10': int((per_column > 0.10).sum()),
                'gas_share_%': 100 * float(split.loc['221200'].sum() / weights.sum()),
                'petroleum_share_%': 100
                * float(split.loc['324110'].sum() / weights.sum()),
                'coal_share_%': 100 * float(split.loc['212100'].sum() / weights.sum()),
            }
        )
    return pd.DataFrame(records).set_index('year')


def coverage() -> pd.DataFrame:
    """How much of the BEA group MECS reaches, and at what NAICS depth."""
    use = _use_2017_detail()
    rows = [c for c in FUEL_GROUP if c in use.index]
    man = _manufacturing_bea_industries()
    dollars = use.loc[rows, man].sum(axis=0)

    records = []
    for year in MECS_VINTAGES:
        spend = mecs_fuel_expenditure(year)
        routed = pd.DataFrame(
            {
                commodity: spend[
                    [c for c in spend.columns if CARRIER_TO_BEA.get(c) == commodity]
                ].sum(axis=1)
                for commodity in FUEL_GROUP
            }
        )
        depths = {}
        for industry in man:
            digits = ''.join(ch for ch in str(industry) if ch.isdigit())
            for length in (6, 5, 4, 3):
                if digits[:length] in routed.index:
                    depths[industry] = length
                    break
        reached = pd.Index(list(depths))
        records.append(
            {
                'year': year,
                'mecs_rows': int(len(routed)),
                'columns_reached': len(reached),
                'columns_total': len(man),
                'group_$B_reached': float(dollars.reindex(reached).sum()) / 1000.0,
                'group_$B_total': float(dollars.sum()) / 1000.0,
                'matched_at_6': sum(1 for d in depths.values() if d == 6),
                'matched_at_4': sum(1 for d in depths.values() if d in (4, 5)),
                'matched_at_3': sum(1 for d in depths.values() if d == 3),
            }
        )
    return pd.DataFrame(records).set_index('year')


def aggregate_bill() -> pd.DataFrame:
    """Manufacturing's fuel bill by commodity, from MECS's three-digit rows.

    ⚠️ **Three-digit rows only, because they partition manufacturing exactly.**
    Summing every published row double-counts each parent against its children,
    and summing :func:`fuel_mix_shares` double-counts a three-digit row against
    every BEA column it was broadcast to.  Neither is the aggregate.
    """
    records = {}
    for year in MECS_VINTAGES:
        spend = mecs_fuel_expenditure(year)
        three = spend.loc[
            [
                n
                for n in spend.index
                if len(n) == 3 and n.isdigit() and n[:2] in ('31', '32', '33')
            ]
        ]
        records[year] = pd.Series(
            {
                commodity: float(
                    three[
                        [c for c in three.columns if CARRIER_TO_BEA.get(c) == commodity]
                    ]
                    .sum()
                    .sum()
                )
                for commodity in FUEL_GROUP
            }
        )
    table = pd.DataFrame(records)
    table.loc['total'] = table.sum()
    for year in MECS_VINTAGES:
        table[f'{year}_%'] = 100 * table[year] / table.loc['total', year]
    return table


def switching() -> pd.DataFrame:
    """Which industries switched carrier between 2018 and 2022, and which did not.

    One row per **MECS industry**, not per BEA column, because a three-digit
    MECS row is broadcast to every BEA column beneath it and those columns are
    not independent observations.  BEA's group dollars are summed back onto the
    MECS row that serves them, so ``bea_$M`` and the MECS fuel bill are
    comparable on the same universe.

    ``switch`` is half the summed absolute share change -- 0 for an industry
    that burns the same mix in both vintages, 1 for one that changed carrier
    completely.

    ``bea_over_mecs`` is BEA's group dollars divided by MECS's fuel bill.  A
    ratio near 1 means BEA's three rows are roughly the fuel this industry
    burns.  A large ratio means the two are measuring different things.

    ❌ **A large ratio does NOT by itself mean "BEA's rows are not fuel", and
    reading it that way was wrong** (Wes, 2026-09-24).  It has at least three
    causes and the numerator is only one of them:

    * **BEA's rows carry non-fuel.**  ``324122`` asphalt shingle at 28.3 is the
      real case -- asphalt bought as a material, which ``CSTFU`` excludes.
    * **MECS's denominator is rounded away.**  ⚠️ See ``mecs_tbtu`` and
      ``rounded_carriers``.
    * **BEA allocates rather than observes** in a small industry.

    ⚠️ **Table 3.2 publishes whole trillion Btu, and 100% of its positive
    quantities are integers.**  Any carrier under 0.5 trillion Btu reads as 0.
    Tobacco (``3122``) is the case that exposed this: its whole energy use is 7
    trillion Btu, only gas and electricity survive rounding, and yet MECS
    publishes 3122 *prices* for distillate, residual, HGL, kerosene and motor
    gasoline -- a price exists only where there is a purchase to price.  So its
    7.3 ratio is a rounded-down denominator, not a non-fuel numerator.  There is
    no finer source: Table 3.1's physical units are whole numbers too.

    ✅ **The exposure is small and it is measured, not assumed.**  Only 3 of 232
    BEA columns sit on a MECS row under 10 trillion Btu, carrying $0.16B of the
    group's $58.6B (**0.3%**), and all three score ``switch`` 0.000 so nothing
    moves on them.  95.6% of the dollars sit above 50 trillion Btu, where half a
    trillion Btu is under 1%.  ``WITHHELD_SHARE`` catches the damaging case
    anyway -- a carrier rounding to zero out of a small total crosses 10% of the
    bill and the column is held.

    ⚠️ **Stillness has several causes and this cannot separate them alone.**  An
    industry can hold its mix because its equipment takes one carrier, because
    what it buys is not burned, or because MECS cannot resolve it.  Read
    ``bea_over_mecs`` with ``mecs_tbtu`` and ``rounded_carriers``, and against
    what the industry actually is.
    """
    use = _use_2017_detail()
    rows = [c for c in FUEL_GROUP if c in use.index]
    man = _manufacturing_bea_industries()
    dollars = use.loc[rows, man].sum(axis=0)

    spend = {year: mecs_fuel_expenditure(year) for year in MECS_VINTAGES}
    routed = {
        year: pd.DataFrame(
            {
                commodity: table[
                    [c for c in table.columns if CARRIER_TO_BEA.get(c) == commodity]
                ].sum(axis=1)
                for commodity in FUEL_GROUP
            }
        )
        for year, table in spend.items()
    }
    base_rows = routed[MECS_BASE]
    # ⚠️ Resolution, so a large ``bea_over_mecs`` can be read for its cause.
    # ``Total`` is the industry's whole energy use; a carrier that MECS prices
    # but reports at zero trillion Btu is a purchase rounded away, not absent.
    quantity = _cell(MECS_BASE, FUEL_QUANTITY)
    price = _cell(MECS_BASE, PRICE)
    energy = quantity.xs('Total', level='carrier')
    rounded: dict[str, int] = {}
    for naics in energy.index:
        rounded[str(naics)] = sum(
            1
            for carrier, priced_as in TABLE_7_2_PRICE.items()
            if float(quantity.get((naics, carrier), 0.0) or 0.0) == 0.0
            and float(price.get((naics, priced_as), 0.0) or 0.0) > 0.0
        )

    matched: dict[str, list[str]] = {}
    for industry in man:
        digits = ''.join(ch for ch in str(industry) if ch.isdigit())
        for length in (6, 5, 4, 3):
            if digits[:length] in base_rows.index:
                matched.setdefault(digits[:length], []).append(industry)
                break

    records = []
    for naics, columns in matched.items():
        early = base_rows.loc[naics]
        late = routed[MECS_VINTAGES[-1]].reindex(index=[naics]).iloc[0].fillna(0.0)
        if float(early.sum()) <= 0 or float(late.sum()) <= 0:
            continue
        share_early = early / float(early.sum())
        share_late = late / float(late.sum())
        bea = float(dollars.reindex(columns).sum())
        records.append(
            {
                'mecs_naics': naics,
                'depth': len(naics),
                'bea_columns': len(columns),
                'bea_$M': bea,
                'mecs_fuel_$M': float(early.sum()),
                'bea_over_mecs': bea / float(early.sum()),
                'mecs_tbtu': float(energy.get(naics, float('nan'))),
                'rounded_carriers': rounded.get(naics, 0),
                'switch': float((share_late - share_early).abs().to_numpy().sum())
                / 2.0,
                'gas_18': float(share_early['221200']),
                'gas_22': float(share_late['221200']),
                'pet_18': float(share_early['324110']),
                'pet_22': float(share_late['324110']),
                'coal_18': float(share_early['212100']),
                'coal_22': float(share_late['212100']),
            }
        )
    table = pd.DataFrame(records).set_index('mecs_naics')
    return table.sort_values('bea_$M', ascending=False)


def invariants() -> pd.DataFrame:
    """The two properties the wiring depends on, asserted rather than described.

    ✅ **The group total is preserved exactly, every year.**  The survey index in
    :func:`~.inputs_structure.nonmaterial_seed` sets what the fuels bucket is
    worth and MECS sets only how it divides, so neither can overwrite the other.
    If this drifts, the ``CSTFU`` level is silently coming from MECS.

    ✅ **2017 and 2018 are untouched.**  The index is taken against the 2018 MECS
    base, so the published benchmark cross-section survives and the first year
    that moves is 2019.
    """
    use = _use_2017_detail()
    rows = [c for c in FUEL_GROUP if c in use.index]
    base = use.loc[rows, _manufacturing_bea_industries()]
    records = []
    for year in range(2017, 2025):
        split = fuel_split_weights(year)
        drift = float((split.sum(axis=0) - base.sum(axis=0)).abs().max())
        change = float((split - base).abs().to_numpy().max())
        records.append(
            {
                'year': year,
                'max_column_total_drift_$M': drift,
                'max_cell_change_$M': change,
                'total_preserved': drift < 1e-6,
                'base_year_untouched': change < 1e-6,
            }
        )
    return pd.DataFrame(records).set_index('year')


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--validate', action='store_true', help='the 7.10 check')
    parser.add_argument('--coverage', action='store_true', help='columns reached')
    parser.add_argument('--bill', action='store_true', help='aggregate fuel bill')
    parser.add_argument('--movement', action='store_true', help='what it moves')
    parser.add_argument('--check', action='store_true', help='assert the invariants')
    parser.add_argument('--switching', action='store_true', help='who switched')
    parser.add_argument('--all', action='store_true')
    args = parser.parse_args()
    chosen = (
        args.validate
        or args.coverage
        or args.bill
        or args.movement
        or args.check
        or args.switching
    )

    if args.all or args.validate or not chosen:
        print('\nDerived expenditure against Table 7.10, which it never reads\n')
        print(expenditure_validation().round(3).to_string())
        print(
            '\n  gas reconciles once Table 2.2 feedstock is added back -- 7.10'
            '\n  prices every purchased therm and 3.2 is the fuel half alone.'
            '\n  electricity has no feedstock term; its 0.97 is 3.2 reporting'
            '\n  electricity net of what the plant sells back.'
        )
    if args.all or args.coverage or not chosen:
        print('\nHow much of the BEA group MECS reaches\n')
        print(coverage().round(1).to_string())
        print(
            '\n  matching BEA column -> MECS row covers all 232. Matching each'
            '\n  MECS row to one BEA column through the crosswalk covers 37.'
        )
    if args.all or args.bill or not chosen:
        print("\nManufacturing's fuel bill, MECS three-digit rows ($M)\n")
        print(aggregate_bill().round(1).to_string())
        print(
            '\n  coal falls by a third of its share and gas takes it. This is'
            '\n  the movement BEA-frozen splits cannot see.'
        )
    if args.all or args.movement or not chosen:
        print('\nWhat the reweighting does to the group, against frozen BEA\n')
        print(mix_movement().round(4).to_string())
        print(
            '\n  1.0 at 2017 and 2018 by construction: the index is taken'
            '\n  against the 2018 MECS base, so the benchmark cross-section is'
            '\n  untouched and the adjustment grows toward 2022.'
        )
    if args.all or args.switching or not chosen:
        table = switching()
        moved = table[table['switch'] > 0.05].sort_values('switch', ascending=False)
        still = table[table['switch'] <= 0.02].sort_values('bea_$M', ascending=False)
        print('\nSwitched carrier 2018 -> 2022 (switch > 0.05)\n')
        print(moved.round(3).to_string())
        print('\nHeld their mix (switch <= 0.02)\n')
        print(still.round(3).to_string())
        print(
            '\n  bea_over_mecs near 1 means BEA three rows are roughly what'
            '\n  this industry burns. LARGE HAS THREE CAUSES, not one: BEA'
            '\n  rows carrying non-fuel (324122 asphalt, 28.3), a denominator'
            '\n  rounded away (3122 tobacco, 7 TBtu total), or BEA allocating'
            '\n  rather than observing. Read it with mecs_tbtu and'
            '\n  rounded_carriers -- Table 3.2 publishes whole trillion Btu,'
            '\n  so a carrier under 0.5 reads as zero while MECS still'
            '\n  prices it.'
        )
    if args.all or args.check or not chosen:
        print('\nInvariants the wiring depends on\n')
        table = invariants()
        print(table.round(9).to_string())
        base_years = table.loc[[2017, 2018], 'base_year_untouched']
        if not bool(table['total_preserved'].all()):
            raise SystemExit('FAIL: a column total moved -- MECS is setting a level')
        if not bool(base_years.all()):
            raise SystemExit('FAIL: 2017 or 2018 moved -- the index base is wrong')
        print('\n  PASS. every column total preserved, 2017 and 2018 untouched.')


if __name__ == '__main__':
    main()
