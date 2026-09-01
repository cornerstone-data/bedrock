"""Does the seller matrix beat economy-wide shares at placing the trade margin?

The producer-price MUT books each buyer's trade margin on the **19 detail trade
commodity rows** - which wholesaler or retailer earned it. Converting the SUT
has to reproduce that, and the open question is what splits a buyer's margin
across those 19 rows.

Two candidate rules, graded here against the published 2017 answer:

``matrix``
    ``bea_trade_matrix`` column shares - which sellers sell this product,
    observed from Economic Census product lines.
``fallback``
    each trade code's economy-wide share of its kind's give-up, applied to every
    commodity alike.

The matrix earns its place in the conversion only by beating the fallback. If it
does not, the conversion uses the simple rule and this told us cheaply.

What is actually under test
---------------------------

The matrix is a **commodity-level** object, so using it assumes **buyer
invariance**: every buyer of a product draws its margin from the same seller
mix. That is the assumption the buyer-distribution grade measures, and it is the
reason a row-total grade alone would flatter both rules.

⚠️ The matrix observes *sales* composition while margin *rates* differ by
product, so a miss points at margin-rate-weighted shares as the refinement
rather than at discarding the matrix.

Scope
-----

**Intermediate block only.** The answer key is
``load_2017_Utot_before_redef_usa`` (commodity x industry), so this grades the
986,763 $M of trade margin that industry buyers pay - **27% of the 3,656,094 $M
in the Margins table**. The rest is final demand, PCE above all, and needs the
final-demand block to score.

⚠️ One consequence: the "row totals recover the Supply give-up per giver" check
cannot run here. Give-up is economy-wide (3,264,931 $M on the 19) while this
sees only the intermediate slice, so the row-total grade below is against the
observed MUT rows, not against give-up.

The target
----------

``MUT - SUT`` on the 19 trade rows, not the MUT rows themselves: the purchaser
SUT already carries direct purchases of trade services (``425000`` 33,045 $M,
``423A00`` 4,436 $M), which are not margin and which both rules would be graded
on unfairly. The difference is the margin added, and it agrees with the Margins
table's industry-buyer total to **0.06%** - 987,389 $M against 986,763 $M.

Run::

    uv run python -m bedrock.analysis.nowcasting.seller_matrix_grading
"""

from __future__ import annotations

import argparse
import sys
import typing as ta

import numpy as np
import pandas as pd

from bedrock.extract.iot.io_2017 import (
    _load_2017_detail_supply_use_usa,
    load_2017_Utot_before_redef_usa,
)
from bedrock.transform.inventories.seller_matrix import bea_trade_matrix
from bedrock.transform.iot.nowcast_margins import load_margins_transactions_2017
from bedrock.transform.iot.nowcast_trade_margins import (
    GIVER_COMMODITIES,
    published_trade_by_commodity,
)
from bedrock.transform.iot.nowcast_transport_margins import (
    MODE_COMMODITIES,
    joint_mode_shares,
    mode_allocations,
    mode_control_total,
)
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES
from bedrock.utils.validation.exceptions import APIError

ANCHOR_YEAR = 2017

#: House thresholds, as everywhere on this path.
RTOL = 0.01
ATOL = 0.5 * MILLION_CURRENCY_TO_CURRENCY

#: The Margins table's column per kind of trade margin.
MARGIN_COLUMN = {'wholesale': 'Wholesale', 'retail': 'Retail'}

#: What a missing ``Census_EC_PxI`` artifact raises. There is no GCS fallback
#: for an FBA - the load order is the local cache, then generation from the
#: live source - so without a Census key the matrix arm cannot run while the
#: fallback arm still can. The report degrades rather than crashing.
MISSING_ARTIFACT = (APIError, FileNotFoundError)

BUYER_LEVEL = 'Industry Code'
COMMODITY_LEVEL = 'Commodity Code'


class Grade(ta.NamedTuple):
    """How well one rule places the margin, against the published MUT."""

    name: str
    candidate: pd.DataFrame
    observed: pd.DataFrame

    @property
    def diff(self) -> pd.DataFrame:
        return self.candidate - self.observed

    @property
    def gross(self) -> float:
        """Total absolute misplacement, USD."""
        return float(self.diff.abs().to_numpy().sum())

    @property
    def n_outside(self) -> int:
        close = np.isclose(
            self.candidate.to_numpy(), self.observed.to_numpy(), rtol=RTOL, atol=ATOL
        )
        return int((~close).sum())

    @property
    def row_error(self) -> pd.Series:
        """Per trade code, candidate row total minus observed, USD."""
        return self.candidate.sum(axis=1) - self.observed.sum(axis=1)

    def buyer_dissimilarity(self) -> pd.Series:
        """Per trade code, the share of the row's dollars on the wrong buyer.

        The index of dissimilarity between the candidate's and the observed
        row's buyer distributions - scale-free, so a trade code whose total is
        right but whose buyers are wrong scores badly here and nowhere else.
        **This is the buyer-invariance test.**
        """
        out = {}
        for code in self.observed.index:
            obs = self.observed.loc[code]
            cand = self.candidate.loc[code]
            obs_total, cand_total = float(obs.sum()), float(cand.sum())
            if obs_total <= 0 or cand_total <= 0:
                continue
            out[code] = float((obs / obs_total - cand / cand_total).abs().sum() / 2)
        return pd.Series(out).sort_values(ascending=False)

    def weighted_dissimilarity(self) -> float:
        """Dollar-weighted mean of :meth:`buyer_dissimilarity`."""
        per_row = self.buyer_dissimilarity()
        weight = self.observed.loc[per_row.index].sum(axis=1)
        return float((per_row * weight).sum() / weight.sum())


def industry_margin(kind: str) -> pd.DataFrame:
    """Buyer x commodity margin of *kind*, industry buyers only, USD."""
    margins = load_margins_transactions_2017()
    industries = set(USA_2017_INDUSTRY_CODES)
    buyers = margins.index.get_level_values(BUYER_LEVEL)
    block = margins.loc[buyers.isin(industries), MARGIN_COLUMN[kind]]
    return block.unstack(COMMODITY_LEVEL).fillna(0.0)


def observed_margin(rows: ta.Sequence[str] | None = None) -> pd.DataFrame:
    """Giver row x industry buyer, the margin the published MUT actually books.

    ``MUT - SUT`` on the giver rows - see the module docstring on why the
    difference rather than the MUT row. Defaults to the 19 trade codes; pass
    :data:`TRANSPORT_ROWS` for the transport side.
    """
    published = load_2017_Utot_before_redef_usa()
    sut = _load_2017_detail_supply_use_usa('Use_SUT_detail')
    purchaser = (
        sut.loc[list(USA_2017_COMMODITY_CODES), list(USA_2017_INDUSTRY_CODES)].astype(
            float
        )
        * MILLION_CURRENCY_TO_CURRENCY
    )
    purchaser.index, purchaser.columns = published.index, published.columns
    added = published - purchaser
    added.index = [str(code) for code in added.index]
    added.columns = [str(code) for code in added.columns]
    return added.loc[list(rows) if rows is not None else _all_givers()]


def matrix_shares(kind: str, year: int = ANCHOR_YEAR) -> pd.DataFrame:
    """Trade code x commodity shares of *kind*, from the seller matrix.

    Each commodity's column is restricted to the kind's giver codes and
    renormalised, so wholesale margin can only reach wholesalers. Commodities
    the matrix does not cover come back all-zero and the caller substitutes the
    fallback - :func:`uncovered_commodities` counts them.
    """
    givers = list(GIVER_COMMODITIES[kind])
    matrix = bea_trade_matrix(year).reindex(index=givers).fillna(0.0)
    totals = matrix.sum(axis=0)
    shares = matrix.div(totals.where(totals > 0, 1.0), axis=1)
    # ⚠️ Zero the uncovered columns by label. ``DataFrame.where`` with a Series
    # condition aligns on the *index*, so ``.where(totals > 0, 0.0)`` matches a
    # commodity-indexed mask against trade-code rows, finds nothing, and blanks
    # the whole frame - every commodity then reads as uncovered and the matrix
    # arm silently becomes the fallback.
    shares.loc[:, totals <= 0] = 0.0
    return shares


def fallback_shares(kind: str) -> pd.Series:
    """Each giver's economy-wide share of its kind's published give-up."""
    givers = list(GIVER_COMMODITIES[kind])
    giveup = -published_trade_by_commodity().reindex(givers).astype(float)
    return giveup / giveup.sum()


def distribute(margin: pd.DataFrame, shares: pd.DataFrame) -> pd.DataFrame:
    """Spread a buyer x commodity margin onto trade code x buyer.

    ``candidate[t, b] = sum_c margin[b, c] * shares[t, c]`` - a matrix product,
    so every dollar of margin lands on exactly one row and none is created.
    """
    commodities = [c for c in margin.columns if c in shares.columns]
    return pd.DataFrame(
        shares[commodities].to_numpy() @ margin[commodities].T.to_numpy(),
        index=shares.index,
        columns=margin.index,
    )


def uncovered_commodities(kind: str, year: int = ANCHOR_YEAR) -> pd.Series:
    """Margin of *kind* sitting on commodities the matrix does not cover, USD."""
    shares = matrix_shares(kind, year)
    margin = industry_margin(kind)
    covered = shares.columns[shares.sum(axis=0) > 0]
    missing = [c for c in margin.columns if c not in set(covered)]
    return margin[missing].sum(axis=0).sort_values(ascending=False)


def _all_givers() -> list[str]:
    return list(GIVER_COMMODITIES['wholesale']) + list(GIVER_COMMODITIES['retail'])


def grade(rule: str, year: int = ANCHOR_YEAR) -> Grade:
    """Score *rule* (``matrix`` or ``fallback``) against the published MUT."""
    observed = observed_margin()
    total = pd.DataFrame(0.0, index=observed.index, columns=observed.columns)
    for kind in ('wholesale', 'retail'):
        margin = industry_margin(kind)
        fallback = fallback_shares(kind)
        if rule == 'matrix':
            shares = matrix_shares(kind, year).copy()
            # commodities outside the matrix fall back rather than vanish
            bare = shares.columns[shares.sum(axis=0) <= 0]
            for commodity in bare:
                shares[commodity] = fallback
            missing = [c for c in margin.columns if c not in shares.columns]
            for commodity in missing:
                shares[commodity] = fallback
        elif rule == 'fallback':
            shares = pd.DataFrame(
                {c: fallback for c in margin.columns}, index=fallback.index
            )
        else:
            raise ValueError(f"rule must be 'matrix' or 'fallback', got {rule!r}")
        placed = distribute(margin, shares)
        total = total.add(placed.reindex_like(total).fillna(0.0), fill_value=0.0)
    return Grade(name=rule, candidate=total, observed=observed)


def verdict(year: int = ANCHOR_YEAR) -> pd.DataFrame:
    """One row per rule: how well each places the margin. USD and shares."""
    rows = []
    for rule in ('fallback', 'matrix'):
        scored = grade(rule, year)
        rows.append(
            {
                'rule': rule,
                'gross_musd': scored.gross / MILLION_CURRENCY_TO_CURRENCY,
                'cells_outside': scored.n_outside,
                'buyer_dissimilarity': scored.weighted_dissimilarity(),
                'worst_row_musd': float(
                    scored.row_error.abs().max() / MILLION_CURRENCY_TO_CURRENCY
                ),
            }
        )
    return pd.DataFrame(rows).set_index('rule')


# --- the transport side ----------------------------------------------------

#: The five freight-mode commodity rows, in the mode order the allocations use.
TRANSPORT_ROWS: tuple[str, ...] = tuple(MODE_COMMODITIES[m] for m in MODE_COMMODITIES)


def transport_margin() -> pd.DataFrame:
    """Buyer x commodity transportation margin, industry buyers only, USD."""
    margins = load_margins_transactions_2017()
    industries = set(USA_2017_INDUSTRY_CODES)
    buyers = margins.index.get_level_values(BUYER_LEVEL)
    block = margins.loc[buyers.isin(industries), 'Transportation']
    return block.unstack(COMMODITY_LEVEL).fillna(0.0)


def transport_shares(rule: str, year: int = ANCHOR_YEAR) -> pd.DataFrame:
    """Mode commodity x commodity shares, columns summing to one.

    ``independent``
        each mode's own revenue-controlled allocation - what
        ``transport_mode_matrix`` reads today.
    ``joint``
        the #772 joint fit, whose column margins are the published
        ``Transportation`` column.
    """
    if rule == 'independent':
        levels = pd.DataFrame(mode_allocations(year)).fillna(0.0).T
    elif rule == 'joint':
        within = joint_mode_shares()
        totals = pd.Series(
            {mode: mode_control_total(mode, year) for mode in within.index}
        )
        levels = within.mul(totals, axis=0)
    else:
        raise ValueError(f"rule must be 'independent' or 'joint', got {rule!r}")

    levels.index = [MODE_COMMODITIES[mode] for mode in levels.index]
    totals_by_commodity = levels.sum(axis=0)
    shares = levels.div(totals_by_commodity.where(totals_by_commodity > 0, 1.0), axis=1)
    shares.loc[:, totals_by_commodity <= 0] = 0.0
    return shares


def grade_transport(rule: str, year: int = ANCHOR_YEAR) -> Grade:
    """Score a transport rule against the published MUT's five mode rows."""
    observed = observed_margin(TRANSPORT_ROWS)
    margin = transport_margin()
    shares = (
        transport_shares(rule, year).reindex(index=list(TRANSPORT_ROWS)).fillna(0.0)
    )
    placed = distribute(margin, shares)
    return Grade(
        name=rule,
        candidate=placed.reindex_like(observed).fillna(0.0),
        observed=observed,
    )


def transport_verdict(year: int = ANCHOR_YEAR) -> pd.DataFrame:
    """Both transport rules, graded the same way as the trade rules."""
    rows = []
    for rule in ('independent', 'joint'):
        scored = grade_transport(rule, year)
        rows.append(
            {
                'rule': rule,
                'gross_musd': scored.gross / MILLION_CURRENCY_TO_CURRENCY,
                'cells_outside': scored.n_outside,
                'buyer_dissimilarity': scored.weighted_dissimilarity(),
                'worst_row_musd': float(
                    scored.row_error.abs().max() / MILLION_CURRENCY_TO_CURRENCY
                ),
            }
        )
    return pd.DataFrame(rows).set_index('rule')


# --- report / check --------------------------------------------------------


def report(year: int = ANCHOR_YEAR) -> None:
    """Print both rules' grades and the per-code detail behind them."""
    million = MILLION_CURRENCY_TO_CURRENCY
    observed = observed_margin()
    print(
        f'Trade-margin placement, {year}: {len(observed)} trade rows x '
        f'{observed.shape[1]} industry buyers'
    )
    print(f'  margin to place   {observed.to_numpy().sum() / million:>12,.0f} $M')

    try:
        table = verdict(year)
    except MISSING_ARTIFACT as error:  # pragma: no cover - depends on the cache
        print(f'\n  ⚠️ the matrix arm cannot run: {error}')
        print(
            '  Needs CENSUS_API_KEY in a project-root .env: the FBA has no\n'
            '  GCS fallback, so it is generated from source on first use.'
        )
        scored = grade('fallback', year)
        print(
            f'\n  fallback only: gross {scored.gross / million:,.0f} $M, '
            f'buyer dissimilarity {scored.weighted_dissimilarity():.4f}'
        )
        return

    print('\n  grades (lower is better):')
    print(table.round(4).to_string())

    matrix, fallback = table.loc['matrix'], table.loc['fallback']
    better = matrix['buyer_dissimilarity'] < fallback['buyer_dissimilarity']
    print(
        f'\n  VERDICT: the matrix {"beats" if better else "does NOT beat"} the '
        f'fallback on buyer distribution '
        f'({matrix["buyer_dissimilarity"]:.4f} vs '
        f'{fallback["buyer_dissimilarity"]:.4f})'
    )

    scored = grade('matrix', year)
    print('\n  per trade code - row error and buyer dissimilarity:')
    dissimilarity = scored.buyer_dissimilarity()
    for code in scored.observed.sum(axis=1).sort_values(ascending=False).index:
        print(
            f'    {code:<8} observed '
            f'{scored.observed.loc[code].sum() / million:>10,.0f} $M   '
            f'row err {scored.row_error[code] / million:>+9,.0f} $M   '
            f'dissim {dissimilarity.get(code, float("nan")):.4f}'
        )

    print('\n  margin on commodities the matrix does not cover:')
    for kind in ('wholesale', 'retail'):
        uncovered = uncovered_commodities(kind, year)
        total = industry_margin(kind).to_numpy().sum()
        print(
            f'    {kind:<10} {uncovered.sum() / million:>10,.0f} $M on '
            f'{int((uncovered > 0).sum()):>3} commodities '
            f'({100 * uncovered.sum() / total:.1f}% of the kind)'
        )


def check(year: int = ANCHOR_YEAR) -> int:
    """Assert the grading setup holds, independent of either rule's score."""
    million = MILLION_CURRENCY_TO_CURRENCY
    failures: list[str] = []

    observed = observed_margin()
    if len(observed) != 19:
        failures.append(f'{len(observed)} trade rows, expected 19 givers')

    placed = sum(
        industry_margin(kind).to_numpy().sum() for kind in ('wholesale', 'retail')
    )
    gap = abs(observed.to_numpy().sum() - placed) / placed
    if gap > 0.005:
        failures.append(
            f'the margin to place ({placed / million:,.0f} $M from the Margins '
            f'table) and the margin the MUT books '
            f'({observed.to_numpy().sum() / million:,.0f} $M) differ by '
            f'{gap:.2%}, not the measured 0.06%. One side changed vintage.'
        )

    for kind in ('wholesale', 'retail'):
        shares = fallback_shares(kind)
        if abs(float(shares.sum()) - 1.0) > 1e-9:
            failures.append(f'{kind} fallback shares sum to {shares.sum()}, not 1')
        if (shares < 0).any():
            failures.append(f'{kind} fallback has a negative share')

    # a rule may move margin between rows but never create or destroy it
    scored = grade('fallback', year)
    if abs(scored.candidate.to_numpy().sum() - placed) / placed > 1e-9:
        failures.append(
            'the fallback distribution does not conserve mass; a share column '
            'that does not sum to one loses margin silently'
        )

    if failures:
        print('FAILED:')
        for failure in failures:
            print(f'  - {failure}')
        return 1
    print(
        f'OK: {len(observed)} trade rows, '
        f'{observed.to_numpy().sum() / million:,.0f} $M to place, '
        f'both rules conserve it'
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--year', type=int, default=ANCHOR_YEAR)
    parser.add_argument(
        '--check',
        action='store_true',
        help='assert the grading setup instead of printing the report',
    )
    args = parser.parse_args(argv)
    if args.check:
        return check(args.year)
    report(args.year)
    return 0


if __name__ == '__main__':
    sys.exit(main())
