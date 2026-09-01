"""Tests for the trade-margin placement grader.

The metrics are arithmetic over two small frames, so they are exercised on
hand-checkable inputs. The setup facts - how much margin there is to place, and
that both rules conserve it - are checked against the published tables, because
those are properties of BEA's workbooks rather than of this code.

The matrix arm needs the ``Census_EC_PxI`` FBA and skips, named, when it is not
cached; the fallback arm needs only the Supply table.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import bedrock.analysis.nowcasting.seller_matrix_grading as grading
from bedrock.analysis.nowcasting.seller_matrix_grading import (
    MISSING_ARTIFACT,
    Grade,
    distribute,
    fallback_shares,
    grade,
    industry_margin,
    matrix_shares,
    observed_margin,
)
from bedrock.transform.iot.nowcast_trade_margins import GIVER_COMMODITIES
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY

MILLION = MILLION_CURRENCY_TO_CURRENCY


def graded(
    observed: dict[str, dict[str, float]], candidate: dict[str, dict[str, float]]
) -> Grade:
    """A Grade over two ``{trade code: {buyer: value}}`` frames."""
    obs = pd.DataFrame(observed).T.astype(float)
    cand = pd.DataFrame(candidate).T.astype(float).reindex_like(obs).fillna(0.0)
    return Grade(name='test', candidate=cand, observed=obs)


# --- the metrics -----------------------------------------------------------


def test_gross_counts_misplacement_in_both_directions() -> None:
    score = graded(
        {'423100': {'1111A0': 100.0, '111200': 0.0}},
        {'423100': {'1111A0': 60.0, '111200': 40.0}},
    )
    assert score.gross == pytest.approx(80.0)
    assert score.row_error['423100'] == pytest.approx(0.0)


def test_row_error_is_blind_to_the_buyer_split() -> None:
    """Which is exactly why the row grade alone would flatter both rules."""
    score = graded(
        {'423100': {'1111A0': 100.0, '111200': 0.0}},
        {'423100': {'1111A0': 0.0, '111200': 100.0}},
    )
    assert score.row_error['423100'] == pytest.approx(0.0)
    assert score.buyer_dissimilarity()['423100'] == pytest.approx(1.0)


def test_buyer_dissimilarity_is_the_share_on_the_wrong_buyer() -> None:
    score = graded(
        {'423100': {'1111A0': 60.0, '111200': 40.0}},
        {'423100': {'1111A0': 80.0, '111200': 20.0}},
    )
    # |0.6-0.8| + |0.4-0.2| = 0.4, halved
    assert score.buyer_dissimilarity()['423100'] == pytest.approx(0.2)


def test_buyer_dissimilarity_ignores_scale() -> None:
    """A row twice too large but correctly split scores zero here."""
    score = graded(
        {'423100': {'1111A0': 60.0, '111200': 40.0}},
        {'423100': {'1111A0': 120.0, '111200': 80.0}},
    )
    assert score.buyer_dissimilarity()['423100'] == pytest.approx(0.0)
    assert score.row_error['423100'] == pytest.approx(100.0)


def test_weighted_dissimilarity_follows_the_dollars() -> None:
    """A perfect big row should outweigh a wrong small one."""
    score = graded(
        {
            '423100': {'1111A0': 900.0, '111200': 100.0},
            '441000': {'1111A0': 5.0, '111200': 5.0},
        },
        {
            '423100': {'1111A0': 900.0, '111200': 100.0},
            '441000': {'1111A0': 10.0, '111200': 0.0},
        },
    )
    per_row = score.buyer_dissimilarity()
    assert per_row['423100'] == pytest.approx(0.0)
    assert per_row['441000'] == pytest.approx(0.5)
    # 0.5 weighted by 10 against 0.0 weighted by 1000
    assert score.weighted_dissimilarity() == pytest.approx(0.5 * 10 / 1010)


def test_rows_with_no_observed_margin_are_skipped_not_scored_zero() -> None:
    score = graded(
        {'423100': {'1111A0': 100.0}, '441000': {'1111A0': 0.0}},
        {'423100': {'1111A0': 100.0}, '441000': {'1111A0': 50.0}},
    )
    assert '441000' not in score.buyer_dissimilarity().index


# --- the distribution ------------------------------------------------------


def test_distribute_is_the_matrix_product_and_conserves_mass() -> None:
    margin = pd.DataFrame(
        {'311111': [10.0, 0.0], '423A00': [0.0, 4.0]}, index=['1111A0', '111200']
    )
    shares = pd.DataFrame(
        {'311111': [0.25, 0.75], '423A00': [1.0, 0.0]}, index=['423100', '441000']
    )
    placed = distribute(margin, shares)

    assert placed.loc['423100', '1111A0'] == pytest.approx(2.5)
    assert placed.loc['441000', '1111A0'] == pytest.approx(7.5)
    assert placed.loc['423100', '111200'] == pytest.approx(4.0)
    assert placed.to_numpy().sum() == pytest.approx(margin.to_numpy().sum())


def test_distribute_drops_commodities_the_shares_do_not_cover() -> None:
    """Without a share column a commodity has nowhere to go; the caller
    substitutes the fallback before calling, and this pins the raw behaviour."""
    margin = pd.DataFrame({'311111': [10.0], 'S00401': [5.0]}, index=['1111A0'])
    shares = pd.DataFrame({'311111': [1.0]}, index=['423100'])

    assert distribute(margin, shares).to_numpy().sum() == pytest.approx(10.0)


# --- the setup, against the published tables -------------------------------


@pytest.mark.parametrize(('kind', 'count'), [('wholesale', 10), ('retail', 9)])
def test_fallback_shares_are_a_distribution_over_the_kind(
    kind: str, count: int
) -> None:
    shares = fallback_shares(kind)

    assert len(shares) == count
    assert sorted(shares.index) == sorted(GIVER_COMMODITIES[kind])
    assert float(shares.sum()) == pytest.approx(1.0)
    assert (shares >= 0).all()


def test_observed_margin_is_the_nineteen_trade_rows() -> None:
    observed = observed_margin()

    assert observed.shape[0] == 19
    assert sorted(observed.index) == sorted(
        list(GIVER_COMMODITIES['wholesale']) + list(GIVER_COMMODITIES['retail'])
    )


def test_the_margin_to_place_matches_what_the_mut_books() -> None:
    """The Margins table and the published MUT agree on the intermediate slice.

    If these drift apart the grade is measuring a vintage mismatch rather than a
    placement rule.
    """
    observed = observed_margin().to_numpy().sum()
    from_margins = sum(
        industry_margin(kind).to_numpy().sum() for kind in ('wholesale', 'retail')
    )

    assert observed / MILLION == pytest.approx(987_389, abs=1)
    assert from_margins / MILLION == pytest.approx(986_763, abs=1)
    assert abs(observed - from_margins) / from_margins < 0.001


def test_the_fallback_conserves_every_dollar_of_margin() -> None:
    scored = grade('fallback')
    from_margins = sum(
        industry_margin(kind).to_numpy().sum() for kind in ('wholesale', 'retail')
    )

    assert scored.candidate.to_numpy().sum() == pytest.approx(from_margins, rel=1e-9)
    assert np.all(scored.candidate.to_numpy() >= 0)


def test_the_matrix_conserves_every_dollar_too() -> None:
    try:
        scored = grade('matrix')
    except MISSING_ARTIFACT as error:
        pytest.skip(f'Census_EC_PxI is not available: {error}')
    from_margins = sum(
        industry_margin(kind).to_numpy().sum() for kind in ('wholesale', 'retail')
    )

    assert scored.candidate.to_numpy().sum() == pytest.approx(from_margins, rel=1e-6)


# --- the share construction ------------------------------------------------


def test_matrix_shares_normalise_covered_commodities_and_zero_the_rest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every covered commodity's column must sum to 1, uncovered ones to 0.

    ⚠️ The regression this pins: zeroing the uncovered columns with
    ``DataFrame.where(totals > 0, 0.0)`` aligns the commodity-indexed mask
    against the trade-code *rows*, matches nothing, and blanks the whole frame.
    Every commodity then reads as uncovered, the matrix arm silently becomes the
    fallback, and the verdict comes back "matrix ties fallback" - a result that
    looks like a finding rather than a bug.
    """
    wholesale = list(GIVER_COMMODITIES['wholesale'])
    stub = pd.DataFrame(
        {
            '311111': [4.0] + [0.0] * (len(wholesale) - 1),
            '311119': [1.0, 3.0] + [0.0] * (len(wholesale) - 2),
            'S00401': [0.0] * len(wholesale),
        },
        index=wholesale,
    )
    monkeypatch.setattr(grading, 'bea_trade_matrix', lambda year: stub)

    shares = matrix_shares('wholesale')

    assert shares['311111'].sum() == pytest.approx(1.0)
    assert shares['311119'].sum() == pytest.approx(1.0)
    assert shares.loc[wholesale[1], '311119'] == pytest.approx(0.75)
    assert shares['S00401'].sum() == pytest.approx(0.0)


def test_matrix_shares_keep_wholesale_margin_away_from_retailers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The kind split is what makes the matrix comparable to the fallback."""
    every_code = list(GIVER_COMMODITIES['wholesale']) + list(
        GIVER_COMMODITIES['retail']
    )
    stub = pd.DataFrame({'311111': [1.0] * len(every_code)}, index=every_code)
    monkeypatch.setattr(grading, 'bea_trade_matrix', lambda year: stub)

    shares = matrix_shares('wholesale')

    assert sorted(shares.index) == sorted(GIVER_COMMODITIES['wholesale'])
    assert shares['311111'].sum() == pytest.approx(1.0)
