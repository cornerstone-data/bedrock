"""Tests for the SUT -> MUT Use conversion and its replay scorer.

The scorer runs on constructed matrices - it is arithmetic over two frames, so
the inputs should be hand-checkable, including the cases that make a scorer
wrong: a missing row, a blank BEA leaves, a difference on the tolerance
boundary. The answer-key loaders run against the published tables, because what
they assert is a property of BEA's workbook rather than of our code.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from bedrock.transform.iot.sut_use_to_mut_use import (
    REPLAY_ATOL,
    by_job,
    by_row,
    published_mut_use_2017,
    score_replay,
    use_producer_from_sut,
)
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_final_demand import (
    USA_2017_FINAL_DEMAND_CODES,
    USA_2017_FINAL_DEMAND_IMPORT_CODE,
)
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES
from bedrock.utils.taxonomy.bea.v2017_value_added import USA_2017_VALUE_ADDED_CODES

MILLION = MILLION_CURRENCY_TO_CURRENCY


def frame(values: dict[str, dict[str, float]]) -> pd.DataFrame:
    """A small table from ``{row: {column: value}}``, USD."""
    return pd.DataFrame(values).T.astype(float)


# --- the scorer ------------------------------------------------------------


def test_identical_frames_score_exact() -> None:
    published = frame({'111CA': {'1111A0': 10.0, 'F01000': 4.0}})
    score = score_replay(published, published)

    assert score.n_cells == 2
    assert score.n_outside == 0
    assert score.gross == 0.0
    assert score.net == 0.0


def test_gross_and_net_separate_moved_money_from_created_money() -> None:
    """A margin moved between two rows nets to zero; the gross still sees it."""
    published = frame({'423A00': {'1111A0': 0.0}, '311111': {'1111A0': 100.0}})
    candidate = frame({'423A00': {'1111A0': 30.0}, '311111': {'1111A0': 70.0}})
    score = score_replay(candidate, published)

    assert score.net == pytest.approx(0.0)
    assert score.gross == pytest.approx(60.0)


@pytest.mark.parametrize(
    ('published_m', 'diff_m', 'expected_outside'),
    [
        # the two terms ADD: threshold is 0.5 $M + 1% of the published cell
        (1.0, 0.4, 0),  # 0.51 $M allowed
        (1.0, 0.6, 1),
        (100.0, 1.4, 0),  # 1.5 $M allowed
        (100.0, 1.5, 0),  # exactly on the boundary, and np.isclose is inclusive
        (100.0, 1.6, 1),
        (1000.0, 9.0, 0),  # 10.5 $M allowed
        (1000.0, 11.0, 1),
    ],
)
def test_tolerance_is_atol_plus_one_percent_of_the_published_side(
    published_m: float, diff_m: float, expected_outside: int
) -> None:
    """``np.isclose`` adds its terms: a cell passes within ``atol + rtol x published``.

    So the 0.5 $M floor dominates small cells and the 1% term dominates large
    ones, but neither is ever the whole threshold - worth pinning, because
    reading them as alternatives understates the tolerance on every big cell.
    """
    published = frame({'111CA': {'1111A0': published_m * MILLION}})
    candidate = frame({'111CA': {'1111A0': (published_m + diff_m) * MILLION}})

    assert score_replay(candidate, published).n_outside == expected_outside


def test_a_missing_row_fails_by_its_full_amount() -> None:
    """A dropped row must not score better than a wrong one."""
    published = frame({'111CA': {'1111A0': 10.0}, '423A00': {'1111A0': 5.0 * MILLION}})
    candidate = published.drop(index=['423A00'])
    score = score_replay(candidate, published)

    assert score.n_cells == 2
    assert score.n_outside == 1
    assert score.gross == pytest.approx(5.0 * MILLION)
    assert score.net == pytest.approx(-5.0 * MILLION)


def test_an_invented_cell_fails_too() -> None:
    """Writing into a cell BEA leaves blank is as wrong as omitting one."""
    published = frame({'V00200': {'1111A0': 10.0, 'F01000': np.nan}})
    candidate = frame({'V00200': {'1111A0': 10.0, 'F01000': 5.0 * MILLION}})
    score = score_replay(candidate, published)

    assert score.n_cells == 2
    assert score.n_outside == 1
    assert score.gross == pytest.approx(5.0 * MILLION)


def test_cells_blank_on_both_sides_are_excluded_from_every_count() -> None:
    """BEA leaves value added blank in the final-demand columns."""
    published = frame({'V00200': {'1111A0': 10.0, 'F01000': np.nan}})
    candidate = published.copy()
    score = score_replay(candidate, published)

    assert score.n_cells == 1
    assert score.n_outside == 0
    assert score.gross == 0.0


def test_a_zero_where_bea_leaves_a_blank_still_matches() -> None:
    """The conversion may emit 0.0 rather than NaN; that is not an error."""
    published = frame({'V00200': {'1111A0': 10.0, 'F01000': np.nan}})
    candidate = frame({'V00200': {'1111A0': 10.0, 'F01000': 0.0}})

    assert score_replay(candidate, published).n_outside == 0


def test_by_row_ranks_the_worst_first() -> None:
    published = frame(
        {
            '423A00': {'1111A0': 0.0, '111200': 0.0},
            '311111': {'1111A0': 0.0, '111200': 0.0},
        }
    )
    candidate = frame(
        {
            '423A00': {'1111A0': 7.0, '111200': 0.0},
            '311111': {'1111A0': 0.0, '111200': 3.0},
        }
    )
    score = score_replay(candidate, published)

    assert list(by_row(score.diff).index) == ['423A00', '311111']


def test_by_job_attributes_each_residual_to_one_bucket() -> None:
    """The imports column wins over the row tests; the rest is the margin join."""
    rows = ['311111', 'V00200']
    columns = ['1111A0', USA_2017_FINAL_DEMAND_IMPORT_CODE]
    published = pd.DataFrame(0.0, index=rows, columns=columns)
    candidate = published.copy()
    candidate.loc['311111', USA_2017_FINAL_DEMAND_IMPORT_CODE] = 1.0
    candidate.loc['V00200', '1111A0'] = 2.0
    candidate.loc['311111', '1111A0'] = 4.0

    jobs = by_job(score_replay(candidate, published).diff)

    assert jobs['F05000'] == pytest.approx(1.0)
    assert jobs['VA collapse'] == pytest.approx(2.0)
    assert jobs['margin join'] == pytest.approx(4.0)
    # every dollar lands in exactly one bucket
    assert jobs.sum() == pytest.approx(7.0)


def test_by_job_sums_to_gross_on_a_random_frame() -> None:
    rng = np.random.default_rng(0)
    rows = ['311111', '423A00', '484000', 'V00100', 'V00200']
    columns = ['1111A0', '111200', 'F01000', USA_2017_FINAL_DEMAND_IMPORT_CODE]
    published = pd.DataFrame(
        rng.normal(size=(len(rows), len(columns))) * MILLION,
        index=rows,
        columns=columns,
    )
    candidate = published + rng.normal(size=published.shape) * MILLION

    score = score_replay(candidate, published)
    assert by_job(score.diff).sum() == pytest.approx(score.gross)


# --- the conversion contract -----------------------------------------------


def test_conversion_is_not_implemented_yet_and_says_which_pr_lands_it() -> None:
    empty = pd.DataFrame()
    with pytest.raises(NotImplementedError, match='F05000'):
        use_producer_from_sut(empty, empty, empty, empty, 2017)


# --- the answer key, against the published workbook ------------------------


def test_answer_key_has_the_axes_the_conversion_must_produce() -> None:
    table = published_mut_use_2017()

    assert list(table.index) == [
        *USA_2017_COMMODITY_CODES,
        *USA_2017_VALUE_ADDED_CODES,
    ]
    assert list(table.columns) == [
        *USA_2017_INDUSTRY_CODES,
        *USA_2017_FINAL_DEMAND_CODES,
    ]


def test_answer_key_leaves_value_added_blank_in_final_demand() -> None:
    """BEA does, and a scorer that reads those blanks as zero invents residual."""
    table = published_mut_use_2017()
    block = table.loc[
        list(USA_2017_VALUE_ADDED_CODES), list(USA_2017_FINAL_DEMAND_CODES)
    ]
    assert block.isna().to_numpy().all()


def test_f05000_is_published_negative_with_the_duties_cell_positive() -> None:
    """The sign trap in job 1: the column is negative, ``4200ID`` is not.

    Customs duties are booked on the synthetic duties commodity as a *positive*
    entry in an otherwise negative column, which is why ``F05000`` reconciles to
    Supply ``MCIF + MADJ`` in total but on only a minority of commodities.
    """
    column = published_mut_use_2017()[USA_2017_FINAL_DEMAND_IMPORT_CODE]

    assert column.sum() / MILLION == pytest.approx(-2_626_305, abs=1)
    assert column.loc['4200ID'] / MILLION == pytest.approx(38_513, abs=1)
    assert int((column < 0).sum()) == 296
    assert int((column > 0).sum()) == 6


def test_value_added_block_is_the_muts_three_rows() -> None:
    """Three here against the SUT's six - the collapse job 2 has to perform."""
    table = published_mut_use_2017()
    block = table.loc[list(USA_2017_VALUE_ADDED_CODES), list(USA_2017_INDUSTRY_CODES)]

    assert list(block.index) == ['V00100', 'V00200', 'V00300']
    assert block.loc['V00200'].sum() / MILLION == pytest.approx(1_304_095, abs=1)


def test_4b0000_is_exactly_zero_before_the_redistribution() -> None:
    """The named first cell. It is only diagnostic because it is *exactly* zero."""
    from bedrock.extract.iot.io_2017 import (  # noqa: PLC0415
        _load_2017_detail_supply_use_usa,
    )

    industries = list(USA_2017_INDUSTRY_CODES)
    sut = _load_2017_detail_supply_use_usa('Use_SUT_detail')
    purchaser_row = sut.loc[['4B0000'], industries].astype(float)
    assert purchaser_row.to_numpy().sum() == 0.0

    producer_row = published_mut_use_2017().loc[['4B0000'], industries]
    assert producer_row.to_numpy().sum() / MILLION == pytest.approx(8_602, abs=1)


def test_scoring_the_answer_key_against_itself_is_exact() -> None:
    """If this drifts, every later PR's score is measured against nothing."""
    table = published_mut_use_2017()
    score = score_replay(table, table)

    assert score.n_outside == 0
    assert score.gross == 0.0
    assert score.n_cells == table.notna().to_numpy().sum()


def test_replay_atol_is_half_a_million_dollars() -> None:
    assert REPLAY_ATOL == 0.5 * MILLION
