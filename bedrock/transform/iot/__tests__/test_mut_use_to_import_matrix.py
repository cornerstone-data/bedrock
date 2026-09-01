"""Tests for the proportional import-matrix allocation.

The allocation itself runs on constructed matrices - it is one row of
arithmetic, so the cases that decide whether it is right should be
hand-checkable: a column excluded from the denominator, a negative cell, a row
with nothing positive to spread over. The scope constants and the control rule
run against the published workbook, because what they assert is a property of
BEA's table rather than of our code.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from bedrock.extract.iot.io_2017 import _load_benchmark_detail_supply_use_usa
from bedrock.transform.iot.mut_use_to_import_matrix import (
    ALLOCATION_COLUMNS,
    DEAD_ROW_COMMODITIES,
    IMPORT_FINAL_DEMAND_CODES,
    NEGATIVE_CONTROL_TOLERANCE,
    PUBLISHED_YEARS,
    REPLAY_EXPECTATIONS,
    ZERO_IMPORT_FINAL_DEMAND_CODES,
    _allocated,
    _control_residual,
    _naive_misplacement,
    allocation_weights,
    import_control,
    import_matrix_from_use,
    proportionality_strain,
    published_import_control,
    published_import_matrix,
    summary_divergence,
)
from bedrock.transform.iot.sut_use_to_mut_use import (
    REPLAY_ATOL,
    f05000_column,
    published_mut_use_2017,
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

MILLION = MILLION_CURRENCY_TO_CURRENCY

#: Two real commodity codes to hang constructed rows off, so the frames index
#: the way the allocation expects without standing in for anything.
FIRST = USA_2017_COMMODITY_CODES[0]
SECOND = USA_2017_COMMODITY_CODES[1]

#: Two real industry codes, likewise.
BUYER = USA_2017_INDUSTRY_CODES[0]
OTHER_BUYER = USA_2017_INDUSTRY_CODES[1]


def _use_table(cells: dict[tuple[str, str], float]) -> pd.DataFrame:
    """A producer-price Use table, zero everywhere but *cells*."""
    table = pd.DataFrame(
        0.0,
        index=list(USA_2017_COMMODITY_CODES),
        columns=[*USA_2017_INDUSTRY_CODES, *USA_2017_FINAL_DEMAND_CODES],
        dtype=float,
    )
    for (row, column), value in cells.items():
        table.loc[row, column] = value
    return table


def _allocated_row(table: pd.DataFrame, commodity: str) -> float:
    """What the matrix placed on one commodity's allocation columns."""
    return float(table.loc[[commodity], list(ALLOCATION_COLUMNS)].to_numpy().sum())


def _control(values: dict[str, float]) -> pd.Series:
    """A commodity-indexed import control, zero everywhere but *values*."""
    control = pd.Series(0.0, index=list(USA_2017_COMMODITY_CODES))
    for commodity, value in values.items():
        control[commodity] = value
    return control


# --- the weights -----------------------------------------------------------


def test_weights_are_the_rows_own_use_shares() -> None:
    weights = allocation_weights(
        _use_table({(FIRST, BUYER): 75.0, (FIRST, OTHER_BUYER): 25.0})
    )

    assert weights.loc[FIRST, BUYER] == pytest.approx(0.75)
    assert weights.loc[FIRST, OTHER_BUYER] == pytest.approx(0.25)
    assert weights.loc[FIRST].sum() == pytest.approx(1.0)


def test_f05000_is_kept_out_of_the_denominator() -> None:
    """``F05000`` *is* the import total, so counting it would halve every share."""
    cells = {(FIRST, BUYER): 75.0, (FIRST, OTHER_BUYER): 25.0}
    without = allocation_weights(_use_table(cells))
    with_imports = allocation_weights(
        _use_table({**cells, (FIRST, USA_2017_FINAL_DEMAND_IMPORT_CODE): -100.0})
    )

    pd.testing.assert_frame_equal(without.loc[[FIRST]], with_imports.loc[[FIRST]])


@pytest.mark.parametrize('column', ZERO_IMPORT_FINAL_DEMAND_CODES)
def test_the_columns_bea_leaves_empty_never_dilute_a_share(column: str) -> None:
    cells = {(FIRST, BUYER): 100.0}
    plain = allocation_weights(_use_table(cells))
    with_use = allocation_weights(_use_table({**cells, (FIRST, column): 400.0}))

    assert plain.loc[FIRST, BUYER] == pytest.approx(1.0)
    assert with_use.loc[FIRST, BUYER] == pytest.approx(1.0)
    assert column not in with_use.columns


def test_a_negative_cell_is_clipped_rather_than_shrinking_the_denominator() -> None:
    """The give-up rows go negative; a negative weight would book negative imports."""
    weights = allocation_weights(
        _use_table(
            {
                (FIRST, BUYER): 100.0,
                (FIRST, OTHER_BUYER): -60.0,
            }
        )
    )

    assert weights.loc[FIRST, BUYER] == pytest.approx(1.0)
    assert weights.loc[FIRST, OTHER_BUYER] == pytest.approx(0.0)
    assert (weights.to_numpy() >= 0).all()


def test_a_row_with_nothing_positive_to_spread_over_weighs_zero() -> None:
    weights = allocation_weights(_use_table({(FIRST, BUYER): -100.0}))

    assert weights.loc[FIRST].sum() == pytest.approx(0.0)
    assert not weights.loc[FIRST].isna().any()


def test_a_truncated_use_table_is_refused_rather_than_spread_narrowly() -> None:
    table = _use_table({(FIRST, BUYER): 100.0}).drop(columns=[OTHER_BUYER])

    with pytest.raises(AssertionError, match='missing 1 allocation columns'):
        allocation_weights(table)


# --- the matrix ------------------------------------------------------------


def test_the_row_sums_to_its_control_across_the_allocation_columns() -> None:
    table = import_matrix_from_use(
        _use_table({(FIRST, BUYER): 75.0, (FIRST, OTHER_BUYER): 25.0}),
        _control({FIRST: 400.0}),
    )

    assert table.loc[FIRST, BUYER] == pytest.approx(300.0)
    assert table.loc[FIRST, OTHER_BUYER] == pytest.approx(100.0)
    assert _allocated_row(table, FIRST) == pytest.approx(400.0)


def test_f05000_is_written_negative_and_the_seven_columns_zero() -> None:
    table = import_matrix_from_use(
        _use_table({(FIRST, BUYER): 100.0}), _control({FIRST: 400.0})
    )

    assert table.loc[FIRST, USA_2017_FINAL_DEMAND_IMPORT_CODE] == pytest.approx(-400.0)
    assert (
        table[list(ZERO_IMPORT_FINAL_DEMAND_CODES)].to_numpy() == 0.0
    ).all(), 'BEA measured these at zero; they belong in the frame as zeros'


def test_the_matrix_has_the_axes_the_published_table_has() -> None:
    table = import_matrix_from_use(
        _use_table({(FIRST, BUYER): 100.0}), _control({FIRST: 400.0})
    )
    published = published_import_matrix()

    assert list(table.index) == list(published.index)
    assert list(table.columns) == list(published.columns)


def test_a_commodity_with_no_use_takes_none_of_the_control() -> None:
    """The control is not redistributed onto other commodities to make it fit."""
    table = import_matrix_from_use(
        _use_table({(FIRST, BUYER): 100.0}), _control({FIRST: 400.0, SECOND: 50.0})
    )

    assert _allocated_row(table, SECOND) == pytest.approx(0.0)
    assert _allocated_row(table, FIRST) == pytest.approx(400.0)
    assert table.loc[SECOND, USA_2017_FINAL_DEMAND_IMPORT_CODE] == pytest.approx(-50.0)


def test_beas_rest_of_world_adjustment_passes_but_a_flipped_sign_does_not() -> None:
    use = _use_table({(FIRST, BUYER): 100.0})
    small = -10.0 * MILLION

    import_matrix_from_use(use, _control({FIRST: small}))

    with pytest.raises(AssertionError, match='sign convention that broke upstream'):
        import_matrix_from_use(use, _control({FIRST: -2 * NEGATIVE_CONTROL_TOLERANCE}))


# --- the control -----------------------------------------------------------


def test_import_control_needs_all_three_bridge_columns() -> None:
    bridge = pd.DataFrame({'MCIF': [1.0], 'MADJ': [2.0]}, index=[FIRST], dtype=float)

    with pytest.raises(AssertionError, match=r"missing \['MDTY'\]"):
        import_control(bridge)


def test_import_control_leaves_the_duty_on_the_commodity_that_bore_it() -> None:
    """The contrast with ``f05000_column``, which credits ``4200ID``.

    Reusing that column here is the easiest way to get this wrong, so the
    difference is pinned rather than described.
    """
    bridge = pd.DataFrame(
        0.0,
        index=list(USA_2017_COMMODITY_CODES),
        columns=['MCIF', 'MADJ', 'MDTY'],
        dtype=float,
    )
    bridge.loc[FIRST, 'MCIF'] = 900.0
    bridge.loc[FIRST, 'MDTY'] = 100.0

    control = import_control(bridge)
    use_column = f05000_column(bridge)

    assert control[FIRST] == pytest.approx(1000.0)
    assert control['4200ID'] == pytest.approx(0.0)
    assert use_column['4200ID'] == pytest.approx(100.0)
    assert control.sum() - -use_column.sum() == pytest.approx(100.0)


# --- the published table ---------------------------------------------------


def test_answer_key_has_the_axes_the_allocation_must_produce() -> None:
    table = published_import_matrix()

    assert list(table.index) == list(USA_2017_COMMODITY_CODES)
    assert list(table.columns) == [
        *USA_2017_INDUSTRY_CODES,
        *USA_2017_FINAL_DEMAND_CODES,
    ]


@pytest.mark.parametrize('year', PUBLISHED_YEARS)
def test_seven_final_demand_columns_are_exactly_zero_in_every_vintage(
    year: USA_BENCHMARK_DETAIL_SUT_YEARS,
) -> None:
    """The scope restriction rests on these being empty, not merely small."""
    zero = published_import_matrix(year)[list(ZERO_IMPORT_FINAL_DEMAND_CODES)]

    assert float(zero.abs().to_numpy().sum()) == 0.0


@pytest.mark.parametrize('year', PUBLISHED_YEARS)
def test_the_twelve_other_final_demand_columns_are_populated(
    year: USA_BENCHMARK_DETAIL_SUT_YEARS,
) -> None:
    populated = published_import_matrix(year)[list(IMPORT_FINAL_DEMAND_CODES)]
    empty = [c for c in populated.columns if populated[c].abs().sum() == 0.0]

    assert not empty, (
        f'{empty} carry no imports at {year}, so the allocation would spread '
        f'into a column BEA leaves empty'
    )


@pytest.mark.parametrize('year', PUBLISHED_YEARS)
def test_the_published_row_sums_to_its_own_import_column(
    year: USA_BENCHMARK_DETAIL_SUT_YEARS,
) -> None:
    """Per commodity, never on the total - the column nets to zero economy-wide."""
    published = published_import_matrix(year)
    gap = published[list(ALLOCATION_COLUMNS)].sum(axis=1) - published_import_control(
        year
    )

    assert float(gap.abs().sum()) / MILLION < 2_000, (
        'the published matrix no longer spreads its own F05000 across the '
        'allocation columns, so the scope or the control changed vintage'
    )


def test_the_control_beats_both_of_its_alternatives() -> None:
    """``MDTY`` belongs in, and leaving it out is not a rounding error."""
    supply = _load_benchmark_detail_supply_use_usa('Supply_detail', 2017).rename(
        columns=lambda column: str(column).strip()
    )
    bridge = (
        supply.loc[list(USA_2017_COMMODITY_CODES), ['MCIF', 'MADJ', 'MDTY']].astype(
            float
        )
        * MILLION
    )
    published = published_import_control(2017)

    def gross(candidate: pd.Series) -> float:
        return float((candidate - published).abs().sum()) / MILLION

    assert gross(import_control(bridge)) == pytest.approx(23_163, abs=500)
    assert gross(bridge['MCIF']) == pytest.approx(38_508, abs=500)
    assert gross(bridge['MCIF'] + bridge['MADJ']) == pytest.approx(61_624, abs=500)


def test_the_control_and_the_use_f05000_disagree_on_seven_commodities() -> None:
    """Recorded so a later change to either one has to face the list."""
    matrix_column = published_import_matrix()[USA_2017_FINAL_DEMAND_IMPORT_CODE]
    use_column = published_mut_use_2017()[USA_2017_FINAL_DEMAND_IMPORT_CODE]
    difference = matrix_column - use_column.reindex(matrix_column.index)
    disagreeing = sorted(difference.index[difference.abs() > REPLAY_ATOL])

    assert disagreeing == [
        '4200ID',
        '481000',
        '482000',
        '483000',
        '484000',
        '492000',
        '5241XX',
    ]
    assert float(difference.abs().sum()) / MILLION == pytest.approx(61_631, abs=100)
    assert difference['4200ID'] / MILLION == pytest.approx(-38_513, abs=1)
    pd.testing.assert_series_equal(
        published_import_control(2017), -matrix_column, check_names=False
    )


# --- the replay ------------------------------------------------------------


@pytest.mark.parametrize(('year', 'n_outside', 'gross'), REPLAY_EXPECTATIONS)
def test_the_replay_lands_at_the_proportionality_ceiling(
    year: USA_BENCHMARK_DETAIL_SUT_YEARS, n_outside: int, gross: int
) -> None:
    """⚠️ **A score far below this is a leak, not an improvement.**

    Both inputs are published, so what is left is the proportionality
    assumption alone: every buyer of a commodity assumed to draw imports in the
    same mix. 23-25% of the published gross is what that assumption costs.
    """
    score = score_replay(_allocated(year), published_import_matrix(year))

    assert score.n_outside == pytest.approx(n_outside, abs=200)
    assert score.gross / MILLION == pytest.approx(gross, abs=2_000)


def test_restricting_the_scope_keeps_the_zero_columns_empty() -> None:
    """The counterfactual: what a naive all-column spread would misplace."""
    assert _naive_misplacement(2017) / MILLION == pytest.approx(291_890, abs=1_000)

    allocated = _allocated(2017)
    assert (
        float(allocated[list(ZERO_IMPORT_FINAL_DEMAND_CODES)].abs().to_numpy().sum())
        == 0.0
    )


def test_the_dead_rows_are_the_ones_with_no_positive_use() -> None:
    """Named in the module, so a change to the Use table has to face the list."""
    weights = allocation_weights(published_mut_use_2017())
    dead = sorted(weights.index[weights.sum(axis=1) == 0.0])

    assert dead == sorted(DEAD_ROW_COMMODITIES)


def test_the_control_residual_sits_where_the_module_says_it_does() -> None:
    residual = _control_residual(2017)
    outside = residual.index[residual.abs() > REPLAY_ATOL]

    assert len(outside) == pytest.approx(53, abs=5)
    assert float(residual.abs().sum()) / MILLION == pytest.approx(23_163, abs=500)


def test_strain_ranks_insurance_carriers_first() -> None:
    """A signal about the assumption, not a list of defects to fix."""
    score = score_replay(_allocated(2017), published_import_matrix())
    strain = proportionality_strain(score.diff, published_import_matrix())

    assert strain.index[0] == '5241XX'
    assert strain['gross'].iloc[0] / MILLION == pytest.approx(89_502, abs=500)
    assert strain['gross'].is_monotonic_decreasing


def test_the_summary_aggregate_reproduces_the_published_summary_matrix() -> None:
    """Per summary commodity - the validation axis that survives past 2017."""
    divergence = summary_divergence(_allocated(2017), 2017)

    assert len(divergence) > 70
    assert float(divergence['difference'].abs().sum()) / MILLION < 1_000, (
        'the row control or the detail-to-summary crosswalk broke: the '
        'allocation redistributes within a row, so its summary totals are the '
        'published ones up to rounding whatever the interior looks like'
    )


def test_the_allocation_creates_no_money() -> None:
    """Every dollar of the control lands somewhere, or on a named dead row."""
    allocated = _allocated(2017)
    control = published_import_control(2017)
    placed = allocated[list(ALLOCATION_COLUMNS)].sum(axis=1)
    stranded = control - placed

    assert float(stranded.drop(list(DEAD_ROW_COMMODITIES)).abs().sum()) < REPLAY_ATOL
    assert float(stranded.abs().sum()) / MILLION == pytest.approx(26, abs=5)
    assert np.isfinite(allocated.to_numpy()).all()
