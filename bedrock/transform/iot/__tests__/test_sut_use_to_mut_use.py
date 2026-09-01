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
    DUTIES_COMMODITY,
    REPLAY_ATOL,
    REPLAY_RTOL,
    TRADE_GIVERS,
    TRANSPORT_GIVERS,
    V00200_COMPONENTS,
    by_job,
    by_row,
    f05000_column,
    margin_recovery,
    producer_value_block,
    published_mut_use,
    published_mut_use_2017,
    score_replay,
    use_producer_from_sut,
    v00200_row,
)
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.taxonomy.bea.matrix_mappings import (
    USA_BENCHMARK_DETAIL_SUT_YEARS,
)
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_final_demand import (
    SUT_FINAL_DEMAND_CODES,
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


# --- the conversion contract -----------------------------------------------


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


# --- job 1: the F05000 column ----------------------------------------------


def bridge(rows: dict[str, tuple[float, float, float]]) -> pd.DataFrame:
    """A supply bridge from ``{commodity: (MCIF, MADJ, MDTY)}``, USD."""
    return pd.DataFrame(rows, index=['MCIF', 'MADJ', 'MDTY']).T.astype(float)


def test_f05000_is_negative_and_carries_the_duty_credit() -> None:
    frame_in = bridge({'311111': (100.0, 5.0, 7.0), DUTIES_COMMODITY: (0.0, 0.0, 0.0)})
    column = f05000_column(frame_in)

    assert column['311111'] == pytest.approx(-112.0)
    # the whole duty total is credited back, positive, on the duties commodity
    assert column[DUTIES_COMMODITY] == pytest.approx(7.0)
    # imports are a deduction, so the column nets negative
    assert column.sum() == pytest.approx(-105.0)


def test_f05000_needs_the_duties_commodity_to_credit_onto() -> None:
    with pytest.raises(AssertionError, match=DUTIES_COMMODITY):
        f05000_column(bridge({'311111': (100.0, 5.0, 7.0)}))


def test_f05000_needs_all_three_bridge_columns() -> None:
    frame_in = bridge({DUTIES_COMMODITY: (1.0, 0.0, 0.0)}).drop(columns=['MDTY'])
    with pytest.raises(AssertionError, match='MDTY'):
        f05000_column(frame_in)


def test_f05000_refuses_a_bridge_with_mass_on_the_duties_commodity() -> None:
    """Money the bridge books against 4200ID must not vanish under the credit."""
    frame_in = bridge(
        {'311111': (100.0, 5.0, 7.0), DUTIES_COMMODITY: (2.0 * MILLION, 0.0, 0.0)}
    )
    with pytest.raises(AssertionError, match='silently drop'):
        f05000_column(frame_in)


@pytest.mark.parametrize(('year', 'n_wrong_without_duties'), [(2012, 108), (2017, 111)])
def test_f05000_reproduces_the_published_column_per_commodity(
    year: USA_BENCHMARK_DETAIL_SUT_YEARS, n_wrong_without_duties: int
) -> None:
    """Duties are part of the rule, not a rounding error - on both benchmarks.

    ``MCIF + MADJ`` alone matches the published totals to single-digit $M and
    is wrong on ~110 commodities in each benchmark year; adding ``MDTY`` puts
    every commodity inside tolerance in both. Two independent published years
    make the rule an identity rather than a 2017 fit.
    """
    from bedrock.extract.iot.io_2017 import (  # noqa: PLC0415
        _load_benchmark_detail_supply_use_usa,
    )

    supply = _load_benchmark_detail_supply_use_usa('Supply_detail', year)
    supply = supply.rename(columns=str.strip)
    commodities = list(USA_2017_COMMODITY_CODES)
    frame_in = supply.loc[commodities, ['MCIF', 'MADJ', 'MDTY']].astype(float) * MILLION
    published = published_mut_use(year).loc[
        commodities, USA_2017_FINAL_DEMAND_IMPORT_CODE
    ]

    built = f05000_column(frame_in)
    outside = ~np.isclose(
        built.to_numpy(), published.to_numpy(), rtol=REPLAY_RTOL, atol=REPLAY_ATOL
    )
    assert outside.sum() == 0

    without_duties = -(frame_in['MCIF'] + frame_in['MADJ'])
    still_outside = ~np.isclose(
        without_duties.to_numpy(),
        published.to_numpy(),
        rtol=REPLAY_RTOL,
        atol=REPLAY_ATOL,
    )
    assert still_outside.sum() == n_wrong_without_duties


# --- job 2: the V00200 collapse --------------------------------------------


def va_block(rows: dict[str, dict[str, float]]) -> pd.DataFrame:
    """A six-row value-added block, defaulting any row not given to zero."""
    industries = sorted({i for cells in rows.values() for i in cells})
    block = pd.DataFrame(
        0.0, index=['V00100', 'V00300', *V00200_COMPONENTS], columns=industries
    )
    for row, cells in rows.items():
        for industry, value in cells.items():
            block.loc[row, industry] = value
    return block


def test_v00200_is_a_plain_sum_of_the_four_tax_rows() -> None:
    block = va_block(
        {
            'T00OTOP': {'1111A0': 10.0},
            'T00OSUB': {'1111A0': -3.0},
            'T00TOP': {'1111A0': 20.0},
            'T00SUB': {'1111A0': -4.0},
            'V00100': {'1111A0': 999.0},
        }
    )
    assert v00200_row(block)['1111A0'] == pytest.approx(23.0)


def test_v00200_sums_t00osub_rather_than_ignoring_it() -> None:
    """The row is zero at the anchor, so only a nonzero case can prove it."""
    without = va_block({'T00TOP': {'1111A0': 20.0}})
    with_row = va_block({'T00TOP': {'1111A0': 20.0}, 'T00OSUB': {'1111A0': -538.0}})

    assert v00200_row(without)['1111A0'] == pytest.approx(20.0)
    assert v00200_row(with_row)['1111A0'] == pytest.approx(-518.0)


def test_v00200_raises_on_a_block_missing_t00osub() -> None:
    """A five-row panel replays 2017 perfectly and is wrong from 2020."""
    block = va_block({'T00TOP': {'1111A0': 20.0}}).drop(index=['T00OSUB'])
    with pytest.raises(AssertionError, match='T00OSUB'):
        v00200_row(block)


@pytest.mark.parametrize('row', ['T00SUB', 'T00OSUB'])
def test_v00200_raises_on_bea_signed_subsidies(row: str) -> None:
    """Summing a BEA-signed block overstates V00200 by twice the subsidies."""
    block = va_block({row: {'1111A0': 4.0 * MILLION}})
    with pytest.raises(ValueError, match=row):
        v00200_row(block)


def test_v00200_tolerates_rounding_dust_in_the_subsidy_rows() -> None:
    block = va_block({'T00SUB': {'1111A0': 0.5 * MILLION}})
    assert v00200_row(block)['1111A0'] == pytest.approx(0.5 * MILLION)


@pytest.mark.parametrize('year', [2012, 2017])
def test_v00200_reproduces_the_published_row_from_the_sut_block(
    year: USA_BENCHMARK_DETAIL_SUT_YEARS,
) -> None:
    """The collapse is an identity, to the workbook's own rounding.

    ``T00OSUB`` is absent from the published detail workbooks and zero in both
    benchmark years, so it enters as an explicit zero row - which is exactly
    why a conversion that forgot it would still pass here.
    """
    from bedrock.extract.iot.io_2017 import (  # noqa: PLC0415
        _load_benchmark_detail_supply_use_usa,
    )

    sut = _load_benchmark_detail_supply_use_usa('Use_SUT_detail', year)
    industries = list(USA_2017_INDUSTRY_CODES)
    block = pd.DataFrame(0.0, index=list(V00200_COMPONENTS), columns=industries)
    for row in V00200_COMPONENTS:
        if row in sut.index:
            values = sut.loc[[row], industries].apply(pd.to_numeric, errors='coerce')
            block.loc[row] = values.fillna(0.0).to_numpy()[0] * MILLION
    # BEA publishes the Use subsidy row positive; the build stores it negative
    block.loc['T00SUB'] = -block.loc['T00SUB']
    assert 'T00OSUB' in sut.index or (block.loc['T00OSUB'] == 0).all()

    built = v00200_row(block)
    published = published_mut_use(year).loc[['V00200'], industries].iloc[0]

    assert built.sum() / MILLION == pytest.approx(published.sum() / MILLION, abs=25)
    worst = float(np.abs(built.to_numpy() - published.to_numpy()).max())
    assert worst / MILLION <= 1


@pytest.mark.parametrize('year', [2012, 2017])
def test_v00100_and_v00300_cross_unchanged(
    year: USA_BENCHMARK_DETAIL_SUT_YEARS,
) -> None:
    """Neither row is part of the collapse; the before-redef pair is identical."""
    from bedrock.extract.iot.io_2017 import (  # noqa: PLC0415
        _load_benchmark_detail_supply_use_usa,
    )

    sut = _load_benchmark_detail_supply_use_usa('Use_SUT_detail', year)
    industries = list(USA_2017_INDUSTRY_CODES)
    mut = published_mut_use(year)
    for row in ('V00100', 'V00300'):
        published = mut.loc[[row], industries].to_numpy()
        sut_side = (
            sut.loc[[row], industries].apply(pd.to_numeric, errors='coerce').fillna(0.0)
            * MILLION
        ).to_numpy()
        assert np.abs(sut_side - published).max() == 0.0


# --- the conversion --------------------------------------------------------


def sut_panel() -> pd.DataFrame:
    """The published 2017 Use SUT on the build's sign convention."""
    from bedrock.extract.iot.io_2017 import (  # noqa: PLC0415
        _load_2017_detail_supply_use_usa,
    )

    raw = _load_2017_detail_supply_use_usa('Use_SUT_detail')
    rows = [*USA_2017_COMMODITY_CODES, 'V00100', 'V00300', *V00200_COMPONENTS]
    columns = [*USA_2017_INDUSTRY_CODES, *SUT_FINAL_DEMAND_CODES]
    panel = pd.DataFrame(0.0, index=rows, columns=columns)
    for row in rows:
        if row in raw.index:
            panel.loc[row] = (
                raw.loc[[row], columns]
                .apply(pd.to_numeric, errors='coerce')
                .fillna(0.0)
                .to_numpy()[0]
                * MILLION
            )
    # BEA publishes the Use subsidy row positive; the build stores it negative
    panel.loc['T00SUB'] = -panel.loc['T00SUB']
    return panel


def supply_bridge_2017() -> pd.DataFrame:
    from bedrock.extract.iot.io_2017 import (  # noqa: PLC0415
        _load_2017_detail_supply_use_usa,
    )

    supply = _load_2017_detail_supply_use_usa('Supply_detail').rename(
        columns=lambda column: column.strip()
    )
    return (
        supply.loc[
            list(USA_2017_COMMODITY_CODES),
            ['MCIF', 'MADJ', 'MDTY', 'TRADE', 'TRANS'],
        ].astype(float)
        * MILLION
    )


def test_producer_value_block_prefers_the_margins_table() -> None:
    purchaser = pd.DataFrame(
        {'1111A0': [10.0, 20.0]}, index=['311111', '423A00']
    ).astype(float)
    margins = pd.DataFrame(
        {"Producers' Value": [7.0]},
        index=pd.MultiIndex.from_tuples(
            [('1111A0', '311111')], names=['Industry Code', 'Commodity Code']
        ),
    )
    block = producer_value_block(purchaser, margins)

    assert block.loc['311111', '1111A0'] == pytest.approx(7.0)
    # no margins row: nothing was booked against it, so the valuations coincide
    assert block.loc['423A00', '1111A0'] == pytest.approx(20.0)


def test_conversion_reproduces_the_published_interior_exactly() -> None:
    """The margins table's Producers' Value *is* the producer-price cell.

    The redistribution is already inside that column, so the conversion is a
    reshape and the margin-join bucket scores zero.
    """
    from bedrock.transform.iot.nowcast_margins import (  # noqa: PLC0415
        load_margins_transactions_2017,
    )

    converted = use_producer_from_sut(
        sut_panel(), supply_bridge_2017(), load_margins_transactions_2017(), 2017
    )
    score = score_replay(converted, published_mut_use_2017())

    assert by_job(score.diff)['margin join'] == pytest.approx(0.0)
    # what is left is the two known rounding sources, both from PR 2
    assert score.gross / MILLION == pytest.approx(141, abs=2)
    assert score.n_outside <= 10


def test_conversion_produces_the_answer_key_axes() -> None:
    from bedrock.transform.iot.nowcast_margins import (  # noqa: PLC0415
        load_margins_transactions_2017,
    )

    converted = use_producer_from_sut(
        sut_panel(), supply_bridge_2017(), load_margins_transactions_2017(), 2017
    )
    assert list(converted.index) == list(published_mut_use_2017().index)
    assert list(converted.columns) == list(published_mut_use_2017().columns)


def test_4b0000_goes_from_zero_to_large() -> None:
    """The named first cell: exactly zero before, 230,440 $M after."""
    from bedrock.transform.iot.nowcast_margins import (  # noqa: PLC0415
        load_margins_transactions_2017,
    )

    panel = sut_panel()
    converted = use_producer_from_sut(
        panel, supply_bridge_2017(), load_margins_transactions_2017(), 2017
    )
    assert float(panel.loc['4B0000'].sum()) == 0.0
    assert float(converted.loc['4B0000'].sum()) / MILLION == pytest.approx(
        230_440, abs=1
    )


def test_transport_recovers_its_supply_column_per_commodity() -> None:
    """Free and exact: a margin is value moved, never created."""
    from bedrock.transform.iot.nowcast_margins import (  # noqa: PLC0415
        load_margins_transactions_2017,
    )

    panel = sut_panel()
    bridge = supply_bridge_2017()
    converted = use_producer_from_sut(
        panel, bridge, load_margins_transactions_2017(), 2017
    )
    recovery = margin_recovery(
        converted, panel.loc[list(USA_2017_COMMODITY_CODES), panel.columns], bridge
    )
    transport = recovery[recovery['kind'] == 'transport']

    assert len(transport) == len(TRANSPORT_GIVERS)
    assert abs(transport['residual'].sum()) / MILLION < 50
    assert float(transport['residual'].abs().max()) / MILLION < 50


def test_trade_over_recovers_by_the_trade_level_tax() -> None:
    """⚠️ Not a free identity: the sales tax rides inside the margin columns.

    Asserting trade against give-up alone would fail on correct data by
    ~391,800 $M.
    """
    from bedrock.transform.iot.nowcast_margins import (  # noqa: PLC0415
        load_margins_transactions_2017,
    )
    from bedrock.transform.iot.nowcast_trade_margins import (  # noqa: PLC0415
        trade_level_tax_2017,
    )

    panel = sut_panel()
    bridge = supply_bridge_2017()
    converted = use_producer_from_sut(
        panel, bridge, load_margins_transactions_2017(), 2017
    )
    recovery = margin_recovery(
        converted, panel.loc[list(USA_2017_COMMODITY_CODES), panel.columns], bridge
    )
    trade = recovery[recovery['kind'] == 'trade']

    assert len(trade) == len(TRADE_GIVERS)
    residual = float(trade['residual'].sum())
    tax = float(trade_level_tax_2017().sum())
    # the two independent measures of the same tax agree to 0.3%
    assert abs(residual - tax) / tax < 0.003


def test_the_supply_margin_columns_net_to_nothing() -> None:
    bridge = supply_bridge_2017()
    assert abs(float(bridge['TRADE'].sum())) / MILLION < 50
    assert abs(float(bridge['TRANS'].sum())) / MILLION < 50
