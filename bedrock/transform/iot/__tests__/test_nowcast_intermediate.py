"""Tests for Step 3, the Use table's intermediate block (#497).

Structural, following ``test_nowcast_targets.py``: the arithmetic of the carry
and the control is separated from its data wiring
(:func:`~bedrock.transform.iot.nowcast_intermediate.carry_shares` and
:func:`~bedrock.transform.iot.nowcast_intermediate.apply_column_control` take
frames), so these run on a toy panel and need neither the GCS workbook nor the
gross-output parquet.

The two highest-value tests here are
:func:`test_a_negative_seed_cell_stays_negative` and
:func:`test_theta_zero_is_a_frozen_structure`. The first is #497's acceptance
criterion that the seven published negatives survive, and every layer below
would absorb a clip silently. The second pins the meaning of ``theta``, which is
the one parameter of this step and fits **negative** at 2023-24 -- a sign error
in the exponent would still produce a plausible-looking table.

``theta`` is no longer a constant: :func:`test_theta_splits_on_the_price_surge_
and_not_on_span_length` pins the rule that replaced it, which keys off whether
the span crosses 2021-22 rather than off how long it is.
"""

from __future__ import annotations

from typing import cast

import numpy as np
import pandas as pd
import pytest

from bedrock.analysis.nowcasting.agriculture_expense_seed import farm_industries
from bedrock.analysis.nowcasting.inputs_structure import (
    MINING_SEEDED,
    _manufacturing_bea_industries,
)
from bedrock.analysis.nowcasting.services_transport_expense_seed import (
    services_transport_industries,
)
from bedrock.analysis.nowcasting.utilities_expense_seed import ELECTRIC
from bedrock.transform.iot import nowcast_intermediate as ni
from bedrock.transform.iot.nowcast_intermediate import (
    BENCHMARK_SCRAP_PPI_BY_BUYER,
    INDISPENSABLE_COMMODITIES,
    INTERMEDIATE_YEARS,
    LAST_MATERIALS_CENSUS,
    MARGIN_YEARS,
    MILLION_CURRENCY_TO_CURRENCY,
    PRICE_SURGE,
    SCRAP_COMMODITY,
    SCRAP_PPI_BY_BUYER,
    SEED_YEAR,
    SUPPLY_VALUATION_COLUMNS,
    THETA_497,
    THETA_ACROSS_SURGE,
    THETA_OFF_SURGE,
    UNPRICED_COMMODITIES,
    _require_margin_year,
    apply_column_control,
    benchmark_scrap_value_factor,
    carry_shares,
    commodity_deflator,
    default_theta,
    derive_intermediate_use,
    fitted_regime_theta,
    held_scrap_price_factor,
    margin_rate,
    recovered_paper_consumption,
    scrap_ppi,
)
from bedrock.utils.config.common import load_env_file_key
from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES

COMMODITIES = ('111130', '211000', '531ORE')
INDUSTRIES = ('1111B0', '324110', '4200ID')


def _seed() -> pd.DataFrame:
    """A toy 2017 interior: three commodities, three industries, one dead column.

    ``4200ID`` is all-zero here for the same reason it is all-zero in the
    published table - customs duties buy no intermediates - so the toy carries
    the awkward case rather than only the easy one.
    """
    frame = pd.DataFrame(
        [
            [100.0, 20.0, 0.0],
            [50.0, 300.0, 0.0],
            [-10.0, 80.0, 0.0],
        ],
        index=pd.Index(COMMODITIES, name='commodity'),
        columns=pd.Index(INDUSTRIES, name='industry'),
    )
    return frame


def _factor(values: tuple[float, float, float] = (2.0, 1.0, 1.0)) -> pd.Series:
    return pd.Series(dict(zip(COMMODITIES, values, strict=True)))


def _cell(frame: pd.DataFrame, row: str, column: str) -> float:
    """One scalar out of a frame, as a float.

    ``.loc[row, column]`` is typed as a union of every pandas scalar - including
    timestamps - so comparing or dividing one reads as a type error without this.
    """
    return float(cast(float, frame.loc[row, column]))


def test_every_live_column_sums_to_one() -> None:
    """The carry estimates shares, so it must hand back shares."""
    shares = carry_shares(_seed(), _factor(), THETA_497)
    live = ['1111B0', '324110']
    assert shares[live].sum(axis=0).round(12).tolist() == [1.0, 1.0]


def test_a_dead_column_stays_dead_instead_of_dividing_by_zero() -> None:
    """``4200ID`` and ``814000`` have no 2017 structure to normalise."""
    shares = carry_shares(_seed(), _factor(), THETA_497)
    assert (shares['4200ID'] == 0.0).all()


def test_theta_zero_is_a_frozen_structure() -> None:
    """``theta = 0`` must reproduce 2017's shares exactly, factor or no factor.

    This is the baseline every measurement in the plan is scored against; if it
    drifts, every reported gain from the carry is measured off the wrong zero.
    """
    seed = _seed()
    frozen = carry_shares(seed, _factor((3.0, 0.5, 7.0)), theta=0.0)
    expected = seed / seed.sum(axis=0).replace(0, np.nan)
    pd.testing.assert_frame_equal(
        frozen[['1111B0', '324110']],
        expected[['1111B0', '324110']].astype(float),
        check_names=False,
    )


def test_a_negative_seed_cell_stays_negative() -> None:
    """#497's acceptance criterion: the published negatives are not clipped.

    Seven cells of the 2017 interior are negative. They survive the carry
    because the factor is positive and the operation is multiplicative, and they
    have to survive the control for the same reason - a clip would be a silent
    change to the seed's own source.
    """
    shares = carry_shares(_seed(), _factor(), THETA_497)
    assert _cell(shares, '531ORE', '1111B0') < 0
    block = apply_column_control(shares, pd.Series(dict.fromkeys(INDUSTRIES, 1000.0)))
    assert _cell(block, '531ORE', '1111B0') < 0


def test_the_carry_moves_share_towards_the_dearer_commodity() -> None:
    """``theta = 1`` is a full nominal carry: double the price, double the share.

    Stated as a ratio between two rows of the same column, because the
    renormalisation rescales both and only the ratio is the carry's doing.
    """
    seed = _seed()
    frozen = carry_shares(seed, _factor(), theta=0.0)
    carried = carry_shares(seed, _factor((2.0, 1.0, 1.0)), theta=1.0)
    before = _cell(frozen, '111130', '1111B0') / _cell(frozen, '211000', '1111B0')
    after = _cell(carried, '111130', '1111B0') / _cell(carried, '211000', '1111B0')
    assert after == pytest.approx(2.0 * before)


def test_a_negative_theta_moves_share_the_other_way() -> None:
    """The exponent's sign is the finding, so it is pinned by a test.

    theta fits -0.25 at 2023 and -0.50 at 2024: the frozen structure scores
    better when shares move *against* their own prices. A build that silently
    clamped theta at zero would report that as "the carry contributes nothing",
    which is what an earlier grid floor did.
    """
    seed = _seed()
    frozen = carry_shares(seed, _factor(), theta=0.0)
    against = carry_shares(seed, _factor((2.0, 1.0, 1.0)), theta=-1.0)
    before = _cell(frozen, '111130', '1111B0') / _cell(frozen, '211000', '1111B0')
    after = _cell(against, '111130', '1111B0') / _cell(against, '211000', '1111B0')
    assert after == pytest.approx(0.5 * before)


def test_the_control_is_reproduced_column_by_column() -> None:
    """The whole point of the control: the block arrives at the given level."""
    control = pd.Series({'1111B0': 1_000.0, '324110': 2_500.0, '4200ID': 0.0})
    block = apply_column_control(carry_shares(_seed(), _factor(), THETA_497), control)
    pd.testing.assert_series_equal(
        block.sum(axis=0), control, check_names=False, check_index_type=False
    )


def test_dollars_aimed_at_a_dead_column_are_refused() -> None:
    """A control with no structure to spread over is an error, not a silent zero.

    ``apply_column_control`` cannot honour it, and dropping the dollars would
    make the block quietly disagree with the control it was scaled to.
    """
    control = pd.Series({'1111B0': 1_000.0, '324110': 2_500.0, '4200ID': 9e9})
    with pytest.raises(ValueError, match='no.*structure to spread'):
        apply_column_control(carry_shares(_seed(), _factor(), THETA_497), control)


def test_a_control_missing_an_industry_raises() -> None:
    control = pd.Series({'1111B0': 1_000.0, '324110': 2_500.0})
    with pytest.raises(KeyError, match='missing industries'):
        apply_column_control(carry_shares(_seed(), _factor(), THETA_497), control)


def _cancelling_seed(second: float) -> pd.DataFrame:
    seed = pd.DataFrame(
        {'1111B0': [10.0, second]},
        index=pd.Index(['111130', '211000'], name='commodity'),
    )
    seed['324110'] = [5.0, 5.0]
    return seed


def test_a_seed_column_that_cancels_is_refused_not_flattened() -> None:
    """A cancelling column has structure; an empty one does not.

    Both sum to zero, and returning all-zero for both would lose the
    distinction. Cannot happen on the published table - one negative cell never
    cancels a whole column - but it is the failure mode of the normalisation.
    """
    with pytest.raises(ValueError, match='summing to zero'):
        carry_shares(
            _cancelling_seed(-10.0),
            pd.Series({'111130': 1.0, '211000': 1.0}),
            THETA_497,
        )


def test_a_column_whose_carried_shares_cancel_is_refused() -> None:
    """The same failure one step later: the seed is fine, the carry cancels it.

    ``10 x 1 - 5 x 2 = 0``, so the renormalisation would divide by zero and
    propagate ``inf`` through the whole column.
    """
    with pytest.raises(ValueError, match='cannot be renormalised'):
        carry_shares(
            _cancelling_seed(-5.0),
            pd.Series({'111130': 1.0, '211000': 2.0}),
            THETA_497,
        )


def test_years_outside_the_gross_output_span_are_refused() -> None:
    """2025 has a price index and no gross output, so it is not buildable."""
    assert INTERMEDIATE_YEARS == tuple(range(2017, 2025))
    with pytest.raises(ValueError, match='gross output is extracted for'):
        derive_intermediate_use(2025)


def test_the_unpriced_commodities_are_the_four_with_no_industry_code() -> None:
    """They are held at a factor of 1.0 because no deflator exists for them."""
    commodities = set(USA_2017_COMMODITY_CODES)
    industries = set(USA_2017_INDUSTRY_CODES)
    assert set(UNPRICED_COMMODITIES) == commodities - industries


def test_497s_theta_is_what_runs_again() -> None:
    """#497 specified ``theta = 1``; after #891 that is the default once more.

    ⚠️ The fitted two-regime rule held the default from #699 until 2026-09-26.
    It was retired because its headline predictor is 96.2% collinear with "the
    target year's summary panel has neither the 2022 Economic Census nor AIES" --
    75 of 78 spans classified identically -- so it cannot separate substitution
    from a panel that stopped incorporating source data.
    """
    assert THETA_497 == 1.0
    assert SEED_YEAR == 2017
    assert default_theta(2024) == THETA_497
    assert default_theta(2019) == THETA_497


def test_theta_splits_on_the_price_surge_and_not_on_span_length() -> None:
    """The fitted rule is a regime, so a longer span alone does not move it.

    2017 -> 2021 is four years and does not cross 2021-22; 2020 -> 2022 is two
    and does. If this ever starts keying off ``year - base`` the R^2 0.14
    elapsed-years model has quietly replaced the R^2 0.61 regime one.
    """
    assert fitted_regime_theta(2021, base=2017) == THETA_OFF_SURGE
    assert fitted_regime_theta(2022, base=2020) == THETA_ACROSS_SURGE
    assert fitted_regime_theta(2019, base=2018) == THETA_OFF_SURGE
    assert PRICE_SURGE == (2021, 2022)


def test_every_target_year_from_2022_crosses_the_surge() -> None:
    """The build seeds from 2017, so 2022 on is the frozen-A regime."""
    fitted = {year: fitted_regime_theta(year) for year in INTERMEDIATE_YEARS}
    assert set(list(fitted.values())[:5]) == {THETA_OFF_SURGE}
    assert set(list(fitted.values())[5:]) == {THETA_ACROSS_SURGE}


def test_a_year_with_no_published_margins_is_refused_not_carried() -> None:
    """A missing Supply sheet must raise rather than read as "margins held".

    ``INTERMEDIATE_YEARS`` is bounded by gross output and ``MARGIN_YEARS`` by
    BEA's published Supply table; they agree at 2024 today and a 2025 build
    (#707) would reach a year with one and not the other. Falling through to a
    factor of 1.0 there would be invisible in the built block.
    """
    assert MARGIN_YEARS[-1] == INTERMEDIATE_YEARS[-1]
    with pytest.raises(ValueError, match='no margin rate for 2025'):
        commodity_deflator(2025)


def test_the_guard_is_margin_specific_and_not_a_year_range() -> None:
    """It must fire on the margin data alone.

    The price index reaches 2025 and gross output does not, so a blanket year
    check here would duplicate ``_require_year`` and mask which input is
    actually missing. ``margins=False`` gets past this one.
    """
    _require_margin_year(MARGIN_YEARS[-1])
    for absent in (MARGIN_YEARS[0] - 1, MARGIN_YEARS[-1] + 1):
        with pytest.raises(ValueError, match='no margin rate for'):
            _require_margin_year(absent)


def test_the_margin_rate_denominator_is_producer_not_basic_value() -> None:
    """``mu = T014 / (T013 + T015)``.

    Dividing by ``T013`` alone would double-count the product-tax wedge, which
    the price index already carries: a median 3.3% overstatement of the rate.
    """
    valuation = pd.DataFrame(
        {'T013': [800.0], 'T014': [100.0], 'T015': [200.0], 'T016': [1100.0]},
        index=pd.Index(['315AL'], name='commodity'),
    )
    assert list(SUPPLY_VALUATION_COLUMNS) == ['T013', 'T014', 'T015']
    assert margin_rate(valuation).loc['315AL'] == pytest.approx(100.0 / 1000.0)


def test_a_zero_producer_value_gives_no_margin_rate_rather_than_infinity() -> None:
    valuation = pd.DataFrame(
        {'T013': [0.0], 'T014': [50.0], 'T015': [0.0]},
        index=pd.Index(['S00900'], name='commodity'),
    )
    assert np.isnan(margin_rate(valuation).loc['S00900'])


def _census_key_available() -> bool:
    """Whether a Census API key resolves, without raising if it does not.

    ⚠️ **The seed tests reach live Census endpoints.** That is deliberate -- what
    they guard is how the real seeds compose, and a toy frame composes however
    the toy was built -- but it means they cannot run where no key is
    configured. They skip there rather than fail, and run in full wherever one
    exists.

    ⚠️ **Asks the real accessor rather than reading the environment.** The key
    normally lives in the project-root ``.env`` and only reaches ``os.environ``
    when ``get_api_key`` loads it, so an ``os.getenv`` check reports "no key" on
    a machine that has one -- which skipped all seven tests locally instead of
    running them.
    """
    try:
        return bool(load_env_file_key('api_key', 'Census'))
    except Exception:  # noqa: BLE001 - any failure to resolve means "cannot run"
        return False


needs_census = pytest.mark.skipif(
    not _census_key_available(),
    reason='no Census API key: set CENSUS_API_KEY (repo secret in CI, .env locally)',
)


# --- the composed seed ------------------------------------------------------
#
# Real sources rather than synthetic frames, for the reason the module docstring
# gives: the defects worth guarding against here are properties of how the seeds
# compose, and a toy frame composes however the toy was built.


@needs_census
@pytest.mark.parametrize('block', ['manufacturing', 'services', 'agriculture'])
def test_every_seed_is_the_identity_at_2017(block: str) -> None:
    """The composition must not move 2017, whatever it overlays.

    ⚠️ **This is the test that caught the first composition.** Every seed is a
    ratio against its own 2017 base, so at 2017 each is the identity and so is
    any correct composition of them. Adding ``materials_seed`` to
    ``nonmaterial_seed`` instead of overlaying them put ``334111`` **55% above**
    the benchmark here, while still producing a well-formed 402 x 402 block of
    plausible dollars.
    """
    benchmark = ni.benchmark_intermediate()
    composed = ni.composed_seed(ni.SEED_YEAR)

    difference = (composed - benchmark).abs()
    # The grain is BEA's own $1M rounding; this is float noise on a $14.9T block.
    assert difference.to_numpy().max() < 1.0


@needs_census
def test_the_composition_loses_no_dollars_and_invents_no_gaps() -> None:
    """Overlaying must not strand mass or leave a cell unfilled.

    ⚠️ **The NaN is the failure this guards.** Reindexing an overlay onto all 402
    commodity rows fills the rows that seed does not carry with ``NaN`` rather
    than leaving them at their benchmark value, which makes the grand total
    ``NaN`` -- and a column of NaNs is not something ``carry_shares`` refuses.
    """
    benchmark = ni.benchmark_intermediate()
    composed = ni.composed_seed(ni.SEED_YEAR)

    assert not composed.isna().to_numpy().any()
    assert composed.shape == benchmark.shape
    assert float(composed.to_numpy().sum()) == pytest.approx(
        float(benchmark.to_numpy().sum()), rel=1e-9
    )


@needs_census
def test_no_column_is_seeded_by_two_blocks() -> None:
    """The blocks must partition the industries they reach.

    A column claimed twice is indexed twice, and the second index compounds the
    first rather than replacing it. ``composed_seed`` raises on this; the point
    here is that the real seeds do not trip it, which is a fact about the source
    mappings rather than about the guard.
    """
    blocks = {
        'manufacturing': set(_manufacturing_bea_industries()),
        'services': set(services_transport_industries()),
        'agriculture': set(farm_industries()),
        'utilities': set(ELECTRIC),
        'mining': set(MINING_SEEDED),
    }
    names = sorted(blocks)
    for i, left in enumerate(names):
        for right in names[i + 1 :]:
            overlap = blocks[left] & blocks[right]
            assert not overlap, f'{left} and {right} both claim {sorted(overlap)}'


@needs_census
def test_a_later_year_actually_moves_off_the_2017_shape() -> None:
    """The counterpart to the identity test: a seed that never moves is not one.

    ⚠️ **Both failures are silent.** A composition that moves 2017 is wrong, and
    one that moves *nothing* in a later year is equally wrong and even easier to
    miss, because every total still ties and the block is still a valid 402 x 402
    of dollars -- it is just the frozen benchmark wearing a seed's name.
    """
    benchmark = ni.benchmark_intermediate()
    later = ni.composed_seed(2022)

    moved = (later - benchmark).abs().sum(axis=0) > MILLION_CURRENCY_TO_CURRENCY
    # 342 columns are reachable by some seed; requiring most of them to move
    # keeps this from passing on one column while the rest quietly freeze.
    assert int(moved.sum()) > 300


@needs_census
def test_the_columns_no_seed_reaches_hold_their_benchmark() -> None:
    """Government, trade and construction hold 2017, and that is the claim.

    ⚠️ **Holding is a statement, not an omission.** Nothing observes the movement
    of these columns, so the seed says so by leaving them alone. A change that
    started moving them would be claiming an observation that does not exist.
    """
    benchmark = ni.benchmark_intermediate()
    later = ni.composed_seed(2022)

    for column in ('GSLGO', '441000', '230301', '213111'):
        assert later[column].equals(benchmark[column]), column


@needs_census
def test_the_observed_mask_is_empty_at_the_base_year() -> None:
    """Nothing is observed at 2017, and that is what pins the mask's definition.

    ⚠️ **The mask means "a survey moved this cell's share", not "a seed wrote
    here".** Every seed is the identity at 2017, so a correct mask is empty
    there. A first version marked whichever cells a seed *touched* and flagged
    **76.2%** of the table, because ``materials_seed`` rewrites a whole column
    and renormalisation moves every cell in it -- that would have switched the
    price carry off almost everywhere.

    This test is the cheapest thing that separates the two definitions
    (jvendries, review of #1000).
    """
    observed = ni.observed_cells(2017)

    assert not bool(observed.to_numpy().any()), (
        f'{int(observed.to_numpy().sum())} of {observed.size} cells are marked '
        'observed at the base year; the mask is counting seed reach, not survey '
        'movement'
    )


@needs_census
def test_a_per_cell_theta_reaches_carry_shares() -> None:
    """A ``commodity x industry`` theta is a documented hook, so it must work.

    ⚠️ ``carried_column_shares`` used to test the exponent for truthiness and
    cast it with ``float()``, which raised *"The truth value of a DataFrame is
    ambiguous"* on exactly the input :func:`carry_shares` advertises. The scalar
    path hid it because production never passes a frame (jvendries, review of
    #1000).

    A theta of zero everywhere is the frozen structure, whether it arrives as a
    scalar or as a frame -- which is the invariant worth asserting, because it
    holds only if the frame is aligned and masked rather than ignored.
    """
    seed, _ = ni.composed_seed_and_observed(2022)
    frame = pd.DataFrame(0.0, index=seed.index, columns=seed.columns)

    scalar = ni.carried_column_shares(2022, theta=0.0)
    per_cell = ni.carried_column_shares(2022, theta=frame)

    pd.testing.assert_frame_equal(scalar, per_cell)


def test_the_indispensable_rows_are_energy_and_exclude_mixed_use_products() -> None:
    """The set is combustion and electricity, and ``324199`` is kept out.

    ⚠️ ``324199`` other petroleum and coal products is the tempting sixth
    member -- MECS fits coal coke at 1.087 -- but the BEA row also carries
    asphalt, lubricants and waxes, which are not burned and do substitute.
    Adding it would pin a mixed-use row on evidence measured for one component.
    """
    assert set(INDISPENSABLE_COMMODITIES) == {
        '211000',
        '212100',
        '221100',
        '221200',
        '324110',
    }
    assert '324199' not in INDISPENSABLE_COMMODITIES
    assert set(INDISPENSABLE_COMMODITIES) <= set(USA_2017_COMMODITY_CODES)
    # They are priced rows, so the pin has a factor to act on.
    assert not set(INDISPENSABLE_COMMODITIES) & set(UNPRICED_COMMODITIES)


def test_the_pin_ships_on() -> None:
    """Asserted in both directions, because the default is the whole change.

    ⚠️ The alternative is not a neutral prior. ``theta = 0`` across the surge
    freezes the *nominal* share, which asserts a real quantity cut equal to the
    price rise -- on commodities an industry cannot do without, that cut did
    not happen and the model reads it as structural change.
    """
    assert get_usa_config().carry_indispensable_commodities_in_full is True
    assert THETA_497 == 1.0
    # ⚠️ With the theta = 1 prior the default, the pin is a no-op in effect and
    # a GUARD in intent: it must still bind against a lower default, which is
    # exactly what a future evidenced departure elsewhere would introduce.
    assert default_theta(2022) == THETA_497
    assert fitted_regime_theta(2022) != THETA_497


@needs_census
def test_the_pin_reaches_the_energy_rows_and_leaves_the_rest_alone() -> None:
    """Energy shares rise against the default; a non-energy row does not move.

    2022 is the year that matters: ``default_theta`` is 0.0 there, so the pin
    is the difference between a frozen nominal share and a frozen real mix.
    """
    config = get_usa_config()
    assert config.carry_indispensable_commodities_in_full

    # Scored against the RETIRED regime value, not against the live default:
    # the prior and the pin now agree, so the default cannot show the pin works.
    pinned = ni.carried_column_shares(2022)
    plain = ni.carried_column_shares(2022, theta=float(fitted_regime_theta(2022)))

    rows = [c for c in INDISPENSABLE_COMMODITIES if c in pinned.index]
    assert float(pinned.loc[rows].to_numpy().sum()) > float(
        plain.loc[rows].to_numpy().sum()
    ), 'the pin must raise the energy rows against a frozen nominal share'


@needs_census
def test_the_pin_never_carries_a_cell_a_survey_already_answered() -> None:
    """Order matters: the observed mask runs after the pin, not before it.

    ⚠️ A seeded energy cell is already nominal, so pinning it at 1.0 and then
    carrying it would double-count the same price movement -- the #997 defect,
    reintroduced through the back door. On these rows 59-84% of the mass is
    survey-answered, so getting this order wrong would be expensive and silent.
    """
    seed, observed = ni.composed_seed_and_observed(2022)
    rows = [c for c in INDISPENSABLE_COMMODITIES if c in seed.index]
    assert bool(observed.loc[rows].to_numpy().any()), 'no observed energy cells'

    frozen = ni.carried_column_shares(2022, theta=0.0)
    pinned = ni.carried_column_shares(2022)

    # Renormalisation moves every cell in a column that contains a carried one,
    # so compare only columns where no indispensable cell is carried at all.
    untouched = [
        column for column in seed.columns if bool(observed.loc[rows, column].all())
    ]
    assert untouched, 'no column has all its energy cells observed'
    pd.testing.assert_frame_equal(
        frozen.loc[rows, untouched], pinned.loc[rows, untouched]
    )


def test_the_prior_does_not_depend_on_the_span() -> None:
    """theta = 1 is a prior about substitution, not a function of the calendar.

    ⚠️ The retired rule keyed off the span; the prior does not. If this starts
    varying by year again, something has reintroduced a fit without saying so.
    """
    values = {
        default_theta(year, base=base)
        for year in range(2018, 2025)
        for base in (2012, 2017, 2020, 2022)
    }
    assert values == {THETA_497}


def test_the_retired_regime_is_still_reachable_and_still_disagrees() -> None:
    """Kept runnable so the choice can be scored rather than argued.

    ⚠️ On the summary panel taken at face value the retired rule scores *better*
    -- no single constant beats its splice (0.5262 against 0.5319 for the best
    constant, 0.25) and theta = 1 sums to 0.5610. The case for retiring it is
    that the panel is not ground truth for 2022-2024, not that it fit worse. So
    it has to stay reachable.
    """
    assert fitted_regime_theta(2024) == THETA_ACROSS_SURGE
    assert fitted_regime_theta(2019) == THETA_OFF_SURGE
    assert fitted_regime_theta(2024) != default_theta(2024)


def test_the_regime_binary_cannot_be_told_from_a_stale_panel() -> None:
    """The measurement that retired the fit, pinned so it cannot be forgotten.

    ⚠️ "Crosses the 2021-22 surge" and "the target year's panel has neither the
    2022 Economic Census nor AIES 2023/24" classify **75 of 78** spans
    identically. So the fit's R² 0.613 supports either reading, and the three
    spans that separate them are all post-2022. Every surge-crossing span ends
    inside the region where BEA stopped incorporating source data.

    This is a property of the span inventory, so it needs no data to check.
    """
    spans = [
        (base, target)
        for base in range(2012, 2025)
        for target in range(2012, 2025)
        if base < target
    ]
    assert len(spans) == 78

    def crosses(base: int, target: int) -> bool:
        return base <= PRICE_SURGE[0] and target >= PRICE_SURGE[1]

    def stale_panel(base: int, target: int) -> bool:
        _ = base
        return target >= PRICE_SURGE[1]

    disagree = [s for s in spans if crosses(*s) != stale_panel(*s)]
    assert len(disagree) == 3
    assert set(disagree) == {(2022, 2023), (2022, 2024), (2023, 2024)}
    # and every surge-crossing span sits inside the unreliable region
    crossing = [s for s in spans if crosses(*s)]
    assert len(crossing) == 30
    assert all(target >= 2022 for _, target in crossing)


# --- held scrap on the BLS PPIs (#768) ---------------------------------------


def test_scrap_ppi_covers_the_span_for_every_series() -> None:
    ppi = scrap_ppi()
    assert set(ppi.columns) == set(SCRAP_PPI_BY_BUYER.values()) | set(
        BENCHMARK_SCRAP_PPI_BY_BUYER.values()
    )
    assert set(INTERMEDIATE_YEARS) <= set(ppi.index)
    assert bool(ppi.notna().to_numpy().all())
    assert bool((ppi > 0).to_numpy().all())


def test_scrap_buyers_are_detail_industries_and_scrap_is_unpriced() -> None:
    assert set(SCRAP_PPI_BY_BUYER) <= set(USA_2017_INDUSTRY_CODES)
    assert set(BENCHMARK_SCRAP_PPI_BY_BUYER) <= set(USA_2017_INDUSTRY_CODES)
    # A buyer is census-measured or carried from the benchmark, never both:
    # both would move the same cell twice.
    assert not set(SCRAP_PPI_BY_BUYER) & set(BENCHMARK_SCRAP_PPI_BY_BUYER)
    # The row has no BEA price index, which is why it needs its own.
    assert SCRAP_COMMODITY in UNPRICED_COMMODITIES
    assert get_usa_config().carry_held_scrap_on_ppi is True
    assert get_usa_config().set_paper_scrap_from_recovered_paper is True


def test_scrap_factor_is_one_through_the_last_census() -> None:
    for year in range(SEED_YEAR, LAST_MATERIALS_CENSUS + 1):
        assert bool((held_scrap_price_factor(year) == 1.0).all()), year


def test_scrap_factor_is_the_ppi_relative_to_2022() -> None:
    """Values checked against data.bls.gov annual averages, 2022 = 1."""
    f2023 = held_scrap_price_factor(2023)
    f2024 = held_scrap_price_factor(2024)
    assert f2023['331110'] == pytest.approx(583.069 / 643.302)
    assert f2024['331110'] == pytest.approx(542.943 / 643.302)
    assert f2024['331314'] == pytest.approx(296.144 / 282.917)
    # One index per metal: the buyers of the same scrap move together.
    assert f2024['331110'] == f2024['331200'] == f2024['331510']
    assert f2024['331314'] == f2024['33131B']


def test_recovered_paper_is_apparent_consumption_over_the_span() -> None:
    consumed = recovered_paper_consumption()
    assert set(INTERMEDIATE_YEARS) <= set(consumed.index)
    # FAOSTAT 2017: 47,626,950 produced + 897,000 imported - 18,289,000 exported.
    assert consumed[2017] == pytest.approx(30_234_950)
    # Mills' use stays within a few percent of 2017 while the price swings.
    ratio = consumed.loc[list(INTERMEDIATE_YEARS)] / consumed[2017]
    assert bool(((ratio > 0.9) & (ratio < 1.05)).all())


def test_benchmark_scrap_value_factor_is_quantity_times_price() -> None:
    """Both relative to 2017, because the cells hold BEA's 2017 dollars."""
    assert bool((benchmark_scrap_value_factor(SEED_YEAR) == 1.0).all())
    consumed = recovered_paper_consumption()
    f2019 = benchmark_scrap_value_factor(2019)
    assert f2019['322130'] == pytest.approx(
        (181.2 / 399.9) * (consumed[2019] / consumed[2017])
    )
    assert f2019.nunique() == 1


def test_set_benchmark_scrap_shares_hits_the_target_and_keeps_the_sum(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The scrap share becomes 2017 dollars x factor / total; the column sums to 1."""
    shares = pd.DataFrame(
        {'322130': [0.10, 0.90], 'other': [0.05, 0.95]},
        index=[SCRAP_COMMODITY, 'c2'],
    )
    bench = pd.DataFrame(
        {'322130': [2.4e9, 21.6e9], 'other': [0.1e9, 0.9e9]},
        index=[SCRAP_COMMODITY, 'c2'],
    )
    monkeypatch.setattr(ni, 'benchmark_intermediate', lambda: bench)
    control = pd.Series({'322130': 30e9, 'other': 1e9})
    out = ni.set_benchmark_scrap_shares(shares, control, 2019)

    target = 2.4e9 * benchmark_scrap_value_factor(2019)['322130'] / 30e9
    assert out.at[SCRAP_COMMODITY, '322130'] == pytest.approx(target)
    assert float(out['322130'].sum()) == pytest.approx(1.0)
    # A column that is not a paper buyer is untouched, and so is the input.
    pd.testing.assert_series_equal(out['other'], shares['other'])
    assert shares.at[SCRAP_COMMODITY, '322130'] == 0.10
    # The identity at the seed year.
    pd.testing.assert_frame_equal(
        ni.set_benchmark_scrap_shares(shares, control, SEED_YEAR), shares
    )


@needs_census
def test_paper_scrap_is_not_census_observed() -> None:
    """The census measures metal scrap only; paper's cells are BEA 2017 values."""
    for year in (2018, 2022, 2024):
        _, observed = ni.composed_seed_and_observed(year)
        scrap = observed.loc[[SCRAP_COMMODITY]]
        paper = list(BENCHMARK_SCRAP_PPI_BY_BUYER)
        assert not bool(scrap[paper].to_numpy().any()), year
        # The metal buyers are still census-measured.
        assert bool(scrap[list(SCRAP_PPI_BY_BUYER)].to_numpy().all()), year


def test_carry_held_scrap_moves_only_observed_buyer_cells() -> None:
    """A held (observed) scrap cell moves; an unobserved one and other rows do not.

    ⚠️ The mask decides. Past 2022 an observed cell holds 2022 dollars and has
    no price movement in it yet; an unobserved cell is priced by the ordinary
    carry, so moving it here would be the double count #997 removed.
    """
    rows = [SCRAP_COMMODITY, 'c2']
    seed = pd.DataFrame(
        {'331110': [50.0, 50.0], '331314': [60.0, 40.0], 'other': [10.0, 90.0]},
        index=rows,
    )
    observed = pd.DataFrame(False, index=rows, columns=seed.columns)
    observed.at[SCRAP_COMMODITY, '331110'] = True
    observed.at[SCRAP_COMMODITY, 'other'] = True

    out = ni._carry_held_scrap(seed, observed, 2024)

    factor = held_scrap_price_factor(2024)['331110'] ** default_theta(2024)
    assert out.at[SCRAP_COMMODITY, '331110'] == pytest.approx(50.0 * factor)
    # 331314 is a scrap buyer but its cell is not observed.
    assert out.at[SCRAP_COMMODITY, '331314'] == 60.0
    # 'other' is observed but not a priced scrap buyer.
    assert out.at[SCRAP_COMMODITY, 'other'] == 10.0
    pd.testing.assert_frame_equal(out.loc[['c2']], seed.loc[['c2']])
    # The input is not mutated.
    assert seed.at[SCRAP_COMMODITY, '331110'] == 50.0
    # And nothing moves through the last census year.
    pd.testing.assert_frame_equal(
        ni._carry_held_scrap(seed, observed, LAST_MATERIALS_CENSUS), seed
    )


@needs_census
def test_held_scrap_carry_reaches_the_metal_buyers_and_nothing_else() -> None:
    """2024: steel scrap shares fall with price, aluminum's rise; others untouched.

    An explicit ``theta`` switches this step off, and at theta = 1.0 it is
    otherwise the same computation as the default, so the difference between
    the two is this step alone.
    """
    default = ni.carried_column_shares(2024)
    explicit = ni.carried_column_shares(2024, theta=float(default_theta(2024)))

    buyers = list(SCRAP_PPI_BY_BUYER)
    pd.testing.assert_frame_equal(
        default.drop(columns=buyers), explicit.drop(columns=buyers)
    )

    def scrap_share(shares: pd.DataFrame, buyer: str) -> float:
        return float(np.asarray(shares.at[SCRAP_COMMODITY, buyer]).item())

    # Steel scrap fell to 0.844 of its 2022 level ...
    assert scrap_share(default, '331110') < scrap_share(explicit, '331110')
    # ... and aluminum scrap rose to 1.047 of it.
    assert scrap_share(default, '331314') > scrap_share(explicit, '331314')


# --- services seed chained across SAS -> AIES --------------------------------


@needs_census
def test_services_seed_chains_across_the_survey_change(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """2023 takes 2022's SAS index; indexing AIES against SAS is the old path.

    ⚠️ Under the old path 2023 is not 2022: management consulting's electricity
    triples at an 11% CV. The chain removes that seam; 2024 then moves only on
    AIES's own 2024/2023 ratios.
    """
    from bedrock.analysis.nowcasting import (  # noqa: PLC0415
        services_transport_expense_seed as st,
    )

    assert get_usa_config().chain_services_seed_across_aies is True
    chained_2022 = st.services_transport_seed(2022)
    chained_2023 = st.services_transport_seed(2023)
    pd.testing.assert_frame_equal(chained_2023, chained_2022)

    monkeypatch.setattr(st, '_chain_across_aies', lambda: False)
    indexed_2023 = st.services_transport_seed(2023)
    assert float((indexed_2023 - chained_2022).abs().to_numpy().max()) > 0.0
