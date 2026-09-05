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
    INTERMEDIATE_YEARS,
    MARGIN_YEARS,
    MILLION_CURRENCY_TO_CURRENCY,
    PRICE_SURGE,
    SEED_YEAR,
    SUPPLY_VALUATION_COLUMNS,
    THETA_497,
    THETA_ACROSS_SURGE,
    THETA_OFF_SURGE,
    UNPRICED_COMMODITIES,
    _require_margin_year,
    apply_column_control,
    carry_shares,
    commodity_deflator,
    default_theta,
    derive_intermediate_use,
    margin_rate,
)
from bedrock.utils.config.common import load_env_file_key
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


def test_497s_theta_is_kept_under_its_own_name() -> None:
    """``theta = 1`` is what #497 specified; it is no longer what runs."""
    assert THETA_497 == 1.0
    assert SEED_YEAR == 2017
    assert default_theta(2024) != THETA_497


def test_theta_splits_on_the_price_surge_and_not_on_span_length() -> None:
    """The fitted rule is a regime, so a longer span alone does not move it.

    2017 -> 2021 is four years and does not cross 2021-22; 2020 -> 2022 is two
    and does. If this ever starts keying off ``year - base`` the R^2 0.14
    elapsed-years model has quietly replaced the R^2 0.61 regime one.
    """
    assert default_theta(2021, base=2017) == THETA_OFF_SURGE
    assert default_theta(2022, base=2020) == THETA_ACROSS_SURGE
    assert default_theta(2019, base=2018) == THETA_OFF_SURGE
    assert PRICE_SURGE == (2021, 2022)


def test_every_target_year_from_2022_crosses_the_surge() -> None:
    """The build seeds from 2017, so 2022 on is the frozen-A regime."""
    fitted = {year: default_theta(year) for year in INTERMEDIATE_YEARS}
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
