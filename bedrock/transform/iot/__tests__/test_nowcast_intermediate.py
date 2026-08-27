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
"""

from __future__ import annotations

from typing import cast

import numpy as np
import pandas as pd
import pytest

from bedrock.transform.iot.nowcast_intermediate import (
    DEFAULT_THETA,
    INTERMEDIATE_YEARS,
    SEED_YEAR,
    UNPRICED_COMMODITIES,
    apply_column_control,
    carry_shares,
    derive_intermediate_use,
)
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
    shares = carry_shares(_seed(), _factor())
    live = ['1111B0', '324110']
    assert shares[live].sum(axis=0).round(12).tolist() == [1.0, 1.0]


def test_a_dead_column_stays_dead_instead_of_dividing_by_zero() -> None:
    """``4200ID`` and ``814000`` have no 2017 structure to normalise."""
    shares = carry_shares(_seed(), _factor())
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
    shares = carry_shares(_seed(), _factor())
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
    block = apply_column_control(carry_shares(_seed(), _factor()), control)
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
        apply_column_control(carry_shares(_seed(), _factor()), control)


def test_a_control_missing_an_industry_raises() -> None:
    control = pd.Series({'1111B0': 1_000.0, '324110': 2_500.0})
    with pytest.raises(KeyError, match='missing industries'):
        apply_column_control(carry_shares(_seed(), _factor()), control)


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
        carry_shares(_cancelling_seed(-10.0), pd.Series({'111130': 1.0, '211000': 1.0}))


def test_a_column_whose_carried_shares_cancel_is_refused() -> None:
    """The same failure one step later: the seed is fine, the carry cancels it.

    ``10 x 1 - 5 x 2 = 0``, so the renormalisation would divide by zero and
    propagate ``inf`` through the whole column.
    """
    with pytest.raises(ValueError, match='cannot be renormalised'):
        carry_shares(_cancelling_seed(-5.0), pd.Series({'111130': 1.0, '211000': 2.0}))


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


def test_the_defaults_are_497_as_written() -> None:
    """``theta = 1`` and a 2017 seed are the issue's scope, not a preference."""
    assert DEFAULT_THETA == 1.0
    assert SEED_YEAR == 2017
