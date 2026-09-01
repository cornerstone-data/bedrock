"""The interior two-margin fit (nowcast_interior_fit).

Pinned on constructed matrices, where the answers are exact: the fitter must
meet both margins to tolerance, conserve zeros (the implicit mask), hold
rather than fabricate on empty or sign-conflicted axes, and report the
row-vs-column wedge instead of hiding it. The real-data behaviour is the
module CLI's job, not a unit test's.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from bedrock.transform.iot import nowcast_interior_fit as fi
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES

MILLION = 1e6


def _seed(rng_fill: float = 0.35) -> pd.DataFrame:
    """A full-size sparse nonnegative seed with a known pattern of zeros."""
    rng = np.random.default_rng(11)
    shape = (len(USA_2017_COMMODITY_CODES), len(USA_2017_INDUSTRY_CODES))
    values = rng.uniform(1.0, 9.0, size=shape) * MILLION * 10
    mask = rng.uniform(size=shape) < rng_fill
    return pd.DataFrame(
        np.where(mask, values, 0.0),
        index=pd.Index(USA_2017_COMMODITY_CODES, name='commodity'),
        columns=pd.Index(USA_2017_INDUSTRY_CODES, name='industry'),
    )


@pytest.fixture(scope='module')
def toy() -> fi.FitResult:
    """One fit of a synthetic seed against synthetic, feasible targets."""
    seed = _seed()
    row_t = seed.sum(axis=1) * 1.30
    col_t = seed.sum(axis=0) * 1.30
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(
            fi, 'interior_row_targets', lambda year: row_t.rename('row_target')
        )
        patch.setattr(
            fi,
            'interior_column_targets',
            lambda year: col_t.rename('column_target'),
        )
        return fi.fit_interior(2023, seed=seed)


def test_both_margins_land_on_their_targets(toy: fi.FitResult) -> None:
    active_rows = toy.row_targets.index.difference(toy.held_rows.index)
    row_miss = (toy.interior.sum(axis=1) - toy.row_targets).loc[active_rows].abs().max()
    active_cols = toy.column_targets.index.difference(toy.held_columns.index)
    col_miss = (
        (toy.interior.sum(axis=0) - toy.column_targets).loc[active_cols].abs().max()
    )
    assert row_miss < fi.TOLERANCE_USD
    assert col_miss < fi.TOLERANCE_USD
    assert toy.residual_usd < fi.TOLERANCE_USD


def test_zero_cells_stay_zero(toy: fi.FitResult) -> None:
    """Multiplicative scaling is the implicit mask; a fabricated cell would be
    a mask violation the balance's Tier 0 could no longer trust."""
    seed = _seed()
    was_zero = seed.to_numpy() == 0.0
    assert (toy.interior.to_numpy()[was_zero] == 0.0).all()


def test_uniform_targets_move_no_structure(toy: fi.FitResult) -> None:
    """Scaling both margins by the same factor must reproduce seed x factor —
    the fit relocates nothing within a row."""
    seed = _seed()
    ratio = toy.interior / seed.replace(0.0, np.nan)
    spread = float(ratio.stack().std())
    assert ratio.stack().mean() == pytest.approx(1.30, abs=0.01)
    assert spread < 0.01


def test_empty_axis_with_a_target_is_held_and_reported() -> None:
    seed = _seed()
    victim = str(seed.index[7])
    seed.loc[victim] = 0.0
    row_t = seed.sum(axis=1) * 1.1
    row_t[victim] = 500 * MILLION  # a target the row cannot reach
    col_t = (
        seed.sum(axis=0)
        * (float(row_t.sum()) / float(seed.sum(axis=0).sum() * 1.1))
        * 1.1
    )

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(
            fi, 'interior_row_targets', lambda year: row_t.rename('row_target')
        )
        patch.setattr(
            fi,
            'interior_column_targets',
            lambda year: col_t.rename('column_target'),
        )
        result = fi.fit_interior(2023, seed=seed)

    assert victim in result.held_rows.index
    assert (result.interior.loc[victim] == 0.0).all()


def test_years_outside_the_margin_span_are_refused() -> None:
    with pytest.raises(ValueError, match='interior fit runs for'):
        fi.fit_interior(2024, seed=_seed())
