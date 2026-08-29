"""The GO control on the Supply industry axis (#724).

The module's whole contract is three invariants at once, so that is what is
tested: the summary cells survive, the columns land on GO shares, and 2017 is
not touched.  One year of the controlled span is enough to exercise every code
path -- the fits are cached and expensive, so the parametrised span lives in
``control_residuals --check``, not here.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import bedrock.transform.iot.nowcast_supply_go_control as gc
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY

MILLION = MILLION_CURRENCY_TO_CURRENCY

#: One controlled year exercises fit, skip, revert and the wedge fixed point.
YEAR = 2023


@pytest.fixture(scope='module')
def seed() -> pd.DataFrame:
    return gc.raw_supply_block(YEAR, download_sources_ok=True)


@pytest.fixture(scope='module')
def controlled(seed: pd.DataFrame) -> pd.DataFrame:
    return gc.go_controlled_supply_block(YEAR, download_sources_ok=True)


def _summary_cells(block: pd.DataFrame) -> pd.DataFrame:
    rows = pd.Series(block.index, index=block.index).map(gc._commodity_parent())
    cols = pd.Series(block.columns, index=block.columns).map(gc._industry_parent())
    grouped = block.groupby(rows).sum()
    return grouped.T.groupby(cols).sum().T


def test_2017_passes_through_untouched() -> None:
    """The benchmark year's detail split is observed and outranks GO's."""
    raw = gc.raw_supply_block(2017, download_sources_ok=True)
    out = gc.go_controlled_supply_block(2017, download_sources_ok=True)

    pd.testing.assert_frame_equal(out, raw)


def test_every_summary_cell_is_preserved(
    seed: pd.DataFrame, controlled: pd.DataFrame
) -> None:
    """Constraint 1: the published summary control must not be given up."""
    gap = (_summary_cells(controlled) - _summary_cells(seed)).abs()

    assert float(gap.to_numpy().max()) < 1.0 * MILLION


def test_the_block_total_is_unchanged(
    seed: pd.DataFrame, controlled: pd.DataFrame
) -> None:
    """The control redistributes; it must not reprice the economy."""
    assert float(controlled.to_numpy().sum()) == pytest.approx(
        float(seed.to_numpy().sum()), rel=1e-12
    )


def test_converged_columns_land_on_their_go_share(controlled: pd.DataFrame) -> None:
    """Constraint 2, on every group the fit did not skip or revert."""
    diagnostics = gc.group_diagnostics(YEAR, download_sources_ok=True)
    fitted_groups = set(diagnostics.index[~diagnostics['note'].astype(bool)])
    wedge = gc._wedge(YEAR, controlled)
    target = gc.gross_output_at_basic(YEAR, wedge)

    parents = pd.Series(
        {code: gc._industry_parent()[code] for code in controlled.columns}
    )
    columns = controlled.sum(axis=0)
    for group in fitted_groups:
        members = list(parents.index[parents == group])
        want = target[members] * (
            float(columns[members].sum()) / float(target[members].sum())
        )
        worst = float((columns[members] - want).abs().max())
        assert worst <= gc.TOLERANCE_USD, (group, worst)


def test_reverted_groups_keep_the_seed_exactly(
    seed: pd.DataFrame, controlled: pd.DataFrame
) -> None:
    """All or nothing per group: a failed fit must not ship a half-fit."""
    diagnostics = gc.group_diagnostics(YEAR, download_sources_ok=True)
    reverted = diagnostics.index[diagnostics['note'].astype(bool)]
    parents = pd.Series({code: gc._industry_parent()[code] for code in seed.columns})
    for group in reverted:
        members = list(parents.index[parents == group])
        pd.testing.assert_frame_equal(controlled[members], seed[members])


def test_no_cell_changes_sign(seed: pd.DataFrame, controlled: pd.DataFrame) -> None:
    """Multiplicative scaling cannot invent mass where the seed has none."""
    seeded = seed.to_numpy()
    fitted = controlled.to_numpy()

    assert not np.any((seeded == 0) & (fitted != 0))
    assert not np.any(np.sign(seeded) * np.sign(fitted) < 0)


def test_uncontrolled_years_are_refused_nothing(seed: pd.DataFrame) -> None:
    """The seed accessor is the cycle-breaker: same frame, no fit."""
    assert gc.seed_commodity_output(YEAR, download_sources_ok=True).equals(
        seed.sum(axis=1)
    )
